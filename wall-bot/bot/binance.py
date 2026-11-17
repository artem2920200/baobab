"""REST-клиент Binance USDT-M Futures (публичные + подписанные запросы)."""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import time
from urllib.parse import urlencode

import aiohttp

log = logging.getLogger("binance")

MAINNET_REST = "https://fapi.binance.com"
MAINNET_WS = "wss://fstream.binance.com"
TESTNET_REST = "https://testnet.binancefuture.com"
TESTNET_WS = "wss://stream.binancefuture.com"


class BinanceError(Exception):
    def __init__(self, code: int, msg: str):
        self.code = code
        self.msg = msg
        super().__init__(f"[{code}] {msg}")


class BinanceBlocked(Exception):
    """HTTP 451/403 — регион/IP заблокирован Binance."""


class BinanceREST:
    def __init__(self, key: str = "", secret: str = "", testnet: bool = False,
                 recv_window: int = 10000):
        self.base = TESTNET_REST if testnet else MAINNET_REST
        self.ws_base = TESTNET_WS if testnet else MAINNET_WS
        self.key = key
        self.secret = secret
        self.recv_window = recv_window
        self.time_offset = 0
        self.used_weight = 0
        self._session: aiohttp.ClientSession | None = None
        self._sem = asyncio.Semaphore(8)

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15),
                headers={"User-Agent": "wall-bot/1.0"},
            )
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def _request(self, method: str, path: str, params: dict | None = None,
                       signed: bool = False, retries: int = 3):
        params = dict(params or {})
        last_err: Exception | None = None
        for attempt in range(retries):
            p = dict(params)
            if signed:
                if not self.key or not self.secret:
                    raise BinanceError(-2015, "API-ключи не заданы")
                p["timestamp"] = int(time.time() * 1000) + self.time_offset
                p["recvWindow"] = self.recv_window
                query = urlencode(p)
                p["signature"] = hmac.new(self.secret.encode(), query.encode(),
                                          hashlib.sha256).hexdigest()
            headers = {"X-MBX-APIKEY": self.key} if signed else {}
            try:
                session = await self._get_session()
                async with self._sem:
                    async with session.request(method, self.base + path,
                                               params=p, headers=headers) as resp:
                        w = resp.headers.get("x-mbx-used-weight-1m")
                        if w:
                            self.used_weight = int(w)
                        if resp.status in (451, 403):
                            raise BinanceBlocked(f"HTTP {resp.status} для {path}")
                        if resp.status in (418, 429):
                            wait = int(resp.headers.get("Retry-After", "5"))
                            log.warning("rate limit %s, sleep %ss", resp.status, wait)
                            await asyncio.sleep(wait + 1)
                            last_err = BinanceError(resp.status, "rate limit")
                            continue
                        if resp.status >= 500:
                            await asyncio.sleep(1 + attempt * 2)
                            last_err = BinanceError(resp.status, "server error")
                            continue
                        data = await resp.json()
                        if resp.status != 200:
                            raise BinanceError(data.get("code", resp.status),
                                               data.get("msg", str(data)))
                        # мягкий троттлинг при приближении к лимиту веса
                        if self.used_weight > 2100:
                            await asyncio.sleep(10)
                        return data
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                last_err = e
                await asyncio.sleep(1 + attempt * 2)
        raise last_err or BinanceError(-1, "request failed")

    # ---------- публичные ----------
    async def ping(self) -> bool:
        await self._request("GET", "/fapi/v1/ping", retries=1)
        return True

    async def sync_time(self) -> None:
        data = await self._request("GET", "/fapi/v1/time")
        self.time_offset = int(data["serverTime"]) - int(time.time() * 1000)

    async def exchange_info(self) -> dict:
        return await self._request("GET", "/fapi/v1/exchangeInfo")

    async def ticker_24h(self) -> list:
        return await self._request("GET", "/fapi/v1/ticker/24hr")

    async def klines(self, symbol: str, interval: str, limit: int = 50) -> list:
        return await self._request("GET", "/fapi/v1/klines",
                                   {"symbol": symbol, "interval": interval, "limit": limit})

    async def depth(self, symbol: str, limit: int = 500) -> dict:
        return await self._request("GET", "/fapi/v1/depth",
                                   {"symbol": symbol, "limit": limit})

    # ---------- торговые (signed) ----------
    async def new_order(self, **params) -> dict:
        return await self._request("POST", "/fapi/v1/order", params, signed=True)

    async def cancel_order(self, symbol: str, client_order_id: str) -> dict:
        return await self._request("DELETE", "/fapi/v1/order",
                                   {"symbol": symbol, "origClientOrderId": client_order_id},
                                   signed=True)

    async def balance(self) -> list:
        return await self._request("GET", "/fapi/v2/balance", signed=True)

    async def set_leverage(self, symbol: str, leverage: int) -> dict:
        return await self._request("POST", "/fapi/v1/leverage",
                                   {"symbol": symbol, "leverage": leverage}, signed=True)

    async def set_margin_type(self, symbol: str, margin_type: str) -> dict | None:
        try:
            return await self._request("POST", "/fapi/v1/marginType",
                                       {"symbol": symbol, "marginType": margin_type}, signed=True)
        except BinanceError as e:
            if e.code == -4046:  # уже установлен нужный тип
                return None
            raise

    async def position_risk(self) -> list:
        return await self._request("GET", "/fapi/v2/positionRisk", signed=True)

    async def create_listen_key(self) -> str:
        data = await self._request("POST", "/fapi/v1/listenKey", signed=True)
        return data["listenKey"]

    async def keepalive_listen_key(self, key: str) -> None:
        await self._request("PUT", "/fapi/v1/listenKey", {"listenKey": key}, signed=True)
