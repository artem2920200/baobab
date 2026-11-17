"""Реальные рыночные данные Binance Futures.

Одно combined-WS соединение: <sym>@depth@250ms + <sym>@kline_1m на каждую монету.
Локальные стаканы синхронизируются REST-снапшотами по официальному протоколу.
"""
from __future__ import annotations

import asyncio
import json
import logging

import aiohttp

from .binance import BinanceREST
from .models import KlineRing, SymbolMeta
from .orderbook import OrderBook
from .state import AppState
from .utils import now

log = logging.getLogger("feed")


class LiveFeed:
    source = "live"

    def __init__(self, cfg: dict, client: BinanceREST, state: AppState):
        self.cfg = cfg
        self.client = client
        self.state = state
        self.meta: dict[str, SymbolMeta] = {}
        self.books: dict[str, OrderBook] = {}
        self.rings: dict[str, KlineRing] = {}
        self.universe: list[str] = []
        self.connected = False
        self._ws = None
        self._subs: set[str] = set()
        self._sub_id = 0
        self._tasks: list[asyncio.Task] = []
        self._sync_sem = asyncio.Semaphore(3)
        self._stopping = False
        self._seed_retry: dict[str, float] = {}

    # ---------- жизненный цикл ----------
    async def start(self) -> None:
        info = await self.client.exchange_info()
        for s in info["symbols"]:
            if (s.get("contractType") == "PERPETUAL"
                    and s.get("quoteAsset") == "USDT"
                    and s.get("status") == "TRADING"):
                f = {x["filterType"]: x for x in s["filters"]}
                self.meta[s["symbol"]] = SymbolMeta(
                    symbol=s["symbol"],
                    tick_size=float(f["PRICE_FILTER"]["tickSize"]),
                    step_size=float(f["LOT_SIZE"]["stepSize"]),
                    min_qty=float(f["LOT_SIZE"]["minQty"]),
                    min_notional=float(f.get("MIN_NOTIONAL", {}).get("notional", 5.0)),
                )
        log.info("exchangeInfo: %d USDT-M перпетуалов", len(self.meta))
        self._tasks.append(asyncio.create_task(self._ws_loop()))
        self._tasks.append(asyncio.create_task(self._sync_loop()))

    async def stop(self) -> None:
        self._stopping = True
        for t in self._tasks:
            t.cancel()
        if self._ws is not None:
            await self._ws.close()

    # ---------- интерфейс для сканера ----------
    def all_symbols(self) -> list[str]:
        return list(self.meta)

    async def fetch_24h_volumes(self) -> dict[str, float]:
        data = await self.client.ticker_24h()
        return {t["symbol"]: float(t["quoteVolume"]) for t in data}

    async def fetch_klines(self, symbol: str, interval: str, limit: int = 50) -> list:
        return await self.client.klines(symbol, interval, limit)

    # ---------- вселенная ----------
    async def set_universe(self, symbols: list[str]) -> None:
        new, old = set(symbols), set(self.universe)
        self.universe = list(symbols)
        for s in old - new:
            self.books.pop(s, None)
            self.rings.pop(s, None)
        for s in new - old:
            if s not in self.meta:
                continue
            self.books[s] = OrderBook(s, self.meta[s].tick_size)
            ring = KlineRing()
            try:
                rows = await self.client.klines(s, "1m", 20)
                ring.seed(rows)
            except Exception as e:
                log.warning("kline seed %s: %s", s, e)
                self._seed_retry[s] = now()
            self.rings[s] = ring
        await self._update_subs()

    def _streams(self, syms) -> list[str]:
        speed = self.cfg["book"]["depth_speed"]
        out = []
        for s in syms:
            out.append(f"{s.lower()}@depth@{speed}")
            out.append(f"{s.lower()}@kline_1m")
        return out

    async def _send_sub(self, method: str, streams: list[str]) -> None:
        if not self._ws or not streams:
            return
        for i in range(0, len(streams), 50):
            self._sub_id += 1
            await self._ws.send_json({"method": method, "params": streams[i:i + 50],
                                      "id": self._sub_id})

    async def _update_subs(self) -> None:
        if not self.connected or self._ws is None:
            return
        desired = set(self._streams(self.universe))
        await self._send_sub("UNSUBSCRIBE", list(self._subs - desired))
        await self._send_sub("SUBSCRIBE", list(desired - self._subs))
        self._subs = desired

    # ---------- WebSocket ----------
    async def _ws_loop(self) -> None:
        backoff = 1.0
        while not self._stopping:
            try:
                session = await self.client._get_session()
                async with session.ws_connect(self.client.ws_base + "/stream",
                                              heartbeat=30, max_msg_size=8 * 1024 * 1024) as ws:
                    self._ws = ws
                    self.connected = True
                    self.state.ws_connected = True
                    backoff = 1.0
                    self._subs = set()
                    await self._update_subs()
                    # после (ре)коннекта все стаканы ресинкаем
                    for b in self.books.values():
                        b.synced = False
                        b.needs_resync = True
                        b._buffer.clear()
                    log.info("WS подключен, стримов: %d", len(self._subs))
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            self._handle(json.loads(msg.data))
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("WS ошибка: %s", e)
            finally:
                self.connected = False
                self.state.ws_connected = False
                self._ws = None
            if not self._stopping:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    def _handle(self, msg: dict) -> None:
        stream = msg.get("stream")
        if not stream:
            return  # ответы на SUBSCRIBE и т.п.
        data = msg.get("data", {})
        sym = stream.split("@")[0].upper()
        if "@depth" in stream:
            book = self.books.get(sym)
            if book is not None:
                book.on_event(data)
        elif "@kline_1m" in stream:
            ring = self.rings.get(sym)
            if ring is not None and "k" in data:
                ring.update_ws(data["k"])

    # ---------- синхронизация стаканов ----------
    async def _sync_loop(self) -> None:
        while not self._stopping:
            try:
                synced = 0
                for sym, book in list(self.books.items()):
                    if book.synced:
                        synced += 1
                    if self.connected and book.needs_resync and not book.syncing:
                        asyncio.create_task(self._sync_book(sym))
                    ring = self.rings.get(sym)
                    if (ring is not None and not ring.ready and self.connected
                            and now() - self._seed_retry.get(sym, 0) > 30):
                        self._seed_retry[sym] = now()
                        asyncio.create_task(self._seed_ring(sym))
                self.state.universe_info["books_synced"] = synced
            except Exception:
                log.exception("sync loop")
            await asyncio.sleep(2)

    async def _sync_book(self, sym: str) -> None:
        book = self.books.get(sym)
        if book is None:
            return
        book.syncing = True
        try:
            async with self._sync_sem:
                snap = await self.client.depth(sym, self.cfg["book"]["depth_limit"])
                if not book.start_sync(snap):
                    book.needs_resync = True
                await asyncio.sleep(0.15)
        except Exception as e:
            log.warning("depth sync %s: %s", sym, e)
            book.needs_resync = True
        finally:
            book.syncing = False

    async def _seed_ring(self, sym: str) -> None:
        try:
            rows = await self.client.klines(sym, "1m", 20)
            ring = self.rings.get(sym)
            if ring is not None and not ring.ready:
                ring.seed(rows)
        except Exception as e:
            log.warning("kline reseed %s: %s", sym, e)
