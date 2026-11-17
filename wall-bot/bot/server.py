"""FastAPI-сервер: веб-дашборд + WebSocket-трансляция состояния."""
from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse

from .binance import BinanceREST
from .demo import DemoFeed
from .feed import LiveFeed
from .notify import Notifier
from .scanner import Scanner
from .state import AppState
from .trading import Trading
from .utils import now
from .walls import WallDetector

log = logging.getLogger("server")
WEB_DIR = Path(__file__).resolve().parent.parent / "web"


class BotContext:
    """Собирает и запускает все компоненты бота."""

    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.state = AppState(cfg)
        self.data_client = BinanceREST()
        self.trade_client: BinanceREST | None = None
        self.feed = None
        self.trading: Trading | None = None
        self.scanner: Scanner | None = None
        self.detector: WallDetector | None = None
        self.notifier: Notifier | None = None
        self._tasks: list[asyncio.Task] = []

    async def start(self) -> None:
        cfg, state = self.cfg, self.state
        src = cfg["data_source"]
        live_ok = False
        if src in ("auto", "live"):
            try:
                await asyncio.wait_for(self.data_client.ping(), timeout=8)
                live_ok = True
            except Exception as e:
                if src == "live":
                    state.add_event("error", "Binance недоступен",
                                    f"data_source=live, но API не отвечает: {e}",
                                    level="error")
                else:
                    state.add_event(
                        "error", "Binance недоступен — включён DEMO-режим",
                        f"{e}. Синтетический рынок для отладки; на твоём ПК "
                        f"с нормальным доступом к Binance будут реальные данные.",
                        level="warn")
        if live_ok:
            self.feed = LiveFeed(cfg, self.data_client, state)
            state.data_source = "live"
        else:
            self.feed = DemoFeed(cfg, state)
            state.data_source = "demo"

        if state.mode in ("live", "testnet"):
            self.trade_client = BinanceREST(
                key=cfg["api"]["key"], secret=cfg["api"]["secret"],
                testnet=(state.mode == "testnet"),
                recv_window=int(cfg["api"]["recv_window"]))

        self.trading = Trading(cfg, self.feed, state, self.trade_client)
        self.scanner = Scanner(cfg, self.feed, state)
        self.detector = WallDetector(cfg, self.feed, state, self.trading.on_approach)
        self.notifier = Notifier(cfg, state)

        await self.feed.start()
        await self.trading.start()
        self._tasks = [
            asyncio.create_task(self.scanner.loop()),
            asyncio.create_task(self.detector.loop()),
            asyncio.create_task(self.notifier.loop()),
            asyncio.create_task(self._equity_loop()),
        ]
        if state.data_source == "demo" and state.mode != "paper":
            state.add_event(
                "info", "Демо-данные: сделки симулируются",
                f"Режим {state.mode.upper()} запрошен, но рынок синтетический — "
                f"ордера на биржу не отправляются, работает paper-симуляция.",
                level="warn")
        state.add_event(
            "info", "Бот запущен",
            f"режим {state.mode.upper()} | данные {state.data_source.upper()} | "
            f"подход {cfg['signal']['approach_pct']}% | стена ≥ ×{cfg['walls']['ratio']} "
            f"к 15м объёму | NATR ≤ {cfg['scan']['natr_max_pct']}% | "
            f"объём 24ч > ${cfg['scan']['min_volume_24h_usd']/1e6:.0f}M")

    async def _equity_loop(self) -> None:
        while True:
            s = self.state
            s.equity_curve.append([round(now(), 1),
                                   round(s.equity() + s.unrealized(), 2)])
            await asyncio.sleep(5)

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        if self.scanner:
            self.scanner.stop()
        if self.detector:
            self.detector.stop()
        if self.trading:
            await self.trading.stop()
        if self.feed:
            await self.feed.stop()
        await self.data_client.close()
        if self.trade_client:
            await self.trade_client.close()


def create_app(cfg: dict) -> FastAPI:
    ctx = BotContext(cfg)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await ctx.start()
        yield
        await ctx.stop()

    app = FastAPI(title="Wall Bot", lifespan=lifespan)
    app.state.ctx = ctx

    @app.get("/")
    async def index():
        return FileResponse(WEB_DIR / "index.html")

    @app.get("/api/state")
    async def get_state():
        return JSONResponse(ctx.state.snapshot())

    @app.post("/api/autotrade")
    async def set_autotrade(body: dict):
        enabled = bool(body.get("enabled"))
        ctx.state.autotrade = enabled
        ctx.state.add_event(
            "info", f"Автоторговля {'включена' if enabled else 'выключена'}",
            "переключено из интерфейса", level="warn" if enabled else "info")
        return {"ok": True, "autotrade": enabled}

    @app.post("/api/close/{pos_id}")
    async def close_position(pos_id: str):
        ok = await ctx.trading.close_manual(pos_id)
        return {"ok": ok}

    @app.websocket("/ws")
    async def ws_endpoint(ws: WebSocket):
        await ws.accept()
        try:
            while True:
                await ws.send_json(ctx.state.snapshot())
                await asyncio.sleep(0.5)
        except (WebSocketDisconnect, RuntimeError):
            pass
        except Exception:
            log.debug("ws closed", exc_info=True)

    return app
