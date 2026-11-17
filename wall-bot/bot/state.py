"""Общее состояние приложения: стены, позиции, сделки, события, эквити."""
from __future__ import annotations

import json
import logging
from collections import deque
from pathlib import Path

from .models import Event, Position, Wall
from .utils import now

log = logging.getLogger("state")


class AppState:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.walls: dict[str, Wall] = {}
        self.positions: dict[str, Position] = {}
        self.trades: deque = deque(maxlen=200)
        self.events: deque = deque(maxlen=300)
        self.equity_curve: deque = deque(maxlen=400)
        self._event_id = 0
        self.event_sink = None                      # колбэк нотификатора (Telegram)

        self.paper_equity: float = float(cfg["trading"]["paper_equity"])
        self.paper_start: float = self.paper_equity
        self.live_balance: float = 0.0
        self.cooldowns: dict[str, float] = {}
        self.natr_map: dict[str, float] = {}
        self.universe_info = {"scanned": 0, "passed": 0, "symbols": []}

        self.mode: str = cfg["mode"]
        self.data_source: str = "live"
        self.execution_enabled: bool = False
        self.autotrade: bool = bool(cfg["trading"]["autotrade"])
        self.ws_connected: bool = False
        self.telegram_active: bool = False
        self.started_at: float = now()
        self.last_error: str = ""

        self.data_dir = Path(__file__).resolve().parent.parent / "data"
        self.data_dir.mkdir(exist_ok=True)
        self._load_history()

    # ---------- события ----------
    def add_event(self, type_: str, title: str, text: str = "",
                  symbol: str = "", level: str = "info") -> Event:
        self._event_id += 1
        ev = Event(type=type_, title=title, text=text, symbol=symbol,
                   level=level, id=self._event_id)
        self.events.append(ev)
        log.info("[%s] %s %s | %s", type_, symbol, title, text)
        if type_ == "error":
            self.last_error = f"{title}: {text}"
        if self.event_sink:
            try:
                self.event_sink(ev)
            except Exception:
                log.exception("event sink failed")
        return ev

    # ---------- деньги ----------
    def equity(self) -> float:
        if self.mode == "live" or self.mode == "testnet":
            return self.live_balance
        return self.paper_equity

    def unrealized(self) -> float:
        return sum(p.upnl(p.last_price) for p in self.positions.values()
                   if p.status == "open")

    def realized_today(self) -> float:
        day_start = now() - 86400
        return sum(t["realized"] for t in self.trades if t["closed_at"] >= day_start)

    # ---------- сделки ----------
    def record_trade(self, pos: Position) -> None:
        d = pos.to_dict()
        self.trades.appendleft(d)
        self._persist_trade(d)

    def open_positions(self) -> list[Position]:
        return [p for p in self.positions.values() if p.status == "open"]

    def position_for_symbol(self, symbol: str) -> Position | None:
        for p in self.positions.values():
            if p.symbol == symbol and p.status == "open":
                return p
        return None

    # ---------- персистентность ----------
    def _state_file(self) -> Path:
        return self.data_dir / ("paper_state.json" if self.mode == "paper" else "live_trades.json")

    def _load_history(self) -> None:
        f = self._state_file()
        if not f.exists():
            return
        try:
            data = json.loads(f.read_text())
            if self.mode == "paper":
                self.paper_equity = float(data.get("equity", self.paper_equity))
                self.paper_start = float(data.get("start", self.paper_start))
            for t in data.get("trades", [])[:100]:
                self.trades.append(t)
        except Exception:
            log.exception("не удалось загрузить историю")

    def _persist_trade(self, trade: dict) -> None:
        if self.data_source == "demo":
            return  # демо-сделки не сохраняем
        try:
            f = self._state_file()
            data = {"equity": self.paper_equity, "start": self.paper_start,
                    "trades": list(self.trades)[:100]}
            f.write_text(json.dumps(data, ensure_ascii=False))
        except Exception:
            log.exception("не удалось сохранить историю")

    # ---------- снапшот для UI ----------
    def snapshot(self) -> dict:
        walls = sorted(self.walls.values(),
                       key=lambda w: (not w.alive, w.distance_pct))[:150]
        cfg = self.cfg
        return {
            "ts": now(),
            "mode": self.mode,
            "data_source": self.data_source,
            "execution_enabled": self.execution_enabled,
            "autotrade": self.autotrade,
            "ws_connected": self.ws_connected,
            "telegram_active": self.telegram_active,
            "uptime_sec": round(now() - self.started_at),
            "last_error": self.last_error,
            "equity": self.equity(),
            "paper_start": self.paper_start,
            "unrealized": self.unrealized(),
            "realized_today": self.realized_today(),
            "universe": self.universe_info,
            "books_synced": self.universe_info.get("books_synced", 0),
            "walls": [w.to_dict() for w in walls],
            "positions": [p.to_dict() for p in self.open_positions()],
            "trades": list(self.trades)[:60],
            "events": [e.to_dict() for e in list(self.events)[-120:]][::-1],
            "equity_curve": list(self.equity_curve),
            "settings": {
                "approach_pct": cfg["signal"]["approach_pct"],
                "ratio": cfg["walls"]["ratio"],
                "natr_max": cfg["scan"]["natr_max_pct"],
                "min_vol": cfg["scan"]["min_volume_24h_usd"],
                "risk": cfg["trading"]["risk_per_trade"],
                "max_lev": cfg["trading"]["max_leverage"],
                "max_pos": cfg["trading"]["max_open_positions"],
                "stop_buffer": cfg["trading"]["stop_buffer_pct"],
                "ladder": cfg["trading"]["take_profit"],
                "exit_on_pull": cfg["trading"]["exit_on_wall_pull"],
            },
        }
