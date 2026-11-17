"""Модели данных: метаданные символов, свечи, стены, позиции, события."""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field

from .utils import now, short_id


@dataclass
class SymbolMeta:
    symbol: str
    tick_size: float
    step_size: float
    min_qty: float
    min_notional: float


class KlineRing:
    """Кольцо 1-минутных свечей для подсчёта рыночного объёма за N минут."""

    def __init__(self, maxlen: int = 40):
        self.candles: deque[dict] = deque(maxlen=maxlen)

    def seed(self, rest_rows: list) -> None:
        """REST /fapi/v1/klines: [openTime, o, h, l, c, v, closeTime, q, ...]"""
        self.candles.clear()
        for r in rest_rows:
            self.candles.append({
                "t": int(r[0]), "o": float(r[1]), "h": float(r[2]),
                "l": float(r[3]), "c": float(r[4]), "v": float(r[5]),
                "q": float(r[7]), "closed": True,
            })
        if self.candles:
            self.candles[-1]["closed"] = False  # последняя свеча ещё формируется

    def update_ws(self, k: dict) -> None:
        t = int(k["t"])
        row = {
            "t": t, "o": float(k["o"]), "h": float(k["h"]), "l": float(k["l"]),
            "c": float(k["c"]), "v": float(k["v"]), "q": float(k["q"]),
            "closed": bool(k["x"]),
        }
        if not self.candles:
            self.candles.append(row)
            return
        last = self.candles[-1]
        if t > last["t"]:
            last["closed"] = True
            self.candles.append(row)
        elif t == last["t"]:
            self.candles[-1] = row
        # более старые свечи игнорируем

    def volume_usd(self, window_min: int = 15) -> float:
        """Суммарный объём сделок в USDT за последние window_min минут.

        Каждая сделка имеет агрессора (рыночный ордер), поэтому полный объём
        свечей = рыночный (taker) объём.
        """
        closed = [c for c in self.candles if c["closed"]]
        total = sum(c["q"] for c in closed[-window_min:])
        if self.candles and not self.candles[-1]["closed"]:
            total += self.candles[-1]["q"]
        return total

    @property
    def ready(self) -> bool:
        return len(self.candles) >= 5


@dataclass
class Wall:
    symbol: str
    side: str                    # 'bid' | 'ask'
    price: float                 # VWAP кластера
    near: float                  # ближняя к рынку граница
    far: float                   # дальняя граница (за ней ставим стоп)
    notional: float              # $ в стене
    ratio: float                 # notional / объём за 15м
    vol15m: float
    natr: float = 0.0
    id: str = field(default_factory=short_id)
    peak_notional: float = 0.0
    first_seen: float = field(default_factory=now)
    last_seen: float = field(default_factory=now)
    distance_pct: float = 999.0
    status: str = "watching"     # watching | approaching | triggered | pulled | broken
    triggered: bool = False
    triggered_at: float = 0.0
    gone_at: float = 0.0

    def __post_init__(self):
        self.peak_notional = self.notional

    @property
    def age(self) -> float:
        return now() - self.first_seen

    @property
    def alive(self) -> bool:
        return self.status in ("watching", "approaching", "triggered")

    def to_dict(self) -> dict:
        return {
            "id": self.id, "symbol": self.symbol, "side": self.side,
            "price": self.price, "near": self.near, "far": self.far,
            "notional": self.notional, "peak": self.peak_notional,
            "ratio": self.ratio, "vol15m": self.vol15m, "natr": self.natr,
            "distance_pct": self.distance_pct, "status": self.status,
            "age_sec": round(self.age, 1), "first_seen": self.first_seen,
        }


@dataclass
class Position:
    symbol: str
    side: str                    # 'long' | 'short'
    mode: str                    # paper | testnet | live
    entry: float
    qty_initial: float
    stop: float
    wall_id: str
    wall_price: float
    id: str = field(default_factory=short_id)
    qty_remaining: float = 0.0
    initial_stop: float = 0.0
    be_moved: bool = False
    status: str = "open"         # open | closed
    reason: str = ""             # tp_final | stop | breakeven | wall_pull | manual | error
    opened_at: float = field(default_factory=now)
    closed_at: float = 0.0
    realized: float = 0.0        # $ с учётом комиссий
    fees: float = 0.0
    closes: list = field(default_factory=list)   # {ts, price, qty, pnl, reason}
    ladder: list = field(default_factory=list)   # {move, frac, be, done, qty, price}
    orders: dict = field(default_factory=dict)   # clientOrderId защитных ордеров (live)
    last_price: float = 0.0

    def __post_init__(self):
        if self.qty_remaining == 0.0:
            self.qty_remaining = self.qty_initial
        if self.initial_stop == 0.0:
            self.initial_stop = self.stop
        self.last_price = self.entry

    @property
    def dir(self) -> int:
        return 1 if self.side == "long" else -1

    def move_pct(self, price: float) -> float:
        if self.entry <= 0:
            return 0.0
        return (price / self.entry - 1.0) * 100.0 * self.dir

    def upnl(self, price: float) -> float:
        return (price - self.entry) * self.qty_remaining * self.dir

    @property
    def avg_exit(self) -> float:
        q = sum(c["qty"] for c in self.closes)
        if q <= 0:
            return 0.0
        return sum(c["price"] * c["qty"] for c in self.closes) / q

    def to_dict(self, price: float | None = None) -> dict:
        p = price if price is not None else self.last_price
        return {
            "id": self.id, "symbol": self.symbol, "side": self.side, "mode": self.mode,
            "entry": self.entry, "qty_initial": self.qty_initial,
            "qty_remaining": self.qty_remaining, "stop": self.stop,
            "initial_stop": self.initial_stop, "be_moved": self.be_moved,
            "status": self.status, "reason": self.reason,
            "opened_at": self.opened_at, "closed_at": self.closed_at,
            "realized": self.realized, "fees": self.fees,
            "closes": self.closes, "ladder": self.ladder,
            "price": p, "move_pct": self.move_pct(p), "upnl": self.upnl(p),
            "wall_price": self.wall_price,
            "notional": self.qty_initial * self.entry,
            "duration_sec": round((self.closed_at or now()) - self.opened_at, 1),
        }


@dataclass
class Event:
    type: str                    # approach | entry | tp | breakeven | stop | close | pull | broken | error | info
    title: str
    text: str = ""
    symbol: str = ""
    level: str = "info"          # info | signal | success | warn | error
    id: int = 0
    ts: float = field(default_factory=now)

    def to_dict(self) -> dict:
        return {
            "id": self.id, "ts": self.ts, "type": self.type, "title": self.title,
            "text": self.text, "symbol": self.symbol, "level": self.level,
        }
