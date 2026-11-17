"""Демо-источник данных: синтетический рынок с реалистичными плотностями.

Используется, когда Binance недоступен (data_source: auto/demo).
Полностью повторяет интерфейс LiveFeed, чтобы весь конвейер
(сканер -> детектор -> вход -> лестница тейков) работал без биржи.
"""
from __future__ import annotations

import asyncio
import logging
import math
import random

from .models import KlineRing, SymbolMeta
from .orderbook import OrderBook
from .state import AppState
from .utils import now, round_tick

log = logging.getLogger("demo")

# symbol, base_price, tick, step, volume_24h_usd
DEMO_SYMBOLS = [
    ("BTCUSDT", 97000.0, 0.1, 0.001, 820e6),
    ("ETHUSDT", 3450.0, 0.01, 0.01, 410e6),
    ("SOLUSDT", 195.0, 0.01, 1.0, 260e6),
    ("BNBUSDT", 645.0, 0.01, 0.1, 150e6),
    ("XRPUSDT", 2.25, 0.0001, 1.0, 130e6),
    ("DOGEUSDT", 0.32, 0.00001, 1.0, 95e6),
    ("ADAUSDT", 0.92, 0.0001, 1.0, 60e6),
    ("LINKUSDT", 22.5, 0.001, 0.1, 48e6),
    ("AVAXUSDT", 41.2, 0.01, 1.0, 42e6),
    ("LTCUSDT", 108.0, 0.01, 0.01, 30e6),
]


class _DemoWall:
    __slots__ = ("side", "price", "notional", "expires", "outcome", "pull_at")

    def __init__(self, side, price, notional, expires):
        self.side = side
        self.price = price
        self.notional = notional
        self.expires = expires
        self.outcome = None
        self.pull_at = 0.0


class _Sym:
    def __init__(self, name, base, tick, step, vol24):
        self.name = name
        self.base = base
        self.mid = base
        self.tick = tick
        self.step = step
        self.vol24 = vol24
        self.sigma = 0.00012                      # шум за тик (~0.012%)
        self.v15 = random.uniform(150_000, 1_500_000)
        self.level_usd = self.v15 * 0.004
        self.walls: list[_DemoWall] = []
        self.next_wall_at = now() + random.uniform(3, 25)
        self.drift_dir = 0
        self.drift_until = 0.0
        self.drift_strength = 0.0
        self.minute = int(now() // 60)
        self.candle = {"o": base, "h": base, "l": base, "c": base, "q": 0.0}


class DemoFeed:
    source = "demo"

    def __init__(self, cfg: dict, state: AppState):
        self.cfg = cfg
        self.state = state
        self.meta: dict[str, SymbolMeta] = {}
        self.books: dict[str, OrderBook] = {}
        self.rings: dict[str, KlineRing] = {}
        self.universe: list[str] = []
        self.connected = True
        self._syms: dict[str, _Sym] = {}
        self._tasks: list[asyncio.Task] = []
        self._stopping = False

        for name, base, tick, step, vol24 in DEMO_SYMBOLS:
            s = _Sym(name, base, tick, step, vol24)
            self._syms[name] = s
            self.meta[name] = SymbolMeta(name, tick, step, step, 5.0)
            book = OrderBook(name, tick)
            book.synced = True
            book.needs_resync = False
            self.books[name] = book
            ring = KlineRing()
            ring.seed(self._seed_1m(s))
            self.rings[name] = ring
        self.universe = list(self._syms)

    # ---------- генерация истории ----------
    def _seed_1m(self, s: _Sym) -> list:
        rows = []
        t0 = (int(now() // 60) - 20) * 60_000
        price = s.base
        sigma1m = s.sigma * math.sqrt(240)
        for i in range(20):
            o = price
            c = o * (1 + random.gauss(0, sigma1m))
            h = max(o, c) * (1 + abs(random.gauss(0, sigma1m * 0.5)))
            l = min(o, c) * (1 - abs(random.gauss(0, sigma1m * 0.5)))
            q = s.v15 / 15 * random.lognormvariate(0, 0.25)
            rows.append([t0 + i * 60_000, o, h, l, c, q / c, 0, q])
            price = c
        s.mid = price
        s.candle = {"o": price, "h": price, "l": price, "c": price, "q": 0.0}
        return rows

    def _gen_5m(self, s: _Sym, limit: int) -> list:
        rows = []
        t0 = (int(now() // 300) - limit) * 300_000
        price = s.mid
        sigma5m = s.sigma * math.sqrt(1200)
        for i in range(limit):
            o = price
            c = o
            h, l = o, o
            for _ in range(20):
                c *= 1 + random.gauss(0, sigma5m / math.sqrt(20))
                h, l = max(h, c), min(l, c)
            q = s.v15 / 3 * random.lognormvariate(0, 0.2)
            rows.append([t0 + i * 300_000, o, h, l, c, q / c, 0, q])
            price = c
        return rows

    # ---------- интерфейс ----------
    async def start(self) -> None:
        self._tasks.append(asyncio.create_task(self._tick_loop()))
        log.info("DEMO фид запущен: %d символов", len(self._syms))

    async def stop(self) -> None:
        self._stopping = True
        for t in self._tasks:
            t.cancel()

    def all_symbols(self) -> list[str]:
        return list(self._syms)

    async def fetch_24h_volumes(self) -> dict[str, float]:
        return {n: s.vol24 for n, s in self._syms.items()}

    async def fetch_klines(self, symbol: str, interval: str, limit: int = 50) -> list:
        s = self._syms[symbol]
        if interval == "1m":
            return self._seed_1m(s)
        return self._gen_5m(s, limit)

    async def set_universe(self, symbols: list[str]) -> None:
        self.universe = [s for s in symbols if s in self._syms]

    # ---------- симуляция ----------
    async def _tick_loop(self) -> None:
        while not self._stopping:
            try:
                self._tick()
            except Exception:
                log.exception("demo tick")
            await asyncio.sleep(0.25)

    def _tick(self) -> None:
        t = now()
        for name in self.universe:
            s = self._syms[name]
            self._spawn_walls(s, t)
            self._move_price(s, t)
            self._walls_lifecycle(s, t)
            self._rebuild_book(s)
            self._update_kline(s, t)
        self.state.ws_connected = True

    def _spawn_walls(self, s: _Sym, t: float) -> None:
        if t >= s.next_wall_at and len(s.walls) < 2:
            side = random.choice(["bid", "ask"])
            dist = random.uniform(0.25, 0.9) / 100
            price = round_tick(s.mid * (1 - dist if side == "bid" else 1 + dist), s.tick)
            notional = s.v15 * random.uniform(2.5, 10)
            s.walls.append(_DemoWall(side, price, notional, t + random.uniform(45, 160)))
            s.next_wall_at = t + random.uniform(20, 60)

    def _move_price(self, s: _Sym, t: float) -> None:
        step = random.gauss(0, s.sigma) * s.mid
        # лёгкое притяжение к ближайшей стене — чтобы подход случался
        nearest = None
        nd = 1e9
        for w in s.walls:
            d = abs(w.price - s.mid) / s.mid
            if d < nd:
                nd, nearest = d, w
        if nearest is not None and nd < 0.012 and nearest.outcome != "bounce":
            direction = 1 if nearest.price > s.mid else -1
            step += direction * 0.00008 * s.mid
        if t < s.drift_until:
            step += s.drift_dir * s.drift_strength * s.mid
        step += (s.base - s.mid) * 0.0004  # мягкий возврат к базе
        s.mid = max(s.base * 0.92, min(s.base * 1.08, s.mid + step))

    def _walls_lifecycle(self, s: _Sym, t: float) -> None:
        keep = []
        for w in s.walls:
            d = (s.mid - w.price) / s.mid * 100 * (1 if w.side == "bid" else -1)
            if w.outcome is None and d <= 0.12:
                r = random.random()
                if r < 0.55:      # отскок
                    w.outcome = "bounce"
                    s.drift_dir = 1 if w.side == "bid" else -1
                    s.drift_strength = 0.00025
                    s.drift_until = t + random.uniform(50, 110)
                elif r < 0.85:    # пробой
                    w.outcome = "break"
                    s.drift_dir = -1 if w.side == "bid" else 1
                    s.drift_strength = 0.0003
                    s.drift_until = t + 30
                else:             # спуфинг — снимают перед касанием
                    w.outcome = "spoof"
                    w.pull_at = t + random.uniform(1, 4)
            crossed = (w.side == "bid" and s.mid < w.price * 0.999) or \
                      (w.side == "ask" and s.mid > w.price * 1.001)
            if crossed or t > w.expires or (w.pull_at and t >= w.pull_at):
                continue
            keep.append(w)
        s.walls = keep

    def _rebuild_book(self, s: _Sym) -> None:
        book = self.books[s.name]
        book.bids.clear()
        book.asks.clear()
        spacing = max(s.tick, round_tick(s.mid * 0.0002, s.tick))
        for i in range(1, 251):
            bp = round_tick(s.mid - i * spacing, s.tick)
            ap = round_tick(s.mid + i * spacing, s.tick)
            if bp <= 0:
                continue
            depth_mult = 1 + i / 60
            book.bids[book._key(bp)] = s.level_usd * depth_mult * random.lognormvariate(0, 0.5) / bp
            book.asks[book._key(ap)] = s.level_usd * depth_mult * random.lognormvariate(0, 0.5) / ap
        for w in s.walls:
            k = book._key(w.price)
            target = book.bids if w.side == "bid" else book.asks
            # стена из 3 соседних тиков, 70% в центральном
            for off, frac in ((0, 0.7), (-1, 0.15), (1, 0.15)):
                kk = k + (off if w.side == "bid" else -off)
                price = kk * s.tick
                if price > 0:
                    target[kk] = target.get(kk, 0.0) + w.notional * frac / price
        book.last_event_ts = now()

    def _update_kline(self, s: _Sym, t: float) -> None:
        minute = int(t // 60)
        c = s.candle
        if minute != s.minute:
            s.minute = minute
            s.candle = {"o": s.mid, "h": s.mid, "l": s.mid, "c": s.mid, "q": 0.0}
            c = s.candle
        c["c"] = s.mid
        c["h"] = max(c["h"], s.mid)
        c["l"] = min(c["l"], s.mid)
        c["q"] += s.v15 / 15 / 240 * random.lognormvariate(0, 0.4)
        ring = self.rings[s.name]
        ring.update_ws({"t": minute * 60_000, "o": c["o"], "h": c["h"], "l": c["l"],
                        "c": c["c"], "v": c["q"] / s.mid, "q": c["q"], "x": False})
