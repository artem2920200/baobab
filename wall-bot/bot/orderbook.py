"""Локальный стакан с синхронизацией по протоколу Binance Futures diff-depth.

Алгоритм (официальный):
  1. Открыть стрим <symbol>@depth, буферизовать события.
  2. Получить REST-снапшот /fapi/v1/depth.
  3. Отбросить события с u < lastUpdateId снапшота.
  4. Первое применённое событие: U <= lastUpdateId <= u.
  5. Далее pu каждого события == u предыдущего, иначе ресинк.
  6. Количество в событии — абсолютное; 0 = удалить уровень.
"""
from __future__ import annotations

from .utils import now


class OrderBook:
    def __init__(self, symbol: str, tick_size: float):
        self.symbol = symbol
        self.tick = tick_size if tick_size > 0 else 1e-8
        self.bids: dict[int, float] = {}   # ключ = цена в тиках
        self.asks: dict[int, float] = {}
        self.synced = False
        self.needs_resync = True
        self.syncing = False
        self.last_update_id = 0
        self.last_event_ts = 0.0
        self._last_u = 0
        self._buffer: list[dict] = []
        self._await_first = False

    def _key(self, price: float) -> int:
        return int(round(price / self.tick))

    # ---------- поток событий ----------
    def on_event(self, d: dict) -> None:
        self.last_event_ts = now()
        if not self.synced:
            self._buffer.append(d)
            if len(self._buffer) > 3000:
                self._buffer.clear()
                self.needs_resync = True
            return
        if self._await_first:
            # допускаем и случай U == lastUpdateId+1 (между снапшотом и событием не было апдейтов)
            if not (d["U"] <= self.last_update_id + 1 <= d["u"]):
                self._invalidate()
                return
            self._await_first = False
        elif d.get("pu") != self._last_u:
            self._invalidate()
            return
        self._apply(d)
        self._last_u = d["u"]

    def _invalidate(self) -> None:
        self.synced = False
        self.needs_resync = True
        self._buffer.clear()

    def _apply(self, d: dict) -> None:
        for field, book in (("b", self.bids), ("a", self.asks)):
            for p_str, q_str in d.get(field, []):
                k = self._key(float(p_str))
                q = float(q_str)
                if q == 0.0:
                    book.pop(k, None)
                else:
                    book[k] = q

    # ---------- снапшот ----------
    def start_sync(self, snap: dict) -> bool:
        self.bids.clear()
        self.asks.clear()
        for p_str, q_str in snap.get("bids", []):
            q = float(q_str)
            if q > 0:
                self.bids[self._key(float(p_str))] = q
        for p_str, q_str in snap.get("asks", []):
            q = float(q_str)
            if q > 0:
                self.asks[self._key(float(p_str))] = q
        self.last_update_id = int(snap["lastUpdateId"])
        self._last_u = 0

        bridged = False
        buf, self._buffer = self._buffer, []
        for d in buf:
            if d["u"] < self.last_update_id:
                continue
            if not bridged:
                if not (d["U"] <= self.last_update_id + 1 <= d["u"]):
                    self._invalidate()
                    return False
                bridged = True
            elif d.get("pu") != self._last_u:
                self._invalidate()
                return False
            self._apply(d)
            self._last_u = d["u"]

        self.synced = True
        self.needs_resync = False
        self._await_first = not bridged
        return True

    # ---------- лучшие цены ----------
    @property
    def best_bid(self) -> float | None:
        return max(self.bids) * self.tick if self.bids else None

    @property
    def best_ask(self) -> float | None:
        return min(self.asks) * self.tick if self.asks else None

    @property
    def mid(self) -> float | None:
        bb, ba = self.best_bid, self.best_ask
        if bb is None or ba is None or ba <= 0:
            return None
        return (bb + ba) / 2.0

    def levels(self, side: str, mid: float, max_dist_pct: float) -> list[tuple[float, float]]:
        """Уровни (цена, нотариал $) от лучших наружу до max_dist_pct от mid."""
        out: list[tuple[float, float]] = []
        if side == "bid":
            lo = mid * (1 - max_dist_pct / 100.0)
            for k, q in self.bids.items():
                p = k * self.tick
                if p >= lo:
                    out.append((p, p * q))
            out.sort(key=lambda x: -x[0])
        else:
            hi = mid * (1 + max_dist_pct / 100.0)
            for k, q in self.asks.items():
                p = k * self.tick
                if p <= hi:
                    out.append((p, p * q))
            out.sort(key=lambda x: x[0])
        return out
