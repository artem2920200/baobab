"""Математика и мелкие утилиты."""
from __future__ import annotations

import math
import time
import uuid


def now() -> float:
    return time.time()


def short_id() -> str:
    return uuid.uuid4().hex[:8]


def natr_pct(klines_hlc: list[tuple[float, float, float]], period: int = 14) -> float | None:
    """NATR = ATR(period) / close * 100, сглаживание Уайлдера.

    klines_hlc — список (high, low, close) в хронологическом порядке,
    нужно минимум period+1 свечей.
    """
    if len(klines_hlc) < period + 1:
        return None
    trs: list[float] = []
    for i in range(1, len(klines_hlc)):
        h, l, _c = klines_hlc[i]
        pc = klines_hlc[i - 1][2]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    atr = sum(trs[:period]) / period
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period
    last_close = klines_hlc[-1][2]
    if last_close <= 0:
        return None
    return atr / last_close * 100.0


def floor_step(value: float, step: float) -> float:
    """Округлить количество ВНИЗ до шага лота."""
    if step <= 0:
        return value
    n = math.floor(value / step + 1e-9)
    return round(n * step, 12)


def round_tick(price: float, tick: float) -> float:
    """Округлить цену до тика."""
    if tick <= 0:
        return price
    return round(round(price / tick) * tick, 12)


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def median(values: list[float]) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    n = len(s)
    mid = n // 2
    return s[mid] if n % 2 else (s[mid - 1] + s[mid]) / 2.0


def fmt_usd(v: float) -> str:
    a = abs(v)
    if a >= 1_000_000:
        return f"{v / 1_000_000:.2f}M"
    if a >= 1_000:
        return f"{v / 1_000:.1f}K"
    return f"{v:.0f}"


def fmt_price(p: float) -> str:
    if p >= 1000:
        return f"{p:,.2f}"
    if p >= 1:
        return f"{p:.4f}"
    return f"{p:.6f}"
