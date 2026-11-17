"""Детектор плотностей: кластеризация аномальных уровней и трекинг подхода цены."""
from __future__ import annotations

import asyncio
import logging

from .models import Wall
from .state import AppState
from .utils import median, now

log = logging.getLogger("walls")


class WallDetector:
    def __init__(self, cfg: dict, feed, state: AppState, on_approach):
        self.cfg = cfg
        self.wcfg = cfg["walls"]
        self.scfg = cfg["signal"]
        self.feed = feed
        self.state = state
        self.on_approach = on_approach      # async callable(wall)
        self._stopping = False

    async def loop(self) -> None:
        while not self._stopping:
            try:
                self.tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("detector tick")
            await asyncio.sleep(0.5)

    def stop(self) -> None:
        self._stopping = True

    # ---------- основной проход ----------
    def tick(self) -> None:
        t = now()
        w = self.wcfg
        window = int(w["volume_window_min"])
        approach = float(self.scfg["approach_pct"])

        for sym in list(self.feed.universe):
            book = self.feed.books.get(sym)
            ring = self.feed.rings.get(sym)
            if book is None or not book.synced or ring is None or not ring.ready:
                continue
            mid = book.mid
            if not mid or mid <= 0:
                continue
            vol15 = ring.volume_usd(window)
            if vol15 <= 0:
                continue
            threshold = max(float(w["min_wall_usd"]), float(w["ratio"]) * vol15)
            natr = self.state.natr_map.get(sym, 0.0)

            found: list[tuple] = []
            for side in ("bid", "ask"):
                levels = book.levels(side, mid, float(w["max_distance_pct"]))
                found.extend(self._clusters(side, levels, threshold, mid))

            existing = [x for x in self.state.walls.values()
                        if x.symbol == sym and x.alive]
            matched: set[str] = set()

            for side, near, far, vwap, total in found:
                best, best_d = None, 1e18
                lo, hi = min(near, far), max(near, far)
                for x in existing:
                    if x.side != side or x.id in matched:
                        continue
                    xlo, xhi = min(x.near, x.far), max(x.near, x.far)
                    overlap = lo <= xhi and hi >= xlo
                    d = abs(vwap - x.price) / x.price * 100
                    if (overlap or d <= float(w["match_tolerance_pct"])) and d < best_d:
                        best, best_d = x, d
                ratio = total / vol15 if vol15 > 0 else 0.0
                if best is not None:
                    matched.add(best.id)
                    best.price, best.near, best.far = vwap, near, far
                    best.notional = total
                    best.peak_notional = max(best.peak_notional, total)
                    best.ratio, best.vol15m, best.natr = ratio, vol15, natr
                    best.last_seen = t
                else:
                    wall = Wall(symbol=sym, side=side, price=vwap, near=near, far=far,
                                notional=total, ratio=ratio, vol15m=vol15, natr=natr)
                    self.state.walls[wall.id] = wall
                    existing.append(wall)
                    matched.add(wall.id)

            # судьба несопоставленных стен
            for x in existing:
                if x.id in matched:
                    continue
                if t - x.last_seen < float(w["pull_grace_sec"]):
                    continue
                crossed = (x.side == "bid" and mid < x.far) or \
                          (x.side == "ask" and mid > x.far)
                x.status = "broken" if crossed else "pulled"
                x.gone_at = t
                if x.triggered or x.distance_pct <= approach * 2:
                    if crossed:
                        self.state.add_event(
                            "broken", f"{sym}: стена пробита",
                            f"{'Бид' if x.side == 'bid' else 'Аск'} ${x.notional/1e3:.0f}K "
                            f"на {x.price:.6g} съеден рынком", symbol=sym, level="warn")
                    else:
                        self.state.add_event(
                            "pull", f"{sym}: стену сняли (возможный спуфинг)",
                            f"{'Бид' if x.side == 'bid' else 'Аск'} ${x.notional/1e3:.0f}K "
                            f"на {x.price:.6g} исчез из стакана", symbol=sym, level="warn")

            # дистанция и триггер подхода
            for x in existing:
                if not x.alive:
                    continue
                d = (mid - x.near) / mid * 100 if x.side == "bid" \
                    else (x.near - mid) / mid * 100
                x.distance_pct = max(d, 0.0)
                if not x.triggered:
                    x.status = "approaching" if x.distance_pct <= approach * 2 else "watching"
                if (not x.triggered and x.distance_pct <= approach
                        and x.age >= float(w["min_age_sec"])
                        and x.notional >= threshold * 0.9):
                    x.triggered = True
                    x.triggered_at = t
                    x.status = "triggered"
                    asyncio.create_task(self.on_approach(x))

        # уборка мёртвых стен
        dead = [k for k, x in self.state.walls.items()
                if not x.alive and t - x.gone_at > 60]
        for k in dead:
            self.state.walls.pop(k, None)
        if len(self.state.walls) > 500:
            oldest = sorted(self.state.walls.values(), key=lambda x: x.last_seen)[:100]
            for x in oldest:
                if not x.alive:
                    self.state.walls.pop(x.id, None)

    # ---------- кластеризация ----------
    def _clusters(self, side: str, levels: list[tuple[float, float]],
                  threshold: float, mid: float) -> list[tuple]:
        """levels: (цена, $) от лучших наружу. Возвращает (side, near, far, vwap, total)."""
        w = self.wcfg
        gap = float(w["cluster_gap_pct"]) / 100 * mid
        width = float(w["max_cluster_width_pct"]) / 100 * mid
        min_member = 0.08 * threshold
        min_seed = float(w["min_seed_frac"]) * threshold
        anomaly_mult = float(w["anomaly_mult"])
        out: list[tuple] = []

        def finalize(cluster: list[tuple[float, float]]) -> None:
            if not cluster:
                return
            total = sum(n for _, n in cluster)
            mx = max(n for _, n in cluster)
            if total < threshold or mx < min_seed:
                return
            if anomaly_mult > 0:
                prices = {p for p, _ in cluster}
                others = [n for p, n in levels if p not in prices][:80]
                med = median(others)
                if med > 0 and total < anomaly_mult * med:
                    return
            vwap = sum(p * n for p, n in cluster) / total
            out.append((side, cluster[0][0], cluster[-1][0], vwap, total))

        cluster: list[tuple[float, float]] = []
        for p, n in levels:
            if n < min_member:
                finalize(cluster)
                cluster = []
                continue
            if cluster and (abs(cluster[-1][0] - p) > gap
                            or abs(cluster[0][0] - p) > width):
                finalize(cluster)
                cluster = []
            cluster.append((p, n))
        finalize(cluster)
        return out
