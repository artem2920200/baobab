"""Сканер вселенной: фильтр монет по дневному объёму и NATR."""
from __future__ import annotations

import asyncio
import logging

from .state import AppState
from .utils import natr_pct

log = logging.getLogger("scanner")


class Scanner:
    def __init__(self, cfg: dict, feed, state: AppState):
        self.cfg = cfg
        self.scfg = cfg["scan"]
        self.feed = feed
        self.state = state
        self._stopping = False
        self._first = True

    async def loop(self) -> None:
        while not self._stopping:
            try:
                await self.refresh()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.exception("scanner refresh")
                self.state.add_event("error", "Ошибка сканера", str(e), level="error")
            await asyncio.sleep(self.scfg["refresh_sec"])

    def stop(self) -> None:
        self._stopping = True

    async def refresh(self) -> None:
        min_vol = float(self.scfg["min_volume_24h_usd"])
        max_sym = int(self.scfg["max_symbols"])
        period = int(self.scfg["natr_period"])
        natr_max = float(self.scfg["natr_max_pct"])
        interval = self.scfg["kline_interval"]

        symbols = self.feed.all_symbols()
        volumes = await self.feed.fetch_24h_volumes()
        candidates = sorted(
            (s for s in symbols if volumes.get(s, 0.0) >= min_vol),
            key=lambda s: -volumes[s],
        )
        # проверяем NATR у топа по объёму (с запасом), чтобы не жечь лимиты API
        to_check = candidates[: max_sym * 3]
        passed: dict[str, float] = {}
        sem = asyncio.Semaphore(6)

        async def check(sym: str) -> None:
            async with sem:
                try:
                    rows = await self.feed.fetch_klines(sym, interval, 50)
                    hlc = [(float(r[2]), float(r[3]), float(r[4])) for r in rows]
                    n = natr_pct(hlc, period)
                    if n is not None and n <= natr_max:
                        passed[sym] = round(n, 3)
                except Exception as e:
                    log.debug("natr %s: %s", sym, e)

        await asyncio.gather(*(check(s) for s in to_check))
        selected = sorted(passed, key=lambda s: -volumes[s])[:max_sym]

        self.state.natr_map = passed
        prev = set(self.state.universe_info.get("symbols", []))
        self.state.universe_info = {
            "scanned": len(symbols),
            "volume_passed": len(candidates),
            "passed": len(passed),
            "symbols": selected,
            "volumes": {s: volumes.get(s, 0.0) for s in selected},
        }
        await self.feed.set_universe(selected)

        if self._first or set(selected) != prev:
            added = set(selected) - prev
            removed = prev - set(selected)
            txt = f"{len(selected)} монет под наблюдением (объём>${min_vol/1e6:.0f}M, NATR≤{natr_max}%)"
            if not self._first:
                if added:
                    txt += f" | +{', '.join(sorted(added))}"
                if removed:
                    txt += f" | −{', '.join(sorted(removed))}"
            self.state.add_event("info", "Сканер обновил список монет", txt)
            self._first = False
