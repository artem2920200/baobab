"""Торговля: вход на отскок, стоп за плотностью, лестница фиксации прибыли.

Paper: симуляция по ценам стакана (комиссии + проскальзывание).
Live/Testnet: реальные ордера MARKET + STOP_MARKET + TAKE_PROFIT_MARKET,
события исполнения через user data stream.
"""
from __future__ import annotations

import asyncio
import json
import logging

import aiohttp

from .binance import BinanceError, BinanceREST
from .models import Position, Wall
from .state import AppState
from .utils import floor_step, fmt_price, fmt_usd, now, round_tick

log = logging.getLogger("trading")


def _decimals(step: float) -> int:
    s = f"{step:.12f}".rstrip("0")
    return len(s.split(".")[1]) if "." in s else 0


def qstr(q: float, step: float) -> str:
    return f"{q:.{_decimals(step)}f}"


def pstr(p: float, tick: float) -> str:
    return f"{p:.{_decimals(tick)}f}"


class Trading:
    def __init__(self, cfg: dict, feed, state: AppState, client: BinanceREST | None):
        self.cfg = cfg
        self.tcfg = cfg["trading"]
        self.feed = feed
        self.state = state
        self.client = client
        # симуляция, если paper-режим или демо-данные
        self.sim = state.mode == "paper" or state.data_source == "demo"
        self._lev_ready: set[str] = set()
        self._locks: dict[str, asyncio.Lock] = {}
        self._tasks: list[asyncio.Task] = []
        self._stopping = False

    # ================= жизненный цикл =================
    async def start(self) -> None:
        self._tasks.append(asyncio.create_task(self._loop()))
        if self.sim:
            self.state.execution_enabled = True
            self.state.add_event(
                "info", "Симуляция сделок активна (paper)",
                f"Стартовый депозит ${self.state.paper_equity:.2f} | "
                f"риск {self.tcfg['risk_per_trade']*100:.1f}% на сделку",
                level="success")
        else:
            await self._preflight()
            if self.state.execution_enabled:
                self._tasks.append(asyncio.create_task(self._balance_loop()))
                self._tasks.append(asyncio.create_task(self._user_stream_loop()))

    async def stop(self) -> None:
        self._stopping = True
        for t in self._tasks:
            t.cancel()

    async def _preflight(self) -> None:
        if not self.client or not self.client.key or not self.client.secret:
            self.state.execution_enabled = False
            self.state.add_event(
                "error", "API-ключи не заданы",
                "Режим live, но ключей нет — бот работает как монитор (сигналы без сделок). "
                "Впиши key/secret в config.yaml или env BINANCE_API_KEY/SECRET.",
                level="warn")
            return
        try:
            await self.client.sync_time()
            await self._poll_balance()
            risks = await self.client.position_risk()
            open_foreign = [r for r in risks if abs(float(r.get("positionAmt", 0))) > 0]
            if open_foreign:
                syms = ", ".join(r["symbol"] for r in open_foreign[:5])
                self.state.add_event(
                    "error", "На аккаунте уже есть открытые позиции",
                    f"{syms} — бот ими НЕ управляет. Закрой их или будь осторожен.",
                    level="warn")
            self.state.execution_enabled = True
            self.state.add_event(
                "info", f"Торговля активна: {self.state.mode.upper()}",
                f"Депозит ${self.state.live_balance:.2f} | риск {self.tcfg['risk_per_trade']*100:.1f}% "
                f"| плечо ≤{self.tcfg['max_leverage']}x", level="success")
        except Exception as e:
            self.state.execution_enabled = False
            self.state.add_event("error", "Не удалось подключиться к торговому API",
                                 str(e), level="error")

    # ================= сигнал подхода =================
    async def on_approach(self, wall: Wall) -> None:
        side_ru = "Бид-стена" if wall.side == "bid" else "Аск-стена"
        direction = "LONG (отскок вверх)" if wall.side == "bid" else "SHORT (отскок вниз)"
        self.state.add_event(
            "approach", f"{wall.symbol}: цена подошла к плотности",
            f"{side_ru} ${fmt_usd(wall.notional)} (×{wall.ratio:.1f} к 15м объёму) "
            f"на {fmt_price(wall.price)} | дистанция {wall.distance_pct:.3f}% | {direction}",
            symbol=wall.symbol, level="signal")
        if not self.state.autotrade or not self.state.execution_enabled:
            return
        try:
            await self.try_entry(wall)
        except Exception as e:
            log.exception("entry")
            self.state.add_event("error", f"{wall.symbol}: ошибка входа", str(e),
                                 symbol=wall.symbol, level="error")

    # ================= вход =================
    async def try_entry(self, wall: Wall) -> None:
        sym = wall.symbol
        lock = self._locks.setdefault(sym, asyncio.Lock())
        async with lock:
            t = now()
            if not wall.alive:
                return
            if self.state.position_for_symbol(sym):
                return
            if len(self.state.open_positions()) >= int(self.tcfg["max_open_positions"]):
                self.state.add_event("info", f"{sym}: пропуск входа",
                                     "достигнут лимит одновременных позиций", symbol=sym)
                return
            if t < self.state.cooldowns.get(sym, 0):
                return
            book = self.feed.books.get(sym)
            meta = self.feed.meta.get(sym)
            ring = self.feed.rings.get(sym)
            if not book or not book.synced or not meta or not ring:
                return
            bb, ba = book.best_bid, book.best_ask
            if not bb or not ba:
                return
            vol15 = ring.volume_usd(int(self.cfg["walls"]["volume_window_min"]))
            threshold = max(float(self.cfg["walls"]["min_wall_usd"]),
                            float(self.cfg["walls"]["ratio"]) * vol15)
            if wall.notional < threshold * 0.9:
                return  # стена усохла к моменту входа
            if wall.age < float(self.cfg["walls"]["min_age_sec"]):
                return

            side = "long" if wall.side == "bid" else "short"
            slip = float(self.tcfg["slippage_pct"]) / 100
            entry = ba * (1 + slip) if side == "long" else bb * (1 - slip)
            buf = float(self.tcfg["stop_buffer_pct"]) / 100
            stop = wall.far * (1 - buf) if side == "long" else wall.far * (1 + buf)
            stop = round_tick(stop, meta.tick_size)
            stop_dist = abs(entry - stop) / entry
            if stop_dist <= 0:
                return
            if stop_dist * 100 > float(self.tcfg["max_stop_pct"]):
                self.state.add_event(
                    "info", f"{sym}: пропуск входа",
                    f"стоп за стеной слишком далеко: {stop_dist*100:.2f}% "
                    f"> макс {self.tcfg['max_stop_pct']}%", symbol=sym)
                return

            equity = self.state.equity()
            if equity <= 0:
                return
            risk_usd = equity * float(self.tcfg["risk_per_trade"])
            qty = risk_usd / (entry * stop_dist)
            max_notional = equity * float(self.tcfg["max_leverage"])
            if qty * entry > max_notional:
                qty = max_notional / entry
            qty = floor_step(qty, meta.step_size)
            if qty <= 0 or qty < meta.min_qty or qty * entry < meta.min_notional:
                self.state.add_event(
                    "info", f"{sym}: пропуск входа",
                    "размер позиции меньше минимума биржи при заданном риске", symbol=sym)
                return

            pos = Position(symbol=sym, side=side,
                           mode="paper" if self.sim else self.state.mode,
                           entry=entry, qty_initial=qty, stop=stop,
                           wall_id=wall.id, wall_price=wall.price)
            pos.ladder = [{"move": float(s["move_pct"]), "frac": float(s["close_frac"]),
                           "be": bool(s.get("breakeven", False)),
                           "done": False, "qty": 0.0, "price": 0.0}
                          for s in self.tcfg["take_profit"]]

            if self.sim:
                fee = entry * qty * float(self.tcfg["fee_rate"])
                pos.fees += fee
                pos.realized -= fee
                self.state.paper_equity -= fee
                self.state.positions[pos.id] = pos
                self._entry_event(pos, risk_usd)
            else:
                if await self._live_open(pos, meta):
                    self.state.positions[pos.id] = pos
                    self._entry_event(pos, risk_usd)

    def _entry_event(self, pos: Position, risk_usd: float) -> None:
        ladder_txt = ", ".join(
            f"+{s['move']:g}%→{s['frac']*100:.0f}%{' +БУ' if s['be'] else ''}"
            for s in pos.ladder)
        self.state.add_event(
            "entry", f"{pos.symbol}: вход {pos.side.upper()}",
            f"{qstr(pos.qty_initial, self.feed.meta[pos.symbol].step_size)} "
            f"по {fmt_price(pos.entry)} (${fmt_usd(pos.qty_initial*pos.entry)}) | "
            f"стоп за стеной {fmt_price(pos.stop)} | риск ~${risk_usd:.2f} | TP: {ladder_txt}",
            symbol=pos.symbol, level="success")

    # ================= live: ордера =================
    async def _live_open(self, pos: Position, meta) -> bool:
        assert self.client is not None
        sym = pos.symbol
        exit_side = "SELL" if pos.side == "long" else "BUY"
        try:
            if sym not in self._lev_ready:
                await self.client.set_margin_type(sym, self.tcfg["margin_type"])
                await self.client.set_leverage(sym, int(self.tcfg["max_leverage"]))
                self._lev_ready.add(sym)
            resp = await self.client.new_order(
                symbol=sym, side="BUY" if pos.side == "long" else "SELL",
                type="MARKET", quantity=qstr(pos.qty_initial, meta.step_size),
                newClientOrderId=f"wb{pos.id}e", newOrderRespType="RESULT")
            ap = float(resp.get("avgPrice") or 0)
            filled = float(resp.get("executedQty") or 0)
            if filled <= 0:
                raise BinanceError(-1, "вход не исполнен")
            if ap > 0:
                pos.entry = ap
            pos.qty_initial = pos.qty_remaining = filled
            await self._place_protection(pos, meta, exit_side)
            return True
        except Exception as e:
            self.state.add_event("error", f"{sym}: ошибка выставления ордеров",
                                 str(e), symbol=sym, level="error")
            await self._emergency_close(pos)
            return False

    async def _place_protection(self, pos: Position, meta, exit_side: str) -> None:
        assert self.client is not None
        sym = pos.symbol
        sl_cid = f"wb{pos.id}s"
        await self.client.new_order(
            symbol=sym, side=exit_side, type="STOP_MARKET",
            quantity=qstr(pos.qty_remaining, meta.step_size),
            stopPrice=pstr(pos.stop, meta.tick_size), reduceOnly="true",
            workingType="CONTRACT_PRICE", newClientOrderId=sl_cid)
        pos.orders["sl"] = sl_cid
        allocated = 0.0
        for i, step in enumerate(pos.ladder):
            last = i == len(pos.ladder) - 1
            q = pos.qty_initial - allocated if last \
                else floor_step(pos.qty_initial * step["frac"], meta.step_size)
            q = floor_step(q, meta.step_size)
            if q <= 0:
                step["done"] = True
                continue
            allocated += q
            act = pos.entry * (1 + step["move"] / 100 * pos.dir)
            cid = f"wb{pos.id}t{i + 1}"
            await self.client.new_order(
                symbol=sym, side=exit_side, type="TAKE_PROFIT_MARKET",
                quantity=qstr(q, meta.step_size),
                stopPrice=pstr(round_tick(act, meta.tick_size), meta.tick_size),
                reduceOnly="true", workingType="CONTRACT_PRICE", newClientOrderId=cid)
            pos.orders[f"t{i + 1}"] = cid

    async def _emergency_close(self, pos: Position) -> None:
        """Позиция открыта, но защита не встала — немедленно закрываем."""
        if not self.client:
            return
        try:
            await self.client.new_order(symbol=pos.symbol,
                                        side="SELL" if pos.side == "long" else "BUY",
                                        type="MARKET", closePosition="true")
            self.state.add_event("error", f"{pos.symbol}: аварийное закрытие",
                                 "защитные ордера не выставились, позиция закрыта по рынку",
                                 symbol=pos.symbol, level="warn")
        except Exception as e:
            self.state.add_event(
                "error", f"{pos.symbol}: НЕ УДАЛОСЬ аварийно закрыть позицию!",
                f"{e} — ЗАКРОЙ ВРУЧНУЮ В ТЕРМИНАЛЕ", symbol=pos.symbol, level="error")

    async def _live_move_be(self, pos: Position) -> None:
        assert self.client is not None
        meta = self.feed.meta[pos.symbol]
        exit_side = "SELL" if pos.side == "long" else "BUY"
        try:
            try:
                await self.client.cancel_order(pos.symbol, pos.orders["sl"])
            except BinanceError as e:
                if e.code != -2011:
                    raise
            cid = f"wb{pos.id}b"
            await self.client.new_order(
                symbol=pos.symbol, side=exit_side, type="STOP_MARKET",
                quantity=qstr(pos.qty_remaining, meta.step_size),
                stopPrice=pstr(pos.stop, meta.tick_size), reduceOnly="true",
                workingType="CONTRACT_PRICE", newClientOrderId=cid)
            pos.orders["sl"] = cid
        except Exception as e:
            self.state.add_event("error", f"{pos.symbol}: не удалось переставить стоп в БУ",
                                 f"{e} — закрываю позицию по рынку", symbol=pos.symbol,
                                 level="error")
            await self.live_close(pos, "error")

    async def live_close(self, pos: Position, reason: str) -> None:
        if getattr(pos, "_closing", False) or pos.status != "open":
            return
        pos._closing = True
        assert self.client is not None
        meta = self.feed.meta[pos.symbol]
        try:
            for cid in list(pos.orders.values()):
                try:
                    await self.client.cancel_order(pos.symbol, cid)
                except BinanceError:
                    pass
            resp = await self.client.new_order(
                symbol=pos.symbol, side="SELL" if pos.side == "long" else "BUY",
                type="MARKET", quantity=qstr(pos.qty_remaining, meta.step_size),
                reduceOnly="true", newOrderRespType="RESULT")
            ap = float(resp.get("avgPrice") or pos.last_price)
            z = float(resp.get("executedQty") or pos.qty_remaining)
            self._apply_close(pos, ap, z, reason)
            self._finalize(pos, reason)
        except Exception as e:
            self.state.add_event("error", f"{pos.symbol}: ошибка закрытия", str(e),
                                 symbol=pos.symbol, level="error")
        finally:
            pos._closing = False

    # ================= user data stream =================
    async def _user_stream_loop(self) -> None:
        assert self.client is not None
        backoff = 1.0
        while not self._stopping:
            keepalive: asyncio.Task | None = None
            try:
                key = await self.client.create_listen_key()

                async def _ka():
                    while True:
                        await asyncio.sleep(1800)
                        await self.client.keepalive_listen_key(key)

                keepalive = asyncio.create_task(_ka())
                session = await self.client._get_session()
                async with session.ws_connect(self.client.ws_base + "/ws/" + key,
                                              heartbeat=30) as ws:
                    backoff = 1.0
                    log.info("user data stream подключен")
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            if data.get("e") == "ORDER_TRADE_UPDATE":
                                self._on_order_update(data.get("o", {}))
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("user stream: %s", e)
            finally:
                if keepalive:
                    keepalive.cancel()
            if not self._stopping:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    def _on_order_update(self, o: dict) -> None:
        cid = o.get("c", "")
        if not cid.startswith("wb") or o.get("X") != "FILLED":
            return
        pos = self.state.positions.get(cid[2:10])
        if not pos or pos.status != "open":
            return
        ap = float(o.get("ap") or o.get("L") or 0) or pos.last_price
        z = float(o.get("z") or 0)
        if z <= 0:
            return
        if cid == pos.orders.get("sl"):
            self._apply_close(pos, ap, z, "breakeven" if pos.be_moved else "stop")
            self._finalize(pos, "breakeven" if pos.be_moved else "stop")
            asyncio.create_task(self._cancel_protective(pos))
            return
        for i, step in enumerate(pos.ladder):
            if cid == pos.orders.get(f"t{i + 1}") and not step["done"]:
                step.update(done=True, qty=z, price=ap)
                self._apply_close(pos, ap, z, f"tp{i + 1}")
                self._tp_event(pos, i, ap, z)
                if step["be"] and not pos.be_moved:
                    pos.be_moved = True
                    off = float(self.tcfg["breakeven_offset_pct"]) / 100
                    pos.stop = round_tick(pos.entry * (1 + off * pos.dir),
                                          self.feed.meta[pos.symbol].tick_size)
                    self._be_event(pos)
                    asyncio.create_task(self._live_move_be(pos))
                if pos.qty_remaining <= 1e-12 or i == len(pos.ladder) - 1:
                    self._finalize(pos, "tp_final")
                    asyncio.create_task(self._cancel_protective(pos))
                return

    async def _cancel_protective(self, pos: Position) -> None:
        if not self.client:
            return
        for cid in list(pos.orders.values()):
            try:
                await self.client.cancel_order(pos.symbol, cid)
            except BinanceError:
                pass

    # ================= общий цикл сопровождения =================
    async def _loop(self) -> None:
        while not self._stopping:
            try:
                self._tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("trading tick")
            await asyncio.sleep(0.25)

    def _tick(self) -> None:
        for pos in self.state.open_positions():
            book = self.feed.books.get(pos.symbol)
            if not book or not book.synced:
                continue
            price = book.best_bid if pos.side == "long" else book.best_ask
            if not price:
                continue
            pos.last_price = price
            # спуфинг-защита: стену убрали после входа → выходим
            if self.tcfg["exit_on_wall_pull"]:
                wall = self.state.walls.get(pos.wall_id)
                if (wall and wall.status == "pulled"
                        and now() - pos.opened_at > 5
                        and not getattr(pos, "_closing", False)):
                    if self.sim:
                        self._close_all(pos, price, "wall_pull")
                    else:
                        asyncio.create_task(self.live_close(pos, "wall_pull"))
                    continue
            if self.sim:
                self._manage_paper(pos, price)

    def _manage_paper(self, pos: Position, price: float) -> None:
        # 1) стоп проверяем первым (консервативно при гэпах)
        if (pos.side == "long" and price <= pos.stop) or \
           (pos.side == "short" and price >= pos.stop):
            self._close_all(pos, price, "breakeven" if pos.be_moved else "stop")
            return
        # 2) лестница тейков
        move = pos.move_pct(price)
        meta = self.feed.meta[pos.symbol]
        slip = float(self.tcfg["slippage_pct"]) / 100
        for i, step in enumerate(pos.ladder):
            if step["done"] or move < step["move"]:
                continue
            last = i == len(pos.ladder) - 1
            q = pos.qty_remaining if last else min(
                floor_step(pos.qty_initial * step["frac"], meta.step_size),
                pos.qty_remaining)
            if q <= 0:
                step["done"] = True
                continue
            fill = price * (1 - slip * pos.dir)
            self._apply_close(pos, fill, q, f"tp{i + 1}")
            step.update(done=True, qty=q, price=fill)
            self._tp_event(pos, i, fill, q)
            if step["be"] and not pos.be_moved:
                pos.be_moved = True
                off = float(self.tcfg["breakeven_offset_pct"]) / 100
                pos.stop = round_tick(pos.entry * (1 + off * pos.dir), meta.tick_size)
                self._be_event(pos)
            if pos.qty_remaining <= 1e-12:
                self._finalize(pos, "tp_final")
                return

    def _close_all(self, pos: Position, price: float, reason: str) -> None:
        slip = float(self.tcfg["slippage_pct"]) / 100
        fill = price * (1 - slip * pos.dir)
        self._apply_close(pos, fill, pos.qty_remaining, reason)
        self._finalize(pos, reason)

    # ================= учёт закрытий =================
    def _apply_close(self, pos: Position, price: float, qty: float, reason: str) -> None:
        qty = min(qty, pos.qty_remaining)
        if qty <= 0:
            return
        pnl = (price - pos.entry) * qty * pos.dir
        fee = price * qty * float(self.tcfg["fee_rate"])
        pos.realized += pnl - fee
        pos.fees += fee
        pos.qty_remaining = max(0.0, pos.qty_remaining - qty)
        pos.closes.append({"ts": now(), "price": price, "qty": qty,
                           "pnl": pnl - fee, "reason": reason})
        if self.sim:
            self.state.paper_equity += pnl - fee

    def _tp_event(self, pos: Position, i: int, price: float, qty: float) -> None:
        step = pos.ladder[i]
        self.state.add_event(
            "tp", f"{pos.symbol}: TP{i + 1} (+{step['move']:g}%)",
            f"закрыто {qty:.6g} по {fmt_price(price)} | "
            f"остаток {pos.qty_remaining:.6g} | PnL сделки ${pos.realized:+.2f}",
            symbol=pos.symbol, level="success")

    def _be_event(self, pos: Position) -> None:
        self.state.add_event(
            "breakeven", f"{pos.symbol}: стоп → безубыток",
            f"стоп переставлен на {fmt_price(pos.stop)} (вход {fmt_price(pos.entry)})",
            symbol=pos.symbol, level="success")

    def _finalize(self, pos: Position, reason: str) -> None:
        if pos.status != "open":
            return
        pos.status = "closed"
        pos.closed_at = now()
        pos.reason = reason
        self.state.record_trade(pos)
        self.state.positions.pop(pos.id, None)
        self.state.cooldowns[pos.symbol] = now() + float(self.tcfg["cooldown_sec"])
        names = {"tp_final": "лестница тейков выполнена", "stop": "стоп за стеной",
                 "breakeven": "безубыток", "wall_pull": "стену сняли (спуфинг)",
                 "manual": "закрыто вручную", "error": "ошибка исполнения"}
        lvl = "success" if pos.realized > 0 else "warn"
        self.state.add_event(
            "close", f"{pos.symbol}: позиция закрыта",
            f"{names.get(reason, reason)} | PnL ${pos.realized:+.2f} "
            f"({pos.realized / (pos.qty_initial * pos.entry) * 100:+.2f}% от номинала) "
            f"| вход {fmt_price(pos.entry)} → выход {fmt_price(pos.avg_exit)}",
            symbol=pos.symbol, level=lvl)

    # ================= ручное закрытие =================
    async def close_manual(self, pos_id: str) -> bool:
        pos = self.state.positions.get(pos_id)
        if not pos or pos.status != "open":
            return False
        if self.sim:
            self._close_all(pos, pos.last_price or pos.entry, "manual")
        else:
            await self.live_close(pos, "manual")
        return True

    # ================= баланс (live) =================
    async def _poll_balance(self) -> None:
        assert self.client is not None
        bals = await self.client.balance()
        for b in bals:
            if b["asset"] == "USDT":
                self.state.live_balance = float(b["balance"])
                return

    async def _balance_loop(self) -> None:
        while not self._stopping:
            try:
                await self._poll_balance()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("balance: %s", e)
            await asyncio.sleep(60)
