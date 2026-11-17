"""Тесты ключевой логики: индикаторы, стакан, кластеры, лестница тейков, сайзинг."""
import asyncio
import copy
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bot.config import DEFAULTS
from bot.models import KlineRing, SymbolMeta, Wall
from bot.orderbook import OrderBook
from bot.state import AppState
from bot.trading import Trading
from bot.utils import floor_step, natr_pct, now
from bot.walls import WallDetector


def make_cfg():
    cfg = copy.deepcopy(DEFAULTS)
    cfg["mode"] = "paper"
    return cfg


def make_state(cfg):
    st = AppState(cfg)
    st.data_source = "demo"          # чтобы тесты не писали историю на диск
    st.paper_equity = 1000.0
    st.paper_start = 1000.0
    st.trades.clear()
    st.execution_enabled = True
    st.autotrade = True
    return st


def make_book(sym, tick, bb, ba, n=5, qty=10.0):
    b = OrderBook(sym, tick)
    b.synced = True
    b.needs_resync = False
    for i in range(n):
        b.bids[b._key(bb - i * tick)] = qty
        b.asks[b._key(ba + i * tick)] = qty
    return b


def make_ring(vol15=100_000.0):
    r = KlineRing()
    t0 = int(now() // 60 - 20) * 60_000
    rows = [[t0 + i * 60_000, 100, 101, 99, 100, 10, 0, vol15 / 15] for i in range(20)]
    r.seed(rows)
    return r


def make_feed(sym="TESTUSDT", bb=99.1, ba=99.12, tick=0.01, vol15=100_000.0):
    meta = SymbolMeta(sym, tick, 0.001, 0.001, 5.0)
    return SimpleNamespace(
        books={sym: make_book(sym, tick, bb, ba)},
        rings={sym: make_ring(vol15)},
        meta={sym: meta},
        universe=[sym],
    )


# ---------------- NATR ----------------
def test_natr_known():
    # постоянный TR=2 при close=100 -> NATR=2%
    hlc = [(101.0, 99.0, 100.0)] * 20
    assert abs(natr_pct(hlc, 14) - 2.0) < 1e-9


def test_natr_insufficient():
    assert natr_pct([(101, 99, 100)] * 5, 14) is None


def test_rounding():
    assert floor_step(1.23456, 0.001) == 1.234
    assert floor_step(10.0, 1.0) == 10.0


# ---------------- стакан ----------------
def test_orderbook_sync_protocol():
    b = OrderBook("X", 0.5)
    b.on_event({"U": 98, "u": 101, "pu": 97, "b": [["100.0", "6"]], "a": []})
    b.on_event({"U": 102, "u": 103, "pu": 101,
                "b": [["100.0", "0"]], "a": [["101.0", "2"]]})
    snap = {"lastUpdateId": 100, "bids": [["100.0", "5"], ["99.5", "4"]],
            "asks": [["100.5", "3"]]}
    assert b.start_sync(snap) is True
    assert b.synced
    assert b.best_bid == 99.5          # уровень 100 удалён нулевым объёмом
    assert b.best_ask == 100.5
    assert b.asks[b._key(101.0)] == 2.0
    # нормальное продолжение
    b.on_event({"U": 104, "u": 105, "pu": 103, "b": [["99.0", "1"]], "a": []})
    assert b.synced
    # разрыв последовательности -> ресинк
    b.on_event({"U": 200, "u": 201, "pu": 999, "b": [], "a": []})
    assert not b.synced and b.needs_resync


def test_orderbook_gap_before_bridge():
    b = OrderBook("X", 0.5)
    b.on_event({"U": 150, "u": 151, "pu": 149, "b": [], "a": []})
    snap = {"lastUpdateId": 100, "bids": [["100.0", "5"]], "asks": [["100.5", "3"]]}
    assert b.start_sync(snap) is False   # первое событие не покрывает снапшот
    assert b.needs_resync


# ---------------- кластеры плотностей ----------------
def test_cluster_detection():
    cfg = make_cfg()
    st = make_state(cfg)
    det = WallDetector(cfg, SimpleNamespace(universe=[]), st, None)
    mid = 100.0
    levels = [(100.1 + i * 0.01, 1000.0) for i in range(30)]
    levels += [(100.5, 300_000.0), (100.51, 60_000.0)]
    levels.sort(key=lambda x: x[0])
    out = det._clusters("ask", levels, threshold=200_000.0, mid=mid)
    assert len(out) == 1
    side, near, far, vwap, total = out[0]
    assert side == "ask" and total >= 200_000.0
    assert near == 100.5 and far == 100.51
    assert 100.5 < vwap < 100.51


def test_cluster_rejects_uniform_book():
    cfg = make_cfg()
    st = make_state(cfg)
    det = WallDetector(cfg, SimpleNamespace(universe=[]), st, None)
    # равномерный стакан без аномалии — стен быть не должно
    levels = [(100.1 + i * 0.01, 30_000.0) for i in range(40)]
    out = det._clusters("ask", levels, threshold=200_000.0, mid=100.0)
    assert out == []


# ---------------- лестница тейков (paper) ----------------
def make_pos(feed, st, cfg, entry=100.0, qty=10.0, stop=98.9):
    tr = Trading(cfg, feed, st, None)
    from bot.models import Position
    pos = Position(symbol="TESTUSDT", side="long", mode="paper", entry=entry,
                   qty_initial=qty, stop=stop, wall_id="w1", wall_price=99.0)
    pos.ladder = [{"move": float(s["move_pct"]), "frac": float(s["close_frac"]),
                   "be": bool(s.get("breakeven", False)), "done": False,
                   "qty": 0.0, "price": 0.0} for s in cfg["trading"]["take_profit"]]
    st.positions[pos.id] = pos
    return tr, pos


def test_ladder_full_cycle():
    cfg = make_cfg()
    st = make_state(cfg)
    feed = make_feed(bb=100.0, ba=100.02)
    tr, pos = make_pos(feed, st, cfg)

    tr._manage_paper(pos, 100.5)                     # +0.5% — ничего
    assert pos.qty_remaining == 10.0 and not pos.be_moved

    tr._manage_paper(pos, 101.05)                    # +1.05% -> 50% + БУ
    assert pos.ladder[0]["done"]
    assert abs(pos.qty_remaining - 5.0) < 0.01
    assert pos.be_moved and pos.stop == pos.entry

    tr._manage_paper(pos, 102.1)                     # +2.1% -> ещё 25%
    assert pos.ladder[1]["done"]
    assert abs(pos.qty_remaining - 2.5) < 0.01

    tr._manage_paper(pos, 103.2)                     # +3.2% -> остаток
    assert pos.status == "closed" and pos.reason == "tp_final"
    assert pos.qty_remaining <= 1e-9
    assert pos.realized > 0
    assert st.trades and st.cooldowns.get("TESTUSDT", 0) > now()


def test_stop_behind_wall():
    cfg = make_cfg()
    st = make_state(cfg)
    feed = make_feed()
    tr, pos = make_pos(feed, st, cfg)
    tr._manage_paper(pos, 98.5)                      # ниже стопа 98.9
    assert pos.status == "closed" and pos.reason == "stop"
    assert pos.realized < 0


def test_breakeven_after_tp1():
    cfg = make_cfg()
    st = make_state(cfg)
    feed = make_feed()
    tr, pos = make_pos(feed, st, cfg)
    tr._manage_paper(pos, 101.1)                     # TP1 + БУ
    assert pos.be_moved
    tr._manage_paper(pos, 99.9)                      # возврат к входу
    assert pos.status == "closed" and pos.reason == "breakeven"
    assert pos.realized > 0                          # прибыль первой половины осталась


def test_short_ladder():
    cfg = make_cfg()
    st = make_state(cfg)
    feed = make_feed()
    tr, pos = make_pos(feed, st, cfg, entry=100.0, qty=10.0, stop=101.1)
    pos.side = "short"
    tr._manage_paper(pos, 98.9)                      # +1.1% для шорта
    assert pos.ladder[0]["done"] and pos.be_moved
    tr._manage_paper(pos, 96.8)                      # +3.2% -> всё закрыто
    assert pos.status == "closed" and pos.realized > 0


# ---------------- сайзинг и вход ----------------
def test_entry_sizing_and_stop():
    cfg = make_cfg()
    st = make_state(cfg)
    feed = make_feed(bb=99.1, ba=99.12, vol15=100_000.0)
    tr = Trading(cfg, feed, st, None)
    wall = Wall(symbol="TESTUSDT", side="bid", price=99.0, near=99.05, far=98.95,
                notional=500_000.0, ratio=5.0, vol15m=100_000.0)
    wall.first_seen = now() - 30
    asyncio.run(tr.try_entry(wall))
    positions = st.open_positions()
    assert len(positions) == 1
    p = positions[0]
    assert p.side == "long"
    assert p.stop < wall.far                          # стоп ЗА стеной
    risk = (p.entry - p.stop) * p.qty_initial
    assert risk <= 1000.0 * cfg["trading"]["risk_per_trade"] * 1.05
    assert p.qty_initial * p.entry <= 1000.0 * cfg["trading"]["max_leverage"] * 1.01


def test_entry_skips_too_far_stop():
    cfg = make_cfg()
    st = make_state(cfg)
    feed = make_feed(bb=99.1, ba=99.12)
    tr = Trading(cfg, feed, st, None)
    wall = Wall(symbol="TESTUSDT", side="bid", price=97.0, near=97.1, far=96.9,
                notional=500_000.0, ratio=5.0, vol15m=100_000.0)
    wall.first_seen = now() - 30
    asyncio.run(tr.try_entry(wall))
    assert st.open_positions() == []                  # стоп ~2.2% > max 1%


def test_one_position_per_symbol():
    cfg = make_cfg()
    st = make_state(cfg)
    feed = make_feed()
    tr = Trading(cfg, feed, st, None)
    w1 = Wall(symbol="TESTUSDT", side="bid", price=99.0, near=99.05, far=98.95,
              notional=500_000.0, ratio=5.0, vol15m=100_000.0)
    w1.first_seen = now() - 30
    w2 = Wall(symbol="TESTUSDT", side="ask", price=99.3, near=99.25, far=99.35,
              notional=500_000.0, ratio=5.0, vol15m=100_000.0)
    w2.first_seen = now() - 30
    asyncio.run(tr.try_entry(w1))
    asyncio.run(tr.try_entry(w2))
    assert len(st.open_positions()) == 1
