"""Загрузка конфигурации с дефолтами и переменными окружения."""
from __future__ import annotations

import copy
import os
from pathlib import Path

import yaml

DEFAULTS = {
    "mode": "paper",
    "data_source": "auto",
    "api": {"key": "", "secret": "", "recv_window": 10000},
    "scan": {"min_volume_24h_usd": 1_000_000, "natr_period": 14, "natr_max_pct": 1.0,
             "kline_interval": "5m", "refresh_sec": 300, "max_symbols": 50},
    "book": {"depth_limit": 500, "depth_speed": "250ms"},
    "walls": {"volume_window_min": 15, "ratio": 2.0, "min_wall_usd": 20_000,
              "max_distance_pct": 2.0, "cluster_gap_pct": 0.05,
              "max_cluster_width_pct": 0.35, "min_seed_frac": 0.35,
              "anomaly_mult": 8, "pull_grace_sec": 3.0, "min_age_sec": 2.0,
              "match_tolerance_pct": 0.15},
    "signal": {"approach_pct": 0.10},
    "trading": {"autotrade": True, "risk_per_trade": 0.01, "max_leverage": 5,
                "margin_type": "ISOLATED", "max_open_positions": 3,
                "cooldown_sec": 900, "stop_buffer_pct": 0.10, "max_stop_pct": 1.0,
                "fee_rate": 0.0005, "slippage_pct": 0.02, "exit_on_wall_pull": True,
                "paper_equity": 1000,
                "take_profit": [
                    {"move_pct": 1.0, "close_frac": 0.50, "breakeven": True},
                    {"move_pct": 2.0, "close_frac": 0.25},
                    {"move_pct": 3.0, "close_frac": 1.00},
                ],
                "breakeven_offset_pct": 0.0},
    "telegram": {"enabled": True, "token": "", "chat_id": "",
                 "events": ["approach", "entry", "tp", "breakeven", "stop",
                            "close", "pull", "error"]},
    "server": {"host": "0.0.0.0", "port": 8000},
}


def _merge(base: dict, override: dict) -> dict:
    out = copy.deepcopy(base)
    for k, v in (override or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _merge(out[k], v)
        else:
            out[k] = v
    return out


def load_config(path: str | Path) -> dict:
    p = Path(path)
    user = yaml.safe_load(p.read_text(encoding="utf-8")) if p.exists() else {}
    cfg = _merge(DEFAULTS, user or {})
    if cfg["mode"] not in ("paper", "testnet", "live"):
        raise ValueError(f"неверный mode: {cfg['mode']}")
    if cfg["data_source"] not in ("auto", "live", "demo"):
        raise ValueError(f"неверный data_source: {cfg['data_source']}")
    # env имеет приоритет над файлом
    cfg["api"]["key"] = os.environ.get("BINANCE_API_KEY") or cfg["api"]["key"]
    cfg["api"]["secret"] = os.environ.get("BINANCE_API_SECRET") or cfg["api"]["secret"]
    ladder = cfg["trading"]["take_profit"]
    if not ladder or ladder[-1]["move_pct"] < ladder[0]["move_pct"]:
        raise ValueError("take_profit должен быть возрастающей лестницей")
    return cfg
