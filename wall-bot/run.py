#!/usr/bin/env python3
"""Wall Bot — запуск: python run.py [--config config.yaml] [--port 8000] [--demo]"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

import uvicorn

from bot.config import load_config
from bot.server import create_app


def main() -> None:
    parser = argparse.ArgumentParser(description="Wall Bot — плотности стакана Binance Futures")
    parser.add_argument("--config", default=str(Path(__file__).parent / "config.yaml"))
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--mode", choices=["paper", "testnet", "live"], default=None)
    parser.add_argument("--demo", action="store_true", help="синтетические данные")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)

    cfg = load_config(args.config)
    if args.port:
        cfg["server"]["port"] = args.port
    if args.mode:
        cfg["mode"] = args.mode
    if args.demo:
        cfg["data_source"] = "demo"

    app = create_app(cfg)
    print(f"\n  Wall Bot → http://localhost:{cfg['server']['port']}  "
          f"(режим: {cfg['mode']}, данные: {cfg['data_source']})\n")
    uvicorn.run(app, host=cfg["server"]["host"], port=cfg["server"]["port"],
                log_level="warning")


if __name__ == "__main__":
    main()
