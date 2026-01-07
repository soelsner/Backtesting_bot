"""Command line interface for the backtesting framework."""

from __future__ import annotations

import argparse
import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable

from src.cache.spy_cache import Spy1mCache
from src.config import ConfigError, load_config
from src.providers.alpaca import AlpacaBroker
from src.providers.massive import MassiveMarketDataProvider


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _date_range(start: date, end: date) -> Iterable[date]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def health_check(args: argparse.Namespace) -> int:
    try:
        config = load_config(Path(args.config) if args.config else None)
    except ConfigError as exc:
        logging.error("Config error: %s", exc)
        return 1

    massive = MassiveMarketDataProvider(config.massive)
    alpaca = AlpacaBroker(config.alpaca)

    if args.skip_ping:
        logging.info("Config validated. Skipping provider pings.")
        return 0

    try:
        massive.ping()
        logging.info("Massive ping OK.")
    except Exception as exc:  # noqa: BLE001 - surface provider failures
        logging.error("Massive ping failed: %s", exc)
        return 1

    try:
        alpaca.ping()
        logging.info("Alpaca ping OK.")
    except Exception as exc:  # noqa: BLE001 - surface provider failures
        logging.error("Alpaca ping failed: %s", exc)
        return 1

    return 0


def fetch_spy(args: argparse.Namespace) -> int:
    try:
        config = load_config(Path(args.config) if args.config else None)
    except ConfigError as exc:
        logging.error("Config error: %s", exc)
        return 1

    start = _parse_date(args.start)
    end = _parse_date(args.end)
    if end < start:
        logging.error("End date must be on or after start date.")
        return 1

    provider = MassiveMarketDataProvider(config.massive)
    cache = Spy1mCache(config.local.data_dir)

    cached = 0
    fetched = 0
    for session_date in _date_range(start, end):
        if cache.has_date(session_date):
            cached += 1
            logging.info("Cache hit for %s", session_date)
            continue
        try:
            path = cache.fetch_and_cache(provider, session_date)
        except Exception as exc:  # noqa: BLE001 - surface provider failures
            logging.error("Failed to fetch %s: %s", session_date, exc)
            return 1
        fetched += 1
        logging.info("Cached %s -> %s", session_date, path)

    logging.info("Done. Cached=%s Skipped=%s", fetched, cached)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="SPY options backtesting CLI")
    parser.add_argument("--config", help="Path to YAML config file", default=None)
    parser.add_argument("--log-level", default="INFO", help="Logging level")

    subparsers = parser.add_subparsers(dest="command", required=True)

    health_parser = subparsers.add_parser("health-check", help="Validate config and ping providers")
    health_parser.add_argument("--skip-ping", action="store_true", help="Only validate config")
    health_parser.set_defaults(func=health_check)

    fetch_parser = subparsers.add_parser("fetch-spy", help="Fetch SPY 1-minute bars")
    fetch_parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    fetch_parser.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    fetch_parser.set_defaults(func=fetch_spy)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
