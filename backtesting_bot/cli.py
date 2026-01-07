from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path

from backtesting_bot.pass1 import Pass1Config, run_pass1_pipeline


def _parse_date(value: str) -> dt.date:
    return dt.datetime.strptime(value, "%Y-%m-%d").date()


def _parse_time(value: str) -> dt.time:
    return dt.datetime.strptime(value, "%H:%M").time()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backtesting bot CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    pass1_parser = subparsers.add_parser("pass1", help="Run Pass 1 signal generation")
    pass1_parser.add_argument("--start", required=True, type=_parse_date)
    pass1_parser.add_argument("--end", required=True, type=_parse_date)
    pass1_parser.add_argument("--strategy", required=True)
    pass1_parser.add_argument("--run-id", required=True)
    pass1_parser.add_argument(
        "--spy-1m-path", default="data_local/spy_1m.parquet"
    )
    pass1_parser.add_argument(
        "--max-trades-per-day", type=int, default=1, dest="max_trades_per_day"
    )
    pass1_parser.add_argument(
        "--no-entries-after",
        type=_parse_time,
        dest="no_entries_after",
        help="Cutoff time in ET (HH:MM)",
    )

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "pass1":
        config = Pass1Config(
            start=args.start,
            end=args.end,
            strategy=args.strategy,
            run_id=args.run_id,
            spy_1m_path=args.spy_1m_path,
            max_trades_per_day=args.max_trades_per_day,
            no_entries_after=args.no_entries_after,
        )
        run_dir = run_pass1_pipeline(config)
        print(f"Pass 1 run complete. Outputs saved to {Path(run_dir)}")


if __name__ == "__main__":
    main()
