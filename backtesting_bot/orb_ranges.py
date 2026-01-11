from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path

import pandas as pd

from backtesting_bot.constants import MARKET_TIMEZONE
from backtesting_bot.io import load_spy_1m_bars


def _parse_date(value: str) -> dt.date:
    return dt.datetime.strptime(value, "%Y-%m-%d").date()


def _date_range(start: dt.date, end: dt.date) -> list[dt.date]:
    days: list[dt.date] = []
    current = start
    while current <= end:
        days.append(current)
        current += dt.timedelta(days=1)
    return days


def _orb_window(
    session_df: pd.DataFrame, session_date: dt.date, orb_minutes: int
) -> tuple[float, float, pd.Timestamp, int] | None:
    start_ts = pd.Timestamp(
        dt.datetime.combine(session_date, dt.time(9, 30)),
        tz=MARKET_TIMEZONE,
    )
    end_ts = start_ts + pd.Timedelta(minutes=orb_minutes)
    window = session_df.loc[(session_df.index >= start_ts) & (session_df.index < end_ts)]
    if window.empty:
        return None
    return (
        float(window["high"].max()),
        float(window["low"].min()),
        window.index.max(),
        int(len(window)),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compute 15/30-min ORB ranges for each session."
    )
    parser.add_argument("--start", required=True, type=_parse_date)
    parser.add_argument("--end", required=True, type=_parse_date)
    parser.add_argument(
        "--cache-root",
        default="data_local/spy/1m",
        help="Root directory containing date=YYYY-MM-DD/data.parquet files.",
    )
    parser.add_argument(
        "--out",
        default="data_local/spy_orb_ranges.csv",
        help="Output CSV path.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    cache_root = Path(args.cache_root)
    try:
        df = load_spy_1m_bars(cache_root, args.start, args.end)
    except FileNotFoundError as exc:
        print(f"Error: {exc}")
        return 1

    if df.empty:
        print("No SPY 1m bars found for the requested range.")
        return 1

    df = df.sort_index()
    df.index = df.index.tz_convert(MARKET_TIMEZONE)

    rows: list[dict[str, object]] = []
    for session_date in _date_range(args.start, args.end):
        mask = df.index.date == session_date
        if not mask.any():
            rows.append(
                {
                    "date": session_date.isoformat(),
                    "bars": 0,
                    "orb_15_high": None,
                    "orb_15_low": None,
                    "orb_15_end_ts": None,
                    "orb_15_bars": 0,
                    "orb_30_high": None,
                    "orb_30_low": None,
                    "orb_30_end_ts": None,
                    "orb_30_bars": 0,
                    "status": "missing_bars",
                }
            )
            continue

        session_df = df.loc[mask]
        orb_15 = _orb_window(session_df, session_date, 15)
        orb_30 = _orb_window(session_df, session_date, 30)

        rows.append(
            {
                "date": session_date.isoformat(),
                "bars": int(len(session_df)),
                "orb_15_high": orb_15[0] if orb_15 else None,
                "orb_15_low": orb_15[1] if orb_15 else None,
                "orb_15_end_ts": orb_15[2].isoformat() if orb_15 else None,
                "orb_15_bars": orb_15[3] if orb_15 else 0,
                "orb_30_high": orb_30[0] if orb_30 else None,
                "orb_30_low": orb_30[1] if orb_30 else None,
                "orb_30_end_ts": orb_30[2].isoformat() if orb_30 else None,
                "orb_30_bars": orb_30[3] if orb_30 else 0,
                "status": "ok" if orb_15 and orb_30 else "missing_orb",
            }
        )

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(out_path, index=False)
    print(f"Wrote ORB ranges to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
