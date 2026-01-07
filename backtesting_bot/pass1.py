from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd

from backtesting_bot.constants import (
    DEFAULT_MAX_TRADES_PER_DAY,
    DEFAULT_SPY_1M_PATH,
    MARKET_TIMEZONE,
    SESSION_END,
    SESSION_START,
)
from backtesting_bot.indicators import ema, rsi
from backtesting_bot.io import load_spy_1m_bars, save_json, save_parquet
from backtesting_bot.strategies.orb import (
    calculate_orb_range,
    find_orb_entry,
    resample_to_five_minutes,
)


@dataclass(frozen=True)
class Pass1Config:
    start: dt.date
    end: dt.date
    strategy: str
    run_id: str
    spy_1m_path: str = DEFAULT_SPY_1M_PATH
    max_trades_per_day: int = DEFAULT_MAX_TRADES_PER_DAY
    no_entries_after: dt.time | None = None


@dataclass(frozen=True)
class EntrySignal:
    trade_date: dt.date
    entry_ts: pd.Timestamp
    direction: str
    spy_price_at_entry: float
    strategy_name: str
    context: dict


def _session_filter(df: pd.DataFrame) -> pd.DataFrame:
    et_index = df.index.tz_convert(MARKET_TIMEZONE)
    df = df.copy()
    df["_et_ts"] = et_index
    df = df.set_index("_et_ts")
    df = df.between_time(SESSION_START, SESSION_END, inclusive="both")
    return df


def _iterate_trade_dates(df: pd.DataFrame) -> Iterable[dt.date]:
    et_index = df.index.tz_convert(MARKET_TIMEZONE)
    return pd.Index(et_index.date).unique().tolist()


def _prepare_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ema_fast"] = ema(df["close"], 8)
    df["ema_slow"] = ema(df["close"], 21)
    df["rsi_14"] = rsi(df["close"], 14)
    return df


def generate_orb_entries(
    df: pd.DataFrame, config: Pass1Config
) -> list[EntrySignal]:
    entries: list[EntrySignal] = []
    for trade_date in _iterate_trade_dates(df):
        day_mask = df.index.tz_convert(MARKET_TIMEZONE).date == trade_date
        day_df = df.loc[day_mask]
        if day_df.empty:
            continue

        session_df = _session_filter(day_df)
        if session_df.empty:
            continue

        five_min_df = resample_to_five_minutes(session_df)
        orb_range = calculate_orb_range(five_min_df)
        if orb_range is None:
            continue

        entry = find_orb_entry(
            five_min_df, orb_range, cutoff_time=config.no_entries_after
        )
        if entry is None:
            continue

        entry_ts_utc = entry["entry_ts"].tz_convert("UTC")
        entries.append(
            EntrySignal(
                trade_date=trade_date,
                entry_ts=entry_ts_utc,
                direction=entry["direction"],
                spy_price_at_entry=entry["spy_price_at_entry"],
                strategy_name=config.strategy,
                context=entry["context"],
            )
        )

    return entries


def _build_run_metadata(entries_df: pd.DataFrame, dates: list[dt.date]) -> dict:
    counts = {date.isoformat(): 0 for date in dates}
    if not entries_df.empty:
        for trade_date, group_df in entries_df.groupby("trade_date"):
            counts[str(trade_date)] = len(group_df)
    no_trades = [date for date, count in counts.items() if count == 0]
    return {
        "counts_per_day": counts,
        "days_with_no_trades": no_trades,
        "total_entries": int(entries_df.shape[0]),
    }


def write_run_outputs(
    entries_df: pd.DataFrame, config: Pass1Config, dates: list[dt.date]
) -> Path:
    run_dir = Path("data_local") / "runs" / config.run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    save_parquet(entries_df, run_dir / "entries.parquet")
    save_json(_build_run_metadata(entries_df, dates), run_dir / "run_metadata.json")
    save_json(
        {
            "start": config.start.isoformat(),
            "end": config.end.isoformat(),
            "strategy": config.strategy,
            "run_id": config.run_id,
            "spy_1m_path": config.spy_1m_path,
            "timezone": "UTC",
            "no_entries_after": config.no_entries_after.isoformat()
            if config.no_entries_after
            else None,
            "max_trades_per_day": config.max_trades_per_day,
        },
        run_dir / "config_snapshot.json",
    )
    return run_dir


def run_pass1_pipeline(config: Pass1Config) -> Path:
    df = load_spy_1m_bars(config.spy_1m_path, config.start, config.end)
    df = _prepare_indicators(df)

    dates = list(pd.Index(df.index.tz_convert(MARKET_TIMEZONE).date).unique())

    if config.strategy == "orb_v1":
        entries = generate_orb_entries(df, config)
    elif config.strategy in {"ema_v1", "rsi_v1"}:
        entries = []
    else:
        raise ValueError(f"Unsupported strategy: {config.strategy}")

    entries_df = pd.DataFrame(
        [
            {
                "trade_date": entry.trade_date.isoformat(),
                "entry_ts": entry.entry_ts,
                "direction": entry.direction,
                "spy_price_at_entry": entry.spy_price_at_entry,
                "strategy_name": entry.strategy_name,
                "context": json.dumps(entry.context, sort_keys=True),
            }
            for entry in entries
        ]
    )

    if not entries_df.empty:
        entries_df = entries_df.sort_values(["entry_ts", "strategy_name"]).reset_index(
            drop=True
        )

    write_run_outputs(entries_df, config, dates)
    return Path("data_local") / "runs" / config.run_id
