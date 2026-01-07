from __future__ import annotations

import datetime as dt
from pathlib import Path

import json

import pandas as pd

from backtesting_bot.constants import MARKET_TIMEZONE


def _ensure_datetime_index(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.index, pd.DatetimeIndex):
        df = df.copy()
        df.index.name = df.index.name or "timestamp"
        return df

    for column in ("timestamp", "ts", "datetime"):
        if column in df.columns:
            df = df.set_index(column)
            return df

    raise ValueError("SPY parquet must contain a datetime index or timestamp column")


def _normalize_timezone(index: pd.DatetimeIndex) -> pd.DatetimeIndex:
    if index.tz is None:
        return index.tz_localize("UTC")
    return index.tz_convert("UTC")


def load_spy_1m_bars(path: str | Path, start: dt.date, end: dt.date) -> pd.DataFrame:
    df = pd.read_parquet(path)
    df = _ensure_datetime_index(df)
    df.index = _normalize_timezone(df.index)
    df = df.sort_index()

    et_index = df.index.tz_convert(MARKET_TIMEZONE)
    trade_date = et_index.date
    mask = (trade_date >= start) & (trade_date <= end)
    return df.loc[mask].copy()


def save_parquet(df: pd.DataFrame, path: str | Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def save_json(data: dict, path: str | Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True))
