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


def _date_range(start: dt.date, end: dt.date) -> list[dt.date]:
    days: list[dt.date] = []
    current = start
    while current <= end:
        days.append(current)
        current += dt.timedelta(days=1)
    return days


def _load_spy_cache(path: Path, start: dt.date, end: dt.date) -> pd.DataFrame:
    base = path if path.is_dir() else path.parent
    if base.name == "1m" and base.parent.name == "spy":
        cache_root = base
    elif base.name == "spy":
        cache_root = base / "1m"
    else:
        cache_root = base / "spy" / "1m"

    frames: list[pd.DataFrame] = []
    for session_date in _date_range(start, end):
        date_path = cache_root / f"date={session_date.isoformat()}" / "data.parquet"
        if date_path.exists():
            frames.append(pd.read_parquet(date_path))

    if not frames:
        raise FileNotFoundError(
            f"No SPY cache files found under {cache_root} for {start} to {end}."
        )
    return pd.concat(frames, ignore_index=True)


def load_spy_1m_bars(path: str | Path, start: dt.date, end: dt.date) -> pd.DataFrame:
    path = Path(path)
    if path.exists() and path.is_file():
        df = pd.read_parquet(path)
    else:
        df = _load_spy_cache(path, start, end)
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
