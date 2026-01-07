from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Optional

import pandas as pd

from backtesting_bot.constants import DEFAULT_ORB_CANDLES, MARKET_TIMEZONE


@dataclass(frozen=True)
class OrbRange:
    high: float
    low: float
    end_ts: pd.Timestamp


def resample_to_five_minutes(session_df: pd.DataFrame) -> pd.DataFrame:
    if session_df.empty:
        return session_df.copy()

    resampled = (
        session_df.resample(
            "5min",
            label="right",
            closed="left",
            origin="start_day",
            offset="30min",
        )
        .agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            volume=("volume", "sum"),
        )
        .dropna()
    )
    return resampled


def calculate_orb_range(
    five_min_df: pd.DataFrame, orb_candles: int = DEFAULT_ORB_CANDLES
) -> Optional[OrbRange]:
    if len(five_min_df) < orb_candles:
        return None

    orb_slice = five_min_df.iloc[:orb_candles]
    return OrbRange(
        high=float(orb_slice["high"].max()),
        low=float(orb_slice["low"].min()),
        end_ts=orb_slice.index[-1],
    )


def find_orb_entry(
    five_min_df: pd.DataFrame,
    orb_range: OrbRange,
    cutoff_time: dt.time | None = None,
) -> dict | None:
    breakout_df = five_min_df.loc[five_min_df.index > orb_range.end_ts]
    if cutoff_time is not None:
        cutoff = dt.datetime.combine(orb_range.end_ts.date(), cutoff_time)
        cutoff = pd.Timestamp(cutoff, tz=MARKET_TIMEZONE)
        breakout_df = breakout_df.loc[breakout_df.index <= cutoff]

    for ts, row in breakout_df.iterrows():
        close_price = row["close"]
        if close_price > orb_range.high:
            return {
                "entry_ts": ts,
                "direction": "CALL",
                "spy_price_at_entry": float(close_price),
                "context": {
                    "orb_high": orb_range.high,
                    "orb_low": orb_range.low,
                    "orb_end_ts": orb_range.end_ts.isoformat(),
                    "candle_close": float(close_price),
                },
            }
        if close_price < orb_range.low:
            return {
                "entry_ts": ts,
                "direction": "PUT",
                "spy_price_at_entry": float(close_price),
                "context": {
                    "orb_high": orb_range.high,
                    "orb_low": orb_range.low,
                    "orb_end_ts": orb_range.end_ts.isoformat(),
                    "candle_close": float(close_price),
                },
            }
    return None
