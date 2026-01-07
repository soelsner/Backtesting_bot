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


def resample_bars(session_df: pd.DataFrame, interval_minutes: int) -> pd.DataFrame:
    if session_df.empty:
        return session_df.copy()

    resampled = (
        session_df.resample(
            f"{interval_minutes}min",
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


def resample_to_five_minutes(session_df: pd.DataFrame) -> pd.DataFrame:
    return resample_bars(session_df, interval_minutes=5)


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
    breakout_basis: str = "close",
    confirm_full_candle: bool = False,
) -> dict | None:
    breakout_df = five_min_df.loc[five_min_df.index > orb_range.end_ts]
    if cutoff_time is not None:
        cutoff = dt.datetime.combine(orb_range.end_ts.date(), cutoff_time)
        cutoff = pd.Timestamp(cutoff, tz=MARKET_TIMEZONE)
        breakout_df = breakout_df.loc[breakout_df.index <= cutoff]

    for ts, row in breakout_df.iterrows():
        close_price = float(row["close"])
        open_price = float(row["open"])
        high_price = float(row["high"])
        low_price = float(row["low"])

        if breakout_basis == "wick":
            if high_price > orb_range.high:
                return {
                    "entry_ts": ts,
                    "direction": "CALL",
                    "spy_price_at_entry": high_price,
                    "context": {
                        "orb_high": orb_range.high,
                        "orb_low": orb_range.low,
                        "orb_end_ts": orb_range.end_ts.isoformat(),
                        "candle_high": high_price,
                        "candle_low": low_price,
                        "candle_close": close_price,
                        "breakout_basis": breakout_basis,
                    },
                }
            if low_price < orb_range.low:
                return {
                    "entry_ts": ts,
                    "direction": "PUT",
                    "spy_price_at_entry": low_price,
                    "context": {
                        "orb_high": orb_range.high,
                        "orb_low": orb_range.low,
                        "orb_end_ts": orb_range.end_ts.isoformat(),
                        "candle_high": high_price,
                        "candle_low": low_price,
                        "candle_close": close_price,
                        "breakout_basis": breakout_basis,
                    },
                }
            continue

        if close_price > orb_range.high:
            if confirm_full_candle and open_price <= orb_range.high:
                continue
            return {
                "entry_ts": ts,
                "direction": "CALL",
                "spy_price_at_entry": close_price,
                "context": {
                    "orb_high": orb_range.high,
                    "orb_low": orb_range.low,
                    "orb_end_ts": orb_range.end_ts.isoformat(),
                    "candle_open": open_price,
                    "candle_close": close_price,
                    "breakout_basis": breakout_basis,
                },
            }
        if close_price < orb_range.low:
            if confirm_full_candle and open_price >= orb_range.low:
                continue
            return {
                "entry_ts": ts,
                "direction": "PUT",
                "spy_price_at_entry": close_price,
                "context": {
                    "orb_high": orb_range.high,
                    "orb_low": orb_range.low,
                    "orb_end_ts": orb_range.end_ts.isoformat(),
                    "candle_open": open_price,
                    "candle_close": close_price,
                    "breakout_basis": breakout_basis,
                },
            }
    return None
