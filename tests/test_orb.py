import datetime as dt

import pandas as pd

from backtesting_bot.strategies.orb import (
    calculate_orb_range,
    find_orb_entry,
    resample_to_five_minutes,
)


def _make_minute_bars(start: dt.datetime, minutes: int, base_price: float) -> pd.DataFrame:
    index = pd.date_range(start=start, periods=minutes, freq="1min", tz=start.tzinfo)
    values = [base_price + i * 0.1 for i in range(minutes)]
    data = {
        "open": values,
        "high": [v + 0.2 for v in values],
        "low": [v - 0.2 for v in values],
        "close": values,
        "volume": [100] * minutes,
    }
    return pd.DataFrame(data, index=index)


def test_orb_range_calculation():
    start = dt.datetime(2025, 1, 2, 9, 30, tzinfo=dt.timezone(dt.timedelta(hours=-5)))
    df = _make_minute_bars(start, minutes=30, base_price=100)

    five_min = resample_to_five_minutes(df)
    orb_range = calculate_orb_range(five_min)

    assert orb_range is not None
    assert round(orb_range.high, 2) == 101.6
    assert round(orb_range.low, 2) == 99.8


def test_orb_breakout_detection_call():
    start = dt.datetime(2025, 1, 2, 9, 30, tzinfo=dt.timezone(dt.timedelta(hours=-5)))
    df = _make_minute_bars(start, minutes=60, base_price=100)

    df.loc[df.index >= df.index[15], "close"] = 103
    df.loc[df.index >= df.index[15], "high"] = 103.2
    df.loc[df.index >= df.index[15], "low"] = 102.8

    five_min = resample_to_five_minutes(df)
    orb_range = calculate_orb_range(five_min)
    assert orb_range is not None

    entry = find_orb_entry(five_min, orb_range)
    assert entry is not None
    assert entry["direction"] == "CALL"
    assert entry["spy_price_at_entry"] == 103
