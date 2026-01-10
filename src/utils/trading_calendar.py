from __future__ import annotations

from datetime import date, timedelta


def is_trading_day(day: date) -> bool:
    return day.weekday() < 5


def next_trading_day(start: date, days_ahead: int = 1) -> date:
    if days_ahead < 1:
        raise ValueError("days_ahead must be >= 1")

    current = start
    remaining = days_ahead
    while remaining > 0:
        current += timedelta(days=1)
        if is_trading_day(current):
            remaining -= 1
    return current


def trading_days_ahead(start: date, count: int) -> list[date]:
    return [next_trading_day(start, days_ahead=i) for i in range(1, count + 1)]
