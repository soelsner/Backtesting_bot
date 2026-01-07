from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime


@dataclass(frozen=True)
class EntrySignal:
    entry_ts: datetime
    direction: str
    spy_price_at_entry: float
    trade_date: date

    @classmethod
    def from_record(cls, record: dict) -> "EntrySignal":
        entry_ts = _coerce_datetime(record["entry_ts"])
        trade_date = _coerce_date(record.get("trade_date") or entry_ts.date())
        return cls(
            entry_ts=entry_ts,
            direction=str(record["direction"]).upper(),
            spy_price_at_entry=float(record["spy_price_at_entry"]),
            trade_date=trade_date,
        )


def _coerce_datetime(value) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))


def _coerce_date(value) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    return date.fromisoformat(str(value))
