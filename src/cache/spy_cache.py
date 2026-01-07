"""Local Parquet cache for SPY 1-minute bars."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable

import pandas as pd

from src.providers.base import MarketDataProvider


@dataclass
class Spy1mCache:
    root_dir: Path

    def _date_path(self, session_date: date) -> Path:
        return self.root_dir / "spy" / "1m" / f"date={session_date.isoformat()}" / "data.parquet"

    def has_date(self, session_date: date) -> bool:
        return self._date_path(session_date).exists()

    def write_date(self, session_date: date, frame: pd.DataFrame) -> Path:
        path = self._date_path(session_date)
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(path, index=False)
        return path

    def fetch_and_cache(self, provider: MarketDataProvider, session_date: date) -> Path:
        frame = provider.fetch_spy_1m(session_date)
        return self.write_date(session_date, frame)

    def cache_range(
        self, provider: MarketDataProvider, session_dates: Iterable[date]
    ) -> list[Path]:
        written: list[Path] = []
        for session_date in session_dates:
            if self.has_date(session_date):
                continue
            written.append(self.fetch_and_cache(provider, session_date))
        return written
