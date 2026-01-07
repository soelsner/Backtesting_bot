"""Provider abstractions."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
from typing import Optional

import pandas as pd


class MarketDataProvider(ABC):
    @abstractmethod
    def ping(self) -> bool:
        """Return True if provider is reachable."""

    @abstractmethod
    def fetch_spy_1m(self, session_date: date) -> pd.DataFrame:
        """Fetch SPY 1-minute bars for the given date."""


class Broker(ABC):
    @abstractmethod
    def ping(self) -> bool:
        """Return True if broker is reachable."""
