"""Alpaca broker and market data providers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from typing import Any, Dict, List

import pandas as pd
import requests

from src.config import AlpacaConfig
from src.providers.base import Broker, MarketDataProvider


@dataclass
class AlpacaBroker(Broker):
    config: AlpacaConfig

    def ping(self) -> bool:
        url = f"{self.config.base_url}/v2/account"
        headers = {
            "APCA-API-KEY-ID": self.config.api_key,
            "APCA-API-SECRET-KEY": self.config.secret_key,
        }
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return True


@dataclass
class AlpacaMarketDataProvider(MarketDataProvider):
    config: AlpacaConfig

    def _headers(self) -> Dict[str, str]:
        return {
            "APCA-API-KEY-ID": self.config.api_key,
            "APCA-API-SECRET-KEY": self.config.secret_key,
        }

    def ping(self) -> bool:
        url = f"{self.config.data_base_url}/v2/stocks/SPY/bars"
        response = requests.get(
            url,
            headers=self._headers(),
            params={"timeframe": "1Min", "limit": 1},
            timeout=10,
        )
        response.raise_for_status()
        return True

    def fetch_spy_1m(self, session_date: date) -> pd.DataFrame:
        url = f"{self.config.data_base_url}/v2/stocks/SPY/bars"
        start_dt = datetime.combine(session_date, time(0, 0), tzinfo=timezone.utc)
        end_dt = datetime.combine(session_date, time(23, 59, 59), tzinfo=timezone.utc)
        params = {
            "timeframe": "1Min",
            "start": start_dt.isoformat(),
            "end": end_dt.isoformat(),
            "adjustment": "all",
            "limit": 10000,
        }
        response = requests.get(url, headers=self._headers(), params=params, timeout=30)
        response.raise_for_status()
        payload: Dict[str, Any] = response.json()
        bars: List[Dict[str, Any]] = payload.get("bars", [])
        if not bars:
            return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

        frame = pd.DataFrame(bars)
        frame = frame.rename(
            columns={
                "t": "timestamp",
                "o": "open",
                "h": "high",
                "l": "low",
                "c": "close",
                "v": "volume",
            }
        )
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True).dt.tz_convert("America/New_York")
        market_open = time(9, 30)
        market_close = time(16, 0)
        times = frame["timestamp"].dt.time
        frame = frame[(times >= market_open) & (times <= market_close)]
        frame = frame[["timestamp", "open", "high", "low", "close", "volume"]]
        return frame
