"""Massive (Polygon) market data provider."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Dict, Any

import pandas as pd
import requests

from src.config import MassiveConfig
from src.providers.base import MarketDataProvider


@dataclass
class MassiveMarketDataProvider(MarketDataProvider):
    config: MassiveConfig

    def ping(self) -> bool:
        url = f"{self.config.base_url}/v1/marketstatus/now"
        response = requests.get(url, params={"apiKey": self.config.api_key}, timeout=10)
        response.raise_for_status()
        return True

    def fetch_spy_1m(self, session_date: date) -> pd.DataFrame:
        url = (
            f"{self.config.base_url}/v2/aggs/ticker/SPY/range/1/minute/"
            f"{session_date}/{session_date}"
        )
        params = {
            "adjusted": "true",
            "sort": "asc",
            "limit": 50000,
            "apiKey": self.config.api_key,
        }
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        payload: Dict[str, Any] = response.json()
        results = payload.get("results", [])
        if not results:
            return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

        frame = pd.DataFrame(results)
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
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], unit="ms", utc=True)
        frame = frame[["timestamp", "open", "high", "low", "close", "volume"]]
        return frame
