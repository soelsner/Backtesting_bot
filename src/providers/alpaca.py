"""Alpaca broker provider."""

from __future__ import annotations

from dataclasses import dataclass

import requests

from src.config import AlpacaConfig
from src.providers.base import Broker


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
