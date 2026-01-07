from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Iterable

import requests


@dataclass(frozen=True)
class MassiveConfig:
    base_url: str = "https://api.polygon.io"
    api_key: str | None = None


class MassiveProvider:
    def __init__(self, config: MassiveConfig) -> None:
        self._config = config

    def list_option_contracts(
        self,
        underlying: str,
        expiration_date: date,
        right: str,
    ) -> list[dict]:
        params = {
            "underlying_ticker": underlying,
            "expiration_date": expiration_date.isoformat(),
            "contract_type": "call" if right.upper() == "C" else "put",
            "limit": 1000,
            "apiKey": self._config.api_key,
        }
        url = f"{self._config.base_url}/v3/reference/options/contracts"
        results: list[dict] = []
        while True:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            payload = response.json()
            results.extend(payload.get("results", []))
            next_url = payload.get("next_url")
            if not next_url:
                break
            url = next_url
            params = {"apiKey": self._config.api_key}
        return results

    def fetch_option_aggregates_1s(
        self,
        contract_ticker: str,
        trade_date: date,
    ) -> list[dict]:
        url = (
            f"{self._config.base_url}/v2/aggs/ticker/{contract_ticker}"
            f"/range/1/second/{trade_date.isoformat()}/{trade_date.isoformat()}"
        )
        params = {
            "adjusted": "true",
            "sort": "asc",
            "limit": 50000,
            "apiKey": self._config.api_key,
        }
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()
        return payload.get("results", [])


def iter_contract_strikes(contracts: Iterable[dict]) -> list[float]:
    return [float(contract["strike_price"]) for contract in contracts]
