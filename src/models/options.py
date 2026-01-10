from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class ContractSpec:
    underlying: str
    expiration_date: date
    right: str
    strike: float
    provider_ticker: str
