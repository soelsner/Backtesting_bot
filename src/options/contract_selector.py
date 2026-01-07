from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

from src.models.options import ContractSpec
from src.models.signals import EntrySignal
from src.providers.massive import MassiveProvider
from src.utils.trading_calendar import next_trading_day


@dataclass(frozen=True)
class ContractSelectionConfig:
    dte_choices: list[int] = field(default_factory=lambda: [1, 2])
    otm_dollars: list[int] = field(default_factory=lambda: [1, 2, 3])
    prefer_otm: int = 2
    underlying: str = "SPY"


class ContractSelector:
    def __init__(self, provider: MassiveProvider, config: ContractSelectionConfig) -> None:
        self._provider = provider
        self._config = config

    def select_contract(self, signal: EntrySignal) -> ContractSpec:
        right = self._direction_to_right(signal.direction)
        spot = signal.spy_price_at_entry
        for dte in self._config.dte_choices:
            expiration = next_trading_day(signal.trade_date, days_ahead=dte)
            contracts = self._provider.list_option_contracts(
                underlying=self._config.underlying,
                expiration_date=expiration,
                right=right,
            )
            if not contracts:
                continue
            strike = self._select_strike(contracts, right, spot)
            contract = self._pick_contract_for_strike(contracts, strike)
            return ContractSpec(
                underlying=self._config.underlying,
                expiration_date=expiration,
                right=right,
                strike=float(contract["strike_price"]),
                provider_ticker=contract["ticker"],
            )
        raise ValueError("No option contract found for entry signal.")

    def _select_strike(self, contracts: list[dict], right: str, spot: float) -> float:
        offsets = [
            self._config.prefer_otm,
            *[offset for offset in self._config.otm_dollars if offset != self._config.prefer_otm],
        ]
        for offset in offsets:
            desired = spot + offset if right == "C" else spot - offset
            strike = self._find_strike_in_direction(contracts, desired, right)
            if strike is not None:
                return strike
        fallback = self._find_strike_in_direction(contracts, spot, right)
        if fallback is None:
            raise ValueError("No strikes available in desired direction.")
        return fallback

    def _find_strike_in_direction(
        self,
        contracts: list[dict],
        desired: float,
        right: str,
    ) -> float | None:
        strikes = sorted({float(contract["strike_price"]) for contract in contracts})
        if right == "C":
            candidates = [strike for strike in strikes if strike >= desired]
            return candidates[0] if candidates else None
        candidates = [strike for strike in strikes if strike <= desired]
        return candidates[-1] if candidates else None

    @staticmethod
    def _pick_contract_for_strike(contracts: list[dict], strike: float) -> dict:
        for contract in contracts:
            if float(contract["strike_price"]) == float(strike):
                return contract
        raise ValueError("Selected strike not found in contract list.")

    @staticmethod
    def _direction_to_right(direction: str) -> str:
        normalized = direction.upper()
        if normalized in {"CALL", "C", "BULL", "LONG"}:
            return "C"
        if normalized in {"PUT", "P", "BEAR", "SHORT"}:
            return "P"
        raise ValueError(f"Unsupported direction: {direction}")
