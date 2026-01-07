from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Iterable

import yaml


@dataclass(frozen=True)
class OrbParams:
    orb_minutes: int
    candle_interval_minutes: int
    breakout_basis: str
    confirm_full_candle: bool
    max_trades_per_day: int
    no_entries_after: dt.time | None


@dataclass(frozen=True)
class ExitParams:
    stop_loss_pct: float
    take_profit_mode: str
    take_profit_pct: float
    trailing_enabled: bool
    trail_pct: float
    partial_tp_enabled: bool
    split_pct: float
    first_tp_pct: float
    runner_trail_pct: float
    both_hit_same_second: str


@dataclass(frozen=True)
class ContractSelectionParams:
    dte_choices: list[int]
    otm_dollars: list[int]
    prefer_otm: bool


@dataclass(frozen=True)
class AccountParams:
    starting_cash: float
    allocation_pct_per_trade: float
    max_daily_loss_pct: float | None


@dataclass(frozen=True)
class ExperimentConfig:
    test_name: str
    start_date: dt.date
    end_date: dt.date
    strategy: str
    orb: OrbParams
    exit: ExitParams
    contract: ContractSelectionParams
    account: AccountParams

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["start_date"] = self.start_date.isoformat()
        payload["end_date"] = self.end_date.isoformat()
        payload["orb"]["no_entries_after"] = (
            self.orb.no_entries_after.isoformat() if self.orb.no_entries_after else None
        )
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "ExperimentConfig":
        start_date = dt.date.fromisoformat(payload["start_date"])
        end_date = dt.date.fromisoformat(payload["end_date"])
        orb_payload = payload["orb"]
        orb = OrbParams(
            orb_minutes=int(orb_payload["orb_minutes"]),
            candle_interval_minutes=int(orb_payload["candle_interval_minutes"]),
            breakout_basis=str(orb_payload["breakout_basis"]),
            confirm_full_candle=bool(orb_payload["confirm_full_candle"]),
            max_trades_per_day=int(orb_payload["max_trades_per_day"]),
            no_entries_after=(
                dt.time.fromisoformat(orb_payload["no_entries_after"])
                if orb_payload.get("no_entries_after")
                else None
            ),
        )
        exit_payload = payload["exit"]
        exit_params = ExitParams(
            stop_loss_pct=float(exit_payload["stop_loss_pct"]),
            take_profit_mode=str(exit_payload["take_profit_mode"]),
            take_profit_pct=float(exit_payload["take_profit_pct"]),
            trailing_enabled=bool(exit_payload["trailing_enabled"]),
            trail_pct=float(exit_payload["trail_pct"]),
            partial_tp_enabled=bool(exit_payload["partial_tp_enabled"]),
            split_pct=float(exit_payload["split_pct"]),
            first_tp_pct=float(exit_payload["first_tp_pct"]),
            runner_trail_pct=float(exit_payload["runner_trail_pct"]),
            both_hit_same_second=str(exit_payload["both_hit_same_second"]),
        )
        contract_payload = payload["contract"]
        contract = ContractSelectionParams(
            dte_choices=[int(value) for value in contract_payload["dte_choices"]],
            otm_dollars=[int(value) for value in contract_payload["otm_dollars"]],
            prefer_otm=bool(contract_payload["prefer_otm"]),
        )
        account_payload = payload["account"]
        account = AccountParams(
            starting_cash=float(account_payload["starting_cash"]),
            allocation_pct_per_trade=float(account_payload["allocation_pct_per_trade"]),
            max_daily_loss_pct=(
                float(account_payload["max_daily_loss_pct"])
                if account_payload.get("max_daily_loss_pct") is not None
                else None
            ),
        )
        return cls(
            test_name=str(payload["test_name"]),
            start_date=start_date,
            end_date=end_date,
            strategy=str(payload["strategy"]),
            orb=orb,
            exit=exit_params,
            contract=contract,
            account=account,
        )

    def to_yaml(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handle:
            yaml.safe_dump(self.to_dict(), handle, sort_keys=False)

    @classmethod
    def from_yaml(cls, path: Path) -> "ExperimentConfig":
        with path.open("r", encoding="utf-8") as handle:
            payload = yaml.safe_load(handle) or {}
        return cls.from_dict(payload)


def normalize_experiment_ids(ids: Iterable[str]) -> list[str]:
    return sorted({value for value in ids if value})
