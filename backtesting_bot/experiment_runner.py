from __future__ import annotations

import datetime as dt
import json
import re
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from backtesting_bot.constants import DEFAULT_SPY_1M_PATH
from backtesting_bot.experiment_config import ExperimentConfig
from backtesting_bot.pass1 import Pass1Config, run_pass1_pipeline
from backtesting_bot.pass2 import Pass2Config, run_pass2_pipeline


@dataclass(frozen=True)
class ExperimentResult:
    experiment_id: str
    experiment_dir: Path
    metrics: dict
    trades: pd.DataFrame
    equity_curve: pd.DataFrame


def _slugify(value: str) -> str:
    value = value.lower().strip()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-") or "experiment"


def generate_experiment_id(test_name: str, now: dt.datetime | None = None) -> str:
    now = now or dt.datetime.utcnow()
    timestamp = now.strftime("%Y%m%d-%H%M%S")
    return f"{timestamp}-{_slugify(test_name)}"


def _experiment_root() -> Path:
    return Path("data_local") / "experiments"


def run_experiment(
    config: ExperimentConfig, experiment_id: str | None = None, spy_1m_path: str = DEFAULT_SPY_1M_PATH
) -> ExperimentResult:
    experiment_id = experiment_id or generate_experiment_id(config.test_name)
    experiment_dir = _experiment_root() / experiment_id
    config_snapshot = experiment_dir / "config_snapshot" / "experiment.yaml"
    config.to_yaml(config_snapshot)

    pass1_dir = experiment_dir / "pass1"
    pass1_config = Pass1Config(
        start=config.start_date,
        end=config.end_date,
        strategy=config.strategy,
        run_id=experiment_id,
        spy_1m_path=spy_1m_path,
        max_trades_per_day=config.orb.max_trades_per_day,
        no_entries_after=config.orb.no_entries_after,
        orb_minutes=config.orb.orb_minutes,
        candle_interval_minutes=config.orb.candle_interval_minutes,
        breakout_basis=config.orb.breakout_basis,
        confirm_full_candle=config.orb.confirm_full_candle,
        output_dir=pass1_dir,
    )
    run_pass1_pipeline(pass1_config)

    entries_df = pd.read_parquet(pass1_dir / "entries.parquet")

    pass2_dir = experiment_dir / "pass2"
    pass2_config = Pass2Config(
        start=config.start_date,
        end=config.end_date,
        spy_1m_path=spy_1m_path,
        exit_params=config.exit,
        account_params=config.account,
        output_dir=pass2_dir,
    )
    run_pass2_pipeline(pass2_config, entries_df)

    return load_experiment_result(experiment_id)


def list_experiments() -> list[str]:
    root = _experiment_root()
    if not root.exists():
        return []
    return sorted([path.name for path in root.iterdir() if path.is_dir()])


def load_experiment_result(experiment_id: str) -> ExperimentResult:
    experiment_dir = _experiment_root() / experiment_id
    metrics_path = experiment_dir / "pass2" / "metrics.json"
    trades_path = experiment_dir / "pass2" / "trades.parquet"
    equity_path = experiment_dir / "pass2" / "equity_curve.parquet"

    metrics = {}
    if metrics_path.exists():
        metrics = json.loads(metrics_path.read_text())
    trades = pd.DataFrame()
    equity_curve = pd.DataFrame()
    if trades_path.exists():
        trades = pd.read_parquet(trades_path)
    if equity_path.exists():
        equity_curve = pd.read_parquet(equity_path)

    return ExperimentResult(
        experiment_id=experiment_id,
        experiment_dir=experiment_dir,
        metrics=metrics,
        trades=trades,
        equity_curve=equity_curve,
    )
