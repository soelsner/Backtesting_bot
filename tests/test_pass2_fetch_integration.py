from datetime import date, datetime
from pathlib import Path

import pandas as pd

from src.cli import run_pass2_fetch
from src.options.contract_selector import ContractSelectionConfig, ContractSelector


class FakeProvider:
    def list_option_contracts(self, underlying, expiration_date, right):
        return [{"strike_price": 452, "ticker": "O:SPY240103C00452000"}]

    def fetch_option_aggregates_1s(self, contract_ticker, trade_date):
        return [
            {"t": 1704294000000, "o": 1.0, "h": 1.1, "l": 0.9, "c": 1.05, "v": 10},
            {"t": 1704294001000, "o": 1.05, "h": 1.2, "l": 1.0, "c": 1.1, "v": 12},
        ]


class Args:
    def __init__(self, run_id: str, data_root: Path):
        self.run_id = run_id
        self.data_root = str(data_root)
        self.limit = None
        self.command = "pass2-fetch"


def test_pass2_fetch_creates_contracts_and_cache(tmp_path: Path) -> None:
    run_id = "test-run"
    run_dir = tmp_path / "runs" / run_id
    run_dir.mkdir(parents=True)

    entries = pd.DataFrame(
        [
            {
                "entry_ts": datetime(2024, 1, 2, 10, 0, 0),
                "direction": "CALL",
                "spy_price_at_entry": 450.0,
                "trade_date": date(2024, 1, 2),
            }
        ]
    )
    entries.to_parquet(run_dir / "entries.parquet", index=False)

    provider = FakeProvider()
    selector = ContractSelector(provider, ContractSelectionConfig(prefer_otm=2))
    args = Args(run_id, tmp_path)

    run_pass2_fetch(args, provider=provider, selector=selector)

    contracts_path = run_dir / "contracts.parquet"
    assert contracts_path.exists()

    options_path = (
        tmp_path
        / "options"
        / "1s"
        / "date=2024-01-02"
        / "ticker=O_SPY240103C00452000.parquet"
    )
    assert options_path.exists()
