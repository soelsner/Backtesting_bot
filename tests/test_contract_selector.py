from datetime import date, datetime

from src.models.signals import EntrySignal
from src.options.contract_selector import ContractSelectionConfig, ContractSelector


class FakeProvider:
    def __init__(self, contracts):
        self._contracts = contracts

    def list_option_contracts(self, underlying, expiration_date, right):
        return self._contracts


def _make_contract(strike, ticker):
    return {"strike_price": strike, "ticker": ticker}


def test_contract_selector_call_prefers_otm() -> None:
    provider = FakeProvider(
        contracts=[
            _make_contract(451, "CALL451"),
            _make_contract(452, "CALL452"),
            _make_contract(453, "CALL453"),
        ]
    )
    selector = ContractSelector(provider, ContractSelectionConfig(prefer_otm=2))
    signal = EntrySignal(
        entry_ts=datetime(2024, 1, 2, 10, 0, 0),
        direction="CALL",
        spy_price_at_entry=450.0,
        trade_date=date(2024, 1, 2),
    )

    spec = selector.select_contract(signal)

    assert spec.right == "C"
    assert spec.strike == 452


def test_contract_selector_put_prefers_otm() -> None:
    provider = FakeProvider(
        contracts=[
            _make_contract(447, "PUT447"),
            _make_contract(448, "PUT448"),
            _make_contract(449, "PUT449"),
        ]
    )
    selector = ContractSelector(provider, ContractSelectionConfig(prefer_otm=2))
    signal = EntrySignal(
        entry_ts=datetime(2024, 1, 2, 10, 0, 0),
        direction="PUT",
        spy_price_at_entry=450.0,
        trade_date=date(2024, 1, 2),
    )

    spec = selector.select_contract(signal)

    assert spec.right == "P"
    assert spec.strike == 448


def test_contract_selector_fallback_when_missing_strike() -> None:
    provider = FakeProvider(
        contracts=[
            _make_contract(455, "CALL455"),
            _make_contract(456, "CALL456"),
        ]
    )
    selector = ContractSelector(provider, ContractSelectionConfig(prefer_otm=2))
    signal = EntrySignal(
        entry_ts=datetime(2024, 1, 2, 10, 0, 0),
        direction="CALL",
        spy_price_at_entry=450.0,
        trade_date=date(2024, 1, 2),
    )

    spec = selector.select_contract(signal)

    assert spec.strike == 455
