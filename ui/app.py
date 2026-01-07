from __future__ import annotations

import datetime as dt
import streamlit as st

from backtesting_bot.experiment_config import (
    AccountParams,
    ContractSelectionParams,
    ExitParams,
    ExperimentConfig,
    OrbParams,
)
from backtesting_bot.experiment_runner import (
    generate_experiment_id,
    list_experiments,
    load_experiment_result,
    run_experiment,
)


st.set_page_config(page_title="Experiment Lab", layout="wide")

st.markdown(
    """
    <style>
    .stButton>button { width: 100%; }
    .section-title { font-size: 1.1rem; font-weight: 600; margin-top: 0.5rem; }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("Experiment Lab")


@st.cache_data(show_spinner=False)
def _load_experiment_ids() -> list[str]:
    return list_experiments()


def _render_results(experiment_id: str) -> None:
    result = load_experiment_result(experiment_id)
    st.subheader(f"Results: {experiment_id}")

    if not result.metrics:
        st.warning("No metrics found for this experiment.")
    else:
        cols = st.columns(4)
        cols[0].metric("Total Trades", result.metrics.get("total_trades", 0))
        cols[1].metric("Win Rate", f"{result.metrics.get('win_rate', 0):.2%}")
        cols[2].metric("Total PnL", f"${result.metrics.get('total_pnl', 0):,.2f}")
        cols[3].metric("Ending Equity", f"${result.metrics.get('ending_equity', 0):,.2f}")

        with st.expander("Raw metrics", expanded=False):
            st.json(result.metrics)

    st.subheader("Trades")
    if result.trades.empty:
        st.info("No trades found.")
    else:
        st.dataframe(result.trades)

    st.subheader("Equity Curve")
    if result.equity_curve.empty:
        st.info("No equity curve data found.")
    else:
        st.dataframe(result.equity_curve)


with st.sidebar:
    st.header("Load previous experiment")
    experiment_ids = _load_experiment_ids()
    selected_experiment = st.selectbox(
        "Experiment ID",
        options=["-"] + experiment_ids,
    )
    if st.button("Load", use_container_width=True) and selected_experiment != "-":
        _render_results(selected_experiment)


with st.form("experiment_form"):
    st.subheader("Experiment Settings")
    test_name = st.text_input("Test name", value="ORB baseline")
    date_range = st.date_input(
        "Date range",
        value=(dt.date.today() - dt.timedelta(days=7), dt.date.today()),
    )

    strategy = st.selectbox("Strategy", options=["ORB (orb_v1)"])

    st.markdown('<div class="section-title">ORB Parameters</div>', unsafe_allow_html=True)
    orb_minutes_choice = st.selectbox("ORB minutes", options=[15, 30, "Custom"])
    if orb_minutes_choice == "Custom":
        orb_minutes = st.number_input("Custom ORB minutes", min_value=5, value=15, step=5)
    else:
        orb_minutes = int(orb_minutes_choice)

    candle_interval_minutes = st.selectbox("Candle interval (minutes)", options=[1, 3, 5])
    breakout_basis = st.selectbox("Breakout basis", options=["close", "wick"])
    confirm_full_candle = st.checkbox("Confirm full candle (close basis only)")
    max_trades_per_day = st.number_input(
        "Max trades per day", min_value=1, value=1, step=1
    )
    no_entries_after_enabled = st.checkbox("Set no-entries-after cutoff")
    no_entries_after = None
    if no_entries_after_enabled:
        no_entries_after = st.time_input("No entries after (ET)", value=dt.time(11, 30))

    st.markdown('<div class="section-title">Exit Parameters</div>', unsafe_allow_html=True)
    stop_loss_pct = st.number_input("Stop loss (%)", min_value=0.0, value=20.0) / 100
    take_profit_mode = st.selectbox(
        "Take profit mode", options=["static_pct", "dynamic_spy_rule", "partial_tp"]
    )
    take_profit_pct = st.number_input("Take profit (%)", min_value=0.0, value=30.0) / 100
    trailing_enabled = st.checkbox("Trailing stop enabled")
    trail_pct = st.number_input("Trail (%)", min_value=0.0, value=10.0) / 100

    partial_tp_enabled = st.checkbox("Partial take profit enabled")
    split_pct = st.number_input("Split % for first exit", min_value=0.1, max_value=0.9, value=0.7)
    first_tp_pct = st.number_input("First TP (%)", min_value=0.0, value=20.0) / 100
    runner_trail_pct = st.number_input("Runner trail (%)", min_value=0.0, value=15.0) / 100
    both_hit_same_second = st.selectbox(
        "Both hit in same bar", options=["stop_first", "tp_first"]
    )

    st.markdown('<div class="section-title">Contract Selection</div>', unsafe_allow_html=True)
    dte_choices = st.multiselect("DTE choices", options=[1, 2], default=[1])
    otm_dollars = st.multiselect("OTM dollars", options=[1, 2, 3], default=[1])
    prefer_otm = st.checkbox("Prefer OTM", value=True)

    st.markdown('<div class="section-title">Account</div>', unsafe_allow_html=True)
    starting_cash = st.number_input("Starting cash", min_value=1000.0, value=25000.0)
    allocation_pct_per_trade = (
        st.number_input("Allocation % per trade", min_value=0.0, value=10.0) / 100
    )
    max_daily_loss_pct = (
        st.number_input("Max daily loss % (0 to disable)", min_value=0.0, value=0.0) / 100
    )
    run_clicked = st.form_submit_button("Run Experiment")


if run_clicked:
    if not isinstance(date_range, tuple) or len(date_range) != 2:
        st.error("Please select a start and end date.")
    else:
        start_date, end_date = date_range
        config = ExperimentConfig(
            test_name=test_name,
            start_date=start_date,
            end_date=end_date,
            strategy="orb_v1",
            orb=OrbParams(
                orb_minutes=int(orb_minutes),
                candle_interval_minutes=int(candle_interval_minutes),
                breakout_basis=breakout_basis,
                confirm_full_candle=confirm_full_candle,
                max_trades_per_day=int(max_trades_per_day),
                no_entries_after=no_entries_after,
            ),
            exit=ExitParams(
                stop_loss_pct=float(stop_loss_pct),
                take_profit_mode=take_profit_mode,
                take_profit_pct=float(take_profit_pct),
                trailing_enabled=trailing_enabled,
                trail_pct=float(trail_pct),
                partial_tp_enabled=partial_tp_enabled or take_profit_mode == "partial_tp",
                split_pct=float(split_pct),
                first_tp_pct=float(first_tp_pct),
                runner_trail_pct=float(runner_trail_pct),
                both_hit_same_second=both_hit_same_second,
            ),
            contract=ContractSelectionParams(
                dte_choices=[int(value) for value in dte_choices],
                otm_dollars=[int(value) for value in otm_dollars],
                prefer_otm=prefer_otm,
            ),
            account=AccountParams(
                starting_cash=float(starting_cash),
                allocation_pct_per_trade=float(allocation_pct_per_trade),
                max_daily_loss_pct=float(max_daily_loss_pct) if max_daily_loss_pct > 0 else None,
            ),
        )

        experiment_id = generate_experiment_id(test_name)
        with st.spinner("Running backtest..."):
            result = run_experiment(config, experiment_id=experiment_id)

        _render_results(result.experiment_id)

st.caption("Experiment Lab writes outputs under data_local/experiments/.")
