from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd

from backtesting_bot.constants import MARKET_TIMEZONE, SESSION_END
from backtesting_bot.experiment_config import AccountParams, ExitParams
from backtesting_bot.io import load_spy_1m_bars, save_json, save_parquet


@dataclass(frozen=True)
class Pass2Config:
    start: dt.date
    end: dt.date
    spy_1m_path: str
    exit_params: ExitParams
    account_params: AccountParams
    output_dir: Path


def _iter_dates(entries_df: pd.DataFrame) -> Iterable[dt.date]:
    for value in entries_df["trade_date"].unique():
        yield dt.date.fromisoformat(value)


def _session_end_timestamp(trade_date: dt.date) -> pd.Timestamp:
    end_dt = dt.datetime.combine(trade_date, SESSION_END)
    return pd.Timestamp(end_dt, tz=MARKET_TIMEZONE).tz_convert("UTC")


def _resolve_stop_tp(
    entry_price: float, direction: str, exit_params: ExitParams
) -> tuple[float, float]:
    stop_loss_pct = exit_params.stop_loss_pct
    take_profit_pct = exit_params.take_profit_pct
    if direction == "CALL":
        stop_price = entry_price * (1 - stop_loss_pct)
        take_profit_price = entry_price * (1 + take_profit_pct)
    else:
        stop_price = entry_price * (1 + stop_loss_pct)
        take_profit_price = entry_price * (1 - take_profit_pct)
    return stop_price, take_profit_price


def _select_exit_price(
    direction: str,
    bar: pd.Series,
    stop_price: float,
    take_profit_price: float,
    both_hit_rule: str,
) -> tuple[float | None, str | None]:
    high_price = float(bar["high"])
    low_price = float(bar["low"])

    if direction == "CALL":
        tp_hit = high_price >= take_profit_price
        sl_hit = low_price <= stop_price
    else:
        tp_hit = low_price <= take_profit_price
        sl_hit = high_price >= stop_price

    if tp_hit and sl_hit:
        if both_hit_rule == "tp_first":
            return take_profit_price, "take_profit"
        return stop_price, "stop_loss"
    if tp_hit:
        return take_profit_price, "take_profit"
    if sl_hit:
        return stop_price, "stop_loss"
    return None, None


def _apply_trailing_stop(
    direction: str,
    bar: pd.Series,
    current_trail: float,
    trail_pct: float,
) -> float:
    high_price = float(bar["high"])
    low_price = float(bar["low"])
    if direction == "CALL":
        candidate = high_price * (1 - trail_pct)
        return max(current_trail, candidate)
    candidate = low_price * (1 + trail_pct)
    return min(current_trail, candidate)


def _simulate_trade(
    entry: pd.Series,
    day_df: pd.DataFrame,
    exit_params: ExitParams,
    allocation: float,
) -> dict:
    entry_ts = pd.Timestamp(entry["entry_ts"])
    if entry_ts.tzinfo is None:
        entry_ts = entry_ts.tz_localize("UTC")
    entry_price = float(entry["spy_price_at_entry"])
    direction = entry["direction"]

    stop_price, take_profit_price = _resolve_stop_tp(
        entry_price, direction, exit_params
    )

    trade_df = day_df.loc[day_df.index >= entry_ts].copy()
    if trade_df.empty:
        return {}

    exit_ts = trade_df.index[-1]
    exit_price = float(trade_df.iloc[-1]["close"])
    exit_reason = "session_end"

    split_pct = exit_params.split_pct
    remaining_pct = 1 - split_pct
    partial_taken = False
    partial_exit_price = None
    partial_exit_ts = None

    trail_stop = stop_price
    runner_trail_pct = (
        exit_params.runner_trail_pct
        if exit_params.partial_tp_enabled
        else exit_params.trail_pct
    )

    for ts, row in trade_df.iterrows():
        if exit_params.partial_tp_enabled and not partial_taken:
            first_tp = entry_price * (
                1 + exit_params.first_tp_pct
                if direction == "CALL"
                else 1 - exit_params.first_tp_pct
            )
            hit_price, reason = _select_exit_price(
                direction,
                row,
                stop_price,
                first_tp,
                exit_params.both_hit_same_second,
            )
            if hit_price is not None:
                if reason == "take_profit":
                    partial_taken = True
                    partial_exit_price = hit_price
                    partial_exit_ts = ts
                    trail_stop = hit_price
                    continue
                exit_ts = ts
                exit_price = hit_price
                exit_reason = reason
                break

        if exit_params.trailing_enabled:
            trail_stop = _apply_trailing_stop(direction, row, trail_stop, runner_trail_pct)

        hit_price, reason = _select_exit_price(
            direction,
            row,
            trail_stop,
            take_profit_price,
            exit_params.both_hit_same_second,
        )
        if hit_price is not None:
            exit_ts = ts
            exit_price = hit_price
            exit_reason = reason
            break

    qty = allocation / entry_price if entry_price else 0
    if direction == "CALL":
        pnl_full = qty * (exit_price - entry_price)
    else:
        pnl_full = qty * (entry_price - exit_price)

    pnl_partial = None
    pnl_runner = None
    total_pnl = pnl_full
    if partial_taken and partial_exit_price is not None:
        if direction == "CALL":
            pnl_partial = qty * split_pct * (partial_exit_price - entry_price)
            pnl_runner = qty * remaining_pct * (exit_price - entry_price)
        else:
            pnl_partial = qty * split_pct * (entry_price - partial_exit_price)
            pnl_runner = qty * remaining_pct * (entry_price - exit_price)
        total_pnl = pnl_partial + pnl_runner

    return {
        "trade_date": entry["trade_date"],
        "entry_ts": entry_ts,
        "exit_ts": exit_ts,
        "direction": direction,
        "entry_price": entry_price,
        "exit_price": exit_price,
        "exit_reason": exit_reason,
        "allocation": allocation,
        "pnl": total_pnl,
        "return_pct": total_pnl / allocation if allocation else 0,
        "partial_exit_ts": partial_exit_ts,
        "partial_exit_price": partial_exit_price,
        "partial_pnl": pnl_partial,
        "runner_pnl": pnl_runner,
    }


def _build_equity_curve(trades_df: pd.DataFrame, starting_cash: float) -> pd.DataFrame:
    if trades_df.empty:
        return pd.DataFrame(columns=["timestamp", "equity"])

    sorted_df = trades_df.sort_values("exit_ts").copy()
    equity = starting_cash
    records = []
    for _, row in sorted_df.iterrows():
        equity += float(row["pnl"])
        records.append({"timestamp": row["exit_ts"], "equity": equity})
    return pd.DataFrame(records)


def _build_metrics(trades_df: pd.DataFrame, equity_df: pd.DataFrame, starting_cash: float) -> dict:
    total_trades = int(trades_df.shape[0])
    wins = int((trades_df["pnl"] > 0).sum()) if total_trades else 0
    losses = int((trades_df["pnl"] <= 0).sum()) if total_trades else 0
    total_pnl = float(trades_df["pnl"].sum()) if total_trades else 0.0
    total_return_pct = total_pnl / starting_cash if starting_cash else 0.0

    max_drawdown_pct = 0.0
    if not equity_df.empty:
        equity_series = equity_df["equity"]
        running_max = equity_series.cummax()
        drawdowns = (equity_series - running_max) / running_max
        max_drawdown_pct = float(drawdowns.min())

    return {
        "total_trades": total_trades,
        "wins": wins,
        "losses": losses,
        "win_rate": wins / total_trades if total_trades else 0.0,
        "total_pnl": total_pnl,
        "total_return_pct": total_return_pct,
        "max_drawdown_pct": max_drawdown_pct,
        "ending_equity": starting_cash + total_pnl,
    }


def run_pass2_pipeline(config: Pass2Config, entries_df: pd.DataFrame) -> Path:
    spy_df = load_spy_1m_bars(config.spy_1m_path, config.start, config.end)
    if spy_df.empty or entries_df.empty:
        trades_df = pd.DataFrame(
            columns=[
                "trade_date",
                "entry_ts",
                "exit_ts",
                "direction",
                "entry_price",
                "exit_price",
                "exit_reason",
                "allocation",
                "pnl",
                "return_pct",
                "partial_exit_ts",
                "partial_exit_price",
                "partial_pnl",
                "runner_pnl",
            ]
        )
        equity_df = _build_equity_curve(trades_df, config.account_params.starting_cash)
        metrics = _build_metrics(trades_df, equity_df, config.account_params.starting_cash)
        save_parquet(trades_df, config.output_dir / "trades.parquet")
        save_parquet(equity_df, config.output_dir / "equity_curve.parquet")
        save_json(metrics, config.output_dir / "metrics.json")
        return config.output_dir

    trades: list[dict] = []
    current_cash = config.account_params.starting_cash
    daily_loss_limit = (
        config.account_params.max_daily_loss_pct
        if config.account_params.max_daily_loss_pct is not None
        else None
    )

    for trade_date in _iter_dates(entries_df):
        daily_entries = entries_df.loc[entries_df["trade_date"] == trade_date.isoformat()]
        day_mask = spy_df.index.tz_convert(MARKET_TIMEZONE).date == trade_date
        day_df = spy_df.loc[day_mask]
        if day_df.empty:
            continue

        day_loss = 0.0
        for _, entry in daily_entries.iterrows():
            allocation = current_cash * config.account_params.allocation_pct_per_trade
            if allocation <= 0:
                continue

            trade = _simulate_trade(entry, day_df, config.exit_params, allocation)
            if not trade:
                continue
            trades.append(trade)
            current_cash += trade["pnl"]
            day_loss += min(0.0, trade["pnl"])

            if daily_loss_limit is not None:
                if abs(day_loss) >= config.account_params.starting_cash * daily_loss_limit:
                    break

    trades_df = pd.DataFrame(trades)
    equity_df = _build_equity_curve(trades_df, config.account_params.starting_cash)
    metrics = _build_metrics(trades_df, equity_df, config.account_params.starting_cash)

    config.output_dir.mkdir(parents=True, exist_ok=True)
    save_parquet(trades_df, config.output_dir / "trades.parquet")
    save_parquet(equity_df, config.output_dir / "equity_curve.parquet")
    save_json(metrics, config.output_dir / "metrics.json")
    return config.output_dir
