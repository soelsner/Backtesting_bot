# Backtesting_bot
# SPY Options Backtesting Framework (ORB + EMA/RSI) — Two-Pass, Options-on-Demand

This README is a detailed build plan intended for coding agents (Codex/Claude). It describes architecture, modules, data flow, storage, configs, and acceptance criteria for a backtesting system focused on SPY options (1DTE/2DTE) with option price management at 1-second resolution.

---

## Architecture + Decisions

- Architecture overview: [`docs/ARCHITECTURE_OVERVIEW.md`](docs/ARCHITECTURE_OVERVIEW.md)
- Project decisions: [`docs/PROJECT_DECISIONS.md`](docs/PROJECT_DECISIONS.md)

---

## 0) Core Goals

### Primary goals
1. Backtest multiple SPY signal strategies:
   - ORB (Opening Range Breakout)
   - EMA crossovers
   - RSI and other technicals
2. Use SPY historical bars for signals/indicators.
3. Simulate options trades using historical option **1-second aggregates** for:
   - stop loss
   - take profit
   - trailing stop
   - partial exits (optional)
   - time stop (optional)
4. Avoid downloading massive option datasets by using a **two-pass approach**:
   - Pass 1: discover entries using SPY only
   - Pass 2: fetch only the needed option contract series (1-second) for those entries
5. Support scenario testing:
   - configurable TP/SL parameters
   - slippage model variants
   - account size / allocation rules
   - max trades per day, cooldown, etc.
6. Deterministic reruns with caching of downloaded data.

### Non-goals (for v1)
- Perfect replication of Robinhood routing/fills (requires full quotes + venue microstructure).
- Full OPRA tick replay or historical order book.
- Modeling limit orders with queue priority.
- Portfolio-level multi-asset strategies (v1 is SPY only).

---

## 1) High-Level Design: Two-Pass Backtest

### Pass 1 — Signal Discovery (SPY only)
Inputs:
- SPY 1-minute bars (preferred) or 5-minute bars
- indicator features (ORB range, EMA, RSI, VWAP, etc.)
Outputs:
- EntrySignal records (time, direction, context)

**No options data required** in Pass 1.

### Pass 2 — Trade Simulation (options on demand)
For each EntrySignal:
1. Select an option contract (1DTE/2DTE, $1–$3 OTM by entry-time SPY price)
2. Fetch historical 1-second option aggregates for that contract (cached locally)
3. Simulate trade second-by-second (sparse seconds OK; carry-forward last state)
4. Record trade outcome + account impact

This yields realistic “would stop/TP have been hit?” behavior without pulling billions of rows.

---

## 2) Data Requirements

### SPY underlying data (signals + indicators)
- SPY 1-minute OHLCV for 2 years (regular trading session recommended initially)

### Options data (position management)
- For each traded contract:
  - 1-second aggregates OHLCV for the trade date (or entry→exit window)
- Sparse seconds are expected; simulation must handle carry-forward.

### Vendor/API assumptions
- A market data provider that supports:
  - SPY equity aggregates (1m)
  - Options contract reference lookup (by expiration, strike, call/put)
  - Options 1-second aggregates for a specific option ticker/contract

**IMPORTANT:** Do not hardcode to one provider in the core engine. Implement provider adapters.

---

## 3) Repository Structure (Proposed)
