# Architecture Overview

```
providers/              -> data providers (Massive, Alpaca)
  massive.py            -> historical SPY + options 1s aggregates
  alpaca.py             -> paper/live execution (future)

data/                   -> local data stores
  spy_store.py          -> SPY bars cache
  options_store.py      -> options 1s cache

strategies/             -> Pass 1 signal generation
  pass1.py              -> ORB/EMA/RSI signals -> EntrySignals

options/                -> contract selection
  contract_selector.py  -> EntrySignal -> ContractSpec

simulation/             -> Pass 2 trade engine (later)
  pass2.py              -> option series + exits
  exit_engine.py        -> SL/TP/time stop logic

metrics/                -> reporting + analysis
  summary.py            -> results, charts, CSV/Parquet outputs
```

## Flow
1. **Pass 1** loads SPY bars from `spy_store` and produces `EntrySignals`.
2. **Contract selection** maps each EntrySignal to a specific option contract using `options/contract_selector.py`.
3. **Options store** fetches and caches 1-second aggregates for the selected contract in `data_local/options/1s/`.
4. **Pass 2 simulation** (later) replays second bars to model exits with conservative fill rules.
5. **Metrics/reporting** summarize results for evaluation and iteration.
