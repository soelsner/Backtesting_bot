# Experiments

The Experiment Lab UI stores each run under `data_local/experiments/<experiment_id>/`.

## Directory layout

```
data_local/experiments/<experiment_id>/
  config_snapshot/
    experiment.yaml
  pass1/
    entries.parquet
    run_metadata.json
    config_snapshot.json
  pass2/
    trades.parquet
    equity_curve.parquet
    metrics.json
```

- `experiment.yaml` contains the serialized `ExperimentConfig` that created the run.
- `pass1/` captures the entry signals produced by the Pass 1 pipeline.
- `pass2/` stores trade-level results, equity curve data, and summary metrics.

## Rerunning experiments

To rerun an experiment, open the Streamlit UI and select the experiment ID from the
"Load previous experiment" panel. The UI reads the stored metrics and tables without
recomputing the backtest.

If you need to re-run with the same configuration, open the YAML snapshot, create a
new experiment ID, and run again via the UI. This preserves prior outputs for comparison.
