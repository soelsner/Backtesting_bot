# Backtesting Bot (SPY Options)

Foundation scaffold for a two-pass SPY options backtesting framework. This step focuses on configuration, provider stubs, and local caching for SPY 1-minute bars.

## Requirements

- Python 3.10+
- Dependencies: `requests`, `pyyaml`, `pandas`, `pyarrow`

Install:

```bash
pip install -r requirements.txt
```

## Configuration

You can provide configuration via environment variables and/or an optional `config.yaml`.
Environment variables always take precedence.

**Required environment variables**

- `MASSIVE_API_KEY`
- `ALPACA_API_KEY`
- `ALPACA_SECRET_KEY`

Optional `config.yaml` example:

```yaml
massive:
  api_key: "your_key_here"
  base_url: "https://api.polygon.io"

alpaca:
  api_key: "your_key_here"
  secret_key: "your_secret_here"
  base_url: "https://paper-api.alpaca.markets"

local:
  data_dir: "data_local"
```

## Commands

### Health check

Validates required env vars and pings Massive (Polygon) + Alpaca paper endpoints.

```bash
python -m src.cli.main health-check
```

To validate config without external calls:

```bash
python -m src.cli.main health-check --skip-ping
```

### Fetch SPY 1-minute bars

Downloads SPY 1-minute bars from Alpaca and caches them under:
`data_local/spy/1m/date=YYYY-MM-DD/data.parquet`

```bash
python -m src.cli.main fetch-spy --start 2025-01-02 --end 2025-01-10
```

Re-running the command will skip cached dates.

Cached bars are stored in `America/New_York` and filtered to regular market hours
(09:30–16:00 ET).

## Experiment Lab UI

Run the Streamlit UI to configure experiments and execute backtests:

```bash
streamlit run ui/app.py
```

Experiment outputs are written under `data_local/experiments/`. See
[`docs/EXPERIMENTS.md`](docs/EXPERIMENTS.md) for details.

## Project Structure

```
src/
  cli/main.py            # CLI entrypoint
  config.py              # YAML + env configuration loading
  providers/
    base.py              # Provider abstractions
    massive.py           # Massive (Polygon) provider stub + fetch
    alpaca.py            # Alpaca broker stub + ping
  cache/spy_cache.py     # Parquet caching for SPY 1m bars
```
