from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class OptionFetchResult:
    path: Path
    cache_hit: bool
    row_count: int


def fetch_and_cache_option_1s(
    contract_ticker: str,
    trade_date: date,
    provider,
    data_root: Path,
) -> OptionFetchResult:
    safe_key = _safe_ticker_key(contract_ticker)
    output_dir = data_root / "options" / "1s" / f"date={trade_date.isoformat()}"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"ticker={safe_key}.parquet"

    if output_path.exists() and output_path.stat().st_size > 0:
        existing = pd.read_parquet(output_path)
        return OptionFetchResult(output_path, True, len(existing))

    results = provider.fetch_option_aggregates_1s(contract_ticker, trade_date)
    frame = _normalize_aggregates(results)
    frame.to_parquet(output_path, index=False)
    return OptionFetchResult(output_path, False, len(frame))


def _normalize_aggregates(results: list[dict]) -> pd.DataFrame:
    if not results:
        return pd.DataFrame(columns=["ts", "o", "h", "l", "c", "volume"])

    frame = pd.DataFrame(results)
    renamed = frame.rename(
        columns={
            "t": "ts",
            "o": "o",
            "h": "h",
            "l": "l",
            "c": "c",
            "v": "volume",
            "vw": "vwap",
            "n": "transactions",
        }
    )
    keep_cols = [col for col in ["ts", "o", "h", "l", "c", "volume", "vwap", "transactions"] if col in renamed]
    return renamed[keep_cols]


def _safe_ticker_key(ticker: str) -> str:
    return ticker.replace(":", "_").replace("/", "_")
