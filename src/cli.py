from __future__ import annotations

import argparse
import os
from collections import Counter
from dataclasses import asdict
from pathlib import Path

import pandas as pd

from src.data.options_store import fetch_and_cache_option_1s
from src.models.signals import EntrySignal
from src.options.contract_selector import ContractSelectionConfig, ContractSelector
from src.providers.massive import MassiveConfig, MassiveProvider


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="SPY options backtest tooling")
    subparsers = parser.add_subparsers(dest="command", required=True)

    pass2 = subparsers.add_parser("pass2-fetch", help="Resolve option contracts + cache 1s data")
    pass2.add_argument("--run-id", required=True)
    pass2.add_argument("--limit", type=int)
    pass2.add_argument("--data-root", default="data_local")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "pass2-fetch":
        run_pass2_fetch(args)
        return

    raise SystemExit(f"Unknown command: {args.command}")


def run_pass2_fetch(
    args: argparse.Namespace,
    provider: MassiveProvider | None = None,
    selector: ContractSelector | None = None,
) -> None:
    data_root = Path(args.data_root)
    run_dir = data_root / "runs" / args.run_id
    entries_path = run_dir / "entries.parquet"
    if not entries_path.exists():
        raise SystemExit(f"entries.parquet not found at {entries_path}")

    entries = pd.read_parquet(entries_path)
    if args.limit:
        entries = entries.head(args.limit)

    if provider is None:
        api_key = os.getenv("MASSIVE_API_KEY") or os.getenv("POLYGON_API_KEY")
        provider = MassiveProvider(MassiveConfig(api_key=api_key))
    if selector is None:
        selector = ContractSelector(provider, ContractSelectionConfig())

    contracts_rows: list[dict] = []
    cache_hits = 0
    downloads = 0
    skips: Counter[str] = Counter()

    for index, record in entries.iterrows():
        signal = EntrySignal.from_record(record.to_dict())
        try:
            spec = selector.select_contract(signal)
        except Exception as exc:  # noqa: BLE001 - summary reporting
            skips[f"selection:{type(exc).__name__}"] += 1
            continue

        try:
            result = fetch_and_cache_option_1s(
                spec.provider_ticker,
                signal.trade_date,
                provider,
                data_root,
            )
        except Exception as exc:  # noqa: BLE001 - summary reporting
            skips[f"fetch:{type(exc).__name__}"] += 1
            continue

        cache_hits += 1 if result.cache_hit else 0
        downloads += 0 if result.cache_hit else 1
        row = {
            "entry_index": index,
            "entry_ts": signal.entry_ts,
            "direction": signal.direction,
            "spy_price_at_entry": signal.spy_price_at_entry,
            "trade_date": signal.trade_date,
            **asdict(spec),
            "option_rows": result.row_count,
            "cache_hit": result.cache_hit,
        }
        contracts_rows.append(row)

    contracts_df = pd.DataFrame(contracts_rows)
    contracts_path = run_dir / "contracts.parquet"
    contracts_df.to_parquet(contracts_path, index=False)

    summary = {
        "entries": len(entries),
        "contracts_resolved": len(contracts_df),
        "downloads": downloads,
        "cache_hits": cache_hits,
        "skips": sum(skips.values()),
    }

    print("Pass2 fetch summary")
    for key, value in summary.items():
        print(f"- {key}: {value}")
    if skips:
        print("Skip reasons:")
        for reason, count in skips.items():
            print(f"  - {reason}: {count}")


if __name__ == "__main__":
    main()
