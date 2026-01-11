from __future__ import annotations

import argparse
import math
from pathlib import Path
from typing import Iterable, List

import pandas as pd


def _parse_int_list(value: str) -> List[int]:
    return [int(item.strip()) for item in value.split(",") if item.strip()]


def _strike_ladder(price: float, direction: str, count: int = 3) -> List[int]:
    if direction not in {"CALL", "PUT"}:
        raise ValueError("direction must be CALL or PUT")
    if count <= 0:
        return []
    if direction == "CALL":
        base = math.ceil(price)
        return [base + i for i in range(count)]
    base = math.floor(price)
    return [base - i for i in range(count)]


def _rows_for_range(
    date_value: str,
    label: str,
    high: float | None,
    low: float | None,
    strike_count: int,
    dtes: Iterable[int],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    if high is not None and not pd.isna(high):
        rows.append(
            {
                "date": date_value,
                "orb_window": label,
                "side": "CALL",
                "reference_price": float(high),
                "strikes": ",".join(str(s) for s in _strike_ladder(high, "CALL", strike_count)),
                "dtes": ",".join(str(dte) for dte in dtes),
            }
        )
    if low is not None and not pd.isna(low):
        rows.append(
            {
                "date": date_value,
                "orb_window": label,
                "side": "PUT",
                "reference_price": float(low),
                "strikes": ",".join(str(s) for s in _strike_ladder(low, "PUT", strike_count)),
                "dtes": ",".join(str(dte) for dte in dtes),
            }
        )
    return rows


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate OTM strike ladders from ORB ranges."
    )
    parser.add_argument(
        "--orb-csv",
        default="data_local/spy_orb_ranges.csv",
        help="CSV produced by backtesting_bot.orb_ranges.",
    )
    parser.add_argument(
        "--out",
        default="data_local/spy_orb_strikes.csv",
        help="Output CSV with strike ladders.",
    )
    parser.add_argument(
        "--strike-count",
        type=int,
        default=3,
        help="Number of OTM strikes to include (e.g., 3 for +1/+2/+3).",
    )
    parser.add_argument(
        "--dtes",
        default="1,2",
        help="Comma-separated DTE values to request (e.g., 1,2).",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    orb_path = Path(args.orb_csv)
    if not orb_path.exists():
        print(f"Error: ORB CSV not found at {orb_path}")
        return 1

    orb_df = pd.read_csv(orb_path)
    dtes = _parse_int_list(args.dtes)
    if not dtes:
        print("Error: --dtes must include at least one value.")
        return 1

    rows: list[dict[str, object]] = []
    for _, row in orb_df.iterrows():
        date_value = str(row.get("date", ""))
        rows.extend(
            _rows_for_range(
                date_value,
                "15m",
                row.get("orb_15_high"),
                row.get("orb_15_low"),
                args.strike_count,
                dtes,
            )
        )
        rows.extend(
            _rows_for_range(
                date_value,
                "30m",
                row.get("orb_30_high"),
                row.get("orb_30_low"),
                args.strike_count,
                dtes,
            )
        )

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(out_path, index=False)
    print(f"Wrote strike plan to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
