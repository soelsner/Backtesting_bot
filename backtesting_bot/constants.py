from __future__ import annotations

import datetime as dt

MARKET_TIMEZONE = "America/New_York"
SESSION_START = dt.time(9, 30)
SESSION_END = dt.time(16, 0)
DEFAULT_SPY_1M_PATH = "data_local/spy_1m.parquet"
DEFAULT_ORB_CANDLES = 3
DEFAULT_MAX_TRADES_PER_DAY = 1
