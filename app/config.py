import os
from pathlib import Path

# Static configuration for tickers and watchlists

TICKERS = {
    "VIX": "^VIX",
    "VIX3M": "^VIX3M",
    "TNX": "^TNX",
    "IRX": "^IRX",
    "DXY": "DX-Y.NYB",
    "HYG": "HYG",
    "LQD": "LQD",
    "SPY": "SPY",
    "QQQ": "QQQ",
    "XLY": "XLY",
    "XLP": "XLP",
    "IWM": "IWM",
    "GLD": "GLD",
    "SLV": "SLV"
}

WATCH_LIST = ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA", "WMT", "JPM", "GS"]

PRIMARY_SYMBOL = "SPY"

# Data paths - strictly /app/data as per GEMINI.md
DATA_DIR = Path("/app/data")
REPORT_PATH = DATA_DIR / "report.md"

# Schwab Token - Default to /app/tokens/tokens.json as per deployment standards
SCHWAB_TOKEN_FILE = Path("/app/tokens/tokens.json")
