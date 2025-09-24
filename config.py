# config.py
DEFAULT_EXCHANGES = ["binance", "bitget", "bybit"]

# Fees taker (basis points). Ajusta a tu cuenta/nivel.
TAKER_FEES_BPS = {
    "binance": 10.0, "bitget": 10.0, "bitrue": 10.0, "bybit": 10.0,
    "coinbase": 50.0, "cryptocom": 40.0, "mexc": 20.0,
}

# Excluir tokens apalancados/raros que distorsionan el screener
EXCLUDE_PATTERNS = ("3L", "3S", "5L", "5S", "BULL", "BEAR", "UP/", "DOWN/", "-UP/", "-DOWN/")

