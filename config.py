# config.py
DEFAULT_EXCHANGES = ["binance", "bitget", "bybit", "mexc"]

# Fees taker (basis points). Ajusta a tu cuenta/nivel.
TAKER_FEES_BPS = {
    "binance": 10.0, "bitget": 10.0, "bitrue": 10.0, "bybit": 10.0,
    "coinbase": 50.0, "cryptocom": 40.0, "mexc": 20.0,
}

# Excluir tokens apalancados/raros que distorsionan el screener
EXCLUDE_PATTERNS = ("3L", "3S", "5L", "5S", "BULL", "BEAR", "UP/", "DOWN/", "-UP/", "-DOWN/")

# Endpoints verificación de redes
BITGET_COINS_URL  = "https://api.bitget.com/api/v2/spot/public/coins"
BINANCE_CONFIG_URL = "https://api.binance.com/sapi/v1/capital/config/getall"  # requiere API key
BYBIT_COIN_INFO_URL = "https://api.bybit.com/v5/asset/coin/query-info"
MEXC_CONFIG_URL     = "https://api.mexc.com/api/v3/capital/config/getall"
