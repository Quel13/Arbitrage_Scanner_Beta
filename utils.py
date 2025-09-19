# utils.py
import time
from datetime import datetime, timezone
import queue

from .config import EXCLUDE_PATTERNS

# ---- tiempo / formato ----
def now_ts_ms() -> int:
    return int(time.time() * 1000)

def ts_iso(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms/1000, tz=timezone.utc).isoformat()

# ---- normalización de símbolos ----
def normalize_symbol(symbol: str) -> str:
    """'BTC/USDT:USDT' -> 'BTC/USDT'."""
    if ":" in symbol:
        return symbol.split(":")[0]
    return symbol

def quote_of(symbol: str) -> str:
    try:
        return symbol.split("/")[1].split(":")[0]
    except Exception:
        return ""

def is_spot_usual(market: dict) -> bool:
    if not market.get("spot", False):
        return False
    sym = market.get("symbol", "")
    return not any(p in sym for p in EXCLUDE_PATTERNS)

# ---- métricas (spread, volumen) ----
def compute_spread_bps(bid, ask):
    if bid is None or ask is None: return None
    if bid <= 0 or ask <= 0: return None
    mid = 0.5 * (bid + ask)
    if mid <= 0: return None
    return (ask - bid) / mid * 1e4  # bps

def volume_quote_est(ticker: dict):
    qv = ticker.get('quoteVolume')
    if isinstance(qv, (int, float)) and qv is not None:
        return float(qv)
    bv = ticker.get('baseVolume')
    last = ticker.get('last') or ticker.get('close') or (ticker.get('info', {}) or {}).get('last')
    try:
        return float(bv) * float(last)
    except Exception:
        return None

# ---- logging thread-safe (para GUI y consola) ----
log_queue: "queue.Queue[str]" = queue.Queue()

def qlog(msg: str):
    ts = datetime.utcnow().strftime('%H:%M:%S')
    log_queue.put(f"[{ts}] {msg}")

# ---- normalización de cadenas (networks) ----
CHAIN_ALIASES = {
    "ERC20":"ETHEREUM","ETH":"ETHEREUM","ETHEREUM":"ETHEREUM",
    "BSC":"BSC","BEP20":"BSC","BEP20(BSC)":"BSC",
    "TRC20":"TRON","TRON":"TRON","TRX":"TRON",
    "ARBITRUM":"ARBITRUM","ARB":"ARBITRUM",
    "OPTIMISM":"OPTIMISM","OP":"OPTIMISM",
    "SOL":"SOLANA","SOLANA":"SOLANA",
    "POLYGON":"POLYGON","MATIC":"POLYGON",
}
def norm_chain(name: str) -> str:
    if not name:
        return ""
    return CHAIN_ALIASES.get(name.strip().upper(), name.strip().upper())
