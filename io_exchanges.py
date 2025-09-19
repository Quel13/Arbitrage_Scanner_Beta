# io_exchanges.py
import asyncio
import ccxt.async_support as ccxt

from .utils import is_spot_usual, qlog

async def create_exchange(exchange_id: str):
    cls = getattr(ccxt, exchange_id)
    return cls({'enableRateLimit': True, 'timeout': 15000})

async def load_markets_safe(ex):
    try:
        return await ex.load_markets(reload=True)
    except Exception as e:
        qlog(f"[WARN] load_markets fallo en {ex.id}: {e}")
        return {}

async def fetch_tickers_safe(ex):
    """Devuelve {symbol: ticker} con bid/ask/volúmenes. Intenta fetch_tickers(); si falla, per-symbol."""
    if ex.has.get('fetchTickers', False):
        try:
            t = await ex.fetch_tickers()
            return t or {}
        except Exception as e:
            qlog(f"[WARN] fetch_tickers fallo en {ex.id}: {e}")

    markets = ex.markets or await load_markets_safe(ex)
    symbols = [s for s, m in markets.items() if m.get('active', True) and is_spot_usual(m)]
    sem = asyncio.Semaphore(8)

    async def fetch_one(sym):
        async with sem:
            try:
                return sym, await ex.fetch_ticker(sym)
            except Exception:
                return sym, None

    results = await asyncio.gather(*[fetch_one(s) for s in symbols])
    return {s: t for s, t in results if t}

# ---- order books (para estimar tamaño ejecutable) ----
async def fetch_order_book_once(ex_id: str, symbol: str, limit: int = 20):
    ex = await create_exchange(ex_id)
    try:
        await load_markets_safe(ex)
        return await ex.fetch_order_book(symbol, limit=limit)
    except Exception as e:
        qlog(f"[WARN] order_book {ex_id} {symbol}: {e}")
        return None
    finally:
        try: await ex.close()
        except Exception: pass
