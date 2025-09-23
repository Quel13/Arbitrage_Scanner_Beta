from typing import Dict, List, Optional

import aiohttp

from .utils import is_spot_usual, qlog, quote_of, now_ts_ms
from .feeds.binance_ws import ExchangeWS as BinanceWS
from .feeds.bybit_ws import ExchangeWS as BybitWS
from .feeds.bitget_ws import ExchangeWS as BitgetWS


FEED_FACTORIES = {
    "binance": BinanceWS,
    "bybit": BybitWS,
    "bitget": BitgetWS,
}


async def _fetch_json(session: aiohttp.ClientSession, url: str, params: Optional[dict] = None) -> dict:
    try:
        async with session.get(url, params=params, timeout=15) as resp:
            resp.raise_for_status()
            return await resp.json()
    except Exception as exc:
        qlog(f"[WARN] fallo al obtener {url}: {exc}")
        return {}


async def _fetch_binance_markets(session: aiohttp.ClientSession) -> Dict[str, dict]:
    url = "https://api.binance.com/api/v3/exchangeInfo"
    data = await _fetch_json(session, url, params={"permissions": "SPOT"})
    symbols = {}
    for sym in data.get("symbols", []):
        if sym.get("status") != "TRADING":
            continue
        if not sym.get("isSpotTradingAllowed", True):
            continue
        base = sym.get("baseAsset")
        quote = sym.get("quoteAsset")
        if not base or not quote:
            continue
        symbol = f"{base}/{quote}"
        symbols[symbol] = {"symbol": symbol, "spot": True, "active": True}
    return symbols


async def _fetch_bybit_markets(session: aiohttp.ClientSession) -> Dict[str, dict]:
    url = "https://api.bybit.com/v5/market/instruments-info"
    symbols: Dict[str, dict] = {}
    cursor: Optional[str] = None
    while True:
        params = {"category": "spot"}
        if cursor:
            params["cursor"] = cursor
        data = await _fetch_json(session, url, params=params)
        result = data.get("result", {})
        for item in result.get("list", []):
            if item.get("status") not in ("Trading", "1"):
                continue
            base = item.get("baseCoin")
            quote = item.get("quoteCoin")
            if not base or not quote:
                continue
            symbol = f"{base}/{quote}"
            symbols[symbol] = {"symbol": symbol, "spot": True, "active": True}
        cursor = result.get("nextPageCursor")
        if not cursor:
            break
    return symbols


async def _fetch_bitget_markets(session: aiohttp.ClientSession) -> Dict[str, dict]:
    url = "https://api.bitget.com/api/spot/v1/public/products"
    data = await _fetch_json(session, url)
    symbols: Dict[str, dict] = {}
    for item in data.get("data", []):
        if item.get("status") not in ("online", "1"):
            continue
        base = item.get("baseCoin") or item.get("baseCoinName") or item.get("base")
        quote = item.get("quoteCoin") or item.get("quoteCoinName") or item.get("quote")
        if not base or not quote:
            continue
        symbol = f"{base}/{quote}"
        symbols[symbol] = {"symbol": symbol, "spot": True, "active": True}
    return symbols


MARKET_LOADERS = {
    "binance": _fetch_binance_markets,
    "bybit": _fetch_bybit_markets,
    "bitget": _fetch_bitget_markets,
}


class ExchangeHandle:
    def __init__(self, hub: "DataHub", exchange_id: str):
        self.id = exchange_id
        self._hub = hub

    async def close(self):
        # La conexión permanece viva en DataHub; aquí no se hace nada.
        return


class DataHub:
    def __init__(self, loop: asyncio.AbstractEventLoop):
        self.loop = loop
        self._feeds: Dict[str, object] = {}
        self._raw_markets: Dict[str, Dict[str, dict]] = {}
        self._filtered_markets: Dict[str, Dict[str, dict]] = {}
        self._symbols: Dict[str, List[str]] = {}
        self._cache: Dict[str, Dict[str, dict]] = {}
        self.quotes: set[str] = set()

    async def update_targets(self, exchanges: List[str], quotes: set[str]):
        target = set(exchanges)
        new_quotes = set(q.upper() for q in (quotes or set()))
        quotes_changed = new_quotes != self.quotes
        self.quotes = new_quotes

        async with aiohttp.ClientSession() as session:
            for ex in target:
                if ex in self._raw_markets:
                    continue
                loader = MARKET_LOADERS.get(ex)
                if not loader:
                    qlog(f"[WARN] No hay loader de mercados para {ex}")
                    continue
                markets = await loader(session)
                self._raw_markets[ex] = markets

        # eliminar exchanges que ya no se usan
        for ex in list(self._feeds.keys()):
            if ex not in target:
                feed = self._feeds.pop(ex)
                try:
                    await feed.stop()
                except Exception:
                    pass
                self._raw_markets.pop(ex, None)
                self._filtered_markets.pop(ex, None)
                self._symbols.pop(ex, None)
                self._cache.pop(ex, None)

        # preparar feeds para los target
        for ex in target:
            await self._ensure_feed(ex, force=quotes_changed)

    async def _ensure_feed(self, exchange_id: str, force: bool = False):
        loader = FEED_FACTORIES.get(exchange_id)
        if not loader:
            qlog(f"[WARN] Exchange no soportado por WS: {exchange_id}")
            return
        raw_markets = self._raw_markets.get(exchange_id, {})
        if not raw_markets:
            qlog(f"[WARN] No hay mercados para {exchange_id}")
            return

        filtered = {
            sym: info
            for sym, info in raw_markets.items()
            if info.get("active", True) and is_spot_usual(info) and (not self.quotes or quote_of(sym) in self.quotes)
        }
        self._filtered_markets[exchange_id] = filtered
        symbols = sorted(filtered.keys())

        current_symbols = self._symbols.get(exchange_id)
        feed = self._feeds.get(exchange_id)
        if not symbols:
            if feed:
                await feed.stop()
                self._feeds.pop(exchange_id, None)
            self._symbols.pop(exchange_id, None)
            self._cache.pop(exchange_id, None)
            return

        if feed and not force and current_symbols == symbols:
            return

        if feed:
            try:
                await feed.stop()
            except Exception:
                pass

        handler = self._make_handler(exchange_id)
        feed_instance = loader(self.loop, symbols, handler)
        self._feeds[exchange_id] = feed_instance
        self._symbols[exchange_id] = symbols
        cache = self._cache.setdefault(exchange_id, {})
        for obsolete in [s for s in cache if s not in symbols]:
            cache.pop(obsolete, None)
        await feed_instance.start()

    def _make_handler(self, exchange_id: str):
        def handler(symbol: str, data: dict):
            cache = self._cache.setdefault(exchange_id, {})
            cache[symbol] = data
        return handler

    async def close(self):
        for feed in list(self._feeds.values()):
            try:
                await feed.stop()
            except Exception:
                pass
        self._feeds.clear()

    def snapshot_for(self, exchange_id: str) -> Dict[str, dict]:
        now = now_ts_ms()
        cache = self._cache.get(exchange_id, {})
        result = {}
        for sym, data in cache.items():
            ts = data.get("timestamp")
            try:
                ts_val = float(ts)
            except (TypeError, ValueError):
                continue
            if now - ts_val > 1500:
                continue
            result[sym] = data.copy()
        return result

    def markets_for(self, exchange_id: str) -> Dict[str, dict]:
        return self._filtered_markets.get(exchange_id, {})

    def top_of_book(self, exchange_id: str, symbol: str) -> Optional[dict]:
        data = self._cache.get(exchange_id, {}).get(symbol)
        if not data:
            return None
        bid = data.get("bid")
        ask = data.get("ask")
        if bid is None or ask is None:
            return None
        return {
            "bids": [(bid, 1.0)],
            "asks": [(ask, 1.0)],
            "timestamp": data.get("timestamp"),
        }


_hub: DataHub | None = None


async def initialize_data_hub(loop: asyncio.AbstractEventLoop, exchanges: List[str], quotes: set[str]):
    global _hub
    if _hub is None:
        _hub = DataHub(loop)
    await _hub.update_targets(exchanges, quotes)
    return _hub


def get_data_hub() -> DataHub:
    if _hub is None:
        raise RuntimeError("DataHub no inicializado")
    return _hub


async def shutdown_data_hub():
    global _hub
    if _hub is not None:
        await _hub.close()
        _hub = None


async def create_exchange(exchange_id: str) -> ExchangeHandle:
    hub = get_data_hub()
    await hub._ensure_feed(exchange_id)
    return ExchangeHandle(hub, exchange_id)


async def load_markets_safe(ex: ExchangeHandle):
    hub = get_data_hub()
    return hub.markets_for(ex.id)


async def fetch_tickers_safe(ex: ExchangeHandle):
    hub = get_data_hub()
    return hub.snapshot_for(ex.id)


async def fetch_order_book_once(ex_id: str, symbol: str, limit: int = 20):
    hub = get_data_hub()
    return hub.top_of_book(ex_id, symbol)
