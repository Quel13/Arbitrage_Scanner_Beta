# io_exchanges.py
import asyncio
from typing import Dict, Iterable, List, Optional

import ccxt.async_support as ccxt_async
import ccxtpro

from .utils import compute_spread_bps, is_spot_usual, now_ts_ms, qlog


class _ProFeed:
    """Supervisa las subscripciones CCXT Pro por exchange."""

    def __init__(self, loop: asyncio.AbstractEventLoop, exchange_id: str):
        self.loop = loop
        self.exchange_id = exchange_id
        self.exchange: Optional[ccxtpro.Exchange] = None
        self._running = False
        self._symbols: set[str] = set()
        self._tasks: Dict[str, asyncio.Task] = {}
        self.cache: Dict[str, dict] = {}
        self._lock = asyncio.Lock()

    async def start(self):
        if self.exchange is None:
            cls = getattr(ccxtpro, self.exchange_id)
            self.exchange = cls({
                "enableRateLimit": True,
                "options": {"defaultType": "spot"},
                "newUpdates": True,
            })
            await self.exchange.load_markets()
        self._running = True

    async def stop(self):
        self._running = False
        for task in list(self._tasks.values()):
            task.cancel()
        for task in list(self._tasks.values()):
            try:
                await task
            except Exception:
                pass
        self._tasks.clear()
        self._symbols.clear()
        if self.exchange is not None:
            try:
                await self.exchange.close()
            except Exception:
                pass
            self.exchange = None

    async def set_symbols(self, symbols: Iterable[str]):
        await self.start()
        new_symbols = set(symbols)
        async with self._lock:
            to_add = new_symbols - self._symbols
            to_remove = self._symbols - new_symbols
            self._symbols = new_symbols

            for sym in to_remove:
                task = self._tasks.pop(sym, None)
                if task:
                    task.cancel()
                self.cache.pop(sym, None)

            for sym in to_add:
                self._tasks[sym] = self.loop.create_task(self._symbol_loop(sym))

    async def _symbol_loop(self, symbol: str):
        assert self.exchange is not None
        backoff = 1.0
        while self._running and symbol in self._symbols:
            try:
                ticker = await self.exchange.watch_ticker(symbol)
                if not ticker:
                    continue
                bid = ticker.get("bid")
                ask = ticker.get("ask")
                last = ticker.get("last") or ticker.get("close")
                qv_raw = ticker.get("quoteVolume") or ticker.get("info", {}).get("q")
                qv = None
                if qv_raw is not None:
                    try:
                        qv = float(qv_raw)
                    except (TypeError, ValueError):
                        qv = None
                ts = ticker.get("timestamp")
                if ts is None:
                    ts = ticker.get("info", {}).get("E") or ticker.get("info", {}).get("ts")
                if ts is None and self.exchange is not None:
                    ts = self.exchange.milliseconds()
                self.cache[symbol] = {
                    "bid": bid,
                    "ask": ask,
                    "last": last,
                    "quoteVolume": qv,
                    "timestamp": float(ts) if ts is not None else now_ts_ms(),
                }
                backoff = 1.0
            except asyncio.CancelledError:
                break
            except Exception as exc:
                qlog(f"[WARN] WS {self.exchange_id}:{symbol} -> {exc}")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)


class DataHub:
    def __init__(self, loop: asyncio.AbstractEventLoop, max_pairs: int = 200, max_age_ms: int = 2500):
        self.loop = loop
        self.max_pairs = max_pairs
        self.max_age_ms = max_age_ms
        self._feeds: Dict[str, _ProFeed] = {}
        self._stale: Dict[str, set[str]] = {}
        self._rest_orderbooks: Dict[str, ccxt_async.Exchange] = {}
        self._rest_lock = asyncio.Lock()

    async def configure(self, exchanges: Iterable[str]):
        target = set(exchanges)
        # remove unused
        for ex_id in list(self._feeds.keys()):
            if ex_id not in target:
                feed = self._feeds.pop(ex_id)
                await feed.stop()
                self._stale.pop(ex_id, None)
        # add new
        for ex_id in target:
            if ex_id not in self._feeds:
                self._feeds[ex_id] = _ProFeed(self.loop, ex_id)

    async def update_from_rankings(self, ranked_rows: List[dict]) -> List[dict]:
        # determina top-M global y ajusta subscripciones
        sorted_rows = sorted(ranked_rows, key=lambda r: r.get("score", 0.0), reverse=True)
        limited = sorted_rows[: self.max_pairs]
        targets: Dict[str, List[str]] = {}
        for row in limited:
            targets.setdefault(row['exchange'], []).append(row['symbol'])

        await asyncio.gather(*[
            self._feeds[ex_id].set_symbols(symbols)
            for ex_id, symbols in targets.items()
            if ex_id in self._feeds
        ])

        # exchanges sin símbolos -> limpiar
        empty = [ex_id for ex_id in self._feeds if ex_id not in targets]
        for ex_id in empty:
            await self._feeds[ex_id].set_symbols([])

        snapshots = {ex_id: self.snapshot_for(ex_id) for ex_id in self._feeds}
        enriched: List[dict] = []
        for row in ranked_rows:
            ex_id = row['exchange']
            sym = row['symbol']
            ws_data = snapshots.get(ex_id, {}).get(sym)
            new_row = dict(row)
            if ws_data:
                if ws_data.get('bid') is not None:
                    new_row['bid'] = ws_data['bid']
                if ws_data.get('ask') is not None:
                    new_row['ask'] = ws_data['ask']
                if ws_data.get('quoteVolume') is not None:
                    new_row['quote_volume'] = ws_data['quoteVolume']
                new_row['last'] = ws_data.get('last')
                new_row['ws_timestamp'] = ws_data.get('timestamp')
                new_row['stale'] = False
            else:
                new_row['stale'] = True

            spread = compute_spread_bps(new_row.get('bid'), new_row.get('ask'))
            if spread is None:
                continue
            new_row['spread_bps'] = spread
            qv = new_row.get('quote_volume')
            if isinstance(qv, (int, float)):
                new_row['score'] = float(qv) / max(spread, 1e-6)
            enriched.append(new_row)
        return enriched

    def snapshot_for(self, exchange_id: str) -> Dict[str, dict]:
        feed = self._feeds.get(exchange_id)
        if not feed:
            return {}
        now = now_ts_ms()
        stale_prev = self._stale.get(exchange_id, set())
        fresh: Dict[str, dict] = {}
        stale_now: set[str] = set()
        for sym, data in feed.cache.items():
            ts = data.get('timestamp')
            try:
                ts_val = float(ts)
            except (TypeError, ValueError):
                ts_val = 0.0
            if ts_val <= 0 or now - ts_val > self.max_age_ms:
                stale_now.add(sym)
                continue
            fresh[sym] = dict(data)
        became = stale_now - stale_prev
        recovered = stale_prev - stale_now
        if became:
            qlog(f"[{exchange_id}] STALE: {', '.join(sorted(became))}")
        if recovered:
            qlog(f"[{exchange_id}] recuperados: {', '.join(sorted(recovered))}")
        self._stale[exchange_id] = stale_now
        return fresh

    def is_stale(self, exchange_id: str, symbol: str) -> bool:
        return symbol in self._stale.get(exchange_id, set())

    async def close(self):
        for feed in list(self._feeds.values()):
            await feed.stop()
        self._feeds.clear()
        self._stale.clear()
        for client in list(self._rest_orderbooks.values()):
            try:
                await client.close()
            except Exception:
                pass
        self._rest_orderbooks.clear()

    async def fetch_order_book(self, exchange_id: str, symbol: str, limit: int = 20):
        async with self._rest_lock:
            client = self._rest_orderbooks.get(exchange_id)
            if client is None:
                cls = getattr(ccxt_async, exchange_id)
                client = cls({
                    "enableRateLimit": True,
                    "options": {"defaultType": "spot"},
                })
                await client.load_markets()
                self._rest_orderbooks[exchange_id] = client
        try:
            return await client.fetch_order_book(symbol, limit=limit)
        except Exception as exc:
            qlog(f"[WARN] orderbook {exchange_id}:{symbol} -> {exc}")
            return None


_hub: Optional[DataHub] = None


async def initialize_data_hub(loop: asyncio.AbstractEventLoop, exchanges: List[str],
                              quotes: set[str], ws_top: int) -> DataHub:
    global _hub
    if _hub is None:
        _hub = DataHub(loop, max_pairs=ws_top)
    await _hub.configure(exchanges)
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


async def create_exchange(exchange_id: str):
    cls = getattr(ccxt_async, exchange_id)
    inst = cls({
        "enableRateLimit": True,
        "options": {"defaultType": "spot"},
    })
    return inst


async def load_markets_safe(ex):
    try:
        markets = await ex.load_markets()
    except Exception as exc:
        qlog(f"[WARN] load_markets {ex.id}: {exc}")
        return {}
    return {
        sym: info
        for sym, info in markets.items()
        if is_spot_usual(info)
    }


async def fetch_tickers_safe(ex) -> Dict[str, dict]:
    try:
        if ex.has.get('fetchTickers'):
            tickers = await ex.fetch_tickers()
        else:
            tickers = {}
            for sym in ex.symbols:
                try:
                    tickers[sym] = await ex.fetch_ticker(sym)
                except Exception:
                    continue
    except Exception as exc:
        qlog(f"[WARN] fetch_tickers {ex.id}: {exc}")
        return {}
    return tickers


async def fetch_order_book_once(ex_id: str, symbol: str, limit: int = 20):
    hub = get_data_hub()
    return await hub.fetch_order_book(ex_id, symbol, limit=limit)


async def sync_rankings_with_ws(ranked_rows: List[dict]) -> List[dict]:
    hub = get_data_hub()
    return await hub.update_from_rankings(ranked_rows)
