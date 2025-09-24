import asyncio
import json
from typing import Callable, Dict, List

import aiohttp

from ..utils import now_ts_ms, qlog


class ExchangeWS:
    """Binance Spot websocket consumer for best bid/ask and ticker updates."""

    STREAM_CHUNK = 100  # symbols per connection (each uses two streams)

    def __init__(self, loop: asyncio.AbstractEventLoop, symbols: List[str], on_update: Callable[[str, Dict], None]):
        self.loop = loop
        self.symbols = sorted(set(symbols))
        self.on_update = on_update
        self.cache: Dict[str, Dict] = {}
        self._stop = asyncio.Event()
        self._tasks: List[asyncio.Task] = []

    async def start(self):
        if not self.symbols:
            return
        self._stop.clear()
        chunks = [self.symbols[i:i + self.STREAM_CHUNK] for i in range(0, len(self.symbols), self.STREAM_CHUNK)]
        for chunk in chunks:
            task = self.loop.create_task(self._run_chunk(chunk))
            self._tasks.append(task)

    async def stop(self):
        self._stop.set()
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    async def _run_chunk(self, chunk: List[str]):
        base_url = "wss://stream.binance.com:9443/stream"
        streams = []
        for sym in chunk:
            stream_name = sym.replace("/", "").lower()
            streams.append(f"{stream_name}@bookTicker")
            streams.append(f"{stream_name}@ticker")
        url = f"{base_url}?streams={'/'.join(streams)}"

        backoff = 1.0
        while not self._stop.is_set():
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(url, heartbeat=30) as ws:
                        qlog(f"[binance] WS conectado ({len(chunk)} símbolos)")
                        backoff = 1.0
                        ping_task = self.loop.create_task(self._ping_periodically(ws))
                        try:
                            while not self._stop.is_set():
                                msg = await ws.receive()
                                if msg.type == aiohttp.WSMsgType.TEXT:
                                    payload = json.loads(msg.data)
                                    self._handle_message(payload)
                                elif msg.type == aiohttp.WSMsgType.BINARY:
                                    payload = json.loads(msg.data.decode("utf-8"))
                                    self._handle_message(payload)
                                elif msg.type == aiohttp.WSMsgType.ERROR:
                                    raise ws.exception() or RuntimeError("binance ws error")
                                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSE):
                                    break
                        finally:
                            ping_task.cancel()
                            await asyncio.gather(ping_task, return_exceptions=True)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                qlog(f"[WARN] binance ws reinicio ({exc})")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
            else:
                await asyncio.sleep(1)

    async def _ping_periodically(self, ws: aiohttp.ClientWebSocketResponse, interval: float = 20.0):
        while not self._stop.is_set():
            await asyncio.sleep(interval)
            try:
                await ws.ping()
            except Exception:
                return

    def _handle_message(self, payload: Dict):
        stream = payload.get("stream")
        data = payload.get("data")
        if not stream or not data:
            return
        symbol_norm = data.get("s") or data.get("S")
        if not symbol_norm:
            return
        symbol = f"{symbol_norm[:-4]}/{symbol_norm[-4:]}" if symbol_norm.endswith("USDT") else self._symbol_from_id(symbol_norm)

        entry = self.cache.setdefault(symbol, {"bid": None, "ask": None, "last": None, "quoteVolume": None, "timestamp": None})
        changed = False
        ts_raw = data.get("E") or now_ts_ms()
        ts_val = None
        try:
            ts_val = float(ts_raw)
        except (TypeError, ValueError):
            ts_val = float(now_ts_ms())

        if stream.endswith("@bookTicker"):
            bid = data.get("b") or data.get("B")
            ask = data.get("a") or data.get("A")
            try:
                if bid is not None:
                    bid_f = float(bid)
                    if entry.get("bid") != bid_f:
                        entry["bid"] = bid_f
                        changed = True
                if ask is not None:
                    ask_f = float(ask)
                    if entry.get("ask") != ask_f:
                        entry["ask"] = ask_f
                        changed = True
            except (TypeError, ValueError):
                pass
        elif stream.endswith("@ticker"):
            last = data.get("c")
            qv = data.get("q")
            try:
                if last is not None:
                    last_f = float(last)
                    if entry.get("last") != last_f:
                        entry["last"] = last_f
                        changed = True
            except (TypeError, ValueError):
                pass
            try:
                if qv is not None:
                    qv_f = float(qv)
                    if entry.get("quoteVolume") != qv_f:
                        entry["quoteVolume"] = qv_f
                        changed = True
            except (TypeError, ValueError):
                pass

        if ts_val is not None and entry.get("timestamp") != ts_val:
            entry["timestamp"] = ts_val
            changed = True

        if changed:
            self.on_update(symbol, entry.copy())

    @staticmethod
    def _symbol_from_id(symbol_id: str) -> str:
        if "/" in symbol_id:
            return symbol_id
        if symbol_id.endswith("USDT"):
            base = symbol_id[:-4]
            quote = symbol_id[-4:]
            return f"{base}/{quote}"
        if symbol_id.endswith("USDC"):
            return f"{symbol_id[:-4]}/USDC"
        return symbol_id
