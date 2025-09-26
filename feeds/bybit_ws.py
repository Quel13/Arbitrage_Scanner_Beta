import asyncio
import json
from typing import Callable, Dict, List

import aiohttp

from utils import now_ts_ms, qlog


class ExchangeWS:
    ENDPOINT = "wss://stream.bybit.com/v5/public/spot"

    def __init__(self, loop: asyncio.AbstractEventLoop, symbols: List[str], on_update: Callable[[str, Dict], None]):
        self.loop = loop
        self.symbols = sorted(set(symbols))
        self.on_update = on_update
        self.cache: Dict[str, Dict] = {}
        self._stop = asyncio.Event()
        self._task: asyncio.Task | None = None

    async def start(self):
        if not self.symbols:
            return
        self._stop.clear()
        if self._task is None or self._task.done():
            self._task = self.loop.create_task(self._run())

    async def stop(self):
        self._stop.set()
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None

    async def _run(self):
        backoff = 1.0
        channels = []
        for sym in self.symbols:
            symbol_id = sym.replace("/", "")
            channels.append(f"orderbook.1.{symbol_id}")
            channels.append(f"tickers.{symbol_id}")
        while not self._stop.is_set():
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(self.ENDPOINT, heartbeat=20) as ws:
                        sub_req = {"op": "subscribe", "args": channels}
                        await ws.send_json(sub_req)
                        qlog(f"[bybit] WS conectado ({len(self.symbols)} símbolos)")
                        backoff = 1.0
                        ping_task = self.loop.create_task(self._ping(ws))
                        try:
                            while not self._stop.is_set():
                                msg = await ws.receive()
                                if msg.type == aiohttp.WSMsgType.TEXT:
                                    payload = json.loads(msg.data)
                                    if self._handle_control(ws, payload):
                                        continue
                                    self._handle_message(payload)
                                elif msg.type == aiohttp.WSMsgType.ERROR:
                                    raise ws.exception() or RuntimeError("bybit ws error")
                                elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED):
                                    break
                        finally:
                            ping_task.cancel()
                            await asyncio.gather(ping_task, return_exceptions=True)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                qlog(f"[WARN] bybit ws reinicio ({exc})")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
            else:
                await asyncio.sleep(1)

    async def _ping(self, ws: aiohttp.ClientWebSocketResponse, interval: float = 15.0):
        while not self._stop.is_set():
            await asyncio.sleep(interval)
            try:
                await ws.send_json({"op": "ping"})
            except Exception:
                return

    def _handle_control(self, ws: aiohttp.ClientWebSocketResponse, payload: Dict) -> bool:
        if not isinstance(payload, dict):
            return False
        if payload.get("op") == "pong":
            return True
        if payload.get("op") == "ping":
            try:
                self.loop.create_task(ws.send_json({"op": "pong"}))
            except Exception:
                pass
            return True
        if payload.get("type") in ("snapshot", "delta") and payload.get("topic", "").startswith("orderbook.1."):
            return False
        if payload.get("topic", "").startswith("tickers."):
            return False
        return False

    def _handle_message(self, payload: Dict):
        topic = payload.get("topic", "")
        data = payload.get("data")
        if not topic or data is None:
            return
        if topic.startswith("orderbook.1."):
            symbol_id = topic.split(".", 2)[-1]
            symbol = self._symbol_from_id(symbol_id)
            bids = data.get("b") or data.get("bid") or data.get("bids") or []
            asks = data.get("a") or data.get("ask") or data.get("asks") or []
            ts_raw = payload.get("ts") or data.get("ts") or now_ts_ms()
            entry = self.cache.setdefault(symbol, {"bid": None, "ask": None, "last": None, "quoteVolume": None, "timestamp": None})
            changed = False
            try:
                if bids:
                    bid_price = float(bids[0][0])
                    if entry.get("bid") != bid_price:
                        entry["bid"] = bid_price
                        changed = True
                if asks:
                    ask_price = float(asks[0][0])
                    if entry.get("ask") != ask_price:
                        entry["ask"] = ask_price
                        changed = True
            except (TypeError, ValueError):
                return
            try:
                ts_val = float(ts_raw)
            except (TypeError, ValueError):
                ts_val = float(now_ts_ms())
            if entry.get("timestamp") != ts_val:
                entry["timestamp"] = ts_val
                changed = True
            if changed:
                self.on_update(symbol, entry.copy())
        elif topic.startswith("tickers."):
            symbol_id = topic.split(".", 1)[-1]
            symbol = self._symbol_from_id(symbol_id)
            ts_raw = payload.get("ts") or data.get("ts") or now_ts_ms()
            entry = self.cache.setdefault(symbol, {"bid": None, "ask": None, "last": None, "quoteVolume": None, "timestamp": None})
            changed = False
            last = data.get("lastPrice") or data.get("last")
            qv = data.get("turnover24h") or data.get("quoteVolume24h")
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
            try:
                ts_val = float(ts_raw)
            except (TypeError, ValueError):
                ts_val = float(now_ts_ms())
            if entry.get("timestamp") != ts_val:
                entry["timestamp"] = ts_val
                changed = True
            if changed:
                self.on_update(symbol, entry.copy())

    @staticmethod
    def _symbol_from_id(symbol_id: str) -> str:
        if symbol_id.endswith("USDT"):
            return f"{symbol_id[:-4]}/USDT"
        if symbol_id.endswith("USDC"):
            return f"{symbol_id[:-4]}/USDC"
        if "/" in symbol_id:
            return symbol_id
        base = symbol_id[:-3]
        quote = symbol_id[-3:]
        return f"{base}/{quote}"
