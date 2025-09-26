import asyncio
import json
from typing import Callable, Dict, List

import aiohttp

from utils import now_ts_ms, qlog


class ExchangeWS:
    ENDPOINT = "wss://ws.bitget.com/spot/v1/stream"
    CHUNK = 60

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
        chunks = [self.symbols[i:i + self.CHUNK] for i in range(0, len(self.symbols), self.CHUNK)]
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
        args = []
        for sym in chunk:
            symbol_id = sym.replace("/", "")
            args.append({"instType": "SP", "channel": "ticker", "instId": symbol_id})
            args.append({"instType": "SP", "channel": "books1", "instId": symbol_id})
        backoff = 1.0
        while not self._stop.is_set():
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(self.ENDPOINT, heartbeat=20) as ws:
                        await ws.send_json({"op": "subscribe", "args": args})
                        qlog(f"[bitget] WS conectado ({len(chunk)} símbolos)")
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
                                    raise ws.exception() or RuntimeError("bitget ws error")
                                elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED):
                                    break
                        finally:
                            ping_task.cancel()
                            await asyncio.gather(ping_task, return_exceptions=True)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                qlog(f"[WARN] bitget ws reinicio ({exc})")
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
        if payload.get("event") == "pong" or payload.get("op") == "pong":
            return True
        if payload.get("event") == "ping":
            try:
                self.loop.create_task(ws.send_json({"event": "pong"}))
            except Exception:
                pass
            return True
        if payload.get("op") == "ping":
            try:
                self.loop.create_task(ws.send_json({"op": "pong"}))
            except Exception:
                pass
            return True
        if payload.get("event") in ("subscribe", "error"):
            return True
        return False

    def _handle_message(self, payload: Dict):
        arg = payload.get("arg", {})
        data = payload.get("data")
        if not arg or data is None:
            return
        channel = arg.get("channel")
        symbol_id = arg.get("instId")
        if not channel or not symbol_id:
            return
        symbol = self._symbol_from_id(symbol_id)
        ts = payload.get("ts")
        if isinstance(data, list) and data:
            data = data[0]
        entry = self.cache.setdefault(symbol, {"bid": None, "ask": None, "last": None, "quoteVolume": None, "timestamp": None})
        changed = False
        if channel == "books1":
            bids = data.get("bids") or data.get("bid")
            asks = data.get("asks") or data.get("ask")
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
            ts_raw = ts or data.get("ts") or now_ts_ms()
            try:
                ts_val = float(ts_raw)
            except (TypeError, ValueError):
                ts_val = float(now_ts_ms())
            if entry.get("timestamp") != ts_val:
                entry["timestamp"] = ts_val
                changed = True
            if changed:
                self.on_update(symbol, entry.copy())
        elif channel == "ticker":
            last = data.get("last") or data.get("close")
            qv = data.get("quoteVol24h") or data.get("turnover24h") or data.get("quoteVolume")
            ts_raw = data.get("ts") or ts or now_ts_ms()
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
