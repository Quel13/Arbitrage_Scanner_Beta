# feeds/mexc_ws.py
from __future__ import annotations
import threading, time, json, queue
from typing import Callable, Dict, List
from datetime import datetime, timezone

from websocket import WebSocketApp, ABNF
from utils import now_ts_ms, qlog

# ==================== CONFIG WS ====================
WS_URL = "wss://wbs-api.mexc.com/ws"
MAX_SUBS_PER_CONN = 30
CHANNEL_FMT = "spot@public.aggre.bookTicker.v3.api.pb@100ms@{sid}"
CHUNK_SYMS = 12  # 12 símbolos → 12 subs (margen <30)
# ===================================================

# ==================== PARSER (TU LÓGICA) ====================
# Basado en tu fichero mexc_ws_btcusdt_pb.py, adaptado a función:
from google.protobuf.internal.decoder import _DecodeVarint  # requiere 'protobuf' instalado

def _parse_bookticker_from_wrapper(buf: bytes):
    """
    Tu decodificador mínimo del wrapper: localiza el submensaje con bidprice/askprice
    y devuelve (bid, ask) como float, o (None, None) si no pudo parsear.
    """
    i = 0
    while i < len(buf):
        tag, new_i = _DecodeVarint(buf, i)
        i = new_i
        wire_type = tag & 0x7
        if wire_type == 2:  # length-delimited
            length, new_i = _DecodeVarint(buf, i)
            i = new_i
            field_bytes = buf[i:i+length]
            i += length
            try:
                j = 0
                prices = []
                while j < len(field_bytes):
                    sub_tag, j2 = _DecodeVarint(field_bytes, j)
                    j = j2
                    sub_wire = sub_tag & 0x7
                    if sub_wire == 2:
                        slen, j3 = _DecodeVarint(field_bytes, j)
                        j = j3
                        sval = field_bytes[j:j+slen]
                        j += slen
                        try:
                            s = sval.decode("utf-8")
                            if all(c.isdigit() or c == '.' for c in s) and s.count('.') <= 1:
                                prices.append(s)
                                if len(prices) >= 2:
                                    return float(prices[0]), float(prices[1])
                        except Exception:
                            pass
                    elif sub_wire == 0:
                        _, j = _DecodeVarint(field_bytes, j)
                    elif sub_wire == 5:
                        j += 4
                    elif sub_wire == 1:
                        j += 8
                    else:
                        break
            except Exception:
                continue
        else:
            if wire_type == 0:
                _, i = _DecodeVarint(buf, i)
            elif wire_type == 1:
                i += 8
            elif wire_type == 5:
                i += 4
            else:
                break
    return None, None

def parse_mexc_bookticker_frame(frame: bytes) -> tuple[float | None, float | None, int | None]:
    """
    Entrada: frame binario del WS (Protobuf envuelto).
    Salida: (bid, ask, ts_ms). Si no hay timestamp en tu parser, devolvemos None.
    """
    bid, ask = _parse_bookticker_from_wrapper(frame)
    ts_ms = None
    return bid, ask, ts_ms
# ================= FIN PARSER (TU LÓGICA) ====================

def _sid(symbol: str) -> str:
    return symbol.replace("/", "").upper()

def _fmt_iso(ts):
    try:
        if ts and ts > 1e12: ts /= 1000.0
        return datetime.fromtimestamp(ts or time.time(), tz=timezone.utc).isoformat()
    except Exception:
        return "-"

class _ConnThread(threading.Thread):
    def __init__(self, symbols: List[str], out_q: "queue.Queue[tuple[str,dict]]"):
        super().__init__(daemon=True)
        self.symbols = symbols
        self.out_q = out_q
        self._stop = threading.Event()
        self._ws: WebSocketApp | None = None

    def run(self):
        params = [CHANNEL_FMT.format(sid=_sid(s)) for s in self.symbols]
        sub_msg = json.dumps({"method": "SUBSCRIPTION", "params": params})
        ping_payload = json.dumps({"method": "PING"})

        def on_open(ws: WebSocketApp):
            try:
                ws.send(sub_msg)
                qlog(f"[mexc] suscrito {len(params)} canales ({', '.join(self.symbols)})")
            except Exception as e:
                qlog(f"[mexc] error al suscribir: {e}")

        def on_message(ws: WebSocketApp, message: str):
            # ACK / PONG en texto
            # qlog(f"[mexc] TEXT {message[:160]}")
            pass

        def on_error(ws: WebSocketApp, error: Exception):
            qlog(f"[mexc] error WS: {error}")

        def on_close(ws: WebSocketApp, code, reason):
            qlog(f"[mexc] cerrado WS: code={code} reason={reason}")

        def on_data(ws: WebSocketApp, frame: bytes, data_type: int, cont: bool):
            if data_type != ABNF.OPCODE_BINARY:
                return
            try:
                bid, ask, ts_ms = parse_mexc_bookticker_frame(frame)
            except Exception as e:
                qlog(f"[mexc] parse fail: {e}")
                return

            if bid is None and ask is None:
                return

            ts = float(ts_ms or now_ts_ms())
            for s in self.symbols:
                tick = {"bid": bid, "ask": ask, "last": None, "quoteVolume": None, "timestamp": ts}
                self.out_q.put((s, tick))

        while not self._stop.is_set():
            try:
                self._ws = WebSocketApp(
                    WS_URL,
                    on_open=on_open,
                    on_message=on_message,
                    on_error=on_error,
                    on_close=on_close,
                    on_data=on_data,
                )
                self._ws.run_forever(
                    ping_interval=20,
                    ping_payload=ping_payload,
                    reconnect=5,
                    skip_utf8_validation=True,
                )
            except Exception as e:
                qlog(f"[mexc] run_forever error: {e}")
                time.sleep(2)

    def stop(self):
        self._stop.set()
        try:
            if self._ws: self._ws.close()
        except Exception:
            pass

class ExchangeWS:
    """Adaptador usado por io_exchanges.DataHub"""
    ENDPOINT = WS_URL
    CHUNK = CHUNK_SYMS

    def __init__(self, loop, symbols: List[str], on_update: Callable[[str, Dict], None]):
        self.loop = loop
        self.symbols = sorted(set(symbols))
        self.on_update = on_update
        self.cache: Dict[str, Dict] = {}
        self._threads: List[_ConnThread] = []
        self._q: "queue.Queue[tuple[str,dict]]" = queue.Queue()
        self._pump_task = None
        self._stopping = False

    async def start(self):
        if not self.symbols:
            return
        chunks = [self.symbols[i:i + self.CHUNK] for i in range(0, len(self.symbols), self.CHUNK)]
        for ch in chunks:
            t = _ConnThread(ch, self._q)
            t.start()
            self._threads.append(t)
        self._pump_task = self.loop.create_task(self._pump())

    async def stop(self):
        self._stopping = True
        for t in self._threads:
            t.stop()
        self._threads.clear()
        if self._pump_task:
            self._pump_task.cancel()
            try:
                await self._pump_task
            except Exception:
                pass
            self._pump_task = None

    async def _pump(self):
        while not self._stopping:
            try:
                symbol, tick = await self.loop.run_in_executor(None, self._q.get)
                self.cache[symbol] = tick
                self.on_update(symbol, tick)
            except Exception:
                await asyncio.sleep(0.05)
