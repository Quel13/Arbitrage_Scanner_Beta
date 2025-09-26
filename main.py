# main.py
import argparse, asyncio, threading, queue, time
import pandas as pd

try:
    import tkinter as tk
    from tkinter import ttk
except Exception:
    tk = None
    ttk = None

from .config import DEFAULT_EXCHANGES, DEFAULT_WS_TOP, TAKER_FEES_BPS
from .utils import qlog, log_queue
from .io_exchanges import (
    initialize_data_hub,
    shutdown_data_hub,
    create_exchange,
    load_markets_safe,
    fetch_tickers_safe,
    sync_rankings_with_ws,
)
from .core import rank_pairs_for_exchange, compute_opportunities

# -------- GUI (dos ventanas) --------
class DualWindows:
    def __init__(self, title_prefix="Arb Screener"):
        if tk is None: raise RuntimeError("tkinter no disponible en este entorno")
        self.root = tk.Tk(); self.root.withdraw()

        self.win_log = tk.Toplevel(); self.win_log.title(f"{title_prefix} — LOG")
        self.txt = tk.Text(self.win_log, height=30, width=110); self.txt.pack(fill='both', expand=True)

        self.max_log_lines = 400
        self.trim_interval_ms = 60_000

        self.win_tab = tk.Toplevel(); self.win_tab.title(f"{title_prefix} — SCREENER")
        cols = (
            "PAR",
            "EXC COMPRA",
            "EXC VENTA",
            "BUY PRICE",
            "SELL PRICE",
            "BENEFICIO (%)",
            "VOLUMEN (USDT)",
            "TIEMPO ACTIVO (s)",
            "TAMAÑO ESTIMADO (USDT)",
        )
        self.tree = ttk.Treeview(self.win_tab, columns=cols, show='headings', height=20)
        for c in cols:
            self.tree.heading(c, text=c)
            self.tree.column(c, anchor='center', width=160)
        self.tree.pack(fill='both', expand=True)

        self.root.after(300, self._drain_logs)
        self.root.after(self.trim_interval_ms, self._trim_log)
        self.latest_rows_keyed = {}

    def _drain_logs(self):
        try:
            while True:
                line = log_queue.get_nowait()
                self.txt.insert('end', line + "\n")
                self.txt.see('end')
        except queue.Empty:
            pass
        self.root.after(300, self._drain_logs)

    def _trim_log(self):
        try:
            end_index = self.txt.index('end-1c')
            total_lines = int(end_index.split('.')[0]) if end_index else 0
            if total_lines > self.max_log_lines:
                cutoff = total_lines - self.max_log_lines + 1
                self.txt.delete('1.0', f'{cutoff}.0')
        except Exception:
            pass
        self.root.after(self.trim_interval_ms, self._trim_log)

    def update_table(self, opps: list):
        keyset = set()
        for o in opps[:100]:
            key = (o['symbol'], o['buy_ex'], o['sell_ex'])
            keyset.add(key)
            values = (
                o['symbol'],
                o['buy_ex'],
                o['sell_ex'],
                f"{o.get('buy_price') or 0:.6f}",
                f"{o.get('sell_price') or 0:.6f}",
                f"{o['net_bps']/100:.4f}",
                f"{int(o['buy_qv'] or 0):,}",
                str(o['active_sec']),
                f"{int(o.get('est_usdt') or 0):,}"
            )
            iid = self.latest_rows_keyed.get(key)
            if iid is None:
                self.latest_rows_keyed[key] = self.tree.insert('', 'end', values=values)
            else:
                self.tree.item(iid, values=values)

        # remover filas que ya no están
        for k in [k for k in self.latest_rows_keyed if k not in keyset]:
            try: self.tree.delete(self.latest_rows_keyed[k])
            except Exception: pass
            self.latest_rows_keyed.pop(k, None)

    def run(self):
        self.root.deiconify()
        self.root.mainloop()

# -------- pipeline principal --------
async def build_snapshot(exchanges: list, quotes: set, topk: int, min_qv: float,
                         max_spread_bps: float):
    qlog("[INFO] Iniciando snapshot de mercados…")
    instances = {}
    for ex_id in exchanges:
        try:
            qlog(f"[INFO] Preparando exchange: {ex_id}")
            instances[ex_id] = await create_exchange(ex_id)
        except Exception as e:
            qlog(f"[ERROR] creando exchange {ex_id}: {e}")
    ranked_all = []
    for ex_id, ex in instances.items():
        try:
            qlog(f"[INFO] Cargando mercados en {ex_id}…")
            markets = await load_markets_safe(ex)
            if not markets: continue
            qlog(f"[INFO] Descargando tickers en {ex_id}… (puede tardar)")
            tickers = await fetch_tickers_safe(ex)
            ranked = rank_pairs_for_exchange(ex_id, tickers, quotes, topk, min_qv, max_spread_bps)
            ranked_all.extend(ranked)
            qlog(f"[{ex_id}] candidatos: {len(ranked)}")
        except Exception as e:
            qlog(f"[WARN] fallo en {ex_id}: {e}")
        finally:
            try: await ex.close()
            except Exception: pass
    if not ranked_all:
        qlog("[INFO] Snapshot completado sin candidatos.")
        return []

    ranked_all = await sync_rankings_with_ws(ranked_all)
    qlog(f"[INFO] Snapshot completado con {len(ranked_all)} filas tras datos en vivo.")
    return ranked_all

async def main_async(args, ui: DualWindows | None = None):
    exchanges = args.exchanges or DEFAULT_EXCHANGES
    quotes = set([q.upper() for q in args.quotes]) if args.quotes else {"USDT"}
    topk = args.topk
    min_qv = args.min_quote_vol
    max_spread_bps = args.max_spread_bps
    min_net_bps = args.min_net_bps
    slippage_bps = args.slippage_bps
    refresh = args.refresh

    qlog(f"Exchanges: {exchanges}")
    qlog(f"Quotes: {quotes}")
    qlog(f"Top-K por exchange: {topk} | min_qv: {min_qv} | max_spread_bps: {max_spread_bps}")
    qlog(f"Filtros: min_net_bps: {min_net_bps} | slippage_bps: {slippage_bps}")

    loop = asyncio.get_running_loop()
    await initialize_data_hub(loop, exchanges, quotes, args.ws_top)

    try:
        while True:
            t0 = time.time()
            qlog("===== NUEVA PASADA =====")
            ranked = await build_snapshot(
                exchanges,
                quotes,
                topk,
                min_qv,
                max_spread_bps,
            )
            if not ranked:
                qlog("Sin candidatos tras ranking. Revisa filtros o conectividad.")
                if refresh <= 0: break
                await asyncio.sleep(max(0, refresh)); continue

            from .config import TAKER_FEES_BPS
            from .core import compute_opportunities
            opps = await compute_opportunities(ranked, TAKER_FEES_BPS, slippage_bps, min_net_bps)

            if ui is not None:
                ui.update_table(opps)
            else:
                df_opp = pd.DataFrame(opps)
                qlog("=== TOP OPORTUNIDADES (net_bps) ===")
                qlog(df_opp.head(args.top_show).to_string(index=False) if not df_opp.empty else "No hay oportunidades que pasen el umbral.")

            if args.save_csv:
                from datetime import datetime
                ts = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
                pd.DataFrame(ranked).to_csv(f"ranked_{ts}.csv", index=False)
                pd.DataFrame(opps).to_csv(f"opps_{ts}.csv", index=False)
                qlog(f"Guardados: ranked_{ts}.csv, opps_{ts}.csv")

            dt = time.time() - t0
            if refresh <= 0: break
            await asyncio.sleep(max(0.0, refresh - dt))
    finally:
        await shutdown_data_hub()

# -------- CLI / arranque --------
def parse_args():
    p = argparse.ArgumentParser(description="Screener de arbitraje spot inter-exchange (REST + GUI)")
    p.add_argument('--exchanges', nargs='*', default=DEFAULT_EXCHANGES, help='Lista de exchanges CCXT')
    p.add_argument('--quotes', nargs='*', default=['USDT'], help='Quotes (USDT, USDC, USD, EUR...)')
    p.add_argument('--topk', type=int, default=100, help='Top-K por exchange para universo')
    p.add_argument('--min-quote-vol', type=float, default=5e5, help='Volumen mínimo 24h en quote')
    p.add_argument('--max-spread-bps', type=float, default=50.0, help='Spread máximo en bps')
    p.add_argument('--min-net-bps', type=float, default=5.0, help='Umbral de edge neto en bps')
    p.add_argument('--slippage-bps', type=float, default=2.0, help='Slippage estimado (bps)')
    p.add_argument('--ws-top', type=int, default=DEFAULT_WS_TOP,
                   help='Máximo de pares totales para abrir websockets (top-M global)')
    p.add_argument('--top-show', type=int, default=20, help='Filas a mostrar en modo texto')
    p.add_argument('--save-csv', action='store_true', help='Guardar CSV de ranked y opps')
    p.add_argument('--refresh', type=float, default=0.0, help='Segundos entre pasadas (0 = solo una)')
    p.add_argument('--no-gui', action='store_true', help='Desactiva ventanas GUI, usa solo consola')
    return p.parse_args()

def run_with_gui(args):
    ui = DualWindows()
    def worker():
        try:
            asyncio.run(main_async(args, ui))
        except Exception as e:
            qlog(f"[FATAL] {e}")
    th = threading.Thread(target=worker, daemon=True)
    th.start()
    ui.run()

if __name__ == '__main__':
    args = parse_args()
    try:
        if tk is not None and not args.no_gui:
            run_with_gui(args)
        else:
            asyncio.run(main_async(args, ui=None))
    except KeyboardInterrupt:
        print("Interrumpido por usuario.")
