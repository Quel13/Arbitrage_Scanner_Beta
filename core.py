# core.py
import os, math, time, asyncio, aiohttp
import pandas as pd

from .config import (BITGET_COINS_URL, BINANCE_CONFIG_URL, BYBIT_COIN_INFO_URL,
                     MEXC_CONFIG_URL)
from .utils import (normalize_symbol, quote_of, compute_spread_bps, volume_quote_est,
                    norm_chain, now_ts_ms, ts_iso, qlog)
from .io_exchanges import create_exchange, load_markets_safe, fetch_tickers_safe, fetch_order_book_once

# ---------- Ranking por exchange ----------
def rank_pairs_for_exchange(exchange_id: str, tickers: dict, quotes: set,
                            topk: int = 100, min_qv: float = 5e5, max_spread_bps: float = 50.0):
    rows = []
    for sym, t in tickers.items():
        nsym = normalize_symbol(sym)
        q = quote_of(nsym)
        if quotes and q not in quotes:
            continue
        bid, ask = t.get('bid'), t.get('ask')
        sp = compute_spread_bps(bid, ask)
        if sp is None or sp > max_spread_bps:
            continue
        qv = volume_quote_est(t)
        if qv is None or qv < min_qv:
            continue
        score = qv / max(sp, 1e-6)
        rows.append({
            'exchange': exchange_id, 'symbol': nsym, 'bid': bid, 'ask': ask,
            'spread_bps': sp, 'quote_volume': qv, 'score': score,
        })
    rows.sort(key=lambda r: r['score'], reverse=True)
    return rows[:topk]

# ---------- Verificación de redes (matriz) ----------
async def bitget_coin_info(session: aiohttp.ClientSession):
    try:
        async with session.get(BITGET_COINS_URL, timeout=15) as r:
            j = await r.json()
            data = j.get('data') or []
            out = {}
            for item in data:
                coin = (item.get('coin') or '').upper()
                lst = []
                for ch in (item.get('chains') or []):
                    lst.append({
                        'chain': norm_chain(ch.get('chain')),
                        'deposit': str(ch.get('rechargeable', '')).lower() == 'true',
                        'withdraw': str(ch.get('withdrawable', '')).lower() == 'true',
                        'fee': ch.get('withdrawFee'), 'min': ch.get('minWithdrawAmount'),
                        'raw': ch,
                    })
                out[coin] = lst
            return out
    except Exception as e:
        qlog(f"[WARN] bitget coins error: {e}"); return {}

async def binance_coin_info(session: aiohttp.ClientSession):
    key, secret = os.getenv('BINANCE_KEY'), os.getenv('BINANCE_SECRET')
    if not key or not secret: return {}
    try:
        async with session.get(BINANCE_CONFIG_URL, headers={"X-MBX-APIKEY": key}, timeout=15) as r:
            j = await r.json(); out = {}
            for item in j:
                coin = (item.get('coin') or '').upper()
                lst = []
                for ch in (item.get('networkList') or []):
                    lst.append({
                        'chain': norm_chain(ch.get('network')),
                        'deposit': bool(ch.get('depositEnable')),
                        'withdraw': bool(ch.get('withdrawEnable')),
                        'fee': ch.get('withdrawFee'), 'min': ch.get('withdrawMin'),
                        'raw': ch,
                    })
                out[coin] = lst
            return out
    except Exception as e:
        qlog(f"[WARN] binance config error: {e}"); return {}

async def bybit_coin_info(session: aiohttp.ClientSession):
    try:
        async with session.get(BYBIT_COIN_INFO_URL, timeout=15) as r:
            j = await r.json()
            rows = (j.get('result') or {}).get('rows') or []
            out = {}
            for row in rows:
                coin = (row.get('coin') or '').upper()
                lst = []
                for ch in (row.get('chains') or []):
                    lst.append({
                        'chain': norm_chain(ch.get('chain')),
                        'deposit': str(ch.get('chainDeposit', '1')) == '1',
                        'withdraw': str(ch.get('chainWithdraw', '1')) == '1',
                        'fee': ch.get('withdrawFee'), 'min': ch.get('withdrawMin'),
                        'raw': ch,
                    })
                out[coin] = lst
            return out
    except Exception as e:
        qlog(f"[WARN] bybit coin info error: {e}"); return {}

async def mexc_coin_info(session: aiohttp.ClientSession):
    key, secret = os.getenv('MEXC_KEY'), os.getenv('MEXC_SECRET')
    if not key or not secret: return {}
    try:
        async with session.get(MEXC_CONFIG_URL, headers={"X-MEXC-APIKEY": key}, timeout=15) as r:
            j = await r.json()
            data = j if isinstance(j, list) else (j.get('data') or [])
            out = {}
            for item in data:
                coin = (item.get('coin') or item.get('currency') or '').upper()
                nets = item.get('networkList') or item.get('networks') or []
                lst = []
                for ch in nets:
                    name = ch.get('network') or ch.get('netWork')
                    lst.append({
                        'chain': norm_chain(name),
                        'deposit': bool(ch.get('depositEnable')),
                        'withdraw': bool(ch.get('withdrawEnable')),
                        'fee': ch.get('withdrawFee'), 'min': ch.get('withdrawMin'),
                        'raw': ch,
                    })
                out[coin] = lst
            return out
    except Exception as e:
        qlog(f"[WARN] mexc config error: {e}"); return {}

async def fetch_chain_matrix(exchanges: list):
    matrix = {ex: {} for ex in exchanges}
    async with aiohttp.ClientSession() as session:
        if 'bitget' in exchanges:  matrix['bitget']  = await bitget_coin_info(session)
        if 'binance' in exchanges: matrix['binance'] = await binance_coin_info(session)
        if 'bybit' in exchanges:   matrix['bybit']   = await bybit_coin_info(session)
        if 'mexc' in exchanges:    matrix['mexc']    = await mexc_coin_info(session)
    return matrix

def pick_viable_chain(asset_base: str, src: str, dst: str, chain_matrix: dict):
    asset = asset_base.upper()
    src_map = chain_matrix.get(src.lower()) or {}
    dst_map = chain_matrix.get(dst.lower()) or {}
    if not src_map and not dst_map:
        return "DESCONOCIDO", ""
    src_wd = {it['chain'] for it in (src_map.get(asset, []) or []) if it.get('withdraw')}
    dst_dp = {it['chain'] for it in (dst_map.get(asset, []) or []) if it.get('deposit')}
    commons = src_wd.intersection(dst_dp)
    if commons:
        return "OK", sorted(list(commons))[0]
    return "DESCONOCIDO", ""

# ---------- Depth y tamaño ejecutable ----------
def consume_depth(ob, side: str, price_cap: float, max_usdt: float = 1e9):
    if not ob or side not in ob: return 0.0
    total = 0.0
    for px, sz in ob[side]:
        if side == 'asks' and px > price_cap: break
        if side == 'bids' and px < price_cap: break
        total += px * sz
        if total >= max_usdt: return max_usdt
    return total

_last_depth = {}
def _cache_ok(key):  # 10s cache
    ts, _ = _last_depth.get(key, (0, 0.0))
    return (time.time() - ts) < 10

def estimate_executable_usdt(symbol: str, buy_ex: str, sell_ex: str, bps_window: float = 5.0):
    key = (symbol, buy_ex, sell_ex)
    if _cache_ok(key):
        return _last_depth[key][1]

    async def _go():
        ob_buy  = await fetch_order_book_once(buy_ex, symbol, limit=20)
        ob_sell = await fetch_order_book_once(sell_ex, symbol, limit=20)
        if not ob_buy or not ob_sell: return 0.0
        try:
            best_ask = ob_buy['asks'][0][0]
            best_bid = ob_sell['bids'][0][0]
            mid = 0.5*(best_ask + best_bid)
            cap_buy  = mid * (1 + bps_window/1e4)
            cap_sell = mid * (1 - bps_window/1e4)
            usdt_buy  = consume_depth(ob_buy,  'asks', cap_buy)
            usdt_sell = consume_depth(ob_sell, 'bids', cap_sell)
            return max(0.0, min(usdt_buy, usdt_sell))
        except Exception:
            return 0.0

    try:
        est = asyncio.run(_go())
    except RuntimeError:
        est = 0.0
    _last_depth[key] = (time.time(), est)
    return est

# ---------- Oportunidades ----------
_first_seen = {}  # (symbol, buy_ex, sell_ex) -> ts_ms

def compute_opportunities(ranked_rows, taker_fees_bps: dict, slippage_bps: float = 2.0,
                          min_net_bps: float = 5.0, chain_matrix: dict | None = None):
    by_symbol = {}
    for r in ranked_rows:
        by_symbol.setdefault(r['symbol'], []).append(r)

    opps, ts_now = [], now_ts_ms()
    for sym, rows in by_symbol.items():
        if len(rows) < 2: continue
        best_ask = min(rows, key=lambda r: (r['ask'] if r['ask'] else math.inf))
        best_bid = max(rows, key=lambda r: (r['bid'] if r['bid'] else -math.inf))
        if not best_ask['ask'] or not best_bid['bid']: continue
        if best_ask['exchange'] == best_bid['exchange']: continue
        if best_bid['bid'] <= best_ask['ask']: continue

        mid = 0.5 * (best_bid['bid'] + best_ask['ask'])
        gross_bps = (best_bid['bid'] - best_ask['ask']) / mid * 1e4
        fee_buy = taker_fees_bps.get(best_ask['exchange'], 10.0)
        fee_sell = taker_fees_bps.get(best_bid['exchange'], 10.0)
        net_bps = gross_bps - fee_buy - fee_sell - slippage_bps
        if net_bps < min_net_bps: continue

        key = (sym, best_ask['exchange'], best_bid['exchange'])
        first = _first_seen.get(key)
        if first is None:
            _first_seen[key] = ts_now
            first = ts_now
        active_sec = max(0, (now_ts_ms() - first) // 1000)
        est_size = estimate_executable_usdt(sym, best_ask['exchange'], best_bid['exchange'])

        base = sym.split('/')[0]
        chain_status, best_chain = ("DESCONOCIDO", "")
        if chain_matrix is not None:
            chain_status, best_chain = pick_viable_chain(base, best_ask['exchange'], best_bid['exchange'], chain_matrix)

        opps.append({
            'symbol': sym, 'buy_ex': best_ask['exchange'], 'sell_ex': best_bid['exchange'],
            'gross_bps': gross_bps, 'net_bps': net_bps,
            'buy_qv': best_ask['quote_volume'], 'sell_qv': best_bid['quote_volume'],
            'active_sec': active_sec, 'chain_status': chain_status, 'best_chain': best_chain,
            'est_usdt': est_size, 'ts_iso': ts_iso(now_ts_ms()),
        })
    opps.sort(key=lambda o: o['net_bps'], reverse=True)
    return opps
