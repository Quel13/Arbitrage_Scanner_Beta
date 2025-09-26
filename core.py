# core.py
import math, time, asyncio

from .utils import (
    normalize_symbol,
    quote_of,
    compute_spread_bps,
    volume_quote_est,
    now_ts_ms,
    ts_iso,
)
from .io_exchanges import fetch_order_book_once


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
            'stale': False,
        })
    rows.sort(key=lambda r: r['score'], reverse=True)
    return rows[:topk]

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


async def estimate_executable_usdt(symbol: str, buy_ex: str, sell_ex: str,
                                   bps_window: float = 5.0) -> float:
    key = (symbol, buy_ex, sell_ex)
    if _cache_ok(key):
        return _last_depth[key][1]

    try:
        ob_buy, ob_sell = await asyncio.gather(
            fetch_order_book_once(buy_ex, symbol, limit=20),
            fetch_order_book_once(sell_ex, symbol, limit=20),
        )
        if not ob_buy or not ob_sell:
            est = 0.0
        else:
            try:
                best_ask = ob_buy['asks'][0][0]
                best_bid = ob_sell['bids'][0][0]
                mid = 0.5 * (best_ask + best_bid)
                cap_buy = mid * (1 + bps_window / 1e4)
                cap_sell = mid * (1 - bps_window / 1e4)
                usdt_buy = consume_depth(ob_buy, 'asks', cap_buy)
                usdt_sell = consume_depth(ob_sell, 'bids', cap_sell)
                est = max(0.0, min(usdt_buy, usdt_sell))
            except Exception:
                est = 0.0
    except Exception:
        est = 0.0

    _last_depth[key] = (time.time(), est)
    return est

# ---------- Oportunidades ----------
_first_seen = {}  # (symbol, buy_ex, sell_ex) -> ts_ms


async def compute_opportunities(
    ranked_rows,
    taker_fees_bps: dict,
    slippage_bps: float = 2.0,
    min_net_bps: float = 5.0,
    max_paths_per_symbol: int = 3,
):
    by_symbol = {}
    for r in ranked_rows:
        by_symbol.setdefault(r['symbol'], []).append(r)

    ts_now = now_ts_ms()
    combos = []
    for sym, rows in by_symbol.items():
        if len(rows) < 2:
            continue
        buys = [r for r in rows if r.get('ask') is not None and not r.get('stale')]
        sells = [r for r in rows if r.get('bid') is not None and not r.get('stale')]
        if not buys or not sells:
            continue
        buys.sort(key=lambda r: r['ask'] if r['ask'] is not None else math.inf)
        sells.sort(key=lambda r: r['bid'] if r['bid'] is not None else -math.inf, reverse=True)

        for buy in buys[:max_paths_per_symbol]:
            ask = buy.get('ask')
            if ask is None:
                continue
            for sell in sells[:max_paths_per_symbol]:
                bid = sell.get('bid')
                if bid is None or bid <= ask:
                    continue
                if buy['exchange'] == sell['exchange']:
                    continue

                mid = 0.5 * (bid + ask)
                if mid <= 0:
                    continue
                gross_bps = (bid - ask) / mid * 1e4
                fee_buy = taker_fees_bps.get(buy['exchange'], 10.0)
                fee_sell = taker_fees_bps.get(sell['exchange'], 10.0)
                net_bps = gross_bps - fee_buy - fee_sell - slippage_bps
                if net_bps < min_net_bps:
                    continue

                key = (sym, buy['exchange'], sell['exchange'])
                first_ts = _first_seen.get(key)
                if first_ts is None:
                    _first_seen[key] = ts_now
                    first_ts = ts_now

                combos.append({
                    'symbol': sym,
                    'buy': buy,
                    'sell': sell,
                    'gross_bps': gross_bps,
                    'net_bps': net_bps,
                    'key': key,
                    'first_ts': first_ts,
                })

    depth_tasks = [
        estimate_executable_usdt(c['symbol'], c['buy']['exchange'], c['sell']['exchange'])
        for c in combos
    ]
    if depth_tasks:
        depth_results = await asyncio.gather(*depth_tasks, return_exceptions=True)
    else:
        depth_results = []

    opps = []
    now_ms = now_ts_ms()
    now_iso = ts_iso(now_ms)
    for combo, depth in zip(combos, depth_results):
        est_size = 0.0
        if not isinstance(depth, Exception) and depth is not None:
            try:
                est_size = float(depth)
            except (TypeError, ValueError):
                est_size = 0.0

        active_sec = max(0, (now_ms - combo['first_ts']) // 1000)
        expected_usdt = est_size * combo['net_bps'] / 1e4
        volume_factor = math.log10(1.0 + max(est_size, 0.0) / 1000.0)
        edge_score = combo['net_bps'] * (1.0 + volume_factor)

        opps.append({
            'symbol': combo['symbol'],
            'buy_ex': combo['buy']['exchange'],
            'sell_ex': combo['sell']['exchange'],
            'buy_price': combo['buy'].get('ask'),
            'sell_price': combo['sell'].get('bid'),
            'gross_bps': combo['gross_bps'],
            'net_bps': combo['net_bps'],
            'buy_qv': combo['buy']['quote_volume'],
            'sell_qv': combo['sell']['quote_volume'],
            'active_sec': active_sec,
            'est_usdt': est_size,
            'expected_usdt': expected_usdt,
            'edge_score': edge_score,
            'ts_iso': now_iso,
        })

    opps.sort(key=lambda o: (o['edge_score'], o['net_bps']), reverse=True)
    return opps
