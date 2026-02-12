"""
Hyperliquid Analytics Dashboard - Data Fetcher
- Dashboard stats, Markets, Funding rates
- Whale tracker (PnL top 200, Biggest positions)
- Position aggregation by coin (for chart)
- Liquidation map by price buckets (for chart)
"""

import json
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from collections import defaultdict
import math

INFO_URL = "https://api.hyperliquid.xyz/info"
LEADERBOARD_URL = "https://stats-data.hyperliquid.xyz/Mainnet/leaderboard"
TOP_N = 500
MAX_WORKERS = 10

def api_request(payload):
    try:
        r = requests.post(INFO_URL, json=payload, headers={"Content-Type": "application/json"}, timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"API Error: {e}")
        return None

def fetch_meta():
    print("📊 Fetching markets...")
    return api_request({"type": "metaAndAssetCtxs"})

def fetch_leaderboard():
    print("🏆 Fetching leaderboard...")
    try:
        r = requests.get(LEADERBOARD_URL, timeout=30)
        r.raise_for_status()
        return r.json().get('leaderboardRows', [])
    except Exception as e:
        print(f"Leaderboard error: {e}")
        return []

def fetch_positions(addr):
    try:
        data = api_request({"type": "clearinghouseState", "user": addr})
        if not data: return []
        positions = []
        for ap in data.get('assetPositions', []):
            p = ap.get('position', {})
            szi = float(p.get('szi', 0))
            if szi != 0:
                lev = p.get('leverage', {})
                lev_val = float(lev.get('value', 0)) if lev.get('value') else 0
                positions.append({
                    'coin': p.get('coin', ''),
                    'direction': 'Long' if szi > 0 else 'Short',
                    'size': abs(szi),
                    'entryPx': p.get('entryPx', '0'),
                    'leverage': lev_val,
                    'positionValue': float(p.get('positionValue', 0)),
                    'unrealizedPnl': float(p.get('unrealizedPnl', 0)),
                    'liquidationPx': p.get('liquidationPx', ''),
                })
        return positions
    except:
        return []

def process_markets(meta):
    if not meta or len(meta) < 2: return [], {}
    universe, ctxs = meta[0].get('universe', []), meta[1]
    markets, total_oi, total_vol = [], 0, 0
    
    for i, asset in enumerate(universe):
        if i >= len(ctxs): break
        ctx = ctxs[i]
        mark = float(ctx.get('markPx', 0))
        oi = float(ctx.get('openInterest', 0))
        vol = float(ctx.get('dayNtlVlm', 0))
        prev = float(ctx.get('prevDayPx', 0))
        chg = ((mark - prev) / prev * 100) if prev > 0 else 0
        oi_val = oi * mark
        total_oi += oi_val
        total_vol += vol
        markets.append({
            'name': asset.get('name', ''),
            'markPx': mark,
            'indexPx': float(ctx.get('oraclePx', mark)),
            'change24h': chg,
            'volume24h': vol,
            'openInterest': oi_val,
            'openInterestCoins': oi,
            'funding': float(ctx.get('funding', 0)) * 100,
            'maxLeverage': asset.get('maxLeverage', 0)
        })
    
    markets.sort(key=lambda x: x['openInterest'], reverse=True)
    return markets, {'totalOI': total_oi, 'volume24h': total_vol, 'activeMarkets': len(markets)}

def get_pnl(trader, period):
    for perf in trader.get('windowPerformances', []):
        if perf[0] == period:
            return {'pnl': float(perf[1].get('pnl', 0)), 'roi': float(perf[1].get('roi', 0)), 'volume': float(perf[1].get('vlm', 0))}
    return {'pnl': 0, 'roi': 0, 'volume': 0}

def process_traders_pnl(traders, period, n=TOP_N):
    result = []
    for t in traders:
        pnl = get_pnl(t, period)
        result.append({'address': t.get('ethAddress', ''), 'displayName': t.get('displayName'), 'accountValue': float(t.get('accountValue', 0)), **pnl})
    result.sort(key=lambda x: x['pnl'], reverse=True)
    return result[:n]

def process_traders_value(traders, n=TOP_N):
    result = [{'address': t.get('ethAddress', ''), 'displayName': t.get('displayName'), 'accountValue': float(t.get('accountValue', 0))} for t in traders]
    result.sort(key=lambda x: x['accountValue'], reverse=True)
    return result[:n]

def fetch_all_positions(addresses):
    pos_map = {}
    total = len(addresses)
    print(f"\n🐋 Fetching positions for {total} addresses...")
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch_positions, a): a for a in addresses}
        done = 0
        for f in as_completed(futures):
            addr = futures[f]
            done += 1
            try:
                pos_map[addr] = f.result()
            except:
                pos_map[addr] = []
            if done % 50 == 0:
                print(f"   └─ {done}/{total}")
            time.sleep(0.1)
    
    print(f"   └─ Done: {done}/{total}")
    return pos_map

def aggregate_positions(traders):
    agg = defaultdict(lambda: {'long': 0, 'short': 0, 'longSize': 0, 'shortSize': 0})
    for t in traders:
        for p in t.get('positions', []):
            c = p['coin']
            if p['direction'] == 'Long':
                agg[c]['long'] += p['positionValue']
                agg[c]['longSize'] += p['size']
            else:
                agg[c]['short'] += p['positionValue']
                agg[c]['shortSize'] += p['size']
    
    result = [{'coin': c, **d, 'total': d['long'] + d['short']} for c, d in agg.items()]
    result.sort(key=lambda x: x['total'], reverse=True)
    return result[:25]

def get_bucket_size(coin, price):
    buckets = {'BTC': 1000, 'ETH': 50, 'SOL': 5, 'BNB': 10}
    if coin in buckets: return buckets[coin]
    if price >= 10000: return 500
    if price >= 1000: return 100
    if price >= 100: return 10
    if price >= 10: return 1
    if price >= 1: return 0.1
    return 0.01

def get_lev_cat(lev):
    if lev >= 100: return '100x'
    if lev >= 50: return '50x'
    if lev >= 25: return '25x'
    return '10x'

def build_liq_map(traders, markets_dict):
    print("🗺️  Building liquidation map...")
    liq_by_coin = defaultdict(list)
    
    for t in traders:
        for p in t.get('positions', []):
            liq = p.get('liquidationPx')
            if not liq: continue
            try:
                liq = float(liq)
                if liq <= 0 or liq > 1e12: continue
                liq_by_coin[p['coin']].append({**p, 'liquidationPx': liq, 'traderAddress': t.get('address', '')})
            except:
                continue
    
    result = {}
    for coin, positions in liq_by_coin.items():
        if len(positions) < 2: continue
        market = markets_dict.get(coin, {})
        curr = market.get('markPx', 0)
        if curr <= 0: continue
        
        bucket_size = get_bucket_size(coin, curr)
        min_p, max_p = curr * 0.5, curr * 1.5
        
        long_buckets = defaultdict(lambda: {'10x': 0, '25x': 0, '50x': 0, '100x': 0})
        short_buckets = defaultdict(lambda: {'10x': 0, '25x': 0, '50x': 0, '100x': 0})
        total_long, total_short = 0, 0
        
        for p in positions:
            liq = p['liquidationPx']
            if liq < min_p or liq > max_p: continue
            key = round(math.floor(liq / bucket_size) * bucket_size, 2)
            cat = get_lev_cat(p['leverage'])
            d = p['direction'].lower()
            
            if d == 'long':
                long_buckets[key][cat] += p['positionValue']
                total_long += p['positionValue']
            else:
                short_buckets[key][cat] += p['positionValue']
                total_short += p['positionValue']
        
        # Format for UI: longLiquidations and shortLiquidations arrays
        long_list = [{'price': k, '10x': v['10x'], '25x': v['25x'], '50x': v['50x'], '100x': v['100x']} 
                     for k, v in sorted(long_buckets.items()) if sum(v.values()) > 0]
        short_list = [{'price': k, '10x': v['10x'], '25x': v['25x'], '50x': v['50x'], '100x': v['100x']} 
                      for k, v in sorted(short_buckets.items()) if sum(v.values()) > 0]
        
        if long_list or short_list:
            result[coin] = {
                'currentPrice': curr,
                'longLiquidations': long_list,
                'shortLiquidations': short_list
            }
    
    print(f"   └─ {len(result)} coins mapped")
    return result

def get_biggest_positions(pos_map, trader_info, n=TOP_N):
    lookup = {t['address']: t for t in trader_info}
    all_pos = []
    for addr, positions in pos_map.items():
        info = lookup.get(addr, {})
        for p in positions:
            all_pos.append({**p, 'traderAddress': addr, 'traderName': info.get('displayName'), 'traderAccountValue': info.get('accountValue', 0)})
    all_pos.sort(key=lambda x: x.get('positionValue', 0), reverse=True)
    return all_pos[:n]

def get_liq_risks(pos_map, markets_dict):
    risks = []
    for addr, positions in pos_map.items():
        for p in positions:
            liq = p.get('liquidationPx')
            if not liq: continue
            try:
                liq = float(liq)
                if liq <= 0: continue
                curr = markets_dict.get(p['coin'], {}).get('markPx', 0)
                if curr <= 0: continue
                dist = ((curr - liq) / curr * 100) if p['direction'] == 'Long' else ((liq - curr) / curr * 100)
                if 0 < dist < 50:
                    risks.append({**p, 'currentPx': curr, 'liquidationPx': liq, 'distancePct': dist, 'traderAddress': addr})
            except:
                continue
    risks.sort(key=lambda x: x['distancePct'])
    return risks[:200]

def main():
    print("=" * 60)
    print("🚀 Hyperliquid Analytics - Data Fetcher")
    print("=" * 60)
    
    meta = fetch_meta()
    markets, stats = process_markets(meta)
    if not markets:
        print("❌ Failed to fetch markets")
        return
    
    markets_dict = {m['name']: m for m in markets}
    funding = sorted(markets[:50], key=lambda x: abs(x['funding']), reverse=True)
    
    traders = fetch_leaderboard()
    if not traders:
        print("❌ Failed to fetch leaderboard")
        return
    
    print("\n📋 Processing traders...")
    daily = process_traders_pnl(traders, 'day')
    weekly = process_traders_pnl(traders, 'week')
    by_value = process_traders_value(traders)
    
    # Collect unique addresses
    all_addr = set()
    addr_info = {}
    for t in daily + weekly + by_value:
        all_addr.add(t['address'])
        addr_info[t['address']] = t
    
    pos_map = fetch_all_positions(list(all_addr))
    
    # Attach positions
    for t in daily: t['positions'] = pos_map.get(t['address'], [])
    for t in weekly: t['positions'] = pos_map.get(t['address'], [])
    for t in by_value: t['positions'] = pos_map.get(t['address'], [])
    
    print("\n📊 Aggregating positions...")
    agg_daily = aggregate_positions(daily)
    agg_weekly = aggregate_positions(weekly)
    agg_size = aggregate_positions(by_value)
    
    liq_daily = build_liq_map(daily, markets_dict)
    liq_weekly = build_liq_map(weekly, markets_dict)
    liq_size = build_liq_map(by_value, markets_dict)
    
    biggest = get_biggest_positions(pos_map, list(addr_info.values()))
    liq_risks = get_liq_risks(pos_map, markets_dict)
    
    print(f"\n{'='*60}")
    print(f"📊 Total OI: ${stats['totalOI']:,.0f}")
    print(f"📈 24h Volume: ${stats['volume24h']:,.0f}")
    print(f"🏪 Markets: {stats['activeMarkets']}")
    print(f"👥 Traders: {len(traders)}")
    print("=" * 60)
    
    output = {
        'dashboard': {**stats, 'totalTraders': len(traders)},
        'markets': markets,
        'fundingRates': [{'coin': m['name'], 'rate': m['funding'], 'annualized': m['funding'] * 3 * 365, 'markPx': m['markPx'], 'openInterest': m['openInterest']} for m in funding],
        'liquidationRisks': liq_risks,
        'whaleTracker': {'daily': daily, 'weekly': weekly, 'byAccountValue': by_value, 'biggestPositions': biggest},
        'positionAggregation': {'byPnlDaily': agg_daily, 'byPnlWeekly': agg_weekly, 'bySize': agg_size},
        'liquidationMap': {'byPnlDaily': liq_daily, 'byPnlWeekly': liq_weekly, 'bySize': liq_size},
        'lastUpdated': datetime.utcnow().isoformat() + 'Z'
    }
    
    # Save to legacy location
    with open('whale_data.json', 'w') as f:
        json.dump(output, f)
    
    # Save to new multi-DEX structure
    import os
    os.makedirs('data', exist_ok=True)
    
    # Whale data (new format)
    whale_output = {
        'lastUpdated': datetime.utcnow().isoformat() + 'Z',
        'dashboard': {**stats, 'totalTraders': len(traders)},
        'whaleTracker': {'daily': daily, 'weekly': weekly, 'biggestPositions': biggest},
        'positionAggregation': {'byPnlDaily': agg_daily, 'byPnlWeekly': agg_weekly, 'bySize': agg_size}
    }
    
    with open('data/hyperliquid_whales.json', 'w') as f:
        json.dump(whale_output, f)
    
    # Liquidation data (new format)
    liq_output = {
        'lastUpdated': datetime.utcnow().isoformat() + 'Z',
        'tradersCount': len(traders),
        'coins': list(liq_daily.keys()) if liq_daily else [],
        'data': liq_daily
    }
    
    with open('data/hyperliquid_liq.json', 'w') as f:
        json.dump(liq_output, f)
    
    print(f"\n✅ Saved to whale_data.json ({os.path.getsize('whale_data.json') / 1024:.1f} KB)")
    print(f"✅ Saved to data/hyperliquid_whales.json")
    print(f"✅ Saved to data/hyperliquid_liq.json")

if __name__ == '__main__':
    main()
