"""
Hyperliquid Analytics Dashboard - Complete Data Fetcher
Sections: Dashboard, Markets & OI, Funding Rates, Liquidation Risk, Whale Tracker
"""

import json
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

INFO_URL = "https://api.hyperliquid.xyz/info"
LEADERBOARD_URL = "https://stats-data.hyperliquid.xyz/Mainnet/leaderboard"
DATA_FILE = "data.json"

TOP_N_TRADERS = 200
MAX_WORKERS = 10
REQUEST_DELAY = 0.1


def api_request(payload):
    """Make API request to Hyperliquid"""
    try:
        response = requests.post(
            INFO_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"API Error: {e}")
        return None


def fetch_meta_and_contexts():
    """Fetch market metadata and asset contexts"""
    print("Fetching market metadata...")
    return api_request({"type": "metaAndAssetCtxs"})


def fetch_leaderboard():
    """Fetch leaderboard data"""
    print("Fetching leaderboard...")
    try:
        response = requests.get(LEADERBOARD_URL, timeout=30)
        response.raise_for_status()
        data = response.json()
        traders = data.get('leaderboardRows', [])
        print(f"  Received {len(traders)} traders")
        return traders
    except Exception as e:
        print(f"Leaderboard error: {e}")
        return []


def fetch_positions(address):
    """Fetch positions for a wallet"""
    try:
        data = api_request({
            "type": "clearinghouseState",
            "user": address
        })
        
        if not data:
            return []
        
        positions = []
        for asset_pos in data.get('assetPositions', []):
            pos = asset_pos.get('position', {})
            if pos:
                szi = float(pos.get('szi', 0))
                if szi != 0:
                    leverage_data = pos.get('leverage', {})
                    positions.append({
                        'coin': pos.get('coin', ''),
                        'direction': 'Long' if szi > 0 else 'Short',
                        'size': abs(szi),
                        'entryPx': pos.get('entryPx', '0'),
                        'leverage': leverage_data.get('value', 0),
                        'leverageType': leverage_data.get('type', 'cross'),
                        'positionValue': float(pos.get('positionValue', 0)),
                        'unrealizedPnl': pos.get('unrealizedPnl', '0'),
                        'returnOnEquity': pos.get('returnOnEquity', '0'),
                        'liquidationPx': pos.get('liquidationPx', ''),
                        'marginUsed': pos.get('marginUsed', '0'),
                        'traderAddress': address
                    })
        return positions
    except Exception as e:
        return []


def process_markets_data(meta_data):
    """Process market data for Markets & OI section"""
    print("Processing market data...")
    
    if not meta_data or len(meta_data) < 2:
        return [], {}
    
    universe = meta_data[0].get('universe', [])
    asset_ctxs = meta_data[1]
    
    markets = []
    total_oi = 0
    total_volume_24h = 0
    
    for i, asset in enumerate(universe):
        if i >= len(asset_ctxs):
            break
            
        ctx = asset_ctxs[i]
        name = asset.get('name', '')
        
        mark_px = float(ctx.get('markPx', 0))
        open_interest = float(ctx.get('openInterest', 0))
        funding = float(ctx.get('funding', 0))
        prev_day_px = float(ctx.get('prevDayPx', 0))
        volume_24h = float(ctx.get('dayNtlVlm', 0))
        
        change_24h = 0
        if prev_day_px > 0:
            change_24h = ((mark_px - prev_day_px) / prev_day_px) * 100
        
        oi_value = open_interest * mark_px
        total_oi += oi_value
        total_volume_24h += volume_24h
        
        markets.append({
            'name': name,
            'markPx': mark_px,
            'indexPx': float(ctx.get('oraclePx', mark_px)),
            'change24h': change_24h,
            'volume24h': volume_24h,
            'openInterest': oi_value,
            'openInterestCoins': open_interest,
            'funding': funding * 100,
            'maxLeverage': asset.get('maxLeverage', 0)
        })
    
    markets.sort(key=lambda x: x['openInterest'], reverse=True)
    
    stats = {
        'totalOI': total_oi,
        'volume24h': total_volume_24h,
        'activeMarkets': len(markets)
    }
    
    return markets, stats


def process_funding_rates(markets):
    """Get current funding rates for all markets"""
    print("Processing funding rates...")
    
    funding_data = []
    for market in markets[:50]:
        funding_data.append({
            'coin': market['name'],
            'rate': market['funding'],
            'annualized': market['funding'] * 3 * 365,
            'markPx': market['markPx'],
            'openInterest': market['openInterest']
        })
    
    funding_data.sort(key=lambda x: abs(x['rate']), reverse=True)
    return funding_data


def analyze_liquidation_risk(positions_map, markets_dict):
    """Analyze liquidation risk from whale positions"""
    print("Analyzing liquidation risk...")
    
    liquidation_risks = []
    
    for address, positions in positions_map.items():
        for pos in positions:
            liq_px = pos.get('liquidationPx')
            if not liq_px or liq_px == '':
                continue
                
            try:
                liq_px = float(liq_px)
                if liq_px <= 0 or liq_px > 1e10:
                    continue
                    
                coin = pos['coin']
                market = markets_dict.get(coin, {})
                current_px = market.get('markPx', 0)
                
                if current_px <= 0:
                    continue
                
                if pos['direction'] == 'Long':
                    distance_pct = ((current_px - liq_px) / current_px) * 100
                else:
                    distance_pct = ((liq_px - current_px) / current_px) * 100
                
                if 0 < distance_pct < 50:
                    liquidation_risks.append({
                        'coin': coin,
                        'direction': pos['direction'],
                        'positionValue': pos['positionValue'],
                        'size': pos['size'],
                        'entryPx': pos['entryPx'],
                        'currentPx': current_px,
                        'liquidationPx': liq_px,
                        'distancePct': distance_pct,
                        'leverage': pos['leverage'],
                        'traderAddress': pos['traderAddress'],
                        'unrealizedPnl': pos['unrealizedPnl']
                    })
            except (ValueError, TypeError):
                continue
    
    liquidation_risks.sort(key=lambda x: x['distancePct'])
    return liquidation_risks[:200]


def get_pnl_data(trader, period):
    """Extract PnL data for a period"""
    for perf in trader.get('windowPerformances', []):
        if perf[0] == period:
            return {
                'pnl': float(perf[1].get('pnl', 0)),
                'roi': float(perf[1].get('roi', 0)),
                'volume': float(perf[1].get('vlm', 0))
            }
    return {'pnl': 0, 'roi': 0, 'volume': 0}


def process_traders(traders, period, top_n=TOP_N_TRADERS):
    """Process traders by PnL"""
    processed = []
    for trader in traders:
        pnl_data = get_pnl_data(trader, period)
        if pnl_data['pnl'] == 0:
            continue
        processed.append({
            'address': trader.get('ethAddress', ''),
            'displayName': trader.get('displayName'),
            'accountValue': float(trader.get('accountValue', 0)),
            'pnl': pnl_data['pnl'],
            'roi': pnl_data['roi'],
            'volume': pnl_data['volume']
        })
    processed.sort(key=lambda x: x['pnl'], reverse=True)
    return processed[:top_n]


def fetch_positions_batch(addresses):
    """Fetch positions for multiple addresses"""
    positions_map = {}
    total = len(addresses)
    print(f"\nFetching positions for {total} addresses...")
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_addr = {executor.submit(fetch_positions, addr): addr for addr in addresses}
        completed = 0
        for future in as_completed(future_to_addr):
            addr = future_to_addr[future]
            completed += 1
            try:
                positions_map[addr] = future.result()
                if completed % 50 == 0:
                    print(f"  Progress: {completed}/{total}")
            except Exception as e:
                positions_map[addr] = []
            time.sleep(REQUEST_DELAY)
    
    return positions_map


def extract_biggest_positions(positions_map, traders_info, top_n=TOP_N_TRADERS):
    """Extract biggest positions by value"""
    all_positions = []
    trader_lookup = {t['address']: t for t in traders_info}
    
    for address, positions in positions_map.items():
        trader_info = trader_lookup.get(address, {})
        for pos in positions:
            pos_copy = pos.copy()
            pos_copy['traderName'] = trader_info.get('displayName')
            pos_copy['traderAccountValue'] = trader_info.get('accountValue', 0)
            all_positions.append(pos_copy)
    
    all_positions.sort(key=lambda x: float(x.get('positionValue', 0)), reverse=True)
    return all_positions[:top_n]


def main():
    print("=" * 60)
    print("Hyperliquid Analytics Dashboard - Data Fetcher")
    print("=" * 60)
    
    # 1. Fetch market data
    meta_data = fetch_meta_and_contexts()
    markets, dashboard_stats = process_markets_data(meta_data)
    
    if not markets:
        print("Failed to fetch market data")
        return
    
    markets_dict = {m['name']: m for m in markets}
    print(f"  Processed {len(markets)} markets")
    
    # 2. Process funding rates
    funding_rates = process_funding_rates(markets)
    print(f"  Processed {len(funding_rates)} funding rates")
    
    # 3. Fetch leaderboard for whale tracker
    traders = fetch_leaderboard()
    
    daily_top = process_traders(traders, 'day', TOP_N_TRADERS) if traders else []
    weekly_top = process_traders(traders, 'week', TOP_N_TRADERS) if traders else []
    
    # Collect addresses
    all_addresses = set()
    all_traders_info = []
    
    for t in daily_top + weekly_top:
        if t['address'] not in [ti['address'] for ti in all_traders_info]:
            all_traders_info.append(t)
        all_addresses.add(t['address'])
    
    # Add top traders by account value
    if traders:
        traders_by_value = sorted(traders, key=lambda x: float(x.get('accountValue', 0)), reverse=True)
        for t in traders_by_value[:TOP_N_TRADERS]:
            addr = t.get('ethAddress', '')
            if addr and addr not in all_addresses:
                all_addresses.add(addr)
                all_traders_info.append({
                    'address': addr,
                    'displayName': t.get('displayName'),
                    'accountValue': float(t.get('accountValue', 0)),
                    'pnl': 0, 'roi': 0, 'volume': 0
                })
    
    # 4. Fetch all positions
    positions_map = fetch_positions_batch(list(all_addresses)) if all_addresses else {}
    
    # Attach positions to traders
    for trader in daily_top:
        trader['positions'] = positions_map.get(trader['address'], [])
    for trader in weekly_top:
        trader['positions'] = positions_map.get(trader['address'], [])
    
    # 5. Extract biggest positions
    biggest_positions = extract_biggest_positions(positions_map, all_traders_info, TOP_N_TRADERS)
    
    # 6. Analyze liquidation risk
    liquidation_risks = analyze_liquidation_risk(positions_map, markets_dict)
    print(f"  Analyzed {len(liquidation_risks)} liquidation risk positions")
    
    # Summary
    print("\n" + "=" * 60)
    print(f"Total OI: ${dashboard_stats['totalOI']:,.0f}")
    print(f"24h Volume: ${dashboard_stats['volume24h']:,.0f}")
    print(f"Active Markets: {dashboard_stats['activeMarkets']}")
    print("=" * 60)
    
    # Build output
    output = {
        'dashboard': {
            'totalOI': dashboard_stats['totalOI'],
            'volume24h': dashboard_stats['volume24h'],
            'activeMarkets': dashboard_stats['activeMarkets'],
            'totalTraders': len(traders) if traders else 0
        },
        'markets': markets,
        'fundingRates': funding_rates,
        'liquidationRisks': liquidation_risks,
        'whaleTracker': {
            'daily': daily_top,
            'weekly': weekly_top,
            'biggestPositions': biggest_positions
        },
        'lastUpdated': datetime.utcnow().isoformat() + 'Z'
    }
    
    with open(DATA_FILE, 'w') as f:
        json.dump(output, f, indent=2)
    
    print(f"\nSaved to {DATA_FILE}")


if __name__ == '__main__':
    main()
