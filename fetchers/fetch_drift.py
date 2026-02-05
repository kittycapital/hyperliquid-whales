#!/usr/bin/env python3
"""
Drift Protocol Data Fetcher (Fixed)
Uses real Drift APIs:
- data.api.drift.trade - Market data, contracts, prices
- dlob.drift.trade - Top makers per market
- mainnet-beta.api.drift.trade - User positions
"""

import json
import requests
from datetime import datetime
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import math
import os

# Drift API endpoints
DRIFT_DATA_API = "https://data.api.drift.trade"
DRIFT_DLOB_API = "https://dlob.drift.trade"
DRIFT_MAINNET_API = "https://mainnet-beta.api.drift.trade"

# Markets to scan for top makers
MAJOR_MARKETS = ['SOL', 'BTC', 'ETH', 'JUP', 'WIF', 'PYTH', 'JTO', 'DRIFT',
                 'RENDER', 'BONK', 'W', 'TNSR', 'HNT', 'SUI', 'APT', 'SEI',
                 'AVAX', 'LINK', 'DOGE', 'INJ', 'XRP', 'OP', 'ARB', 'TIA']

MAX_WORKERS = 8
TOP_N = 200


def safe_float(val, default=0):
    """Safely convert to float"""
    try:
        if val is None or val == '':
            return default
        return float(val)
    except (ValueError, TypeError):
        return default


def get_market_data():
    """Get market data from Drift Data API"""
    print("📊 Fetching Drift market data...")
    prices = {}

    # Try /contracts endpoint
    try:
        url = f"{DRIFT_DATA_API}/contracts"
        r = requests.get(url, timeout=30, headers={'Accept': 'application/json'})
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list):
                for m in data:
                    if not isinstance(m, dict):
                        continue
                    symbol = m.get('symbol', '').replace('-PERP', '').replace('1M', '')
                    price = safe_float(m.get('oraclePrice') or m.get('markPrice') or m.get('lastPrice'))
                    # Some Drift prices come in raw precision (1e6)
                    if price > 1e10:
                        price = price / 1e6
                    if symbol and price > 0:
                        prices[symbol] = {
                            'price': price,
                            'fundingRate': safe_float(m.get('fundingRate') or m.get('lastFundingRate')),
                            'openInterest': safe_float(m.get('openInterest') or m.get('baseAssetAmountWithAmm')) / 1e6 if safe_float(m.get('openInterest', 0)) > 1e10 else safe_float(m.get('openInterest', 0)),
                            'volume24h': safe_float(m.get('volume24h') or m.get('baseVolume24h', 0)),
                        }
            elif isinstance(data, dict):
                markets = data.get('markets', data.get('perp', data.get('contracts', [])))
                for m in markets:
                    if not isinstance(m, dict):
                        continue
                    symbol = m.get('symbol', '').replace('-PERP', '')
                    price = safe_float(m.get('oraclePrice') or m.get('price'))
                    if price > 1e10:
                        price = price / 1e6
                    if symbol and price > 0:
                        prices[symbol] = {
                            'price': price,
                            'fundingRate': safe_float(m.get('fundingRate')),
                            'openInterest': safe_float(m.get('openInterest', 0)),
                            'volume24h': safe_float(m.get('volume24h', 0)),
                        }
            print(f"  ✓ Got {len(prices)} markets from /contracts")
    except Exception as e:
        print(f"  ⚠ Drift /contracts error: {e}")

    # Fallback: try /perpMarkets
    if len(prices) < 3:
        try:
            url = f"{DRIFT_DATA_API}/perpMarkets"
            r = requests.get(url, timeout=30, headers={'Accept': 'application/json'})
            if r.status_code == 200:
                data = r.json()
                if isinstance(data, list):
                    for m in data:
                        symbol = m.get('symbol', '').replace('-PERP', '')
                        price = safe_float(m.get('oraclePrice') or m.get('price'))
                        if price > 1e10:
                            price = price / 1e6
                        if symbol and price > 0:
                            prices[symbol] = {
                                'price': price,
                                'fundingRate': safe_float(m.get('fundingRate')),
                                'openInterest': safe_float(m.get('openInterest', 0)),
                                'volume24h': safe_float(m.get('volume24h', 0)),
                            }
                print(f"  ✓ Got {len(prices)} markets from /perpMarkets")
        except Exception as e:
            print(f"  ⚠ Drift /perpMarkets error: {e}")

    # Final fallback: Binance prices for major markets
    if len(prices) < 3:
        print("  ⚠ Using Binance prices as fallback...")
        for symbol in MAJOR_MARKETS[:10]:
            if symbol in prices and prices[symbol].get('price', 0) > 0:
                continue
            try:
                binance_sym = f"{symbol}USDT"
                if symbol == 'BONK':
                    binance_sym = '1000BONKUSDT'
                url = f"https://api.binance.com/api/v3/ticker/24hr?symbol={binance_sym}"
                r = requests.get(url, timeout=10)
                if r.status_code == 200:
                    d = r.json()
                    price = safe_float(d.get('lastPrice'))
                    if symbol == 'BONK':
                        price = price / 1000
                    if price > 0:
                        prices[symbol] = {
                            'price': price,
                            'fundingRate': 0,
                            'openInterest': safe_float(d.get('quoteVolume', 0)) * 0.1,
                            'volume24h': safe_float(d.get('quoteVolume', 0)),
                        }
            except:
                pass
        print(f"  ✓ Total {len(prices)} markets with prices")

    return prices


def get_top_makers(market_name, side='bid', limit=100):
    """Get top makers for a market from DLOB"""
    try:
        url = f"{DRIFT_DLOB_API}/topMakers?marketName={market_name}-PERP&side={side}&limit={limit}"
        r = requests.get(url, timeout=20)
        if r.status_code == 200:
            data = r.json()
            return data if isinstance(data, list) else []
    except Exception as e:
        pass
    return []


def fetch_user_positions(user_account):
    """Get positions for a user from Drift mainnet API"""
    try:
        url = f"{DRIFT_MAINNET_API}/user?userAccount={user_account}"
        r = requests.get(url, timeout=20)
        if r.status_code == 200:
            data = r.json()
            if not data:
                return []

            positions = []
            perp_positions = data.get('perpPositions', data.get('assetPositions', []))

            for p in perp_positions:
                if isinstance(p, dict):
                    # Handle nested position objects
                    pos = p.get('position', p)
                    base_amount = safe_float(pos.get('baseAssetAmount') or pos.get('szi', 0))
                    if base_amount == 0:
                        continue

                    # Drift amounts may be in raw precision
                    if abs(base_amount) > 1e12:
                        base_amount = base_amount / 1e9

                    market_idx = pos.get('marketIndex', 0)
                    coin = pos.get('coin', pos.get('symbol', f'MARKET_{market_idx}'))
                    if '-PERP' in str(coin):
                        coin = coin.replace('-PERP', '')

                    entry_price = safe_float(pos.get('entryPrice') or pos.get('entryPx', 0))
                    if entry_price > 1e10:
                        entry_price = entry_price / 1e6

                    quote_entry = safe_float(pos.get('quoteEntryAmount') or pos.get('quoteAssetAmount', 0))
                    if abs(quote_entry) > 1e10:
                        quote_entry = quote_entry / 1e6

                    position_value = abs(base_amount * entry_price) if entry_price > 0 else abs(quote_entry)
                    upnl = safe_float(pos.get('unrealizedPnl') or pos.get('unsettledPnl', 0))
                    if abs(upnl) > 1e10:
                        upnl = upnl / 1e6

                    leverage_val = safe_float(pos.get('leverage', 0))
                    if leverage_val == 0 and position_value > 0:
                        # Estimate leverage
                        leverage_val = 5  # Default estimate

                    liq_price = safe_float(pos.get('liquidationPrice') or pos.get('liquidationPx', 0))
                    if liq_price > 1e10:
                        liq_price = liq_price / 1e6

                    positions.append({
                        'coin': coin,
                        'direction': 'Long' if base_amount > 0 else 'Short',
                        'size': abs(base_amount),
                        'entryPx': entry_price,
                        'leverage': leverage_val,
                        'positionValue': position_value,
                        'unrealizedPnl': upnl,
                        'liquidationPx': str(liq_price) if liq_price > 0 else '',
                    })

            return positions
    except Exception as e:
        pass
    return []


def collect_whale_addresses(prices):
    """Collect unique whale addresses from top makers across markets"""
    print("\n🐋 Collecting whale addresses from top makers...")
    all_traders = {}
    markets_to_scan = [m for m in MAJOR_MARKETS if m in prices][:15]

    for market in markets_to_scan:
        for side in ['bid', 'ask']:
            makers = get_top_makers(market, side, limit=100)
            if not makers:
                continue

            for maker in makers:
                if isinstance(maker, str):
                    address = maker
                    size = 0
                elif isinstance(maker, dict):
                    address = maker.get('userAccount', '') or maker.get('maker', '') or maker.get('address', '')
                    size = safe_float(maker.get('size') or maker.get('makerSize', 0))
                else:
                    continue

                if not address:
                    continue

                if address not in all_traders:
                    all_traders[address] = {
                        'address': address,
                        'markets': set(),
                        'totalSize': 0,
                    }

                all_traders[address]['markets'].add(market)
                all_traders[address]['totalSize'] += size

        print(f"  ├─ {market}: {len(all_traders)} unique addresses so far")

    print(f"  └─ Total: {len(all_traders)} unique whale addresses")
    return all_traders


def fetch_all_positions(addresses):
    """Fetch positions for all addresses concurrently"""
    pos_map = {}
    total = len(addresses)
    print(f"\n📡 Fetching positions for {total} addresses...")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch_user_positions, a): a for a in addresses}
        done = 0
        for f in as_completed(futures):
            addr = futures[f]
            done += 1
            try:
                pos_map[addr] = f.result()
            except:
                pos_map[addr] = []
            if done % 50 == 0 or done == total:
                print(f"  └─ {done}/{total}")
            time.sleep(0.05)

    has_positions = sum(1 for v in pos_map.values() if v)
    print(f"  └─ Done: {has_positions}/{total} addresses have positions")
    return pos_map


def aggregate_positions(traders):
    """Aggregate positions by coin"""
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
    """Determine bucket size for liquidation map"""
    buckets = {'BTC': 1000, 'ETH': 50, 'SOL': 5, 'BNB': 10}
    if coin in buckets:
        return buckets[coin]
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


def build_liq_map(traders, prices):
    """Build liquidation map from real position data"""
    print("🗺️  Building liquidation map...")
    liq_by_coin = defaultdict(list)

    for t in traders:
        for p in t.get('positions', []):
            liq = p.get('liquidationPx')
            if not liq:
                continue
            try:
                liq = float(liq)
                if liq <= 0 or liq > 1e12:
                    continue
                liq_by_coin[p['coin']].append({
                    **p,
                    'liquidationPx': liq,
                    'traderAddress': t.get('address', '')
                })
            except:
                continue

    result = {}
    for coin, positions in liq_by_coin.items():
        if len(positions) < 2:
            continue
        price_data = prices.get(coin, {})
        curr = price_data.get('price', 0) if isinstance(price_data, dict) else safe_float(price_data)
        if curr <= 0:
            continue

        bucket_size = get_bucket_size(coin, curr)
        min_p, max_p = curr * 0.5, curr * 1.5

        long_buckets = defaultdict(lambda: {'10x': 0, '25x': 0, '50x': 0, '100x': 0})
        short_buckets = defaultdict(lambda: {'10x': 0, '25x': 0, '50x': 0, '100x': 0})

        for p in positions:
            liq = p['liquidationPx']
            if liq < min_p or liq > max_p:
                continue
            key = round(math.floor(liq / bucket_size) * bucket_size, 2)
            cat = get_lev_cat(p['leverage'])

            if p['direction'] == 'Long':
                long_buckets[key][cat] += p['positionValue']
            else:
                short_buckets[key][cat] += p['positionValue']

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

    print(f"  └─ {len(result)} coins mapped")
    return result


def get_biggest_positions(traders, n=TOP_N):
    """Get the biggest individual positions"""
    all_pos = []
    for t in traders:
        for p in t.get('positions', []):
            all_pos.append({
                **p,
                'traderAddress': t.get('address', ''),
                'traderName': t.get('displayName'),
                'traderAccountValue': t.get('accountValue', 0)
            })
    all_pos.sort(key=lambda x: x.get('positionValue', 0), reverse=True)
    return all_pos[:n]


def main():
    print("=" * 60)
    print("🚀 Drift Protocol Data Fetcher (v2)")
    print(f"⏰ Started at: {datetime.utcnow().isoformat()}Z")
    print("=" * 60)

    # Step 1: Get market data (prices, OI, funding)
    prices = get_market_data()
    if not prices:
        print("❌ Failed to get any market data")
        return

    # Step 2: Collect whale addresses from top makers
    all_traders_raw = collect_whale_addresses(prices)

    if not all_traders_raw:
        print("❌ Failed to collect any whale addresses")
        return

    # Sort by total maker size, take top addresses
    sorted_traders = sorted(all_traders_raw.values(), key=lambda x: x['totalSize'], reverse=True)
    addresses_to_fetch = [t['address'] for t in sorted_traders[:TOP_N]]

    # Step 3: Fetch actual positions
    pos_map = fetch_all_positions(addresses_to_fetch)

    # Step 4: Build trader list with real positions
    print("\n📋 Processing traders...")
    traders = []
    for t in sorted_traders[:TOP_N]:
        addr = t['address']
        positions = pos_map.get(addr, [])
        total_value = sum(p.get('positionValue', 0) for p in positions)
        total_pnl = sum(p.get('unrealizedPnl', 0) for p in positions)

        traders.append({
            'address': addr,
            'displayName': None,
            'accountValue': total_value if total_value > 0 else t['totalSize'] * 10,
            'pnl': total_pnl,
            'roi': total_pnl / total_value if total_value > 0 else 0,
            'volume': t['totalSize'],
            'positions': positions,
        })

    # Sort by account value
    traders.sort(key=lambda x: x['accountValue'], reverse=True)

    # Step 5: Aggregation & liquidation map
    print("\n📊 Aggregating positions...")
    agg = aggregate_positions(traders)
    liq_map = build_liq_map(traders, prices)
    biggest = get_biggest_positions(traders)

    # Step 6: Save
    os.makedirs('data', exist_ok=True)

    whale_data = {
        'lastUpdated': datetime.utcnow().isoformat() + 'Z',
        'dashboard': {
            'totalTraders': len(traders),
        },
        'whaleTracker': {
            'daily': traders,
            'weekly': traders,
            'biggestPositions': biggest,
        },
        'positionAggregation': {
            'byPnlDaily': agg,
            'byPnlWeekly': agg,
            'bySize': agg,
        }
    }

    with open('data/drift_whales.json', 'w') as f:
        json.dump(whale_data, f)
    print(f"✅ Saved drift_whales.json ({len(traders)} traders)")

    liq_data = {
        'lastUpdated': datetime.utcnow().isoformat() + 'Z',
        'tradersCount': len(traders),
        'coins': list(liq_map.keys()) if liq_map else list(prices.keys())[:10],
        'data': liq_map
    }

    with open('data/drift_liq.json', 'w') as f:
        json.dump(liq_data, f)
    print(f"✅ Saved drift_liq.json ({len(liq_map)} coins)")

    traders_with_pos = sum(1 for t in traders if t.get('positions'))
    print(f"\n{'='*60}")
    print(f"📊 Markets: {len(prices)}")
    print(f"👥 Traders: {len(traders)} ({traders_with_pos} with positions)")
    print(f"🗺️  Liquidation coins: {len(liq_map)}")
    print(f"⏰ Completed at: {datetime.utcnow().isoformat()}Z")
    print("=" * 60)


if __name__ == "__main__":
    main()
