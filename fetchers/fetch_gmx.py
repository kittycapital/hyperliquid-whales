#!/usr/bin/env python3
"""
GMX Data Fetcher (Fixed)
Uses real GMX V2 APIs:
- arbitrum-api.gmxinfra.io - REST API for markets, prices, OI
- gmx.squids.live - Subsquid GraphQL for positions/trades
- CoinGecko/Binance - Fallback prices
"""

import json
import requests
from datetime import datetime
from collections import defaultdict
import time
import math
import os

# GMX V2 API endpoints
GMX_ORACLE_API = "https://arbitrum-api.gmxinfra.io"
GMX_ORACLE_FALLBACK = "https://arbitrum-api-fallback.gmxinfra.io"
GMX_SUBSQUID = "https://gmx.squids.live/gmx-synthetics-arbitrum:prod/api/graphql"

# Token address to symbol mapping (GMX V2 Arbitrum)
TOKEN_MAP = {
    '0x82af49447d8a07e3bd95bd0d56f35241523fbab1': 'ETH',
    '0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f': 'BTC',
    '0xf97f4df75117a78c1a5a0dbb814af92458539fb4': 'LINK',
    '0xfa7f8980b0f1e64a2062791cc3b0871572f1f7f0': 'UNI',
    '0x912ce59144191c1204e64559fe8253a0e49e6548': 'ARB',
    '0xfc5a1a6eb076a2c7ad06ed22c90d7e710e35ad0a': 'GMX',
    '0xaf88d065e77c8cc2239327c5edb3a432268e5831': 'USDC',
    '0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9': 'USDT',
    '0x2bcbdd1f14ea02136ab5006d3f8cee8dbc161e2c': 'SOL',
    '0xa9004a5421372e1d83fb1f85b0fc986c912f91f3': 'DOGE',
    '0xb46a094bc4b0adbd801e14b9db95e05e28962764': 'LTC',
    '0xc14e065b0067de91534e032868f5ac6ecf2c6868': 'XRP',
    '0xba5ddd1f9d7f570dc94a51479a000e3bce967196': 'AAVE',
    '0xaed233e2b7ab0a4e1b5783462c85ee0d1fed43c0': 'ATOM',
    '0x1ff7f3efc7295089a41dd0d118e38b55ca53e5eb': 'NEAR',
    '0x13ad51ed4f1b7e9dc168d8a00cb3f4ddd85efa3e': 'LDO',
    '0x3082cc23568ea640225c2467653db90e9250aaa0': 'RDNT',
    '0x7d258e452e9507a3c3c3c10a53c25e29de9b7cbe': 'PEPE',
    '0xa14453084318277b11d38fbe05d857a4f647442b': 'WIF',
    '0xb261104a83887ae92392fb5ce5899fcfe5481456': 'ORDI',
    '0x4945970efeec98d393b4b979b9be265a3ae28a8b': 'STX',
    '0x565609faf65b92f7be02468acf86f8979423f5ed': 'SHIB',
    '0xc618a9e96e4386a171c2e8ddb9169e4a51be1e69': 'AVAX',
}

TOP_N = 200
MAX_WORKERS = 8


def safe_float(val, default=0):
    try:
        if val is None or val == '':
            return default
        return float(val)
    except (ValueError, TypeError):
        return default


def token_to_symbol(address):
    """Convert token address to symbol"""
    if not address:
        return 'UNKNOWN'
    return TOKEN_MAP.get(address.lower(), address[:8] + '...')


def get_gmx_markets():
    """Get real market data from GMX V2 REST API"""
    print("📊 Fetching GMX V2 market data...")
    markets = {}

    for api_url in [GMX_ORACLE_API, GMX_ORACLE_FALLBACK]:
        try:
            # Get market info (includes OI, prices, etc.)
            url = f"{api_url}/markets/info"
            r = requests.get(url, timeout=30)
            if r.status_code != 200:
                continue

            data = r.json()
            if not isinstance(data, list):
                continue

            for m in data:
                if not isinstance(m, dict):
                    continue

                index_token = m.get('indexTokenAddress', '').lower()
                symbol = token_to_symbol(index_token)
                if symbol == 'UNKNOWN' or symbol.endswith('...'):
                    continue

                # GMX V2 values are in USD with 30 decimals for some fields
                long_oi = safe_float(m.get('longOpenInterestUsd') or m.get('longInterestUsd', 0))
                short_oi = safe_float(m.get('shortOpenInterestUsd') or m.get('shortInterestUsd', 0))

                # Handle different precision formats
                if long_oi > 1e25:
                    long_oi = long_oi / 1e30
                    short_oi = short_oi / 1e30

                total_oi = long_oi + short_oi

                # Get price from various fields
                min_price = safe_float(m.get('minPrice', 0))
                max_price = safe_float(m.get('maxPrice', 0))
                if min_price > 1e25:
                    min_price = min_price / 1e30
                    max_price = max_price / 1e30

                price = (min_price + max_price) / 2 if min_price > 0 else 0

                # Funding rate
                funding_long = safe_float(m.get('longFundingRatePerHour') or m.get('longsPayShorts', 0))
                funding_short = safe_float(m.get('shortFundingRatePerHour') or m.get('shortsPayLongs', 0))

                if symbol not in markets or total_oi > markets[symbol].get('totalOI', 0):
                    markets[symbol] = {
                        'price': price,
                        'longOI': long_oi,
                        'shortOI': short_oi,
                        'totalOI': total_oi,
                        'fundingLong': funding_long,
                        'fundingShort': funding_short,
                        'marketAddress': m.get('marketTokenAddress', ''),
                    }

            if markets:
                print(f"  ✓ Got {len(markets)} markets from {api_url}")
                break

        except Exception as e:
            print(f"  ⚠ {api_url} error: {e}")
            continue

    return markets


def get_prices_fallback():
    """Get prices from CoinGecko or Binance as fallback"""
    prices = {}
    symbols = ['BTC', 'ETH', 'LINK', 'UNI', 'AVAX', 'SOL', 'ARB', 'DOGE', 'LTC', 'XRP',
               'AAVE', 'ATOM', 'NEAR', 'GMX', 'PEPE', 'WIF', 'SHIB', 'ORDI', 'STX']

    # Try CoinGecko
    COINGECKO_IDS = {
        'BTC': 'bitcoin', 'ETH': 'ethereum', 'LINK': 'chainlink', 'UNI': 'uniswap',
        'AVAX': 'avalanche-2', 'SOL': 'solana', 'ARB': 'arbitrum', 'DOGE': 'dogecoin',
        'LTC': 'litecoin', 'XRP': 'ripple', 'AAVE': 'aave', 'ATOM': 'cosmos',
        'NEAR': 'near', 'GMX': 'gmx', 'WIF': 'dogwifcoin', 'PEPE': 'pepe',
        'SHIB': 'shiba-inu', 'ORDI': 'ordinals', 'STX': 'blockstack',
    }

    try:
        ids = ','.join(COINGECKO_IDS.values())
        url = f"https://api.coingecko.com/api/v3/simple/price?ids={ids}&vs_currencies=usd"
        r = requests.get(url, timeout=20, headers={'Accept': 'application/json'})
        if r.status_code == 200:
            data = r.json()
            for symbol, gecko_id in COINGECKO_IDS.items():
                if gecko_id in data:
                    price = data[gecko_id].get('usd', 0)
                    if price > 0:
                        prices[symbol] = price
            print(f"  ✓ Got {len(prices)} prices from CoinGecko")
    except Exception as e:
        print(f"  ⚠ CoinGecko error: {e}")

    # Binance fallback
    if len(prices) < 5:
        print("  Trying Binance...")
        for symbol in symbols:
            if symbol in prices:
                continue
            try:
                url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}USDT"
                r = requests.get(url, timeout=10)
                if r.status_code == 200:
                    price = safe_float(r.json().get('price'))
                    if price > 0:
                        prices[symbol] = price
            except:
                pass

    return prices


def fetch_subsquid_trades():
    """Fetch recent large trades from GMX Subsquid"""
    print("📡 Fetching trade data from Subsquid...")
    traders = {}

    # Query for recent trade actions (increase/decrease positions)
    queries = [
        # Get top traders by volume
        """
        query {
            tradeActions(
                orderBy: sizeDeltaUsd_DESC
                limit: 500
                where: { sizeDeltaUsd_gt: "0" }
            ) {
                id
                account
                marketAddress
                sizeDeltaUsd
                isLong
                indexTokenPriceMin
                indexTokenPriceMax
                timestamp
            }
        }
        """,
        # Get recent position increases
        """
        query {
            positionIncreases(
                orderBy: sizeDeltaUsd_DESC
                limit: 500
            ) {
                id
                account
                marketAddress
                collateralAmount
                sizeDeltaUsd
                sizeInUsd
                isLong
                indexTokenPriceMin
                indexTokenPriceMax
                timestamp
            }
        }
        """,
    ]

    for query in queries:
        try:
            r = requests.post(
                GMX_SUBSQUID,
                json={'query': query},
                timeout=30,
                headers={'Content-Type': 'application/json'}
            )
            if r.status_code != 200:
                continue

            data = r.json().get('data', {})

            # Process trade actions
            actions = data.get('tradeActions', data.get('positionIncreases', []))
            for action in actions:
                account = action.get('account', '').lower()
                if not account:
                    continue

                size_delta = safe_float(action.get('sizeDeltaUsd', 0))
                if size_delta > 1e25:
                    size_delta = size_delta / 1e30
                size_in_usd = safe_float(action.get('sizeInUsd', 0))
                if size_in_usd > 1e25:
                    size_in_usd = size_in_usd / 1e30

                collateral = safe_float(action.get('collateralAmount', 0))
                if collateral > 1e15:
                    collateral = collateral / 1e18  # Token precision

                if account not in traders:
                    traders[account] = {
                        'address': account,
                        'displayName': None,
                        'totalVolume': 0,
                        'totalSize': 0,
                        'tradeCount': 0,
                        'positions_raw': [],
                    }

                traders[account]['totalVolume'] += size_delta
                traders[account]['totalSize'] = max(traders[account]['totalSize'], size_in_usd)
                traders[account]['tradeCount'] += 1

                # Extract position info
                min_price = safe_float(action.get('indexTokenPriceMin', 0))
                max_price = safe_float(action.get('indexTokenPriceMax', 0))
                if min_price > 1e25:
                    min_price = min_price / 1e30
                    max_price = max_price / 1e30
                avg_price = (min_price + max_price) / 2 if min_price > 0 else 0

                market_addr = action.get('marketAddress', '')

                traders[account]['positions_raw'].append({
                    'marketAddress': market_addr,
                    'isLong': action.get('isLong', True),
                    'sizeDelta': size_delta,
                    'sizeUsd': size_in_usd,
                    'price': avg_price,
                    'collateral': collateral,
                })

            if traders:
                print(f"  ✓ Got data for {len(traders)} traders from Subsquid")
                break

        except Exception as e:
            print(f"  ⚠ Subsquid error: {e}")
            continue

    return traders


def build_trader_list(raw_traders, markets, fallback_prices):
    """Build formatted trader list with positions"""
    print("📋 Building trader list...")

    # Create market address to symbol mapping
    market_to_symbol = {}
    for symbol, info in markets.items():
        if isinstance(info, dict) and info.get('marketAddress'):
            market_to_symbol[info['marketAddress'].lower()] = symbol

    # Get price lookup
    price_lookup = {}
    for symbol, info in markets.items():
        if isinstance(info, dict):
            price_lookup[symbol] = info.get('price', 0)
    for symbol, price in fallback_prices.items():
        if symbol not in price_lookup or price_lookup[symbol] <= 0:
            price_lookup[symbol] = price

    traders = []
    for addr, raw in raw_traders.items():
        # Build positions
        positions = []
        seen_markets = set()

        for pos_raw in raw.get('positions_raw', []):
            market_addr = pos_raw.get('marketAddress', '').lower()
            symbol = market_to_symbol.get(market_addr, 'UNKNOWN')

            # Skip duplicates and unknown markets
            if symbol == 'UNKNOWN' or symbol in seen_markets:
                continue
            seen_markets.add(symbol)

            size_usd = pos_raw.get('sizeUsd', 0) or pos_raw.get('sizeDelta', 0)
            price = pos_raw.get('price', 0) or price_lookup.get(symbol, 0)
            collateral = pos_raw.get('collateral', 0)
            leverage = size_usd / collateral if collateral > 0 else 10

            # Estimate liquidation price
            liq_px = 0
            if price > 0 and leverage > 0:
                liq_pct = 1 / leverage * 0.9
                if pos_raw.get('isLong', True):
                    liq_px = price * (1 - liq_pct)
                else:
                    liq_px = price * (1 + liq_pct)

            positions.append({
                'coin': symbol,
                'direction': 'Long' if pos_raw.get('isLong', True) else 'Short',
                'size': size_usd / price if price > 0 else 0,
                'positionValue': size_usd,
                'entryPx': price,
                'leverage': min(leverage, 100),
                'unrealizedPnl': 0,
                'liquidationPx': str(round(liq_px, 2)) if liq_px > 0 else '',
            })

        # Calculate totals
        total_value = sum(p['positionValue'] for p in positions) if positions else raw['totalVolume']
        pnl = 0

        traders.append({
            'address': raw['address'],
            'displayName': raw.get('displayName'),
            'accountValue': total_value,
            'pnl': pnl,
            'roi': 0,
            'volume': raw['totalVolume'],
            'positions': positions,
        })

    # Sort by account value / volume
    traders.sort(key=lambda x: x['accountValue'], reverse=True)
    return traders[:TOP_N]


def aggregate_positions(traders):
    """Aggregate positions by coin"""
    agg = defaultdict(lambda: {'long': 0, 'short': 0, 'longSize': 0, 'shortSize': 0})
    for t in traders:
        for p in t.get('positions', []):
            c = p.get('coin', 'BTC')
            val = p.get('positionValue', 0)
            size = p.get('size', 0)
            if p.get('direction') == 'Long':
                agg[c]['long'] += val
                agg[c]['longSize'] += size
            else:
                agg[c]['short'] += val
                agg[c]['shortSize'] += size

    result = [{'coin': c, **d, 'total': d['long'] + d['short']} for c, d in agg.items()]
    result.sort(key=lambda x: x['total'], reverse=True)
    return result


def get_biggest_positions(traders, limit=200):
    all_pos = []
    for t in traders:
        for p in t.get('positions', []):
            all_pos.append({
                **p,
                'traderAddress': t.get('address', ''),
                'traderName': t.get('displayName'),
                'traderAccountValue': t.get('accountValue', 0),
            })
    all_pos.sort(key=lambda x: x.get('positionValue', 0), reverse=True)
    return all_pos[:limit]


def get_bucket_size(coin, price):
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
    """Build liquidation map from position data"""
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

        curr = prices.get(coin, 0)
        if isinstance(curr, dict):
            curr = curr.get('price', 0)
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


def main():
    print("=" * 60)
    print("🚀 GMX Data Fetcher (v2)")
    print(f"⏰ Started at: {datetime.utcnow().isoformat()}Z")
    print("=" * 60)

    # Step 1: Get real market data from GMX REST API
    markets = get_gmx_markets()
    fallback_prices = {}

    # Get prices as backup
    if not markets or all(m.get('price', 0) == 0 for m in markets.values()):
        print("  Getting fallback prices...")
        fallback_prices = get_prices_fallback()
    else:
        # Extract prices from markets
        for sym, info in markets.items():
            if isinstance(info, dict) and info.get('price', 0) > 0:
                fallback_prices[sym] = info['price']

    # Make sure we have prices
    if not fallback_prices and not markets:
        print("  ⚠ No prices from GMX API, fetching from CoinGecko/Binance...")
        fallback_prices = get_prices_fallback()

    print(f"  Total: {len(markets)} markets, {len(fallback_prices)} price entries")

    # Step 2: Fetch trade data from Subsquid
    raw_traders = fetch_subsquid_trades()

    # Step 3: Build trader list
    if raw_traders:
        traders = build_trader_list(raw_traders, markets, fallback_prices)
    else:
        print("  ⚠ No trader data from Subsquid, creating minimal data from markets...")
        traders = []

    if not traders:
        print("  ⚠ No traders available")
        # Create minimal placeholder with real market prices
        traders = [{
            'address': '0x0000000000000000000000000000000000000000',
            'displayName': 'No data available',
            'accountValue': 0,
            'pnl': 0,
            'roi': 0,
            'volume': 0,
            'positions': [],
        }]

    # Step 4: Build aggregation & liquidation data
    agg = aggregate_positions(traders)
    biggest = get_biggest_positions(traders)

    # Build price lookup for liq map
    price_map = {}
    for sym, info in markets.items():
        if isinstance(info, dict):
            price_map[sym] = info.get('price', 0)
    for sym, price in fallback_prices.items():
        if sym not in price_map or price_map[sym] <= 0:
            price_map[sym] = price

    liq_map = build_liq_map(traders, price_map)

    # Step 5: Save
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

    with open('data/gmx_whales.json', 'w') as f:
        json.dump(whale_data, f)
    print(f"✅ Saved gmx_whales.json ({len(traders)} traders)")

    liq_data = {
        'lastUpdated': datetime.utcnow().isoformat() + 'Z',
        'tradersCount': len(traders),
        'coins': list(liq_map.keys()) if liq_map else list(price_map.keys())[:10],
        'data': liq_map,
    }

    with open('data/gmx_liq.json', 'w') as f:
        json.dump(liq_data, f)
    print(f"✅ Saved gmx_liq.json ({len(liq_map)} coins)")

    print(f"\n{'='*60}")
    print(f"📊 Markets: {len(markets)}")
    print(f"👥 Traders: {len(traders)}")
    print(f"🗺️  Liquidation coins: {len(liq_map)}")
    print(f"⏰ Completed at: {datetime.utcnow().isoformat()}Z")
    print("=" * 60)


if __name__ == "__main__":
    main()
