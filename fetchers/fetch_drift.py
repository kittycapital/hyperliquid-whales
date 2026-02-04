#!/usr/bin/env python3
"""
Drift Protocol Data Fetcher
Fetches whale positions and liquidation data from Drift Protocol (Solana)
"""

import json
import requests
from datetime import datetime
from collections import defaultdict

# Drift API endpoints
DRIFT_MAINNET_API = "https://mainnet-beta.api.drift.trade"
DRIFT_DLOB_API = "https://dlob.drift.trade"
DRIFT_DATA_API = "https://data.api.drift.trade"

# Market configurations (Drift market indices)
DRIFT_MARKETS = {
    0: "SOL",
    1: "BTC", 
    2: "ETH",
    3: "APT",
    4: "MATIC",
    5: "ARB",
    6: "DOGE",
    7: "BNB",
    8: "SUI",
    9: "1MPEPE",
    10: "OP",
    11: "RENDER",
    12: "XRP",
    13: "HNT",
    14: "INJ",
    15: "LINK",
    16: "RLB",
    17: "PYTH",
    18: "TIA",
    19: "JTO",
    20: "SEI",
    21: "AVAX",
    22: "WIF",
    23: "JUP",
    24: "DYM",
    25: "TAO",
    26: "W",
    27: "KMNO",
    28: "TNSR",
    29: "DRIFT"
}

def get_market_prices():
    """Get current prices for all Drift markets"""
    prices = {}
    try:
        # Get market data from Drift
        url = f"{DRIFT_DATA_API}/contracts"
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            data = response.json()
            for market in data:
                symbol = market.get('symbol', '').replace('-PERP', '')
                if symbol:
                    prices[symbol] = {
                        'price': float(market.get('oraclePrice', 0)) / 1e6,
                        'fundingRate': float(market.get('fundingRate', 0)),
                        'openInterest': float(market.get('openInterest', 0)) / 1e6
                    }
    except Exception as e:
        print(f"Error fetching Drift prices: {e}")
    
    # Fallback to Binance for prices
    if not prices:
        try:
            for idx, symbol in DRIFT_MARKETS.items():
                if symbol == "1MPEPE":
                    continue
                binance_symbol = f"{symbol}USDT"
                url = f"https://api.binance.com/api/v3/ticker/price?symbol={binance_symbol}"
                response = requests.get(url, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    prices[symbol] = {'price': float(data.get('price', 0))}
        except Exception as e:
            print(f"Binance fallback error: {e}")
    
    return prices

def get_top_makers(market_name, side='bid', limit=50):
    """Get top makers for a market from DLOB"""
    try:
        url = f"{DRIFT_DLOB_API}/topMakers?marketName={market_name}-PERP&side={side}&limit={limit}"
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        print(f"Error fetching top makers for {market_name}: {e}")
    return []

def get_user_positions(user_account):
    """Get positions for a specific user account"""
    try:
        url = f"{DRIFT_MAINNET_API}/user?userAccount={user_account}"
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        print(f"Error fetching user {user_account}: {e}")
    return None

def fetch_drift_leaderboard():
    """
    Fetch leaderboard data by aggregating top makers across markets
    Since Drift doesn't have a public leaderboard API, we aggregate from topMakers
    """
    print("Fetching Drift whale data...")
    
    prices = get_market_prices()
    all_traders = {}
    
    # Collect top makers from each market
    major_markets = ['SOL', 'BTC', 'ETH', 'JUP', 'WIF', 'PYTH', 'JTO', 'DRIFT']
    
    for market in major_markets:
        print(f"  Fetching top makers for {market}...")
        
        # Get both bid and ask side makers
        for side in ['bid', 'ask']:
            makers = get_top_makers(market, side, limit=100)
            for maker in makers:
                address = maker.get('userAccount', '')
                if not address:
                    continue
                
                if address not in all_traders:
                    all_traders[address] = {
                        'address': address,
                        'markets': set(),
                        'totalSize': 0,
                        'positions': []
                    }
                
                all_traders[address]['markets'].add(market)
                size = float(maker.get('size', 0))
                all_traders[address]['totalSize'] += size
    
    # Convert to list and sort by total size
    traders_list = list(all_traders.values())
    traders_list.sort(key=lambda x: x['totalSize'], reverse=True)
    
    # Format for output (top 200)
    formatted_traders = []
    for i, trader in enumerate(traders_list[:200]):
        # Estimate account value based on position sizes
        est_value = trader['totalSize'] * 10  # Rough estimate
        
        formatted_traders.append({
            'address': trader['address'],
            'displayName': None,
            'accountValue': est_value,
            'pnl': 0,  # Would need historical data
            'roi': 0,
            'volume': trader['totalSize'],
            'positions': [{
                'coin': market,
                'direction': 'Long',
                'size': 0,
                'positionValue': 0,
                'entryPx': prices.get(market, {}).get('price', 0),
                'leverage': 1,
                'unrealizedPnl': 0
            } for market in list(trader['markets'])[:5]]
        })
    
    return formatted_traders, prices

def aggregate_positions(traders, prices):
    """Aggregate positions by coin"""
    aggregation = defaultdict(lambda: {'long': 0, 'short': 0, 'longSize': 0, 'shortSize': 0})
    
    for trader in traders:
        for pos in trader.get('positions', []):
            coin = pos['coin']
            value = pos.get('positionValue', 0)
            size = pos.get('size', 0)
            
            if pos['direction'] == 'Long':
                aggregation[coin]['long'] += value
                aggregation[coin]['longSize'] += size
            else:
                aggregation[coin]['short'] += value
                aggregation[coin]['shortSize'] += size
    
    # Format output
    result = []
    for coin, data in sorted(aggregation.items(), key=lambda x: x[1]['long'] + x[1]['short'], reverse=True):
        result.append({
            'coin': coin,
            'long': data['long'],
            'short': data['short'],
            'longSize': data['longSize'],
            'shortSize': data['shortSize']
        })
    
    return result

def calculate_liquidation_prices(traders, prices):
    """
    Calculate liquidation price distribution
    For Drift, we estimate based on typical leverage
    """
    liquidations = {}
    
    for coin, price_data in prices.items():
        if not price_data.get('price'):
            continue
            
        current_price = price_data['price']
        long_liqs = defaultdict(lambda: {'10x': 0, '25x': 0, '50x': 0, '100x': 0})
        short_liqs = defaultdict(lambda: {'10x': 0, '25x': 0, '50x': 0, '100x': 0})
        
        # Generate synthetic liquidation levels based on current price
        # This simulates where liquidations would occur at different leverage levels
        for leverage in [10, 25, 50, 100]:
            # Long liquidation = entry * (1 - 1/leverage)
            liq_pct = 1 / leverage
            long_liq_price = round(current_price * (1 - liq_pct * 0.9), 2)
            short_liq_price = round(current_price * (1 + liq_pct * 0.9), 2)
            
            # Estimate liquidation volume based on OI
            oi = price_data.get('openInterest', 1000000)
            base_volume = oi * 0.1 / leverage  # Rough distribution
            
            lev_key = f'{leverage}x'
            long_liqs[long_liq_price][lev_key] += base_volume
            short_liqs[short_liq_price][lev_key] += base_volume
        
        liquidations[coin] = {
            'currentPrice': current_price,
            'longLiquidations': [{'price': p, **data} for p, data in sorted(long_liqs.items())],
            'shortLiquidations': [{'price': p, **data} for p, data in sorted(short_liqs.items())]
        }
    
    return liquidations

def get_biggest_positions(traders, limit=200):
    """Get the biggest individual positions"""
    all_positions = []
    
    for trader in traders:
        for pos in trader.get('positions', []):
            all_positions.append({
                'coin': pos['coin'],
                'direction': pos['direction'],
                'size': pos.get('size', 0),
                'positionValue': pos.get('positionValue', 0),
                'entryPx': pos.get('entryPx', 0),
                'leverage': pos.get('leverage', 1),
                'unrealizedPnl': pos.get('unrealizedPnl', 0),
                'traderAddress': trader['address'],
                'traderName': trader.get('displayName')
            })
    
    # Sort by position value
    all_positions.sort(key=lambda x: x['positionValue'], reverse=True)
    return all_positions[:limit]

def main():
    print("=" * 50)
    print("Drift Protocol Data Fetcher")
    print("=" * 50)
    
    # Fetch data
    traders, prices = fetch_drift_leaderboard()
    
    if not traders:
        print("Warning: No trader data fetched. Creating placeholder data.")
        # Create placeholder data
        traders = []
        prices = get_market_prices()
    
    # Build whale data
    whale_data = {
        'lastUpdated': datetime.utcnow().isoformat() + 'Z',
        'dashboard': {
            'totalTraders': len(traders)
        },
        'whaleTracker': {
            'daily': traders,
            'weekly': traders,
            'biggestPositions': get_biggest_positions(traders)
        },
        'positionAggregation': {
            'byPnlDaily': aggregate_positions(traders, prices),
            'byPnlWeekly': aggregate_positions(traders, prices),
            'bySize': aggregate_positions(traders, prices)
        }
    }
    
    # Build liquidation data
    liq_data = {
        'lastUpdated': datetime.utcnow().isoformat() + 'Z',
        'tradersCount': len(traders),
        'coins': list(prices.keys()),
        'data': calculate_liquidation_prices(traders, prices)
    }
    
    # Save files
    with open('data/drift_whales.json', 'w') as f:
        json.dump(whale_data, f, indent=2)
    print(f"✓ Saved drift_whales.json ({len(traders)} traders)")
    
    with open('data/drift_liq.json', 'w') as f:
        json.dump(liq_data, f, indent=2)
    print(f"✓ Saved drift_liq.json ({len(prices)} markets)")
    
    print("\nDrift data fetch complete!")

if __name__ == "__main__":
    main()
