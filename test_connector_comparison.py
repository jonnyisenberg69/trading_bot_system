#!/usr/bin/env python
"""
Test Connector Comparison

Compare how debug script creates connector vs comprehensive test to find the difference.
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from exchanges.connectors import create_exchange_connector
from config.exchange_keys import get_exchange_config

async def test_connector_comparison():
    """Test if there's a difference between debug and comprehensive test connector creation."""
    
    print("🔍 CONNECTOR COMPARISON TEST")
    print("=" * 50)
    
    # Test both Bybit and Gate.io
    test_configs = [
        {'exchange_name': 'bybit_spot', 'symbol': 'BTC/USDT'},
        {'exchange_name': 'bybit_perp', 'symbol': 'BTC/USDT:USDT'},
        {'exchange_name': 'gateio_spot', 'symbol': 'BTC/USDT'}
    ]
    
    for test_config in test_configs:
        exchange_name = test_config['exchange_name']
        symbol = test_config['symbol']
        
        print(f"\n📋 TESTING {exchange_name.upper()} with {symbol}")
        print("-" * 40)
        
        try:
            # Create connector exactly like comprehensive test
            config = get_exchange_config(exchange_name)
            exchange_type = exchange_name.split('_')[0]  # Extract 'bybit' or 'gateio'
            connector = create_exchange_connector(exchange_type, config)
            
            connected = await connector.connect()
            if connected:
                print(f"✅ Connected successfully")
                
                # Test 1: Debug script approach
                print("\n🔧 Method 1: Debug Script Approach")
                since = datetime.utcnow() - timedelta(days=1)
                trades1 = await connector.exchange.fetch_my_trades(
                    symbol=symbol,
                    since=int(since.timestamp() * 1000),
                    limit=100
                )
                print(f"  Raw CCXT (1 day): {len(trades1)} trades")
                
                # Test 2: Comprehensive test approach (exact same parameters)
                print("\n🔧 Method 2: Comprehensive Test Approach")
                since_30d = datetime.utcnow() - timedelta(days=30)
                
                # ADDED: Debug logging
                print(f"  Debug: Calling get_trade_history with:")
                print(f"    Symbol: {symbol}")
                print(f"    Since: {since_30d}")
                print(f"    Limit: 500")
                
                trades2 = await connector.get_trade_history(
                    symbol=symbol,
                    since=since_30d,
                    limit=500
                )
                print(f"  Connector method (30 days): {len(trades2)} trades")
                
                # ADDED: Try shorter timeframe to match debug
                print("\n🔧 Method 2b: Connector with 1 day (like debug)")
                since_1d = datetime.utcnow() - timedelta(days=1)
                trades2b = await connector.get_trade_history(
                    symbol=symbol,
                    since=since_1d,
                    limit=100
                )
                print(f"  Connector method (1 day): {len(trades2b)} trades")
                
                # Test 3: Apply comprehensive test filtering
                print("\n🔧 Method 3: Comprehensive Test Filtering")
                if trades2:
                    # Apply the same filtering as comprehensive test
                    recent_cutoff = datetime.utcnow() - timedelta(days=30)
                    recent_trades = []
                    
                    for trade in trades2:
                        trade_time = parse_trade_timestamp(trade)
                        if trade_time and trade_time >= recent_cutoff:
                            recent_trades.append(trade)
                    
                    print(f"  After filtering: {len(recent_trades)} recent trades")
                    
                    if recent_trades:
                        # Show sample trades
                        for i, trade in enumerate(recent_trades[-3:]):
                            side = trade.get('side', 'unknown')
                            amount = trade.get('amount', 0)
                            price = trade.get('price', 0)
                            trade_id = trade.get('id', 'unknown')
                            dt = trade.get('datetime', 'unknown')
                            print(f"    Trade {i+1}: {trade_id} - {side} {amount} @ ${price:,.2f} at {dt}")
                else:
                    print("  No trades to filter")
                
                # Test 4: Raw parameter check
                print(f"\n🔍 Parameters:")
                print(f"  Exchange type: {exchange_type}")
                print(f"  Market type: {getattr(connector, 'market_type', 'unknown')}")
                print(f"  Symbol normalized: {connector.normalize_symbol(symbol) if hasattr(connector, 'normalize_symbol') else symbol}")
                
                await connector.disconnect()
            else:
                print(f"❌ Connection failed")
                
        except Exception as e:
            print(f"❌ Error: {e}")
            import traceback
            print(f"Traceback: {traceback.format_exc()}")

def parse_trade_timestamp(trade):
    """Parse trade timestamp exactly like comprehensive test."""
    try:
        if 'datetime' in trade and trade['datetime']:
            if isinstance(trade['datetime'], str):
                # Handle different datetime string formats
                dt_str = trade['datetime'].replace('Z', '+00:00')
                try:
                    return datetime.fromisoformat(dt_str).replace(tzinfo=None)
                except:
                    return datetime.fromisoformat(dt_str.replace('+00:00', '')).replace(tzinfo=None)
            return trade['datetime'].replace(tzinfo=None) if hasattr(trade['datetime'], 'replace') else trade['datetime']
        elif 'timestamp' in trade and trade['timestamp']:
            timestamp = trade['timestamp']
            if isinstance(timestamp, (int, float)):
                if timestamp > 1e12:  # Milliseconds
                    timestamp = timestamp / 1000
                return datetime.utcfromtimestamp(timestamp).replace(tzinfo=None)
    except Exception as e:
        print(f"      Warning: Could not parse timestamp {trade.get('datetime')} or {trade.get('timestamp')}: {e}")
    return None

if __name__ == "__main__":
    asyncio.run(test_connector_comparison()) 