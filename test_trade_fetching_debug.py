#!/usr/bin/env python
"""
Debug Trade Fetching Test

Quick test to debug why trade fetching isn't working on most exchanges.
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime, timedelta
import traceback

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from exchanges.connectors import create_exchange_connector
from config.exchange_keys import get_exchange_config

async def test_trade_fetching():
    """Test trade fetching on each exchange individually."""
    
    exchanges_to_test = [
        'binance_spot',
        'binance_perp', 
        'bybit_spot',
        'bybit_perp',
        'mexc_spot',
        'gateio_spot',
        'bitget_spot',
        'hyperliquid_perp'
    ]
    
    print("🔍 DEBUG: Testing trade fetching on each exchange...")
    print("=" * 60)
    
    for exchange_name in exchanges_to_test:
        print(f"\n📥 Testing {exchange_name}")
        print("-" * 40)
        
        try:
            # Get config and connect
            config = get_exchange_config(exchange_name)
            if not config:
                print(f"   ❌ No configuration found")
                continue
                
            connector = create_exchange_connector(exchange_name.split('_')[0], config)
            if not connector:
                print(f"   ❌ Connector creation failed")
                continue
                
            connected = await connector.connect()
            if not connected:
                print(f"   ❌ Connection failed")
                continue
                
            print(f"   ✅ Connected successfully")
            
            # Determine symbol to test
            if 'perp' in exchange_name or 'future' in exchange_name:
                if 'hyperliquid' in exchange_name:
                    symbol = 'BTC/USDC:USDC'
                else:
                    symbol = 'BTC/USDT:USDT'
            elif 'mexc' in exchange_name:
                symbol = 'BNB/USDT'
            else:
                symbol = 'BTC/USDT'
                
            print(f"   🔍 Testing trade history for {symbol}")
            
            # Test different time ranges
            time_ranges = [
                ("1 hour", datetime.utcnow() - timedelta(hours=1)),
                ("6 hours", datetime.utcnow() - timedelta(hours=6)),
                ("24 hours", datetime.utcnow() - timedelta(hours=24)),
                ("7 days", datetime.utcnow() - timedelta(days=7)),
            ]
            
            for range_name, since_time in time_ranges:
                try:
                    print(f"     🕐 Fetching trades since {range_name} ago...")
                    
                    trades = await connector.get_trade_history(
                        symbol=symbol,
                        since=since_time,
                        limit=50
                    )
                    
                    print(f"     📊 Found {len(trades) if trades else 0} trades")
                    
                    if trades:
                        # Show sample trades
                        for i, trade in enumerate(trades[-3:]):  # Last 3 trades
                            trade_time = trade.get('datetime', trade.get('timestamp', 'N/A'))
                            side = trade.get('side', 'N/A')
                            amount = trade.get('amount', 0)
                            price = trade.get('price', 0)
                            trade_id = trade.get('id', 'N/A')
                            print(f"       #{i+1}: {trade_id} - {side} {amount} @ ${price} at {trade_time}")
                        break  # Found trades, no need to test other ranges
                        
                except Exception as e:
                    print(f"     ❌ Error for {range_name}: {e}")
                    
            # Clean up
            await connector.disconnect()
            
        except Exception as e:
            print(f"   ❌ Overall error: {e}")
            print(f"   🔍 Traceback: {traceback.format_exc()}")
            
        await asyncio.sleep(1)  # Rate limiting
    
    print("\n" + "=" * 60)
    print("🏁 Trade fetching debug test complete")

if __name__ == "__main__":
    asyncio.run(test_trade_fetching()) 