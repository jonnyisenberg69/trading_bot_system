#!/usr/bin/env python
"""
Test Hyperliquid Trade Fetching with Updated Configuration

Test if we can successfully fetch trades using the funded wallet configuration.
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from exchanges.connectors import create_exchange_connector
from config.exchange_keys import get_exchange_config

async def test_hyperliquid_trade_fetching():
    """Test Hyperliquid trade fetching with updated configuration."""
    
    print("🧪 HYPERLIQUID TRADE FETCHING TEST")
    print("=" * 50)
    
    try:
        # Get updated config
        config = get_exchange_config('hyperliquid_perp')
        if not config:
            print("❌ No configuration found")
            return
            
        print(f"📋 Configuration:")
        print(f"   Wallet Address: {config.get('wallet_address')}")
        print(f"   Using Funded Wallet: {'✅' if config.get('wallet_address') == '0x59a7eC7a658777225F7123B2c8420b6D5EC9D64d' else '❌'}")
        
        # Create connector
        print(f"\n🔧 Creating Hyperliquid connector...")
        connector = create_exchange_connector('hyperliquid', config)
        if not connector:
            print("❌ Connector creation failed")
            return
            
        # Connect
        print(f"🔧 Connecting...")
        connected = await connector.connect()
        if not connected:
            print("❌ Connection failed")
            return
            
        print("✅ Connected successfully!")
        
        # Test trade fetching
        print(f"\n📥 Testing trade fetching...")
        
        # Test with different time ranges
        test_cases = [
            {"name": "Last 1 hour", "hours": 1},
            {"name": "Last 6 hours", "hours": 6}, 
            {"name": "Last 24 hours", "hours": 24},
            {"name": "Last 7 days", "hours": 168}
        ]
        
        symbol = 'BTC/USDC:USDC'  # Hyperliquid format
        
        for test_case in test_cases:
            try:
                since = datetime.utcnow() - timedelta(hours=test_case['hours'])
                print(f"\n   🔍 {test_case['name']} (since {since.strftime('%Y-%m-%d %H:%M:%S')}):")
                
                trades = await connector.get_trade_history(
                    symbol=symbol,
                    since=since,
                    limit=100
                )
                
                if trades:
                    print(f"     ✅ Found {len(trades)} trades")
                    
                    # Show recent trades
                    for i, trade in enumerate(trades[-3:]):  # Last 3 trades
                        print(f"     📊 Trade {i+1}:")
                        print(f"        ID: {trade.get('id', 'N/A')}")
                        print(f"        Side: {trade.get('side', 'N/A')}")
                        print(f"        Amount: {trade.get('amount', 'N/A')}")
                        print(f"        Price: ${trade.get('price', 'N/A'):,.2f}")
                        print(f"        Time: {trade.get('datetime', 'N/A')}")
                    
                    # Check if our known order IDs are present
                    known_order_ids = ['99567058791', '99567072273']
                    found_orders = []
                    
                    for trade in trades:
                        trade_id = str(trade.get('id', ''))
                        if trade_id in known_order_ids:
                            found_orders.append(trade_id)
                    
                    if found_orders:
                        print(f"     🎉 Found our test order IDs: {found_orders}")
                        return True  # Success!
                    
                else:
                    print(f"     ⚠️  No trades found")
                    
            except Exception as e:
                print(f"     ❌ Error: {e}")
        
        # Clean up
        await connector.disconnect()
        print(f"\n✅ Test completed")
        return False
        
    except Exception as e:
        print(f"❌ Overall error: {e}")
        return False

if __name__ == "__main__":
    success = asyncio.run(test_hyperliquid_trade_fetching())
    if success:
        print("\n🎉 HYPERLIQUID TRADE FETCHING: SUCCESS")
    else:
        print("\n❌ HYPERLIQUID TRADE FETCHING: FAILED") 