#!/usr/bin/env python
"""
Test Binance Spot in Comprehensive Test Environment
"""

import asyncio
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from exchanges.connectors import create_exchange_connector
from config.exchange_keys import get_exchange_config

async def test_binance_spot_comprehensive_style():
    """Test binance_spot exactly as done in the comprehensive test."""
    
    print("🔧 Testing Binance Spot in comprehensive test style...")
    
    # Step 1: Get configuration (exactly as comprehensive test does)
    exchange_name = 'binance_spot'
    print(f"   🔧 {exchange_name} - getting configuration...")
    config = get_exchange_config(exchange_name)
    if not config:
        print(f"   ❌ {exchange_name} - no configuration found")
        return
    
    print(f"   🔧 {exchange_name} - config keys: {list(config.keys())}")
    
    # Step 2: Create connector (exactly as comprehensive test does)
    print(f"   🔧 {exchange_name} - creating connector...")
    connector = create_exchange_connector(exchange_name.split('_')[0], config)
    if not connector:
        print(f"   ❌ {exchange_name} - connector creation returned None")
        return
    
    print(f"   🔧 {exchange_name} - attempting connection...")
    
    # Step 3: Connect (exactly as comprehensive test does)
    try:
        connected = await connector.connect()
        if connected:
            print(f"   ✅ {exchange_name} - connected successfully with API key: {config.get('api_key', '')[:10]}...")
            
            # Step 4: Test basic operations
            print(f"   🔧 {exchange_name} - testing orderbook...")
            orderbook = await connector.get_orderbook('BERA/USDT', limit=5)
            if orderbook:
                print(f"   ✅ {exchange_name} - orderbook retrieved")
            else:
                print(f"   ❌ {exchange_name} - orderbook failed")
            
            print(f"   🔧 {exchange_name} - testing balance...")
            balance = await connector.get_balance()
            print(f"   ✅ {exchange_name} - balance: {list(balance.keys())}")
            
            await connector.disconnect()
            print(f"   ✅ {exchange_name} - disconnected successfully")
            
        else:
            print(f"   ❌ {exchange_name} - connection returned False")
            
    except Exception as e:
        print(f"   ❌ {exchange_name} - error: {e}")
        import traceback
        print(f"      Traceback: {traceback.format_exc()}")

if __name__ == "__main__":
    asyncio.run(test_binance_spot_comprehensive_style()) 