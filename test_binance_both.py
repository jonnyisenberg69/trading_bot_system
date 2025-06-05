#!/usr/bin/env python
"""
Test Both Binance Spot and Perp Together
"""

import asyncio
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from exchanges.connectors import create_exchange_connector
from config.exchange_keys import get_exchange_config

async def test_both_binance():
    """Test both binance_spot and binance_perp to see if there's interference."""
    
    print("🔧 Testing Both Binance Spot and Perp...")
    
    exchanges = ['binance_spot', 'binance_perp']
    connectors = {}
    
    # Step 1: Create both connectors
    for exchange_name in exchanges:
        print(f"\n   🔧 {exchange_name} - getting configuration...")
        config = get_exchange_config(exchange_name)
        print(f"   🔧 {exchange_name} - config keys: {list(config.keys())}")
        print(f"   🔧 {exchange_name} - market_type: {config.get('market_type')}")
        
        print(f"   🔧 {exchange_name} - creating connector...")
        connector = create_exchange_connector(exchange_name.split('_')[0], config)
        connectors[exchange_name] = connector
        print(f"   ✅ {exchange_name} - connector created")
    
    # Step 2: Try to connect both
    for exchange_name, connector in connectors.items():
        print(f"\n   🔧 {exchange_name} - attempting connection...")
        try:
            connected = await connector.connect()
            if connected:
                print(f"   ✅ {exchange_name} - connected successfully")
                
                # Test basic operations
                balance = await connector.get_balance()
                print(f"   ✅ {exchange_name} - balance keys: {list(balance.keys())}")
                
            else:
                print(f"   ❌ {exchange_name} - connection returned False")
                
        except Exception as e:
            print(f"   ❌ {exchange_name} - error: {e}")
            if 'margin' in str(e).lower():
                print(f"      ⚠️  Margin API error detected!")
            import traceback
            print(f"      Traceback: {traceback.format_exc()}")
    
    # Step 3: Cleanup
    for exchange_name, connector in connectors.items():
        try:
            await connector.disconnect()
            print(f"   ✅ {exchange_name} - disconnected")
        except Exception as e:
            print(f"   ⚠️  {exchange_name} - disconnect error: {e}")

if __name__ == "__main__":
    asyncio.run(test_both_binance()) 