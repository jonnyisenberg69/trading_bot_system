#!/usr/bin/env python
"""
Debug Binance Spot Connection Issue
"""

import asyncio
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from exchanges.connectors import create_exchange_connector
from config.exchange_keys import get_exchange_config

async def test_binance_spot():
    print('🔧 Testing Binance Spot connection...')
    
    try:
        config = get_exchange_config('binance_spot')
        print(f'📊 Config: {list(config.keys())}')
        print(f'📊 Market type: {config.get("market_type", "not_specified")}')
        
        connector = create_exchange_connector('binance', config)
        print(f'📊 Connector created: {type(connector).__name__}')
        print(f'📊 Connector market type: {connector.market_type}')
        print(f'📊 Exchange type: {type(connector.exchange).__name__}')
        
        # Test the actual connection step by step
        print('\n🔧 Step 1: Loading markets...')
        await connector.exchange.load_markets()
        print('✅ Markets loaded successfully')
        
        print('\n🔧 Step 2: Testing server time...')
        server_time = await connector.exchange.fetch_time()
        print(f'✅ Server time: {server_time}')
        
        print('\n🔧 Step 3: Getting balance...')
        balance = await connector.get_balance()
        print(f'✅ Balance keys: {list(balance.keys())}')
        
        await connector.disconnect()
        print('✅ Test completed successfully')
        
    except Exception as e:
        print(f'❌ Error: {e}')
        
        # Get more detailed error info
        import traceback
        print('\n📋 Full traceback:')
        traceback.print_exc()
        
        # Check if it's related to specific API calls
        if 'margin' in str(e).lower():
            print('\n⚠️  This appears to be a margin API issue')
        if 'isolated' in str(e).lower():
            print('⚠️  This appears to be related to isolated margin')

if __name__ == "__main__":
    asyncio.run(test_binance_spot()) 