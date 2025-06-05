#!/usr/bin/env python
"""
Binance Authentication Verification Script

Quick verification of Binance authentication methods and credentials
to debug any authentication issues.
"""

import asyncio
import sys
import logging
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from exchanges.connectors import create_exchange_connector
from config.exchange_keys import get_exchange_config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def verify_binance_auth():
    """Verify Binance authentication methods and credentials."""
    print("🔍 BINANCE AUTHENTICATION VERIFICATION")
    print("="*50)
    
    # Test both spot and futures
    exchanges_to_test = [
        ('binance_spot', 'spot'),
        ('binance_perp', 'future')
    ]
    
    for exchange_name, market_type in exchanges_to_test:
        print(f"\n📡 Testing {exchange_name}...")
        
        try:
            # Create connector
            config = get_exchange_config(exchange_name)
            connector = create_exchange_connector('binance', config)
            
            # Connect
            connected = await connector.connect()
            if not connected:
                print(f"   ❌ Failed to connect to {exchange_name}")
                continue
            
            print(f"   ✅ Connected to {exchange_name}")
            
            # Check exchange type
            exchange_type = connector.exchange.__class__.__name__.lower()
            is_futures = 'usdm' in exchange_type or 'coinm' in exchange_type
            
            print(f"   📋 Exchange class: {connector.exchange.__class__}")
            print(f"   📋 Exchange type: {exchange_type}")
            print(f"   📋 Is futures: {is_futures}")
            print(f"   📋 Market type: {getattr(connector, 'market_type', 'unknown')}")
            
            # Check available methods
            available_methods = [m for m in dir(connector.exchange) 
                               if 'listen' in m.lower() or 'userdata' in m.lower()]
            print(f"   📋 Available auth methods: {len(available_methods)}")
            
            # Group methods by type
            spot_methods = [m for m in available_methods if 'sapi' in m.lower() or 'public' in m.lower()]
            futures_methods = [m for m in available_methods if 'fapi' in m.lower()]
            
            print(f"   📋 Spot methods: {spot_methods[:3]}{'...' if len(spot_methods) > 3 else ''}")
            print(f"   📋 Futures methods: {futures_methods[:3]}{'...' if len(futures_methods) > 3 else ''}")
            
            # Test correct method
            if is_futures:
                # Test futures listenKey
                if hasattr(connector.exchange, 'fapiPrivatePostListenKey'):
                    print(f"   ✅ Found correct futures method: fapiPrivatePostListenKey")
                    try:
                        response = await connector.exchange.fapiPrivatePostListenKey()
                        if response and 'listenKey' in response:
                            listen_key = response['listenKey']
                            print(f"   🎉 SUCCESS: Got futures listenKey: {listen_key[:8]}...")
                        else:
                            print(f"   ❌ Invalid futures response: {response}")
                    except Exception as e:
                        print(f"   ❌ Futures listenKey error: {e}")
                else:
                    print(f"   ❌ Missing fapiPrivatePostListenKey method")
            else:
                # Test spot listenKey
                spot_methods_to_try = [
                    'publicPostUserDataStream',
                    'sapiPostUserDataStream'
                ]
                
                success = False
                for method_name in spot_methods_to_try:
                    if hasattr(connector.exchange, method_name):
                        print(f"   🔍 Trying spot method: {method_name}")
                        try:
                            method = getattr(connector.exchange, method_name)
                            response = await method()
                            if response and 'listenKey' in response:
                                listen_key = response['listenKey']
                                print(f"   🎉 SUCCESS: Got spot listenKey: {listen_key[:8]}...")
                                success = True
                                break
                            else:
                                print(f"   ⚠️  Invalid response from {method_name}: {response}")
                        except Exception as e:
                            print(f"   ⚠️  Error with {method_name}: {e}")
                            if "margin" in str(e).lower():
                                print(f"      💡 This is a margin endpoint error - expected")
                            continue
                
                if not success:
                    print(f"   ❌ No working spot listenKey method found")
            
            # Check API key permissions
            try:
                account_info = await connector.exchange.fetch_balance()
                print(f"   ✅ API key has account access")
            except Exception as e:
                print(f"   ⚠️  API key issue: {e}")
            
            await connector.disconnect()
            
        except Exception as e:
            print(f"   💥 Error testing {exchange_name}: {e}")
            import traceback
            traceback.print_exc()
    
    print(f"\n🎯 VERIFICATION COMPLETE")


async def main():
    """Main verification."""
    try:
        await verify_binance_auth()
    except KeyboardInterrupt:
        print("\n🛑 Verification interrupted")
    except Exception as e:
        print(f"\n💥 Verification failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main()) 