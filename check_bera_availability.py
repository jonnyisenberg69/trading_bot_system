#!/usr/bin/env python
"""
Check BERA Availability

Check if BERA/USDT is available on all exchanges before running comprehensive test.
"""

import asyncio
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from exchanges.connectors import create_exchange_connector
from config.exchange_keys import get_exchange_config

async def check_bera_availability():
    """Check BERA/USDT availability on all exchanges."""
    
    print("🔍 CHECKING BERA/USDT AVAILABILITY")
    print("=" * 60)
    
    exchanges_to_check = [
        'binance_spot',
        'bybit_spot', 
        'mexc_spot',
        'gateio_spot',
        'bitget_spot'
    ]
    
    results = {}
    
    for exchange_name in exchanges_to_check:
        print(f"\n📋 CHECKING {exchange_name.upper()}")
        print("-" * 30)
        
        try:
            # Get configuration and create connector
            config = get_exchange_config(exchange_name)
            connector = create_exchange_connector(exchange_name.split('_')[0], config)
            
            connected = await connector.connect()
            if not connected:
                print(f"❌ Failed to connect")
                results[exchange_name] = "connection_failed"
                continue
            
            # Check if BERA/USDT exists in markets
            markets = connector.exchange.markets
            symbol = 'BERA/USDT'
            
            if symbol in markets:
                market = markets[symbol]
                is_active = market.get('active', False)
                is_spot = market.get('type', '') == 'spot'
                
                if is_active and is_spot:
                    print(f"✅ BERA/USDT: AVAILABLE & ACTIVE")
                    results[exchange_name] = "available"
                    
                    # Test orderbook
                    try:
                        orderbook = await connector.get_orderbook(symbol, limit=1)
                        if orderbook and orderbook.get('bids') and orderbook.get('asks'):
                            bid = orderbook['bids'][0][0]
                            ask = orderbook['asks'][0][0]
                            print(f"   📊 Price: bid=${bid}, ask=${ask}")
                        else:
                            print(f"   ⚠️  Orderbook issue")
                    except Exception as e:
                        print(f"   ⚠️  Orderbook error: {e}")
                else:
                    print(f"❌ BERA/USDT: INACTIVE (active={is_active}, type={market.get('type')})")
                    results[exchange_name] = "inactive"
            else:
                print(f"❌ BERA/USDT: NOT FOUND")
                results[exchange_name] = "not_found"
                
                # Show alternative symbols
                print(f"   🔍 Available alternatives:")
                bera_symbols = [s for s in markets.keys() if 'BERA' in s and markets[s].get('active', False)][:5]
                if bera_symbols:
                    for alt_symbol in bera_symbols:
                        print(f"     {alt_symbol}")
                else:
                    print(f"     (No BERA pairs found)")
            
            await connector.disconnect()
            
        except Exception as e:
            print(f"❌ Error: {e}")
            results[exchange_name] = "error"
    
    # Summary
    print(f"\n🎯 AVAILABILITY SUMMARY:")
    print("=" * 60)
    
    available_count = 0
    for exchange, status in results.items():
        emoji = "✅" if status == "available" else "❌"
        print(f"{emoji} {exchange}: {status}")
        if status == "available":
            available_count += 1
    
    print(f"\n📊 BERA/USDT available on {available_count}/{len(exchanges_to_check)} exchanges")
    
    if available_count >= len(exchanges_to_check) // 2:
        print(f"🎉 READY TO RUN COMPREHENSIVE TEST WITH BERA/USDT")
    else:
        print(f"⚠️  Consider using different symbols for some exchanges")
    
    return results

if __name__ == "__main__":
    asyncio.run(check_bera_availability()) 