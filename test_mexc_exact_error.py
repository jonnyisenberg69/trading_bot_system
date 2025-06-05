#!/usr/bin/env python
"""
Test MEXC Exact Error

Mimic the exact comprehensive test flow to pinpoint the MEXC issue.
"""

import asyncio
import sys
from pathlib import Path
from decimal import Decimal

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from exchanges.connectors import create_exchange_connector
from config.exchange_keys import get_exchange_config
from exchanges.base_connector import OrderType

async def test_mexc_exact_error():
    """Test MEXC with exact comprehensive test parameters."""
    
    print("🔍 MEXC EXACT ERROR TEST")
    print("=" * 50)
    
    try:
        # Get MEXC configuration
        config = get_exchange_config('mexc_spot')
        connector = create_exchange_connector('mexc', config)
        
        connected = await connector.connect()
        if not connected:
            print("❌ Failed to connect to MEXC")
            return
            
        print("✅ Connected to MEXC")
        
        # Use EXACT comprehensive test configuration
        symbol = 'BERA/USDT'
        amount = Decimal('10')  # EXACT amount from comprehensive test
        market_buy_requires_quote = True  # EXACT setting from comprehensive test
        
        print(f"🧪 Testing with EXACT comprehensive test parameters:")
        print(f"   Symbol: {symbol}")
        print(f"   Amount: {amount}")
        print(f"   market_buy_requires_quote: {market_buy_requires_quote}")
        
        # Get market price
        orderbook = await connector.get_orderbook(symbol, limit=5)
        market_price = (orderbook['bids'][0][0] + orderbook['asks'][0][0]) / 2
        print(f"   Market price: ${market_price}")
        
        # EXACT comprehensive test logic
        print(f"\n🧪 Step 1: Direct connector.place_order() (comprehensive test method)")
        
        try:
            if market_buy_requires_quote:
                # EXACT comprehensive test calculation
                quote_amount = float(amount) * float(market_price) * 1.01  # 1% buffer
                buy_params = {'createMarketBuyOrderRequiresPrice': False}
                
                print(f"   Quote amount: ${quote_amount}")
                print(f"   Buy params: {buy_params}")
                print(f"   Calling connector.place_order()...")
                
                result = await connector.place_order(
                    symbol=symbol,
                    side='buy',
                    amount=Decimal(str(quote_amount)),
                    order_type=OrderType.MARKET,
                    params=buy_params
                )
                print(f"   ✅ SUCCESS: {result.get('id', 'unknown')}")
                
        except Exception as e:
            print(f"   ❌ COMPREHENSIVE TEST METHOD FAILED: {e}")
            
            # Show the exact error details
            import traceback
            print(f"   Full traceback: {traceback.format_exc()}")
            
            # Now try debug method
            print(f"\n🧪 Step 2: Direct exchange.create_market_order() (debug method)")
            try:
                quote_amount = float(amount) * float(market_price) * 1.01
                buy_params = {'createMarketBuyOrderRequiresPrice': False}
                
                result = await connector.exchange.create_market_order(
                    symbol, 'buy', quote_amount, None, buy_params
                )
                print(f"   ✅ DEBUG METHOD SUCCESS: {result.get('id', 'unknown')}")
                
                # If successful, try to cancel
                if result.get('id'):
                    try:
                        await connector.exchange.cancel_order(result['id'], symbol)
                        print(f"   🗑️  Order cancelled")
                    except:
                        print(f"   ⚠️  Could not cancel")
                        
            except Exception as e2:
                print(f"   ❌ DEBUG METHOD ALSO FAILED: {e2}")
                
        # Additional debug: Check what exactly gets passed to CCXT
        print(f"\n🔍 Debug info:")
        print(f"   Connector normalize_symbol('{symbol}'): '{connector.normalize_symbol(symbol)}'")
        print(f"   Market info: {connector.exchange.markets.get(symbol, 'NOT FOUND')}")
        
        await connector.disconnect()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")

if __name__ == "__main__":
    asyncio.run(test_mexc_exact_error()) 