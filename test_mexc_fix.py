#!/usr/bin/env python
"""
Test MEXC Fix

Test the MEXC buy/sell amount fix to prevent oversold errors.
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

async def test_mexc_fix():
    """Test MEXC with the buy/sell amount fix."""
    
    print("🔧 MEXC FIX TEST")
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
        
        symbol = 'BERA/USDT'
        amount = Decimal('5')  # Smaller amount for testing
        
        print(f"🧪 Testing {symbol} with amount: {amount}")
        
        # Get market price
        orderbook = await connector.get_orderbook(symbol, limit=5)
        market_price = (orderbook['bids'][0][0] + orderbook['asks'][0][0]) / 2
        print(f"📊 Market price: ${market_price}")
        
        # STEP 1: BUY order (quote amount)
        print(f"\n🔄 Step 1: BUY order (quote amount)")
        quote_amount = float(amount) * float(market_price) * 1.01
        buy_params = {'createMarketBuyOrderRequiresPrice': False}
        
        print(f"   Quote amount: ${quote_amount}")
        
        buy_order = await connector.place_order(
            symbol=symbol,
            side='buy',
            amount=Decimal(str(quote_amount)),
            order_type=OrderType.MARKET,
            params=buy_params
        )
        
        if buy_order:
            print(f"✅ BUY executed: {buy_order.get('id', 'unknown')}")
            print(f"   Filled: {buy_order.get('filled', 'unknown')} BERA")
            print(f"   Cost: ${buy_order.get('cost', 'unknown')}")
            
            # Wait for settlement
            await asyncio.sleep(3)
            
            # STEP 2: SELL order (using actual filled amount)
            print(f"\n🔄 Step 2: SELL order (using actual filled)")
            actual_filled = buy_order.get('filled', 0)
            
            if actual_filled and float(actual_filled) > 0:
                # Use 99% of actual filled to account for fees
                sell_amount = Decimal(str(float(actual_filled) * 0.99))
                print(f"   Actual filled: {actual_filled} BERA")
                print(f"   Sell amount (99%): {sell_amount} BERA")
                
                sell_order = await connector.place_order(
                    symbol=symbol,
                    side='sell',
                    amount=sell_amount,
                    order_type=OrderType.MARKET,
                    params={}
                )
                
                if sell_order:
                    print(f"✅ SELL executed: {sell_order.get('id', 'unknown')}")
                    print(f"   Sold: {sell_order.get('filled', 'unknown')} BERA")
                    print(f"   Received: ${sell_order.get('cost', 'unknown')}")
                    print(f"\n🎉 MEXC FIX SUCCESSFUL!")
                else:
                    print(f"❌ SELL order failed")
            else:
                print(f"❌ No filled amount from BUY order")
        else:
            print(f"❌ BUY order failed")
        
        await connector.disconnect()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")

if __name__ == "__main__":
    asyncio.run(test_mexc_fix()) 