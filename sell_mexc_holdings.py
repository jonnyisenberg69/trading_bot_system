#!/usr/bin/env python
"""
Sell MEXC Holdings

Sell all SOL and BERA on MEXC to free up USDT balance.
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

async def sell_mexc_holdings():
    """Sell all SOL and BERA holdings on MEXC."""
    
    print("💰 SELLING MEXC HOLDINGS")
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
        print(f"Cancelling all orders")
        # await connector.cancel_all_orders()
        
        # Check current balance
        print("\n📊 Current Balance:")
        balance = await connector.get_balance()
        for currency, amount in balance.items():
            if float(amount) > 0:
                print(f"   {currency}: {amount}")
        
        # Sell SOL if available
        SOL_balance = balance.get('SOL', Decimal('0'))
        if float(SOL_balance) > 0:
            print(f"\n🔄 Selling {SOL_balance} SOL...")
            
            try:
                # Use 99% of balance to account for precision
                sell_amount = Decimal(str(float(SOL_balance) * 0.99))
                
                SOL_order = await connector.place_order(
                    symbol='SOL/USDT',
                    side='sell',
                    amount=sell_amount,
                    order_type=OrderType.MARKET,
                    params={}
                )
                
                if SOL_order:
                    print(f"✅ SOL SOLD: {SOL_order.get('id', 'unknown')}")
                    print(f"   Amount: {SOL_order.get('filled', 'unknown')} SOL")
                    print(f"   Received: ${SOL_order.get('cost', 'unknown')}")
                else:
                    print(f"❌ SOL sell failed")
                    
            except Exception as e:
                print(f"❌ SOL sell error: {e}")
        else:
            print(f"\n⚠️  No SOL balance to sell")
        
        # Sell BERA if available
        bera_balance = balance.get('BERA', Decimal('0'))
        if float(bera_balance) > 0:
            print(f"\n🔄 Selling {bera_balance} BERA...")
            
            try:
                # Use 99% of balance to account for precision
                sell_amount = Decimal(str(float(bera_balance) * 0.99))
                
                bera_order = await connector.place_order(
                    symbol='BERA/USDT',
                    side='sell',
                    amount=sell_amount,
                    order_type=OrderType.MARKET,
                    params={}
                )
                
                if bera_order:
                    print(f"✅ BERA SOLD: {bera_order.get('id', 'unknown')}")
                    print(f"   Amount: {bera_order.get('filled', 'unknown')} BERA")
                    print(f"   Received: ${bera_order.get('cost', 'unknown')}")
                else:
                    print(f"❌ BERA sell failed")
                    
            except Exception as e:
                print(f"❌ BERA sell error: {e}")
        else:
            print(f"\n⚠️  No BERA balance to sell")
        
        # Wait for settlement
        print(f"\n⏱️  Waiting 5 seconds for settlement...")
        await asyncio.sleep(5)
        
        # Check final balance
        print(f"\n📊 Final Balance:")
        final_balance = await connector.get_balance()
        for currency, amount in final_balance.items():
            if float(amount) > 0:
                print(f"   {currency}: {amount}")
        
        usdt_balance = final_balance.get('USDT', Decimal('0'))
        print(f"\n💰 USDT Available: ${usdt_balance}")
        print(f"🎯 Ready for trading: {'✅' if float(usdt_balance) > 10 else '❌'}")
        
        await connector.disconnect()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")

if __name__ == "__main__":
    asyncio.run(sell_mexc_holdings()) 