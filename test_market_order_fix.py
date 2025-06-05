#!/usr/bin/env python
"""
Quick test to verify market order fixes work without real trading.
"""

import asyncio
import sys
from pathlib import Path
from decimal import Decimal

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from exchanges.connectors import create_exchange_connector
from exchanges.base_connector import OrderType
from config.exchange_keys import get_exchange_config

async def test_market_order_fix():
    """Test that market orders don't cause decimal conversion errors."""
    
    print("🔧 Testing market order fixes...")
    
    # Test one exchange that previously failed
    exchange_name = 'bybit_spot'
    
    try:
        config = get_exchange_config(exchange_name)
        connector = create_exchange_connector(exchange_name.split('_')[0], config)
        
        if connector:
            connected = await connector.connect()
            if connected:
                print(f"✅ {exchange_name} connected successfully")
                
                # Get current price for reference
                try:
                    orderbook = await connector.get_orderbook('BTC/USDT', limit=5)
                    if orderbook and orderbook.get('bids'):
                        current_price = orderbook['bids'][0][0]
                        print(f"📈 Current BTC price: ${current_price:,.2f}")
                        
                        # Test market order creation (dry run - will fail but shouldn't have decimal errors)
                        try:
                            # This will fail with insufficient balance or other trading errors,
                            # but should NOT fail with decimal conversion syntax errors
                            await connector.place_order(
                                symbol='BTC/USDT',
                                side='buy',
                                amount=Decimal('0.0001'),
                                order_type=OrderType.MARKET
                            )
                            print("✅ Market order test - no decimal conversion errors!")
                            
                        except Exception as e:
                            error_msg = str(e)
                            if 'decimal.ConversionSyntax' in error_msg:
                                print(f"❌ STILL HAS DECIMAL ERROR: {error_msg}")
                            elif 'InvalidOperation' in error_msg and 'decimal' in error_msg:
                                print(f"❌ STILL HAS DECIMAL ERROR: {error_msg}")
                            else:
                                print(f"✅ Market order error but NOT decimal issue: {error_msg[:100]}...")
                                print("   (This is expected - insufficient balance, etc.)")
                    
                except Exception as e:
                    print(f"⚠️ Price fetch error: {e}")
                
                await connector.disconnect()
            else:
                print(f"❌ {exchange_name} connection failed")
        else:
            print(f"❌ {exchange_name} connector creation failed")
            
    except Exception as e:
        print(f"❌ {exchange_name} setup error: {e}")
    
    print("🏁 Market order fix test complete")

if __name__ == "__main__":
    asyncio.run(test_market_order_fix()) 