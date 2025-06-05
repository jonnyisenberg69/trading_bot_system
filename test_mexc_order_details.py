#!/usr/bin/env python
"""
Test MEXC Order Details

Examine the actual MEXC order response structure.
"""

import asyncio
import sys
from pathlib import Path
from decimal import Decimal
import json

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from exchanges.connectors import create_exchange_connector
from config.exchange_keys import get_exchange_config
from exchanges.base_connector import OrderType

async def test_mexc_order_details():
    """Test MEXC order details."""
    
    print("🔍 MEXC ORDER DETAILS TEST")
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
        
        # Get market price
        orderbook = await connector.get_orderbook(symbol, limit=5)
        market_price = (orderbook['bids'][0][0] + orderbook['asks'][0][0]) / 2
        print(f"📊 Market price: ${market_price}")
        
        # Place small buy order
        quote_amount = 3.0  # Small $3 order
        print(f"\n🔄 Placing ${quote_amount} buy order...")
        
        buy_params = {'createMarketBuyOrderRequiresPrice': False}
        
        buy_order = await connector.place_order(
            symbol=symbol,
            side='buy',
            amount=Decimal(str(quote_amount)),
            order_type=OrderType.MARKET,
            params=buy_params
        )
        
        print(f"\n📋 BUY ORDER RESPONSE:")
        print(json.dumps(buy_order, indent=2, default=str))
        
        if buy_order and buy_order.get('id'):
            # Wait and then check order status
            print(f"\n⏱️  Waiting 5 seconds then checking order status...")
            await asyncio.sleep(5)
            
            order_status = await connector.get_order_status(buy_order['id'], symbol)
            print(f"\n📋 ORDER STATUS RESPONSE:")
            print(json.dumps(order_status, indent=2, default=str))
            
            # Try to get recent trades
            print(f"\n📋 RECENT TRADES:")
            trades = await connector.get_trade_history(symbol=symbol, limit=10)
            for i, trade in enumerate(trades[-3:]):
                print(f"  Trade {i+1}: {trade.get('id')} - {trade.get('side')} {trade.get('amount')} @ ${trade.get('price')}")
        
        await connector.disconnect()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")

if __name__ == "__main__":
    asyncio.run(test_mexc_order_details()) 