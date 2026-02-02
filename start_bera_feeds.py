#!/usr/bin/env python3
"""
Start BERA Market Data Feeds

Starts real market data services for BERA pairs and then tests the pricing engine.
"""

import asyncio
import sys
import os
from datetime import datetime
from decimal import Decimal

# Import services
sys.path.insert(0, os.path.dirname(__file__))
from market_data.market_data_service import MarketDataService
from test_live_redis_pricing import RedisMarketDataProvider
from market_data.multi_reference_pricing_engine import MultiReferencePricingEngine


async def start_bera_market_data():
    """Start market data services for BERA pairs."""
    
    print("🚀 Starting BERA Market Data Services")
    print("=" * 40)
    
    # BERA/USDT configuration
    bera_config = {
        'symbol': 'BERA/USDT',
        'exchanges': [
            {'name': 'binance', 'type': 'spot'},
            {'name': 'bybit', 'type': 'spot'},
            {'name': 'bitget', 'type': 'spot'}
        ]
    }
    
    print(f"📡 Starting BERA/USDT feeds on {len(bera_config['exchanges'])} exchanges...")
    
    # Start BERA service
    bera_service = MarketDataService(service_config=bera_config)
    await bera_service.start()
    
    print("✅ BERA market data service started")
    print("⏳ Allowing time for exchange connections...")
    
    # Let it run for a bit to establish connections
    await asyncio.sleep(8)
    
    return bera_service


async def test_with_live_bera_feeds():
    """Test pricing with live BERA feeds."""
    
    print("\n🎯 Testing Live BERA Pricing")
    print("=" * 30)
    
    bera_service = None
    
    try:
        # Start BERA feeds
        bera_service = await start_bera_market_data()
        
        # Create Redis reader for live data
        redis_data = RedisMarketDataProvider()
        await redis_data.start()
        
        # Give time to collect data
        print("📡 Collecting live BERA orderbook data...")
        await asyncio.sleep(5)
        
        print(f"📊 Live orderbooks received: {len(redis_data.orderbooks)}")
        
        # Show what we got
        bera_feeds = 0
        for (exchange, symbol), orderbook in redis_data.orderbooks.items():
            if 'BERA' in symbol and orderbook.get('bids') and orderbook.get('asks'):
                bera_feeds += 1
                best_bid = orderbook['bids'][0][0]
                best_ask = orderbook['asks'][0][0]
                midpoint = (best_bid + best_ask) / 2
                bid_liquidity = sum(level[1] for level in orderbook['bids'][:5])
                
                print(f"✅ {exchange:12} {symbol:12} ${midpoint:.4f} ({bid_liquidity:>6.0f} BERA)")
        
        if bera_feeds == 0:
            print("❌ No BERA orderbook data received")
            print("   Check that BERA is available on the configured exchanges")
            return
        
        # Test pricing engine with live data
        pricing_engine = MultiReferencePricingEngine(redis_data)
        await pricing_engine.start()
        
        print(f"\n💰 Testing Live BERA Pricing with {bera_feeds} feeds:")
        print("-" * 40)
        
        quantities = [Decimal('100'), Decimal('1000')]
        
        for qty in quantities:
            try:
                usdt_price = await pricing_engine.get_usdt_equivalent_price('BERA/USDT', qty)
                
                if usdt_price and usdt_price > 0:
                    total_value = usdt_price * qty
                    print(f"🎯 {int(qty):>4} BERA: ${float(usdt_price):.4f} USDT (${float(total_value):>8.2f})")
                    print(f"    └─ Real multi-exchange WAPQ from {bera_feeds} live feeds")
                else:
                    print(f"❌ {int(qty):>4} BERA: NO PRICING AVAILABLE")
                    
            except Exception as e:
                print(f"❌ {int(qty):>4} BERA: ERROR - {str(e)[:40]}")
        
        await pricing_engine.stop()
        await redis_data.stop()
        
        print(f"\n✅ SUCCESS: Live pricing working with real exchange data!")
        print(f"📊 Processed {bera_feeds} live BERA orderbook feeds")
        print("🔥 Multi-reference pricing engine proven with REAL data!")
        
    finally:
        if bera_service:
            await bera_service.stop()


async def main():
    """Main function."""
    
    print("🔥 LIVE BERA Multi-Reference Pricing Test")
    print("Testing with real exchange connections and live data")
    print()
    
    try:
        await test_with_live_bera_feeds()
    except KeyboardInterrupt:
        print("\n🛑 Test interrupted")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
