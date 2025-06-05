#!/usr/bin/env python
"""
Debug MEXC Symbols

Check available symbols on MEXC to find a working trading pair.
"""

import asyncio
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from exchanges.connectors import create_exchange_connector
from config.exchange_keys import get_exchange_config

async def debug_mexc_symbols():
    """Debug MEXC available symbols."""
    
    print("🔍 MEXC SYMBOL DEBUG")
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
        
        # Load markets
        markets = connector.exchange.markets
        print(f"📊 Total markets: {len(markets)}")
        
        # Test symbols that might work
        test_symbols = [
            'BERA/USDT',  # Current symbol
            'BTC/USDT',   # Most common
            'ETH/USDT',   # Second most common
            'USDT/USD',   # Stable pair
            'BNB/USDT',   # Binance coin
            'SOL/USDT',   # Solana
            'ADA/USDT',   # Cardano
            'DOT/USDT',   # Polkadot
            'MATIC/USDT', # Polygon
            'AVAX/USDT'   # Avalanche
        ]
        
        print(f"\n🧪 Testing symbols:")
        working_symbols = []
        
        for symbol in test_symbols:
            try:
                # Check if symbol exists in markets
                if symbol in markets:
                    market = markets[symbol]
                    is_active = market.get('active', False)
                    is_spot = market.get('type', '') == 'spot'
                    
                    print(f"  {symbol}: {'✅' if is_active and is_spot else '❌'} "
                          f"(active={is_active}, type={market.get('type', 'unknown')})")
                    
                    if is_active and is_spot:
                        working_symbols.append(symbol)
                else:
                    print(f"  {symbol}: ❌ Not found")
                    
            except Exception as e:
                print(f"  {symbol}: ❌ Error: {e}")
        
        print(f"\n✅ Working symbols: {working_symbols}")
        
        # Test actual trading on a working symbol
        if working_symbols:
            test_symbol = working_symbols[0]
            print(f"\n🧪 Testing orderbook for {test_symbol}:")
            
            try:
                orderbook = await connector.get_orderbook(test_symbol, limit=1)
                if orderbook and orderbook.get('bids') and orderbook.get('asks'):
                    bid = orderbook['bids'][0][0]
                    ask = orderbook['asks'][0][0]
                    spread = float(ask) - float(bid)
                    print(f"  ✅ Orderbook working: bid=${bid}, ask=${ask}, spread=${spread:.4f}")
                else:
                    print(f"  ❌ Orderbook failed")
            except Exception as e:
                print(f"  ❌ Orderbook error: {e}")
        
        # Show some available USDT pairs
        print(f"\n📋 Sample USDT pairs available:")
        usdt_pairs = [symbol for symbol in markets.keys() 
                     if '/USDT' in symbol and markets[symbol].get('active', False) 
                     and markets[symbol].get('type', '') == 'spot'][:15]
        
        for pair in usdt_pairs:
            market = markets[pair]
            print(f"  {pair} (active={market.get('active', False)})")
        
        await connector.disconnect()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")

if __name__ == "__main__":
    asyncio.run(debug_mexc_symbols()) 