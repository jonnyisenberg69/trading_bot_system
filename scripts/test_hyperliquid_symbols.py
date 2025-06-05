#!/usr/bin/env python
"""
Test script to check Hyperliquid symbols and find out why BTC/USDT isn't working.
"""

import asyncio
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from exchanges.connectors.hyperliquid_ws_connector import HyperliquidWSConnector

async def main():
    connector = HyperliquidWSConnector()
    
    try:
        await connector.initialize()
        
        # Get all symbols
        all_symbols = await connector.get_available_symbols('all')
        spot_symbols = await connector.get_available_symbols('spot')
        swap_symbols = await connector.get_available_symbols('swap')
        
        print(f"Hyperliquid has {len(all_symbols)} total symbols")
        print(f"Spot: {len(spot_symbols)}, Swap: {len(swap_symbols)}")
        
        # Check if BTC/USDT exists
        print(f"\nBTC/USDT supported: {await connector.is_symbol_supported('BTC/USDT')}")
        
        # Show first 10 spot symbols
        print(f"\nFirst 10 spot symbols: {spot_symbols[:10]}")
        
        # Show first 10 swap symbols  
        print(f"First 10 swap symbols: {swap_symbols[:10]}")
        
        # Check for BTC variants
        btc_symbols = [s for s in all_symbols if 'BTC' in s]
        print(f"\nBTC-related symbols: {btc_symbols[:10]}")
        
        # Try to get orderbook for the first available symbol
        if spot_symbols:
            test_symbol = spot_symbols[0]
            print(f"\nTesting orderbook for {test_symbol}...")
            try:
                orderbook = await connector.get_orderbook(test_symbol, 5)
                print(f"✅ Got orderbook for {test_symbol}")
                print(f"Bids: {len(orderbook.get('bids', []))}, Asks: {len(orderbook.get('asks', []))}")
            except Exception as e:
                print(f"❌ Failed to get orderbook for {test_symbol}: {e}")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await connector.close()

if __name__ == "__main__":
    asyncio.run(main()) 