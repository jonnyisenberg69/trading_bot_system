#!/usr/bin/env python

import asyncio
import ccxt.pro as ccxt

async def check_hyperliquid_symbols():
    """Check available symbols on Hyperliquid."""
    exchange = ccxt.hyperliquid({
        'enableRateLimit': True,
    })
    
    try:
        print("🔍 Loading Hyperliquid markets...")
        markets = await exchange.load_markets()
        print(f"📊 Total markets: {len(markets)}")
        
        # Look for BTC related symbols with detailed info
        btc_symbols = [symbol for symbol in markets.keys() if 'BTC' in symbol]
        print(f"🔸 BTC related symbols: {btc_symbols}")
        
        # Check each BTC symbol in detail
        for symbol in btc_symbols:
            market = markets[symbol]
            print(f"\n📋 Symbol: {symbol}")
            print(f"   Type: {market.get('type', 'N/A')}")
            print(f"   Active: {market.get('active', False)}")
            print(f"   Base: {market.get('base', 'N/A')}")
            print(f"   Quote: {market.get('quote', 'N/A')}")
            print(f"   Settle: {market.get('settle', 'N/A')}")
            print(f"   ID: {market.get('id', 'N/A')}")
            print(f"   Contract: {market.get('contract', False)}")
            print(f"   Linear: {market.get('linear', 'N/A')}")
        
        # Test the exact symbol that should work
        test_symbol = "BTC/USDC:USDC"
        if test_symbol in markets:
            print(f"\n✅ Found exact match: {test_symbol}")
            market = markets[test_symbol]
            print(f"   Market details: {market}")
        else:
            print(f"\n❌ {test_symbol} not found in markets")
            
        await exchange.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        await exchange.close()

if __name__ == "__main__":
    asyncio.run(check_hyperliquid_symbols()) 