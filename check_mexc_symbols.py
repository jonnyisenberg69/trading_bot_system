#!/usr/bin/env python

import asyncio
import ccxt.pro as ccxt

async def check_mexc_symbols():
    """Check available symbols on MEXC."""
    exchange = ccxt.mexc({
        'enableRateLimit': True,
    })
    
    try:
        print("🔍 Loading MEXC markets...")
        markets = await exchange.load_markets()
        print(f"📊 Total markets: {len(markets)}")
        
        # Look for BTC related symbols
        btc_symbols = [symbol for symbol in markets.keys() if 'BTC' in symbol and 'USDT' in symbol]
        print(f"🔸 BTC/USDT related symbols: {btc_symbols[:10]}")
        
        # Check specific symbols with more detail
        test_symbols = ['BTC/USDT', 'ETH/USDT', 'BNB/USDT', 'ADA/USDT']
        print("\n🔍 Detailed symbol check:")
        for symbol in test_symbols:
            if symbol in markets:
                market_info = markets[symbol]
                active = market_info.get('active', False)
                status = "✅ ACTIVE" if active else "⚠️ INACTIVE"
                print(f"{status} {symbol}")
                print(f"   Market ID: {market_info.get('id', 'N/A')}")
                print(f"   Type: {market_info.get('type', 'N/A')}")
                print(f"   Base: {market_info.get('base', 'N/A')}")
                print(f"   Quote: {market_info.get('quote', 'N/A')}")
            else:
                print(f"❌ NOT FOUND: {symbol}")
        
        await exchange.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        await exchange.close()

if __name__ == "__main__":
    asyncio.run(check_mexc_symbols()) 