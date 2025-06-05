#!/usr/bin/env python
import asyncio
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from exchanges.connectors.hyperliquid import HyperliquidConnector
from config.exchange_keys import get_exchange_config

async def test_hyperliquid():
    config = get_exchange_config('hyperliquid_perp')
    connector = HyperliquidConnector(config)
    await connector.connect()
    
    # Check available markets
    exchange_info = await connector.get_exchange_info()
    symbols = exchange_info.get('symbols', [])
    print('Available symbols:', [s for s in symbols[:10] if 'BTC' in s])
    
    await connector.disconnect()

if __name__ == "__main__":
    asyncio.run(test_hyperliquid()) 