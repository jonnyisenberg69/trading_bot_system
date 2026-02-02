"""
Start Reference Pairs for Cross-Reference Pricing

To enable proper cross-reference calculations like:
BERA/BTC = x
BTC/USDT = y  
BERA/USDT = x * y

We need to collect the reference pairs (BTC/USDT, BNB/USDT, etc.)
"""

import asyncio
import json
import subprocess
import time
from pathlib import Path

async def start_reference_pairs():
    """Start market data collection for reference pairs needed for cross-pricing."""
    
    print("📊 STARTING REFERENCE PAIRS FOR CROSS-REFERENCE CALCULATIONS")
    print("="*70)
    
    # Reference pairs needed for BERA cross-calculations
    reference_pairs_by_exchange = {
        'binance': [
            'BTC/USDT',   # For BERA/BTC conversion
            'BNB/USDT',   # For BERA/BNB conversion  
            'TRY/USDT',   # For BERA/TRY conversion
            'FDUSD/USDT', # For BERA/FDUSD conversion
            'USDC/USDT'   # For BERA/USDC conversion
        ],
        'bybit': [
            'USDC/USDT'   # For BERA/USDC conversion
        ],
        'mexc': [
            'USDC/USDT'   # For BERA/USDC conversion
        ],
        'hyperliquid': [
            'USDC/USDT'   # For BERA/USDC conversion
        ]
    }
    
    config_dir = Path('data/market_data_services')
    started_services = []
    
    for exchange, pairs in reference_pairs_by_exchange.items():
        print(f"\n🏢 Starting {exchange} reference pairs...")
        
        for symbol in pairs:
            try:
                # Create config for reference pair
                if exchange == 'hyperliquid':
                    auth = {
                        'api_key': '0xccFBeA3725fd479D574863c85b933C17E4B40116',
                        'api_secret': '0xe0894c5fc0d90670844a348795669a6f29f43ff7849c911cc08d139711836fc9',
                        'wallet_address': '0xbA52b1BD928d1a471030d3D4F7BB1991c76679C4',
                        'private_key': '7918a7d4d79f79ac045ea54f7e1ee65c909113d1f742fd3247034a980f8c962c',
                        'passphrase': ''
                    }
                else:
                    auth = {'api_key': '', 'api_secret': '', 'passphrase': ''}
                
                config = {
                    "symbol": symbol,
                    "exchanges": [{
                        "name": exchange,
                        "type": "spot",
                        "api_key": auth['api_key'],
                        "api_secret": auth['api_secret'],
                        "testnet": False,
                        "wallet_address": auth.get('wallet_address', ''),
                        "private_key": auth.get('private_key', ''),
                        "passphrase": auth['passphrase']
                    }],
                    "redis_url": "redis://localhost:6379"
                }
                
                # Create config file
                safe_symbol = symbol.replace('/', '_').lower()
                config_filename = f"ref_{exchange}_{safe_symbol}.json"
                config_path = config_dir / config_filename
                
                with open(config_path, 'w') as f:
                    json.dump(config, f, indent=2)
                
                # Start service
                cmd = [
                    'bash', '-c',
                    f'cd /Users/jonnyisenberg/Desktop/MM_UNI_1.1.0/trading_bot_system && source /Users/jonnyisenberg/Desktop/MM_UNI_1.1.0/.venv/bin/activate && python market_data/market_data_service.py --config {config_path} --account-hash ref_{exchange}_{safe_symbol} &'
                ]
                
                subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                started_services.append({'exchange': exchange, 'symbol': symbol})
                
                print(f"  🚀 Started: {exchange} {symbol}")
                await asyncio.sleep(3)
                
            except Exception as e:
                print(f"  ❌ Failed to start {exchange} {symbol}: {e}")
    
    print(f"\n✅ Started {len(started_services)} reference pair services")
    print(f"⏱️  Waiting 60 seconds for reference pairs to initialize...")
    
    await asyncio.sleep(60)
    
    # Validate reference pairs are now available
    import redis
    redis_client = redis.Redis(decode_responses=True)
    
    ref_keys = [k for k in redis_client.keys('orderbook:*') if 'BERA' not in k]
    print(f"\n📊 VALIDATION: {len(ref_keys)} reference pairs now available")
    
    if ref_keys:
        print(f"✅ Reference pairs working:")
        for key in sorted(ref_keys)[:5]:
            print(f"  🔗 {key}")
        if len(ref_keys) > 5:
            print(f"     ... and {len(ref_keys) - 5} more")
    else:
        print(f"❌ No reference pairs available yet")
    
    return started_services

if __name__ == "__main__":
    asyncio.run(start_reference_pairs())
