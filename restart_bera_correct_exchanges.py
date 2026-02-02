"""
Restart BERA Market Data with CORRECT Exchanges Only

Start market data services for exchanges we actually have connectors for:
✅ Binance, Bybit, MEXC (working)
✅ Bitget, Gate (we have connectors!)  
✅ Hyperliquid SPOT AND PERP (both markets)
❌ Skip OKX, KuCoin (no connectors)
"""

import asyncio
import json
import logging
import subprocess
import time
import os
from pathlib import Path
from typing import Dict, List, Any

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CorrectExchangeBERAStarter:
    """Start BERA market data for exchanges we actually have connectors for."""
    
    def __init__(self):
        # ONLY exchanges we have connectors for
        self.exchange_configs = {
            # Proven working exchanges
            'binance': {
                'pairs': ['BERA/USDT', 'BERA/BTC', 'BERA/USDC'],
                'auth': {'api_key': '', 'api_secret': '', 'passphrase': ''}
            },
            'bybit': {
                'pairs': ['BERA/USDT', 'BERA/USDC'],
                'auth': {'api_key': '', 'api_secret': '', 'passphrase': ''}
            },
            'mexc': {
                'pairs': ['BERA/USDT', 'BERA/USDC'],
                'auth': {'api_key': '', 'api_secret': '', 'passphrase': ''}
            },
            
            # Exchanges we have connectors for but weren't working
            'bitget': {
                'pairs': ['BERA/USDT'],
                'auth': {'api_key': '', 'api_secret': '', 'passphrase': ''}
            },
            'gateio': {  # Note: file is gateio.py
                'pairs': ['BERA/USDT'],
                'auth': {'api_key': '', 'api_secret': '', 'passphrase': ''}
            },
            
            # Hyperliquid - BOTH spot AND perp as requested
            'hyperliquid': {
                'pairs': ['BERA/USDC'],  # Spot market
                'auth': {
                    'api_key': '0xccFBeA3725fd479D574863c85b933C17E4B40116',
                    'api_secret': '0xe0894c5fc0d90670844a348795669a6f29f43ff7849c911cc08d139711836fc9',
                    'wallet_address': '0xbA52b1BD928d1a471030d3D4F7BB1991c76679C4',
                    'private_key': '7918a7d4d79f79ac045ea54f7e1ee65c909113d1f742fd3247034a980f8c962c',
                    'passphrase': ''
                }
            },
            'hyperliquid_perp': {  # Separate config for perp
                'pairs': ['BERA/USDC:USDC'],  # Perp market
                'auth': {
                    'api_key': '0xccFBeA3725fd479D574863c85b933C17E4B40116',
                    'api_secret': '0xe0894c5fc0d90670844a348795669a6f29f43ff7849c911cc08d139711836fc9',
                    'wallet_address': '0xbA52b1BD928d1a471030d3D4F7BB1991c76679C4',
                    'private_key': '7918a7d4d79f79ac045ea54f7e1ee65c909113d1f742fd3247034a980f8c962c',
                    'passphrase': ''
                }
            }
        }
        
        self.config_dir = Path('data/market_data_services')
        self.config_dir.mkdir(parents=True, exist_ok=True)
        
    def create_exchange_config(self, exchange: str, symbol: str, market_type: str = 'spot') -> str:
        """Create config file for exchange-symbol combination."""
        
        auth = self.exchange_configs[exchange]['auth']
        
        config = {
            "symbol": symbol,
            "exchanges": [{
                "name": exchange.replace('_perp', ''),  # Remove _perp suffix for actual exchange name
                "type": market_type,
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
        safe_symbol = symbol.replace('/', '_').replace(':', '_').lower()
        config_filename = f"correct_{exchange}_{safe_symbol}.json"
        config_path = self.config_dir / config_filename
        
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)
        
        return str(config_path)
    
    async def start_correct_exchanges(self):
        """Start market data services for exchanges with available connectors."""
        
        print("🚀 STARTING BERA DATA WITH CORRECT EXCHANGES ONLY")
        print("="*80)
        print("✅ Exchanges with connectors: Binance, Bybit, MEXC, Bitget, Gate, Hyperliquid")
        print("❌ Skipping: OKX, KuCoin (no connectors)")
        print("🎯 Hyperliquid: BOTH spot AND perp markets")
        
        started_services = []
        
        for exchange_key, config in self.exchange_configs.items():
            pairs = config['pairs']
            
            # Determine market type
            if 'perp' in exchange_key:
                market_type = 'perp'
                exchange_name = exchange_key.replace('_perp', '')
            else:
                market_type = 'spot'
                exchange_name = exchange_key
            
            print(f"\n📊 Starting {exchange_name} ({market_type}) with {len(pairs)} BERA pairs...")
            
            for symbol in pairs:
                try:
                    # Create config file
                    config_path = self.create_exchange_config(exchange_key, symbol, market_type)
                    
                    # Start service
                    cmd = [
                        'bash', '-c',
                        f'cd /Users/jonnyisenberg/Desktop/MM_UNI_1.1.0/trading_bot_system && source /Users/jonnyisenberg/Desktop/MM_UNI_1.1.0/.venv/bin/activate && python market_data/market_data_service.py --config {config_path} --account-hash correct_{exchange_name}_{symbol.replace("/", "_").replace(":", "_")}'
                    ]
                    
                    print(f"  🚀 Starting: {exchange_name} {symbol} ({market_type})")
                    
                    # Start as background process
                    process = subprocess.Popen(
                        cmd,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE
                    )
                    
                    started_services.append({
                        'exchange': exchange_name,
                        'symbol': symbol,
                        'market_type': market_type,
                        'config_path': config_path,
                        'process': process
                    })
                    
                    # Brief delay between starts
                    await asyncio.sleep(5)
                    
                except Exception as e:
                    logger.error(f"  ❌ Failed to start {exchange_name} {symbol}: {e}")
        
        print(f"\n✅ Started {len(started_services)} correct market data services")
        return started_services
    
    async def wait_and_validate(self, started_services: List[Dict]):
        """Wait for services to initialize and validate results."""
        
        print(f"\n⏱️  Waiting 2 minutes for services to initialize...")
        await asyncio.sleep(120)
        
        print(f"\n🔍 VALIDATING CORRECT EXCHANGES")
        print("="*60)
        
        import redis
        redis_client = redis.Redis(decode_responses=True)
        
        # Check what's actually working
        all_keys = redis_client.keys('orderbook:*')
        bera_keys = [k for k in all_keys if 'BERA' in k]
        
        print(f"📊 Total BERA orderbook keys: {len(bera_keys)}")
        
        # Parse exchange data
        exchange_data = {}
        for key in bera_keys:
            parts = key.split(':')
            if len(parts) >= 3:
                exchange = parts[1]
                symbol = parts[2]
                
                if exchange not in exchange_data:
                    exchange_data[exchange] = []
                if symbol not in exchange_data[exchange]:
                    exchange_data[exchange].append(symbol)
        
        # Check each started service
        working_count = 0
        failed_count = 0
        
        print(f"\n📈 VALIDATION RESULTS:")
        
        for service in started_services:
            exchange = service['exchange']
            symbol = service['symbol']
            market_type = service['market_type']
            
            # Look for data in Redis
            expected_key_spot = f"orderbook:{exchange}_spot:{symbol}"
            expected_key_perp = f"orderbook:{exchange}_perp:{symbol}"
            
            found_data = False
            
            if expected_key_spot in bera_keys:
                found_data = True
                data = redis_client.get(expected_key_spot)
                if data:
                    parsed = json.loads(data)
                    age = (time.time() * 1000 - parsed.get('timestamp', 0)) / 1000
                    print(f"  ✅ {exchange:10} {symbol:12} (spot): Fresh data, age={age:.1f}s")
                    working_count += 1
                else:
                    print(f"  ⚠️  {exchange:10} {symbol:12} (spot): Key exists but no data")
                    
            elif expected_key_perp in bera_keys:
                found_data = True
                data = redis_client.get(expected_key_perp)
                if data:
                    parsed = json.loads(data)
                    age = (time.time() * 1000 - parsed.get('timestamp', 0)) / 1000
                    print(f"  ✅ {exchange:10} {symbol:12} (perp): Fresh data, age={age:.1f}s")
                    working_count += 1
                else:
                    print(f"  ⚠️  {exchange:10} {symbol:12} (perp): Key exists but no data")
            
            if not found_data:
                print(f"  ❌ {exchange:10} {symbol:12} ({market_type}): No data found")
                failed_count += 1
        
        # Summary
        total_services = len(started_services)
        success_rate = working_count / total_services * 100 if total_services > 0 else 0
        
        print(f"\n🎯 FINAL VALIDATION:")
        print(f"✅ Working services: {working_count}/{total_services} ({success_rate:.1f}%)")
        print(f"❌ Failed services: {failed_count}/{total_services}")
        
        if exchange_data:
            print(f"\n📊 LIVE EXCHANGES:")
            for exchange, symbols in exchange_data.items():
                print(f"  🔥 {exchange}: {len(symbols)} pairs - {symbols}")
        
        return working_count, failed_count


async def main():
    """Main restart function with correct exchanges."""
    
    print("🎯 RESTARTING BERA WITH CORRECT EXCHANGES")
    print("="*60)
    print("Based on available connectors:")
    print("  ✅ Binance, Bybit, MEXC (proven)")
    print("  ✅ Bitget, Gate (have connectors)")  
    print("  ✅ Hyperliquid spot + perp (both markets)")
    print("  ❌ Skip OKX, KuCoin (no connectors)")
    
    starter = CorrectExchangeBERAStarter()
    
    try:
        # Start services
        started_services = await starter.start_correct_exchanges()
        
        # Wait and validate
        working, failed = await starter.wait_and_validate(started_services)
        
        if working >= failed:
            print(f"\n🎉 SUCCESS: Majority of exchanges working!")
        else:
            print(f"\n⚠️  ISSUES: More failures than successes")
            
    except Exception as e:
        print(f"❌ Restart failed: {e}")


if __name__ == "__main__":
    asyncio.run(main())
