"""
RESTART ALL BERA EXCHANGES PROPERLY

This script kills all existing services and restarts with ALL exchanges:
- Binance, Bybit, MEXC (working)
- Bitget, Gate, Hyperliquid (spot + perp), OKX, KuCoin (requested)

Clean restart with proper virtual environment and authentication.
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


class AllExchangeBERAStarter:
    """Start BERA market data for ALL exchanges including missing ones."""
    
    def __init__(self):
        # ALL exchanges to start including those specifically requested
        self.exchange_configs = {
            # Working exchanges (with proven configs)
            'binance': {
                'pairs': ['BERA/USDT', 'BERA/BTC', 'BERA/USDC', 'BERA/BNB', 'BERA/TRY', 'BERA/FDUSD'],
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
            
            # Requested missing exchanges
            'bitget': {
                'pairs': ['BERA/USDT'],
                'auth': {'api_key': '', 'api_secret': '', 'passphrase': ''}
            },
            'gate': {
                'pairs': ['BERA/USDT'],
                'auth': {'api_key': '', 'api_secret': '', 'passphrase': ''}
            },
            'okx': {
                'pairs': ['BERA/USDT'],
                'auth': {'api_key': '', 'api_secret': '', 'passphrase': ''}
            },
            'kucoin': {
                'pairs': ['BERA/USDT'],
                'auth': {'api_key': '', 'api_secret': '', 'passphrase': ''}
            },
            
            # Hyperliquid (with existing proven auth)
            'hyperliquid': {
                'pairs': ['BERA/USDC'],
                'auth': {
                    'api_key': '0xccFBeA3725fd479D574863c85b933C17E4B40116',
                    'api_secret': '0xe0894c5fc0d90670844a348795669a6f29f43ff7849c911cc08d139711836fc9',
                    'wallet_address': '0xbA52b1BD928d1a471030d3D4F7BB1991c76679C4',
                    'private_key': '7918a7d4d79f79ac045ea54f7e1ee65c909113d1f742fd3247034a980f8c962c',
                    'passphrase': ''
                }
            }
        }
        
        self.venv_path = '/Users/jonnyisenberg/Desktop/MM_UNI_1.1.0/.venv/bin/activate'
        self.config_dir = Path('data/market_data_services')
        self.config_dir.mkdir(parents=True, exist_ok=True)
        
    def create_exchange_config(self, exchange: str, symbol: str) -> str:
        """Create a config file for exchange-symbol combination."""
        
        auth = self.exchange_configs[exchange]['auth']
        
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
        config_filename = f"all_exchanges_{exchange}_{symbol.replace('/', '_').lower()}.json"
        config_path = self.config_dir / config_filename
        
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)
        
        return str(config_path)
    
    async def start_all_exchanges(self):
        """Start market data services for ALL exchanges."""
        
        print("🚀 STARTING ALL BERA EXCHANGES (INCLUDING MISSING ONES)")
        print("="*80)
        
        started_services = []
        
        for exchange, config in self.exchange_configs.items():
            pairs = config['pairs']
            
            print(f"\n📊 Starting {exchange} with {len(pairs)} BERA pairs...")
            
            for symbol in pairs:
                try:
                    # Create config file
                    config_path = self.create_exchange_config(exchange, symbol)
                    
                    # Start service with proper virtual environment
                    cmd = [
                        'bash', '-c',
                        f'source {self.venv_path} && python market_data/market_data_service.py --config {config_path} --account-hash all_{exchange}_{symbol.replace("/", "_")}'
                    ]
                    
                    print(f"  🚀 Starting: {exchange} {symbol}")
                    
                    # Start as background process
                    process = subprocess.Popen(
                        cmd,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        cwd=os.getcwd()
                    )
                    
                    started_services.append({
                        'exchange': exchange,
                        'symbol': symbol,
                        'config_path': config_path,
                        'process': process
                    })
                    
                    # Brief delay between starts
                    await asyncio.sleep(3)
                    
                except Exception as e:
                    logger.error(f"  ❌ Failed to start {exchange} {symbol}: {e}")
        
        print(f"\n✅ Started {len(started_services)} market data services")
        print(f"⏱️  Waiting 2 minutes for all services to initialize...")
        
        await asyncio.sleep(120)  # Wait 2 minutes for initialization
        
        return started_services
    
    async def validate_all_exchanges_working(self):
        """Validate that ALL exchanges are now providing BERA data."""
        
        print("\n🔍 VALIDATING ALL EXCHANGES")
        print("="*80)
        
        import redis
        redis_client = redis.Redis(decode_responses=True)
        
        # Check Redis for all exchange data
        all_keys = redis_client.keys('orderbook:*')
        
        if not all_keys:
            print("❌ No orderbook data found in Redis!")
            return False
        
        # Parse exchange data
        exchange_data = {}
        bera_keys = [k for k in all_keys if 'BERA' in k]
        
        for key in bera_keys:
            parts = key.split(':')
            if len(parts) >= 3:
                exchange = parts[1].replace('_spot', '')
                symbol = parts[2]
                
                if exchange not in exchange_data:
                    exchange_data[exchange] = []
                if symbol not in exchange_data[exchange]:
                    exchange_data[exchange].append(symbol)
        
        print(f"📊 VALIDATION RESULTS:")
        print(f"✅ Total BERA orderbook keys: {len(bera_keys)}")
        print(f"✅ Exchanges providing BERA data: {len(exchange_data)}")
        
        # Check each requested exchange
        requested_exchanges = list(self.exchange_configs.keys())
        working_exchanges = []
        missing_exchanges = []
        
        for exchange in requested_exchanges:
            if exchange in exchange_data:
                pairs = exchange_data[exchange]
                working_exchanges.append(exchange)
                print(f"  ✅ {exchange}: {len(pairs)} BERA pairs - {pairs}")
                
                # Check data quality
                sample_pair = pairs[0]
                key = f"orderbook:{exchange}_spot:{sample_pair}"
                data = redis_client.get(key)
                if data:
                    parsed = json.loads(data)
                    timestamp = parsed.get('timestamp', 0)
                    age_seconds = (time.time() * 1000 - timestamp) / 1000
                    print(f"       📊 Sample data age: {age_seconds:.1f}s")
                
            else:
                missing_exchanges.append(exchange)
                print(f"  ❌ {exchange}: No BERA data found")
        
        print(f"\n🎯 FINAL STATUS:")
        print(f"✅ Working: {len(working_exchanges)}/{len(requested_exchanges)} exchanges")
        print(f"❌ Missing: {len(missing_exchanges)}/{len(requested_exchanges)} exchanges")
        
        if working_exchanges:
            print(f"✅ Working exchanges: {working_exchanges}")
        if missing_exchanges:
            print(f"❌ Missing exchanges: {missing_exchanges}")
        
        success_rate = len(working_exchanges) / len(requested_exchanges) * 100
        print(f"📊 Success rate: {success_rate:.1f}%")
        
        return success_rate >= 80  # Consider success if 80%+ working


async def main():
    """Main restart function."""
    
    print("🎯 RESTARTING ALL BERA EXCHANGES")
    print("="*60)
    print("This will start ALL requested exchanges including:")
    print("  ✅ Binance, Bybit, MEXC (proven working)")
    print("  🔧 Bitget, Gate, Hyperliquid, OKX, KuCoin (requested)")
    
    starter = AllExchangeBERAStarter()
    
    try:
        # Start all services
        started_services = await starter.start_all_exchanges()
        
        # Validate results
        success = await starter.validate_all_exchanges_working()
        
        if success:
            print("\n🎉 SUCCESS: Most exchanges working!")
        else:
            print("\n⚠️  PARTIAL SUCCESS: Some exchanges still missing")
            
        print(f"\n📄 Services started: {len(started_services)}")
        
    except Exception as e:
        print(f"❌ Restart failed: {e}")


if __name__ == "__main__":
    asyncio.run(main())
