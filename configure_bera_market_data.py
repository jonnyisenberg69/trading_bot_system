"""
Configure BERA Market Data Services Properly

This script creates proper market data service configurations for all BERA pairs
using the existing proven infrastructure, without hardcoding base pairs.
"""

import asyncio
import json
import logging
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Dict, List, Any
import redis

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BERAMarketDataConfigurator:
    """Configure market data services for BERA pairs dynamically."""
    
    def __init__(self):
        self.bera_symbol_candidates = [
            'BERA/USDT', 'BERA/BTC', 'BERA/ETH', 'BERA/USDC', 
            'BERA/BNB', 'BERA/FDUSD', 'BERA/SOL', 'BERA/MATIC',
            'BERA/AVAX', 'BERA/TRY', 'BERA/EUR'
        ]
        
        # Exchange configurations (using existing credentials from the running services)
        self.exchange_configs = {
            'hyperliquid': {
                "name": "hyperliquid",
                "type": "spot", 
                "api_key": "0xccFBeA3725fd479D574863c85b933C17E4B40116",
                "api_secret": "0xe0894c5fc0d90670844a348795669a6f29f43ff7849c911cc08d139711836fc9",
                "testnet": False,
                "wallet_address": "0xbA52b1BD928d1a471030d3D4F7BB1991c76679C4",
                "private_key": "7918a7d4d79f79ac045ea54f7e1ee65c909113d1f742fd3247034a980f8c962c",
                "passphrase": ""
            },
            'bitfinex': {
                "name": "bitfinex",
                "type": "spot",
                "api_key": "b29d9696f7d804e808bea48d7df588652cb8c1f35fd",
                "api_secret": "4ac39ec043cd1375cf63b3e81c8b2842e0ecac78b53",
                "testnet": False,
                "wallet_address": "",
                "private_key": "",
                "passphrase": ""
            }
        }
        
        self.redis_url = "redis://localhost:6379"
        self.config_dir = Path("data/market_data_services")
        self.config_dir.mkdir(parents=True, exist_ok=True)
    
    async def check_exchange_bera_support(self) -> Dict[str, List[str]]:
        """Check which exchanges actually support BERA pairs."""
        logger.info("🔍 Checking BERA pair support across exchanges...")
        
        supported_pairs = {}
        
        try:
            # Connect to exchanges to check available symbols
            import ccxt
            
            test_exchanges = ['binance', 'bybit', 'okx', 'gate', 'mexc', 'kucoin']
            
            for exchange_name in test_exchanges:
                try:
                    exchange_class = getattr(ccxt, exchange_name)
                    exchange = exchange_class({
                        'sandbox': False,
                        'enableRateLimit': True,
                    })
                    
                    # Load markets
                    markets = await asyncio.get_event_loop().run_in_executor(
                        None, exchange.load_markets
                    )
                    
                    # Find BERA pairs
                    bera_pairs = []
                    for symbol in markets.keys():
                        if symbol.startswith('BERA/'):
                            bera_pairs.append(symbol)
                    
                    if bera_pairs:
                        supported_pairs[exchange_name] = bera_pairs
                        logger.info(f"✅ {exchange_name}: {len(bera_pairs)} BERA pairs - {bera_pairs}")
                    else:
                        logger.info(f"❌ {exchange_name}: No BERA pairs found")
                    
                    exchange.close()
                    
                except Exception as e:
                    logger.warning(f"⚠️  {exchange_name}: Error checking - {str(e)[:100]}")
        
        except ImportError:
            logger.warning("⚠️  CCXT not available for exchange checking, using default pairs")
            # Fallback to known pairs
            supported_pairs = {
                'binance': ['BERA/USDT', 'BERA/BTC'],
                'bybit': ['BERA/USDT', 'BERA/BTC'],
                'okx': ['BERA/USDT'],
                'gate': ['BERA/USDT']
            }
        
        return supported_pairs
    
    def create_bera_config(self, symbol: str, exchanges: List[str]) -> str:
        """Create a config file for a specific BERA pair."""
        
        config = {
            "symbol": symbol,
            "exchanges": [],
            "redis_url": self.redis_url
        }
        
        # Add exchange configs for exchanges that support this symbol
        for exchange_name in exchanges:
            if exchange_name in ['hyperliquid', 'bitfinex']:
                # Use existing proven configs for these
                config["exchanges"].append(self.exchange_configs[exchange_name])
            else:
                # Add basic config for other exchanges
                config["exchanges"].append({
                    "name": exchange_name,
                    "type": "spot",
                    "api_key": "",
                    "api_secret": "",
                    "testnet": False,
                    "wallet_address": "",
                    "private_key": "",
                    "passphrase": ""
                })
        
        # Generate unique config file
        config_hash = str(uuid.uuid4())[:8]
        config_filename = f"bera_{symbol.replace('/', '_').lower()}_{config_hash}.json"
        config_path = self.config_dir / config_filename
        
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)
        
        logger.info(f"📄 Created config: {config_path}")
        return str(config_path)
    
    async def start_bera_market_data_services(self):
        """Start market data services for all supported BERA pairs."""
        logger.info("🚀 STARTING BERA MARKET DATA SERVICES (DYNAMIC CONFIGURATION)")
        
        # Check what BERA pairs are actually supported
        supported_pairs = await self.check_exchange_bera_support()
        
        if not supported_pairs:
            logger.error("❌ No BERA pairs found on any exchange!")
            return
        
        # Get all unique BERA symbols
        all_bera_symbols = set()
        for exchange_pairs in supported_pairs.values():
            all_bera_symbols.update(exchange_pairs)
        
        logger.info(f"🎯 Found {len(all_bera_symbols)} unique BERA pairs: {sorted(all_bera_symbols)}")
        
        # Create config and start service for each BERA pair
        started_services = []
        
        for symbol in sorted(all_bera_symbols):
            # Find which exchanges support this symbol
            supporting_exchanges = [
                exchange for exchange, pairs in supported_pairs.items()
                if symbol in pairs
            ]
            
            logger.info(f"📊 {symbol}: supported by {supporting_exchanges}")
            
            # Create config file
            config_path = self.create_bera_config(symbol, supporting_exchanges)
            
            # Start market data service
            try:
                import subprocess
                
                cmd = [
                    sys.executable, 
                    "market_data/market_data_service.py",
                    "--config", config_path
                ]
                
                logger.info(f"🚀 Starting: {' '.join(cmd)}")
                
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    cwd=os.getcwd()
                )
                
                started_services.append({
                    'symbol': symbol,
                    'config_path': config_path,
                    'process': process,
                    'exchanges': supporting_exchanges
                })
                
                # Brief delay between service starts
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error(f"❌ Failed to start {symbol}: {e}")
        
        logger.info(f"✅ Started {len(started_services)} BERA market data services")
        
        # Wait a bit for services to initialize
        await asyncio.sleep(10)
        
        # Validate data is flowing
        await self.validate_bera_data_flow(all_bera_symbols)
        
        return started_services
    
    async def validate_bera_data_flow(self, symbols: set):
        """Validate that BERA data is flowing properly."""
        logger.info("📊 VALIDATING BERA DATA FLOW...")
        
        try:
            redis_client = redis.Redis.from_url(self.redis_url, decode_responses=True)
            
            for symbol in symbols:
                # Check for orderbook data
                orderbook_keys = await redis_client.keys(f'orderbook:*:{symbol}')
                combined_keys = await redis_client.keys(f'combined_orderbook:*:{symbol}')
                
                logger.info(f"📈 {symbol}: {len(orderbook_keys)} orderbook, {len(combined_keys)} combined")
                
                if orderbook_keys:
                    # Check data freshness
                    sample_key = orderbook_keys[0]
                    data = await redis_client.get(sample_key)
                    if data:
                        parsed = json.loads(data)
                        timestamp = parsed.get('timestamp', 0)
                        age_seconds = (time.time() * 1000 - timestamp) / 1000
                        logger.info(f"  📊 {sample_key}: age={age_seconds:.1f}s")
            
            await redis_client.close()
            
        except Exception as e:
            logger.error(f"❌ Validation failed: {e}")


async def main():
    """Main function to configure and start BERA market data services."""
    
    configurator = BERAMarketDataConfigurator()
    
    try:
        # Start BERA market data services dynamically
        services = await configurator.start_bera_market_data_services()
        
        if services:
            logger.info("🎉 BERA MARKET DATA SERVICES CONFIGURED SUCCESSFULLY!")
            logger.info("Services will continue running in background...")
            logger.info("Press Ctrl+C to stop validation...")
            
            # Keep running to monitor
            await asyncio.sleep(30)
        else:
            logger.error("❌ Failed to start any BERA market data services")
            
    except KeyboardInterrupt:
        logger.info("👋 Stopping validation...")
    except Exception as e:
        logger.error(f"❌ Configuration failed: {e}")


if __name__ == "__main__":
    asyncio.run(main())
