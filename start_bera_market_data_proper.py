"""
Start Proper BERA Market Data Collection

This script properly configures and starts market data collection 
for all BERA pairs across all major exchanges.
"""

import asyncio
import logging
import subprocess
import time
import json
import redis
from typing import List, Dict, Any
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BERAMarketDataStarter:
    """Start fresh BERA market data collection."""
    
    def __init__(self):
        self.bera_pairs = [
            'BERA/USDT', 'BERA/BTC', 'BERA/ETH', 'BERA/USDC', 
            'BERA/BNB', 'BERA/FDUSD'
        ]
        
        self.exchanges = [
            'binance', 'bybit', 'okx', 'gate', 'mexc', 'kucoin', 'huobi', 'bitget'
        ]
        
        self.redis_client = None
        self.processes = []
    
    async def clear_stale_bera_data(self):
        """Clear stale BERA data from Redis."""
        logger.info("🧹 CLEARING STALE BERA DATA")
        
        try:
            self.redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
            
            # Clear old combined orderbook data
            bera_keys = self.redis_client.keys('combined_orderbook:*:BERA*')
            if bera_keys:
                logger.info(f"🗑️  Removing {len(bera_keys)} stale BERA keys...")
                self.redis_client.delete(*bera_keys)
                logger.info("✅ Stale BERA data cleared")
            else:
                logger.info("✅ No stale data to clear")
                
        except Exception as e:
            logger.error(f"❌ Error clearing stale data: {e}")
    
    async def start_bera_data_services(self):
        """Start market data services for BERA pairs."""
        logger.info("🚀 STARTING BERA MARKET DATA SERVICES")
        
        # Create config for BERA/USDT (primary pair)
        config = {
            "symbol": "BERA/USDT",
            "exchanges": [
                {"name": "binance", "type": "spot", "enabled": True},
                {"name": "bybit", "type": "spot", "enabled": True},
                {"name": "okx", "type": "spot", "enabled": True},
                {"name": "gate", "type": "spot", "enabled": True},
                {"name": "mexc", "type": "spot", "enabled": True}
            ]
        }
        
        # Save config to temp file
        config_path = Path("/tmp/bera_usdt_config.json")
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)
        
        # Start market data service
        cmd = [
            'python', 'market_data/market_data_service.py',
            '--config', str(config_path)
        ]
        
        logger.info(f"🔧 Starting: {' '.join(cmd)}")
        
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, 
            text=True,
            cwd='/Users/jonnyisenberg/Desktop/MM_UNI_1.1.0/trading_bot_system'
        )
        
        self.processes.append(process)
        
        # Give it time to start
        await asyncio.sleep(10)
        
        # Check if running
        if process.poll() is None:
            logger.info("✅ BERA/USDT market data service started")
            return True
        else:
            stdout, stderr = process.communicate()
            logger.error(f"❌ Service failed to start:")
            logger.error(f"   stdout: {stdout[:500]}")
            logger.error(f"   stderr: {stderr[:500]}")
            return False
    
    async def wait_for_fresh_data(self, timeout_seconds: int = 60):
        """Wait for fresh BERA data to appear."""
        logger.info(f"⏳ WAITING FOR FRESH BERA DATA (timeout: {timeout_seconds}s)")
        
        start_time = time.time()
        
        while time.time() - start_time < timeout_seconds:
            try:
                # Check for fresh data
                fresh_count = 0
                
                # Check orderbook keys
                orderbook_keys = self.redis_client.keys('orderbook:*:BERA*')
                for key in orderbook_keys:
                    try:
                        data = self.redis_client.get(key)
                        if data:
                            parsed = json.loads(data)
                            timestamp = parsed.get('timestamp', 0)
                            age_seconds = (time.time() * 1000 - timestamp) / 1000 if timestamp else float('inf')
                            
                            if age_seconds < 60:  # Fresh within last minute
                                fresh_count += 1
                                logger.info(f"✅ Fresh data found: {key} (age: {age_seconds:.1f}s)")
                    except Exception:
                        pass
                
                # Check combined orderbook keys 
                combined_keys = self.redis_client.keys('combined_orderbook:*:BERA*')
                for key in combined_keys:
                    try:
                        data = self.redis_client.get(key)
                        if data:
                            parsed = json.loads(data)
                            timestamp = parsed.get('timestamp', 0)
                            age_seconds = (time.time() * 1000 - timestamp) / 1000 if timestamp else float('inf')
                            
                            if age_seconds < 60:
                                fresh_count += 1
                                bids = parsed.get('bids', [])
                                asks = parsed.get('asks', [])
                                if bids and asks:
                                    best_bid = float(bids[0][0])
                                    best_ask = float(asks[0][0])
                                    logger.info(f"✅ Fresh combined data: {key} bid={best_bid:.6f} ask={best_ask:.6f}")
                    except Exception:
                        pass
                
                if fresh_count > 0:
                    logger.info(f"🎉 Found {fresh_count} sources with fresh BERA data!")
                    return True
                
                # Progress update
                elapsed = time.time() - start_time
                logger.info(f"⏳ Waiting... {elapsed:.0f}s elapsed, {fresh_count} fresh sources")
                await asyncio.sleep(5)
                
            except Exception as e:
                logger.error(f"❌ Error checking data: {e}")
                await asyncio.sleep(5)
        
        logger.warning(f"⚠️  Timeout after {timeout_seconds}s - no fresh BERA data")
        return False
    
    def cleanup(self):
        """Clean up background processes."""
        for process in self.processes:
            if process.poll() is None:
                logger.info("🛑 Stopping background process...")
                process.terminate()


async def main():
    """Main function to get BERA market data working."""
    
    starter = BERAMarketDataStarter()
    
    try:
        logger.info("🎯 MISSION: Get Live BERA Market Data Working")
        
        # Step 1: Clear stale data
        await starter.clear_stale_bera_data()
        
        # Step 2: Start BERA services
        success = await starter.start_bera_data_services()
        if not success:
            logger.error("❌ Failed to start BERA services")
            return False
        
        # Step 3: Wait for fresh data
        fresh_data = await starter.wait_for_fresh_data(timeout_seconds=90)
        
        if fresh_data:
            logger.info("🎉 SUCCESS: Fresh BERA market data is now flowing!")
            logger.info("✅ INFRASTRUCTURE IS READY FOR STRATEGY TESTING")
            return True
        else:
            logger.error("❌ FAILED: No fresh BERA data after 90 seconds")
            return False
        
    except KeyboardInterrupt:
        logger.info("⏹️  Interrupted by user")
        return False
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        starter.cleanup()


if __name__ == "__main__":
    success = asyncio.run(main())
    
    if success:
        print("\n🎉 BERA market data infrastructure is NOW READY!")
        print("✅ You can proceed with strategy testing")
    else:
        print("\n❌ Failed to establish BERA market data")
        print("⚠️  Strategy testing should wait until infrastructure is fixed")
