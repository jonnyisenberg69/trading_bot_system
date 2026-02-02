"""
Start Fresh BERA Market Data Feeds

This script starts clean market data collection for all BERA pairs
to ensure we have fresh, live pricing data.
"""

import asyncio
import logging
import subprocess
import time
import redis
import json
from typing import List, Dict, Any

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BERAMarketDataStarter:
    """Start fresh market data collection for BERA pairs."""
    
    def __init__(self):
        self.bera_pairs = [
            'BERA/USDT', 'BERA/BTC', 'BERA/ETH', 'BERA/USDC', 
            'BERA/BNB', 'BERA/SOL', 'BERA/MATIC', 'BERA/AVAX',
            'BERA/TRY', 'BERA/EUR', 'BERA/FDUSD'
        ]
        
        self.target_exchanges = [
            'binance', 'bybit', 'okx', 'gate', 'mexc', 
            'kucoin', 'huobi', 'bitget'
        ]
        
        self.redis_client = None
        self.aggregator_process = None
    
    async def clear_stale_data(self):
        """Clear stale market data from Redis."""
        logger.info("🧹 CLEARING STALE MARKET DATA")
        
        try:
            self.redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
            
            # Get all combined orderbook keys
            combined_keys = self.redis_client.keys('combined_orderbook:*')
            
            stale_keys = []
            for key in combined_keys:
                try:
                    data = self.redis_client.get(key)
                    if data:
                        parsed = json.loads(data)
                        timestamp = parsed.get('timestamp', 0)
                        age_hours = (time.time() * 1000 - timestamp) / (1000 * 3600) if timestamp else float('inf')
                        
                        if age_hours > 1:  # Data older than 1 hour
                            stale_keys.append(key)
                except Exception:
                    stale_keys.append(key)  # If can't parse, consider stale
            
            if stale_keys:
                logger.info(f"🗑️  Removing {len(stale_keys)} stale keys...")
                self.redis_client.delete(*stale_keys)
                logger.info("✅ Stale data cleared")
            else:
                logger.info("✅ No stale data to clear")
                
        except Exception as e:
            logger.error(f"❌ Error clearing stale data: {e}")
    
    async def start_aggregator_service(self):
        """Start the aggregator service for BERA pairs."""
        logger.info("🚀 STARTING FRESH AGGREGATOR SERVICE")
        
        try:
            # Start aggregator service
            cmd = [
                'python', '-m', 'market_data.aggregator_service',
                '--symbols'] + self.bera_pairs
            
            logger.info(f"🔧 Running: {' '.join(cmd)}")
            
            self.aggregator_process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                cwd='/Users/jonnyisenberg/Desktop/MM_UNI_1.1.0/trading_bot_system'
            )
            
            # Give it time to start
            await asyncio.sleep(5)
            
            # Check if it's running
            if self.aggregator_process.poll() is None:
                logger.info("✅ Aggregator service started successfully")
                return True
            else:
                stdout, stderr = self.aggregator_process.communicate()
                logger.error(f"❌ Aggregator service failed to start:")
                logger.error(f"   stdout: {stdout}")
                logger.error(f"   stderr: {stderr}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error starting aggregator service: {e}")
            return False
    
    async def wait_for_fresh_data(self, timeout_seconds: int = 60):
        """Wait for fresh market data to appear."""
        logger.info(f"⏳ WAITING FOR FRESH DATA (timeout: {timeout_seconds}s)")
        
        start_time = time.time()
        
        while time.time() - start_time < timeout_seconds:
            try:
                # Check for fresh BERA data
                bera_keys = [k for k in self.redis_client.keys('combined_orderbook:*') if 'BERA' in k]
                
                fresh_count = 0
                for key in bera_keys:
                    try:
                        data = self.redis_client.get(key)
                        if data:
                            parsed = json.loads(data)
                            timestamp = parsed.get('timestamp', 0)
                            age_seconds = (time.time() * 1000 - timestamp) / 1000 if timestamp else float('inf')
                            
                            if age_seconds < 30:  # Fresh data (less than 30 seconds old)
                                fresh_count += 1
                    except Exception:
                        pass
                
                if fresh_count > 0:
                    logger.info(f"🎉 Found {fresh_count} pairs with fresh data!")
                    return True
                
                # Show progress
                elapsed = time.time() - start_time
                logger.info(f"⏳ Waiting... {elapsed:.0f}s elapsed, {fresh_count} fresh pairs found")
                await asyncio.sleep(5)
                
            except Exception as e:
                logger.error(f"❌ Error checking for fresh data: {e}")
                await asyncio.sleep(5)
        
        logger.warning(f"⚠️  Timeout reached after {timeout_seconds}s")
        return False
    
    async def validate_final_state(self):
        """Validate the final market data state."""
        logger.info("🔍 FINAL VALIDATION")
        
        try:
            bera_keys = [k for k in self.redis_client.keys('combined_orderbook:*') if 'BERA' in k]
            
            fresh_pairs = []
            for key in bera_keys:
                try:
                    parts = key.split(':')
                    symbol = parts[2]
                    venue_type = parts[1]
                    
                    data = self.redis_client.get(key)
                    if data:
                        parsed = json.loads(data)
                        timestamp = parsed.get('timestamp', 0)
                        age_seconds = (time.time() * 1000 - timestamp) / 1000 if timestamp else float('inf')
                        
                        if age_seconds < 60:  # Fresh within last minute
                            bids = parsed.get('bids', [])
                            asks = parsed.get('asks', [])
                            
                            if bids and asks:
                                best_bid = float(bids[0][0])
                                best_ask = float(asks[0][0])
                                spread_bps = ((best_ask - best_bid) / best_bid) * 10000
                                
                                fresh_pairs.append({
                                    'symbol': symbol,
                                    'venue': venue_type,
                                    'bid': best_bid,
                                    'ask': best_ask,
                                    'spread_bps': round(spread_bps, 2),
                                    'age_seconds': round(age_seconds, 1)
                                })
                                
                                logger.info(
                                    f"✅ {symbol:12} ({venue_type:4}) "
                                    f"bid={best_bid:.6f} ask={best_ask:.6f} "
                                    f"spread={spread_bps:.1f}bps age={age_seconds:.1f}s"
                                )
                except Exception as e:
                    logger.error(f"❌ Error validating {key}: {e}")
            
            return fresh_pairs
            
        except Exception as e:
            logger.error(f"❌ Final validation failed: {e}")
            return []
    
    def cleanup(self):
        """Cleanup resources."""
        if self.aggregator_process and self.aggregator_process.poll() is None:
            logger.info("🛑 Stopping aggregator service...")
            self.aggregator_process.terminate()


async def main():
    """Main function to start fresh BERA market data."""
    logger.info("🧪 STARTING FRESH BERA MARKET DATA INFRASTRUCTURE")
    
    starter = BERAMarketDataStarter()
    
    try:
        # Step 1: Clear stale data
        await starter.clear_stale_data()
        
        # Step 2: Start aggregator service
        aggregator_started = await starter.start_aggregator_service()
        
        if not aggregator_started:
            logger.error("❌ Failed to start aggregator service")
            return False
        
        # Step 3: Wait for fresh data
        fresh_data_available = await starter.wait_for_fresh_data(timeout_seconds=120)
        
        if not fresh_data_available:
            logger.error("❌ No fresh data received within timeout")
            return False
        
        # Step 4: Final validation
        fresh_pairs = await starter.validate_final_state()
        
        if len(fresh_pairs) >= 3:
            logger.info(f"🎉 SUCCESS: {len(fresh_pairs)} BERA pairs with fresh market data!")
            logger.info("✅ INFRASTRUCTURE IS NOW READY FOR STRATEGY TESTING")
            return True
        else:
            logger.warning(f"⚠️  Only {len(fresh_pairs)} pairs with fresh data - may need more time")
            return False
        
    except KeyboardInterrupt:
        logger.info("⏹️  Interrupted by user")
        return False
    except Exception as e:
        logger.error(f"❌ Error in main: {e}")
        return False
    finally:
        starter.cleanup()


if __name__ == "__main__":
    success = asyncio.run(main())
    
    if success:
        print("\n🎉 BERA market data infrastructure is now ready!")
        print("✅ You can now test the stacked market making strategy")
    else:
        print("\n❌ Failed to establish fresh market data")
        print("⚠️  Strategy testing should wait until data issues are resolved")
