"""
FINAL BERA Infrastructure Status Report

Simple validation of BERA market data readiness for strategy testing.
"""

import asyncio
import logging
import time
import json
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def final_bera_status_check():
    """Final comprehensive check of BERA infrastructure readiness."""
    
    print("\n" + "="*80)
    print("🎯 FINAL BERA INFRASTRUCTURE STATUS REPORT")
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    try:
        import redis
        redis_client = redis.Redis(decode_responses=True)
        
        # Check 1: Fresh BERA orderbook data
        print("\n📊 CHECKING FRESH BERA ORDERBOOK DATA:")
        orderbook_keys = redis_client.keys('orderbook:*:BERA*')
        fresh_sources = []
        
        for key in orderbook_keys:
            try:
                data = redis_client.get(key)
                if data:
                    parsed = json.loads(data)
                    timestamp = parsed.get('timestamp', 0)
                    age_seconds = (time.time() * 1000 - timestamp) / 1000 if timestamp else float('inf')
                    
                    best_bid = parsed.get('best_bid')
                    best_ask = parsed.get('best_ask')
                    
                    if best_bid and best_ask and age_seconds < 300:  # 5 minute threshold
                        fresh_sources.append({
                            'source': key,
                            'age': age_seconds,
                            'bid': float(best_bid),
                            'ask': float(best_ask)
                        })
                        
                        print(f"✅ {key}")
                        print(f"   Age: {age_seconds:.1f}s | Bid: {best_bid} | Ask: {best_ask}")
            except Exception as e:
                print(f"❌ {key}: Error parsing - {e}")
        
        # Check 2: Redis connectivity and general health
        print(f"\n🔌 REDIS CONNECTIVITY:")
        try:
            redis_client.ping()
            total_keys = len(redis_client.keys('*'))
            print(f"✅ Redis connected | Total keys: {total_keys}")
        except Exception as e:
            print(f"❌ Redis connection failed: {e}")
            return False
        
        # Check 3: Background market data processes
        print(f"\n🔄 MARKET DATA PROCESSES:")
        try:
            import subprocess
            result = subprocess.run(['ps', 'aux'], capture_output=True, text=True)
            market_data_lines = [line for line in result.stdout.split('\n') if 'market_data' in line and 'python' in line]
            print(f"✅ Found {len(market_data_lines)} market data processes running")
            for line in market_data_lines[:3]:  # Show first 3
                if 'BERA' in line or 'bera' in line:
                    print(f"   🎯 BERA service: {line.split()[-1]}")
                else:
                    print(f"   📊 Service: {line.split()[-1]}")
        except Exception as e:
            print(f"⚠️  Process check failed: {e}")
        
        # Check 4: Pricing prerequisites
        print(f"\n💰 PRICING PREREQUISITES:")
        
        # Check if we have viable bid/ask spreads
        pricing_ready = 0
        for source in fresh_sources:
            try:
                spread_bps = ((source['ask'] - source['bid']) / source['bid']) * 10000
                if 1 <= spread_bps <= 1000:  # Reasonable spread
                    pricing_ready += 1
                    print(f"✅ {source['source'].split(':')[-1]} - Spread: {spread_bps:.1f}bps ✅ Good for trading")
                else:
                    print(f"⚠️  {source['source'].split(':')[-1]} - Spread: {spread_bps:.1f}bps ⚠️  Too wide/narrow")
            except Exception as e:
                print(f"❌ Spread calculation error: {e}")
        
        # Final Assessment
        print(f"\n🎯 FINAL ASSESSMENT:")
        print(f"📊 Fresh data sources: {len(fresh_sources)}")
        print(f"💰 Trading-ready sources: {pricing_ready}")
        
        if len(fresh_sources) >= 2 and pricing_ready >= 1:
            print(f"\n🎉 STATUS: READY FOR STRATEGY TESTING")
            print(f"✅ Sufficient fresh BERA data available")
            print(f"✅ At least {pricing_ready} sources with good spreads")
            readiness = "READY"
        elif len(fresh_sources) >= 1:
            print(f"\n⚠️  STATUS: PARTIALLY READY")
            print(f"✅ Some fresh BERA data available")
            print(f"⚠️  Limited to basic testing")
            readiness = "PARTIAL"
        else:
            print(f"\n❌ STATUS: NOT READY")
            print(f"❌ No fresh BERA data available")
            readiness = "NOT_READY"
        
        print("="*80)
        
        return readiness, fresh_sources
        
    except Exception as e:
        print(f"❌ Status check failed: {e}")
        return "ERROR", []


async def main():
    """Run final status check."""
    
    readiness, sources = await final_bera_status_check()
    
    print(f"\n🎯 RECOMMENDATION:")
    
    if readiness == "READY":
        print(f"🎉 PROCEED with strategy testing!")
        print(f"✅ BERA infrastructure is ready")
        print(f"📝 Available sources: {[s['source'] for s in sources]}")
    elif readiness == "PARTIAL":
        print(f"⚠️  PROCEED with LIMITED testing")
        print(f"✅ Basic BERA infrastructure working")
        print(f"📝 Available sources: {[s['source'] for s in sources]}")
    else:
        print(f"❌ DO NOT proceed with strategy testing yet")
        print(f"🔧 Fix infrastructure issues first")
    
    return readiness


if __name__ == "__main__":
    status = asyncio.run(main())
    exit(0 if status in ["READY", "PARTIAL"] else 1)
