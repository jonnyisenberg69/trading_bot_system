#!/usr/bin/env python3
"""
Debug Redis data - Check what orderbook data is actually available in Redis
"""
import asyncio
import json
import redis.asyncio as redis
from datetime import datetime

async def check_redis_data():
    """Check what data is available in Redis."""
    client = redis.from_url("redis://localhost:6379")
    
    try:
        # Check if Redis is connected
        await client.ping()
        print("✅ Connected to Redis")
        
        # List all keys
        all_keys = await client.keys("*")
        print(f"\n📊 Total Redis keys: {len(all_keys)}")
        
        # Filter for orderbook-related keys
        orderbook_keys = [key for key in all_keys if b'orderbook' in key.lower()]
        print(f"📚 Orderbook-related keys: {len(orderbook_keys)}")
        
        for key in sorted(orderbook_keys[:20]):  # Show first 20
            key_str = key.decode('utf-8')
            print(f"  📖 {key_str}")
        
        # Check for BERA-related keys specifically
        bera_keys = [key for key in all_keys if b'bera' in key.lower() or b'BERA' in key]
        print(f"\n🐻 BERA-related keys: {len(bera_keys)}")
        
        for key in sorted(bera_keys[:10]):
            key_str = key.decode('utf-8')
            print(f"  🐻 {key_str}")
            
            # Get sample data
            try:
                data = await client.get(key)
                if data:
                    try:
                        json_data = json.loads(data)
                        if isinstance(json_data, dict):
                            # Show basic info
                            timestamp = json_data.get('timestamp', 'N/A')
                            symbol = json_data.get('symbol', 'N/A')
                            exchange = json_data.get('exchange', 'N/A')
                            bids = json_data.get('bids', [])
                            asks = json_data.get('asks', [])
                            print(f"    📊 {exchange} {symbol} - {len(bids)} bids, {len(asks)} asks, ts: {timestamp}")
                            
                            # Show best bid/ask if available
                            if bids and asks:
                                best_bid = bids[0][0] if bids[0] else None
                                best_ask = asks[0][0] if asks[0] else None
                                print(f"    💰 Best: {best_bid} / {best_ask}")
                    except json.JSONDecodeError:
                        print(f"    ❌ Non-JSON data: {data[:100]}...")
            except Exception as e:
                print(f"    ❌ Error reading key {key_str}: {e}")
        
        # Check for aggregated orderbook keys
        agg_keys = [key for key in all_keys if b'aggregated' in key.lower()]
        print(f"\n🔄 Aggregated keys: {len(agg_keys)}")
        for key in sorted(agg_keys[:10]):
            print(f"  🔄 {key.decode('utf-8')}")
        
        # Check for any subscription channels
        channels = await client.pubsub_channels("*orderbook*")
        print(f"\n📡 Active orderbook channels: {len(channels)}")
        for channel in sorted(channels[:10]):
            print(f"  📡 {channel.decode('utf-8')}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        await client.close()

if __name__ == "__main__":
    asyncio.run(check_redis_data())
