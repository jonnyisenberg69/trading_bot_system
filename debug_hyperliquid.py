"""
Debug Hyperliquid Pricing Issue

Check what Hyperliquid data actually looks like and fix the pricing format issue.
"""

import redis
import json

def debug_hyperliquid():
    """Debug Hyperliquid pricing data."""
    
    print("🔍 DEBUGGING HYPERLIQUID PRICING ISSUE")
    print("="*50)
    
    redis_client = redis.Redis(decode_responses=True)
    
    # Find all Hyperliquid keys
    hyper_keys = redis_client.keys('*hyperliquid*')
    
    print(f"📊 Found {len(hyper_keys)} Hyperliquid keys:")
    for key in hyper_keys:
        print(f"  🔑 {key}")
    
    # Check orderbook data specifically
    orderbook_keys = [k for k in hyper_keys if 'orderbook' in k]
    
    print(f"\n📈 Hyperliquid orderbook data:")
    for key in orderbook_keys:
        data = redis_client.get(key)
        if data:
            try:
                parsed = json.loads(data)
                bids = parsed.get('bids', [])
                asks = parsed.get('asks', [])
                timestamp = parsed.get('timestamp', 0)
                
                print(f"\n🔥 {key}:")
                print(f"   📊 Bids: {bids[:3] if bids else 'None'}")
                print(f"   📊 Asks: {asks[:3] if asks else 'None'}")
                print(f"   ⏱️  Timestamp: {timestamp}")
                
                if bids and asks:
                    best_bid = float(bids[0][0]) if bids[0] else 0
                    best_ask = float(asks[0][0]) if asks[0] else 0
                    mid_price = (best_bid + best_ask) / 2
                    
                    print(f"   💰 Best bid: {best_bid}")
                    print(f"   💰 Best ask: {best_ask}")
                    print(f"   💰 Mid price: {mid_price}")
                    print(f"   🚨 ISSUE: This should be ~2.58, not {mid_price}")
                
            except Exception as e:
                print(f"   ❌ Parse error: {e}")
        else:
            print(f"   ❌ No data for {key}")

if __name__ == "__main__":
    debug_hyperliquid()
