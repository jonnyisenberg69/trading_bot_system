"""
FINAL Comprehensive BERA Pricing Demonstration

Show ALL available BERA pairs on ALL exchanges with proper cross-reference calculations.
This addresses the user's request to see all pairs with the format:

Exchange: Binance Spot
Pair: BERA/BTC 
- BERA/BTC = x
- BTC/USDT = y
- BERA/USDT = z (calculated: x * y)
"""

import asyncio
import logging
import time
import json
from decimal import Decimal
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple
import redis

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def show_final_comprehensive_pricing():
    """Show final comprehensive pricing for ALL BERA pairs on ALL exchanges."""
    
    print("\n" + "="*100)
    print("💰 FINAL COMPREHENSIVE BERA PRICING DEMONSTRATION")
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Showing ALL available BERA pairs with cross-reference calculations")
    print("="*100)
    
    redis_client = redis.Redis(decode_responses=True)
    
    # Get all current data
    all_keys = redis_client.keys('orderbook:*')
    bera_keys = [k for k in all_keys if 'BERA' in k]
    ref_keys = [k for k in all_keys if 'BERA' not in k]
    
    print(f"📊 CURRENT DATA STATUS:")
    print(f"✅ Total BERA pairs: {len(bera_keys)}")
    print(f"✅ Total reference pairs: {len(ref_keys)}")
    
    # Parse by exchange
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
    
    def get_pair_price_with_details(redis_key: str) -> Tuple[Optional[Decimal], float, float]:
        """Get price, spread, and age for a pair."""
        data = redis_client.get(redis_key)
        if not data:
            return None, 0, 999
        
        try:
            parsed = json.loads(data)
            timestamp = parsed.get('timestamp', 0)
            age_seconds = (time.time() * 1000 - timestamp) / 1000
            
            bids = parsed.get('bids', [])
            asks = parsed.get('asks', [])
            
            if bids and asks and bids[0] and asks[0]:
                bid_price = float(bids[0][0])
                ask_price = float(asks[0][0])
                mid_price = (bid_price + ask_price) / 2
                spread_bps = ((ask_price - bid_price) / bid_price) * 10000
                
                return Decimal(str(mid_price)), spread_bps, age_seconds
                
        except Exception as e:
            logger.warning(f"Error parsing {redis_key}: {e}")
        
        return None, 0, 999
    
    def find_reference_pair(base_currency: str, exchange: str) -> Optional[str]:
        """Find reference pair for conversion to USDT."""
        
        if base_currency == 'USDT':
            return None
        
        # Try direct conversion pair
        direct_pair = f"{base_currency}/USDT"
        direct_key = f"orderbook:{exchange}:{direct_pair}"
        
        if direct_key in ref_keys:
            return direct_key
        
        # Try any exchange for this reference pair
        for key in ref_keys:
            if f":{direct_pair}" in key:
                return key
        
        return None
    
    print(f"\n💰 COMPREHENSIVE PRICING BY EXCHANGE:")
    
    for exchange, symbols in sorted(exchange_data.items()):
        print(f"\n🏢 Exchange: {exchange.replace('_', ' ').title()}")
        print(f"📊 BERA Pairs: {len(symbols)}")
        
        for symbol in sorted(symbols):
            print(f"\n  📈 Pair: {symbol}")
            
            # Get BERA pair data
            bera_key = f"orderbook:{exchange}:{symbol}"
            bera_price, spread_bps, age_seconds = get_pair_price_with_details(bera_key)
            
            if bera_price:
                print(f"    - {symbol} = {bera_price:.8f} (spread: {spread_bps:.1f}bps, age: {age_seconds:.1f}s)")
                
                # Cross-reference calculation if not USDT pair
                if not symbol.endswith('/USDT') and not symbol.endswith(':USDT'):
                    base_currency = symbol.split('/')[1].split(':')[0]
                    
                    ref_key = find_reference_pair(base_currency, exchange)
                    
                    if ref_key:
                        ref_price, ref_spread, ref_age = get_pair_price_with_details(ref_key)
                        
                        if ref_price:
                            ref_symbol = ref_key.split(':')[2]
                            usdt_equivalent = bera_price * ref_price
                            
                            print(f"    - {ref_symbol} = {ref_price:.8f} (spread: {ref_spread:.1f}bps, age: {ref_age:.1f}s)")
                            print(f"    - BERA/USDT = {usdt_equivalent:.8f} (calculated: {bera_price:.8f} × {ref_price:.8f})")
                        else:
                            print(f"    - {ref_key.split(':')[2]} = No price data")
                            print(f"    - BERA/USDT = Cannot calculate")
                    else:
                        print(f"    - {base_currency}/USDT = Reference pair not found")
                        print(f"    - BERA/USDT = Cannot calculate")
                
                # Show direct BERA/USDT for comparison if different pair
                if not symbol.endswith('/USDT'):
                    direct_usdt_key = f"orderbook:{exchange}:BERA/USDT"
                    if direct_usdt_key in bera_keys:
                        direct_price, direct_spread, direct_age = get_pair_price_with_details(direct_usdt_key)
                        if direct_price:
                            print(f"    - BERA/USDT = {direct_price:.8f} (direct: spread {direct_spread:.1f}bps, age {direct_age:.1f}s)")
            else:
                print(f"    - {symbol} = No price data available")
    
    # Summary
    total_bera_pairs = len(bera_keys)
    total_ref_pairs = len(ref_keys)
    
    print(f"\n🎯 FINAL COMPREHENSIVE PRICING SUMMARY:")
    print(f"✅ Total BERA pairs tracked: {total_bera_pairs}")
    print(f"✅ Total reference pairs available: {total_ref_pairs}")
    print(f"✅ Exchanges with BERA data: {len(exchange_data)}")
    
    # Calculate conversion success
    conversion_attempts = 0
    successful_conversions = 0
    
    for exchange, symbols in exchange_data.items():
        for symbol in symbols:
            if not symbol.endswith('/USDT') and not symbol.endswith(':USDT'):
                conversion_attempts += 1
                base_currency = symbol.split('/')[1].split(':')[0]
                ref_key = find_reference_pair(base_currency, exchange)
                if ref_key:
                    ref_price, _, _ = get_pair_price_with_details(ref_key)
                    if ref_price:
                        successful_conversions += 1
    
    conversion_rate = successful_conversions / conversion_attempts * 100 if conversion_attempts > 0 else 0
    print(f"✅ Cross-reference conversions: {successful_conversions}/{conversion_attempts} ({conversion_rate:.1f}%)")
    
    # Show missing pairs
    expected_binance_pairs = ['BERA/USDT', 'BERA/BTC', 'BERA/USDC', 'BERA/BNB', 'BERA/TRY', 'BERA/FDUSD']
    current_binance_pairs = exchange_data.get('binance_spot', [])
    missing_binance = [pair for pair in expected_binance_pairs if pair not in current_binance_pairs]
    
    if missing_binance:
        print(f"⚠️  Binance missing pairs: {missing_binance}")
    else:
        print(f"✅ Binance: ALL 6 pairs available!")


if __name__ == "__main__":
    asyncio.run(show_final_comprehensive_pricing())
