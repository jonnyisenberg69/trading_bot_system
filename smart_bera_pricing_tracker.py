"""
Smart BERA Pricing Tracker with Intelligent Autodiscovery

This uses autodiscovery to:
1. Find ALL available pairs (BERA pairs + reference pairs)
2. Intelligently determine conversion paths 
3. Handle both directions (USDT/USDC vs USDC/USDT)
4. Show proper cross-reference calculations

Format as requested:
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


class SmartBERAPricingTracker:
    """Smart pricing tracker with autodiscovery and intelligent conversion."""
    
    def __init__(self):
        self.redis_client = redis.Redis(decode_responses=True)
        self.discovered_pairs = {}
        self.reference_pairs = {}
        
    async def autodiscover_all_pairs(self) -> Dict[str, Dict[str, List[str]]]:
        """Autodiscover ALL available pairs (BERA + reference pairs) on each exchange."""
        
        print("🔍 AUTODISCOVERING ALL AVAILABLE PAIRS")
        print("="*60)
        
        all_keys = self.redis_client.keys('orderbook:*')
        
        # Parse all available pairs by exchange
        exchange_pairs = {}
        
        for key in all_keys:
            parts = key.split(':')
            if len(parts) >= 3:
                exchange = parts[1]
                symbol = parts[2]
                
                if exchange not in exchange_pairs:
                    exchange_pairs[exchange] = {'bera_pairs': [], 'reference_pairs': []}
                
                if 'BERA' in symbol:
                    if symbol not in exchange_pairs[exchange]['bera_pairs']:
                        exchange_pairs[exchange]['bera_pairs'].append(symbol)
                else:
                    # Check if it's a useful reference pair
                    if any(ref in symbol for ref in ['BTC', 'ETH', 'BNB', 'TRY', 'FDUSD', 'USDC', 'USDT']):
                        if symbol not in exchange_pairs[exchange]['reference_pairs']:
                            exchange_pairs[exchange]['reference_pairs'].append(symbol)
        
        print(f"📊 AUTODISCOVERY RESULTS:")
        for exchange, pairs in exchange_pairs.items():
            bera_count = len(pairs['bera_pairs'])
            ref_count = len(pairs['reference_pairs'])
            print(f"  🏢 {exchange}: {bera_count} BERA pairs, {ref_count} reference pairs")
            print(f"       BERA: {pairs['bera_pairs']}")
            print(f"       REF:  {pairs['reference_pairs'][:5]}{'...' if ref_count > 5 else ''}")
        
        self.discovered_pairs = exchange_pairs
        return exchange_pairs
    
    def find_conversion_path(self, base_currency: str, exchange: str) -> Optional[Tuple[str, str, bool]]:
        """
        Find the best conversion path to USDT for a given base currency.
        
        Returns: (reference_pair, conversion_rate_key, is_inverted)
        - reference_pair: The pair to use for conversion (e.g., 'BTC/USDT' or 'USDT/BTC')
        - conversion_rate_key: Redis key for the conversion rate
        - is_inverted: True if we need to invert the rate (1/rate)
        """
        
        if base_currency == 'USDT':
            return None  # Already USDT
        
        exchange_data = self.discovered_pairs.get(exchange, {})
        reference_pairs = exchange_data.get('reference_pairs', [])
        
        # Try direct pair first: BASE/USDT
        direct_pair = f"{base_currency}/USDT"
        direct_key = f"orderbook:{exchange}:{direct_pair}"
        
        if direct_pair in reference_pairs:
            return (direct_pair, direct_key, False)
        
        # Try inverted pair: USDT/BASE  
        inverted_pair = f"USDT/{base_currency}"
        inverted_key = f"orderbook:{exchange}:{inverted_pair}"
        
        if inverted_pair in reference_pairs:
            return (inverted_pair, inverted_key, True)
        
        # Try other exchanges for reference pair
        all_ref_keys = self.redis_client.keys(f'orderbook:*:{direct_pair}')
        if all_ref_keys:
            return (direct_pair, all_ref_keys[0], False)
        
        all_inv_keys = self.redis_client.keys(f'orderbook:*:{inverted_pair}')
        if all_inv_keys:
            return (inverted_pair, all_inv_keys[0], True)
        
        return None
    
    def get_pair_price(self, redis_key: str) -> Optional[Decimal]:
        """Get mid price for a pair from Redis."""
        
        data = self.redis_client.get(redis_key)
        if not data:
            return None
        
        try:
            parsed = json.loads(data)
            bids = parsed.get('bids', [])
            asks = parsed.get('asks', [])
            
            if bids and asks and bids[0] and asks[0]:
                bid_price = float(bids[0][0])
                ask_price = float(asks[0][0])
                mid_price = (bid_price + ask_price) / 2
                return Decimal(str(mid_price))
                
        except Exception as e:
            logger.warning(f"Error parsing price from {redis_key}: {e}")
        
        return None
    
    def calculate_usdt_equivalent(self, bera_price: Decimal, base_currency: str, exchange: str) -> Optional[Tuple[Decimal, str, str]]:
        """
        Calculate BERA/USDT equivalent price with detailed breakdown.
        
        Returns: (usdt_equivalent, reference_pair_used, calculation_details)
        """
        
        if base_currency == 'USDT':
            return (bera_price, 'Direct USDT pair', f'{bera_price:.8f}')
        
        # Find conversion path
        conversion_path = self.find_conversion_path(base_currency, exchange)
        
        if not conversion_path:
            return (None, 'No conversion path found', 'N/A')
        
        reference_pair, reference_key, is_inverted = conversion_path
        
        # Get reference rate
        reference_rate = self.get_pair_price(reference_key)
        
        if not reference_rate:
            return (None, f'{reference_pair} data not available', 'N/A')
        
        # Calculate USDT equivalent
        if is_inverted:
            # Reference pair is USDT/BASE, so BERA/USDT = BERA/BASE / (USDT/BASE) = BERA/BASE * (BASE/USDT)
            usdt_equivalent = bera_price / reference_rate
            calculation = f'{bera_price:.8f} ÷ {reference_rate:.8f} = {usdt_equivalent:.8f}'
        else:
            # Reference pair is BASE/USDT, so BERA/USDT = BERA/BASE * BASE/USDT
            usdt_equivalent = bera_price * reference_rate
            calculation = f'{bera_price:.8f} × {reference_rate:.8f} = {usdt_equivalent:.8f}'
        
        return (usdt_equivalent, reference_pair, calculation)
    
    async def demonstrate_smart_pricing_tracking(self):
        """Demonstrate smart pricing tracking with proper autodiscovery and conversion."""
        
        print("\n" + "="*100)
        print("💰 SMART BERA PRICING TRACKER - AUTODISCOVERY + INTELLIGENT CONVERSION")
        print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*100)
        
        # Autodiscover all available pairs
        discovered_pairs = await self.autodiscover_all_pairs()
        
        if not discovered_pairs:
            print("❌ No pairs discovered!")
            return
        
        print(f"\n💰 SMART PRICING TRACKING WITH CROSS-REFERENCE CALCULATIONS")
        print(f"🎯 Tracking for 2 minutes with intelligent conversion...")
        
        # Track for 2 minutes
        monitoring_duration = 120
        
        for second in range(monitoring_duration):
            await asyncio.sleep(1)
            
            if second % 30 == 0:  # Report every 30 seconds
                elapsed = second + 1
                print(f"\n⏱️  Second {elapsed}/{monitoring_duration} - SMART PRICING UPDATE:")
                
                for exchange, pair_data in discovered_pairs.items():
                    bera_pairs = pair_data['bera_pairs']
                    
                    if not bera_pairs:
                        continue
                    
                    print(f"\n🏢 Exchange: {exchange.replace('_', ' ').title()}")
                    
                    for symbol in sorted(bera_pairs):
                        print(f"  📈 Pair: {symbol}")
                        
                        # Get BERA pair price
                        bera_key = f"orderbook:{exchange}:{symbol}"
                        bera_price = self.get_pair_price(bera_key)
                        
                        if bera_price:
                            # Get data age
                            data = self.redis_client.get(bera_key)
                            age_seconds = 0
                            spread_bps = 0
                            
                            if data:
                                parsed = json.loads(data)
                                timestamp = parsed.get('timestamp', 0)
                                age_seconds = (time.time() * 1000 - timestamp) / 1000
                                
                                bids = parsed.get('bids', [])
                                asks = parsed.get('asks', [])
                                if bids and asks and bids[0] and asks[0]:
                                    spread_bps = ((float(asks[0][0]) - float(bids[0][0])) / float(bids[0][0])) * 10000
                            
                            print(f"    - {symbol} = {bera_price:.8f} (spread: {spread_bps:.1f}bps, age: {age_seconds:.1f}s)")
                            
                            # Calculate USDT equivalent if not already USDT pair
                            if not symbol.endswith('/USDT') and not symbol.endswith(':USDT'):
                                base_currency = symbol.split('/')[1].split(':')[0]  # Handle BERA/USDC:USDC format
                                
                                usdt_result = self.calculate_usdt_equivalent(bera_price, base_currency, exchange)
                                
                                if usdt_result[0]:  # Successfully calculated
                                    usdt_equivalent, reference_pair, calculation = usdt_result
                                    print(f"    - {reference_pair} = Used for conversion")
                                    print(f"    - BERA/USDT = {usdt_equivalent:.8f} (calculated: {calculation})")
                                else:
                                    reference_pair, error_msg = usdt_result[1], usdt_result[2]
                                    print(f"    - {reference_pair} = {error_msg}")
                                    print(f"    - BERA/USDT = Cannot calculate")
                            
                        else:
                            print(f"    - {symbol} = No price data available")
        
        print(f"\n✅ SMART PRICING TRACKING COMPLETE")
        
        # Show summary of conversion success
        print(f"\n🎯 CONVERSION SUMMARY:")
        total_conversions_attempted = 0
        successful_conversions = 0
        
        for exchange, pair_data in discovered_pairs.items():
            bera_pairs = pair_data['bera_pairs']
            
            for symbol in bera_pairs:
                if not symbol.endswith('/USDT') and not symbol.endswith(':USDT'):
                    total_conversions_attempted += 1
                    
                    bera_key = f"orderbook:{exchange}:{symbol}"
                    bera_price = self.get_pair_price(bera_key)
                    
                    if bera_price:
                        base_currency = symbol.split('/')[1].split(':')[0]
                        usdt_result = self.calculate_usdt_equivalent(bera_price, base_currency, exchange)
                        
                        if usdt_result[0]:
                            successful_conversions += 1
        
        conversion_rate = successful_conversions / total_conversions_attempted * 100 if total_conversions_attempted > 0 else 0
        print(f"✅ Successful conversions: {successful_conversions}/{total_conversions_attempted} ({conversion_rate:.1f}%)")
        
        return discovered_pairs


async def main():
    """Main smart pricing demonstration."""
    
    print("💰 SMART BERA PRICING TRACKER")
    print("="*60)
    print("Intelligent autodiscovery + proper conversion math")
    print("Handles USDT/USDC vs USDC/USDT automatically")
    
    tracker = SmartBERAPricingTracker()
    
    try:
        pricing_data = await tracker.demonstrate_smart_pricing_tracking()
        
        if pricing_data:
            print(f"\n🎉 SMART PRICING DEMONSTRATION SUCCESSFUL!")
            print(f"✅ Intelligent autodiscovery working")
            print(f"✅ Cross-reference calculations with proper math")
            print(f"✅ ALL available pairs tracked")
        
    except Exception as e:
        print(f"❌ Smart pricing demonstration failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
