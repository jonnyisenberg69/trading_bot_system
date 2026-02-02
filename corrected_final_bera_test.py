"""
CORRECTED Final BERA Pricing Test

Fix the validity check - most pairs are actually CORRECT:
- BERA/BTC = 0.000024 ✅ (Bitcoin pairs are small decimals)  
- BERA/BNB = 0.0030 ✅ (BNB pairs are small decimals)
- BERA/TRY = 106.78 ✅ (Turkish Lira pairs are large numbers)
- BERA/FDUSD = 2.59 ✅ (USD stablecoin pairs)
- BERA/USDC = 2.59 ✅ (USD stablecoin pairs)
- BERA/USDT = 2.59 ✅ (USD stablecoin pairs)

Only Hyperliquid spot has invalid format.
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


async def corrected_final_pricing_test():
    """Corrected final test with proper validity checks."""
    
    print("\n" + "="*100)
    print("🎯 CORRECTED FINAL BERA PRICING TEST - ALL EXCHANGES")
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("With corrected validity checks for different currency pair formats")
    print("="*100)
    
    redis_client = redis.Redis(decode_responses=True)
    
    # Get all current data
    all_keys = redis_client.keys('orderbook:*')
    bera_keys = [k for k in all_keys if 'BERA' in k]
    ref_keys = [k for k in all_keys if 'BERA' not in k]
    
    print(f"📊 INFRASTRUCTURE STATUS:")
    print(f"✅ Total BERA pairs: {len(bera_keys)}")
    print(f"✅ Total reference pairs: {len(ref_keys)}")
    
    # Parse by exchange
    exchange_bera = {}
    
    for key in bera_keys:
        parts = key.split(':')
        if len(parts) >= 3:
            exchange = parts[1]
            symbol = parts[2]
            
            if exchange not in exchange_bera:
                exchange_bera[exchange] = []
            exchange_bera[exchange].append(symbol)
    
    def get_price_details(redis_key: str) -> Tuple[Optional[Decimal], float, float]:
        """Get price, spread, and age."""
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
                spread_bps = ((ask_price - bid_price) / bid_price) * 10000 if bid_price > 0 else 0
                
                return Decimal(str(mid_price)), spread_bps, age_seconds
                
        except Exception as e:
            logger.warning(f"Error parsing {redis_key}: {e}")
        
        return None, 0, 999
    
    def is_price_format_valid(price: Decimal, symbol: str) -> bool:
        """Check if price format is valid for the specific pair type."""
        
        price_float = float(price)
        
        # Define expected ranges for different pair types
        if '/BTC' in symbol:
            return 0.00001 <= price_float <= 0.0001  # Bitcoin pairs are tiny
        elif '/ETH' in symbol:
            return 0.0001 <= price_float <= 0.01     # Ethereum pairs are small
        elif '/BNB' in symbol:
            return 0.001 <= price_float <= 0.01      # BNB pairs are small decimals
        elif '/TRY' in symbol:
            return 50 <= price_float <= 200          # Turkish Lira pairs are large
        elif any(stable in symbol for stable in ['/USDT', '/USDC', '/FDUSD']):
            return 1.0 <= price_float <= 5.0         # Stablecoin pairs around $2-3
        else:
            return True  # Unknown pair type, assume valid
    
    def find_reference_pair(base_currency: str) -> Optional[Tuple[str, bool]]:
        """Find reference pair for conversion."""
        
        if base_currency == 'USDT':
            return None
        
        # Try both directions
        direct_keys = redis_client.keys(f'orderbook:*:{base_currency}/USDT')
        inverted_keys = redis_client.keys(f'orderbook:*:USDT/{base_currency}')
        
        if direct_keys:
            return (direct_keys[0], False)  # Direct multiplication
        elif inverted_keys:
            return (inverted_keys[0], True)   # Inverted (division)
        
        return None
    
    def calculate_conversion(bera_price: Decimal, base_currency: str) -> Optional[Tuple[Decimal, str, str]]:
        """Calculate USDT equivalent."""
        
        if base_currency == 'USDT':
            return (bera_price, 'Direct USDT pair', f'{bera_price:.8f}')
        
        ref_result = find_reference_pair(base_currency)
        
        if not ref_result:
            return (None, f'No {base_currency}<->USDT pair', 'Not found')
        
        ref_key, is_inverted = ref_result
        ref_symbol = ref_key.split(':')[2]
        
        ref_price, _, _ = get_price_details(ref_key)
        
        if not ref_price:
            return (None, f'{ref_symbol} no data', 'Reference unavailable')
        
        try:
            if is_inverted:
                usdt_equivalent = bera_price / ref_price
                calculation = f'{bera_price:.8f} ÷ {ref_price:.8f} = {usdt_equivalent:.8f}'
            else:
                usdt_equivalent = bera_price * ref_price
                calculation = f'{bera_price:.8f} × {ref_price:.8f} = {usdt_equivalent:.8f}'
            
            return (usdt_equivalent, ref_symbol, calculation)
            
        except Exception as e:
            return (None, f'{ref_symbol} calc error', str(e)[:50])
    
    print(f"\n💰 CORRECTED COMPREHENSIVE PRICING:")
    
    total_valid_pairs = 0
    total_invalid_pairs = 0
    total_conversions = 0
    successful_conversions = 0
    
    for exchange in sorted(exchange_bera.keys()):
        symbols = exchange_bera[exchange]
        
        print(f"\n🏢 Exchange: {exchange.replace('_', ' ').title()}")
        
        for symbol in sorted(symbols):
            print(f"\n  📈 Pair: {symbol}")
            
            # Get BERA pair data
            bera_key = f"orderbook:{exchange}:{symbol}"
            bera_price, spread_bps, age_seconds = get_price_details(bera_key)
            
            if bera_price:
                is_valid = is_price_format_valid(bera_price, symbol)
                
                if is_valid:
                    total_valid_pairs += 1
                    status_icon = "✅"
                else:
                    total_invalid_pairs += 1
                    status_icon = "🚨"
                
                print(f"    - {symbol} = {bera_price:.8f} (spread: {spread_bps:.1f}bps, age: {age_seconds:.1f}s) {status_icon}")
                
                # Cross-reference calculation for non-USDT pairs
                if not symbol.endswith('/USDT') and not symbol.endswith(':USDT') and is_valid:
                    base_currency = symbol.split('/')[1].split(':')[0]
                    total_conversions += 1
                    
                    usdt_result = calculate_conversion(bera_price, base_currency)
                    
                    if usdt_result[0]:
                        successful_conversions += 1
                        usdt_equivalent, ref_pair, calculation = usdt_result
                        print(f"    - {ref_pair} = Used for conversion")
                        print(f"    - BERA/USDT = {usdt_equivalent:.8f} (calculated: {calculation})")
                    else:
                        error_pair, error_msg = usdt_result[1], usdt_result[2]
                        print(f"    - {error_pair} = {error_msg}")
                        print(f"    - BERA/USDT = Cannot calculate")
                
                # Show direct BERA/USDT comparison
                if not symbol.endswith('/USDT') and is_valid:
                    direct_key = f"orderbook:{exchange}:BERA/USDT"
                    direct_price, direct_spread, direct_age = get_price_details(direct_key)
                    if direct_price:
                        print(f"    - BERA/USDT = {direct_price:.8f} (direct: spread {direct_spread:.1f}bps, age {direct_age:.1f}s)")
                
                if not is_valid:
                    print(f"    ⚠️  Price format issue - likely wrong symbol or data corruption")
            else:
                print(f"    - {symbol} = No data available")
    
    # Final statistics
    total_pairs = len(bera_keys)
    conversion_rate = successful_conversions / total_conversions * 100 if total_conversions > 0 else 0
    validity_rate = total_valid_pairs / total_pairs * 100 if total_pairs > 0 else 0
    
    print(f"\n🎯 FINAL CORRECTED RESULTS:")
    print(f"✅ Valid BERA pairs: {total_valid_pairs}/{total_pairs} ({validity_rate:.1f}%)")
    print(f"✅ Invalid pairs: {total_invalid_pairs}/{total_pairs}")
    print(f"✅ Reference pairs: {len(ref_keys)}")
    print(f"✅ Cross-reference conversions: {successful_conversions}/{total_conversions} ({conversion_rate:.1f}%)")
    print(f"✅ Working exchanges: {len(exchange_bera)}")
    
    if total_invalid_pairs > 0:
        print(f"\n🔧 ISSUES TO FIX:")
        for exchange, symbols in exchange_bera.items():
            for symbol in symbols:
                bera_key = f"orderbook:{exchange}:{symbol}"
                bera_price, _, _ = get_price_details(bera_key)
                if bera_price and not is_price_format_valid(bera_price, symbol):
                    print(f"  🚨 {exchange} {symbol}: Price format issue (showing {bera_price:.8f})")
    
    print(f"\n🎉 COMPREHENSIVE BERA PRICING VALIDATION COMPLETE!")
    print(f"Ready for strategy testing with {total_valid_pairs} valid BERA pairs!")


if __name__ == "__main__":
    asyncio.run(corrected_final_pricing_test())
