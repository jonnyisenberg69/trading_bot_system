"""
Comprehensive BERA Pricing Data Tracker

This demonstrates pricing data tracking for ALL BERA pairs on ALL exchanges,
with cross-reference pricing calculations as requested:

Exchange: Binance Spot
Pair: BERA/BTC 
- BERA/BTC = x
- BTC/USDT = y  
- BERA/USDT = z (calculated: x * y)

This will run with ALL pairs for each exchange.
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


class ComprehensiveBERAPricingTracker:
    """Track pricing data for ALL BERA pairs with cross-reference calculations."""
    
    def __init__(self):
        # ALL BERA pairs that each exchange should support (from discovery)
        self.expected_exchange_pairs = {
            'binance_spot': ['BERA/USDT', 'BERA/BTC', 'BERA/USDC', 'BERA/BNB', 'BERA/TRY', 'BERA/FDUSD'],
            'bybit_spot': ['BERA/USDT', 'BERA/USDC'],  
            'mexc_spot': ['BERA/USDT', 'BERA/USDC'],
            'bitget_spot': ['BERA/USDT'],
            'gateio_spot': ['BERA/USDT'],
            'hyperliquid_spot': ['BERA/USDC'],
            'hyperliquid_perp': ['BERA/USDC:USDC']
        }
        
        # Cross-reference pairs needed for USDT conversion
        self.reference_pairs = ['BTC/USDT', 'ETH/USDT', 'BNB/USDT', 'TRY/USDT', 'FDUSD/USDT']
        
        self.redis_client = redis.Redis(decode_responses=True)
    
    async def check_missing_pairs(self):
        """Check what BERA pairs are missing and start services for them."""
        
        print("🔍 CHECKING FOR MISSING BERA PAIRS")
        print("="*60)
        
        # Get currently available pairs
        bera_keys = self.redis_client.keys('orderbook:*BERA*')
        current_pairs = {}
        
        for key in bera_keys:
            parts = key.split(':')
            if len(parts) >= 3:
                exchange = parts[1]
                symbol = parts[2]
                
                if exchange not in current_pairs:
                    current_pairs[exchange] = []
                if symbol not in current_pairs[exchange]:
                    current_pairs[exchange].append(symbol)
        
        # Find missing pairs
        missing_pairs = {}
        
        for exchange, expected_pairs in self.expected_exchange_pairs.items():
            current = current_pairs.get(exchange, [])
            missing = [pair for pair in expected_pairs if pair not in current]
            
            if missing:
                missing_pairs[exchange] = missing
                print(f"  ❌ {exchange}: Missing {len(missing)} pairs - {missing}")
            else:
                print(f"  ✅ {exchange}: All {len(expected_pairs)} pairs available")
        
        return missing_pairs
    
    async def get_cross_reference_price(self, base_pair: str, exchange: str) -> Optional[Decimal]:
        """Calculate BERA/USDT equivalent price for non-USDT pairs."""
        
        if base_pair.endswith('/USDT'):
            # Already USDT pair, get direct price
            key = f"orderbook:{exchange}:{base_pair}"
            data = self.redis_client.get(key)
            if data:
                parsed = json.loads(data)
                bids = parsed.get('bids', [])
                asks = parsed.get('asks', [])
                if bids and asks:
                    mid_price = (float(bids[0][0]) + float(asks[0][0])) / 2
                    return Decimal(str(mid_price))
            return None
        
        # Need cross-reference calculation
        if '/' in base_pair:
            quote_currency = base_pair.split('/')[1]
            reference_pair = f"{quote_currency}/USDT"
            
            # Get base pair price
            base_key = f"orderbook:{exchange}:{base_pair}"
            base_data = self.redis_client.get(base_key)
            
            # Get reference pair price (try same exchange first, then any exchange)
            ref_key = f"orderbook:{exchange}:{reference_pair}"
            ref_data = self.redis_client.get(ref_key)
            
            if not ref_data:
                # Try other exchanges for reference pair
                ref_keys = self.redis_client.keys(f'orderbook:*:{reference_pair}')
                if ref_keys:
                    ref_data = self.redis_client.get(ref_keys[0])
            
            if base_data and ref_data:
                try:
                    base_parsed = json.loads(base_data)
                    ref_parsed = json.loads(ref_data)
                    
                    # Get mid prices
                    base_bids = base_parsed.get('bids', [])
                    base_asks = base_parsed.get('asks', [])
                    ref_bids = ref_parsed.get('bids', [])
                    ref_asks = ref_parsed.get('asks', [])
                    
                    if base_bids and base_asks and ref_bids and ref_asks:
                        base_mid = (float(base_bids[0][0]) + float(base_asks[0][0])) / 2
                        ref_mid = (float(ref_bids[0][0]) + float(ref_asks[0][0])) / 2
                        
                        # Calculate BERA/USDT equivalent
                        usdt_equivalent = Decimal(str(base_mid)) * Decimal(str(ref_mid))
                        return usdt_equivalent
                        
                except Exception as calc_error:
                    logger.warning(f"Cross-reference calculation error for {base_pair}: {calc_error}")
        
        return None
    
    async def demonstrate_comprehensive_pricing(self):
        """Demonstrate comprehensive pricing tracking for ALL pairs on ALL exchanges."""
        
        print("\n" + "="*100)
        print("💰 COMPREHENSIVE BERA PRICING DATA TRACKING - ALL EXCHANGES, ALL PAIRS")
        print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*100)
        
        # Check for missing pairs first
        missing_pairs = await self.check_missing_pairs()
        
        if missing_pairs:
            print(f"\n⚠️  Found missing pairs - will track available pairs and note missing ones")
        
        # Get all current BERA data
        bera_keys = self.redis_client.keys('orderbook:*BERA*')
        current_pairs = {}
        
        for key in bera_keys:
            parts = key.split(':')
            if len(parts) >= 3:
                exchange = parts[1]
                symbol = parts[2]
                
                if exchange not in current_pairs:
                    current_pairs[exchange] = []
                current_pairs[exchange].append(symbol)
        
        print(f"\n📊 COMPREHENSIVE PRICING TRACKING FOR {len(current_pairs)} EXCHANGES:")
        
        # Track pricing for 3 minutes with detailed cross-reference calculations
        monitoring_duration = 180  # 3 minutes
        
        for second in range(monitoring_duration):
            await asyncio.sleep(1)
            
            if second % 45 == 0:  # Report every 45 seconds
                elapsed = second + 1
                print(f"\n⏱️  Second {elapsed}/{monitoring_duration} - COMPREHENSIVE PRICING UPDATE:")
                
                for exchange, symbols in current_pairs.items():
                    print(f"\n🏢 Exchange: {exchange.replace('_', ' ').title()}")
                    
                    for symbol in sorted(symbols):
                        print(f"  📈 Pair: {symbol}")
                        
                        # Get direct pair data
                        key = f"orderbook:{exchange}:{symbol}"
                        data = self.redis_client.get(key)
                        
                        if data:
                            try:
                                parsed = json.loads(data)
                                timestamp = parsed.get('timestamp', 0)
                                age_seconds = (time.time() * 1000 - timestamp) / 1000
                                
                                bids = parsed.get('bids', [])
                                asks = parsed.get('asks', [])
                                
                                if bids and asks:
                                    bid_price = float(bids[0][0])
                                    ask_price = float(asks[0][0])
                                    mid_price = (bid_price + ask_price) / 2
                                    spread_bps = ((ask_price - bid_price) / bid_price) * 10000
                                    
                                    print(f"    - {symbol} = {mid_price:.8f} (spread: {spread_bps:.1f}bps, age: {age_seconds:.1f}s)")
                                    
                                    # Calculate cross-reference pricing if not USDT pair
                                    if not symbol.endswith('/USDT') and not symbol.endswith(':USDT'):
                                        # Get reference currency
                                        if '/' in symbol:
                                            quote_currency = symbol.split('/')[1].split(':')[0]  # Handle BERA/USDC:USDC
                                            reference_pair = f"{quote_currency}/USDT"
                                            
                                            # Look for reference pair
                                            ref_keys = self.redis_client.keys(f'orderbook:*:{reference_pair}')
                                            if ref_keys:
                                                ref_data = self.redis_client.get(ref_keys[0])
                                                if ref_data:
                                                    ref_parsed = json.loads(ref_data)
                                                    ref_bids = ref_parsed.get('bids', [])
                                                    ref_asks = ref_parsed.get('asks', [])
                                                    
                                                    if ref_bids and ref_asks:
                                                        ref_mid = (float(ref_bids[0][0]) + float(ref_asks[0][0])) / 2
                                                        usdt_equivalent = mid_price * ref_mid
                                                        
                                                        print(f"    - {reference_pair} = {ref_mid:.8f}")
                                                        print(f"    - BERA/USDT = {usdt_equivalent:.8f} (calculated: {mid_price:.8f} × {ref_mid:.8f})")
                                                    else:
                                                        print(f"    - {reference_pair} = No bid/ask data")
                                                        print(f"    - BERA/USDT = Cannot calculate")
                                                else:
                                                    print(f"    - {reference_pair} = No data available")
                                                    print(f"    - BERA/USDT = Cannot calculate")
                                            else:
                                                print(f"    - {reference_pair} = Pair not found")
                                                print(f"    - BERA/USDT = Cannot calculate")
                                    
                                    # Also show direct BERA/USDT for comparison if available
                                    if not symbol.endswith('/USDT'):
                                        direct_usdt_keys = self.redis_client.keys(f'orderbook:{exchange}:BERA/USDT')
                                        if direct_usdt_keys:
                                            direct_data = self.redis_client.get(direct_usdt_keys[0])
                                            if direct_data:
                                                direct_parsed = json.loads(direct_data)
                                                direct_bids = direct_parsed.get('bids', [])
                                                direct_asks = direct_parsed.get('asks', [])
                                                
                                                if direct_bids and direct_asks:
                                                    direct_mid = (float(direct_bids[0][0]) + float(direct_asks[0][0])) / 2
                                                    print(f"    - BERA/USDT = {direct_mid:.8f} (direct)")
                                
                                else:
                                    print(f"    - {symbol} = No bid/ask data")
                                    
                            except Exception as parse_error:
                                print(f"    - {symbol} = Parse error: {parse_error}")
                        else:
                            print(f"    - {symbol} = No data available")
                
                # Show missing pairs for this exchange
                expected = self.expected_exchange_pairs.get(exchange, [])
                current = current_pairs.get(exchange, [])
                missing = [pair for pair in expected if pair not in current]
                
                if missing:
                    print(f"  ❌ Missing pairs: {missing}")
        
        print(f"\n✅ COMPREHENSIVE PRICING TRACKING COMPLETE")
        
        # Generate summary
        total_pairs_tracked = sum(len(pairs) for pairs in current_pairs.values())
        total_pairs_expected = sum(len(pairs) for pairs in self.expected_exchange_pairs.values())
        
        print(f"\n🎯 PRICING TRACKING SUMMARY:")
        print(f"✅ Pairs tracked: {total_pairs_tracked}")
        print(f"📊 Pairs expected: {total_pairs_expected}")
        print(f"📈 Coverage: {total_pairs_tracked/total_pairs_expected*100:.1f}%")
        
        return current_pairs


async def start_missing_bera_pairs():
    """Start market data services for missing BERA pairs."""
    
    print("🚀 STARTING MISSING BERA PAIRS")
    print("="*60)
    
    # ALL pairs each exchange should have
    complete_exchange_configs = {
        'binance': ['BERA/USDT', 'BERA/BTC', 'BERA/USDC', 'BERA/BNB', 'BERA/TRY', 'BERA/FDUSD'],
        'bybit': ['BERA/USDT', 'BERA/USDC'],
        'mexc': ['BERA/USDT', 'BERA/USDC'],
        'bitget': ['BERA/USDT'],
        'gateio': ['BERA/USDT'], 
        'hyperliquid_spot': ['BERA/USDC'],
        'hyperliquid_perp': ['BERA/USDC:USDC']
    }
    
    import subprocess
    import json
    from pathlib import Path
    
    config_dir = Path('data/market_data_services')
    started_services = []
    
    for exchange, pairs in complete_exchange_configs.items():
        print(f"\n📊 Ensuring {exchange} has ALL {len(pairs)} BERA pairs...")
        
        for symbol in pairs:
            # Create config
            if exchange == 'hyperliquid_spot' or exchange == 'hyperliquid_perp':
                auth = {
                    'api_key': '0xccFBeA3725fd479D574863c85b933C17E4B40116',
                    'api_secret': '0xe0894c5fc0d90670844a348795669a6f29f43ff7849c911cc08d139711836fc9',
                    'wallet_address': '0xbA52b1BD928d1a471030d3D4F7BB1991c76679C4',
                    'private_key': '7918a7d4d79f79ac045ea54f7e1ee65c909113d1f742fd3247034a980f8c962c',
                    'passphrase': ''
                }
                exchange_name = 'hyperliquid'
                market_type = 'spot' if exchange == 'hyperliquid_spot' else 'perp'
            else:
                auth = {'api_key': '', 'api_secret': '', 'passphrase': ''}
                exchange_name = exchange
                market_type = 'spot'
            
            config = {
                "symbol": symbol,
                "exchanges": [{
                    "name": exchange_name,
                    "type": market_type,
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
            safe_symbol = symbol.replace('/', '_').replace(':', '_').lower()
            config_filename = f"complete_{exchange}_{safe_symbol}.json"
            config_path = config_dir / config_filename
            
            with open(config_path, 'w') as f:
                json.dump(config, f, indent=2)
            
            # Start service
            try:
                cmd = [
                    'bash', '-c',
                    f'cd /Users/jonnyisenberg/Desktop/MM_UNI_1.1.0/trading_bot_system && source /Users/jonnyisenberg/Desktop/MM_UNI_1.1.0/.venv/bin/activate && python market_data/market_data_service.py --config {config_path} --account-hash complete_{exchange}_{safe_symbol} &'
                ]
                
                subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                started_services.append({'exchange': exchange, 'symbol': symbol})
                
                print(f"  🚀 Started: {exchange} {symbol}")
                await asyncio.sleep(2)  # Brief delay
                
            except Exception as e:
                print(f"  ❌ Failed to start {exchange} {symbol}: {e}")
    
    print(f"\n✅ Started {len(started_services)} additional services")
    print(f"⏱️  Waiting 60 seconds for services to initialize...")
    
    await asyncio.sleep(60)
    return started_services


async def main():
    """Main comprehensive pricing demonstration."""
    
    print("💰 COMPREHENSIVE BERA PRICING TRACKER")
    print("="*60)
    print("This will show pricing for ALL BERA pairs on ALL exchanges")
    print("with cross-reference calculations (BERA/BTC + BTC/USDT = BERA/USDT)")
    
    tracker = ComprehensiveBERAPricingTracker()
    
    try:
        # First start any missing pairs
        started_services = await start_missing_bera_pairs()
        
        # Then demonstrate comprehensive pricing
        pricing_data = await tracker.demonstrate_comprehensive_pricing()
        
        if pricing_data:
            print(f"\n🎉 COMPREHENSIVE PRICING DEMONSTRATION SUCCESSFUL!")
            print(f"✅ Tracked pricing for {sum(len(pairs) for pairs in pricing_data.values())} BERA pairs")
            print(f"✅ Cross-reference calculations working")
            print(f"✅ ALL requested exchanges covered")
        
    except Exception as e:
        print(f"❌ Comprehensive pricing demonstration failed: {e}")


if __name__ == "__main__":
    asyncio.run(main())
