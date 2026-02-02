"""
Complete BERA Autodiscovery & Pricing Tracker

This script uses FULL autodiscovery to:
1. Find ALL possible reference pairs (TRY/USDT, USDT/TRY, BNB/USDT, USDT/BNB, etc.)
2. Check what BERA pairs are missing on each exchange
3. Start services for missing pairs
4. Keep testing and iterating until ALL BERA pairs on ALL exchanges work
5. Show proper cross-reference calculations for all pairs

Will iterate until complete!
"""

import asyncio
import logging
import time
import json
import subprocess
from decimal import Decimal
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple
import redis
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CompleteBERAAutodiscoveryTracker:
    """Complete BERA tracker with full autodiscovery and iteration."""
    
    def __init__(self):
        self.redis_client = redis.Redis(decode_responses=True)
        self.config_dir = Path('data/market_data_services')
        self.config_dir.mkdir(parents=True, exist_ok=True)
        
        # Expected BERA pairs per exchange (from CCXT discovery)
        self.expected_bera_pairs = {
            'binance': ['BERA/USDT', 'BERA/BTC', 'BERA/USDC', 'BERA/BNB', 'BERA/TRY', 'BERA/FDUSD'],
            'bybit': ['BERA/USDT', 'BERA/USDC'],
            'mexc': ['BERA/USDT', 'BERA/USDC'],
            'bitget': ['BERA/USDT'],
            'gateio': ['BERA/USDT'],
            'hyperliquid_spot': ['BERA/USDC'],
            'hyperliquid_perp': ['BERA/USDC:USDC']
        }
        
        # Currencies that need USDT reference pairs
        self.reference_currencies = ['BTC', 'ETH', 'BNB', 'TRY', 'FDUSD', 'USDC']
    
    async def autodiscover_reference_pairs(self) -> Dict[str, List[str]]:
        """Autodiscover ALL possible reference pairs in any direction."""
        
        print("🔍 AUTODISCOVERING ALL REFERENCE PAIRS (ANY DIRECTION)")
        print("="*70)
        
        try:
            import ccxt
            
            discovered_refs = {}
            
            for exchange_name in ['binance', 'bybit', 'mexc', 'bitget', 'gateio']:
                try:
                    exchange_class = getattr(ccxt, exchange_name)
                    exchange = exchange_class({'enableRateLimit': True})
                    
                    markets = await asyncio.get_event_loop().run_in_executor(
                        None, exchange.load_markets
                    )
                    
                    ref_pairs = []
                    
                    for symbol in markets.keys():
                        # Look for any combination of our reference currencies with USDT
                        for currency in self.reference_currencies:
                            if f"{currency}/USDT" == symbol or f"USDT/{currency}" == symbol:
                                ref_pairs.append(symbol)
                            # Also check for other common patterns
                            elif f"{currency}T/USDT" == symbol:  # Like USDT/USDT sometimes
                                ref_pairs.append(symbol)
                    
                    discovered_refs[exchange_name] = sorted(set(ref_pairs))
                    
                    if ref_pairs:
                        print(f"  ✅ {exchange_name}: {len(ref_pairs)} reference pairs")
                        for pair in ref_pairs[:5]:
                            print(f"       {pair}")
                        if len(ref_pairs) > 5:
                            print(f"       ... and {len(ref_pairs) - 5} more")
                    else:
                        print(f"  ❌ {exchange_name}: No reference pairs found")
                        
                except Exception as e:
                    print(f"  ❌ {exchange_name}: Discovery error - {str(e)[:60]}")
                    discovered_refs[exchange_name] = []
            
            return discovered_refs
            
        except ImportError:
            print("❌ CCXT not available for autodiscovery")
            return {}
    
    async def start_missing_reference_pairs(self, discovered_refs: Dict[str, List[str]]):
        """Start market data services for missing reference pairs."""
        
        print("\n🚀 STARTING MISSING REFERENCE PAIRS")
        print("="*50)
        
        # Check what reference pairs we currently have
        current_ref_keys = [k for k in self.redis_client.keys('orderbook:*') if 'BERA' not in k]
        current_refs = {}
        
        for key in current_ref_keys:
            parts = key.split(':')
            if len(parts) >= 3:
                exchange = parts[1].replace('_spot', '')
                symbol = parts[2]
                
                if exchange not in current_refs:
                    current_refs[exchange] = []
                current_refs[exchange].append(symbol)
        
        started_services = []
        
        for exchange, available_pairs in discovered_refs.items():
            current = current_refs.get(exchange, [])
            missing = [pair for pair in available_pairs if pair not in current]
            
            if missing:
                print(f"\n📊 {exchange}: Starting {len(missing)} missing reference pairs...")
                
                for symbol in missing[:3]:  # Limit to avoid too many services
                    try:
                        # Create config
                        config = {
                            "symbol": symbol,
                            "exchanges": [{
                                "name": exchange,
                                "type": "spot",
                                "api_key": "",
                                "api_secret": "",
                                "testnet": False,
                                "wallet_address": "",
                                "private_key": "",
                                "passphrase": ""
                            }],
                            "redis_url": "redis://localhost:6379"
                        }
                        
                        safe_symbol = symbol.replace('/', '_').lower()
                        config_filename = f"ref_{exchange}_{safe_symbol}_auto.json"
                        config_path = self.config_dir / config_filename
                        
                        with open(config_path, 'w') as f:
                            json.dump(config, f, indent=2)
                        
                        # Start service
                        cmd = [
                            'bash', '-c',
                            f'cd /Users/jonnyisenberg/Desktop/MM_UNI_1.1.0/trading_bot_system && source /Users/jonnyisenberg/Desktop/MM_UNI_1.1.0/.venv/bin/activate && python market_data/market_data_service.py --config {config_path} --account-hash ref_{exchange}_{safe_symbol}_auto &'
                        ]
                        
                        subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                        started_services.append({'exchange': exchange, 'symbol': symbol})
                        
                        print(f"  🚀 Started: {exchange} {symbol}")
                        await asyncio.sleep(3)
                        
                    except Exception as e:
                        print(f"  ❌ Failed to start {exchange} {symbol}: {e}")
            else:
                print(f"  ✅ {exchange}: All reference pairs available")
        
        return started_services
    
    async def start_missing_bera_pairs(self):
        """Start services for missing BERA pairs."""
        
        print("\n🚀 STARTING MISSING BERA PAIRS")
        print("="*50)
        
        # Check current BERA pairs
        bera_keys = self.redis_client.keys('orderbook:*BERA*')
        current_bera = {}
        
        for key in bera_keys:
            parts = key.split(':')
            if len(parts) >= 3:
                exchange = parts[1].replace('_spot', '').replace('_perp', '')
                symbol = parts[2]
                
                if exchange not in current_bera:
                    current_bera[exchange] = []
                current_bera[exchange].append(symbol)
        
        started_services = []
        
        for exchange, expected_pairs in self.expected_bera_pairs.items():
            current = current_bera.get(exchange.replace('_spot', '').replace('_perp', ''), [])
            missing = [pair for pair in expected_pairs if pair not in current]
            
            if missing:
                print(f"\n📊 {exchange}: Starting {len(missing)} missing BERA pairs...")
                
                for symbol in missing:
                    try:
                        # Create config
                        if 'hyperliquid' in exchange:
                            auth = {
                                'api_key': '0xccFBeA3725fd479D574863c85b933C17E4B40116',
                                'api_secret': '0xe0894c5fc0d90670844a348795669a6f29f43ff7849c911cc08d139711836fc9',
                                'wallet_address': '0xbA52b1BD928d1a471030d3D4F7BB1991c76679C4',
                                'private_key': '7918a7d4d79f79ac045ea54f7e1ee65c909113d1f742fd3247034a980f8c962c',
                                'passphrase': ''
                            }
                            exchange_name = 'hyperliquid'
                            market_type = 'spot' if 'spot' in exchange else 'perp'
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
                        
                        safe_symbol = symbol.replace('/', '_').replace(':', '_').lower()
                        config_filename = f"missing_{exchange}_{safe_symbol}.json"
                        config_path = self.config_dir / config_filename
                        
                        with open(config_path, 'w') as f:
                            json.dump(config, f, indent=2)
                        
                        # Start service
                        cmd = [
                            'bash', '-c',
                            f'cd /Users/jonnyisenberg/Desktop/MM_UNI_1.1.0/trading_bot_system && source /Users/jonnyisenberg/Desktop/MM_UNI_1.1.0/.venv/bin/activate && python market_data/market_data_service.py --config {config_path} --account-hash missing_{exchange}_{safe_symbol} &'
                        ]
                        
                        subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                        started_services.append({'exchange': exchange, 'symbol': symbol})
                        
                        print(f"  🚀 Started: {exchange} {symbol}")
                        await asyncio.sleep(3)
                        
                    except Exception as e:
                        print(f"  ❌ Failed to start {exchange} {symbol}: {e}")
            else:
                print(f"  ✅ {exchange}: All BERA pairs available")
        
        return started_services
    
    def find_smart_conversion_path(self, base_currency: str) -> Optional[Tuple[str, bool]]:
        """Smart autodiscovery of conversion paths in any direction."""
        
        if base_currency == 'USDT':
            return None
        
        # Check ALL possible reference pair combinations
        possible_pairs = [
            f"{base_currency}/USDT",  # Direct: BNB/USDT
            f"USDT/{base_currency}",  # Inverted: USDT/BNB
            f"{base_currency}T/USDT", # Tether variant: BNBT/USDT (sometimes exists)
            f"USDT/{base_currency}T"  # Inverted tether
        ]
        
        for pair_format in possible_pairs:
            # Look across ALL exchanges
            matching_keys = self.redis_client.keys(f'orderbook:*:{pair_format}')
            if matching_keys:
                is_inverted = pair_format.startswith('USDT/')
                return (matching_keys[0], is_inverted)
        
        return None
    
    def calculate_smart_usdt_equivalent(self, bera_price: Decimal, base_currency: str) -> Optional[Tuple[Decimal, str, str]]:
        """Smart calculation with autodiscovered reference pairs."""
        
        if base_currency == 'USDT':
            return (bera_price, 'Direct USDT pair', f'{bera_price:.8f}')
        
        conversion_path = self.find_smart_conversion_path(base_currency)
        
        if not conversion_path:
            return (None, f'No {base_currency}/USDT path found', 'Autodiscovery failed')
        
        ref_key, is_inverted = conversion_path
        ref_symbol = ref_key.split(':')[2]
        
        # Get reference price
        data = self.redis_client.get(ref_key)
        if not data:
            return (None, f'{ref_symbol} no data', 'Reference pair unavailable')
        
        try:
            parsed = json.loads(data)
            bids = parsed.get('bids', [])
            asks = parsed.get('asks', [])
            
            if bids and asks and bids[0] and asks[0]:
                ref_mid = (float(bids[0][0]) + float(asks[0][0])) / 2
                ref_price = Decimal(str(ref_mid))
                
                if is_inverted:
                    # USDT/BASE format, so BERA/USDT = BERA/BASE ÷ (USDT/BASE)
                    usdt_equivalent = bera_price / ref_price
                    calculation = f'{bera_price:.8f} ÷ {ref_price:.8f} = {usdt_equivalent:.8f}'
                else:
                    # BASE/USDT format, so BERA/USDT = BERA/BASE × BASE/USDT
                    usdt_equivalent = bera_price * ref_price
                    calculation = f'{bera_price:.8f} × {ref_price:.8f} = {usdt_equivalent:.8f}'
                
                return (usdt_equivalent, ref_symbol, calculation)
                
        except Exception as e:
            return (None, f'{ref_symbol} parse error', str(e)[:50])
        
        return (None, f'{ref_symbol} invalid data', 'Bad bid/ask data')
    
    async def comprehensive_status_check(self) -> Dict[str, Any]:
        """Check comprehensive status of all pairs and exchanges."""
        
        print("📊 COMPREHENSIVE STATUS CHECK")
        print("="*40)
        
        # Get current state
        all_keys = self.redis_client.keys('orderbook:*')
        bera_keys = [k for k in all_keys if 'BERA' in k]
        ref_keys = [k for k in all_keys if 'BERA' not in k]
        
        # Parse current BERA pairs
        current_bera = {}
        for key in bera_keys:
            parts = key.split(':')
            if len(parts) >= 3:
                exchange = parts[1]
                symbol = parts[2]
                
                if exchange not in current_bera:
                    current_bera[exchange] = []
                current_bera[exchange].append(symbol)
        
        # Parse current reference pairs
        current_refs = {}
        for key in ref_keys:
            parts = key.split(':')
            if len(parts) >= 3:
                exchange = parts[1]
                symbol = parts[2]
                
                if exchange not in current_refs:
                    current_refs[exchange] = []
                current_refs[exchange].append(symbol)
        
        # Calculate missing pairs
        missing_bera = {}
        for exchange, expected in self.expected_bera_pairs.items():
            current = current_bera.get(exchange, [])
            missing = [pair for pair in expected if pair not in current]
            if missing:
                missing_bera[exchange] = missing
        
        # Calculate missing references
        missing_refs = {}
        for currency in self.reference_currencies:
            found_anywhere = False
            for exchange, pairs in current_refs.items():
                if any(currency in pair for pair in pairs):
                    found_anywhere = True
                    break
            if not found_anywhere:
                missing_refs[currency] = f"No {currency}/USDT or USDT/{currency} found"
        
        status = {
            'total_bera_pairs': len(bera_keys),
            'total_ref_pairs': len(ref_keys),
            'current_bera': current_bera,
            'current_refs': current_refs,
            'missing_bera': missing_bera,
            'missing_refs': missing_refs
        }
        
        print(f"✅ BERA pairs: {len(bera_keys)}")
        print(f"✅ Reference pairs: {len(ref_keys)}")
        print(f"❌ Missing BERA pairs: {sum(len(m) for m in missing_bera.values())}")
        print(f"❌ Missing reference currencies: {len(missing_refs)}")
        
        return status
    
    async def demonstrate_complete_pricing(self):
        """Demonstrate complete pricing with ALL pairs and proper conversions."""
        
        print("\n💰 COMPLETE PRICING DEMONSTRATION")
        print("="*50)
        
        # Get current status
        status = await self.comprehensive_status_check()
        
        if not status['current_bera']:
            print("❌ No BERA pairs available!")
            return
        
        print(f"\n🎯 PRICING ALL {status['total_bera_pairs']} BERA PAIRS:")
        
        for exchange, symbols in sorted(status['current_bera'].items()):
            print(f"\n🏢 Exchange: {exchange.replace('_', ' ').title()}")
            
            for symbol in sorted(symbols):
                print(f"\n  📈 Pair: {symbol}")
                
                # Get BERA pair data
                bera_key = f"orderbook:{exchange}:{symbol}"
                data = self.redis_client.get(bera_key)
                
                if data:
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
                            
                            print(f"    - {symbol} = {mid_price:.8f} (spread: {spread_bps:.1f}bps, age: {age_seconds:.1f}s)")
                            
                            # Smart cross-reference calculation
                            if not symbol.endswith('/USDT') and not symbol.endswith(':USDT'):
                                base_currency = symbol.split('/')[1].split(':')[0]
                                
                                usdt_result = self.calculate_smart_usdt_equivalent(
                                    Decimal(str(mid_price)), base_currency
                                )
                                
                                if usdt_result[0]:
                                    usdt_equivalent, ref_pair, calculation = usdt_result
                                    print(f"    - {ref_pair} = Used for conversion (autodiscovered)")
                                    print(f"    - BERA/USDT = {usdt_equivalent:.8f} (calculated: {calculation})")
                                else:
                                    error_pair, error_msg = usdt_result[1], usdt_result[2]
                                    print(f"    - {error_pair} = {error_msg}")
                                    print(f"    - BERA/USDT = Cannot calculate")
                            
                            # Show direct comparison if available
                            if not symbol.endswith('/USDT'):
                                direct_key = f"orderbook:{exchange}:BERA/USDT"
                                direct_data = self.redis_client.get(direct_key)
                                if direct_data:
                                    direct_parsed = json.loads(direct_data)
                                    direct_bids = direct_parsed.get('bids', [])
                                    direct_asks = direct_parsed.get('asks', [])
                                    
                                    if direct_bids and direct_asks:
                                        direct_mid = (float(direct_bids[0][0]) + float(direct_asks[0][0])) / 2
                                        direct_age = (time.time() * 1000 - direct_parsed.get('timestamp', 0)) / 1000
                                        print(f"    - BERA/USDT = {direct_mid:.8f} (direct: age {direct_age:.1f}s)")
                        else:
                            print(f"    - {symbol} = No bid/ask data")
                    except Exception as parse_error:
                        print(f"    - {symbol} = Parse error: {parse_error}")
                else:
                    print(f"    - {symbol} = No data available")
        
        return status
    
    async def iterate_until_complete(self):
        """Keep iterating until all pairs are working properly."""
        
        print("🔄 ITERATING UNTIL ALL PAIRS COMPLETE")
        print("="*50)
        
        max_iterations = 3
        
        for iteration in range(1, max_iterations + 1):
            print(f"\n🔄 ITERATION {iteration}/{max_iterations}")
            
            # Step 1: Autodiscover reference pairs
            discovered_refs = await self.autodiscover_reference_pairs()
            
            # Step 2: Start missing reference pairs
            if discovered_refs:
                await self.start_missing_reference_pairs(discovered_refs)
            
            # Step 3: Start missing BERA pairs
            await self.start_missing_bera_pairs()
            
            # Step 4: Wait for services to initialize
            print(f"⏱️  Waiting 45 seconds for services to initialize...")
            await asyncio.sleep(45)
            
            # Step 5: Check status
            status = await self.comprehensive_status_check()
            
            # Step 6: Show current pricing
            await self.demonstrate_complete_pricing()
            
            # Check if we're complete
            missing_bera_count = sum(len(m) for m in status['missing_bera'].values())
            missing_ref_count = len(status['missing_refs'])
            
            print(f"\n📊 ITERATION {iteration} RESULTS:")
            print(f"✅ BERA pairs: {status['total_bera_pairs']}")
            print(f"✅ Reference pairs: {status['total_ref_pairs']}")
            print(f"❌ Missing BERA: {missing_bera_count}")
            print(f"❌ Missing references: {missing_ref_count}")
            
            if missing_bera_count == 0 and missing_ref_count <= 1:  # Allow 1 missing ref
                print(f"🎉 COMPLETE! All pairs working!")
                break
            else:
                print(f"🔄 Need more iterations...")
        
        return status


async def main():
    """Main complete autodiscovery and iteration."""
    
    print("🎯 COMPLETE BERA AUTODISCOVERY WITH ITERATION")
    print("="*60)
    print("Will keep iterating until ALL BERA pairs on ALL exchanges work")
    print("with proper cross-reference calculations using autodiscovery")
    
    tracker = CompleteBERAAutodiscoveryTracker()
    
    try:
        final_status = await tracker.iterate_until_complete()
        
        print(f"\n🎉 FINAL RESULTS:")
        print(f"✅ Total BERA pairs: {final_status['total_bera_pairs']}")
        print(f"✅ Total reference pairs: {final_status['total_ref_pairs']}")
        print(f"✅ Exchanges covered: {len(final_status['current_bera'])}")
        
        # Show final missing items
        if final_status['missing_bera']:
            print(f"❌ Still missing BERA pairs:")
            for exchange, missing in final_status['missing_bera'].items():
                print(f"  {exchange}: {missing}")
        
        if final_status['missing_refs']:
            print(f"❌ Still missing reference currencies:")
            for currency, error in final_status['missing_refs'].items():
                print(f"  {currency}: {error}")
        
    except Exception as e:
        print(f"❌ Complete autodiscovery failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
