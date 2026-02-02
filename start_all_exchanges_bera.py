"""
Start BERA Market Data Collection for ALL Exchanges

This script starts market data collection for BERA pairs across ALL supported exchanges:
- Binance, Bybit, MEXC (already running)  
- Bitget, Gate, Hyperliquid (spot + perp) - missing exchanges
- Autodiscovery of which exchanges actually support BERA pairs
"""

import asyncio
import json
import logging
import subprocess
import time
from typing import Dict, List, Any
import redis

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ComprehensiveExchangeBERAStarter:
    """Start BERA market data collection for all possible exchanges."""
    
    def __init__(self):
        self.all_exchanges = [
            'binance', 'bybit', 'mexc',      # Currently running
            'bitget', 'gate', 'kucoin',     # Need to add
            'hyperliquid', 'okx'            # Need to add  
        ]
        
        self.bera_pairs_to_test = [
            'BERA/USDT', 'BERA/BTC', 'BERA/ETH', 'BERA/USDC',
            'BERA/BNB', 'BERA/SOL', 'BERA/FDUSD', 'BERA/TRY'
        ]
        
        self.redis_client = redis.Redis(decode_responses=True)
    
    async def check_exchange_bera_support_comprehensive(self) -> Dict[str, Dict[str, List[str]]]:
        """Check BERA support across ALL exchanges including spot/perp."""
        
        print("🔍 COMPREHENSIVE BERA EXCHANGE DISCOVERY")
        print("="*60)
        
        supported_data = {}
        
        try:
            import ccxt
            
            for exchange_name in self.all_exchanges:
                print(f"\n📊 Checking {exchange_name}...")
                
                supported_data[exchange_name] = {'spot': [], 'perp': []}
                
                try:
                    # Check spot markets
                    if hasattr(ccxt, exchange_name):
                        exchange_class = getattr(ccxt, exchange_name)
                        exchange = exchange_class({
                            'sandbox': False,
                            'enableRateLimit': True,
                        })
                        
                        markets = await asyncio.get_event_loop().run_in_executor(
                            None, exchange.load_markets
                        )
                        
                        # Find BERA pairs
                        spot_bera_pairs = []
                        perp_bera_pairs = []
                        
                        for symbol in markets.keys():
                            if symbol.startswith('BERA/'):
                                if markets[symbol].get('type') == 'spot':
                                    spot_bera_pairs.append(symbol)
                                elif markets[symbol].get('type') in ['future', 'swap', 'perpetual']:
                                    perp_bera_pairs.append(symbol)
                                else:
                                    # Default to spot if type unclear
                                    spot_bera_pairs.append(symbol)
                        
                        if spot_bera_pairs:
                            supported_data[exchange_name]['spot'] = spot_bera_pairs
                            print(f"  ✅ SPOT: {len(spot_bera_pairs)} pairs - {spot_bera_pairs}")
                        
                        if perp_bera_pairs:
                            supported_data[exchange_name]['perp'] = perp_bera_pairs
                            print(f"  ✅ PERP: {len(perp_bera_pairs)} pairs - {perp_bera_pairs}")
                        
                        if not spot_bera_pairs and not perp_bera_pairs:
                            print(f"  ❌ No BERA pairs found")
                            
                    else:
                        print(f"  ⚠️  Exchange {exchange_name} not available in CCXT")
                        
                except Exception as e:
                    print(f"  ❌ Error checking {exchange_name}: {str(e)[:80]}")
        
        except ImportError:
            print("⚠️  CCXT not available, using manual detection")
            
        return supported_data
    
    async def start_missing_exchange_services(self, supported_data: Dict[str, Dict[str, List[str]]]):
        """Start market data services for exchanges that aren't running yet."""
        
        print("\n🚀 STARTING MISSING EXCHANGE SERVICES")
        print("="*60)
        
        # Get currently running exchanges
        current_keys = self.redis_client.keys('orderbook:*')
        running_exchanges = set()
        for key in current_keys:
            parts = key.split(':')
            if len(parts) >= 2:
                running_exchanges.add(parts[1].replace('_spot', '').replace('_futures', ''))
        
        print(f"📊 Currently running exchanges: {sorted(running_exchanges)}")
        
        started_services = []
        
        for exchange, market_types in supported_data.items():
            if exchange in running_exchanges:
                print(f"  ✅ {exchange}: Already running")
                continue
                
            total_pairs = len(market_types.get('spot', [])) + len(market_types.get('perp', []))
            if total_pairs == 0:
                print(f"  ❌ {exchange}: No BERA pairs, skipping")
                continue
            
            print(f"\n🚀 Starting {exchange} market data service...")
            
            # Create service config for this exchange
            for market_type, pairs in market_types.items():
                if not pairs:
                    continue
                    
                for symbol in pairs[:2]:  # Start with first 2 pairs per exchange
                    try:
                        # Use existing market_data_service.py 
                        cmd = [
                            'python', 'market_data/market_data_service.py',
                            '--symbol', symbol,
                            '--exchanges', exchange
                        ]
                        
                        print(f"  🔄 Starting: {' '.join(cmd)}")
                        
                        # Start as background process
                        process = subprocess.Popen(
                            cmd,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            cwd='.'
                        )
                        
                        started_services.append({
                            'exchange': exchange,
                            'symbol': symbol,
                            'market_type': market_type,
                            'process': process,
                            'cmd': cmd
                        })
                        
                        # Brief delay between service starts
                        await asyncio.sleep(2)
                        
                    except Exception as e:
                        print(f"    ❌ Failed to start {exchange} {symbol}: {e}")
        
        print(f"\n✅ Started {len(started_services)} additional market data services")
        
        # Wait for services to connect and start publishing data
        print(f"⏱️  Waiting 30 seconds for services to initialize...")
        await asyncio.sleep(30)
        
        return started_services
    
    async def demonstrate_all_exchanges_live(self):
        """Demonstrate live data from ALL exchanges including missing ones."""
        
        print("\n🌐 DEMONSTRATING ALL EXCHANGES LIVE DATA")
        print("="*60)
        
        # Check current Redis state after starting new services
        all_keys = self.redis_client.keys('orderbook:*')
        
        # Parse all available exchanges
        exchange_data = {}
        for key in all_keys:
            parts = key.split(':')
            if len(parts) >= 3:
                exchange = parts[1]
                symbol = parts[2]
                
                if 'BERA' in symbol:
                    if exchange not in exchange_data:
                        exchange_data[exchange] = []
                    if symbol not in exchange_data[exchange]:
                        exchange_data[exchange].append(symbol)
        
        print(f"📊 LIVE BERA DATA SUMMARY:")
        total_streams = 0
        for exchange, symbols in exchange_data.items():
            print(f"  🔥 {exchange}: {len(symbols)} BERA pairs - {symbols}")
            total_streams += len(symbols)
        
        print(f"\n🎯 TOTAL LIVE BERA STREAMS: {total_streams}")
        
        # Demonstrate live updates from ALL exchanges
        print(f"\n⏱️  Monitoring live updates from ALL exchanges (30 seconds)...")
        
        for second in range(30):
            await asyncio.sleep(1)
            
            if second % 10 == 0:
                print(f"\n📊 Second {second + 1}/30 - ALL EXCHANGES LIVE DATA:")
                
                for exchange, symbols in exchange_data.items():
                    print(f"  🔥 {exchange.upper()}:")
                    
                    for symbol in symbols:
                        key = f"orderbook:{exchange}:{symbol}"
                        data = self.redis_client.get(key)
                        
                        if data:
                            parsed = json.loads(data)
                            timestamp = parsed.get('timestamp', 0)
                            age_seconds = (time.time() * 1000 - timestamp) / 1000
                            
                            best_bid = parsed.get('bids', [[None]])[0][0]
                            best_ask = parsed.get('asks', [[None]])[0][0]
                            
                            if best_bid and best_ask:
                                spread_bps = ((float(best_ask) - float(best_bid)) / float(best_bid)) * 10000
                                print(f"    📈 {symbol}: bid={float(best_bid):.6f}, ask={float(best_ask):.6f}, spread={spread_bps:.1f}bps, age={age_seconds:.1f}s")
                            else:
                                print(f"    ❌ {symbol}: No bid/ask data")
                        else:
                            print(f"    ❌ {symbol}: No Redis data")
        
        return exchange_data


async def main():
    """Main function to start ALL exchanges for BERA."""
    
    print("🎯 COMPREHENSIVE ALL EXCHANGES BERA DEMONSTRATION")
    print("="*80)
    
    starter = ComprehensiveExchangeBERAStarter()
    
    try:
        # Step 1: Check what exchanges support BERA
        print("🔍 STEP 1: Check BERA support across ALL exchanges...")
        supported_exchanges = await starter.check_exchange_bera_support_comprehensive()
        
        # Step 2: Start missing exchange services
        print("\n🚀 STEP 2: Start missing exchange services...")
        started_services = await starter.start_missing_exchange_services(supported_exchanges)
        
        # Step 3: Demonstrate ALL exchanges live
        print("\n🌐 STEP 3: Demonstrate live data from ALL exchanges...")
        all_exchange_data = await starter.demonstrate_all_exchanges_live()
        
        print(f"\n✅ COMPREHENSIVE EXCHANGE DEMONSTRATION COMPLETE")
        print(f"🎯 Validated live BERA data from {len(all_exchange_data)} exchanges")
        
    except Exception as e:
        print(f"❌ Comprehensive demonstration failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
