"""
Autodiscovery Trade Collector

Build the same autodiscovery system for tick collection that we have for pricing data.
Automatically discover and collect ALL available pairs for ANY base coin across ALL exchanges.
"""

import asyncio
import logging
import redis
import json
from typing import Dict, List, Set, Optional
from datetime import datetime, timezone, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class AutodiscoveryTradeCollector:
    """Trade collector with automatic pair discovery like pricing system."""
    
    def __init__(self, base_pair: str = 'BERA'):
        self.base_pair = base_pair.upper()
        self.redis_client = redis.Redis(decode_responses=True)
        self.discovered_pairs = {}
        self.collector = None
        
        # Known exchange connectors
        self.known_exchange_connectors = [
            'binance', 'bybit', 'bitget', 'gateio', 'mexc', 'hyperliquid'
        ]
        
    async def autodiscover_pairs(self) -> Dict[str, List[str]]:
        """Autodiscover all available pairs for the base coin across exchanges (like pricing system)."""
        
        print(f"\n🔍 AUTODISCOVERING ALL AVAILABLE {self.base_pair} PAIRS")
        print("="*60)
        
        discovered = {}
        
        # Get combined orderbook keys for this base pair
        combined_keys = self.redis_client.keys(f'combined_orderbook:*{self.base_pair}*')
        
        print(f"📊 Found {len(combined_keys)} {self.base_pair} orderbook keys")
        print(f"🔍 Sample keys: {combined_keys[:5]}")
        
        # Parse combined orderbook data to extract exchange/symbol pairs
        for key in combined_keys:
            try:
                # Get the orderbook data
                data = self.redis_client.get(key)
                if not data:
                    continue
                
                parsed = json.loads(data)
                exchanges = parsed.get('exchanges', [])
                
                # Extract symbol from key: combined_orderbook:market_type:symbol
                key_parts = key.split(':')
                if len(key_parts) >= 3:
                    market_type = key_parts[1]  # spot, perp, all
                    symbol = ':'.join(key_parts[2:])  # BERA/USDT or BERA/USDC:USDC
                    
                    # Only process symbols containing our base pair
                    if self.base_pair not in symbol:
                        continue
                        
                    print(f"   🔍 {key} → {market_type} {symbol}")
                    
                    # Extract exchanges that provide this symbol
                    for exchange_data in exchanges:
                        exchange_name = exchange_data.get('exchange', '')
                        
                        # Map exchange names to TradeCollector format
                        if exchange_name in ['binance_spot', 'binance']:
                            exchange_key = 'binance'
                        elif exchange_name in ['bybit_spot', 'bybit']:
                            exchange_key = 'bybit'
                        elif exchange_name in ['bitget_spot', 'bitget']:
                            exchange_key = 'bitget'
                        elif exchange_name in ['gateio_spot', 'gateio']:
                            exchange_key = 'gateio'
                        elif exchange_name in ['mexc_spot', 'mexc']:
                            exchange_key = 'mexc'
                        elif exchange_name in ['hyperliquid_spot', 'hyperliquid_perp', 'hyperliquid']:
                            exchange_key = 'hyperliquid'
                        else:
                            continue
                        
                        if exchange_key not in discovered:
                            discovered[exchange_key] = set()
                        discovered[exchange_key].add(symbol)
                        
                        print(f"     ✅ Added: {exchange_key} {symbol}")
                        
            except Exception as e:
                print(f"   ❌ Error parsing {key}: {e}")
                continue
        
        # Convert sets to lists and sort
        final_discovered = {}
        for exchange, symbols in discovered.items():
            final_discovered[exchange] = sorted(list(symbols))
        
        # Display discovery results
        total_pairs = sum(len(pairs) for pairs in final_discovered.values())
        
        print(f"🎯 AUTODISCOVERED {total_pairs} {self.base_pair} PAIRS:")
        for exchange, pairs in final_discovered.items():
            print(f"   📈 {exchange}: {pairs}")
        
        # Test a few pairs for data availability
        print(f"\n🔍 TESTING DATA AVAILABILITY:")
        for exchange, pairs in final_discovered.items():
            for symbol in pairs[:2]:  # Test first 2 pairs per exchange
                key = f"orderbook:{exchange}:{symbol}"
                combined_key = f"combined_orderbook:{exchange}:{symbol}"
                
                has_orderbook = self.redis_client.exists(key)
                has_combined = self.redis_client.exists(combined_key)
                
                status = "✅" if (has_orderbook or has_combined) else "❌"
                print(f"   {status} {exchange} {symbol}: orderbook={has_orderbook}, combined={has_combined}")
        
        self.discovered_pairs = final_discovered
        return final_discovered
    
    async def start_autodiscovered_collection(self):
        """Start trade collection for all autodiscovered pairs."""
        
        print(f"\n🚀 STARTING AUTODISCOVERED TRADE COLLECTION FOR {self.base_pair}")
        print("="*60)
        
        if not self.discovered_pairs:
            await self.autodiscover_pairs()
        
        if not self.discovered_pairs:
            print(f"❌ No {self.base_pair} pairs discovered - cannot start collection")
            return
        
        try:
            from market_data_collection.collector import TradeCollector, ExchangeConfig
            from market_data_collection.database import TradeDataManager
            
            # Initialize database
            db_manager = TradeDataManager()
            await db_manager.initialize()
            print("✅ Database manager initialized")
            
            # Create custom TradeCollector with autodiscovered pairs
            self.collector = TradeCollector(db_manager=db_manager)
            
            # Override exchange configs with ALL discovered pairs
            self.collector.exchange_configs = {}
            
            total_configured = 0
            
            for exchange, symbols in self.discovered_pairs.items():
                if symbols:
                    symbols_dict = {}
                    
                    # Categorize symbols by type
                    for symbol in symbols:
                        if ':' in symbol and symbol.count(':') == 2:
                            # Perpetual format like BERA/USDC:USDC (base/quote:settlement)
                            symbols_dict['perp'] = symbol
                        else:
                            # Spot format
                            symbols_dict['spot'] = symbol
                    
                    if symbols_dict:
                        self.collector.exchange_configs[exchange] = ExchangeConfig(
                            exchange_id=exchange,
                            symbols=symbols_dict
                        )
                        
                        total_configured += len(symbols)
                        print(f"✅ {exchange}: {symbols_dict}")
            
            print(f"📊 Configured {total_configured} {self.base_pair} pairs across {len(self.collector.exchange_configs)} exchanges")
            
            # Start collection
            await self.collector.start()
            print("✅ AUTODISCOVERED TRADE COLLECTION STARTED")
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to start autodiscovered collection: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    async def run_comprehensive_coefficient_test(self, duration_minutes: int = 3):
        """Run comprehensive coefficient test with autodiscovered pairs."""
        
        print(f"\n🧮 COMPREHENSIVE COEFFICIENT TEST ({duration_minutes} minutes)")
        print("="*70)
        
        # Start autodiscovered collection
        success = await self.start_autodiscovered_collection()
        if not success:
            return
        
        # Let it collect
        print(f"⏱️  Collecting for {duration_minutes} minutes...")
        await asyncio.sleep(duration_minutes * 60)
        
        # Calculate coefficients for all discovered pairs
        print("\n🧮 CALCULATING COEFFICIENTS FOR ALL DISCOVERED PAIRS")
        print("="*60)
        
        try:
            from market_data_collection.moving_averages import MovingAverageCalculator, MovingAverageConfig
            from market_data_collection.exchange_coefficients import SimpleExchangeCoefficientCalculator
            from market_data_collection.database import TradeDataManager
            
            # Setup MA calculator
            ma_configs = [
                MovingAverageConfig(period=2, ma_type='sma', volume_type='base'),
                MovingAverageConfig(period=4, ma_type='sma', volume_type='base'),
                MovingAverageConfig(period=2, ma_type='sma', volume_type='quote'),
                MovingAverageConfig(period=3, ma_type='sma', volume_type='quote'),
                MovingAverageConfig(period=2, ma_type='ewma', volume_type='base'),
                MovingAverageConfig(period=4, ma_type='ewma', volume_type='base'),
                MovingAverageConfig(period=3, ma_type='ewma', volume_type='quote'),
                MovingAverageConfig(period=6, ma_type='ewma', volume_type='quote'),
            ]
            
            ma_calculator = MovingAverageCalculator(ma_configs=ma_configs)
            
            coeff_calculator = SimpleExchangeCoefficientCalculator(
                ma_calculator=ma_calculator,
                calculation_method='min',
                min_coefficient=0.5,
                max_coefficient=2.0
            )
            
            # Get database manager
            db_manager = TradeDataManager()
            await db_manager.initialize()
            
            # Build MA data for all discovered pairs
            current_time = datetime.now(timezone.utc)
            
            for exchange, symbols in self.discovered_pairs.items():
                for symbol in symbols:
                    # Build MA data with multiple intervals
                    for update_num in range(1, 8):
                        window_start = current_time - timedelta(minutes=8-update_num)
                        window_end = current_time - timedelta(minutes=7-update_num)
                        
                        trades = await db_manager.get_trades_by_timerange(window_start, window_end)
                        
                        # Filter for this specific exchange/symbol
                        relevant_trades = []
                        for trade in trades:
                            if trade.exchange == exchange and trade.symbol == symbol:
                                relevant_trades.append({
                                    'side': trade.side,
                                    'amount': float(trade.amount),
                                    'price': float(trade.price),
                                    'cost': float(trade.amount) * float(trade.price),
                                    'timestamp': trade.timestamp
                                })
                        
                        if relevant_trades:
                            volume_data = ma_calculator.calculate_volume_data(relevant_trades[:10])
                            ma_calculator.update_moving_averages(symbol, exchange, volume_data, window_end)
            
            # Display coefficients in requested format
            print("\n" + "="*80)
            print(f"🎯 AUTODISCOVERED {self.base_pair} COEFFICIENTS")
            print("="*80)
            
            total_pairs = 0
            working_coefficients = 0
            
            for exchange, symbols in self.discovered_pairs.items():
                print(f"\nCoefficients {exchange.title()}:")
                
                for symbol in symbols:
                    total_pairs += 1
                    try:
                        coefficient = coeff_calculator.calculate_coefficient(symbol, exchange)
                        
                        if coefficient is not None:
                            print(f"- {symbol}: {coefficient:.4f}")
                            working_coefficients += 1
                        else:
                            print(f"- {symbol}: Building...")
                    
                    except Exception as e:
                        print(f"- {symbol}: Error - {e}")
            
            print(f"\n📊 AUTODISCOVERY SUMMARY:")
            print(f"✅ Total discovered pairs: {total_pairs}")
            print(f"✅ Working coefficients: {working_coefficients}")
            print(f"⏳ Building/No data: {total_pairs - working_coefficients}")
            
            coverage = (working_coefficients / total_pairs * 100) if total_pairs > 0 else 0
            print(f"📈 Coverage: {coverage:.1f}%")
            
            await db_manager.close()
            
        except Exception as e:
            print(f"❌ Coefficient calculation failed: {e}")
            import traceback
            traceback.print_exc()
        
        # Stop collection
        if self.collector:
            await self.collector.stop()
            print("✅ Autodiscovered trade collection stopped")


async def main():
    """Main function to run autodiscovery trade collection and coefficient testing."""
    
    import sys
    
    # Allow base pair to be passed as command line argument
    base_pair = sys.argv[1] if len(sys.argv) > 1 else 'BERA'
    
    print("🔍 AUTODISCOVERY TRADE COLLECTOR")
    print(f"Building autodiscovery for tick collection - Base pair: {base_pair}")
    
    collector = AutodiscoveryTradeCollector(base_pair=base_pair)
    
    # Step 1: Autodiscover all pairs for the base coin
    discovered = await collector.autodiscover_pairs()
    
    if not discovered:
        print(f"❌ No {base_pair} pairs discovered from pricing data")
        return
    
    # Step 2: Start collection and test coefficients
    await collector.run_comprehensive_coefficient_test(duration_minutes=3)


if __name__ == "__main__":
    asyncio.run(main())
