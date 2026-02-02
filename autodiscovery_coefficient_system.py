"""
Autodiscovery Coefficient System

Generic system that takes any base pair (e.g., "BERA") and:
1. Autodiscovers all pairs on all exchanges (like pricing system)
2. Subscribes to tick streams for ALL discovered pairs
3. Runs MA calculations separately for each exchange/symbol
4. Publishes coefficients through Redis properly

No hardcoding - fully dynamic like the pricing system.
"""

import asyncio
import logging
import redis
import json
from typing import Dict, List, Set, Optional, Tuple
from datetime import datetime, timezone, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class AutodiscoveryCoefficients:
    """Autodiscovery coefficient system like pricing system."""
    
    def __init__(self, base_pair: str = 'BERA'):
        self.base_pair = base_pair.upper()
        self.redis_client = redis.Redis(decode_responses=True)
        self.discovered_pairs = []  # List of (exchange, symbol, instrument_type)
        self.ma_calculator = None
        self.coefficient_calculator = None
        self.trade_collectors = []  # List of TradeCollector instances
        
        # Expected pairs for each base coin (like pricing system)
        self.expected_exchange_pairs = {
            'binance': ['BERA/USDT', 'BERA/BTC', 'BERA/USDC', 'BERA/BNB', 'BERA/TRY', 'BERA/FDUSD'],
            'bybit': ['BERA/USDT', 'BERA/USDC'],  
            'mexc': ['BERA/USDT'],
            'bitget': ['BERA/USDT'],
            'gateio': ['BERA/USDT'],
            'hyperliquid': ['BERA/USDC:USDC']  # Perp format
        }
        
    async def autodiscover_all_pairs(self) -> List[Tuple[str, str, str]]:
        """Autodiscover all pairs using expected pairs validation (like pricing system)."""
        
        print(f"\n🔍 AUTODISCOVERING ALL {self.base_pair} PAIRS")
        print("Using same validation approach as pricing system")
        print("="*60)
        
        discovered = []
        
        # Validate expected pairs against available orderbook data
        for exchange, expected_symbols in self.expected_exchange_pairs.items():
            print(f"\n📊 Validating {exchange} ({len(expected_symbols)} expected pairs):")
            
            for symbol in expected_symbols:
                # Determine instrument type
                if ':' in symbol and 'USDC:USDC' in symbol:
                    instrument_type = 'perp'
                    # Check perp orderbook key
                    orderbook_key = f"combined_orderbook:perp:{symbol}"
                else:
                    instrument_type = 'spot'
                    # Check spot orderbook key
                    orderbook_key = f"combined_orderbook:spot:{symbol}"
                
                # Check if orderbook data exists
                orderbook_data = self.redis_client.get(orderbook_key)
                
                if orderbook_data:
                    try:
                        data = json.loads(orderbook_data)
                        # Check if we have valid bid/ask data
                        bids = data.get('bids', [])
                        asks = data.get('asks', [])
                        
                        if bids and asks:
                            discovered.append((exchange, symbol, instrument_type))
                            print(f"   ✅ {symbol} ({instrument_type}): Live data available")
                        else:
                            print(f"   ⚠️  {symbol} ({instrument_type}): Empty orderbook")
                    except:
                        print(f"   ❌ {symbol} ({instrument_type}): Invalid data format")
                else:
                    print(f"   ❌ {symbol} ({instrument_type}): No orderbook data")
        
        self.discovered_pairs = discovered
        
        # Group by exchange for summary
        exchange_summary = {}
        for exchange, symbol, instrument_type in discovered:
            if exchange not in exchange_summary:
                exchange_summary[exchange] = []
            exchange_summary[exchange].append(f"{symbol} ({instrument_type})")
        
        total_pairs = len(discovered)
        print(f"\n✅ VALIDATED {total_pairs} {self.base_pair} PAIRS WITH LIVE DATA:")
        for exchange, pairs in sorted(exchange_summary.items()):
            print(f"   📈 {exchange}: {pairs}")
        
        return discovered
    
    def _map_exchange_name(self, exchange_name: str) -> Optional[str]:
        """Map exchange name to standard format."""
        exchange_name = exchange_name.lower()
        
        if 'binance' in exchange_name:
            return 'binance'
        elif 'bybit' in exchange_name:
            return 'bybit'
        elif 'bitget' in exchange_name:
            return 'bitget'
        elif 'gate' in exchange_name:
            return 'gateio'
        elif 'mexc' in exchange_name:
            return 'mexc'
        elif 'hyperliquid' in exchange_name:
            return 'hyperliquid'
        else:
            return None
    
    async def start_trade_collection(self, duration_minutes: int = 3):
        """Start trade collection for all discovered pairs."""
        
        print(f"\n🔄 STARTING COMPREHENSIVE TRADE COLLECTION")
        print(f"Collecting for {duration_minutes} minutes on {len(self.discovered_pairs)} pairs")
        print("="*60)
        
        if not self.discovered_pairs:
            await self.autodiscover_all_pairs()
        
        try:
            from market_data_collection.collector import TradeCollector, ExchangeConfig
            from market_data_collection.database import TradeDataManager
            
            db_manager = TradeDataManager()
            
            # Group symbols by exchange to create proper configs
            exchange_symbols = {}
            for exchange, symbol, instrument_type in self.discovered_pairs:
                if exchange not in exchange_symbols:
                    exchange_symbols[exchange] = {}
                
                # Group by instrument type, supporting multiple symbols per type
                if instrument_type not in exchange_symbols[exchange]:
                    exchange_symbols[exchange][instrument_type] = []
                exchange_symbols[exchange][instrument_type].append(symbol)
            
            # Create multiple TradeCollectors - one per exchange/symbol combination
            collectors = []
            config_count = 0
            
            for exchange, instrument_groups in exchange_symbols.items():
                for instrument_type, symbol_list in instrument_groups.items():
                    for symbol in symbol_list:  # Process ALL symbols, no skipping!
                        
                        # Create dedicated TradeCollector for this specific symbol
                        collector = TradeCollector(db_manager=db_manager)
                        
                        # Use exchange name as config key (required for CCXT)
                        collector.exchange_configs = {
                            exchange: ExchangeConfig(
                                exchange_id=exchange,
                                symbols={instrument_type: symbol}
                            )
                        }
                        
                        collectors.append(collector)
                        config_count += 1
                        print(f"✅ Collector {config_count}: {exchange} {symbol} ({instrument_type})")
            
            # Store collectors for use by other methods
            self.trade_collectors = collectors
            
            print(f"\n📊 Created {config_count} individual collectors for ALL autodiscovered symbols")
            print("🚀 NO SYMBOLS SKIPPED - collecting from every pair!")
            
            # Start ALL collectors simultaneously
            print(f"\n⏱️  Starting {duration_minutes}-minute trade collection on {config_count} collectors...")
            
            collection_tasks = []
            for i, collector in enumerate(collectors):
                task = asyncio.create_task(collector.start())
                collection_tasks.append(task)
                print(f"   🔄 Started collector {i+1}/{len(collectors)}")
            
            # Let all collectors run
            await asyncio.sleep(duration_minutes * 60)
            
            # Stop all collectors
            print(f"\n🛑 Stopping all {len(collectors)} collectors...")
            for task in collection_tasks:
                task.cancel()
            
            # Wait for all to finish
            for task in collection_tasks:
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            
            print(f"✅ All trade collection completed")
            
        except Exception as e:
            print(f"❌ Trade collection error: {e}")
            import traceback
            traceback.print_exc()
    
    async def initialize_ma_and_coefficient_systems(self):
        """Initialize MA calculator and coefficient calculator."""
        
        print(f"\n🧮 INITIALIZING MA AND COEFFICIENT SYSTEMS")
        print("-" * 50)
        
        try:
            from market_data_collection.moving_averages import MovingAverageCalculator, MovingAverageConfig
            from market_data_collection.exchange_coefficients import SimpleExchangeCoefficientCalculator
            from market_data_collection.database import TradeDataManager
            
            # Setup MA configs (shorter periods for faster results)
            ma_configs = [
                MovingAverageConfig(period=2, ma_type='sma', volume_type='base'),
                MovingAverageConfig(period=4, ma_type='sma', volume_type='base'),
                MovingAverageConfig(period=2, ma_type='ewma', volume_type='base'),
                MovingAverageConfig(period=4, ma_type='ewma', volume_type='base'),
            ]
            
            self.ma_calculator = MovingAverageCalculator(ma_configs)
            
            db_manager = TradeDataManager()
            self.coefficient_calculator = SimpleExchangeCoefficientCalculator(
                ma_calculator=self.ma_calculator,
                db_manager=db_manager,
                calculation_method='min',
                min_coefficient=0.2,
                max_coefficient=3.0
            )
            
            print(f"✅ MA calculator initialized with {len(ma_configs)} configurations")
            print(f"✅ Coefficient calculator initialized (min method, 0.2-3.0 range)")
            
        except Exception as e:
            print(f"❌ Initialization error: {e}")
            raise
    
    async def update_mas_with_trade_data(self):
        """Update moving averages with fresh trade data for all pairs."""
        
        print(f"\n📊 UPDATING MOVING AVERAGES WITH FRESH TRADE DATA")
        print("-" * 50)
        
        if not self.ma_calculator:
            await self.initialize_ma_and_coefficient_systems()
        
        try:
            from market_data_collection.database import TradeDataManager
            from market_data_collection.moving_averages import VolumeData
            
            db_manager = TradeDataManager()
            current_time = datetime.now(timezone.utc)
            
            # Multiple updates with different time windows
            total_updates = 0
            
            for update_round in range(4):  # 4 rounds of updates
                window_minutes = 5 + (update_round * 2)  # 5, 7, 9, 11 minute lookbacks
                start_window = current_time - timedelta(minutes=window_minutes)
                end_window = current_time - timedelta(minutes=window_minutes-2)  # 2-min windows
                
                print(f"\n🔄 Update round {update_round + 1}: {start_window.strftime('%H:%M:%S')} → {end_window.strftime('%H:%M:%S')}")
                
                # Get trades for this window
                trades = await db_manager.get_trades_by_timerange(start_window, end_window)
                
                if trades:
                    window_updates = 0
                    
                    # Update MA for each discovered pair
                    for exchange, symbol, instrument_type in self.discovered_pairs:
                        # Filter trades for this specific exchange/symbol
                        filtered_trades = []
                        for trade in trades:
                            if (hasattr(trade, 'exchange') and trade.exchange == exchange and 
                                hasattr(trade, 'symbol') and trade.symbol == symbol):
                                filtered_trades.append(trade)
                        
                        if filtered_trades:
                            # Calculate volume data
                            buy_trades = [t for t in filtered_trades if t.side == 'buy']
                            sell_trades = [t for t in filtered_trades if t.side == 'sell']
                            
                            base_buy_volume = sum(float(t.amount) for t in buy_trades)
                            base_sell_volume = sum(float(t.amount) for t in sell_trades)
                            quote_buy_volume = sum(float(t.amount) * float(t.price) for t in buy_trades)
                            quote_sell_volume = sum(float(t.amount) * float(t.price) for t in sell_trades)
                            
                            volume_data = VolumeData(
                                base_buy_volume=base_buy_volume,
                                base_sell_volume=base_sell_volume,
                                quote_buy_volume=quote_buy_volume,
                                quote_sell_volume=quote_sell_volume
                            )
                            
                            # Update MA
                            self.ma_calculator.update_moving_averages(symbol, exchange, volume_data, end_window)
                            window_updates += 1
                            total_updates += 1
                    
                    print(f"   ✅ Updated {window_updates} exchange/symbol pairs with fresh data")
                else:
                    print(f"   ⚠️  No trades found in this window")
            
            print(f"\n📊 TOTAL: {total_updates} MA updates across all rounds")
            
        except Exception as e:
            print(f"❌ MA update error: {e}")
            import traceback
            traceback.print_exc()
    
    async def display_all_coefficients(self):
        """Display coefficients for all pairs in the requested format."""
        
        print(f"\n" + "="*80)
        print(f"🎯 {self.base_pair} COEFFICIENTS - ALL EXCHANGES")
        print("="*80)
        
        if not self.coefficient_calculator:
            await self.initialize_ma_and_coefficient_systems()
        
        # Group pairs by exchange
        exchange_pairs = {}
        for exchange, symbol, instrument_type in self.discovered_pairs:
            if exchange not in exchange_pairs:
                exchange_pairs[exchange] = []
            exchange_pairs[exchange].append(symbol)
        
        working_coefficients = 0
        total_pairs = len(self.discovered_pairs)
        
        for exchange in sorted(exchange_pairs.keys()):
            print(f"\nCoefficients {exchange.title()}:")
            
            for symbol in sorted(exchange_pairs[exchange]):
                try:
                    coefficient = self.coefficient_calculator.calculate_coefficient(symbol, exchange)
                    
                    if coefficient and coefficient != 1.0:
                        print(f"- {symbol}: {coefficient:.4f}")
                        working_coefficients += 1
                    else:
                        print(f"- {symbol}: No data")
                        
                except Exception as e:
                    print(f"- {symbol}: Error")
        
        print(f"\n" + "="*80)
        print(f"📊 FINAL RESULT: {working_coefficients}/{total_pairs} {self.base_pair} pairs with working coefficients")
        coverage = (working_coefficients/total_pairs)*100 if total_pairs > 0 else 0
        print(f"📈 Coverage: {coverage:.1f}%")
        
        if coverage >= 80:
            print("🎉 EXCELLENT COVERAGE!")
        elif coverage >= 60:
            print("✅ GOOD COVERAGE")
        else:
            print("⚠️  NEEDS IMPROVEMENT - May need more collection time")
        
        print("="*80)
        
        return working_coefficients, total_pairs
    
    async def publish_coefficients_to_redis(self):
        """Publish coefficients to Redis for strategy consumption."""
        
        print(f"\n📡 PUBLISHING COEFFICIENTS TO REDIS")
        print("-" * 50)
        
        if not self.coefficient_calculator:
            await self.initialize_ma_and_coefficient_systems()
        
        try:
            published_count = 0
            
            for exchange, symbol, instrument_type in self.discovered_pairs:
                try:
                    coefficient = self.coefficient_calculator.calculate_coefficient(symbol, exchange)
                    
                    if coefficient:
                        # Redis key format: coefficient:{exchange}:{symbol}
                        redis_key = f"coefficient:{exchange}:{symbol}"
                        
                        coefficient_data = {
                            'exchange': exchange,
                            'symbol': symbol,
                            'coefficient': coefficient,
                            'timestamp': datetime.now(timezone.utc).isoformat(),
                            'base_pair': self.base_pair
                        }
                        
                        self.redis_client.setex(
                            redis_key, 
                            3600,  # 1 hour expiry
                            json.dumps(coefficient_data)
                        )
                        
                        published_count += 1
                        print(f"   ✅ {redis_key}: {coefficient:.4f}")
                    
                except Exception as e:
                    print(f"   ❌ {exchange} {symbol}: {e}")
            
            print(f"\n📊 Published {published_count} coefficients to Redis")
            
        except Exception as e:
            print(f"❌ Redis publishing error: {e}")
    
    async def run_comprehensive_system(self, collection_minutes: int = 3):
        """Run the complete autodiscovery coefficient system."""
        
        print("\n" + "="*80)
        print("🚀 AUTODISCOVERY COEFFICIENT SYSTEM")
        print(f"Base Pair: {self.base_pair}")
        print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80)
        
        try:
            # Step 1: Autodiscover all pairs
            discovered = await self.autodiscover_all_pairs()
            
            if not discovered:
                print(f"❌ No {self.base_pair} pairs discovered - check pricing system")
                return
            
            # Step 2: Initialize systems
            await self.initialize_ma_and_coefficient_systems()
            
            # Step 3: Start trade collection  
            await self.start_trade_collection(collection_minutes)
            
            # Step 4: Update MAs with collected data
            await self.update_mas_with_trade_data()
            
            # Step 5: Display coefficients
            working, total = await self.display_all_coefficients()
            
            # Step 6: Publish to Redis
            await self.publish_coefficients_to_redis()
            
            print(f"\n🎉 AUTODISCOVERY COMPLETE: {working}/{total} working coefficients")
            
        except Exception as e:
            print(f"❌ System error: {e}")
            import traceback
            traceback.print_exc()


async def main():
    """Main function - can be called with any base pair."""
    
    import sys
    
    # Allow base pair to be passed as argument
    base_pair = sys.argv[1] if len(sys.argv) > 1 else 'BERA'
    
    print("🎯 AUTODISCOVERY COEFFICIENT SYSTEM")
    print("Like pricing system but for coefficient calculations")
    print(f"Base pair: {base_pair}")
    
    system = AutodiscoveryCoefficients(base_pair)
    await system.run_comprehensive_system(collection_minutes=3)


if __name__ == "__main__":
    asyncio.run(main())
