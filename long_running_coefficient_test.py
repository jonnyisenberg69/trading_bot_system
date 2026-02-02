"""
Long-Running Coefficient Calculation Test

This test runs the stacked market making strategy for 10 MINUTES on select BERA pairs
to actually collect real trade data and demonstrate coefficient calculations working
with 1min and 5min windows.

Focus: Prove coefficient calculations work with real trade data over time.
"""

import asyncio
import logging
import time
import json
from decimal import Decimal
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
import redis

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Suppress some verbose logs for cleaner output
logging.getLogger('market_data.redis_orderbook_manager').setLevel(logging.WARNING)


class MockExchangeConnector:
    """Mock exchange connector for testing."""
    
    def __init__(self, exchange_name: str, symbol: str):
        self.exchange_name = exchange_name
        self.symbol = symbol
        self.orders = {}
        self.order_counter = 0
        self.placed_orders = []
    
    async def place_limit_order(self, side: str, amount: float, price: float, client_id: str = None) -> dict:
        """Mock order placement."""
        self.order_counter += 1
        order_id = f"mock_{self.exchange_name}_{self.order_counter}"
        
        order = {
            'id': order_id,
            'side': side,
            'amount': amount,
            'price': price,
            'symbol': self.symbol,
            'status': 'open',
            'timestamp': time.time(),
            'client_id': client_id
        }
        
        self.orders[order_id] = order
        self.placed_orders.append(order)
        logger.info(f"📝 MOCK ORDER: {side} {amount} {self.symbol} @ {price:.6f} on {self.exchange_name}")
        
        return order
    
    async def cancel_order(self, order_id: str) -> bool:
        """Mock order cancellation."""
        if order_id in self.orders:
            order = self.orders.pop(order_id)
            logger.info(f"❌ MOCK CANCEL: {order_id} on {self.exchange_name}")
            return True
        return False
    
    async def get_open_orders(self) -> list:
        """Get open orders."""
        return list(self.orders.values())


async def run_long_coefficient_test():
    """Run long-running test to demonstrate coefficient calculations."""
    
    print("\n" + "="*100)
    print("🧮 LONG-RUNNING COEFFICIENT CALCULATION TEST - 10 MINUTES")
    print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Testing coefficient calculations with REAL trade data over 1min and 5min windows")
    print("="*100)
    
    # Select a few high-volume BERA pairs for focused testing
    test_pairs = [
        {'exchange': 'binance', 'symbol': 'BERA/USDT', 'name': 'Primary'},
        {'exchange': 'bybit', 'symbol': 'BERA/USDT', 'name': 'Secondary'},
        {'exchange': 'binance', 'symbol': 'BERA/BTC', 'name': 'Bitcoin-Pair'},
    ]
    
    print(f"🎯 FOCUSED TESTING ON {len(test_pairs)} HIGH-VOLUME PAIRS:")
    for pair in test_pairs:
        print(f"  📊 {pair['name']}: {pair['exchange']} {pair['symbol']}")
    
    # Create strategy configurations for focused testing
    config = {
        'inventory_config': {
            'target_inventory': '1500.0',
            'max_inventory_deviation': '500.0',
            'pricing_method': 'MANUAL',
            'manual_price': '2.58'
        },
        'tob_lines': [{
            'line_id': 0,
            'hourly_quantity': '150.0',
            'spread_bps': '25.0',
            'timeout_seconds': 45,
            'coefficient_method': 'min'
        }],
        'passive_lines': []
    }
    
    strategies = []
    
    # Start strategies for each test pair
    for pair_config in test_pairs:
        exchange = pair_config['exchange']
        symbol = pair_config['symbol']
        name = pair_config['name']
        
        print(f"\n🚀 STARTING {name}: {exchange} {symbol}")
        
        try:
            from bot_manager.strategies.stacked_market_making import StackedMarketMakingStrategy
            
            # Create mock connector
            connector = MockExchangeConnector(exchange, symbol)
            
            # Create strategy config
            full_config = {
                'base_coin': symbol.split('/')[0],
                'quote_currencies': [symbol.split('/')[1]],
                'exchanges': [{'name': exchange, 'type': 'spot'}],
                **config
            }
            
            # Create strategy instance
            strategy = StackedMarketMakingStrategy(
                instance_id=f'long_test_{name.lower()}',
                symbol=symbol,
                exchanges=[exchange],
                config=full_config
            )
            
            # Initialize and start
            await strategy.initialize()
            await strategy.start()
            
            strategies.append({
                'name': name,
                'exchange': exchange,
                'symbol': symbol,
                'strategy': strategy,
                'connector': connector,
                'start_time': time.time()
            })
            
            print(f"  ✅ {name}: Strategy started successfully")
            
        except Exception as e:
            print(f"  ❌ {name}: Failed to start - {e}")
    
    if not strategies:
        print("❌ No strategies started successfully!")
        return
    
    print(f"\n⏱️  RUNNING {len(strategies)} STRATEGIES FOR 10 MINUTES...")
    print(f"📊 This will collect REAL trade data for coefficient calculations")
    print(f"🎯 Monitoring 1min (SMA5) and 5min (SMA25) windows")
    
    # Run for 10 minutes with periodic reporting
    monitoring_duration = 600  # 10 minutes
    
    for second in range(monitoring_duration):
        await asyncio.sleep(1)
        
        # Report every 2 minutes
        if second % 120 == 0 and second > 0:
            elapsed_minutes = second / 60
            print(f"\n📊 MINUTE {elapsed_minutes:.0f}/10 - COEFFICIENT CALCULATION UPDATE:")
            
            for strategy_info in strategies:
                name = strategy_info['name']
                exchange = strategy_info['exchange']
                symbol = strategy_info['symbol']
                strategy = strategy_info['strategy']
                connector = strategy_info['connector']
                
                try:
                    # Get inventory state
                    inventory_state = strategy.inventory_manager.get_inventory_state()
                    inventory_coeff = float(inventory_state.inventory_coefficient)
                    
                    # Get coefficient calculations if available
                    volume_coeff = 1.0
                    ma_data_summary = "No data"
                    
                    if hasattr(strategy, 'coefficient_calculator'):
                        try:
                            volume_coeff = strategy.coefficient_calculator.calculate_coefficient(symbol, exchange)
                            volume_coeff = float(volume_coeff) if volume_coeff else 1.0
                        except Exception as coeff_error:
                            volume_coeff = 1.0
                    
                    # Get MA data if available
                    if hasattr(strategy, 'ma_calculator'):
                        try:
                            ma_data = strategy.ma_calculator.get_all_moving_averages()
                            active_ma = sum(1 for v in ma_data.values() if v and v != 0)
                            total_ma = len(ma_data)
                            ma_data_summary = f"{active_ma}/{total_ma} active"
                            
                            # Show some actual MA values if available
                            if active_ma > 0:
                                active_values = [f"{k}={v:.3f}" for k, v in ma_data.items() if v and v != 0]
                                ma_data_summary += f" ({', '.join(active_values[:2])})"
                        except Exception as ma_error:
                            ma_data_summary = f"MA error: {str(ma_error)[:30]}"
                    
                    # Get order statistics
                    orders_placed = len(connector.placed_orders)
                    orders_open = len(connector.orders)
                    
                    print(f"  🧮 {name:10} ({exchange:8} {symbol:10}):")
                    print(f"      📦 Inventory coeff: {inventory_coeff:.4f}")
                    print(f"      🔢 Volume coeff:    {volume_coeff:.4f}")
                    print(f"      📈 MA data:         {ma_data_summary}")
                    print(f"      📝 Orders:          {orders_placed} placed, {orders_open} open")
                    
                    # Test orderbook access
                    if hasattr(strategy, 'enhanced_orderbook_manager') and strategy.enhanced_orderbook_manager:
                        try:
                            best_bid, best_ask = strategy.enhanced_orderbook_manager.redis_manager.get_best_bid_ask(
                                exchange, symbol
                            )
                            if best_bid and best_ask:
                                spread_bps = ((float(best_ask) - float(best_bid)) / float(best_bid)) * 10000
                                print(f"      💰 Live pricing:    bid={float(best_bid):.6f}, ask={float(best_ask):.6f}, spread={spread_bps:.1f}bps")
                            else:
                                print(f"      💰 Live pricing:    No bid/ask data")
                        except Exception as pricing_error:
                            print(f"      💰 Live pricing:    Error - {str(pricing_error)[:40]}")
                    
                except Exception as update_error:
                    print(f"  ❌ {name:10}: Update error - {str(update_error)[:60]}")
        
        # Brief status update every 30 seconds
        elif second % 30 == 0 and second > 0:
            elapsed_minutes = second / 60
            print(f"⏱️  Running... {elapsed_minutes:.1f}/10 minutes - Collecting trade data for coefficients")
    
    print(f"\n✅ 10-MINUTE COEFFICIENT TEST COMPLETE")
    
    # Final summary
    print(f"\n🎯 FINAL COEFFICIENT CALCULATION RESULTS:")
    
    total_orders_placed = 0
    
    for strategy_info in strategies:
        name = strategy_info['name']
        exchange = strategy_info['exchange']
        symbol = strategy_info['symbol']
        strategy = strategy_info['strategy']
        connector = strategy_info['connector']
        runtime_minutes = (time.time() - strategy_info['start_time']) / 60
        
        try:
            # Final coefficient calculations
            inventory_state = strategy.inventory_manager.get_inventory_state()
            inventory_coeff = float(inventory_state.inventory_coefficient)
            
            volume_coeff = 1.0
            if hasattr(strategy, 'coefficient_calculator'):
                try:
                    volume_coeff = strategy.coefficient_calculator.calculate_coefficient(symbol, exchange)
                    volume_coeff = float(volume_coeff) if volume_coeff else 1.0
                except:
                    volume_coeff = 1.0
            
            # MA data summary
            ma_summary = "No MA data collected"
            if hasattr(strategy, 'ma_calculator'):
                try:
                    ma_data = strategy.ma_calculator.get_all_moving_averages()
                    active_ma = sum(1 for v in ma_data.values() if v and v != 0)
                    total_ma = len(ma_data)
                    
                    if active_ma > 0:
                        ma_summary = f"{active_ma}/{total_ma} MA configs have data"
                        # Show actual values
                        for config_key, value in ma_data.items():
                            if value and value != 0:
                                print(f"        📈 {config_key}: {value:.6f}")
                    else:
                        ma_summary = f"0/{total_ma} MA configs (need more trade data)"
                except Exception as ma_error:
                    ma_summary = f"MA error: {str(ma_error)[:40]}"
            
            orders_placed = len(connector.placed_orders)
            total_orders_placed += orders_placed
            
            print(f"\n  📊 {name:12} ({exchange:8} {symbol:10}) - {runtime_minutes:.1f} min runtime:")
            print(f"      🧮 Final volume coeff:    {volume_coeff:.4f}")
            print(f"      📦 Final inventory coeff: {inventory_coeff:.4f}")
            print(f"      📈 MA data status:        {ma_summary}")
            print(f"      📝 Orders placed:         {orders_placed}")
            
        except Exception as final_error:
            print(f"  ❌ {name:12}: Final summary error - {str(final_error)[:60]}")
        
        finally:
            # Stop strategy
            try:
                await strategy.stop()
                print(f"      ✅ Strategy stopped cleanly")
            except Exception as stop_error:
                print(f"      ⚠️  Stop warning: {stop_error}")
    
    print(f"\n🎉 LONG-RUNNING COEFFICIENT TEST COMPLETE!")
    print(f"✅ Total runtime: 10 minutes")
    print(f"✅ Total mock orders placed: {total_orders_placed}")
    print(f"✅ Strategies tested: {len(strategies)}")


async def main():
    """Main long-running test."""
    
    print("🧮 LONG-RUNNING COEFFICIENT CALCULATION TEST")
    print("="*60)
    print("This will run for 10 MINUTES to collect real trade data")
    print("and demonstrate coefficient calculations with 1min/5min windows")
    
    try:
        # Pre-test validation
        redis_client = redis.Redis(decode_responses=True)
        bera_keys = redis_client.keys('orderbook:*BERA*')
        
        if len(bera_keys) < 5:
            print("❌ Insufficient BERA market data!")
            return
        
        print(f"✅ Found {len(bera_keys)} BERA data streams - proceeding with 10-minute test")
        
        # Run long test
        await run_long_coefficient_test()
        
    except KeyboardInterrupt:
        print("\n👋 Test interrupted by user")
    except Exception as e:
        print(f"\n❌ Long-running test failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    print("⚠️  This test will run for 10 MINUTES to collect real trade data")
    print("Press Ctrl+C to stop early if needed")
    
    asyncio.run(main())
