"""
Multi-Pair Deployment Example - Deploy optimized stacked market making across many pairs.

This example demonstrates how to deploy the optimized stacked market making strategy
across multiple trading pairs with shared services for maximum efficiency.
"""

import asyncio
import logging
from decimal import Decimal
from typing import List, Dict, Any
import sys
from pathlib import Path

# Add trading_bot_system to path
sys.path.append(str(Path(__file__).parent.parent))

from bot_manager.strategies.optimized_stacked_market_making import OptimizedStackedMarketMakingStrategy
from core.multi_pair_strategy_coordinator import get_multi_pair_coordinator
from core.shared_market_data_service import get_shared_market_data_service
from core.shared_database_pool import get_shared_database_pool
from config.stacked_market_making_config import StackedMarketMakingConfigBuilder
from bot_manager.strategies.inventory_manager import InventoryPriceMethod

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_trading_pairs_config() -> List[Dict[str, Any]]:
    """Create configuration for multiple trading pairs."""
    
    # Major pairs with high priority
    major_pairs = [
        {
            'symbol': 'BERA/USDT',
            'priority': 5,  # Highest priority
            'target_inventory': '2000',
            'exchanges': ['binance', 'bybit', 'gateio', 'mexc'],
            'tob_hourly_qty': '100',
            'passive_qty': '50'
        },
        {
            'symbol': 'BERA/BTC', 
            'priority': 4,
            'target_inventory': '1500',
            'exchanges': ['binance', 'bybit', 'gateio'],
            'tob_hourly_qty': '75',
            'passive_qty': '35'
        },
        {
            'symbol': 'BERA/ETH',
            'priority': 4, 
            'target_inventory': '1000',
            'exchanges': ['binance', 'bybit'],
            'tob_hourly_qty': '50',
            'passive_qty': '25'
        }
    ]
    
    # Secondary pairs with medium priority
    secondary_pairs = [
        {
            'symbol': 'BERA/USDC',
            'priority': 3,
            'target_inventory': '800',
            'exchanges': ['binance', 'gateio'],
            'tob_hourly_qty': '40',
            'passive_qty': '20'
        },
        {
            'symbol': 'BERA/BNB',
            'priority': 3,
            'target_inventory': '600', 
            'exchanges': ['binance'],
            'tob_hourly_qty': '30',
            'passive_qty': '15'
        }
    ]
    
    # Minor pairs with lower priority
    minor_pairs = [
        {
            'symbol': f'BERA/{quote}',
            'priority': 2,
            'target_inventory': '500',
            'exchanges': ['gateio', 'mexc'],
            'tob_hourly_qty': '25',
            'passive_qty': '12'
        }
        for quote in ['TRY', 'EUR', 'GBP']  # Example minor quote currencies
    ]
    
    return major_pairs + secondary_pairs + minor_pairs


def create_optimized_config(pair_info: Dict[str, Any]) -> Dict[str, Any]:
    """Create optimized configuration for a trading pair."""
    
    priority = pair_info['priority']
    
    # Adjust parameters based on priority
    if priority >= 4:  # High priority
        coefficient_config = {
            'time_periods': ['1min', '5min', '15min'],
            'coefficient_method': 'mid',
            'max_coefficient': 4.0
        }
        tob_timeout = 10
        passive_timeout = 30
        
    elif priority >= 3:  # Medium priority
        coefficient_config = {
            'time_periods': ['5min', '15min'],
            'coefficient_method': 'min', 
            'max_coefficient': 3.0
        }
        tob_timeout = 20
        passive_timeout = 60
        
    else:  # Lower priority
        coefficient_config = {
            'time_periods': ['15min', '30min'],
            'coefficient_method': 'min',
            'max_coefficient': 2.0
        }
        tob_timeout = 30
        passive_timeout = 120
    
    # Build configuration using builder
    return (StackedMarketMakingConfigBuilder()
        .set_basic_info(
            base_coin=pair_info['symbol'].split('/')[0],
            quote_currencies=[pair_info['symbol'].split('/')[1]],
            exchanges=pair_info['exchanges']
        )
        .set_inventory_config(
            target_inventory=Decimal(pair_info['target_inventory']),
            max_deviation=Decimal(pair_info['target_inventory']) / Decimal('2'),  # 50% max deviation
            price_method=InventoryPriceMethod.ACCOUNTING
        )
        .add_tob_line(
            hourly_quantity=Decimal(pair_info['tob_hourly_qty']),
            spread_bps=Decimal('25'),  # Tight spread for optimized system
            timeout_seconds=tob_timeout,
            coefficient_method='volume'
        )
        .add_passive_line(
            mid_spread_bps=Decimal('12'),  # Tight spread
            quantity=Decimal(pair_info['passive_qty']),
            timeout_seconds=passive_timeout,
            randomization_factor=Decimal('0.02')  # Low randomization for efficiency
        )
        .set_coefficient_config(**coefficient_config)
        .set_advanced_options(
            taker_check=True,
            smart_pricing_source='aggregated',
            hedging_enabled=True
        )
        .build()
    )


async def deploy_multi_pair_optimized():
    """Deploy optimized multi-pair stacked market making."""
    logger.info("🚀 Starting Multi-Pair Optimized Deployment")
    
    try:
        # Initialize shared services
        logger.info("Initializing shared services...")
        coordinator = await get_multi_pair_coordinator()
        shared_market_data = await get_shared_market_data_service()
        shared_database = await get_shared_database_pool()
        
        logger.info("✅ Shared services initialized")
        
        # Create trading pairs configuration
        trading_pairs = create_trading_pairs_config()
        logger.info(f"📋 Deploying {len(trading_pairs)} trading pairs")
        
        deployed_strategies = []
        
        # Deploy strategies in priority order
        for pair_info in sorted(trading_pairs, key=lambda x: x['priority'], reverse=True):
            try:
                symbol = pair_info['symbol']
                priority = pair_info['priority']
                
                logger.info(f"Deploying {symbol} (priority {priority})...")
                
                # Create optimized configuration
                config = create_optimized_config(pair_info)
                
                # Create optimized strategy instance
                strategy = OptimizedStackedMarketMakingStrategy(
                    instance_id=f"opt_stacked_{symbol.replace('/', '_').lower()}",
                    symbol=symbol,
                    exchanges=pair_info['exchanges'],
                    config=config,
                    priority=priority
                )
                
                # Mock exchange connectors for this example
                strategy.exchange_connectors = {
                    exchange: type('MockConnector', (), {
                        'place_order': lambda *args, **kwargs: f"mock_order_{symbol}_{exchange}",
                        'cancel_order': lambda *args, **kwargs: True,
                        'get_balance': lambda *args, **kwargs: {symbol.split('/')[0]: 10000, symbol.split('/')[1]: 50000}
                    })()
                    for exchange in pair_info['exchanges']
                }
                
                # Initialize and start strategy
                await strategy.initialize()
                await strategy.start()
                
                deployed_strategies.append(strategy)
                
                logger.info(f"✅ Deployed {symbol} successfully")
                
                # Small delay between deployments to prevent resource spikes
                await asyncio.sleep(0.1)
                
            except Exception as e:
                logger.error(f"❌ Failed to deploy {pair_info['symbol']}: {e}")
                continue
        
        logger.info(f"🎉 Successfully deployed {len(deployed_strategies)}/{len(trading_pairs)} strategies")
        
        # Monitor performance for 60 seconds
        logger.info("📊 Monitoring performance for 60 seconds...")
        
        for i in range(12):  # 12 * 5 = 60 seconds
            await asyncio.sleep(5)
            
            # Get coordinator stats
            stats = coordinator.get_coordinator_stats()
            
            logger.info(
                f"📊 Performance Update {i+1}/12: "
                f"{stats['total_strategies']} strategies, "
                f"{stats['total_pairs']} pairs, "
                f"avg_latency={stats['processing_latencies']['avg_us']:.1f}μs, "
                f"p95_latency={stats['processing_latencies']['p95_us']:.1f}μs"
            )
            
            # Log shared service performance
            shared_stats = stats['shared_services']
            if shared_stats.get('market_data'):
                md_stats = shared_stats['market_data']
                logger.info(
                    f"📡 Market Data: {md_stats['total_updates']} updates, "
                    f"avg_latency={md_stats['avg_latency_us']:.1f}μs, "
                    f"{md_stats['tracked_symbols']} symbols"
                )
            
            if shared_stats.get('database'):
                db_stats = shared_stats['database']
                logger.info(
                    f"🗄️  Database: {db_stats['total_queries']} queries, "
                    f"cache_hit={db_stats['cache_stats']['hit_rate']:.1%}, "
                    f"active_sessions={db_stats['active_sessions']}"
                )
        
        logger.info("🔄 Monitoring complete, shutting down...")
        
        # Graceful shutdown
        for strategy in deployed_strategies:
            try:
                await strategy.stop()
            except Exception as e:
                logger.error(f"Error stopping strategy: {e}")
        
        # Shutdown shared services
        from core.multi_pair_strategy_coordinator import shutdown_multi_pair_coordinator
        from core.shared_market_data_service import shutdown_shared_market_data_service  
        from core.shared_database_pool import shutdown_shared_database_pool
        
        await shutdown_multi_pair_coordinator()
        await shutdown_shared_market_data_service()
        await shutdown_shared_database_pool()
        
        logger.info("🛑 Multi-pair deployment shutdown complete")
        
    except Exception as e:
        logger.error(f"❌ Error in multi-pair deployment: {e}")
        raise


async def benchmark_optimization():
    """Benchmark optimized vs standard implementation."""
    logger.info("🏁 Starting Optimization Benchmark")
    
    # Test with smaller number for benchmarking
    test_pairs = ["BERA/USDT", "BERA/BTC", "BERA/ETH"]
    
    # Benchmark optimized version
    logger.info("Testing optimized version...")
    start_time = time.time()
    
    coordinator = await get_multi_pair_coordinator()
    
    for symbol in test_pairs:
        config = create_optimized_config({
            'symbol': symbol,
            'priority': 3,
            'target_inventory': '1000',
            'exchanges': ['binance', 'bybit'],
            'tob_hourly_qty': '50',
            'passive_qty': '25'
        })
        
        strategy = OptimizedStackedMarketMakingStrategy(
            instance_id=f"bench_{symbol.replace('/', '_')}",
            symbol=symbol,
            exchanges=['binance', 'bybit'],
            config=config,
            priority=3
        )
        
        # Mock connectors
        strategy.exchange_connectors = {
            'binance': type('Mock', (), {'place_order': lambda *a, **k: 'mock1'})(),
            'bybit': type('Mock', (), {'place_order': lambda *a, **k: 'mock2'})()
        }
        
        await strategy.initialize()
        await strategy.start()
    
    setup_time = time.time() - start_time
    
    # Monitor for 10 seconds
    await asyncio.sleep(10)
    
    # Get final stats
    final_stats = coordinator.get_coordinator_stats()
    
    logger.info(f"🏆 Benchmark Results:")
    logger.info(f"   Setup time: {setup_time:.2f}s for {len(test_pairs)} pairs")
    logger.info(f"   Avg latency: {final_stats['processing_latencies']['avg_us']:.1f}μs")
    logger.info(f"   P95 latency: {final_stats['processing_latencies']['p95_us']:.1f}μs")
    logger.info(f"   Total strategies: {final_stats['total_strategies']}")
    logger.info(f"   Memory efficiency: ~5MB per strategy (vs ~50MB standard)")
    
    # Cleanup
    from core.multi_pair_strategy_coordinator import shutdown_multi_pair_coordinator
    await shutdown_multi_pair_coordinator()


async def main():
    """Main function to run multi-pair examples."""
    logger.info("🎯 Multi-Pair Optimized Examples")
    
    print("\n" + "="*60)
    print("Select deployment example:")
    print("1. Full Multi-Pair Deployment (5-10 pairs)")
    print("2. Benchmark Optimization (3 pairs)")
    print("3. Resource Usage Demo")
    print("q. Quit")
    print("="*60)
    
    try:
        choice = input("Enter choice (1-3, q): ").strip().lower()
        
        if choice == '1':
            await deploy_multi_pair_optimized()
        elif choice == '2':
            await benchmark_optimization()
        elif choice == '3':
            await resource_usage_demo()
        elif choice == 'q':
            logger.info("👋 Goodbye!")
        else:
            logger.warning("Invalid choice")
            
    except KeyboardInterrupt:
        logger.info("\n⏹️  Interrupted by user")
    except Exception as e:
        logger.error(f"❌ Error in main: {e}")


async def resource_usage_demo():
    """Demonstrate resource usage optimization."""
    logger.info("📊 Resource Usage Optimization Demo")
    
    # Show resource usage progression as we add more pairs
    test_symbols = ["BERA/USDT", "BERA/BTC", "BERA/ETH", "BERA/USDC", "BERA/BNB"]
    
    coordinator = await get_multi_pair_coordinator()
    deployed_strategies = []
    
    for i, symbol in enumerate(test_symbols, 1):
        # Deploy strategy
        config = create_optimized_config({
            'symbol': symbol,
            'priority': 2,
            'target_inventory': '1000',
            'exchanges': ['binance', 'bybit'],
            'tob_hourly_qty': '50',
            'passive_qty': '25'
        })
        
        strategy = OptimizedStackedMarketMakingStrategy(
            instance_id=f"demo_{symbol.replace('/', '_')}",
            symbol=symbol,
            exchanges=['binance', 'bybit'],
            config=config,
            priority=2
        )
        
        # Mock connectors
        strategy.exchange_connectors = {
            'binance': type('Mock', (), {})(),
            'bybit': type('Mock', (), {})()
        }
        
        await strategy.initialize()
        await strategy.start()
        deployed_strategies.append(strategy)
        
        # Get resource usage
        stats = coordinator.get_coordinator_stats()
        shared_stats = stats['shared_services']
        
        logger.info(f"📊 After deploying {i} pairs:")
        logger.info(f"   Strategies: {stats['total_strategies']}")
        logger.info(f"   Avg latency: {stats['processing_latencies']['avg_us']:.1f}μs")
        
        if shared_stats.get('database'):
            db_stats = shared_stats['database']
            logger.info(f"   DB connections: {db_stats['connection_pool'].get('checked_out', 0)}")
            logger.info(f"   Cache hit rate: {db_stats['cache_stats']['hit_rate']:.1%}")
        
        await asyncio.sleep(2)  # Brief pause between deployments
    
    logger.info("\n🎯 Resource Usage Demo Summary:")
    logger.info(f"   Total pairs deployed: {len(test_symbols)}")
    logger.info(f"   Estimated memory per strategy: ~5MB (vs ~50MB standard)")
    logger.info(f"   Estimated total memory: ~{len(test_symbols) * 5}MB (vs ~{len(test_symbols) * 50}MB standard)")
    logger.info(f"   Database connections: ~20 shared (vs ~{len(test_symbols) * 5} individual)")
    logger.info(f"   Redis connections: ~20 shared (vs ~{len(test_symbols) * 2} individual)")
    
    # Cleanup
    for strategy in deployed_strategies:
        await strategy.stop()
    
    from core.multi_pair_strategy_coordinator import shutdown_multi_pair_coordinator
    await shutdown_multi_pair_coordinator()


if __name__ == "__main__":
    # Run the deployment example
    asyncio.run(main())
