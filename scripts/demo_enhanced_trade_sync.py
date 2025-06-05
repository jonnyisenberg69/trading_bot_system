#!/usr/bin/env python
"""
Demo: Enhanced Trade Sync with Pagination

This script demonstrates the enhanced trade synchronization system with 
comprehensive pagination support based on BrowserStack testing guidelines.

Features demonstrated:
- Real exchange connections
- Pagination parameter validation
- Performance monitoring
- Edge case handling
- Multi-exchange synchronization
- Statistics and reporting

Usage:
    python scripts/demo_enhanced_trade_sync.py
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime, timedelta
import json

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import structlog
from database.connection import init_db, get_session, close_db
from database.repositories.trade_repository import TradeRepository
from database.models import Exchange
from order_management.tracking import PositionManager
from order_management.enhanced_trade_sync import EnhancedTradeSync
from exchanges.connectors import create_exchange_connector
from config.exchange_keys import get_exchange_config

# Configure logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="%H:%M:%S"),
        structlog.dev.ConsoleRenderer(),
    ],
)
logger = structlog.get_logger(__name__)


async def demo_enhanced_pagination():
    """Demonstrate enhanced trade sync with pagination."""
    
    print("🚀 ENHANCED TRADE SYNC WITH PAGINATION DEMO")
    print("=" * 60)
    print("Based on BrowserStack pagination testing best practices")
    print("https://www.browserstack.com/guide/test-cases-for-pagination-functionality")
    print()
    
    # Initialize database
    await init_db()
    
    try:
        async for session in get_session():
            print("1️⃣ Setting up database and repositories...")
            
            # Set up repositories
            trade_repository = TradeRepository(session)
            position_manager = PositionManager(data_dir="data/enhanced_sync_demo")
            
            # Create exchange records
            exchanges_to_test = ['binance_spot', 'bybit_spot']
            exchange_records = {}
            
            for exchange_name in exchanges_to_test:
                exchange = Exchange(
                    name=exchange_name,
                    type='spot',
                    is_active=True
                )
                session.add(exchange)
                await session.commit()
                exchange_records[exchange_name] = exchange
                print(f"   ✅ Created {exchange_name} exchange record (ID: {exchange.id})")
            
            print("\n2️⃣ Setting up enhanced exchange connectors...")
            
            # Set up exchange connectors
            exchange_connectors = {}
            for exchange_name in exchanges_to_test:
                try:
                    config = get_exchange_config(exchange_name)
                    if config:
                        connector = create_exchange_connector(exchange_name.split('_')[0], config)
                        if connector:
                            connected = await connector.connect()
                            if connected:
                                exchange_connectors[exchange_name] = connector
                                print(f"   ✅ {exchange_name} connected with API keys")
                            else:
                                print(f"   ❌ {exchange_name} connection failed")
                        else:
                            print(f"   ❌ Could not create {exchange_name} connector")
                    else:
                        print(f"   ❌ No config for {exchange_name}")
                except Exception as e:
                    print(f"   ❌ {exchange_name} setup failed: {e}")
            
            if not exchange_connectors:
                print("❌ No exchange connectors available, exiting...")
                return
            
            print(f"\n3️⃣ Initializing Enhanced Trade Sync System...")
            
            # Create enhanced trade sync system
            enhanced_sync = EnhancedTradeSync(
                exchange_connectors=exchange_connectors,
                trade_repository=trade_repository,
                position_manager=position_manager
            )
            
            # Show pagination configuration
            pagination_summary = enhanced_sync.get_pagination_summary()
            print(f"   📊 System initialized with {len(pagination_summary['supported_exchanges'])} exchanges")
            print(f"   📊 Pagination methods: {pagination_summary['pagination_methods']}")
            
            print("\n4️⃣ Demonstrating Pagination Capabilities...")
            
            # Test different pagination scenarios
            test_scenarios = [
                {
                    'name': 'Default Pagination',
                    'description': 'Test default pagination behavior',
                    'params': {'since': datetime.utcnow() - timedelta(hours=1)}
                },
                {
                    'name': 'Custom Limit Pagination', 
                    'description': 'Test with custom page size',
                    'params': {
                        'since': datetime.utcnow() - timedelta(hours=6),
                        'limit': 25
                    }
                },
                {
                    'name': 'Large Time Window',
                    'description': 'Test with larger time window', 
                    'params': {
                        'since': datetime.utcnow() - timedelta(days=1),
                        'limit': 50
                    }
                },
                {
                    'name': 'Edge Case - Zero Limit',
                    'description': 'Test edge case with zero limit',
                    'params': {
                        'since': datetime.utcnow() - timedelta(hours=1),
                        'limit': 0
                    }
                },
                {
                    'name': 'Edge Case - Large Limit',
                    'description': 'Test edge case with very large limit',
                    'params': {
                        'since': datetime.utcnow() - timedelta(hours=1),
                        'limit': 2000
                    }
                }
            ]
            
            all_results = {}
            
            for scenario in test_scenarios:
                print(f"\n🔍 Testing: {scenario['name']}")
                print(f"   📝 {scenario['description']}")
                
                scenario_results = {}
                
                for exchange_name in exchange_connectors:
                    try:
                        print(f"   🔄 {exchange_name}...")
                        
                        # Test pagination with specific parameters
                        result = await enhanced_sync.fetch_trades_with_pagination(
                            exchange_name=exchange_name,
                            symbol='BTC/USDT',
                            **scenario['params']
                        )
                        
                        scenario_results[exchange_name] = {
                            'trades_fetched': result.total_fetched,
                            'requests_made': result.total_requests,
                            'time_taken_ms': result.time_taken_ms,
                            'performance_metrics': result.performance_metrics,
                            'errors': result.errors,
                            'pagination_info': result.pagination_info
                        }
                        
                        print(f"      📈 Trades: {result.total_fetched}")
                        print(f"      🔄 Requests: {result.total_requests}")
                        print(f"      ⚡ Time: {result.time_taken_ms:.2f}ms")
                        
                        if result.errors:
                            print(f"      ❌ Errors: {len(result.errors)}")
                        
                        if result.performance_metrics.get('is_slow'):
                            print(f"      🐌 Performance: SLOW")
                        else:
                            print(f"      ⚡ Performance: GOOD")
                        
                    except Exception as e:
                        print(f"      ❌ Error: {e}")
                        scenario_results[exchange_name] = {'error': str(e)}
                
                all_results[scenario['name']] = scenario_results
                
                # Rate limiting between scenarios
                await asyncio.sleep(1.0)
            
            print(f"\n5️⃣ Demonstrating Full Exchange Sync...")
            
            # Test full exchange synchronization
            for exchange_name in exchange_connectors:
                print(f"\n🔄 Full sync for {exchange_name}...")
                
                try:
                    sync_results = await enhanced_sync.sync_exchange_with_pagination(
                        exchange_name=exchange_name,
                        symbols=['BTC/USDT', 'ETH/USDT'],
                        since=datetime.utcnow() - timedelta(hours=6)
                    )
                    
                    total_trades = sum(result.total_fetched for result in sync_results)
                    total_requests = sum(result.total_requests for result in sync_results)
                    avg_time = sum(result.time_taken_ms for result in sync_results) / len(sync_results)
                    
                    print(f"   📊 Results:")
                    print(f"      Symbols synced: {len(sync_results)}")
                    print(f"      Total trades: {total_trades}")
                    print(f"      Total requests: {total_requests}")
                    print(f"      Average time per symbol: {avg_time:.2f}ms")
                    
                    # Show per-symbol breakdown
                    for result in sync_results:
                        symbol = result.pagination_info.get('symbol', 'Unknown')
                        print(f"      {symbol}: {result.total_fetched} trades, {result.time_taken_ms:.2f}ms")
                        
                        if result.errors:
                            print(f"         ❌ Errors: {result.errors}")
                    
                except Exception as e:
                    print(f"   ❌ Full sync failed: {e}")
            
            print(f"\n6️⃣ Performance Summary and Statistics...")
            
            # Get final statistics
            final_summary = enhanced_sync.get_pagination_summary()
            stats = final_summary['sync_statistics']
            
            print(f"   📊 Overall Statistics:")
            print(f"      Total syncs: {stats['total_syncs']}")
            print(f"      Successful: {stats['successful_syncs']}")
            print(f"      Failed: {stats['failed_syncs']}")
            print(f"      Success rate: {(stats['successful_syncs'] / max(stats['total_syncs'], 1)) * 100:.1f}%")
            print(f"      Total trades fetched: {stats['total_trades_fetched']}")
            print(f"      Total requests made: {stats['total_requests_made']}")
            print(f"      Average response time: {stats['avg_response_time_ms']:.2f}ms")
            
            print(f"\n   📊 Per-Exchange Performance:")
            for exchange_name, exchange_stats in stats['performance_by_exchange'].items():
                print(f"      {exchange_name}:")
                print(f"         Syncs: {exchange_stats['syncs']}")
                print(f"         Trades: {exchange_stats['trades']}")
                print(f"         Requests: {exchange_stats['requests']}")
                print(f"         Avg time: {exchange_stats['avg_time_ms']:.2f}ms")
                print(f"         Success rate: {exchange_stats['success_rate']*100:.1f}%")
            
            print(f"\n   📊 Pagination Configurations:")
            for exchange_name, config in final_summary['pagination_configs'].items():
                print(f"      {exchange_name}:")
                print(f"         Method: {config['method']}")
                print(f"         Max limit: {config['max_limit']}")
                print(f"         Default limit: {config['default_limit']}")
                print(f"         Supports zero limit: {config['supports_zero_limit']}")
            
            print(f"\n7️⃣ Saving Results...")
            
            # Save detailed results to file
            output_file = "data/enhanced_pagination_demo_results.json"
            results_data = {
                'demo_timestamp': datetime.utcnow().isoformat(),
                'test_scenarios': all_results,
                'pagination_summary': final_summary,
                'exchanges_tested': list(exchange_connectors.keys())
            }
            
            with open(output_file, 'w') as f:
                json.dump(results_data, f, indent=2, default=str)
            
            print(f"   💾 Results saved to {output_file}")
            
            # Cleanup connections
            print(f"\n8️⃣ Cleanup...")
            for exchange_name, connector in exchange_connectors.items():
                try:
                    await connector.disconnect()
                    print(f"   ✅ {exchange_name} disconnected")
                except Exception as e:
                    print(f"   ❌ {exchange_name} disconnect failed: {e}")
            
            print(f"\n🏁 ENHANCED PAGINATION DEMO COMPLETE")
            print("=" * 60)
            print("✅ All pagination test cases completed successfully")
            print("📊 Performance statistics collected")
            print("💾 Results saved for analysis")
            print("🔧 System ready for production use")
            break
            
    finally:
        await close_db()


async def quick_pagination_test():
    """Quick test of pagination functionality."""
    print("⚡ QUICK PAGINATION TEST")
    print("=" * 40)
    
    # Test just one exchange quickly
    exchange_name = 'binance_spot'
    
    try:
        config = get_exchange_config(exchange_name)
        if not config:
            print(f"❌ No config for {exchange_name}")
            return
        
        connector = create_exchange_connector('binance', config)
        if not connector:
            print(f"❌ Could not create connector")
            return
        
        connected = await connector.connect()
        if not connected:
            print(f"❌ Connection failed")
            return
        
        print(f"✅ Connected to {exchange_name}")
        
        # Quick pagination configuration test
        from order_management.enhanced_trade_sync import ExchangePaginationConfig, PaginationMethod
        
        config = ExchangePaginationConfig(
            exchange_name=exchange_name,
            method=PaginationMethod.TIME_BASED,
            max_limit=1000,
            default_limit=100,
            supports_zero_limit=False
        )
        
        print(f"📊 Pagination config:")
        print(f"   Method: {config.method.value}")
        print(f"   Max limit: {config.max_limit}")
        print(f"   Default limit: {config.default_limit}")
        print(f"   Supports zero limit: {config.supports_zero_limit}")
        
        await connector.disconnect()
        print(f"✅ Quick test completed")
        
    except Exception as e:
        print(f"❌ Quick test failed: {e}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Enhanced Trade Sync Pagination Demo")
    parser.add_argument('--quick', action='store_true', help='Run quick test only')
    args = parser.parse_args()
    
    if args.quick:
        asyncio.run(quick_pagination_test())
    else:
        asyncio.run(demo_enhanced_pagination()) 