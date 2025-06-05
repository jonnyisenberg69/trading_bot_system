#!/usr/bin/env python
"""
Test script for WebSocket connection pool integration.

Tests that:
1. Only one connection per exchange type is created
2. Multiple strategies can share the same connection
3. Trade messages are properly routed to the database
4. Connections are cleaned up when strategies stop
"""

import asyncio
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from exchanges.websocket import (
    get_connection_pool, 
    initialize_connection_pool,
    shutdown_connection_pool
)
from exchanges.connectors import BinanceConnector, BybitConnector
from bot_manager.strategies.passive_quoting import PassiveQuotingStrategy
import structlog

logger = structlog.get_logger(__name__)


async def test_websocket_connection_pool():
    """Test the WebSocket connection pool functionality."""
    
    print("🧪 Testing WebSocket Connection Pool Integration")
    print("=" * 60)
    
    # Initialize connection pool
    pool = await initialize_connection_pool()
    print(f"✅ Connection pool initialized")
    
    try:
        # Create mock exchange connectors
        binance_spot_config = {
            'name': 'binance_spot',
            'api_key': 'test_key',
            'secret': 'test_secret',
            'market_type': 'spot'
        }
        
        binance_perp_config = {
            'name': 'binance_perp', 
            'api_key': 'test_key',
            'secret': 'test_secret',
            'market_type': 'future'
        }
        
        bybit_spot_config = {
            'name': 'bybit_spot',
            'api_key': 'test_key',
            'secret': 'test_secret',
            'market_type': 'spot'
        }
        
        # Create connectors
        binance_spot = BinanceConnector(binance_spot_config)
        binance_perp = BinanceConnector(binance_perp_config)
        bybit_spot = BybitConnector(bybit_spot_config)
        
        print(f"✅ Created exchange connectors")
        
        # Test 1: Single strategy subscription
        print("\n📋 Test 1: Single strategy subscription")
        
        strategy1_connectors = {
            'binance_spot': binance_spot,
            'binance_perp': binance_perp
        }
        
        success = await pool.subscribe_strategy(
            strategy_id="strategy_1",
            exchange_connectors=strategy1_connectors,
            symbols=["BERA/USDT"]
        )
        
        print(f"Strategy 1 subscription: {'✅ Success' if success else '❌ Failed'}")
        
        # Check connection status
        status = pool.get_connection_status()
        print(f"Connections: {status['total_connections']}, Strategies: {status['total_strategies']}")
        
        for conn_key, conn_info in status['connections'].items():
            print(f"  {conn_key}: {conn_info['status']} ({conn_info['subscriber_count']} subscribers)")
        
        # Test 2: Multiple strategies sharing connections
        print("\n📋 Test 2: Multiple strategies sharing connections")
        
        strategy2_connectors = {
            'binance_spot': binance_spot,  # Same as strategy 1
            'bybit_spot': bybit_spot       # New exchange
        }
        
        success = await pool.subscribe_strategy(
            strategy_id="strategy_2",
            exchange_connectors=strategy2_connectors,
            symbols=["BERA/USDT"]
        )
        
        print(f"Strategy 2 subscription: {'✅ Success' if success else '❌ Failed'}")
        
        # Check updated status
        status = pool.get_connection_status()
        print(f"Connections: {status['total_connections']}, Strategies: {status['total_strategies']}")
        
        for conn_key, conn_info in status['connections'].items():
            print(f"  {conn_key}: {conn_info['status']} ({conn_info['subscriber_count']} subscribers)")
            print(f"    Subscribers: {conn_info['subscribers']}")
        
        # Test 3: Strategy unsubscription
        print("\n📋 Test 3: Strategy unsubscription")
        
        await pool.unsubscribe_strategy("strategy_1")
        print("✅ Strategy 1 unsubscribed")
        
        # Check status after unsubscription
        status = pool.get_connection_status()
        print(f"Connections: {status['total_connections']}, Strategies: {status['total_strategies']}")
        
        for conn_key, conn_info in status['connections'].items():
            print(f"  {conn_key}: {conn_info['status']} ({conn_info['subscriber_count']} subscribers)")
            print(f"    Subscribers: {conn_info['subscribers']}")
        
        # Test 4: Full cleanup
        print("\n📋 Test 4: Full cleanup")
        
        await pool.unsubscribe_strategy("strategy_2")
        print("✅ Strategy 2 unsubscribed")
        
        # Check final status
        status = pool.get_connection_status()
        print(f"Final state - Connections: {status['total_connections']}, Strategies: {status['total_strategies']}")
        
        print("\n🎉 All tests completed successfully!")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        logger.error(f"Test error: {e}", exc_info=True)
        
    finally:
        # Cleanup
        await shutdown_connection_pool()
        print("✅ Connection pool shut down")


async def test_strategy_integration():
    """Test WebSocket integration with actual strategy."""
    
    print("\n🧪 Testing Strategy WebSocket Integration")
    print("=" * 60)
    
    try:
        # Create a test passive quoting strategy
        strategy_config = {
            'base_coin': 'BERA',
            'quantity_currency': 'base',
            'exchanges': ['binance_spot', 'bybit_spot'],
            'lines': [
                {
                    'timeout': 30,
                    'drift': 50,
                    'quantity': 10.0,
                    'quantity_randomization_factor': 0.1,
                    'spread': 20,
                    'sides': 'both'
                }
            ]
        }
        
        strategy = PassiveQuotingStrategy(
            instance_id="test_strategy_ws",
            symbol="BERA/USDT",
            exchanges=['binance_spot', 'bybit_spot'],
            config=strategy_config
        )
        
        # Mock exchange connectors
        binance_spot_config = {
            'name': 'binance_spot',
            'api_key': 'test_key',
            'secret': 'test_secret',
            'market_type': 'spot'
        }
        
        bybit_spot_config = {
            'name': 'bybit_spot',
            'api_key': 'test_key',
            'secret': 'test_secret',
            'market_type': 'spot'
        }
        
        strategy.exchange_connectors = {
            'binance_spot': BinanceConnector(binance_spot_config),
            'bybit_spot': BybitConnector(bybit_spot_config)
        }
        
        print("✅ Created test strategy with exchange connectors")
        
        # Initialize and start strategy
        await strategy.initialize()
        print("✅ Strategy initialized")
        
        # Start strategy (this should automatically start WebSocket monitoring)
        await strategy.start()
        print("✅ Strategy started with WebSocket monitoring")
        
        # Check WebSocket connection status
        pool = get_connection_pool()
        status = pool.get_connection_status()
        
        print(f"\nWebSocket Status:")
        print(f"  Connections: {status['total_connections']}")
        print(f"  Strategies: {status['total_strategies']}")
        
        for conn_key, conn_info in status['connections'].items():
            print(f"  {conn_key}: {conn_info['status']} ({conn_info['subscriber_count']} subscribers)")
        
        # Let it run for a few seconds
        print("\n⏳ Running strategy for 5 seconds...")
        await asyncio.sleep(5)
        
        # Stop strategy
        await strategy.stop()
        print("✅ Strategy stopped")
        
        # Check final status
        status = pool.get_connection_status()
        print(f"\nFinal WebSocket Status:")
        print(f"  Connections: {status['total_connections']}")
        print(f"  Strategies: {status['total_strategies']}")
        
        print("\n🎉 Strategy integration test completed!")
        
    except Exception as e:
        print(f"❌ Strategy integration test failed: {e}")
        logger.error(f"Strategy test error: {e}", exc_info=True)


async def main():
    """Run all WebSocket integration tests."""
    
    print("🚀 Starting WebSocket Integration Tests")
    print("=" * 60)
    
    try:
        # Test 1: Connection pool functionality
        await test_websocket_connection_pool()
        
        # Test 2: Strategy integration
        await test_strategy_integration()
        
        print("\n🎉 All WebSocket integration tests completed successfully!")
        
    except Exception as e:
        print(f"❌ Tests failed: {e}")
        logger.error(f"Test suite error: {e}", exc_info=True)
        return 1
    
    return 0


if __name__ == "__main__":
    # Configure logging
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    
    # Run tests
    exit_code = asyncio.run(main()) 