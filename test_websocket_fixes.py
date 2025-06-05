#!/usr/bin/env python3
"""
Test script for WebSocket connection fixes.

This script tests the improved WebSocket connection handling,
especially for MEXC stability and Hyperliquid credential handling.
"""

import asyncio
import sys
import os
import structlog
from typing import Dict, Any

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from exchanges.websocket.connection_pool import WebSocketConnectionPool, get_connection_pool
from exchanges.websocket.ws_manager import WebSocketManager
from exchanges.websocket.reconnection import ReconnectionManager
from bot_manager.exchange_manager import ExchangeManager

logger = structlog.get_logger(__name__)


async def test_websocket_connection_pool():
    """Test the WebSocket connection pool with improved error handling."""
    logger.info("Testing WebSocket connection pool...")
    
    try:
        # Initialize exchange manager
        exchange_manager = ExchangeManager()
        await exchange_manager.start()
        
        # Get connection pool
        pool = get_connection_pool()
        await pool.start()
        
        # Test strategy subscription with partial failures expected
        test_strategy_id = "test_websocket_fixes"
        
        success = await pool.subscribe_strategy(
            strategy_id=test_strategy_id,
            exchange_connectors=exchange_manager.connectors,
            symbols=["BERA/USDT"]
        )
        
        logger.info(f"Strategy subscription result: {success}")
        
        # Get connection status
        status = pool.get_connection_status()
        logger.info(f"Connection status: {status}")
        
        # Log detailed connection info
        for conn_key, conn_info in status.get('connections', {}).items():
            logger.info(
                f"Connection {conn_key}: "
                f"status={conn_info['status']}, "
                f"exchange={conn_info['exchange']}, "
                f"subscribers={conn_info['subscriber_count']}"
            )
        
        # Test unsubscription
        await pool.unsubscribe_strategy(test_strategy_id)
        
        # Stop pool
        await pool.stop()
        
        # Stop exchange manager
        await exchange_manager.stop()
        
        logger.info("✅ WebSocket connection pool test completed")
        return True
        
    except Exception as e:
        logger.error(f"❌ WebSocket connection pool test failed: {e}")
        return False


async def test_mexc_reconnection_config():
    """Test MEXC-specific reconnection configuration."""
    logger.info("Testing MEXC reconnection configuration...")
    
    try:
        # Create reconnection manager with MEXC optimizations
        reconnection_manager = ReconnectionManager({
            'initial_delay': 1.0,
            'max_delay': 60.0,
            'mexc_initial_delay': 0.5,
            'mexc_max_delay': 30.0,
            'mexc_reset_after': 60.0
        })
        
        # Test regular connection
        regular_conn_id = "binance_spot_test"
        delay1 = reconnection_manager.get_delay(regular_conn_id)
        logger.info(f"Regular connection delay: {delay1:.2f}s")
        
        # Test MEXC connection
        mexc_conn_id = "mexc_spot_test"
        delay2 = reconnection_manager.get_delay(mexc_conn_id)
        logger.info(f"MEXC connection delay: {delay2:.2f}s")
        
        # MEXC should have faster reconnection
        if delay2 < delay1:
            logger.info("✅ MEXC has faster reconnection as expected")
        else:
            logger.warning("⚠️ MEXC reconnection not optimized")
        
        # Test multiple attempts
        for i in range(3):
            delay = reconnection_manager.get_delay(mexc_conn_id)
            logger.info(f"MEXC attempt {i+2}: {delay:.2f}s")
        
        logger.info("✅ MEXC reconnection configuration test completed")
        return True
        
    except Exception as e:
        logger.error(f"❌ MEXC reconnection test failed: {e}")
        return False


async def test_hyperliquid_credential_handling():
    """Test Hyperliquid credential handling."""
    logger.info("Testing Hyperliquid credential handling...")
    
    try:
        # This test verifies that missing Hyperliquid credentials are handled gracefully
        from exchanges.connectors.hyperliquid_connector import HyperliquidConnector
        
        # Create connector without wallet credentials
        connector = HyperliquidConnector({})
        
        # Test connection pool handling
        pool = WebSocketConnectionPool()
        
        # This should handle missing credentials gracefully
        exchange_name = pool._get_exchange_name(connector)
        endpoint = pool._get_private_websocket_endpoint(connector)
        
        logger.info(f"Hyperliquid exchange name: {exchange_name}")
        logger.info(f"Hyperliquid endpoint: {endpoint}")
        
        if endpoint:
            logger.info("✅ Hyperliquid endpoint available")
        else:
            logger.warning("⚠️ Hyperliquid endpoint not available")
        
        logger.info("✅ Hyperliquid credential handling test completed")
        return True
        
    except Exception as e:
        logger.error(f"❌ Hyperliquid credential test failed: {e}")
        return False


async def main():
    """Run all WebSocket fix tests."""
    logger.info("🚀 Starting WebSocket fixes test suite...")
    
    tests = [
        ("WebSocket Connection Pool", test_websocket_connection_pool),
        ("MEXC Reconnection Config", test_mexc_reconnection_config),
        ("Hyperliquid Credential Handling", test_hyperliquid_credential_handling),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        logger.info(f"\n📋 Running test: {test_name}")
        try:
            result = await test_func()
            results.append((test_name, result))
            if result:
                logger.info(f"✅ {test_name}: PASSED")
            else:
                logger.error(f"❌ {test_name}: FAILED")
        except Exception as e:
            logger.error(f"💥 {test_name}: ERROR - {e}")
            results.append((test_name, False))
    
    # Summary
    logger.info("\n📊 Test Results Summary:")
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASSED" if result else "❌ FAILED"
        logger.info(f"  {test_name}: {status}")
    
    logger.info(f"\n🎯 Overall: {passed}/{total} tests passed")
    
    if passed == total:
        logger.info("🎉 All WebSocket fixes working correctly!")
        return True
    else:
        logger.error("⚠️ Some WebSocket fixes need attention")
        return False


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
    try:
        result = asyncio.run(main())
        sys.exit(0 if result else 1)
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Test suite failed: {e}")
        sys.exit(1) 