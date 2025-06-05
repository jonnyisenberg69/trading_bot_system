#!/usr/bin/env python
"""
Helper script to run order flow tests directly.

This script allows running order flow tests directly without using pytest,
which can be useful for quick testing during development.

Usage:
    python run_order_flow_tests.py [exchange_name]
    
    If exchange_name is provided, it will test only that exchange.
    Otherwise, it will test all exchanges with available credentials.
"""

import os
import sys
import asyncio
import logging
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Import test functions
from integration.test_order_flow import (
    load_exchange_credentials,
    create_exchange_connectors,
    test_order_placement_cancellation_tracking
)


async def run_single_exchange_test(exchange_name):
    """Run test for a single exchange."""
    from integration.test_order_flow import TEST_SYMBOL, TEST_PRICE, TEST_AMOUNT, WAIT_TIME
    from order_management.order_manager import OrderManager
    from order_management.order import OrderSide, OrderType, OrderStatus
    import structlog
    
    logger = structlog.get_logger()
    
    logger.info(f"Testing order flow for {exchange_name}")
    
    # Load credentials
    credentials = load_exchange_credentials()
    if exchange_name not in credentials:
        logger.error(f"No credentials found for {exchange_name}")
        return False
    
    # Create connector
    exchange_config = credentials[exchange_name]
    connectors = create_exchange_connectors({exchange_name: exchange_config})
    
    if not connectors:
        logger.error(f"Failed to create connector for {exchange_name}")
        return False
    
    connector = connectors[exchange_name]
    
    # Create order manager
    order_manager = OrderManager(connectors)
    await order_manager.start()
    
    try:
        # Connect to exchange
        connected = await connector.connect()
        if not connected:
            logger.error(f"Failed to connect to {exchange_name}")
            return False
        
        # Place limit order at a price that won't fill
        try:
            order = await order_manager.create_order(
                symbol=TEST_SYMBOL,
                side=OrderSide.BUY,
                amount=TEST_AMOUNT,
                order_type=OrderType.LIMIT,
                price=TEST_PRICE,
                exchange=exchange_name
            )
            
            logger.info(f"Placed order on {exchange_name}: {order.id}")
            
            # Wait for order to be registered and tracked
            logger.info(f"Waiting {WAIT_TIME} seconds for order tracking...")
            await asyncio.sleep(WAIT_TIME)
            
            # Refresh order status
            updated_order = await order_manager.get_order(order.id)
            if not updated_order:
                logger.error(f"Order not found in tracking system: {order.id}")
                return False
            
            logger.info(f"Order status: {updated_order.status}")
            
            # Cancel the order
            logger.info(f"Cancelling order {order.id}")
            cancelled = await order_manager.cancel_order(order.id)
            if not cancelled:
                logger.error(f"Failed to cancel order {order.id}")
                return False
            
            # Verify cancellation
            cancelled_order = await order_manager.get_order(order.id)
            if cancelled_order.status != OrderStatus.CANCELLED:
                logger.error(f"Order not cancelled: {cancelled_order.status}")
                return False
            
            logger.info(f"Successfully tested order flow for {exchange_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error testing {exchange_name}: {e}")
            return False
            
    finally:
        # Clean up
        await order_manager.stop()
        await connector.disconnect()


async def main():
    """Main function."""
    # Check if exchange name is provided
    if len(sys.argv) > 1:
        exchange_name = sys.argv[1]
        success = await run_single_exchange_test(exchange_name)
        sys.exit(0 if success else 1)
    else:
        # Run tests for all exchanges
        await test_order_placement_cancellation_tracking()


if __name__ == "__main__":
    asyncio.run(main()) 