#!/usr/bin/env python
"""
Simple Order Flow Test

Tests order placement, cancellation, and tracking directly with exchange connectors.
This is a simplified version that doesn't require the full database infrastructure.
"""

import os
import sys
import asyncio
import json
import logging
from decimal import Decimal
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Import project modules
import structlog
from exchanges.connectors import BinanceConnector
from order_management.order import OrderSide, OrderType

# Configure logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
        structlog.processors.format_exc_info,
        structlog.dev.ConsoleRenderer(),
    ],
)
logger = structlog.get_logger()

# Test parameters
TEST_SYMBOL = "BTC/USDT"
TEST_PRICE = Decimal("50000.00")  # ~50% below current market - safe but within filters
TEST_AMOUNT = Decimal("0.0005")   # Small amount: ~$25 at $50k - very safe for $100 account
WAIT_TIME = 10                    # Seconds to wait for order tracking


def load_exchange_credentials() -> Dict[str, Dict[str, Any]]:
    """Load exchange credentials from config file."""
    config_path = Path(__file__).parent / "fixtures" / "exchange_credentials.json"
    
    if config_path.exists():
        try:
            with open(config_path, "r") as f:
                credentials = json.load(f)
            logger.info(f"Loaded credentials from {config_path}")
            return credentials
        except Exception as e:
            logger.error(f"Error loading credentials file: {e}")
            return {}
    else:
        logger.warning(f"Credentials file not found: {config_path}")
        return {}


async def test_binance_spot_order_flow():
    """Test order flow for Binance Spot."""
    logger.info("Testing Binance Spot order flow")
    
    # Load credentials
    credentials = load_exchange_credentials()
    if "binance_spot" not in credentials:
        logger.error("No credentials found for binance_spot")
        return False
        
    # Create connector
    config = credentials["binance_spot"]
    connector = BinanceConnector(config)
    
    try:
        # Connect to exchange
        connected = await connector.connect()
        if not connected:
            logger.error("Failed to connect to Binance Spot")
            return False
            
        logger.info("Successfully connected to Binance Spot")
        
        # Place a limit order at a price that won't fill
        order_result = await connector.place_order(
            symbol=TEST_SYMBOL,
            side=OrderSide.BUY.value,
            amount=TEST_AMOUNT,
            price=TEST_PRICE,
            order_type=OrderType.LIMIT.value
        )
        
        if not order_result:
            logger.error("Failed to place order")
            return False
            
        order_id = order_result.get('id')
        logger.info(f"Successfully placed order: {order_id}")
        
        # Wait for a bit
        logger.info(f"Waiting {WAIT_TIME} seconds...")
        await asyncio.sleep(WAIT_TIME)
        
        # Check order status
        order_status = await connector.get_order_status(order_id, TEST_SYMBOL)
        logger.info(f"Order status: {order_status.get('status', 'unknown')}")
        
        # Cancel the order
        cancel_result = await connector.cancel_order(order_id, TEST_SYMBOL)
        if cancel_result:
            logger.info(f"Successfully cancelled order: {order_id}")
        else:
            logger.error(f"Failed to cancel order: {order_id}")
            return False
            
        # Verify cancellation
        final_status = await connector.get_order_status(order_id, TEST_SYMBOL)
        logger.info(f"Final order status: {final_status.get('status', 'unknown')}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error in Binance Spot test: {e}")
        return False
    finally:
        # Disconnect
        await connector.disconnect()


async def test_binance_perp_order_flow():
    """Test order flow for Binance Perpetual Futures."""
    logger.info("Testing Binance Perpetual Futures order flow")
    
    # Load credentials
    credentials = load_exchange_credentials()
    if "binance_perp" not in credentials:
        logger.error("No credentials found for binance_perp")
        return False
        
    # Create connector
    config = credentials["binance_perp"]
    connector = BinanceConnector(config)
    
    try:
        # Connect to exchange
        connected = await connector.connect()
        if not connected:
            logger.error("Failed to connect to Binance Perp")
            return False
            
        logger.info("Successfully connected to Binance Perp")
        
        # Place a limit order at a price that won't fill
        order_result = await connector.place_order(
            symbol=TEST_SYMBOL,
            side=OrderSide.BUY.value,
            amount=TEST_AMOUNT,
            price=TEST_PRICE,
            order_type=OrderType.LIMIT.value
        )
        
        if not order_result:
            logger.error("Failed to place order")
            return False
            
        order_id = order_result.get('id')
        logger.info(f"Successfully placed order: {order_id}")
        
        # Wait for a bit
        logger.info(f"Waiting {WAIT_TIME} seconds...")
        await asyncio.sleep(WAIT_TIME)
        
        # Check order status
        order_status = await connector.get_order_status(order_id, TEST_SYMBOL)
        logger.info(f"Order status: {order_status.get('status', 'unknown')}")
        
        # Cancel the order
        cancel_result = await connector.cancel_order(order_id, TEST_SYMBOL)
        if cancel_result:
            logger.info(f"Successfully cancelled order: {order_id}")
        else:
            logger.error(f"Failed to cancel order: {order_id}")
            return False
            
        # Verify cancellation
        final_status = await connector.get_order_status(order_id, TEST_SYMBOL)
        logger.info(f"Final order status: {final_status.get('status', 'unknown')}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error in Binance Perp test: {e}")
        return False
    finally:
        # Disconnect
        await connector.disconnect()


async def main():
    """Run all tests."""
    logger.info("Starting simple order flow tests")
    
    results = {}
    
    # Test Binance Spot
    results['binance_spot'] = await test_binance_spot_order_flow()
    
    # Test Binance Perp
    results['binance_perp'] = await test_binance_perp_order_flow()
    
    # Print results
    logger.info("Test Results:")
    for exchange, result in results.items():
        status = "PASSED" if result else "FAILED"
        logger.info(f"  {exchange}: {status}")
    
    # Return overall success
    return all(results.values())


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1) 