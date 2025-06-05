#!/usr/bin/env python
"""
Direct Exchange Test

Tests order placement, cancellation, and tracking directly with exchange connectors.
This version avoids any database dependencies by importing OrderType from base_connector.
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
from enum import Enum

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Import project modules
import structlog
from exchanges.connectors.binance import BinanceConnector
from exchanges.base_connector import OrderType, OrderSide

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
TEST_PRICE = Decimal("50000.00")  # ~50% below current market (~$104k) - safe but within filters  
TEST_AMOUNT = Decimal("0.0005")   # Small amount: ~$25 at $50k - very safe for $100 account
TEST_AMOUNT_PERP = Decimal("0.0025") # Larger for futures: $50k × 0.0025 = $125 notional (meets $100 min)
WAIT_TIME = 10                    # Seconds to wait for order tracking

def calculate_safe_order_amount(usdt_balance: Decimal, price: Decimal) -> Decimal:
    """Calculate a safe order amount based on available balance."""
    # Use 80% of available balance to be safe
    safe_balance = usdt_balance * Decimal("0.8")
    
    # Calculate max BTC we can afford
    max_btc = safe_balance / price
    
    # Use smaller amounts for safety
    safe_amounts = [
        Decimal("0.0001"),  # ~$5 at $50k
        Decimal("0.0002"),  # ~$10 at $50k  
        Decimal("0.0005"),  # ~$25 at $50k
        Decimal("0.001"),   # ~$50 at $50k
    ]
    
    # Find largest amount we can afford
    for amount in reversed(safe_amounts):
        if amount <= max_btc:
            return amount
    
    # If we can't afford even the smallest, return a tiny amount
    return min(max_btc, Decimal("0.00001"))

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
        
        # Check balance first
        balance = await connector.get_balance("USDT")
        usdt_balance = balance.get("USDT", Decimal("0"))
        logger.info(f"USDT Balance: {usdt_balance}")
        
        if usdt_balance < Decimal("5"):  # Need at least $5
            logger.error("Insufficient USDT balance (need at least $5)")
            return False
            
        # Calculate safe order amount
        safe_amount = calculate_safe_order_amount(usdt_balance, TEST_PRICE)
        logger.info(f"Using safe order amount: {safe_amount} BTC (cost: ${safe_amount * TEST_PRICE})")
        
        # Place a limit order at a price that won't fill
        order_result = await connector.place_order(
            symbol=TEST_SYMBOL,
            side=OrderSide.BUY.value,
            amount=safe_amount,
            price=TEST_PRICE,
            order_type=OrderType.LIMIT
        )
        
        if not order_result:
            logger.error("Failed to place order")
            return False
            
        order_id = order_result.get('id')
        logger.info(f"✅ SUCCESS: Successfully placed order: {order_id}")
        logger.info(f"Order details: {order_result}")
        
        # Wait for tracking period
        logger.info(f"⏰ TRACKING: Waiting {WAIT_TIME} seconds for order tracking...")
        await asyncio.sleep(WAIT_TIME)
        
        # Check order status (this demonstrates internal tracking)
        order_status = await connector.get_order_status(order_id, TEST_SYMBOL)
        logger.info(f"📊 ORDER STATUS: {order_status.get('status', 'unknown')}")
        logger.info(f"Status details: {order_status}")
        
        # Cancel the order
        logger.info(f"🗑️ CANCELLING: Cancelling order {order_id}")
        cancel_result = await connector.cancel_order(order_id, TEST_SYMBOL)
        if cancel_result:
            logger.info(f"✅ CANCELLED: Successfully cancelled order: {order_id}")
            logger.info(f"Cancel result: {cancel_result}")
        else:
            logger.error(f"❌ FAILED: Failed to cancel order: {order_id}")
            return False
            
        # Verify cancellation
        final_status = await connector.get_order_status(order_id, TEST_SYMBOL)
        logger.info(f"🔍 FINAL STATUS: {final_status.get('status', 'unknown')}")
        logger.info(f"Final details: {final_status}")
        
        logger.info("🎉 COMPLETE: Order flow test completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Error in Binance Spot test: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False
    finally:
        # Disconnect
        try:
            await connector.disconnect()
        except:
            pass


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
        
        # For futures, we'll use the preset amount since margin is different
        logger.info(f"Using futures order amount: {TEST_AMOUNT_PERP} BTC (notional: ${TEST_AMOUNT_PERP * TEST_PRICE})")
        
        # Place a limit order at a price that won't fill
        order_result = await connector.place_order(
            symbol=TEST_SYMBOL,
            side=OrderSide.BUY.value,
            amount=TEST_AMOUNT_PERP,
            price=TEST_PRICE,
            order_type=OrderType.LIMIT
        )
        
        if not order_result:
            logger.error("Failed to place order")
            return False
            
        order_id = order_result.get('id')
        logger.info(f"✅ SUCCESS: Successfully placed order: {order_id}")
        logger.info(f"Order details: {order_result}")
        
        # Wait for tracking period
        logger.info(f"⏰ TRACKING: Waiting {WAIT_TIME} seconds for order tracking...")
        await asyncio.sleep(WAIT_TIME)
        
        # Check order status (this demonstrates internal tracking)
        order_status = await connector.get_order_status(order_id, TEST_SYMBOL)
        logger.info(f"📊 ORDER STATUS: {order_status.get('status', 'unknown')}")
        logger.info(f"Status details: {order_status}")
        
        # Cancel the order
        logger.info(f"🗑️ CANCELLING: Cancelling order {order_id}")
        cancel_result = await connector.cancel_order(order_id, TEST_SYMBOL)
        if cancel_result:
            logger.info(f"✅ CANCELLED: Successfully cancelled order: {order_id}")
            logger.info(f"Cancel result: {cancel_result}")
        else:
            logger.error(f"❌ FAILED: Failed to cancel order: {order_id}")
            return False
            
        # Verify cancellation
        final_status = await connector.get_order_status(order_id, TEST_SYMBOL)
        logger.info(f"🔍 FINAL STATUS: {final_status.get('status', 'unknown')}")
        logger.info(f"Final details: {final_status}")
        
        logger.info("🎉 COMPLETE: Order flow test completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Error in Binance Perp test: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False
    finally:
        # Disconnect
        try:
            await connector.disconnect()
        except:
            pass


async def main():
    """Run all tests."""
    logger.info("Starting direct exchange order flow tests")
    
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