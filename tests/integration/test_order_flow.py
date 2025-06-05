#!/usr/bin/env python
"""
Order Flow Integration Tests

Tests order placement, cancellation, and tracking for all supported exchanges.
This file implements a standardized test for each exchange connector:
1. Place a limit order for BTC/USDT at $10,000 (a price that will never fill)
2. Wait 10 seconds and verify the order is tracked correctly
3. Cancel the order and verify cancellation succeeded
"""

import os
import sys
import asyncio
import pytest
import logging
import json
from decimal import Decimal
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

# Import project modules
import structlog
from exchanges.connectors import (
    BinanceConnector, BybitConnector, HyperliquidConnector, 
    MexcConnector, GateIOConnector, BitgetConnector
)
from order_management.order_manager import OrderManager
from order_management.order import Order, OrderStatus, OrderSide, OrderType

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
TEST_AMOUNT = Decimal("0.0005")   # Small amount: ~$25 at $50k - very safe for $100 account (spot)
TEST_AMOUNT_PERP = Decimal("0.0025") # Larger for futures: $50k × 0.0025 = $125 notional (meets $100 min)
WAIT_TIME = 10                    # Seconds to wait for order tracking


def load_exchange_credentials() -> Dict[str, Dict[str, Any]]:
    """
    Load exchange credentials from environment variables or config file.
    
    Returns:
        Dictionary of exchange credentials
    """
    # Try to load from environment variables first
    credentials = {}
    
    # Look for config file
    config_file = os.environ.get("EXCHANGE_CONFIG_FILE", "exchange_credentials.json")
    config_path = Path(__file__).parent.parent / "fixtures" / config_file
    
    if config_path.exists():
        try:
            with open(config_path, "r") as f:
                credentials = json.load(f)
            logger.info(f"Loaded credentials from {config_path}")
        except Exception as e:
            logger.error(f"Error loading credentials file: {e}")
    else:
        logger.warning(f"Credentials file not found: {config_path}")
        
        # Try to get from environment variables
        exchanges = ["binance", "bybit", "hyperliquid", "mexc", "gateio", "bitget"]
        for exchange in exchanges:
            api_key = os.environ.get(f"{exchange.upper()}_API_KEY")
            secret = os.environ.get(f"{exchange.upper()}_SECRET")
            
            if api_key and secret:
                credentials[exchange] = {
                    "api_key": api_key,
                    "secret": secret,
                    "sandbox": True
                }
    
    # Validate credentials
    valid_exchanges = [ex for ex, creds in credentials.items() if creds.get("api_key")]
    logger.info(f"Found credentials for exchanges: {', '.join(valid_exchanges)}")
    
    return credentials


def create_exchange_connectors(credentials: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """
    Create exchange connectors for all supported exchanges with credentials.
    
    Args:
        credentials: Dictionary of exchange credentials
    
    Returns:
        Dictionary of exchange connectors
    """
    connectors = {}
    
    # Map exchange names to connector classes
    connector_classes = {
        "binance": BinanceConnector,
        "binance_spot": BinanceConnector,
        "binance_perp": BinanceConnector,
        "bybit": BybitConnector,
        "bybit_spot": BybitConnector,
        "bybit_perp": BybitConnector,
        "hyperliquid": HyperliquidConnector,
        "hyperliquid_perp": HyperliquidConnector,
        "mexc": MexcConnector,
        "mexc_spot": MexcConnector,
        "mexc_perp": MexcConnector,
        "gateio": GateIOConnector,
        "gateio_spot": GateIOConnector,
        "gateio_perp": GateIOConnector,
        "bitget": BitgetConnector,
        "bitget_spot": BitgetConnector,
        "bitget_perp": BitgetConnector,
    }
    
    # Create connectors for each exchange with credentials
    for exchange, config in credentials.items():
        # Skip if no API key provided
        if not config.get("api_key"):
            logger.warning(f"Skipping {exchange} - no API key provided")
            continue
        
        # Handle spot/perp variations
        base_exchange = exchange.split("_")[0]
        if base_exchange in connector_classes:
            # Get correct connector class
            connector_class = connector_classes[exchange] if exchange in connector_classes else connector_classes[base_exchange]
            
            try:
                # Create connector instance
                connector = connector_class(config)
                connectors[exchange] = connector
                logger.info(f"Created connector for {exchange}")
            except Exception as e:
                logger.error(f"Failed to create connector for {exchange}: {e}")
    
    return connectors


@pytest.mark.asyncio
async def test_order_placement_cancellation_tracking():
    """Test order placement, cancellation, and tracking for all supported exchanges."""
    # Load credentials and create connectors
    credentials = load_exchange_credentials()
    exchange_connectors = create_exchange_connectors(credentials)
    
    # Skip test if no connectors are available
    if not exchange_connectors:
        pytest.skip("No exchange connectors available with credentials")
    
    # Create order manager
    order_manager = OrderManager(exchange_connectors)
    await order_manager.start()
    
    try:
        # Test each exchange
        for exchange_name, connector in exchange_connectors.items():
            logger.info(f"Testing order flow for {exchange_name}")
            
            # Connect to exchange
            connected = await connector.connect()
            assert connected, f"Failed to connect to {exchange_name}"
            
            # Place limit order at a price that won't fill
            # Choose appropriate amount based on exchange type
            amount = TEST_AMOUNT_PERP if "_perp" in exchange_name else TEST_AMOUNT
            
            order = await order_manager.create_order(
                symbol=TEST_SYMBOL,
                side=OrderSide.BUY,
                amount=amount,
                order_type=OrderType.LIMIT,
                price=TEST_PRICE,
                exchange=exchange_name
            )
            
            logger.info(f"Placed order on {exchange_name}: {order.id}")
            
            # Verify order was created
            assert order is not None, f"Failed to create order on {exchange_name}"
            assert order.exchange == exchange_name, f"Order exchange mismatch: {order.exchange} != {exchange_name}"
            assert order.symbol == TEST_SYMBOL, f"Order symbol mismatch: {order.symbol} != {TEST_SYMBOL}"
            assert order.price == TEST_PRICE, f"Order price mismatch: {order.price} != {TEST_PRICE}"
            
            # Wait for order to be registered and tracked
            logger.info(f"Waiting {WAIT_TIME} seconds for order tracking...")
            await asyncio.sleep(WAIT_TIME)
            
            # Refresh order status
            updated_order = await order_manager.get_order(order.id)
            assert updated_order is not None, f"Order not found in tracking system: {order.id}"
            
            # Check that order is in expected tracking maps
            orders_by_exchange = await order_manager.get_open_orders(exchange=exchange_name)
            assert any(o.id == order.id for o in orders_by_exchange), f"Order not found in exchange orders: {order.id}"
            
            orders_by_symbol = await order_manager.get_orders_by_symbol(TEST_SYMBOL)
            assert any(o.id == order.id for o in orders_by_symbol), f"Order not found in symbol orders: {order.id}"
            
            # Cancel the order
            logger.info(f"Cancelling order {order.id}")
            cancelled = await order_manager.cancel_order(order.id)
            assert cancelled, f"Failed to cancel order {order.id}"
            
            # Verify cancellation
            cancelled_order = await order_manager.get_order(order.id)
            assert cancelled_order.status == OrderStatus.CANCELLED, f"Order not cancelled: {cancelled_order.status}"
            
            logger.info(f"Successfully tested order flow for {exchange_name}")
            
            # Attempt to cancel order if test fails
            try:
                await order_manager.cancel_order(order.id)
            except:
                pass
            
    finally:
        # Clean up
        await order_manager.stop()
        
        # Disconnect from exchanges
        for exchange_name, connector in exchange_connectors.items():
            try:
                await connector.disconnect()
            except:
                pass


@pytest.mark.asyncio
async def test_individual_exchange_order_flow():
    """
    Parametrized test for individual exchanges.
    This allows running tests for specific exchanges using markers.
    """
    # Load credentials
    credentials = load_exchange_credentials()
    
    # Get available exchanges
    available_exchanges = list(credentials.keys())
    
    # Skip if no exchanges available
    if not available_exchanges:
        pytest.skip("No exchanges available with credentials")
    
    # Create parameter list for each exchange
    exchange_params = [
        pytest.param(
            exchange, 
            marks=pytest.mark.skipif(
                not credentials.get(exchange, {}).get("api_key"),
                reason=f"No credentials for {exchange}"
            )
        )
        for exchange in available_exchanges
    ]
    
    # Create test function for pytest.mark.parametrize
    @pytest.mark.parametrize("exchange_name", exchange_params)
    async def test_exchange(exchange_name):
        logger.info(f"Testing order flow for {exchange_name}")
        
        # Create single connector and order manager
        exchange_config = credentials.get(exchange_name, {})
        
        # Determine correct connector class
        base_exchange = exchange_name.split("_")[0]
        connector_class = None
        
        if base_exchange == "binance":
            connector_class = BinanceConnector
        elif base_exchange == "bybit":
            connector_class = BybitConnector
        elif base_exchange == "hyperliquid":
            connector_class = HyperliquidConnector
        elif base_exchange == "mexc":
            connector_class = MexcConnector
        elif base_exchange == "gateio":
            connector_class = GateIOConnector
        elif base_exchange == "bitget":
            connector_class = BitgetConnector
        else:
            pytest.skip(f"Unsupported exchange: {exchange_name}")
        
        # Create connector
        connector = connector_class(exchange_config)
        connectors = {exchange_name: connector}
        
        # Create order manager
        order_manager = OrderManager(connectors)
        await order_manager.start()
        
        try:
            # Connect to exchange
            connected = await connector.connect()
            assert connected, f"Failed to connect to {exchange_name}"
            
            # Place limit order at a price that won't fill
            # Choose appropriate amount based on exchange type
            amount = TEST_AMOUNT_PERP if "_perp" in exchange_name else TEST_AMOUNT
            
            order = await order_manager.create_order(
                symbol=TEST_SYMBOL,
                side=OrderSide.BUY,
                amount=amount,
                order_type=OrderType.LIMIT,
                price=TEST_PRICE,
                exchange=exchange_name
            )
            
            logger.info(f"Placed order on {exchange_name}: {order.id}")
            
            # Verify order was created
            assert order is not None, f"Failed to create order on {exchange_name}"
            assert order.exchange == exchange_name, f"Order exchange mismatch: {order.exchange} != {exchange_name}"
            
            # Wait for order to be registered and tracked
            logger.info(f"Waiting {WAIT_TIME} seconds for order tracking...")
            await asyncio.sleep(WAIT_TIME)
            
            # Refresh order status
            updated_order = await order_manager.get_order(order.id)
            assert updated_order is not None, f"Order not found in tracking system: {order.id}"
            
            # Check that order is in expected tracking maps
            orders_by_exchange = await order_manager.get_open_orders(exchange=exchange_name)
            assert any(o.id == order.id for o in orders_by_exchange), f"Order not found in exchange orders: {order.id}"
            
            # Cancel the order
            logger.info(f"Cancelling order {order.id}")
            cancelled = await order_manager.cancel_order(order.id)
            assert cancelled, f"Failed to cancel order {order.id}"
            
            # Verify cancellation
            cancelled_order = await order_manager.get_order(order.id)
            assert cancelled_order.status == OrderStatus.CANCELLED, f"Order not cancelled: {cancelled_order.status}"
            
            logger.info(f"Successfully tested order flow for {exchange_name}")
            
        finally:
            # Clean up
            await order_manager.stop()
            await connector.disconnect()
    
    # Run the parametrized test
    await test_exchange
            

if __name__ == "__main__":
    """Run the tests directly for quick testing."""
    # Use asyncio to run the tests
    asyncio.run(test_order_placement_cancellation_tracking())
