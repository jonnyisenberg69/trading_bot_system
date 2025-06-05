#!/usr/bin/env python
"""
Single Exchange Order Flow Test

Tests order placement, cancellation, and tracking for ONE exchange at a time.
This allows for proper account preparation and focused testing.
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
from exchanges.connectors.binance import BinanceConnector
from exchanges.connectors.bybit import BybitConnector
from exchanges.connectors.mexc import MexcConnector
from exchanges.connectors.gateio import GateIOConnector
from exchanges.connectors.bitget import BitgetConnector
from exchanges.connectors.hyperliquid import HyperliquidConnector
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
TEST_SYMBOL = "BTC/USDC:USDC"  # Hyperliquid perpetual futures symbol (uses USDC, not USD)
TEST_PRICE = Decimal("50000.00")  # Low price that won't fill for BTC futures
TEST_AMOUNT_SPOT = Decimal("100")     # 100 BERA tokens for spot (not used for Hyperliquid)
TEST_AMOUNT_PERP = Decimal("0.001")   # Small BTC amount for futures: ~$50 notional at $50k
WAIT_TIME = 10  # Seconds to wait for order tracking

def load_exchange_credentials() -> Dict[str, Any]:
    """Load exchange credentials from JSON file."""
    creds_file = Path("tests/fixtures/exchange_credentials.json")
    if not creds_file.exists():
        logger.error(f"Credentials file not found: {creds_file}")
        return {}
    
    with open(creds_file, 'r') as f:
        return json.load(f)

def get_connector_class(exchange_name: str):
    """Get the appropriate connector class for an exchange."""
    connector_map = {
        'binance_spot': BinanceConnector,
        'binance_perp': BinanceConnector,
        'bybit_spot': BybitConnector,
        'bybit_perp': BybitConnector,
        'mexc_spot': MexcConnector,
        'gateio_spot': GateIOConnector,
        'bitget_spot': BitgetConnector,
        'hyperliquid_perp': HyperliquidConnector,
    }
    return connector_map.get(exchange_name)

async def test_exchange_order_flow(exchange_name: str, config: Dict[str, Any]) -> bool:
    """Test order flow for a specific exchange."""
    logger.info(f"🏦 Testing {exchange_name.upper()} order flow")
    
    # Get connector class
    connector_class = get_connector_class(exchange_name)
    if not connector_class:
        logger.error(f"No connector found for {exchange_name}")
        return False
    
    # Create connector
    connector = connector_class(config)
    
    try:
        # Connect to exchange
        connected = await connector.connect()
        if not connected:
            logger.error(f"❌ Failed to connect to {exchange_name}")
            return False
            
        logger.info(f"✅ Successfully connected to {exchange_name}")
        
        # Choose appropriate amount based on exchange type
        is_perp = "_perp" in exchange_name
        amount = TEST_AMOUNT_PERP if is_perp else TEST_AMOUNT_SPOT
        
        logger.info(f"💰 Using order amount: {amount} {TEST_SYMBOL.split('/')[0]} (notional: ${float(TEST_PRICE * amount):.2f})")
        
        # 1. PLACE ORDER
        logger.info(f"📝 PLACING: Limit order for {amount} {TEST_SYMBOL} at ${TEST_PRICE}")
        order = await connector.place_order(
            symbol=TEST_SYMBOL,
            side=OrderSide.BUY.value,
            amount=amount,
            price=TEST_PRICE,
            order_type=OrderType.LIMIT
        )
        
        if not order or not order.get('id'):
            logger.error(f"❌ Failed to place order on {exchange_name}")
            return False
            
        order_id = order['id']
        logger.info(f"✅ SUCCESS: Successfully placed order: {order_id}")
        logger.info(f"Order details: {order}")
        
        # 2. TRACK ORDER (wait and check status)
        logger.info(f"⏰ TRACKING: Waiting {WAIT_TIME} seconds for order tracking...")
        await asyncio.sleep(WAIT_TIME)
        
        # Check order status
        status_order = await connector.get_order_status(order_id, TEST_SYMBOL)
        if status_order:
            logger.info(f"📊 ORDER STATUS: {status_order.get('status', 'UNKNOWN')}")
            logger.info(f"Status details: {status_order}")
        else:
            logger.warning(f"⚠️ Could not retrieve order status for {order_id}")
        
        # 3. CANCEL ORDER
        logger.info(f"🗑️ CANCELLING: Cancelling order {order_id}")
        cancel_result = await connector.cancel_order(order_id, TEST_SYMBOL)
        
        if cancel_result:
            logger.info(f"✅ CANCELLED: Successfully cancelled order: {order_id}")
            logger.info(f"Cancel result: {cancel_result}")
        else:
            logger.error(f"❌ Failed to cancel order {order_id}")
            return False
        
        # 4. VERIFY FINAL STATUS
        await asyncio.sleep(2)  # Brief wait for cancellation to process
        final_status = await connector.get_order_status(order_id, TEST_SYMBOL)
        if final_status:
            logger.info(f"🔍 FINAL STATUS: {final_status.get('status', 'UNKNOWN')}")
            logger.info(f"Final details: {final_status}")
        
        logger.info(f"🎉 COMPLETE: Order flow test completed successfully for {exchange_name}!")
        return True
        
    except Exception as e:
        logger.error(f"❌ ERROR in {exchange_name} test: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        # Disconnect
        try:
            await connector.disconnect()
            logger.info(f"👋 Disconnected from {exchange_name}")
        except Exception as e:
            logger.error(f"Error disconnecting from {exchange_name}: {e}")

async def main():
    """Test Hyperliquid perpetual futures exchange."""
    # Load credentials
    credentials = load_exchange_credentials()
    if not credentials:
        logger.error("No credentials loaded")
        return
    
    # Test Hyperliquid Perp
    exchange_name = "hyperliquid_perp"
    
    if exchange_name not in credentials:
        logger.error(f"⚠️ No credentials found for {exchange_name}")
        return
        
    # Display test info
    asset_type = "Perpetual Futures"
    amount = TEST_AMOUNT_PERP  # Always use perp amount for Hyperliquid
    
    logger.info(f"🚀 Testing: {exchange_name.upper()}")
    logger.info(f"📊 Asset Type: {asset_type}")
    logger.info(f"🔸 Symbol: {TEST_SYMBOL}")
    logger.info(f"💰 Amount: {amount} {TEST_SYMBOL} (~${float(TEST_PRICE * amount):.0f} notional)")
    logger.info(f"💵 Price: ${TEST_PRICE} (safe - won't fill)")
    
    # Run the test
    success = await test_exchange_order_flow(exchange_name, credentials[exchange_name])
    
    if success:
        logger.info(f"🎉 ✅ {exchange_name.upper()} TEST PASSED!")
    else:
        logger.error(f"❌ {exchange_name.upper()} TEST FAILED!")

if __name__ == "__main__":
    asyncio.run(main()) 