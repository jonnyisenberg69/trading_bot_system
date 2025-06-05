#!/usr/bin/env python
"""
API Permissions Diagnostic Test

This test checks API key permissions and provides guidance on fixing any issues.
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

# Configure logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
        structlog.processors.format_exc_info,
        structlog.dev.ConsoleRenderer(),
    ],
)
logger = structlog.get_logger()


def load_exchange_credentials() -> Dict[str, Dict[str, Any]]:
    """Load exchange credentials from config file."""
    config_path = Path(__file__).parent / "fixtures" / "exchange_credentials.json"
    
    if config_path.exists():
        try:
            with open(config_path, "r") as f:
                credentials = json.load(f)
            return credentials
        except Exception as e:
            logger.error(f"Error loading credentials file: {e}")
            return {}
    else:
        logger.warning(f"Credentials file not found: {config_path}")
        return {}


async def test_api_permissions():
    """Test API permissions for both Binance Spot and Perp."""
    logger.info("=" * 60)
    logger.info("API PERMISSIONS DIAGNOSTIC TEST")
    logger.info("=" * 60)
    
    credentials = load_exchange_credentials()
    
    for exchange_name in ['binance_spot', 'binance_perp']:
        if exchange_name not in credentials:
            logger.warning(f"No credentials found for {exchange_name}")
            continue
            
        logger.info(f"\n🔍 Testing {exchange_name.upper()}")
        logger.info("-" * 40)
        
        config = credentials[exchange_name]
        connector = BinanceConnector(config)
        
        try:
            # Test connection
            logger.info("📡 Testing connection...")
            connected = await connector.connect()
            if not connected:
                logger.error("❌ Connection failed")
                continue
            logger.info("✅ Connection successful")
            
            # Test API key info
            logger.info("🔑 Testing API key permissions...")
            try:
                # Try to get account info (read permission)
                balance = await connector.get_balance()
                logger.info("✅ Read permissions: OK")
                
                # Show available balance for context
                if balance:
                    logger.info(f"💰 Available balances:")
                    for currency, amount in balance.items():
                        if amount > 0:
                            logger.info(f"   {currency}: {amount}")
                else:
                    logger.info("💰 No balances found")
                    
            except Exception as e:
                logger.error(f"❌ Read permissions failed: {e}")
                
            # Test trading permissions with a very small order
            logger.info("📈 Testing trading permissions...")
            try:
                # Get current BTC price to place order far below market
                orderbook = await connector.get_orderbook("BTC/USDT", limit=1)
                if orderbook and orderbook.get('bids'):
                    current_price = float(orderbook['bids'][0][0])
                    # Place order 50% below current price (will never fill)
                    test_price = current_price * 0.5
                    logger.info(f"   Current BTC price: ~${current_price:,.2f}")
                    logger.info(f"   Test order price: ${test_price:,.2f} (safe - won't fill)")
                    
                    # This is where we expect the error
                    logger.info("   Attempting to place test order...")
                    # We'll catch the specific error to diagnose
                    
                else:
                    logger.warning("❌ Could not get BTC price for test")
                    
            except Exception as e:
                error_msg = str(e)
                logger.error(f"❌ Trading permissions test failed: {error_msg}")
                
                # Analyze the error and provide specific guidance
                if "-2015" in error_msg or "Invalid API-key" in error_msg:
                    logger.info("\n🔧 DIAGNOSIS: API Key Permissions Issue")
                    logger.info("   Possible causes:")
                    logger.info("   1. API key doesn't have trading permissions enabled")
                    logger.info("   2. Using sandbox=true but keys are for live trading")
                    logger.info("   3. Using sandbox=false but keys are for testnet")
                    logger.info("   4. IP address restrictions on the API key")
                    logger.info("   5. For futures: Need futures trading permissions")
                    
                    logger.info("\n💡 SOLUTIONS:")
                    logger.info("   1. Check Binance API Management:")
                    logger.info("      - Go to Binance > API Management")
                    logger.info("      - Ensure 'Enable Trading' is checked")
                    logger.info("      - For futures: Enable 'Enable Futures'")
                    logger.info("   2. Check sandbox setting:")
                    if config.get('sandbox', False):
                        logger.info("      - Currently using sandbox=true")
                        logger.info("      - Make sure your API keys are for Binance Testnet")
                        logger.info("      - Or try changing sandbox=false for live keys")
                    else:
                        logger.info("      - Currently using sandbox=false (live trading)")
                        logger.info("      - Make sure your API keys are for live Binance")
                        logger.info("      - Or try changing sandbox=true for testnet keys")
                    logger.info("   3. Check IP restrictions:")
                    logger.info("      - Remove IP restrictions from API key settings")
                    logger.info("      - Or add your current IP to the whitelist")
                    
                elif "Insufficient balance" in error_msg:
                    logger.info("✅ Trading permissions OK - just insufficient balance")
                    
        except Exception as e:
            logger.error(f"❌ General error testing {exchange_name}: {e}")
            
        finally:
            try:
                await connector.disconnect()
                logger.info("🔌 Disconnected")
            except:
                pass
    
    logger.info("\n" + "=" * 60)
    logger.info("DIAGNOSTIC COMPLETE")
    logger.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(test_api_permissions()) 