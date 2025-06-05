#!/usr/bin/env python3

"""
Test script to verify connector order placement functionality.
"""

import asyncio
import json
import logging
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from exchanges.connectors import create_exchange_connector
from config.settings import load_config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

async def test_connector_methods():
    """Test that our connectors have the correct methods."""
    try:
        # Load config
        config = load_config()
        
        # Test one connector
        exchange_config = config['exchanges'][0]  # Get first exchange
        
        connector = create_exchange_connector(
            exchange_config['name'],
            {
                'api_key': exchange_config.get('api_key', ''),
                'secret': exchange_config.get('api_secret', ''),
                'wallet_address': exchange_config.get('wallet_address', ''),
                'private_key': exchange_config.get('private_key', ''),
                'passphrase': exchange_config.get('passphrase', ''),
                'sandbox': exchange_config.get('testnet', True),
                'market_type': 'spot',
                'testnet': exchange_config.get('testnet', True)
            }
        )
        
        if connector:
            logger.info(f"✅ Successfully created {exchange_config['name']} connector")
            
            # Check if methods exist
            methods_to_check = ['place_order', 'cancel_order', 'get_orderbook', 'get_balance']
            
            for method in methods_to_check:
                if hasattr(connector, method):
                    logger.info(f"✅ {exchange_config['name']} has {method}() method")
                else:
                    logger.error(f"❌ {exchange_config['name']} missing {method}() method")
            
            # Test method signatures
            try:
                # Don't actually place an order, just check the method signature
                import inspect
                sig = inspect.signature(connector.place_order)
                logger.info(f"✅ place_order signature: {sig}")
                
                sig = inspect.signature(connector.cancel_order)
                logger.info(f"✅ cancel_order signature: {sig}")
                
            except Exception as sig_error:
                logger.error(f"❌ Error checking method signatures: {sig_error}")
                
        else:
            logger.error(f"❌ Failed to create {exchange_config['name']} connector")
            
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")

if __name__ == "__main__":
    asyncio.run(test_connector_methods()) 