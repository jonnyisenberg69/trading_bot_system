#!/usr/bin/env python3
"""
Test script to verify Binance spot vs perp market type fix.
"""

import asyncio
import json
import logging
from pathlib import Path
import sys

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from api.services.exchange_manager import ExchangeManager
from config.settings import load_config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

async def test_binance_market_types():
    """Test that Binance spot and perp are properly differentiated."""
    try:
        # Load config
        config = load_config()
        
        # Create exchange manager
        exchange_manager = ExchangeManager(config)
        
        # Start exchange manager (this will test connections)
        await exchange_manager.start()
        
        # Get connectors
        connectors = exchange_manager.get_exchange_connectors()
        
        logger.info(f"Found {len(connectors)} connected exchanges:")
        
        for connection_id, connector in connectors.items():
            logger.info(f"  {connection_id}:")
            logger.info(f"    Connector type: {type(connector).__name__}")
            
            if hasattr(connector, 'market_type'):
                logger.info(f"    Market type: {connector.market_type}")
            
            if hasattr(connector, 'exchange'):
                exchange_class = type(connector.exchange).__name__
                logger.info(f"    CCXT class: {exchange_class}")
                
                # Check if it's using the correct Binance API
                if 'binance' in connection_id.lower():
                    if connection_id.endswith('_spot'):
                        expected_class = 'binance'
                        expected_market_type = 'spot'
                    elif connection_id.endswith('_perp'):
                        expected_class = 'binanceusdm'
                        expected_market_type = 'future'
                    else:
                        expected_class = 'unknown'
                        expected_market_type = 'unknown'
                    
                    logger.info(f"    Expected CCXT class: {expected_class}")
                    logger.info(f"    Expected market type: {expected_market_type}")
                    
                    # Verify correct configuration
                    if exchange_class.lower() == expected_class:
                        logger.info(f"    ✅ CORRECT: Using {exchange_class} for {connection_id}")
                    else:
                        logger.error(f"    ❌ WRONG: Using {exchange_class} for {connection_id}, expected {expected_class}")
                    
                    if hasattr(connector, 'market_type') and connector.market_type == expected_market_type:
                        logger.info(f"    ✅ CORRECT: Market type {connector.market_type} for {connection_id}")
                    else:
                        logger.error(f"    ❌ WRONG: Market type {getattr(connector, 'market_type', 'unknown')} for {connection_id}, expected {expected_market_type}")
            
            logger.info("")
        
        # Test a simple order placement to verify they're using different APIs
        if 'binance_spot' in connectors and 'binance_perp' in connectors:
            logger.info("Testing order placement differences...")
            
            spot_connector = connectors['binance_spot']
            perp_connector = connectors['binance_perp']
            
            # Get orderbooks to verify they're accessing different markets
            try:
                spot_orderbook = await spot_connector.get_orderbook('BERA/USDT', limit=5)
                perp_orderbook = await perp_connector.get_orderbook('BERA/USDT', limit=5)
                
                if spot_orderbook and perp_orderbook:
                    logger.info("✅ Both spot and perp orderbooks retrieved successfully")
                    logger.info(f"   Spot best bid: {spot_orderbook['bids'][0][0] if spot_orderbook['bids'] else 'N/A'}")
                    logger.info(f"   Perp best bid: {perp_orderbook['bids'][0][0] if perp_orderbook['bids'] else 'N/A'}")
                else:
                    logger.warning("⚠️  Could not retrieve orderbooks for comparison")
                    
            except Exception as e:
                logger.error(f"Error testing orderbooks: {e}")
        
        # Stop exchange manager
        await exchange_manager.stop()
        
        logger.info("Test completed successfully!")
        
    except Exception as e:
        logger.error(f"Test failed: {e}", exc_info=True)

if __name__ == '__main__':
    asyncio.run(test_binance_market_types()) 