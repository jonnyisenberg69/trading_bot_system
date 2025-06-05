#!/usr/bin/env python3
"""
Simple Hyperliquid test script to debug order placement issues.
"""

import asyncio
import json
import logging
import sys
import os

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

async def test_hyperliquid_minimal():
    """Test Hyperliquid with minimal approach."""
    try:
        import ccxt.pro as ccxt
        
        # Load config
        with open('config/config.json', 'r') as f:
            config = json.load(f)
        
        # Find Hyperliquid config
        hl_config = None
        for exchange in config['exchanges']:
            if exchange['name'] == 'hyperliquid':
                hl_config = exchange
                break
        
        if not hl_config:
            logger.error("Hyperliquid config not found")
            return
        
        logger.info("Testing Hyperliquid with CCXT directly...")
        
        # Initialize CCXT exchange directly
        exchange = ccxt.hyperliquid({
            'walletAddress': hl_config['wallet_address'],
            'privateKey': hl_config['private_key'],
            'sandbox': hl_config.get('testnet', False),
            'enableRateLimit': True,
            'rateLimit': 200,
            'options': {
                'defaultType': 'swap',
            }
        })
        
        # Test connection
        logger.info("Loading markets...")
        markets = await exchange.load_markets()
        logger.info(f"Found {len(markets)} markets")
        
        # Check if BERA/USDC:USDC exists
        symbol = 'BERA/USDC:USDC'
        if symbol not in markets:
            logger.error(f"Symbol {symbol} not found in markets")
            # List available BERA markets
            bera_markets = [m for m in markets.keys() if 'BERA' in m]
            logger.info(f"Available BERA markets: {bera_markets}")
            return
        
        logger.info(f"Found market: {symbol}")
        market_info = markets[symbol]
        logger.info(f"Market info: {market_info}")
        
        # Get balance
        logger.info("Checking balance...")
        try:
            balance = await exchange.fetch_balance()
            logger.info(f"Balance: {balance}")
        except Exception as e:
            logger.warning(f"Balance check failed: {e}")
        
        # Get orderbook
        logger.info("Fetching orderbook...")
        try:
            orderbook = await exchange.fetch_order_book(symbol, 5)
            logger.info(f"Best bid: {orderbook['bids'][0] if orderbook['bids'] else 'None'}")
            logger.info(f"Best ask: {orderbook['asks'][0] if orderbook['asks'] else 'None'}")
        except Exception as e:
            logger.error(f"Orderbook fetch failed: {e}")
            return
        
        # Test order with absolute minimal parameters
        logger.info("Testing order placement with minimal parameters...")
        
        # Calculate order size to meet $10 minimum
        current_price = orderbook['asks'][0][0] if orderbook['asks'] else 2.47
        min_quantity = 10.0 / current_price  # $10 minimum / current price
        order_quantity = max(4.1, min_quantity)  # Use at least 4.1 or whatever meets $10 min
        
        logger.info(f"Using order quantity: {order_quantity} BERA at price: {current_price} = ${order_quantity * current_price:.2f}")
        
        test_cases = [
            # Test 1: Completely empty params - should work if we meet minimum
            {},
            # Test 2: Just client order ID - this causes the JSON error
            {'clientOrderId': f'test_{int(asyncio.get_event_loop().time())}'},
        ]
        
        for i, params in enumerate(test_cases):
            try:
                logger.info(f"Test {i+1}: {params}")
                
                order = await exchange.create_order(
                    symbol,
                    'limit',
                    'buy',
                    order_quantity,  # Use calculated quantity for $10+ value
                    current_price - 0.01,  # Slightly below current price
                    params
                )
                
                logger.info(f"✅ Order {i+1} placed successfully: {order}")
                
                # Cancel immediately
                if order.get('id'):
                    await exchange.cancel_order(order['id'], symbol)
                    logger.info(f"✅ Order {i+1} cancelled")
                
                break  # If successful, no need to try other cases
                
            except Exception as e:
                logger.error(f"❌ Test {i+1} failed: {e}")
                logger.error(f"Error type: {type(e)}")
                if hasattr(e, 'response'):
                    logger.error(f"Response: {e.response}")
        
        await exchange.close()
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()

async def main():
    logger.info("Starting simple Hyperliquid test")
    await test_hyperliquid_minimal()
    logger.info("Test completed")

if __name__ == '__main__':
    asyncio.run(main()) 