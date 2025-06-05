#!/usr/bin/env python
"""
Exchange connector test script.

Tests all exchange connectors with different combinations of exchanges and symbols.
"""

import os
import sys
import asyncio
import argparse
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Import project modules
from market_data.orderbook_analyzer import OrderbookAnalyzer
from market_data.market_logger import MarketLogger
from exchanges.connectors.binance_connector import BinanceConnector
from exchanges.connectors.bybit_connector import BybitConnector
from exchanges.connectors.hyperliquid_connector import HyperliquidConnector
from exchanges.connectors.okx_connector import OKXConnector

# Configure logging
import structlog
from structlog import configure, processors, stdlib, threadlocal

# Configure structlog
configure(
    processors=[
        threadlocal.merge_threadlocal_context,
        stdlib.filter_by_level,
        stdlib.add_logger_name,
        stdlib.add_log_level,
        stdlib.PositionalArgumentsFormatter(),
        processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
        processors.StackInfoRenderer(),
        processors.format_exc_info,
        processors.UnicodeDecoder(),
        stdlib.ProcessorFormatter.wrap_for_formatter,
    ],
    context_class=dict,
    logger_factory=stdlib.LoggerFactory(),
    wrapper_class=stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

# Set up logging
logging.basicConfig(
    format="%(message)s",
    stream=sys.stdout,
    level=logging.INFO,
)

# Create logger
logger = structlog.get_logger()


async def test_exchange_combinations(
    symbols: List[str],
    exchanges: List[str],
    duration: int = 30
):
    """
    Test all combinations of exchanges and symbols.
    
    Args:
        symbols: List of symbols to test
        exchanges: List of exchanges to test
        duration: Test duration in seconds
    """
    # Create analyzer
    analyzer = OrderbookAnalyzer()
    await analyzer.start()
    
    # Initialize connectors
    connectors = {}
    
    try:
        # Create connectors for specified exchanges
        if "binance" in exchanges:
            logger.info("Initializing Binance connector")
            connectors["binance"] = BinanceConnector()
            await connectors["binance"].initialize()
            
        if "bybit" in exchanges:
            logger.info("Initializing Bybit connector")
            connectors["bybit"] = BybitConnector()
            await connectors["bybit"].initialize()
            
        if "okx" in exchanges:
            logger.info("Initializing OKX connector")
            connectors["okx"] = OKXConnector()
            await connectors["okx"].initialize()
            
        if "hyperliquid" in exchanges:
            logger.info("Initializing Hyperliquid connector")
            connectors["hyperliquid"] = HyperliquidConnector()
            await connectors["hyperliquid"].initialize()
            
        # Subscribe to orderbooks
        for symbol in symbols:
            for name, connector in connectors.items():
                try:
                    if await connector.is_symbol_supported(symbol):
                        logger.info(f"Subscribing to {name} {symbol} orderbook")
                        await analyzer.subscribe_orderbook(
                            exchange_connector=connector,
                            symbol=symbol,
                            depth=20
                        )
                    else:
                        logger.warning(f"{symbol} not supported on {name}")
                except Exception as e:
                    logger.error(f"Error subscribing to {name} {symbol}: {e}")
                    
        # Wait for specified duration
        logger.info(f"Running test for {duration} seconds")
        for i in range(duration):
            # Print status every 10 seconds
            if i > 0 and i % 10 == 0:
                logger.info(f"Test running for {i} seconds")
                
                # Print websocket status
                for exchange_name in connectors.keys():
                    if exchange_name in analyzer.ws_managers:
                        ws_manager = analyzer.ws_managers[exchange_name]
                        status = ws_manager.get_status()
                        logger.info(f"{exchange_name} WebSocket status: {status['state']}, "
                                   f"messages: {status['message_count']}, "
                                   f"reconnects: {status['reconnect_attempts']}")
                
                # Print market data for each symbol
                for symbol in symbols:
                    summary = analyzer.get_market_summary(symbol)
                    if summary:
                        logger.info(f"Market summary for {symbol}: "
                                   f"exchanges={summary['exchange_count']}, "
                                   f"best_bid={summary['global_best_bid']}, "
                                   f"best_ask={summary['global_best_ask']}")
                        
            await asyncio.sleep(1)
            
        # Print final results
        logger.info("Final results:")
        
        # Print exchange statistics
        logger.info("Exchange statistics:")
        for exchange_name, connector in connectors.items():
            if exchange_name in analyzer.ws_managers:
                ws_manager = analyzer.ws_managers[exchange_name]
                status = ws_manager.get_status()
                logger.info(f"- {exchange_name}: state={status['state']}, "
                           f"messages={status['message_count']}, "
                           f"errors={status['error_count']}, "
                           f"reconnects={status['reconnect_attempts']}")
            else:
                logger.info(f"- {exchange_name}: No WebSocket connection")
                
        # Print market data for each symbol
        logger.info("Market data:")
        for symbol in symbols:
            orderbooks = [ob for key, ob in analyzer.orderbooks.items() if ob.symbol == symbol]
            exchanges_with_data = [ob.exchange for ob in orderbooks]
            
            logger.info(f"- {symbol}: {len(exchanges_with_data)} exchanges with data: {', '.join(exchanges_with_data)}")
            
            # Print market summary if available
            summary = analyzer.get_market_summary(symbol)
            if summary:
                logger.info(f"  Global best bid: {summary['global_best_bid']}")
                logger.info(f"  Global best ask: {summary['global_best_ask']}")
                logger.info(f"  Spread: {summary['global_spread']} ({summary['global_spread_pct']}%)")
                logger.info(f"  Total liquidity: {summary['total_liquidity_base']} {summary['base']}")
                
                # Print per-exchange data
                for exchange, data in summary['exchange_data'].items():
                    logger.info(f"  {exchange}: bid={data['best_bid']}, ask={data['best_ask']}, spread={data['spread_pct']}%")
                    
    except Exception as e:
        logger.error(f"Error during test: {e}")
        
    finally:
        # Clean up
        logger.info("Cleaning up...")
        
        # Stop analyzer
        await analyzer.stop()
        
        # Close all connectors
        for name, connector in connectors.items():
            if hasattr(connector, 'close'):
                await connector.close()
                
        logger.info("Test complete")


async def main():
    """Main function."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Test exchange connectors')
    parser.add_argument('--symbols', type=str, default="BTC/USDT,ETH/USDT",
                        help='Comma-separated list of symbols (default: BTC/USDT,ETH/USDT)')
    parser.add_argument('--exchanges', type=str, default="binance,bybit,okx,hyperliquid",
                        help='Comma-separated list of exchanges (default: binance,bybit,okx,hyperliquid)')
    parser.add_argument('--duration', type=int, default=30,
                        help='Test duration in seconds (default: 30)')
    
    args = parser.parse_args()
    
    # Parse symbols and exchanges
    symbols = [s.strip() for s in args.symbols.split(",")]
    exchanges = [e.strip().lower() for e in args.exchanges.split(",")]
    
    # Print test configuration
    logger.info(f"Testing with symbols: {', '.join(symbols)}")
    logger.info(f"Testing with exchanges: {', '.join(exchanges)}")
    logger.info(f"Test duration: {args.duration} seconds")
    
    # Run test
    await test_exchange_combinations(
        symbols=symbols,
        exchanges=exchanges,
        duration=args.duration
    )


if __name__ == "__main__":
    asyncio.run(main()) 