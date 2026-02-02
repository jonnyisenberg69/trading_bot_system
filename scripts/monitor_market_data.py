#!/usr/bin/env python
"""
Real-time market data monitoring script.

Connects to multiple exchanges, fetches orderbook data, and logs market metrics.
"""

import os
import sys
import asyncio
import signal
import json
import argparse
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Any, Set

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Import project modules
from market_data.orderbook_analyzer import OrderbookAnalyzer
from market_data.market_logger import MarketLogger
from exchanges.connectors.binance_ws_connector import BinanceWSConnector
from exchanges.connectors.bybit_ws_connector import BybitWSConnector
from exchanges.connectors.hyperliquid_ws_connector import HyperliquidWSConnector

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


class MarketDataMonitor:
    """
    Real-time market data monitor.
    
    Connects to multiple exchanges and monitors orderbook data.
    """
    
    def __init__(
        self,
        symbols: List[str],
        exchanges: List[str] = None,
        log_dir: str = "logs/market_data",
        console_log_interval: float = 5.0,
        orderbook_depth: int = 20
    ):
        """
        Initialize market data monitor.
        
        Args:
            symbols: List of symbols to monitor (e.g., ['BTC/USDT', 'ETH/USDT'])
            exchanges: List of exchanges to monitor (e.g., ['binance', 'bybit'])
            log_dir: Directory for log files
            console_log_interval: Interval for console logging in seconds
            orderbook_depth: Depth of orderbook to fetch
        """
        self.symbols = symbols
        self.exchanges = exchanges or ["binance", "bybit", "hyperliquid"]
        self.log_dir = log_dir
        self.console_log_interval = console_log_interval
        self.orderbook_depth = orderbook_depth
        
        # Create analyzer and logger
        self.analyzer = OrderbookAnalyzer()
        self.market_logger = MarketLogger(
            log_dir=log_dir,
            console_log_interval=console_log_interval
        )
        
        # Register analyzer with logger
        self.market_logger.register_analyzer(self.analyzer)
        
        # Exchange connectors
        self.connectors = {}
        
        # Running state
        self.running = False
        self.stop_event = asyncio.Event()
        
        # Statistics
        self.connection_time = None
        self.messages_received = 0
        
    async def setup_exchanges(self) -> None:
        """Set up exchange connectors."""
        logger.info("Setting up exchange connectors")
        
        # Initialize connectors
        if "binance" in self.exchanges:
            self.connectors["binance"] = BinanceWSConnector()
            
        if "bybit" in self.exchanges:
            self.connectors["bybit"] = BybitWSConnector()
            
        if "hyperliquid" in self.exchanges:
            self.connectors["hyperliquid"] = HyperliquidWSConnector()
            
        # Initialize each connector
        for name, connector in self.connectors.items():
            try:
                await connector.initialize()
                logger.info(f"Initialized {name} connector")
            except Exception as e:
                logger.error(f"Error initializing {name} connector: {e}")
                
    async def start(self) -> None:
        """Start market data monitor."""
        if self.running:
            return
            
        self.running = True
        self.connection_time = datetime.now(timezone.utc)
        
        logger.info("Starting market data monitor")
        
        # Set up exchanges
        await self.setup_exchanges()
        
        # Start analyzer and logger
        await self.analyzer.start()
        await self.market_logger.start()
        
        # Subscribe to orderbooks for each symbol and exchange
        for symbol in self.symbols:
            for name, connector in self.connectors.items():
                try:
                    # Check if symbol is supported by this exchange
                    if not await connector.is_symbol_supported(symbol):
                        logger.warning(f"{symbol} not supported on {name}, skipping")
                        continue
                        
                    # Subscribe to orderbook
                    await self.analyzer.subscribe_orderbook(
                        exchange_connector=connector,
                        symbol=symbol,
                        depth=self.orderbook_depth
                    )
                    
                    logger.info(f"Subscribed to {name} {symbol} orderbook")
                    
                except Exception as e:
                    logger.error(f"Error subscribing to {name} {symbol}: {e}")
                    
        # Wait for stop event
        try:
            await self.stop_event.wait()
        except asyncio.CancelledError:
            pass
            
        # Stop logger and analyzer
        await self.market_logger.stop()
        await self.analyzer.stop()
        
        self.running = False
        logger.info("Market data monitor stopped")
        
    async def stop(self) -> None:
        """Stop market data monitor."""
        if not self.running:
            return
            
        logger.info("Stopping market data monitor")
        self.stop_event.set()
        
    def print_status(self) -> None:
        """Print current status."""
        if not self.running or not self.connection_time:
            print("Market data monitor not running")
            return
            
        uptime = datetime.now(timezone.utc) - self.connection_time
        uptime_str = str(uptime).split('.')[0]  # Remove microseconds
        
        print("\n===== Market Data Monitor Status =====")
        print(f"Uptime: {uptime_str}")
        print(f"Monitoring {len(self.symbols)} symbols on {len(self.connectors)} exchanges")
        print(f"Symbols: {', '.join(self.symbols)}")
        print(f"Exchanges: {', '.join(self.connectors.keys())}")
        
        # Print WebSocket status
        print("\nWebSocket Status:")
        print(f"{'Exchange':<12} {'State':<12} {'Messages':<10} {'Reconnects':<10} {'Errors':<8}")
        print("-" * 60)
        
        for exchange_name, connector in self.connectors.items():
            # Get all WebSocket managers for this exchange from analyzer
            if exchange_name not in self.analyzer.ws_managers:
                continue
                
            ws_manager = self.analyzer.ws_managers[exchange_name]
            status = ws_manager.get_status()
            
            print(f"{exchange_name:<12} {status['state']:<12} {status['message_count']:<10} "
                  f"{status['reconnect_attempts']:<10} {status['error_count']:<8}")
                  
        print("=" * 60)


async def main():
    """Main function."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Monitor real-time market data')
    parser.add_argument('--symbols', type=str, default="BTC/USDT,ETH/USDT",
                        help='Comma-separated list of symbols (default: BTC/USDT,ETH/USDT)')
    parser.add_argument('--exchanges', type=str, default="binance,bybit,hyperliquid",
                        help='Comma-separated list of exchanges (default: binance,bybit,hyperliquid)')
    parser.add_argument('--log-dir', type=str, default="logs/market_data",
                        help='Directory for log files (default: logs/market_data)')
    parser.add_argument('--log-interval', type=float, default=5.0,
                        help='Interval for console logging in seconds (default: 5.0)')
    parser.add_argument('--depth', type=int, default=20,
                        help='Depth of orderbook to fetch (default: 20)')
    
    args = parser.parse_args()
    
    # Parse symbols and exchanges
    symbols = [s.strip() for s in args.symbols.split(",")]
    exchanges = [e.strip().lower() for e in args.exchanges.split(",")]
    
    # Create monitor
    monitor = MarketDataMonitor(
        symbols=symbols,
        exchanges=exchanges,
        log_dir=args.log_dir,
        console_log_interval=args.log_interval,
        orderbook_depth=args.depth
    )
    
    # Handle signals
    loop = asyncio.get_event_loop()
    
    def signal_handler():
        logger.info("Received stop signal, shutting down...")
        asyncio.create_task(monitor.stop())
        
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)
        
    # Start monitor
    try:
        await monitor.start()
    except Exception as e:
        logger.error(f"Error in market data monitor: {e}")
        
    # Print final status
    monitor.print_status()


if __name__ == "__main__":
    asyncio.run(main()) 