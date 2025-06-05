#!/usr/bin/env python
"""
Market data simulation script.

Simulates market data from exchanges for testing WebSocket reconnection logic
and market data logging without connecting to real exchanges.
"""

import os
import sys
import asyncio
import signal
import json
import random
import argparse
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Set, Optional
from decimal import Decimal

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

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


class MockExchangeConnector:
    """
    Mock exchange connector for testing.
    
    Simulates exchange API responses and WebSocket feeds.
    """
    
    def __init__(
        self,
        exchange_name: str,
        simulate_errors: bool = False,
        error_probability: float = 0.05
    ):
        """
        Initialize mock exchange connector.
        
        Args:
            exchange_name: Name of the simulated exchange
            simulate_errors: Whether to simulate errors
            error_probability: Probability of simulating an error
        """
        self.exchange_name = exchange_name
        self.simulate_errors = simulate_errors
        self.error_probability = error_probability
        
        # Generate base prices for symbols
        self.base_prices = {
            'BTC/USDT': Decimal('65000'),
            'ETH/USDT': Decimal('3500'),
            'SOL/USDT': Decimal('150'),
            'BNB/USDT': Decimal('600'),
            'XRP/USDT': Decimal('0.6')
        }
        
        # Generate bid/ask spreads for exchanges (as percentage)
        self.spreads = {
            'binance': Decimal('0.01'),    # 0.01%
            'bybit': Decimal('0.015'),     # 0.015%
            'hyperliquid': Decimal('0.025')  # 0.025%
        }
        
        # Volatility factors for each exchange
        self.volatility = {
            'binance': Decimal('0.0002'),      # 0.02%
            'bybit': Decimal('0.00025'),       # 0.025%
            'hyperliquid': Decimal('0.00035')  # 0.035%
        }
        
        self.logger = logger.bind(exchange=exchange_name)
        self.exchange = self  # For compatibility with real connectors
        
    async def initialize(self) -> None:
        """Initialize the mock connector."""
        self.logger.info(f"Initialized {self.exchange_name} connector (mock)")
        await asyncio.sleep(0.5)  # Simulate initialization delay
        
    async def is_symbol_supported(self, symbol: str) -> bool:
        """
        Check if symbol is supported.
        
        Args:
            symbol: Symbol to check
            
        Returns:
            True if symbol is supported
        """
        return symbol in self.base_prices
        
    async def get_orderbook(self, symbol: str, limit: int = 20) -> Dict[str, Any]:
        """
        Get simulated order book for symbol.
        
        Args:
            symbol: Symbol to get order book for
            limit: Order book depth
            
        Returns:
            Simulated order book dictionary
        """
        # Simulate error
        if self.simulate_errors and random.random() < self.error_probability:
            self.logger.warning(f"Simulating error for {symbol} orderbook")
            raise Exception(f"Simulated error getting {symbol} orderbook")
            
        # Generate simulated orderbook
        orderbook = self._generate_orderbook(symbol, limit)
        await asyncio.sleep(0.1)  # Simulate API delay
        return orderbook
        
    async def watch_order_book(self, symbol: str, limit: int = 20) -> Any:
        """
        Watch order book WebSocket endpoint.
        
        Args:
            symbol: Symbol to watch
            limit: Order book depth
            
        Returns:
            Async generator for orderbook updates
        """
        self.logger.info(f"Watching {symbol} orderbook")
        
        # Setup for allowing WebSocketManager to work with our generator
        class AsyncGeneratorWrapper:
            def __init__(self, generator):
                self.generator = generator
                
            def __aiter__(self):
                return self
                
            async def __anext__(self):
                try:
                    return await self.generator.__anext__()
                except StopAsyncIteration:
                    raise
                
        # Start the generator but return a wrapper
        async def generate_updates():
            # Initial orderbook
            orderbook = self._generate_orderbook(symbol, limit)
            yield orderbook
            
            # Generate updates
            while True:
                # Simulate error or disconnection
                if self.simulate_errors and random.random() < self.error_probability:
                    self.logger.warning(f"Simulating WebSocket error for {symbol} orderbook")
                    raise Exception(f"Simulated WebSocket error for {symbol} orderbook")
                    
                # Wait before next update
                await asyncio.sleep(random.uniform(0.1, 0.5))
                
                # Generate updated orderbook
                orderbook = self._generate_orderbook(symbol, limit)
                yield orderbook
                
        return AsyncGeneratorWrapper(generate_updates())
        
    def _generate_orderbook(self, symbol: str, limit: int = 20) -> Dict[str, Any]:
        """
        Generate simulated orderbook.
        
        Args:
            symbol: Symbol to generate orderbook for
            limit: Order book depth
            
        Returns:
            Simulated orderbook
        """
        if symbol not in self.base_prices:
            raise ValueError(f"Symbol {symbol} not supported")
            
        # Get base price and apply random walk
        base_price = self.base_prices[symbol]
        volatility = self.volatility[self.exchange_name.lower()]
        random_walk = Decimal(str(random.uniform(-1, 1))) * volatility * base_price
        mid_price = base_price + random_walk
        
        # Apply spread
        spread_pct = self.spreads[self.exchange_name.lower()]
        spread = mid_price * spread_pct
        best_bid = mid_price - (spread / Decimal('2'))
        best_ask = mid_price + (spread / Decimal('2'))
        
        # Generate bids (sorted by price descending)
        bids = []
        for i in range(limit):
            price = best_bid * (Decimal('1') - Decimal(str(i * 0.0005)))
            size = Decimal(str(random.uniform(0.1, 2.0)))
            if symbol.startswith('BTC'):
                size = size / Decimal('10')  # Less BTC liquidity
            bids.append([float(price), float(size)])
            
        # Generate asks (sorted by price ascending)
        asks = []
        for i in range(limit):
            price = best_ask * (Decimal('1') + Decimal(str(i * 0.0005)))
            size = Decimal(str(random.uniform(0.1, 2.0)))
            if symbol.startswith('BTC'):
                size = size / Decimal('10')  # Less BTC liquidity
            asks.append([float(price), float(size)])
            
        # Update base price with some drift
        drift = random_walk * Decimal('0.1')
        self.base_prices[symbol] = base_price + drift
        
        return {
            'symbol': symbol,
            'timestamp': int(datetime.now().timestamp() * 1000),
            'datetime': datetime.now().isoformat(),
            'nonce': int(datetime.now().timestamp() * 1000000),
            'bids': bids,
            'asks': asks
        }


async def simulate_market_data(
    symbols: List[str],
    exchanges: List[str],
    simulate_errors: bool = False,
    duration: Optional[int] = None
):
    """
    Simulate market data from exchanges.
    
    Args:
        symbols: List of symbols to simulate
        exchanges: List of exchanges to simulate
        simulate_errors: Whether to simulate errors
        duration: Duration in seconds (None for indefinite)
    """
    from market_data.orderbook_analyzer import OrderbookAnalyzer
    from market_data.market_logger import MarketLogger
    
    # Create analyzer and logger
    analyzer = OrderbookAnalyzer()
    market_logger = MarketLogger(
        log_dir="logs/simulated_market_data",
        console_log_interval=5.0
    )
    
    # Register analyzer with logger
    market_logger.register_analyzer(analyzer)
    
    # Create mock exchange connectors
    connectors = {}
    for exchange_name in exchanges:
        connectors[exchange_name] = MockExchangeConnector(
            exchange_name=exchange_name,
            simulate_errors=simulate_errors
        )
        await connectors[exchange_name].initialize()
        
    # Start analyzer and logger
    await analyzer.start()
    await market_logger.start()
    
    # Subscribe to orderbooks
    for symbol in symbols:
        for name, connector in connectors.items():
            if await connector.is_symbol_supported(symbol):
                await analyzer.subscribe_orderbook(
                    exchange_connector=connector,
                    symbol=symbol,
                    depth=20
                )
                logger.info(f"Subscribed to {name} {symbol} orderbook (simulated)")
                
    # Run for specified duration or indefinitely
    try:
        if duration:
            logger.info(f"Running simulation for {duration} seconds")
            await asyncio.sleep(duration)
        else:
            logger.info("Running simulation indefinitely (press Ctrl+C to stop)")
            # Run until cancelled
            while True:
                await asyncio.sleep(1)
                
    except asyncio.CancelledError:
        logger.info("Simulation cancelled")
        
    finally:
        # Stop logger and analyzer
        await market_logger.stop()
        await analyzer.stop()
        
        # Print final status
        for symbol in symbols:
            summary = analyzer.get_market_summary(symbol)
            if summary:
                print(f"\nFinal market summary for {symbol}:")
                market_logger._log_market_summary_to_console(summary)


async def main():
    """Main function."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Simulate market data from exchanges')
    parser.add_argument('--symbols', type=str, default="BTC/USDT,ETH/USDT",
                      help='Comma-separated list of symbols (default: BTC/USDT,ETH/USDT)')
    parser.add_argument('--exchanges', type=str, default="binance,bybit,hyperliquid",
                      help='Comma-separated list of exchanges (default: binance,bybit,hyperliquid)')
    parser.add_argument('--errors', action='store_true',
                      help='Simulate errors and disconnections')
    parser.add_argument('--duration', type=int, default=None,
                      help='Duration in seconds (default: run indefinitely)')
    
    args = parser.parse_args()
    
    # Parse symbols and exchanges
    symbols = [s.strip() for s in args.symbols.split(",")]
    exchanges = [e.strip().lower() for e in args.exchanges.split(",")]
    
    # Handle signals
    loop = asyncio.get_event_loop()
    
    # Start simulation
    try:
        await simulate_market_data(
            symbols=symbols,
            exchanges=exchanges,
            simulate_errors=args.errors,
            duration=args.duration
        )
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Error in market data simulation: {e}")


if __name__ == "__main__":
    asyncio.run(main()) 