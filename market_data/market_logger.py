"""
Market data logging service.

Logs real-time market data from exchanges to both files and console.
"""

import asyncio
import time
import json
import os
from typing import Dict, List, Optional, Set, Any
from datetime import datetime, timedelta, timezone
import structlog
from pathlib import Path
import pandas as pd
from tabulate import tabulate

from market_data.orderbook_analyzer import OrderbookData, OrderbookAnalyzer

logger = structlog.get_logger(__name__)


class MarketLogger:
    """
    Market data logging service.
    
    Logs real-time market data from exchanges to:
    - Console (prettified tables)
    - JSON log files (one per exchange/symbol)
    - CSV files for later analysis
    """
    
    def __init__(
        self,
        log_dir: str = "logs/market_data",
        console_log_interval: float = 5.0,
        file_log_interval: float = 1.0,
        csv_log_interval: float = 60.0,
        max_log_files: int = 100,
        max_log_age: int = 7  # days
    ):
        """
        Initialize market logger.
        
        Args:
            log_dir: Directory for log files
            console_log_interval: Interval for console logging in seconds
            file_log_interval: Interval for file logging in seconds
            csv_log_interval: Interval for CSV logging in seconds
            max_log_files: Maximum number of log files to keep
            max_log_age: Maximum age of log files in days
        """
        self.log_dir = Path(log_dir)
        self.console_log_interval = console_log_interval
        self.file_log_interval = file_log_interval
        self.csv_log_interval = csv_log_interval
        self.max_log_files = max_log_files
        self.max_log_age = max_log_age
        
        # Create log directory if it doesn't exist
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        # Create subdirectories
        self.json_log_dir = self.log_dir / "json"
        self.csv_log_dir = self.log_dir / "csv"
        self.json_log_dir.mkdir(exist_ok=True)
        self.csv_log_dir.mkdir(exist_ok=True)
        
        # Analyzer reference
        self.analyzer = None
        
        # State
        self.running = False
        self.monitored_symbols = set()
        self.last_console_log = {}
        self.last_file_log = {}
        self.last_csv_log = {}
        
        # Tasks
        self.console_log_task = None
        self.file_log_task = None
        self.csv_log_task = None
        self.cleanup_task = None
        
        # Cached data for logging
        self.orderbook_cache = {}  # exchange_symbol -> OrderbookData
        
        self.logger = logger.bind(component="MarketLogger")
        
    def register_analyzer(self, analyzer: OrderbookAnalyzer) -> None:
        """
        Register orderbook analyzer.
        
        Args:
            analyzer: Orderbook analyzer
        """
        self.analyzer = analyzer
        
        # Register callback for orderbook updates
        self.analyzer.register_orderbook_callback(self._on_orderbook_update)
        
    def _on_orderbook_update(self, orderbook: OrderbookData) -> None:
        """
        Handle orderbook update.
        
        Args:
            orderbook: Updated orderbook data
        """
        # Add symbol to monitored symbols
        self.monitored_symbols.add(orderbook.symbol)
        
        # Update orderbook cache
        key = f"{orderbook.exchange}_{orderbook.symbol}"
        self.orderbook_cache[key] = orderbook
        
    async def start(self) -> None:
        """Start market logger."""
        if self.running:
            return
            
        if not self.analyzer:
            self.logger.error("No analyzer registered, cannot start")
            return
            
        self.running = True
        self.logger.info("Starting market logger")
        
        # Start logging tasks
        self.console_log_task = asyncio.create_task(self._console_log_loop())
        self.file_log_task = asyncio.create_task(self._file_log_loop())
        self.csv_log_task = asyncio.create_task(self._csv_log_loop())
        self.cleanup_task = asyncio.create_task(self._cleanup_loop())
        
    async def stop(self) -> None:
        """Stop market logger."""
        if not self.running:
            return
            
        self.running = False
        self.logger.info("Stopping market logger")
        
        # Cancel all tasks
        for task in [self.console_log_task, self.file_log_task, self.csv_log_task, self.cleanup_task]:
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                    
    async def _console_log_loop(self) -> None:
        """Log market data to console periodically."""
        try:
            while self.running:
                now = time.time()
                
                # Check each symbol
                for symbol in self.monitored_symbols:
                    # Skip if logged recently
                    if symbol in self.last_console_log:
                        if now - self.last_console_log[symbol] < self.console_log_interval:
                            continue
                            
                    # Get market summary
                    summary = self.analyzer.get_market_summary(symbol)
                    if not summary:
                        continue
                        
                    # Log to console
                    self._log_market_summary_to_console(summary)
                    
                    # Update last log time
                    self.last_console_log[symbol] = now
                    
                # Sleep
                await asyncio.sleep(1.0)
                
        except asyncio.CancelledError:
            # Normal cancellation
            pass
            
        except Exception as e:
            self.logger.error(f"Error in console log loop: {e}")
            
    def _log_market_summary_to_console(self, summary: Dict[str, Any]) -> None:
        """
        Log market summary to console.
        
        Args:
            summary: Market summary dictionary
        """
        symbol = summary['symbol']
        base, quote = symbol.split('/')
        
        # Print header
        print(f"\n===== Market Summary for {symbol} =====")
        print(f"Time: {summary['datetime']}")
        print(f"Exchanges: {', '.join(summary['exchanges'])}")
        
        # Print global metrics
        print(f"\nGlobal Market Metrics:")
        print(f"Best Bid: {summary['global_best_bid']:.8f} {quote}")
        print(f"Best Ask: {summary['global_best_ask']:.8f} {quote}")
        print(f"Mid Price: {summary['global_midprice']:.8f} {quote}")
        print(f"Spread: {summary['global_spread']:.8f} {quote} ({summary['global_spread_pct']:.4f}%)")
        
        # Print liquidity
        print(f"\nTotal Liquidity (1% depth):")
        print(f"  Bid: {summary['total_bid_liquidity']:.6f} {base} ({summary['total_bid_liquidity_quote']:.2f} {quote})")
        print(f"  Ask: {summary['total_ask_liquidity']:.6f} {base} ({summary['total_ask_liquidity_quote']:.2f} {quote})")
        print(f"  Total: {summary['total_liquidity_base']:.6f} {base} ({summary['total_liquidity_quote']:.2f} {quote})")
        
        # Print exchange comparison table
        print("\nExchange Comparison:")
        table = []
        headers = ["Exchange", "Bid", "Ask", "Spread", "Spread %"]
        
        for exchange, data in summary['exchange_data'].items():
            if data['best_bid'] and data['best_ask']:
                row = [
                    exchange,
                    f"{data['best_bid']:.8f}",
                    f"{data['best_ask']:.8f}",
                    f"{data['spread']:.8f}",
                    f"{data['spread_pct']:.4f}%"
                ]
                table.append(row)
                
        print(tabulate(table, headers=headers, tablefmt="pretty"))
        print("=" * 60)
        
    async def _file_log_loop(self) -> None:
        """Log market data to files periodically."""
        try:
            while self.running:
                now = time.time()
                
                # Check each symbol
                for symbol in self.monitored_symbols:
                    # Skip if logged recently
                    if symbol in self.last_file_log:
                        if now - self.last_file_log[symbol] < self.file_log_interval:
                            continue
                            
                    # Log each exchange's orderbook
                    for key, orderbook in self.orderbook_cache.items():
                        if orderbook.symbol == symbol:
                            # Log to file
                            self._log_orderbook_to_file(orderbook)
                            
                    # Get and log market summary
                    summary = self.analyzer.get_market_summary(symbol)
                    if summary:
                        self._log_market_summary_to_file(summary)
                        
                    # Update last log time
                    self.last_file_log[symbol] = now
                    
                # Sleep
                await asyncio.sleep(0.5)
                
        except asyncio.CancelledError:
            # Normal cancellation
            pass
            
        except Exception as e:
            self.logger.error(f"Error in file log loop: {e}")
            
    def _log_orderbook_to_file(self, orderbook: OrderbookData) -> None:
        """
        Log orderbook to JSON file.
        
        Args:
            orderbook: Orderbook data
        """
        # Convert to dictionary
        data = orderbook.to_dict()
        
        # Create filename
        date_str = datetime.fromtimestamp(orderbook.timestamp).strftime("%Y-%m-%d")
        filename = f"{orderbook.exchange}_{orderbook.symbol.replace('/', '_')}_{date_str}.jsonl"
        filepath = self.json_log_dir / filename
        
        # Write to file
        try:
            with open(filepath, "a") as f:
                f.write(json.dumps(data) + "\n")
        except Exception as e:
            self.logger.error(f"Error writing to file {filepath}: {e}")
            
    def _log_market_summary_to_file(self, summary: Dict[str, Any]) -> None:
        """
        Log market summary to JSON file.
        
        Args:
            summary: Market summary
        """
        # Create filename
        date_str = datetime.fromisoformat(summary['datetime']).strftime("%Y-%m-%d")
        symbol_str = summary['symbol'].replace('/', '_')
        filename = f"market_summary_{symbol_str}_{date_str}.jsonl"
        filepath = self.json_log_dir / filename
        
        # Write to file
        try:
            with open(filepath, "a") as f:
                f.write(json.dumps(summary) + "\n")
        except Exception as e:
            self.logger.error(f"Error writing to file {filepath}: {e}")
            
    async def _csv_log_loop(self) -> None:
        """Log market data to CSV files periodically."""
        try:
            while self.running:
                now = time.time()
                
                # Check each symbol
                for symbol in self.monitored_symbols:
                    # Skip if logged recently
                    if symbol in self.last_csv_log:
                        if now - self.last_csv_log[symbol] < self.csv_log_interval:
                            continue
                            
                    # Get market summary
                    summary = self.analyzer.get_market_summary(symbol)
                    if summary:
                        # Log to CSV
                        self._log_market_summary_to_csv(summary)
                        
                    # Update last log time
                    self.last_csv_log[symbol] = now
                    
                # Sleep
                await asyncio.sleep(5.0)
                
        except asyncio.CancelledError:
            # Normal cancellation
            pass
            
        except Exception as e:
            self.logger.error(f"Error in CSV log loop: {e}")
            
    def _log_market_summary_to_csv(self, summary: Dict[str, Any]) -> None:
        """
        Log market summary to CSV file.
        
        Args:
            summary: Market summary
        """
        # Create dataframe
        df = pd.DataFrame([{
            'timestamp': summary['timestamp'],
            'datetime': summary['datetime'],
            'symbol': summary['symbol'],
            'exchanges': len(summary['exchanges']),
            'best_bid': summary['global_best_bid'],
            'best_ask': summary['global_best_ask'],
            'midprice': summary['global_midprice'],
            'spread': summary['global_spread'],
            'spread_pct': summary['global_spread_pct'],
            'bid_liquidity': summary['total_bid_liquidity'],
            'ask_liquidity': summary['total_ask_liquidity'],
            'total_liquidity': summary['total_liquidity_base']
        }])
        
        # Create filename
        date_str = datetime.fromisoformat(summary['datetime']).strftime("%Y-%m-%d")
        symbol_str = summary['symbol'].replace('/', '_')
        filename = f"market_data_{symbol_str}_{date_str}.csv"
        filepath = self.csv_log_dir / filename
        
        # Check if file exists
        file_exists = filepath.exists()
        
        # Write to file
        try:
            if file_exists:
                df.to_csv(filepath, mode='a', header=False, index=False)
            else:
                df.to_csv(filepath, index=False)
        except Exception as e:
            self.logger.error(f"Error writing to file {filepath}: {e}")
            
    async def _cleanup_loop(self) -> None:
        """Clean up old log files periodically."""
        try:
            while self.running:
                # Clean up old files
                self._cleanup_old_files()
                
                # Sleep for a day
                await asyncio.sleep(86400)  # 24 hours
                
        except asyncio.CancelledError:
            # Normal cancellation
            pass
            
        except Exception as e:
            self.logger.error(f"Error in cleanup loop: {e}")
            
    def _cleanup_old_files(self) -> None:
        """Clean up old log files."""
        self.logger.info("Cleaning up old log files")
        
        # Get cutoff time
        cutoff = datetime.now(timezone.utc) - timedelta(days=self.max_log_age)
        
        # Check all files in json log directory
        json_files = list(self.json_log_dir.glob("*.jsonl"))
        if len(json_files) > self.max_log_files:
            # Sort by modification time
            json_files.sort(key=lambda x: x.stat().st_mtime)
            
            # Remove oldest files
            for file in json_files[:-self.max_log_files]:
                try:
                    file.unlink()
                    self.logger.info(f"Deleted old log file: {file}")
                except Exception as e:
                    self.logger.error(f"Error deleting file {file}: {e}")
                    
        # Check all files in csv log directory
        csv_files = list(self.csv_log_dir.glob("*.csv"))
        if len(csv_files) > self.max_log_files:
            # Sort by modification time
            csv_files.sort(key=lambda x: x.stat().st_mtime)
            
            # Remove oldest files
            for file in csv_files[:-self.max_log_files]:
                try:
                    file.unlink()
                    self.logger.info(f"Deleted old log file: {file}")
                except Exception as e:
                    self.logger.error(f"Error deleting file {file}: {e}")
                    
        # Check for files older than cutoff
        all_files = list(self.json_log_dir.glob("*.jsonl")) + list(self.csv_log_dir.glob("*.csv"))
        for file in all_files:
            mtime = datetime.fromtimestamp(file.stat().st_mtime)
            if mtime < cutoff:
                try:
                    file.unlink()
                    self.logger.info(f"Deleted old log file: {file}")
                except Exception as e:
                    self.logger.error(f"Error deleting file {file}: {e}")
        
        self.logger.info("Finished cleaning up old log files") 