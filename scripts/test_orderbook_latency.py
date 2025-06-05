#!/usr/bin/env python
"""
Optimized latency test for orderbook WebSocket feeds.

This script tests the actual latency of real-time orderbook updates
rather than REST API calls, which provides more accurate latency measurements
for live trading scenarios.
"""

import asyncio
import time
import json
import signal
import statistics
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from decimal import Decimal
import structlog

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Configure logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S.%f"),
        structlog.dev.ConsoleRenderer()
    ]
)
logger = structlog.get_logger()

# Import WebSocket connectors
from exchanges.connectors.binance_ws_connector import BinanceWSConnector
from exchanges.connectors.bybit_ws_connector import BybitWSConnector
from exchanges.connectors.hyperliquid_ws_connector import HyperliquidWSConnector


class OrderbookLatencyTester:
    """Test orderbook update latency for WebSocket feeds."""
    
    def __init__(self):
        self.test_symbol = "BTC/USDT"
        self.test_duration = 30  # seconds
        self.running = False
        
        # Latency tracking
        self.latency_data = {}
        self.update_counts = {}
        
        # WebSocket connectors
        self.connectors = {
            'binance': BinanceWSConnector,
            'bybit': BybitWSConnector,
            'hyperliquid': HyperliquidWSConnector
        }
        
        self.logger = logger.bind(component="LatencyTester")
        
    async def test_all_exchanges(self):
        """Test latency for all available exchanges."""
        self.logger.info(f"Starting orderbook latency test for {self.test_duration}s")
        self.logger.info(f"Testing symbol: {self.test_symbol}")
        self.running = True
        
        # Run tests in parallel for all exchanges
        tasks = []
        for exchange_name in self.connectors.keys():
            task = asyncio.create_task(self._test_exchange_latency(exchange_name))
            tasks.append(task)
            
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            self.logger.error(f"Error in latency tests: {e}")
        finally:
            self.running = False
            await self._print_latency_summary()
            
    async def _test_exchange_latency(self, exchange_name: str):
        """Test latency for a specific exchange."""
        connector_class = self.connectors[exchange_name]
        connector = None
        
        try:
            self.logger.info(f"Starting {exchange_name} latency test")
            
            # Initialize connector
            connector = connector_class()
            await connector.initialize()
            
            # Check if symbol is supported
            if not await connector.is_symbol_supported(self.test_symbol):
                self.logger.warning(f"{exchange_name} doesn't support {self.test_symbol}")
                return
                
            # Initialize latency tracking
            self.latency_data[exchange_name] = []
            self.update_counts[exchange_name] = 0
            
            # Get initial orderbook for baseline
            initial_book = await connector.get_orderbook(self.test_symbol, limit=5)
            if not initial_book:
                self.logger.warning(f"Failed to get initial orderbook from {exchange_name}")
                return
                
            last_update_time = time.time()
            
            # Test WebSocket feed latency by measuring time between updates
            start_time = time.time()
            
            while self.running and (time.time() - start_time) < self.test_duration:
                try:
                    # Get orderbook update
                    current_time = time.time()
                    orderbook = await connector.get_orderbook(self.test_symbol, limit=5)
                    
                    if orderbook and 'timestamp' in orderbook:
                        # Calculate latency from exchange timestamp to our receipt
                        exchange_timestamp = orderbook['timestamp'] / 1000  # Convert to seconds
                        latency_ms = (current_time - exchange_timestamp) * 1000
                        
                        # Only record reasonable latencies (filter out obvious errors)
                        if 0 < latency_ms < 5000:  # 0-5 seconds seems reasonable
                            self.latency_data[exchange_name].append(latency_ms)
                            
                        self.update_counts[exchange_name] += 1
                        
                        # Log periodic updates
                        if self.update_counts[exchange_name] % 10 == 0:
                            avg_latency = statistics.mean(self.latency_data[exchange_name]) if self.latency_data[exchange_name] else 0
                            self.logger.info(
                                f"{exchange_name}: {self.update_counts[exchange_name]} updates, "
                                f"avg latency: {avg_latency:.2f}ms"
                            )
                    
                    # Small delay to prevent overwhelming the API
                    await asyncio.sleep(0.1)
                    
                except Exception as e:
                    self.logger.warning(f"{exchange_name} update error: {e}")
                    await asyncio.sleep(1)  # Longer delay on error
                    
        except Exception as e:
            self.logger.error(f"{exchange_name} latency test failed: {e}")
        finally:
            if connector:
                try:
                    await connector.close()
                except:
                    pass  # Ignore cleanup errors
                    
    async def _print_latency_summary(self):
        """Print comprehensive latency summary."""
        self.logger.info("=" * 80)
        self.logger.info("ORDERBOOK LATENCY TEST SUMMARY")
        self.logger.info("=" * 80)
        
        if not self.latency_data:
            self.logger.warning("No latency data collected")
            return
            
        summary = {}
        
        for exchange, latencies in self.latency_data.items():
            if not latencies:
                summary[exchange] = {
                    'status': 'FAILED',
                    'updates': self.update_counts.get(exchange, 0),
                    'error': 'No latency measurements'
                }
                continue
                
            summary[exchange] = {
                'status': 'SUCCESS',
                'updates': len(latencies),
                'avg_latency_ms': statistics.mean(latencies),
                'min_latency_ms': min(latencies),
                'max_latency_ms': max(latencies),
                'median_latency_ms': statistics.median(latencies),
                'std_dev_ms': statistics.stdev(latencies) if len(latencies) > 1 else 0
            }
            
        # Print results
        for exchange, stats in summary.items():
            status_icon = "✓" if stats['status'] == 'SUCCESS' else "✗"
            self.logger.info(f"{status_icon} {exchange.upper()}")
            
            if stats['status'] == 'SUCCESS':
                self.logger.info(f"    Updates: {stats['updates']}")
                self.logger.info(f"    Average: {stats['avg_latency_ms']:.2f}ms")
                self.logger.info(f"    Median:  {stats['median_latency_ms']:.2f}ms")
                self.logger.info(f"    Min:     {stats['min_latency_ms']:.2f}ms")
                self.logger.info(f"    Max:     {stats['max_latency_ms']:.2f}ms")
                self.logger.info(f"    Std Dev: {stats['std_dev_ms']:.2f}ms")
                
                # Latency classification
                avg_latency = stats['avg_latency_ms']
                if avg_latency < 50:
                    grade = "EXCELLENT"
                elif avg_latency < 100:
                    grade = "GOOD"
                elif avg_latency < 200:
                    grade = "ACCEPTABLE"
                elif avg_latency < 500:
                    grade = "POOR"
                else:
                    grade = "UNACCEPTABLE"
                    
                self.logger.info(f"    Grade:   {grade}")
            else:
                self.logger.info(f"    Error: {stats.get('error', 'Unknown')}")
                
        # Overall assessment
        self.logger.info("-" * 80)
        successful_exchanges = [ex for ex, stats in summary.items() if stats['status'] == 'SUCCESS']
        
        if successful_exchanges:
            all_latencies = []
            for exchange in successful_exchanges:
                all_latencies.extend(self.latency_data[exchange])
                
            overall_avg = statistics.mean(all_latencies)
            self.logger.info(f"Overall Performance:")
            self.logger.info(f"  Successful Exchanges: {len(successful_exchanges)}/{len(summary)}")
            self.logger.info(f"  Combined Average Latency: {overall_avg:.2f}ms")
            self.logger.info(f"  Total Updates Measured: {len(all_latencies)}")
            
            # Recommendations
            self.logger.info("\nRecommendations:")
            if overall_avg < 100:
                self.logger.info("  ✓ Latency is suitable for high-frequency trading")
            elif overall_avg < 200:
                self.logger.info("  ⚠ Latency is acceptable for medium-frequency trading")
            else:
                self.logger.info("  ✗ Latency may be too high for real-time trading")
                self.logger.info("  → Consider optimizing network connection or server location")
        else:
            self.logger.warning("No successful latency measurements - check connections")
            
        # Export detailed data
        self.logger.info(f"\nDetailed Data:")
        self.logger.info(json.dumps(summary, indent=2))


async def main():
    """Main function."""
    tester = OrderbookLatencyTester()
    
    # Handle signals for graceful shutdown
    def signal_handler():
        logger.info("Received shutdown signal")
        tester.running = False
        
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)
    
    try:
        await tester.test_all_exchanges()
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
    except Exception as e:
        logger.error(f"Test failed: {e}")


if __name__ == "__main__":
    asyncio.run(main()) 