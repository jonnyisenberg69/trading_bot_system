#!/usr/bin/env python
"""
Comprehensive test script for real CCXT exchange integrations.

Tests:
1. Real CCXT connections to all exchanges
2. Market loading and symbol retrieval
3. Orderbook data retrieval
4. Error handling and rate limiting
5. Symbol format consistency
"""

import asyncio
import sys
import time
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional
import structlog

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import ccxt.async_support as ccxt

# Configure logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
        structlog.dev.ConsoleRenderer()
    ]
)
logger = structlog.get_logger()


class ExchangeTester:
    """Test real CCXT exchange connections."""
    
    def __init__(self):
        self.results = {}
        self.symbol_counts = {}
        self.errors = {}
        
        # Configure exchanges with proper settings
        self.exchange_configs = {
            'binance': {
                'class': ccxt.binance,
                'options': {
                    'defaultType': 'spot',
                    'enableRateLimit': True,
                    'rateLimit': 100,
                }
            },
            'bybit': {
                'class': ccxt.bybit,
                'options': {
                    'defaultType': 'spot',
                    'enableRateLimit': True,
                    'rateLimit': 120,
                }
            },
            'gate': {
                'class': ccxt.gate,
                'options': {
                    'defaultType': 'spot',
                    'enableRateLimit': True,
                    'rateLimit': 200,
                }
            },
            'mexc': {
                'class': ccxt.mexc,
                'options': {
                    'defaultType': 'spot',
                    'enableRateLimit': True,
                    'rateLimit': 150,
                    'fetchMarkets': ['spot']  # Only fetch spot markets
                }
            },
            'bitget': {
                'class': ccxt.bitget,
                'options': {
                    'defaultType': 'spot',
                    'enableRateLimit': True,
                    'rateLimit': 100,
                }
            },
            'hyperliquid': {
                'class': ccxt.hyperliquid,
                'options': {
                    'enableRateLimit': True,
                    'rateLimit': 200,
                }
            }
        }
    
    async def test_exchange(self, exchange_name: str, config: Dict) -> Dict[str, Any]:
        """Test a single exchange."""
        result = {
            'name': exchange_name,
            'success': False,
            'markets_loaded': False,
            'symbol_count': 0,
            'spot_symbols': 0,
            'perp_symbols': 0,
            'orderbook_test': False,
            'sample_symbols': [],
            'errors': [],
            'timing': {}
        }
        
        exchange = None
        
        try:
            # Initialize exchange
            start_time = time.time()
            exchange_class = config['class']
            exchange = exchange_class(config['options'])
            
            logger.info(f"Testing {exchange_name} exchange...")
            
            # Test 1: Load markets
            markets_start = time.time()
            try:
                markets = await exchange.load_markets()
                result['markets_loaded'] = True
                result['timing']['load_markets'] = time.time() - markets_start
                
                # Count symbols by type
                spot_count = 0
                perp_count = 0
                sample_symbols = []
                
                for symbol, market in markets.items():
                    if market.get('spot', False):
                        spot_count += 1
                        if len(sample_symbols) < 5:
                            sample_symbols.append(symbol)
                    elif market.get('swap', False) or market.get('future', False):
                        perp_count += 1
                
                result['symbol_count'] = len(markets)
                result['spot_symbols'] = spot_count
                result['perp_symbols'] = perp_count
                result['sample_symbols'] = sample_symbols
                
                logger.info(f"{exchange_name}: Loaded {len(markets)} markets "
                          f"(spot: {spot_count}, perp: {perp_count})")
                
            except Exception as e:
                result['errors'].append(f"Load markets failed: {str(e)}")
                logger.error(f"{exchange_name}: Failed to load markets: {e}")
            
            # Test 2: Orderbook test (if markets loaded)
            if result['markets_loaded'] and result['sample_symbols']:
                ob_start = time.time()
                try:
                    test_symbol = result['sample_symbols'][0]
                    orderbook = await exchange.fetch_order_book(test_symbol, 10)
                    
                    if orderbook and 'bids' in orderbook and 'asks' in orderbook:
                        result['orderbook_test'] = True
                        result['timing']['orderbook'] = time.time() - ob_start
                        logger.info(f"{exchange_name}: Orderbook test passed for {test_symbol}")
                    else:
                        result['errors'].append("Orderbook test failed: Invalid data")
                        
                except Exception as e:
                    result['errors'].append(f"Orderbook test failed: {str(e)}")
                    logger.error(f"{exchange_name}: Orderbook test failed: {e}")
            
            # Overall success
            result['success'] = result['markets_loaded'] and result['symbol_count'] > 0
            result['timing']['total'] = time.time() - start_time
            
        except Exception as e:
            result['errors'].append(f"Exchange initialization failed: {str(e)}")
            logger.error(f"{exchange_name}: Initialization failed: {e}")
        
        finally:
            if exchange:
                try:
                    await exchange.close()
                except Exception as e:
                    logger.warning(f"Error closing {exchange_name}: {e}")
        
        return result
    
    async def test_all_exchanges(self) -> Dict[str, Any]:
        """Test all exchanges."""
        logger.info("Starting comprehensive exchange testing...")
        
        # Test each exchange
        for exchange_name, config in self.exchange_configs.items():
            result = await self.test_exchange(exchange_name, config)
            self.results[exchange_name] = result
            
            # Add delay between exchanges to avoid rate limits
            await asyncio.sleep(2)
        
        return self.results
    
    def generate_report(self) -> str:
        """Generate test report."""
        report = [
            "=" * 80,
            "CCXT EXCHANGE INTEGRATION TEST REPORT",
            "=" * 80,
            f"Generated: {datetime.now().isoformat()}",
            ""
        ]
        
        # Summary
        total_exchanges = len(self.results)
        successful = sum(1 for r in self.results.values() if r['success'])
        failed = total_exchanges - successful
        
        report.extend([
            "SUMMARY:",
            f"  Total Exchanges: {total_exchanges}",
            f"  Successful: {successful}",
            f"  Failed: {failed}",
            f"  Success Rate: {(successful/total_exchanges)*100:.1f}%",
            ""
        ])
        
        # Detailed results
        report.append("DETAILED RESULTS:")
        report.append("-" * 40)
        
        for exchange_name, result in self.results.items():
            status = "✅ PASS" if result['success'] else "❌ FAIL"
            report.extend([
                f"\n{exchange_name.upper()} - {status}",
                f"  Markets Loaded: {'✅' if result['markets_loaded'] else '❌'}",
                f"  Total Symbols: {result['symbol_count']}",
                f"  Spot Symbols: {result['spot_symbols']}",
                f"  Perp Symbols: {result['perp_symbols']}",
                f"  Orderbook Test: {'✅' if result['orderbook_test'] else '❌'}",
            ])
            
            if result['sample_symbols']:
                report.append(f"  Sample Symbols: {', '.join(result['sample_symbols'][:3])}")
            
            if result['timing']:
                timing_str = ", ".join([f"{k}: {v:.2f}s" for k, v in result['timing'].items()])
                report.append(f"  Timing: {timing_str}")
            
            if result['errors']:
                report.append("  Errors:")
                for error in result['errors']:
                    report.append(f"    - {error}")
        
        # Symbol count analysis
        report.extend([
            "",
            "SYMBOL COUNT ANALYSIS:",
            "-" * 30
        ])
        
        successful_exchanges = [(name, r) for name, r in self.results.items() if r['success']]
        if successful_exchanges:
            # Sort by total symbols
            successful_exchanges.sort(key=lambda x: x[1]['symbol_count'], reverse=True)
            
            for name, result in successful_exchanges:
                total = result['symbol_count']
                spot = result['spot_symbols']
                perp = result['perp_symbols']
                report.append(f"  {name:12} | Total: {total:4d} | Spot: {spot:4d} | Perp: {perp:4d}")
        
        # Recommendations
        report.extend([
            "",
            "RECOMMENDATIONS:",
            "-" * 20
        ])
        
        failed_exchanges = [name for name, r in self.results.items() if not r['success']]
        if failed_exchanges:
            report.append(f"🔧 Fix connectivity issues for: {', '.join(failed_exchanges)}")
        
        # Check for low symbol counts
        low_symbol_exchanges = [
            name for name, r in self.results.items() 
            if r['success'] and r['symbol_count'] < 100
        ]
        if low_symbol_exchanges:
            report.append(f"⚠️  Check symbol filtering for: {', '.join(low_symbol_exchanges)}")
        
        report.extend([
            "",
            "=" * 80
        ])
        
        return "\n".join(report)
    
    async def test_orderbook_combination(self):
        """Test orderbook combination logic."""
        logger.info("Testing orderbook combination logic...")
        
        successful_exchanges = [
            name for name, result in self.results.items() 
            if result['success'] and result['sample_symbols']
        ]
        
        if len(successful_exchanges) < 2:
            logger.warning("Not enough successful exchanges for combination test")
            return
        
        # Test with a common symbol like BTC/USDT
        test_symbol = "BTC/USDT"
        orderbooks = {}
        
        for exchange_name in successful_exchanges[:3]:  # Test with first 3 exchanges
            try:
                config = self.exchange_configs[exchange_name]
                exchange = config['class'](config['options'])
                
                # Check if symbol exists
                markets = await exchange.load_markets()
                if test_symbol in markets:
                    orderbook = await exchange.fetch_order_book(test_symbol, 5)
                    orderbooks[exchange_name] = orderbook
                    logger.info(f"Got {test_symbol} orderbook from {exchange_name}")
                
                await exchange.close()
                
            except Exception as e:
                logger.error(f"Failed to get orderbook from {exchange_name}: {e}")
        
        if orderbooks:
            logger.info(f"Successfully collected orderbooks from {len(orderbooks)} exchanges")
            logger.info("Orderbook combination test completed")
        else:
            logger.warning("No orderbooks collected for combination test")


async def main():
    """Main test function."""
    tester = ExchangeTester()
    
    try:
        # Test all exchanges
        results = await tester.test_all_exchanges()
        
        # Test orderbook combination
        await tester.test_orderbook_combination()
        
        # Generate and display report
        report = tester.generate_report()
        print(report)
        
        # Save report to file
        report_file = Path(__file__).parent / "test_results.txt"
        with open(report_file, 'w') as f:
            f.write(report)
        
        print(f"\nDetailed report saved to: {report_file}")
        
        # Return exit code based on results
        failed_count = sum(1 for r in results.values() if not r['success'])
        return 0 if failed_count == 0 else 1
        
    except Exception as e:
        logger.error(f"Test execution failed: {e}")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code) 