#!/usr/bin/env python
"""
Comprehensive test script for fixed WebSocket connectors.

Tests:
1. Real CCXT integration for all WebSocket connectors
2. Symbol retrieval and filtering (spot vs perp)
3. Orderbook data fetching
4. Ticker data retrieval
5. Market data validation
6. Orderbook combination logic verification
"""

import asyncio
import sys
import time
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional
from decimal import Decimal
import structlog

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Import WebSocket connectors
from exchanges.connectors.binance_ws_connector import BinanceWSConnector
from exchanges.connectors.bybit_ws_connector import BybitWSConnector
from exchanges.connectors.hyperliquid_ws_connector import HyperliquidWSConnector

# Configure logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
        structlog.dev.ConsoleRenderer()
    ]
)
logger = structlog.get_logger()


class WSConnectorTester:
    """Test WebSocket connectors."""
    
    def __init__(self):
        self.results = {}
        self.connectors = {}
        
        # Test symbols per exchange - need to be exchange-specific
        self.test_symbols = {
            'binance': ['BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'BNB/USDT', 'ADA/USDT'],
            'bybit': ['BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'BNB/USDT', 'ADA/USDT'],
            'hyperliquid': ['BTC/USDC:USDC', 'ETH/USDC:USDC', 'SOL/USDC:USDC', 'ADHD/USDC', 'ANON/USDC']  # Use actual Hyperliquid symbols
        }
        
    async def test_connector(self, name: str, connector_class) -> Dict[str, Any]:
        """Test a single WebSocket connector."""
        result = {
            'name': name,
            'success': False,
            'initialization': False,
            'markets_loaded': False,
            'symbol_counts': {},
            'sample_symbols': {},
            'orderbook_tests': {},
            'ticker_tests': {},
            'trade_tests': {},
            'errors': [],
            'timing': {}
        }
        
        connector = None
        
        try:
            start_time = time.time()
            
            # Test 1: Initialize connector
            logger.info(f"Testing {name} WebSocket connector...")
            connector = connector_class()
            
            init_start = time.time()
            await connector.initialize()
            result['initialization'] = True
            result['timing']['initialization'] = time.time() - init_start
            
            # Test 2: Load markets
            markets_start = time.time()
            markets = await connector.load_markets()
            result['markets_loaded'] = True
            result['timing']['markets'] = time.time() - markets_start
            
            # Test 3: Get symbol counts by type
            all_symbols = await connector.get_available_symbols('all')
            spot_symbols = await connector.get_available_symbols('spot')
            swap_symbols = await connector.get_available_symbols('swap')
            
            result['symbol_counts'] = {
                'all': len(all_symbols),
                'spot': len(spot_symbols),
                'swap': len(swap_symbols)
            }
            
            result['sample_symbols'] = {
                'spot': spot_symbols[:5] if spot_symbols else [],
                'swap': swap_symbols[:5] if swap_symbols else []
            }
            
            logger.info(f"{name}: Found {len(all_symbols)} total symbols "
                       f"(spot: {len(spot_symbols)}, swap: {len(swap_symbols)})")
            
            # Test 4: Test orderbook for available symbols (use exchange-specific symbols)
            orderbook_start = time.time()
            test_symbols_for_exchange = self.test_symbols.get(name, self.test_symbols['binance'])
            
            for symbol in test_symbols_for_exchange:
                if await connector.is_symbol_supported(symbol):
                    try:
                        orderbook = await connector.get_orderbook(symbol, 10)
                        
                        # Validate orderbook structure
                        valid = (
                            orderbook and
                            'bids' in orderbook and
                            'asks' in orderbook and
                            'symbol' in orderbook and
                            'exchange' in orderbook and
                            orderbook['exchange'] == name and
                            len(orderbook['bids']) > 0 and
                            len(orderbook['asks']) > 0
                        )
                        
                        if valid:
                            # Check bid/ask format
                            first_bid = orderbook['bids'][0]
                            first_ask = orderbook['asks'][0]
                            
                            price_valid = (
                                len(first_bid) == 2 and len(first_ask) == 2 and
                                isinstance(first_bid[0], Decimal) and
                                isinstance(first_bid[1], Decimal) and
                                isinstance(first_ask[0], Decimal) and
                                isinstance(first_ask[1], Decimal)
                            )
                            
                            result['orderbook_tests'][symbol] = {
                                'success': price_valid,
                                'bid_price': float(first_bid[0]),
                                'ask_price': float(first_ask[0]),
                                'spread': float(first_ask[0] - first_bid[0])
                            }
                        else:
                            result['orderbook_tests'][symbol] = {'success': False, 'error': 'Invalid structure'}
                            
                    except Exception as e:
                        result['orderbook_tests'][symbol] = {'success': False, 'error': str(e)}
                        
            result['timing']['orderbooks'] = time.time() - orderbook_start
            
            # Test 5: Test ticker for one symbol
            ticker_start = time.time()
            test_symbol = None
            for symbol in test_symbols_for_exchange:
                if await connector.is_symbol_supported(symbol):
                    test_symbol = symbol
                    break
                    
            if test_symbol:
                try:
                    ticker = await connector.get_ticker(test_symbol)
                    
                    # Validate ticker structure
                    valid = (
                        ticker and
                        'symbol' in ticker and
                        'exchange' in ticker and
                        'last' in ticker and
                        'bid' in ticker and
                        'ask' in ticker and
                        ticker['exchange'] == name
                    )
                    
                    result['ticker_tests'][test_symbol] = {
                        'success': valid,
                        'last': float(ticker.get('last', 0)) if valid else None,
                        'bid': float(ticker.get('bid', 0)) if valid else None,
                        'ask': float(ticker.get('ask', 0)) if valid else None
                    }
                    
                except Exception as e:
                    result['ticker_tests'][test_symbol] = {'success': False, 'error': str(e)}
                    
            result['timing']['ticker'] = time.time() - ticker_start
            
            # Test 6: Test trades for one symbol
            if test_symbol:
                trades_start = time.time()
                try:
                    trades = await connector.get_trades(test_symbol, limit=5)
                    
                    # Validate trades structure
                    valid = (
                        isinstance(trades, list) and
                        len(trades) > 0 and
                        all('symbol' in trade and 'exchange' in trade and 
                            'price' in trade and 'amount' in trade 
                            for trade in trades)
                    )
                    
                    result['trade_tests'][test_symbol] = {
                        'success': valid,
                        'count': len(trades) if valid else 0,
                        'sample_trade': trades[0] if valid and trades else None
                    }
                    
                except Exception as e:
                    result['trade_tests'][test_symbol] = {'success': False, 'error': str(e)}
                    
                result['timing']['trades'] = time.time() - trades_start
            
            # Overall success
            result['success'] = (
                result['initialization'] and
                result['markets_loaded'] and
                result['symbol_counts']['all'] > 0 and
                any(test['success'] for test in result['orderbook_tests'].values())
            )
            
            result['timing']['total'] = time.time() - start_time
            
            logger.info(f"{name}: Test completed - {'✅ PASS' if result['success'] else '❌ FAIL'}")
            
        except Exception as e:
            result['errors'].append(f"Connector test failed: {str(e)}")
            logger.error(f"{name}: Test failed: {e}")
            
        finally:
            if connector:
                try:
                    await connector.close()
                except Exception as e:
                    logger.warning(f"Error closing {name} connector: {e}")
                    
        return result
    
    async def test_all_connectors(self) -> Dict[str, Any]:
        """Test all WebSocket connectors."""
        logger.info("Starting WebSocket connector testing...")
        
        # Define connectors to test
        connectors_to_test = [
            ('binance', BinanceWSConnector),
            ('bybit', BybitWSConnector),
            ('hyperliquid', HyperliquidWSConnector)
        ]
        
        # Test each connector
        for name, connector_class in connectors_to_test:
            result = await self.test_connector(name, connector_class)
            self.results[name] = result
            
            # Add delay between tests
            await asyncio.sleep(2)
            
        return self.results
    
    async def test_orderbook_combination(self):
        """Test orderbook combination logic across exchanges."""
        logger.info("Testing orderbook combination logic...")
        
        successful_exchanges = [
            name for name, result in self.results.items() 
            if result['success']
        ]
        
        if len(successful_exchanges) < 2:
            logger.warning("Not enough successful exchanges for combination test")
            return
            
        # Test with different symbols per exchange since they use different quote currencies
        test_symbols = {
            'binance': 'BTC/USDT',
            'bybit': 'BTC/USDT', 
            'hyperliquid': 'BTC/USDC:USDC'  # Hyperliquid uses USDC and perpetuals
        }
        
        orderbooks = {}
        
        for exchange_name in successful_exchanges:
            try:
                if exchange_name == 'binance':
                    connector = BinanceWSConnector()
                elif exchange_name == 'bybit':
                    connector = BybitWSConnector()
                elif exchange_name == 'hyperliquid':
                    connector = HyperliquidWSConnector()
                else:
                    continue
                    
                await connector.initialize()
                
                test_symbol = test_symbols.get(exchange_name)
                if test_symbol and await connector.is_symbol_supported(test_symbol):
                    orderbook = await connector.get_orderbook(test_symbol, 5)
                    
                    # Validate orderbook
                    if (orderbook and 'bids' in orderbook and 'asks' in orderbook and
                        len(orderbook['bids']) > 0 and len(orderbook['asks']) > 0):
                        orderbooks[exchange_name] = orderbook
                        logger.info(f"Got {test_symbol} orderbook from {exchange_name}")
                    
                await connector.close()
                
            except Exception as e:
                logger.error(f"Failed to get orderbook from {exchange_name}: {e}")
                
        # Analyze combined orderbook data
        if len(orderbooks) >= 2:
            logger.info(f"Successfully collected orderbooks from {len(orderbooks)} exchanges")
            
            # Test combination logic for USDT pairs (exclude Hyperliquid for direct comparison due to USDC)
            usdt_orderbooks = {k: v for k, v in orderbooks.items() if k != 'hyperliquid'}
            
            if len(usdt_orderbooks) >= 2:
                logger.info("Analyzing USDT pair orderbooks:")
                
                # Test combination logic for USDT pairs
                all_bids = []
                all_asks = []
                
                for exchange, orderbook in usdt_orderbooks.items():
                    for bid in orderbook['bids']:
                        all_bids.append((exchange, float(bid[0]), float(bid[1])))
                    for ask in orderbook['asks']:
                        all_asks.append((exchange, float(ask[0]), float(ask[1])))
                        
                # Sort bids (highest price first) and asks (lowest price first)
                all_bids.sort(key=lambda x: x[1], reverse=True)
                all_asks.sort(key=lambda x: x[1])
                
                logger.info("Combined USDT orderbook analysis:")
                logger.info(f"Best bid: {all_bids[0][1]:.2f} from {all_bids[0][0]}")
                logger.info(f"Best ask: {all_asks[0][1]:.2f} from {all_asks[0][0]}")
                logger.info(f"Combined spread: {all_asks[0][1] - all_bids[0][1]:.2f}")
                
                # Check for arbitrage opportunities between USDT pairs
                for exchange1, orderbook1 in usdt_orderbooks.items():
                    for exchange2, orderbook2 in usdt_orderbooks.items():
                        if exchange1 >= exchange2:
                            continue
                            
                        best_bid_1 = float(orderbook1['bids'][0][0])
                        best_ask_2 = float(orderbook2['asks'][0][0])
                        
                        if best_bid_1 > best_ask_2:
                            spread = best_bid_1 - best_ask_2
                            spread_pct = (spread / best_ask_2) * 100
                            logger.info(f"Arbitrage opportunity: Buy on {exchange2} at {best_ask_2:.2f}, "
                                      f"sell on {exchange1} at {best_bid_1:.2f} "
                                      f"(spread: {spread:.2f}, {spread_pct:.3f}%)")
            
            # Show all collected orderbooks for comparison
            logger.info("All collected orderbooks:")
            for exchange, orderbook in orderbooks.items():
                symbol = orderbook['symbol']
                best_bid = float(orderbook['bids'][0][0])
                best_ask = float(orderbook['asks'][0][0])
                spread = best_ask - best_bid
                logger.info(f"{exchange}: {symbol} - Bid: {best_bid:.2f}, Ask: {best_ask:.2f}, Spread: {spread:.2f}")
            
        else:
            logger.warning("Not enough orderbooks for combination test")
    
    def generate_report(self) -> str:
        """Generate test report."""
        report = [
            "=" * 80,
            "WEBSOCKET CONNECTOR TEST REPORT",
            "=" * 80,
            f"Generated: {datetime.now().isoformat()}",
            ""
        ]
        
        # Summary
        total_connectors = len(self.results)
        successful = sum(1 for r in self.results.values() if r['success'])
        failed = total_connectors - successful
        
        report.extend([
            "SUMMARY:",
            f"  Total Connectors: {total_connectors}",
            f"  Successful: {successful}",
            f"  Failed: {failed}",
            f"  Success Rate: {(successful/total_connectors)*100:.1f}%",
            ""
        ])
        
        # Detailed results
        report.append("DETAILED RESULTS:")
        report.append("-" * 40)
        
        for name, result in self.results.items():
            status = "✅ PASS" if result['success'] else "❌ FAIL"
            report.extend([
                f"\n{name.upper()} - {status}",
                f"  Initialization: {'✅' if result['initialization'] else '❌'}",
                f"  Markets Loaded: {'✅' if result['markets_loaded'] else '❌'}",
                f"  Total Symbols: {result['symbol_counts'].get('all', 0)}",
                f"  Spot Symbols: {result['symbol_counts'].get('spot', 0)}",
                f"  Swap Symbols: {result['symbol_counts'].get('swap', 0)}",
            ])
            
            # Orderbook test results
            successful_orderbooks = sum(1 for test in result['orderbook_tests'].values() if test.get('success', False))
            total_orderbooks = len(result['orderbook_tests'])
            if total_orderbooks > 0:
                report.append(f"  Orderbook Tests: {successful_orderbooks}/{total_orderbooks} passed")
                
            # Ticker test results  
            if result['ticker_tests']:
                ticker_success = any(test.get('success', False) for test in result['ticker_tests'].values())
                report.append(f"  Ticker Test: {'✅' if ticker_success else '❌'}")
                
            # Trade test results
            if result['trade_tests']:
                trade_success = any(test.get('success', False) for test in result['trade_tests'].values())
                report.append(f"  Trade Test: {'✅' if trade_success else '❌'}")
            
            # Timing
            if result['timing']:
                timing_items = [f"{k}: {v:.2f}s" for k, v in result['timing'].items()]
                report.append(f"  Timing: {', '.join(timing_items)}")
                
            # Errors
            if result['errors']:
                report.append("  Errors:")
                for error in result['errors']:
                    report.append(f"    - {error}")
        
        # Symbol count comparison
        report.extend([
            "",
            "SYMBOL COUNT COMPARISON:",
            "-" * 30
        ])
        
        successful_connectors = [(name, r) for name, r in self.results.items() if r['success']]
        if successful_connectors:
            successful_connectors.sort(key=lambda x: x[1]['symbol_counts']['all'], reverse=True)
            
            report.append(f"{'Exchange':<12} | {'Total':<6} | {'Spot':<6} | {'Swap':<6}")
            report.append("-" * 40)
            
            for name, result in successful_connectors:
                counts = result['symbol_counts']
                report.append(f"{name:<12} | {counts['all']:<6} | {counts['spot']:<6} | {counts['swap']:<6}")
        
        report.extend([
            "",
            "=" * 80
        ])
        
        return "\n".join(report)


async def main():
    """Main test function."""
    tester = WSConnectorTester()
    
    try:
        # Test all connectors
        results = await tester.test_all_connectors()
        
        # Test orderbook combination
        await tester.test_orderbook_combination()
        
        # Generate and display report
        report = tester.generate_report()
        print(report)
        
        # Save report to file
        report_file = Path(__file__).parent / "ws_connector_test_results.txt"
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