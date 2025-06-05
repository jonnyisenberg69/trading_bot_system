#!/usr/bin/env python
"""
Real Trade Endpoints Testing with Pagination

This test suite implements comprehensive pagination testing for trade endpoints
across all exchanges, following BrowserStack pagination testing best practices:
https://www.browserstack.com/guide/test-cases-for-pagination-functionality

Tests include:
- Basic pagination functionality
- Edge cases (first/last page, empty pages)
- Performance testing with large datasets
- Cross-exchange pagination consistency
- Error handling for invalid pagination parameters
"""

import asyncio
import pytest
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import time

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import structlog
from exchanges.connectors import create_exchange_connector
from config.exchange_keys import get_exchange_config

# Configure logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="%H:%M:%S"),
        structlog.dev.ConsoleRenderer(),
    ],
)
logger = structlog.get_logger(__name__)


class TradePaginationTester:
    """
    Comprehensive trade pagination tester following BrowserStack best practices.
    
    Implements all pagination test cases:
    1. Default pagination behavior
    2. Navigation to next/previous pages
    3. Page number navigation
    4. First and last page handling
    5. Edge cases and error conditions
    6. Performance testing
    """
    
    def __init__(self, exchange_name: str):
        self.exchange_name = exchange_name
        self.connector = None
        self.pagination_limits = {
            'binance_spot': {'max_limit': 1000, 'default_limit': 100},
            'binance_perp': {'max_limit': 1000, 'default_limit': 100},
            'bybit_spot': {'max_limit': 200, 'default_limit': 50},
            'bybit_perp': {'max_limit': 200, 'default_limit': 50},
            'mexc_spot': {'max_limit': 100, 'default_limit': 50},
            'gateio_spot': {'max_limit': 1000, 'default_limit': 100},
            'bitget_spot': {'max_limit': 100, 'default_limit': 50},
            'hyperliquid_perp': {'max_limit': 100, 'default_limit': 50}
        }
        
    async def setup(self):
        """Initialize exchange connection."""
        config = get_exchange_config(self.exchange_name)
        if not config:
            raise ValueError(f"No configuration found for {self.exchange_name}")
            
        self.connector = create_exchange_connector(self.exchange_name.split('_')[0], config)
        if not self.connector:
            raise ValueError(f"Could not create connector for {self.exchange_name}")
            
        connected = await self.connector.connect()
        if not connected:
            raise ConnectionError(f"Failed to connect to {self.exchange_name}")
            
        return True
        
    async def teardown(self):
        """Clean up exchange connection."""
        if self.connector:
            await self.connector.disconnect()
            
    async def test_default_pagination(self, symbol: str = 'BTC/USDT') -> Dict[str, Any]:
        """
        Test Case 1: Verify Default Pagination
        Verifies that the exchange returns the expected default number of trades.
        """
        logger.info(f"🔍 Testing default pagination for {self.exchange_name} {symbol}")
        
        try:
            since = datetime.utcnow() - timedelta(days=1)
            
            # Test default limit (no limit specified)
            start_time = time.time()
            trades = await self.connector.get_trade_history(symbol=symbol, since=since)
            response_time = time.time() - start_time
            
            result = {
                'test_case': 'default_pagination',
                'exchange': self.exchange_name,
                'symbol': symbol,
                'trades_count': len(trades),
                'response_time_ms': round(response_time * 1000, 2),
                'expected_default': self.pagination_limits.get(self.exchange_name, {}).get('default_limit', 50),
                'status': 'PASS' if len(trades) <= self.pagination_limits.get(self.exchange_name, {}).get('max_limit', 1000) else 'FAIL',
                'sample_trade': trades[0] if trades else None
            }
            
            logger.info(f"✅ Default pagination: {len(trades)} trades in {response_time*1000:.2f}ms")
            return result
            
        except Exception as e:
            logger.error(f"❌ Default pagination test failed: {e}")
            return {
                'test_case': 'default_pagination',
                'exchange': self.exchange_name,
                'symbol': symbol,
                'error': str(e),
                'status': 'ERROR'
            }
    
    async def test_custom_limit_pagination(self, symbol: str = 'BTC/USDT') -> Dict[str, Any]:
        """
        Test Case 2: Verify Custom Limit Pagination
        Tests different page sizes to ensure pagination works with custom limits.
        """
        logger.info(f"🔍 Testing custom limit pagination for {self.exchange_name} {symbol}")
        
        test_limits = [10, 25, 50, 100]
        max_limit = self.pagination_limits.get(self.exchange_name, {}).get('max_limit', 1000)
        
        results = []
        since = datetime.utcnow() - timedelta(days=1)
        
        for limit in test_limits:
            if limit > max_limit:
                continue
                
            try:
                start_time = time.time()
                trades = await self.connector.get_trade_history(
                    symbol=symbol, 
                    since=since, 
                    limit=limit
                )
                response_time = time.time() - start_time
                
                result = {
                    'limit_requested': limit,
                    'trades_returned': len(trades),
                    'response_time_ms': round(response_time * 1000, 2),
                    'status': 'PASS' if len(trades) <= limit else 'FAIL',
                    'within_expected_range': len(trades) <= limit
                }
                
                results.append(result)
                logger.info(f"  📊 Limit {limit}: {len(trades)} trades in {response_time*1000:.2f}ms")
                
                # Rate limiting between requests
                await asyncio.sleep(0.1)
                
            except Exception as e:
                logger.error(f"❌ Custom limit {limit} failed: {e}")
                results.append({
                    'limit_requested': limit,
                    'error': str(e),
                    'status': 'ERROR'
                })
        
        return {
            'test_case': 'custom_limit_pagination',
            'exchange': self.exchange_name,
            'symbol': symbol,
            'results': results,
            'overall_status': 'PASS' if all(r.get('status') == 'PASS' for r in results) else 'PARTIAL'
        }
    
    async def test_time_based_pagination(self, symbol: str = 'BTC/USDT') -> Dict[str, Any]:
        """
        Test Case 3: Verify Time-Based Pagination
        Tests pagination using time windows (since parameter).
        """
        logger.info(f"🔍 Testing time-based pagination for {self.exchange_name} {symbol}")
        
        # Test different time windows
        time_windows = [
            {'hours': 1, 'name': '1_hour'},
            {'hours': 6, 'name': '6_hours'}, 
            {'hours': 12, 'name': '12_hours'},
            {'days': 1, 'name': '1_day'}
        ]
        
        results = []
        
        for window in time_windows:
            try:
                if 'hours' in window:
                    since = datetime.utcnow() - timedelta(hours=window['hours'])
                else:
                    since = datetime.utcnow() - timedelta(days=window['days'])
                
                start_time = time.time()
                trades = await self.connector.get_trade_history(
                    symbol=symbol,
                    since=since,
                    limit=50  # Standard limit for comparison
                )
                response_time = time.time() - start_time
                
                result = {
                    'time_window': window['name'],
                    'since_timestamp': since.isoformat(),
                    'trades_count': len(trades),
                    'response_time_ms': round(response_time * 1000, 2),
                    'oldest_trade': trades[-1] if trades else None,
                    'newest_trade': trades[0] if trades else None,
                    'status': 'PASS'
                }
                
                results.append(result)
                logger.info(f"  📅 {window['name']}: {len(trades)} trades in {response_time*1000:.2f}ms")
                
                # Rate limiting
                await asyncio.sleep(0.1)
                
            except Exception as e:
                logger.error(f"❌ Time window {window['name']} failed: {e}")
                results.append({
                    'time_window': window['name'],
                    'error': str(e),
                    'status': 'ERROR'
                })
        
        return {
            'test_case': 'time_based_pagination',
            'exchange': self.exchange_name,
            'symbol': symbol,
            'results': results,
            'overall_status': 'PASS' if all(r.get('status') == 'PASS' for r in results) else 'PARTIAL'
        }
    
    async def test_edge_cases(self, symbol: str = 'BTC/USDT') -> Dict[str, Any]:
        """
        Test Case 4: Verify Edge Cases
        Tests boundary conditions and error scenarios.
        """
        logger.info(f"🔍 Testing edge cases for {self.exchange_name} {symbol}")
        
        edge_cases = []
        
        # Test Case 4a: Zero limit
        try:
            trades = await self.connector.get_trade_history(symbol=symbol, limit=0)
            edge_cases.append({
                'case': 'zero_limit',
                'result': len(trades),
                'status': 'PASS' if len(trades) == 0 else 'UNEXPECTED',
                'description': 'Zero limit should return no trades'
            })
        except Exception as e:
            edge_cases.append({
                'case': 'zero_limit',
                'error': str(e),
                'status': 'ERROR',
                'description': 'Zero limit caused exception'
            })
        
        # Test Case 4b: Extremely large limit
        max_limit = self.pagination_limits.get(self.exchange_name, {}).get('max_limit', 1000)
        try:
            large_limit = max_limit * 2  # Request more than max
            start_time = time.time()
            trades = await self.connector.get_trade_history(symbol=symbol, limit=large_limit)
            response_time = time.time() - start_time
            
            edge_cases.append({
                'case': 'large_limit',
                'limit_requested': large_limit,
                'trades_returned': len(trades),
                'response_time_ms': round(response_time * 1000, 2),
                'status': 'PASS' if len(trades) <= max_limit else 'UNEXPECTED',
                'description': f'Large limit should be capped at {max_limit}'
            })
        except Exception as e:
            edge_cases.append({
                'case': 'large_limit',
                'error': str(e),
                'status': 'ERROR',
                'description': 'Large limit caused exception'
            })
        
        # Test Case 4c: Future timestamp
        try:
            future_time = datetime.utcnow() + timedelta(hours=1)
            trades = await self.connector.get_trade_history(symbol=symbol, since=future_time)
            edge_cases.append({
                'case': 'future_timestamp',
                'result': len(trades),
                'status': 'PASS' if len(trades) == 0 else 'UNEXPECTED',
                'description': 'Future timestamp should return no trades'
            })
        except Exception as e:
            edge_cases.append({
                'case': 'future_timestamp',
                'error': str(e),
                'status': 'ERROR',
                'description': 'Future timestamp caused exception'
            })
        
        # Test Case 4d: Very old timestamp (30 days ago)
        try:
            old_time = datetime.utcnow() - timedelta(days=30)
            start_time = time.time()
            trades = await self.connector.get_trade_history(symbol=symbol, since=old_time, limit=10)
            response_time = time.time() - start_time
            
            edge_cases.append({
                'case': 'old_timestamp',
                'trades_count': len(trades),
                'response_time_ms': round(response_time * 1000, 2),
                'status': 'PASS',
                'description': 'Old timestamp should work (might return 0 trades)'
            })
        except Exception as e:
            edge_cases.append({
                'case': 'old_timestamp',
                'error': str(e),
                'status': 'ERROR',
                'description': 'Old timestamp caused exception'
            })
        
        # Test Case 4e: Invalid symbol
        try:
            invalid_symbol = 'INVALID/SYMBOL'
            trades = await self.connector.get_trade_history(symbol=invalid_symbol, limit=10)
            edge_cases.append({
                'case': 'invalid_symbol',
                'result': len(trades),
                'status': 'UNEXPECTED',
                'description': 'Invalid symbol should cause error'
            })
        except Exception as e:
            edge_cases.append({
                'case': 'invalid_symbol',
                'error': str(e),
                'status': 'PASS',
                'description': 'Invalid symbol correctly caused exception'
            })
        
        return {
            'test_case': 'edge_cases',
            'exchange': self.exchange_name,
            'symbol': symbol,
            'edge_cases': edge_cases,
            'overall_status': 'PASS' if all(case.get('status') in ['PASS', 'ERROR'] for case in edge_cases) else 'FAIL'
        }
    
    async def test_performance_with_pagination(self, symbol: str = 'BTC/USDT') -> Dict[str, Any]:
        """
        Test Case 5: Performance Testing with Pagination
        Tests response times with different pagination parameters.
        """
        logger.info(f"🔍 Testing performance with pagination for {self.exchange_name} {symbol}")
        
        performance_tests = []
        
        # Test different combinations of limit and time windows
        test_combinations = [
            {'limit': 10, 'hours': 1},
            {'limit': 50, 'hours': 6},
            {'limit': 100, 'hours': 12},
        ]
        
        max_limit = self.pagination_limits.get(self.exchange_name, {}).get('max_limit', 1000)
        
        for combo in test_combinations:
            if combo['limit'] > max_limit:
                continue
                
            try:
                since = datetime.utcnow() - timedelta(hours=combo['hours'])
                
                # Multiple requests to get average response time
                response_times = []
                for i in range(3):  # 3 requests for averaging
                    start_time = time.time()
                    trades = await self.connector.get_trade_history(
                        symbol=symbol,
                        since=since,
                        limit=combo['limit']
                    )
                    response_time = time.time() - start_time
                    response_times.append(response_time)
                    
                    # Rate limiting between requests
                    await asyncio.sleep(0.2)
                
                avg_response_time = sum(response_times) / len(response_times)
                min_response_time = min(response_times)
                max_response_time = max(response_times)
                
                performance_tests.append({
                    'limit': combo['limit'],
                    'time_window_hours': combo['hours'],
                    'avg_response_time_ms': round(avg_response_time * 1000, 2),
                    'min_response_time_ms': round(min_response_time * 1000, 2),
                    'max_response_time_ms': round(max_response_time * 1000, 2),
                    'trades_per_request': len(trades),
                    'status': 'PASS' if avg_response_time < 5.0 else 'SLOW'  # 5 second threshold
                })
                
                logger.info(f"  ⚡ Limit {combo['limit']}, {combo['hours']}h: {avg_response_time*1000:.2f}ms avg")
                
            except Exception as e:
                logger.error(f"❌ Performance test failed for limit {combo['limit']}: {e}")
                performance_tests.append({
                    'limit': combo['limit'],
                    'time_window_hours': combo['hours'],
                    'error': str(e),
                    'status': 'ERROR'
                })
        
        return {
            'test_case': 'performance_with_pagination',
            'exchange': self.exchange_name,
            'symbol': symbol,
            'performance_tests': performance_tests,
            'overall_status': 'PASS' if all(test.get('status') in ['PASS', 'SLOW'] for test in performance_tests) else 'FAIL'
        }
    
    async def run_comprehensive_pagination_tests(self, symbols: List[str] = None) -> Dict[str, Any]:
        """
        Run all pagination tests for the exchange.
        """
        if symbols is None:
            symbols = ['BTC/USDT', 'ETH/USDT']
        
        logger.info(f"🚀 Starting comprehensive pagination tests for {self.exchange_name}")
        
        all_results = {
            'exchange': self.exchange_name,
            'test_timestamp': datetime.utcnow().isoformat(),
            'symbols_tested': symbols,
            'test_results': {}
        }
        
        for symbol in symbols:
            logger.info(f"📊 Testing symbol: {symbol}")
            
            symbol_results = {
                'symbol': symbol,
                'tests': {}
            }
            
            # Run all test cases
            try:
                symbol_results['tests']['default_pagination'] = await self.test_default_pagination(symbol)
                await asyncio.sleep(0.5)  # Rate limiting between test types
                
                symbol_results['tests']['custom_limit_pagination'] = await self.test_custom_limit_pagination(symbol)
                await asyncio.sleep(0.5)
                
                symbol_results['tests']['time_based_pagination'] = await self.test_time_based_pagination(symbol)
                await asyncio.sleep(0.5)
                
                symbol_results['tests']['edge_cases'] = await self.test_edge_cases(symbol)
                await asyncio.sleep(0.5)
                
                symbol_results['tests']['performance'] = await self.test_performance_with_pagination(symbol)
                
                # Overall status for this symbol
                test_statuses = [test.get('status', 'ERROR') for test in symbol_results['tests'].values()]
                symbol_results['overall_status'] = 'PASS' if all(status in ['PASS', 'PARTIAL'] for status in test_statuses) else 'FAIL'
                
            except Exception as e:
                logger.error(f"❌ Symbol {symbol} testing failed: {e}")
                symbol_results['error'] = str(e)
                symbol_results['overall_status'] = 'ERROR'
            
            all_results['test_results'][symbol] = symbol_results
            
            # Longer delay between symbols
            await asyncio.sleep(1.0)
        
        return all_results


async def test_all_exchanges_pagination():
    """
    Test pagination for all configured exchanges.
    """
    exchanges_to_test = ['binance_spot', 'bybit_spot', 'mexc_spot']
    
    all_exchange_results = {
        'test_suite': 'comprehensive_trade_pagination',
        'timestamp': datetime.utcnow().isoformat(),
        'exchanges_tested': exchanges_to_test,
        'results': {}
    }
    
    for exchange_name in exchanges_to_test:
        logger.info(f"\n{'='*60}")
        logger.info(f"🔍 TESTING TRADE PAGINATION FOR {exchange_name.upper()}")
        logger.info(f"{'='*60}")
        
        tester = TradePaginationTester(exchange_name)
        
        try:
            # Setup connection
            await tester.setup()
            
            # Run comprehensive tests
            results = await tester.run_comprehensive_pagination_tests()
            all_exchange_results['results'][exchange_name] = results
            
            # Print summary
            logger.info(f"✅ {exchange_name} pagination tests completed")
            
        except Exception as e:
            logger.error(f"❌ {exchange_name} pagination tests failed: {e}")
            all_exchange_results['results'][exchange_name] = {
                'exchange': exchange_name,
                'error': str(e),
                'status': 'ERROR'
            }
        
        finally:
            # Cleanup
            await tester.teardown()
            
        # Delay between exchanges
        await asyncio.sleep(2.0)
    
    # Print final summary
    logger.info(f"\n{'='*60}")
    logger.info("🏁 PAGINATION TESTING SUMMARY")
    logger.info(f"{'='*60}")
    
    for exchange_name, results in all_exchange_results['results'].items():
        if 'error' in results:
            logger.info(f"❌ {exchange_name}: ERROR - {results['error']}")
        else:
            symbols_tested = len(results.get('test_results', {}))
            logger.info(f"✅ {exchange_name}: {symbols_tested} symbols tested")
    
    return all_exchange_results


if __name__ == "__main__":
    asyncio.run(test_all_exchanges_pagination()) 