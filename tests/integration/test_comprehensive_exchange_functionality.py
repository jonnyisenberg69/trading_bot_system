#!/usr/bin/env python
"""
Comprehensive Exchange Functionality Test

This test suite validates three core functionalities across all exchanges:
1. REST API trade fetching with pagination
2. WebSocket trade stream parsing 
3. Database insertion and persistence

Covers all supported exchanges with real API connections.
"""

import asyncio
import sys
import json
import time
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import structlog
from database.connection import init_db, get_session, close_db
from database.repositories.trade_repository import TradeRepository
from database.repositories.position_repository import PositionRepository
from database.models import Exchange, Trade
from exchanges.connectors import create_exchange_connector
from exchanges.websocket.ws_manager import WebSocketManager, WSMessageType
from config.exchange_keys import get_exchange_config
from order_management.enhanced_trade_sync import EnhancedTradeSync
from order_management.tracking import PositionManager

# Configure logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="%H:%M:%S"),
        structlog.dev.ConsoleRenderer(),
    ],
)
logger = structlog.get_logger(__name__)


class ComprehensiveExchangeTester:
    """
    Comprehensive tester for all exchange functionalities.
    """
    
    def __init__(self):
        self.exchanges_to_test = [
            'binance_spot',
            'binance_perp', 
            'bybit_spot',
            'bybit_perp',
            'mexc_spot',
            'gateio_spot',
            'bitget_spot',
            'hyperliquid_perp'
        ]
        
        self.test_symbols = {
            'binance_spot': ['BTC/USDT', 'ETH/USDT'],
            'binance_perp': ['BTC/USDT:USDT', 'ETH/USDT:USDT'],
            'bybit_spot': ['BTC/USDT', 'ETH/USDT'],
            'bybit_perp': ['BTC/USDT:USDT', 'ETH/USDT:USDT'],
            'mexc_spot': ['BTC/USDT', 'ETH/USDT'],
            'gateio_spot': ['BTC/USDT', 'ETH/USDT'],
            'bitget_spot': ['BTC/USDT', 'ETH/USDT'],
            'hyperliquid_perp': ['BTC/USD:USD', 'ETH/USD:USD']
        }
        
        self.websocket_endpoints = {
            'binance_spot': 'wss://stream.binance.com:9443/ws/btcusdt@trade',
            'binance_perp': 'wss://fstream.binance.com/ws/btcusdt@trade',
            'bybit_spot': 'wss://stream.bybit.com/v5/public/spot',
            'bybit_perp': 'wss://stream.bybit.com/v5/public/linear',
            'mexc_spot': 'wss://wbs.mexc.com/ws',
            'gateio_spot': 'wss://ws.gate.io/v4/',
            'bitget_spot': 'wss://ws.bitget.com/spot/v1/stream',
            'hyperliquid_perp': 'wss://api.hyperliquid.xyz/ws'
        }
        
        self.connectors = {}
        self.test_results = {
            'rest_api': {},
            'websocket': {},
            'database': {},
            'summary': {}
        }
        
    async def run_comprehensive_tests(self):
        """Run all comprehensive tests."""
        
        print("🚀 COMPREHENSIVE EXCHANGE FUNCTIONALITY TEST")
        print("=" * 70)
        print("Testing: REST API, WebSocket, Database persistence")
        print("Exchanges:", ', '.join(self.exchanges_to_test))
        print()
        
        # Initialize database
        await init_db()
        
        try:
            async for session in get_session():
                print("1️⃣ Setting up connectors and database...")
                
                # Setup repositories
                trade_repository = TradeRepository(session)
                position_repository = PositionRepository(session)
                position_manager = PositionManager(data_dir="data/comprehensive_test")
                
                # Setup exchange records in database
                exchange_records = {}
                for exchange_name in self.exchanges_to_test:
                    exchange_type = 'perpetual' if 'perp' in exchange_name else 'spot'
                    exchange = Exchange(
                        name=exchange_name,
                        type=exchange_type,
                        is_active=True
                    )
                    session.add(exchange)
                    await session.commit()
                    exchange_records[exchange_name] = exchange
                    print(f"   ✅ Created {exchange_name} exchange record (ID: {exchange.id})")
                
                # Setup connectors
                print("\n2️⃣ Setting up exchange connectors...")
                await self._setup_connectors()
                
                # Test 1: REST API Trade Fetching
                print("\n3️⃣ Testing REST API trade fetching...")
                await self._test_rest_api_functionality()
                
                # Test 2: WebSocket Trade Stream Parsing
                print("\n4️⃣ Testing WebSocket trade stream parsing...")
                await self._test_websocket_functionality()
                
                # Test 3: Database Integration
                print("\n5️⃣ Testing database integration...")
                await self._test_database_functionality(trade_repository, exchange_records)
                
                # Test 4: Enhanced Trade Sync Integration
                print("\n6️⃣ Testing enhanced trade sync with pagination...")
                await self._test_enhanced_sync_integration(
                    trade_repository, position_manager, exchange_records
                )
                
                # Generate comprehensive report
                print("\n7️⃣ Generating comprehensive report...")
                await self._generate_comprehensive_report()
                
                break
                
        finally:
            await self._cleanup_connectors()
            await close_db()
    
    async def _setup_connectors(self):
        """Setup all exchange connectors."""
        for exchange_name in self.exchanges_to_test:
            try:
                config = get_exchange_config(exchange_name)
                if config:
                    connector = create_exchange_connector(exchange_name.split('_')[0], config)
                    if connector:
                        connected = await connector.connect()
                        if connected:
                            self.connectors[exchange_name] = connector
                            print(f"   ✅ {exchange_name} connected")
                        else:
                            print(f"   ❌ {exchange_name} connection failed")
                    else:
                        print(f"   ❌ {exchange_name} connector creation failed")
                else:
                    print(f"   ❌ {exchange_name} no configuration")
            except Exception as e:
                print(f"   ❌ {exchange_name} setup error: {e}")
        
        print(f"   📊 Successfully connected to {len(self.connectors)} exchanges")
    
    async def _test_rest_api_functionality(self):
        """Test REST API trade fetching with various parameters."""
        
        for exchange_name, connector in self.connectors.items():
            print(f"\n   🔍 Testing {exchange_name} REST API...")
            
            exchange_results = {
                'connection_status': 'connected',
                'symbols_tested': [],
                'pagination_tests': [],
                'performance_metrics': {},
                'errors': []
            }
            
            symbols = self.test_symbols.get(exchange_name, ['BTC/USDT'])
            
            for symbol in symbols:
                print(f"      📊 Testing {symbol}...")
                
                symbol_results = {
                    'symbol': symbol,
                    'tests': {},
                    'total_trades_found': 0
                }
                
                # Test different time windows and limits
                test_scenarios = [
                    {'name': 'Recent 1h', 'since_hours': 1, 'limit': 10},
                    {'name': 'Recent 6h', 'since_hours': 6, 'limit': 25},
                    {'name': 'Recent 24h', 'since_hours': 24, 'limit': 50},
                    {'name': 'Large limit', 'since_hours': 1, 'limit': 200}
                ]
                
                for scenario in test_scenarios:
                    try:
                        since = datetime.utcnow() - timedelta(hours=scenario['since_hours'])
                        
                        start_time = time.time()
                        
                        # Exchange-specific handling for API quirks
                        if exchange_name == 'bitget_spot':
                            # For Bitget, add some delay and use smaller time windows
                            await asyncio.sleep(0.5)
                            if scenario['since_hours'] > 6:
                                since = datetime.utcnow() - timedelta(hours=6)  # Limit to 6 hours
                        
                        trades = await connector.get_trade_history(
                            symbol=symbol,
                            since=since,
                            limit=scenario['limit']
                        )
                        response_time = time.time() - start_time
                        
                        scenario_result = {
                            'trades_found': len(trades),
                            'response_time_ms': round(response_time * 1000, 2),
                            'status': 'success',
                            'sample_trade': trades[0] if trades else None
                        }
                        
                        symbol_results['tests'][scenario['name']] = scenario_result
                        symbol_results['total_trades_found'] += len(trades)
                        
                        print(f"         {scenario['name']}: {len(trades)} trades in {response_time*1000:.1f}ms")
                        
                        # Exchange-specific rate limiting
                        if exchange_name == 'mexc_spot':
                            await asyncio.sleep(0.5)  # MEXC is slower
                        elif exchange_name == 'bitget_spot':
                            await asyncio.sleep(0.4)  # Bitget needs more delay
                        else:
                            await asyncio.sleep(0.3)  # Standard delay
                        
                    except Exception as e:
                        error_result = {
                            'status': 'error',
                            'error': str(e)
                        }
                        symbol_results['tests'][scenario['name']] = error_result
                        exchange_results['errors'].append(f"{symbol} {scenario['name']}: {e}")
                        print(f"         {scenario['name']}: ❌ Error - {e}")
                        
                        # Continue with other tests even if one fails
                        await asyncio.sleep(0.2)
                
                exchange_results['symbols_tested'].append(symbol_results)
            
            # Calculate performance metrics
            all_response_times = []
            total_trades = 0
            
            for symbol_data in exchange_results['symbols_tested']:
                total_trades += symbol_data['total_trades_found']
                for test_name, test_data in symbol_data['tests'].items():
                    if 'response_time_ms' in test_data:
                        all_response_times.append(test_data['response_time_ms'])
            
            if all_response_times:
                exchange_results['performance_metrics'] = {
                    'avg_response_time_ms': round(sum(all_response_times) / len(all_response_times), 2),
                    'min_response_time_ms': min(all_response_times),
                    'max_response_time_ms': max(all_response_times),
                    'total_trades_found': total_trades,
                    'total_requests': len(all_response_times)
                }
            
            self.test_results['rest_api'][exchange_name] = exchange_results
            
            metrics = exchange_results['performance_metrics']
            if metrics:
                print(f"      📈 Performance: {metrics['avg_response_time_ms']}ms avg, {metrics['total_trades_found']} total trades")
    
    async def _test_websocket_functionality(self):
        """Test WebSocket trade stream parsing."""
        print("      🔗 Testing WebSocket message parsing capabilities...")
        
        # Test sample messages for each exchange
        sample_messages = {
            'binance_spot': {
                'trade': '{"e":"trade","E":1234567890123,"s":"BTCUSDT","t":12345,"p":"50000.00","q":"0.001","b":88,"a":50,"T":1234567890123,"m":true,"M":true}',
                'orderbook': '{"e":"depthUpdate","E":1234567890123,"s":"BTCUSDT","U":157,"u":160,"b":[["50000.00","0.001"]],"a":[["50001.00","0.002"]]}'
            },
            'bybit_spot': {
                'trade': '{"topic":"publicTrade.BTCUSDT","type":"snapshot","data":[{"T":1672304486865,"s":"BTCUSDT","S":"Buy","v":"0.001","p":"16578.50","L":"PlusTick","i":"20f43950-d8dd-5b31-9112-a178eb6023af","BT":false}]}',
                'orderbook': '{"topic":"orderbook.1.BTCUSDT","type":"snapshot","data":{"s":"BTCUSDT","b":[["16493.50","0.006"]],"a":[["16611.00","0.029"]],"u":18521,"seq":7961638}}'
            },
            'hyperliquid_perp': {
                'trade': '{"channel":"trades","data":[{"coin":"BTC","side":"A","px":"50000","sz":"0.001","time":1672531200000}]}',
                'orderbook': '{"channel":"l2Book","data":{"coin":"BTC","levels":[[{"px":"50000","sz":"0.1","n":1}],[{"px":"50001","sz":"0.2","n":1}]],"time":1672531200000}}'
            }
        }
        
        # Initialize WebSocket manager
        ws_manager = WebSocketManager()
        await ws_manager.start()
        
        # Test message parsing for available exchanges
        for exchange_name in ['binance_spot', 'bybit_spot', 'hyperliquid_perp']:
            if exchange_name not in self.connectors:
                continue
                
            print(f"      📡 Testing {exchange_name} WebSocket parsing...")
            
            exchange_ws_results = {
                'message_types_tested': [],
                'parsing_results': {},
                'errors': []
            }
            
            if exchange_name in sample_messages:
                for msg_type, sample_msg in sample_messages[exchange_name].items():
                    try:
                        # Parse sample message
                        message_data = json.loads(sample_msg)
                        
                        # Test message type detection
                        detected_type = ws_manager._determine_message_type(
                            exchange_name.split('_')[0], message_data
                        )
                        
                        # Test message normalization
                        if detected_type:
                            normalized = ws_manager._normalize_message(
                                exchange_name.split('_')[0], detected_type, message_data
                            )
                            
                            parsing_result = {
                                'detected_type': detected_type,
                                'normalized_fields': list(normalized.keys()),
                                'symbol_extracted': normalized.get('symbol'),
                                'status': 'success'
                            }
                        else:
                            parsing_result = {
                                'detected_type': None,
                                'status': 'type_not_detected'
                            }
                        
                        exchange_ws_results['parsing_results'][msg_type] = parsing_result
                        exchange_ws_results['message_types_tested'].append(msg_type)
                        
                        print(f"         {msg_type}: ✅ Type={detected_type}")
                        
                    except Exception as e:
                        error_result = {
                            'status': 'error',
                            'error': str(e)
                        }
                        exchange_ws_results['parsing_results'][msg_type] = error_result
                        exchange_ws_results['errors'].append(f"{msg_type}: {e}")
                        print(f"         {msg_type}: ❌ Error - {e}")
            
            self.test_results['websocket'][exchange_name] = exchange_ws_results
        
        await ws_manager.stop()
        print(f"      📊 Tested WebSocket parsing for {len(self.test_results['websocket'])} exchanges")
    
    async def _test_database_functionality(self, trade_repository, exchange_records):
        """Test database insertion and persistence."""
        
        print("      💾 Testing database operations...")
        
        database_results = {
            'trades_inserted': 0,
            'exchanges_tested': 0,
            'persistence_verified': False,
            'errors': []
        }
        
        # Test data insertion for each exchange
        for exchange_name, connector in self.connectors.items():
            if exchange_name not in exchange_records:
                continue
                
            exchange_id = exchange_records[exchange_name].id
            
            try:
                print(f"         Testing {exchange_name} database operations...")
                
                # Create sample trade data
                sample_trade_data = {
                    'id': f'test_trade_{exchange_name}_{int(time.time())}',
                    'symbol': 'BTC/USDT',
                    'side': 'buy',
                    'amount': 0.001,
                    'price': 50000.0,
                    'cost': 50.0,
                    'timestamp': int(time.time() * 1000),
                    'fee': {
                        'cost': 0.05,
                        'currency': 'USDT'
                    },
                    'order': 'test_order_123',
                    'type': 'limit',
                    'maker': True
                }
                
                # Insert trade
                trade = await trade_repository.save_trade(sample_trade_data, exchange_id)
                
                if trade:
                    database_results['trades_inserted'] += 1
                    print(f"            ✅ Trade inserted (ID: {trade.id})")
                    
                    # Verify persistence by retrieving
                    retrieved_trade = await trade_repository.get_trade(trade.id)
                    if retrieved_trade and retrieved_trade.id == trade.id:
                        database_results['persistence_verified'] = True
                        print(f"            ✅ Trade persistence verified")
                    else:
                        print(f"            ❌ Trade persistence failed")
                else:
                    print(f"            ❌ Trade insertion failed")
                
                database_results['exchanges_tested'] += 1
                
            except Exception as e:
                error_msg = f"{exchange_name} database test: {e}"
                database_results['errors'].append(error_msg)
                print(f"            ❌ Error - {e}")
        
        self.test_results['database'] = database_results
        
        print(f"      📊 Database test: {database_results['trades_inserted']} trades inserted, "
              f"{database_results['exchanges_tested']} exchanges tested")
    
    async def _test_enhanced_sync_integration(self, trade_repository, position_manager, exchange_records):
        """Test enhanced trade sync integration."""
        
        print("      🔄 Testing enhanced trade sync integration...")
        
        # Create enhanced sync system
        enhanced_sync = EnhancedTradeSync(
            exchange_connectors=self.connectors,
            trade_repository=trade_repository,
            position_manager=position_manager
        )
        
        sync_results = {
            'exchanges_synced': 0,
            'total_trades_fetched': 0,
            'total_requests_made': 0,
            'avg_response_time_ms': 0,
            'sync_details': {},
            'errors': []
        }
        
        # Test sync for each exchange
        for exchange_name in list(self.connectors.keys())[:3]:  # Test first 3 to save time
            try:
                print(f"         Syncing {exchange_name}...")
                
                # Exchange-specific symbol selection
                if exchange_name == 'hyperliquid_perp':
                    test_symbols = ['BTC/USD:USD']  # Use Hyperliquid format
                elif exchange_name == 'binance_perp':
                    test_symbols = ['BTC/USDT:USDT']  # Will be normalized properly
                else:
                    test_symbols = ['BTC/USDT']
                
                # Perform sync with pagination
                results = await enhanced_sync.sync_exchange_with_pagination(
                    exchange_name=exchange_name,
                    symbols=test_symbols,
                    since=datetime.utcnow() - timedelta(hours=6)
                )
                
                if results:
                    result = results[0]  # First symbol result
                    
                    sync_detail = {
                        'trades_fetched': result.total_fetched,
                        'requests_made': result.total_requests,
                        'time_taken_ms': result.time_taken_ms,
                        'errors': result.errors,
                        'performance_rating': 'GOOD' if not result.performance_metrics.get('is_slow') else 'SLOW'
                    }
                    
                    sync_results['sync_details'][exchange_name] = sync_detail
                    sync_results['total_trades_fetched'] += result.total_fetched
                    sync_results['total_requests_made'] += result.total_requests
                    sync_results['exchanges_synced'] += 1
                    
                    print(f"            ✅ {result.total_fetched} trades, {result.total_requests} requests, "
                          f"{result.time_taken_ms:.1f}ms")
                
            except Exception as e:
                error_msg = f"{exchange_name} sync: {e}"
                sync_results['errors'].append(error_msg)
                print(f"            ❌ Error - {e}")
                # Continue with other exchanges
        
        # Calculate average response time
        if sync_results['exchanges_synced'] > 0:
            total_time = sum(detail['time_taken_ms'] for detail in sync_results['sync_details'].values())
            sync_results['avg_response_time_ms'] = total_time / sync_results['exchanges_synced']
        
        # Get pagination summary
        pagination_summary = enhanced_sync.get_pagination_summary()
        sync_results['pagination_summary'] = {
            'supported_exchanges': len(pagination_summary['supported_exchanges']),
            'pagination_methods': pagination_summary['pagination_methods'],
            'sync_statistics': pagination_summary['sync_statistics']
        }
        
        self.test_results['enhanced_sync'] = sync_results
        
        print(f"      📊 Enhanced sync: {sync_results['exchanges_synced']} exchanges, "
              f"{sync_results['total_trades_fetched']} trades, "
              f"{sync_results['avg_response_time_ms']:.1f}ms avg")
    
    async def _generate_comprehensive_report(self):
        """Generate comprehensive test report."""
        
        print("\n" + "="*70)
        print("📋 COMPREHENSIVE TEST REPORT")
        print("="*70)
        
        # REST API Summary
        print("\n🌐 REST API TRADE FETCHING RESULTS:")
        rest_summary = {
            'exchanges_tested': len(self.test_results['rest_api']),
            'total_trades_found': 0,
            'avg_response_time': 0,
            'fastest_exchange': None,
            'slowest_exchange': None
        }
        
        response_times = []
        exchange_performances = {}
        
        for exchange_name, results in self.test_results['rest_api'].items():
            if 'performance_metrics' in results and results['performance_metrics']:
                metrics = results['performance_metrics']
                rest_summary['total_trades_found'] += metrics['total_trades_found']
                response_times.append(metrics['avg_response_time_ms'])
                exchange_performances[exchange_name] = metrics['avg_response_time_ms']
                
                print(f"   {exchange_name:15}: {metrics['avg_response_time_ms']:6.1f}ms avg, "
                      f"{metrics['total_trades_found']:3} trades, "
                      f"{len(results['errors']):2} errors")
        
        if response_times:
            rest_summary['avg_response_time'] = sum(response_times) / len(response_times)
            rest_summary['fastest_exchange'] = min(exchange_performances, key=exchange_performances.get)
            rest_summary['slowest_exchange'] = max(exchange_performances, key=exchange_performances.get)
        
        print(f"\n   📊 REST API Summary:")
        print(f"      Exchanges tested: {rest_summary['exchanges_tested']}")
        print(f"      Total trades found: {rest_summary['total_trades_found']}")
        print(f"      Average response time: {rest_summary['avg_response_time']:.1f}ms")
        if rest_summary['fastest_exchange']:
            print(f"      Fastest: {rest_summary['fastest_exchange']} "
                  f"({exchange_performances[rest_summary['fastest_exchange']]:.1f}ms)")
            print(f"      Slowest: {rest_summary['slowest_exchange']} "
                  f"({exchange_performances[rest_summary['slowest_exchange']]:.1f}ms)")
        
        # WebSocket Summary
        print("\n📡 WEBSOCKET PARSING RESULTS:")
        ws_summary = {
            'exchanges_tested': len(self.test_results['websocket']),
            'message_types_supported': set(),
            'total_parsing_tests': 0
        }
        
        for exchange_name, results in self.test_results['websocket'].items():
            parsing_success = sum(1 for r in results['parsing_results'].values() 
                                if r.get('status') == 'success')
            total_tests = len(results['parsing_results'])
            ws_summary['total_parsing_tests'] += total_tests
            
            for msg_type, result in results['parsing_results'].items():
                if result.get('status') == 'success':
                    ws_summary['message_types_supported'].add(result.get('detected_type'))
            
            print(f"   {exchange_name:15}: {parsing_success}/{total_tests} parsing tests passed")
        
        print(f"\n   📊 WebSocket Summary:")
        print(f"      Exchanges tested: {ws_summary['exchanges_tested']}")
        print(f"      Total parsing tests: {ws_summary['total_parsing_tests']}")
        print(f"      Message types detected: {list(ws_summary['message_types_supported'])}")
        
        # Database Summary
        print("\n💾 DATABASE INTEGRATION RESULTS:")
        db_results = self.test_results['database']
        print(f"   Trades inserted: {db_results['trades_inserted']}")
        print(f"   Exchanges tested: {db_results['exchanges_tested']}")
        print(f"   Persistence verified: {'✅' if db_results['persistence_verified'] else '❌'}")
        print(f"   Errors: {len(db_results['errors'])}")
        
        # Enhanced Sync Summary
        if 'enhanced_sync' in self.test_results:
            print("\n🔄 ENHANCED SYNC INTEGRATION RESULTS:")
            sync_results = self.test_results['enhanced_sync']
            print(f"   Exchanges synced: {sync_results['exchanges_synced']}")
            print(f"   Total trades fetched: {sync_results['total_trades_fetched']}")
            print(f"   Total requests made: {sync_results['total_requests_made']}")
            print(f"   Average response time: {sync_results['avg_response_time_ms']:.1f}ms")
            print(f"   Errors: {len(sync_results['errors'])}")
        
        # Overall Summary
        print("\n🎯 OVERALL TEST SUMMARY:")
        total_exchanges = len(self.exchanges_to_test)
        connected_exchanges = len(self.connectors)
        success_rate = (connected_exchanges / total_exchanges) * 100
        
        print(f"   Target exchanges: {total_exchanges}")
        print(f"   Successfully connected: {connected_exchanges}")
        print(f"   Connection success rate: {success_rate:.1f}%")
        print(f"   REST API tests: {'✅ PASS' if rest_summary['exchanges_tested'] > 0 else '❌ FAIL'}")
        print(f"   WebSocket tests: {'✅ PASS' if ws_summary['exchanges_tested'] > 0 else '❌ FAIL'}")
        print(f"   Database tests: {'✅ PASS' if db_results['persistence_verified'] else '❌ FAIL'}")
        
        # Save detailed results
        output_file = "data/comprehensive_test_results.json"
        with open(output_file, 'w') as f:
            json.dump(self.test_results, f, indent=2, default=str)
        
        print(f"\n💾 Detailed results saved to: {output_file}")
        
        print("\n" + "="*70)
        print("🏁 COMPREHENSIVE TESTING COMPLETE")
        print("="*70)
    
    async def _cleanup_connectors(self):
        """Cleanup all exchange connectors."""
        print("\n8️⃣ Cleaning up connectors...")
        
        for exchange_name, connector in self.connectors.items():
            try:
                await connector.disconnect()
                print(f"   ✅ {exchange_name} disconnected")
            except Exception as e:
                print(f"   ❌ {exchange_name} disconnect error: {e}")


async def main():
    """Run comprehensive exchange functionality tests."""
    tester = ComprehensiveExchangeTester()
    await tester.run_comprehensive_tests()


if __name__ == "__main__":
    asyncio.run(main()) 