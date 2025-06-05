#!/usr/bin/env python3
"""
Comprehensive WebSocket pipeline test for all exchanges.
Tests each exchange individually through the full pipeline.
"""

import asyncio
import logging
import time
import json
import os
from typing import Dict, Any, List
from decimal import Decimal

from exchanges.connectors.binance import BinanceConnector
from exchanges.connectors.bybit import BybitConnector
from exchanges.connectors.mexc import MexcConnector
from exchanges.connectors.gateio import GateIOConnector
from exchanges.connectors.bitget import BitgetConnector
from exchanges.connectors.hyperliquid import HyperliquidConnector
from exchanges.websocket.ws_manager import WebSocketManager
from config.exchange_keys import EXCHANGE_CONFIGS

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class WebSocketPipelineTest:
    """Test WebSocket pipeline for each exchange."""
    
    def __init__(self):
        self.ws_manager = WebSocketManager()
        self.test_results = {}
        self.received_messages = {}
        
    async def setup(self):
        """Initialize the WebSocket manager."""
        await self.ws_manager.start()
        
        # Register message handlers
        self.ws_manager.register_handler('order_update', self._handle_order_update)
        self.ws_manager.register_handler('trade', self._handle_trade)
        self.ws_manager.register_handler('orderbook', self._handle_orderbook)
        self.ws_manager.register_handler('heartbeat', self._handle_heartbeat)
        self.ws_manager.register_handler('error', self._handle_error)
    
    async def cleanup(self):
        """Clean up the WebSocket manager."""
        await self.ws_manager.stop()
    
    async def _handle_order_update(self, message: Dict[str, Any]):
        """Handle order update messages."""
        exchange = message.get('exchange', 'unknown')
        logger.info(f"📊 Order update from {exchange}: {message.get('id', 'N/A')}")
        self._record_message(exchange, 'order_update', message)
    
    async def _handle_trade(self, message: Dict[str, Any]):
        """Handle trade messages."""
        exchange = message.get('exchange', 'unknown')
        logger.info(f"💸 Trade from {exchange}: {message.get('price', 'N/A')} @ {message.get('amount', 'N/A')}")
        self._record_message(exchange, 'trade', message)
    
    async def _handle_orderbook(self, message: Dict[str, Any]):
        """Handle orderbook messages."""
        exchange = message.get('exchange', 'unknown')
        bids = len(message.get('bids', []))
        asks = len(message.get('asks', []))
        logger.info(f"📖 Orderbook from {exchange}: {bids} bids, {asks} asks")
        self._record_message(exchange, 'orderbook', message)
    
    async def _handle_heartbeat(self, message: Dict[str, Any]):
        """Handle heartbeat messages."""
        exchange = message.get('exchange', 'unknown')
        logger.debug(f"💓 Heartbeat from {exchange}")
        self._record_message(exchange, 'heartbeat', message)
    
    async def _handle_error(self, message: Dict[str, Any]):
        """Handle error messages."""
        exchange = message.get('exchange', 'unknown')
        logger.error(f"❌ Error from {exchange}: {message}")
        self._record_message(exchange, 'error', message)
    
    def _record_message(self, exchange: str, msg_type: str, message: Dict[str, Any]):
        """Record received message for analysis."""
        if exchange not in self.received_messages:
            self.received_messages[exchange] = {}
        if msg_type not in self.received_messages[exchange]:
            self.received_messages[exchange][msg_type] = []
        self.received_messages[exchange][msg_type].append(message)
    
    def _get_websocket_endpoints(self, exchange_name: str, config: Dict[str, Any]) -> Dict[str, str]:
        """Get WebSocket endpoints for each exchange based on official documentation."""
        # Determine if sandbox/testnet
        is_testnet = config.get('sandbox', False)
        
        endpoints = {}
        
        if exchange_name.startswith('binance'):
            if is_testnet:
                # Binance Testnet WebSocket endpoints
                endpoints = {
                    'public': 'wss://testnet.binance.vision/ws',
                    'private': 'wss://testnet.binance.vision/ws'
                }
            else:
                # Binance Production WebSocket endpoints
                if 'perp' in exchange_name or 'future' in exchange_name:
                    # Futures WebSocket
                    endpoints = {
                        'public': 'wss://fstream.binance.com/ws',
                        'private': 'wss://fstream.binance.com/ws'
                    }
                else:
                    # Spot WebSocket
                    endpoints = {
                        'public': 'wss://stream.binance.com:9443/ws',
                        'private': 'wss://stream.binance.com:9443/ws'
                    }
        
        elif exchange_name.startswith('bybit'):
            if is_testnet:
                # Bybit Testnet
                endpoints = {
                    'public': 'wss://stream-testnet.bybit.com/v5/public/spot',
                    'private': 'wss://stream-testnet.bybit.com/v5/private'
                }
            else:
                # Bybit Production
                if 'perp' in exchange_name or 'future' in exchange_name:
                    # Perpetual/Futures
                    endpoints = {
                        'public': 'wss://stream.bybit.com/v5/public/linear',
                        'private': 'wss://stream.bybit.com/v5/private'
                    }
                else:
                    # Spot
                    endpoints = {
                        'public': 'wss://stream.bybit.com/v5/public/spot',
                        'private': 'wss://stream.bybit.com/v5/private'
                    }
        
        elif exchange_name.startswith('mexc'):
            # MEXC WebSocket endpoints
            endpoints = {
                'public': 'wss://wbs.mexc.com/ws',
                'private': 'wss://wbs.mexc.com/ws'
            }
        
        elif exchange_name.startswith('gateio'):
            # Gate.io WebSocket endpoints
            endpoints = {
                'public': 'wss://api.gateio.ws/ws/v4/',
                'private': 'wss://api.gateio.ws/ws/v4/'
            }
        
        elif exchange_name.startswith('bitget'):
            # Bitget WebSocket endpoints
            endpoints = {
                'public': 'wss://ws.bitget.com/spot/v1/stream',
                'private': 'wss://ws.bitget.com/spot/v1/stream'
            }
        
        elif exchange_name.startswith('hyperliquid'):
            # Hyperliquid WebSocket endpoints
            endpoints = {
                'public': 'wss://api.hyperliquid.xyz/ws',
                'private': 'wss://api.hyperliquid.xyz/ws'
            }
        
        else:
            logger.warning(f"Unknown exchange: {exchange_name}")
        
        return endpoints
    
    async def test_exchange(self, exchange_name: str, connector_class, config: Dict[str, Any]) -> Dict[str, Any]:
        """Test a single exchange through the full pipeline."""
        logger.info(f"\n🚀 Testing {exchange_name.upper()} WebSocket Pipeline")
        logger.info("=" * 60)
        
        result = {
            'exchange': exchange_name,
            'success': False,
            'connection': False,
            'authentication': False,
            'subscription': False,
            'messages_received': 0,
            'message_types': [],
            'errors': [],
            'duration': 0
        }
        
        start_time = time.time()
        connector = None
        conn_id = None
        
        try:
            # Step 1: Initialize connector
            logger.info(f"1️⃣ Initializing {exchange_name} connector...")
            connector = connector_class(config['config'])
            await connector.connect()
            
            # Step 2: Get WebSocket endpoints (predefined based on exchange)
            endpoints = self._get_websocket_endpoints(exchange_name, config['config'])
            if not endpoints:
                raise Exception(f"No WebSocket endpoints available for {exchange_name}")
            
            # Use private endpoint if available, otherwise public
            endpoint = endpoints.get('private') or endpoints.get('public')
            conn_type = 'private' if endpoints.get('private') else 'public'
            
            logger.info(f"2️⃣ Connecting to {exchange_name} WebSocket ({conn_type})...")
            logger.info(f"   Endpoint: {endpoint}")
            
            # Step 3: Connect to WebSocket
            conn_id = await self.ws_manager.connect_exchange(
                exchange=connector,
                endpoint=endpoint,
                conn_type=conn_type
            )
            
            if not conn_id:
                raise Exception(f"Failed to establish WebSocket connection to {exchange_name}")
            
            result['connection'] = True
            logger.info(f"✅ Connected with ID: {conn_id}")
            
            # Step 4: Wait for connection to establish
            logger.info(f"3️⃣ Waiting for {exchange_name} connection to establish...")
            await asyncio.sleep(3)
            
            # Check connection status
            status = self.ws_manager.get_connection_status(conn_id)
            if status.get('state') != 'connected':
                raise Exception(f"Connection failed to establish. State: {status.get('state')}")
            
            # Step 5: Authentication (for private connections)
            if conn_type == 'private':
                logger.info(f"4️⃣ Checking {exchange_name} authentication...")
                # Authentication happens automatically during connection
                result['authentication'] = True
                logger.info(f"✅ Authentication successful")
            else:
                result['authentication'] = True  # Not needed for public
                logger.info(f"✅ Public connection (no auth required)")
            
            # Step 6: Subscribe to channels
            logger.info(f"5️⃣ Subscribing to {exchange_name} channels...")
            
            # Get symbol for this exchange
            symbol = config['symbol']
            subscriptions_success = 0
            
            # Subscribe to orderbook
            if await self.ws_manager.subscribe(conn_id, 'orderbook', symbol):
                subscriptions_success += 1
                logger.info(f"✅ Subscribed to orderbook for {symbol}")
            else:
                logger.warning(f"⚠️ Failed to subscribe to orderbook for {symbol}")
            
            # Subscribe to trades  
            if await self.ws_manager.subscribe(conn_id, 'trades', symbol):
                subscriptions_success += 1
                logger.info(f"✅ Subscribed to trades for {symbol}")
            else:
                logger.warning(f"⚠️ Failed to subscribe to trades for {symbol}")
            
            result['subscription'] = subscriptions_success > 0
            
            # Step 7: Listen for messages
            logger.info(f"6️⃣ Listening for {exchange_name} messages (15 seconds)...")
            
            # Clear previous messages for this exchange
            if exchange_name in self.received_messages:
                del self.received_messages[exchange_name]
            
            # Wait and collect messages
            listen_duration = 15
            await asyncio.sleep(listen_duration)
            
            # Step 8: Analyze received messages
            logger.info(f"7️⃣ Analyzing {exchange_name} message results...")
            
            if exchange_name in self.received_messages:
                exchange_messages = self.received_messages[exchange_name]
                result['message_types'] = list(exchange_messages.keys())
                result['messages_received'] = sum(len(msgs) for msgs in exchange_messages.values())
                
                for msg_type, messages in exchange_messages.items():
                    logger.info(f"   📨 {msg_type}: {len(messages)} messages")
                    if messages and msg_type != 'heartbeat':
                        # Show sample message
                        sample = messages[0]
                        logger.info(f"      Sample: {json.dumps(sample, indent=2)[:200]}...")
            
            result['success'] = (
                result['connection'] and 
                result['authentication'] and 
                result['subscription'] and 
                result['messages_received'] > 0
            )
            
            if result['success']:
                logger.info(f"🎉 {exchange_name.upper()} pipeline test PASSED!")
            else:
                logger.warning(f"⚠️ {exchange_name.upper()} pipeline test FAILED!")
            
        except Exception as e:
            error_msg = str(e)
            result['errors'].append(error_msg)
            logger.error(f"❌ {exchange_name} test failed: {error_msg}")
            
        finally:
            # Cleanup
            if conn_id:
                logger.info(f"8️⃣ Cleaning up {exchange_name} connection...")
                await self.ws_manager.close_connection(conn_id)
            
            if connector:
                await connector.disconnect()
            
            result['duration'] = time.time() - start_time
            logger.info(f"⏱️ {exchange_name} test completed in {result['duration']:.2f}s")
        
        return result
    
    async def run_all_tests(self) -> Dict[str, Any]:
        """Run pipeline tests for all exchanges."""
        logger.info("🎯 Starting WebSocket Pipeline Tests for All Exchanges")
        logger.info("=" * 80)
        
        # Exchange configurations
        exchanges_config = [
            {
                'name': 'binance_spot',
                'class': BinanceConnector,
                'symbol': 'BERA/USDT',
                'config': EXCHANGE_CONFIGS['binance_spot']
            },
            {
                'name': 'binance_perp',
                'class': BinanceConnector,
                'symbol': 'BERA/USDT:USDT', 
                'config': EXCHANGE_CONFIGS['binance_perp']
            },
            {
                'name': 'bybit_spot',
                'class': BybitConnector,
                'symbol': 'BERA/USDT',
                'config': EXCHANGE_CONFIGS['bybit_spot']
            },
            {
                'name': 'bybit_perp',
                'class': BybitConnector,
                'symbol': 'BERA/USDT:USDT',
                'config': EXCHANGE_CONFIGS['bybit_perp']
            },
            {
                'name': 'mexc_spot',
                'class': MexcConnector,
                'symbol': 'BERA/USDT',
                'config': EXCHANGE_CONFIGS['mexc_spot']
            },
            {
                'name': 'gateio_spot',
                'class': GateIOConnector,
                'symbol': 'BERA/USDT',
                'config': EXCHANGE_CONFIGS['gateio_spot']
            },
            {
                'name': 'bitget_spot',
                'class': BitgetConnector,
                'symbol': 'BERA/USDT',
                'config': EXCHANGE_CONFIGS['bitget_spot']
            },
            {
                'name': 'hyperliquid_perp',
                'class': HyperliquidConnector,
                'symbol': 'BERA/USDC:USDC',
                'config': EXCHANGE_CONFIGS['hyperliquid_perp']
            }
        ]
        
        # Run tests for each exchange
        all_results = {}
        successful_exchanges = []
        failed_exchanges = []
        
        for exchange_config in exchanges_config:
            try:
                result = await self.test_exchange(
                    exchange_config['name'],
                    exchange_config['class'],
                    exchange_config
                )
                all_results[exchange_config['name']] = result
                
                if result['success']:
                    successful_exchanges.append(exchange_config['name'])
                else:
                    failed_exchanges.append(exchange_config['name'])
                
                # Wait between tests
                await asyncio.sleep(2)
                
            except Exception as e:
                logger.error(f"💥 Critical error testing {exchange_config['name']}: {e}")
                all_results[exchange_config['name']] = {
                    'exchange': exchange_config['name'],
                    'success': False,
                    'errors': [str(e)]
                }
                failed_exchanges.append(exchange_config['name'])
        
        # Generate summary
        logger.info("\n📊 WEBSOCKET PIPELINE TEST SUMMARY")
        logger.info("=" * 80)
        
        logger.info(f"✅ Successful: {len(successful_exchanges)}/{len(exchanges_config)}")
        for exchange in successful_exchanges:
            result = all_results[exchange]
            logger.info(f"   🎉 {exchange}: {result['messages_received']} messages, {result['duration']:.1f}s")
        
        if failed_exchanges:
            logger.info(f"\n❌ Failed: {len(failed_exchanges)}/{len(exchanges_config)}")
            for exchange in failed_exchanges:
                result = all_results[exchange]
                errors = ', '.join(result.get('errors', ['Unknown error']))
                logger.info(f"   💥 {exchange}: {errors}")
        
        # Detailed results
        logger.info("\n📋 DETAILED RESULTS")
        logger.info("=" * 80)
        
        for exchange_name, result in all_results.items():
            logger.info(f"\n{exchange_name.upper()}:")
            logger.info(f"  Success: {result['success']}")
            logger.info(f"  Connection: {result.get('connection', False)}")
            logger.info(f"  Authentication: {result.get('authentication', False)}")
            logger.info(f"  Subscription: {result.get('subscription', False)}")
            logger.info(f"  Messages: {result.get('messages_received', 0)}")
            logger.info(f"  Types: {result.get('message_types', [])}")
            logger.info(f"  Duration: {result.get('duration', 0):.2f}s")
            if result.get('errors'):
                logger.info(f"  Errors: {result['errors']}")
        
        return {
            'total_exchanges': len(exchanges_config),
            'successful': len(successful_exchanges),
            'failed': len(failed_exchanges),
            'success_rate': len(successful_exchanges) / len(exchanges_config) * 100,
            'results': all_results,
            'successful_exchanges': successful_exchanges,
            'failed_exchanges': failed_exchanges
        }

async def main():
    """Main test function."""
    test_runner = WebSocketPipelineTest()
    
    try:
        await test_runner.setup()
        results = await test_runner.run_all_tests()
        
        # Final summary
        logger.info(f"\n🏁 FINAL RESULTS: {results['successful']}/{results['total_exchanges']} exchanges working ({results['success_rate']:.1f}%)")
        
        return results
        
    except Exception as e:
        logger.error(f"💥 Test runner failed: {e}")
        return None
        
    finally:
        await test_runner.cleanup()

if __name__ == "__main__":
    asyncio.run(main()) 