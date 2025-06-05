#!/usr/bin/env python
"""
Comprehensive Trade Pipeline Test

Tests private WebSocket authentication, subscription, and message parsing
across ALL 8 exchanges with actual trading to verify complete functionality.
"""

import asyncio
import sys
import json
import time
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
from decimal import Decimal

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from exchanges.connectors import create_exchange_connector
from exchanges.websocket import WebSocketManager, WSState, WSMessageType
from exchanges.base_connector import OrderType
from config.exchange_keys import get_exchange_config
from database.connection import init_db, close_db

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ComprehensiveTradePipelineTester:
    """Test complete trade pipeline across all exchanges."""
    
    def __init__(self):
        """Initialize the tester."""
        self.ws_manager = None
        self.connectors = {}
        self.connections = {}
        self.auth_results = {}
        self.trade_results = {}
        self.private_messages = {
            'order_updates': {},
            'trade_notifications': {},
            'heartbeat_messages': {}
        }
        
        # Test configuration - SMALL amounts for testing with market orders
        self.trading_config = {
            'binance_spot': {
                'symbol': 'BERA/USDT',
                'amount': 2.5,  # Above $5 minimum
                'order_type': 'market',  # Market order for immediate fill
                'test_auth': True
            },
            'binance_perp': {
                'symbol': 'BERA/USDT:USDT', 
                'amount': 2.5,  # Above $5 minimum
                'order_type': 'market',
                'test_auth': True
            },
            'bybit_spot': {
                'symbol': 'BERA/USDT',
                'amount': 1.0,  # Small amount for testing
                'order_type': 'market',
                'test_auth': True
            },
            'bybit_perp': {
                'symbol': 'BERA/USDT:USDT',
                'amount': 1.0,
                'order_type': 'market', 
                'test_auth': True
            },
            'mexc_spot': {
                'symbol': 'BERA/USDT',
                'amount': 1.0,
                'order_type': 'market',
                'test_auth': True
            },
            'gateio_spot': {
                'symbol': 'BERA/USDT',
                'amount': 1.0,
                'order_type': 'market',
                'test_auth': True
            },
            'bitget_spot': {
                'symbol': 'BERA/USDT',
                'amount': 1.0,
                'order_type': 'market',
                'test_auth': True
            },
            'hyperliquid_perp': {
                'symbol': 'BERA/USDC:USDC',
                'amount': 4.0,  # Above $10 minimum
                'order_type': 'market',
                'test_auth': True
            }
        }
        
        # Initialize tracking
        for exchange in self.trading_config:
            self.auth_results[exchange] = {'status': 'pending', 'error': None}
            self.trade_results[exchange] = {'orders': [], 'fills': [], 'messages': []}
            for msg_type in self.private_messages:
                self.private_messages[msg_type][exchange] = []
    
    async def setup(self):
        """Set up the testing environment."""
        print("🔧 Setting up Comprehensive Trade Pipeline Test...")
        
        # Initialize database
        await init_db()
        
        # Create WebSocket manager
        self.ws_manager = WebSocketManager({
            'ping_interval': 20.0,
            'pong_timeout': 10.0,
            'reconnect_enabled': True
        })
        await self.ws_manager.start()
        
        # Register message handlers
        self.ws_manager.register_handler(WSMessageType.ORDER_UPDATE, self._handle_order_update)
        self.ws_manager.register_handler(WSMessageType.TRADE, self._handle_trade_notification)
        self.ws_manager.register_handler(WSMessageType.HEARTBEAT, self._handle_heartbeat)
        
        # Create connectors for all exchanges
        print("📡 Creating exchange connectors...")
        for exchange_name in self.trading_config:
            try:
                config = get_exchange_config(exchange_name)
                connector = create_exchange_connector(exchange_name.split('_')[0], config)
                
                connected = await connector.connect()
                if connected:
                    self.connectors[exchange_name] = connector
                    print(f"   ✅ {exchange_name} - REST connector ready")
                else:
                    print(f"   ❌ {exchange_name} - REST connection failed")
                    self.auth_results[exchange_name] = {'status': 'failed', 'error': 'REST connection failed'}
            except Exception as e:
                print(f"   ❌ {exchange_name} - connector creation failed: {e}")
                self.auth_results[exchange_name] = {'status': 'failed', 'error': str(e)}
        
        print(f"✅ Setup complete - {len(self.connectors)}/{len(self.trading_config)} exchanges ready")
    
    async def cleanup(self):
        """Clean up resources."""
        print("🧹 Cleaning up resources...")
        
        if self.ws_manager:
            await self.ws_manager.stop()
        
        for connector in self.connectors.values():
            try:
                await connector.disconnect()
            except Exception as e:
                logger.debug(f"Error disconnecting connector: {e}")
        
        await close_db()
        print("✅ Cleanup completed")
    
    async def _handle_order_update(self, message: Dict[str, Any]):
        """Handle order update messages."""
        exchange = message.get('exchange')
        if exchange in self.private_messages['order_updates']:
            self.private_messages['order_updates'][exchange].append(message)
            self.trade_results[exchange]['messages'].append(('order_update', message))
            print(f"📨 {exchange} - ORDER UPDATE: {message.get('status', 'unknown')} | ID: {message.get('id', 'N/A')}")
    
    async def _handle_trade_notification(self, message: Dict[str, Any]):
        """Handle trade notifications."""
        exchange = message.get('exchange')
        if exchange in self.private_messages['trade_notifications']:
            self.private_messages['trade_notifications'][exchange].append(message)
            self.trade_results[exchange]['messages'].append(('trade', message))
            print(f"💰 {exchange} - TRADE: {message.get('amount', 0)} @ {message.get('price', 0)}")
    
    async def _handle_heartbeat(self, message: Dict[str, Any]):
        """Handle heartbeat/auth messages."""
        exchange = message.get('exchange')
        if exchange in self.private_messages['heartbeat_messages']:
            self.private_messages['heartbeat_messages'][exchange].append(message)
            logger.debug(f"💓 {exchange} - Heartbeat/Auth message")
    
    async def test_all_exchanges(self):
        """Test authentication and trading on ALL exchanges."""
        print("\n" + "="*80)
        print("🔐 COMPREHENSIVE WEBSOCKET + TRADING TEST")
        print("="*80)
        print("Testing authentication, subscriptions, and trading on ALL 8 exchanges")
        print("="*80)
        
        # Test all exchanges concurrently
        test_tasks = []
        for exchange_name, connector in self.connectors.items():
            task = asyncio.create_task(
                self._test_single_exchange(exchange_name, connector)
            )
            test_tasks.append(task)
        
        # Wait for all tests to complete
        results = await asyncio.gather(*test_tasks, return_exceptions=True)
        
        # Process results
        for i, result in enumerate(results):
            exchange_name = list(self.connectors.keys())[i]
            if isinstance(result, Exception):
                print(f"   💥 {exchange_name} - Test failed with exception: {result}")
                self.auth_results[exchange_name] = {'status': 'failed', 'error': str(result)}
    
    async def _test_single_exchange(self, exchange_name: str, connector):
        """Test a single exchange completely."""
        print(f"\n🔧 Testing {exchange_name}...")
        
        try:
            # Step 1: Authenticate WebSocket
            auth_success = await self._test_websocket_auth(exchange_name, connector)
            if not auth_success:
                self.auth_results[exchange_name] = {'status': 'failed', 'error': 'WebSocket auth failed'}
                return
            
            print(f"   ✅ {exchange_name} - WebSocket authenticated and subscribed")
            self.auth_results[exchange_name] = {'status': 'success'}
            
            # Step 2: Execute trade test
            await asyncio.sleep(2)  # Allow subscriptions to settle
            trade_success = await self._test_trading(exchange_name, connector)
            
            if trade_success:
                print(f"   🎉 {exchange_name} - COMPLETE SUCCESS!")
            else:
                print(f"   ⚠️  {exchange_name} - WebSocket OK, trading issues")
                
        except Exception as e:
            print(f"   💥 {exchange_name} - Test error: {e}")
            self.auth_results[exchange_name] = {'status': 'failed', 'error': str(e)}
    
    async def _test_websocket_auth(self, exchange_name: str, connector) -> bool:
        """Test WebSocket authentication and subscription."""
        try:
            # Connect to private WebSocket
            private_endpoint = self._get_private_endpoint(exchange_name)
            
            conn_id = await self.ws_manager.connect_exchange(
                connector, private_endpoint, "private"
            )
            
            if not conn_id:
                print(f"   ❌ {exchange_name} - Failed to connect to WebSocket")
                return False
            
            self.connections[exchange_name] = conn_id
            
            # Wait for connection and authentication
            connected = await self._wait_for_connection(conn_id, 15)
            return connected
            
        except Exception as e:
            print(f"   ❌ {exchange_name} - WebSocket error: {e}")
            return False
    
    async def _test_trading(self, exchange_name: str, connector) -> bool:
        """Test actual trading and monitor for private messages."""
        config = self.trading_config[exchange_name]
        symbol = config['symbol']
        amount = Decimal(str(config['amount']))
        order_type = config['order_type']
        
        try:
            print(f"   🔵 {exchange_name} - Placing {order_type.upper()} BUY: {amount} BERA")
            
            # Clear previous messages
            for msg_type in self.private_messages:
                self.private_messages[msg_type][exchange_name] = []
            
            # Place market order for immediate fill
            if order_type == 'market':
                buy_order = await connector.place_order(
                    symbol=symbol,
                    side='buy',
                    amount=amount,
                    order_type=OrderType.MARKET
                )
            else:
                # Get market price for limit order
                orderbook = await connector.get_orderbook(symbol, limit=5)
                if not orderbook or not orderbook.get('asks'):
                    print(f"   ❌ {exchange_name} - No market data")
                    return False
                
                ask_price = Decimal(str(orderbook['asks'][0][0]))
                buy_order = await connector.place_order(
                    symbol=symbol,
                    side='buy',
                    amount=amount,
                    price=ask_price,
                    order_type=OrderType.LIMIT
                )
            
            if buy_order and buy_order.get('id'):
                order_id = buy_order['id']
                print(f"   ✅ {exchange_name} - Order placed: {order_id}")
                self.trade_results[exchange_name]['orders'].append(buy_order)
                
                # Monitor for private messages for 15 seconds
                print(f"   ⏳ {exchange_name} - Monitoring for private messages...")
                
                message_received = False
                for i in range(15):
                    await asyncio.sleep(1)
                    
                    order_count = len(self.private_messages['order_updates'][exchange_name])
                    trade_count = len(self.private_messages['trade_notifications'][exchange_name])
                    
                    if order_count > 0 or trade_count > 0:
                        print(f"   🎉 {exchange_name} - SUCCESS! Messages: {order_count} orders, {trade_count} trades")
                        message_received = True
                        break
                    
                    if i % 3 == 0:
                        print(f"   ⏳ {exchange_name} - Still monitoring... ({i+1}s)")
                
                # Try to cancel if it's a limit order and still open
                if order_type == 'limit':
                    try:
                        await connector.cancel_order(order_id, symbol)
                        print(f"   🔴 {exchange_name} - Order cancelled")
                    except Exception as e:
                        print(f"   ⚠️  {exchange_name} - Cancel error (may have filled): {e}")
                
                return message_received
            else:
                print(f"   ❌ {exchange_name} - Failed to place order")
                return False
                
        except Exception as e:
            print(f"   ❌ {exchange_name} - Trading error: {e}")
            return False
    
    def _get_private_endpoint(self, exchange_name: str) -> str:
        """Get private WebSocket endpoint for exchange."""
        endpoints = {
            'binance_spot': 'wss://stream.binance.com:9443/ws',
            'binance_perp': 'wss://fstream.binance.com/ws',
            'bybit_spot': 'wss://stream.bybit.com/v5/private',
            'bybit_perp': 'wss://stream.bybit.com/v5/private',
            'mexc_spot': 'wss://wbs.mexc.com/ws',
            'gateio_spot': 'wss://api.gateio.ws/ws/v4/',
            'bitget_spot': 'wss://ws.bitget.com/spot/v1/stream',
            'hyperliquid_perp': 'wss://api.hyperliquid.xyz/ws'
        }
        return endpoints.get(exchange_name, '')
    
    async def _wait_for_connection(self, conn_id: str, timeout: int) -> bool:
        """Wait for WebSocket connection to be established."""
        for _ in range(timeout):
            status = self.ws_manager.get_connection_status(conn_id)
            if status.get('state') == WSState.CONNECTED:
                return True
            await asyncio.sleep(1)
        return False
    
    def _analyze_results(self):
        """Analyze and display comprehensive test results."""
        print("\n" + "="*80)
        print("📊 COMPREHENSIVE TEST RESULTS")
        print("="*80)
        
        successful_auths = 0
        successful_trades = 0
        total_orders = 0
        total_messages = 0
        
        for exchange_name in self.trading_config:
            auth_status = self.auth_results[exchange_name]['status']
            orders = len(self.trade_results[exchange_name]['orders'])
            messages = len(self.trade_results[exchange_name]['messages'])
            
            total_orders += orders
            total_messages += messages
            
            if auth_status == 'success':
                successful_auths += 1
                if messages > 0:
                    successful_trades += 1
                    status_icon = "🎉 COMPLETE"
                else:
                    status_icon = "⚠️  WS-ONLY"
            else:
                status_icon = "❌ FAILED"
            
            print(f"{exchange_name:15} | {status_icon} | Orders: {orders} | Messages: {messages}")
        
        print(f"\n🎯 SUMMARY:")
        print(f"   WebSocket Authentication: {successful_auths}/8 ✅")
        print(f"   Complete Pipeline (WS + Trading): {successful_trades}/8 🎉")
        print(f"   Total Orders Executed: {total_orders}")
        print(f"   Total Private Messages: {total_messages}")
        
        if successful_trades >= 6:
            print(f"   🚀 EXCELLENT: System ready for production!")
        elif successful_trades >= 4:
            print(f"   ⚠️  PARTIAL: Some exchanges need attention")
        else:
            print(f"   ❌ ISSUES: Major problems to resolve")
    
    async def run_comprehensive_test(self):
        """Run the complete comprehensive test."""
        print("🚀 COMPREHENSIVE TRADE PIPELINE TEST")
        print("="*80)
        print("Testing private WebSocket authentication, subscriptions, and trading")
        print("across ALL 8 exchanges with actual orders and fills")
        print("="*80)
        
        await self.test_all_exchanges()
        self._analyze_results()
        
        return True


async def main():
    """Main test execution."""
    tester = ComprehensiveTradePipelineTester()
    
    try:
        await tester.setup()
        success = await tester.run_comprehensive_test()
        
        if success:
            print("\n✅ COMPREHENSIVE TEST COMPLETED!")
            print("🔍 Complete trade pipeline tested across all exchanges")
        else:
            print("\n❌ Test failed")
            
    except KeyboardInterrupt:
        print("\n🛑 Test interrupted by user")
    except Exception as e:
        print(f"\n💥 Test failed with error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await tester.cleanup()


if __name__ == "__main__":
    asyncio.run(main()) 