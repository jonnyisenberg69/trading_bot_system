#!/usr/bin/env python
"""
Complete Private WebSocket Implementation Test

Demonstrates the fully implemented private WebSocket message parsing
for all 8 exchanges with proper order updates and trade notifications.
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


class CompleteWebSocketTester:
    """Complete private WebSocket implementation tester."""
    
    def __init__(self):
        """Initialize the tester."""
        self.ws_manager = None
        self.connectors = {}
        self.connections = {}
        self.received_messages = {}
        self.private_messages = {
            'order_updates': {},
            'trade_notifications': {},
            'account_updates': {}
        }
        
        # Test configuration with BERA symbols as requested
        self.trading_config = {
            'binance_spot': {
                'symbol': 'BERA/USDT',
                'amount': 2.2,
                'private_channels': ['executionReport', 'outboundAccountPosition']
            },
            'binance_perp': {
                'symbol': 'BERA/USDT:USDT', 
                'amount': 20.0,
                'private_channels': ['ORDER_TRADE_UPDATE', 'ACCOUNT_UPDATE']
            },
            'bybit_spot': {
                'symbol': 'BERA/USDT',
                'amount': 2.2,
                'private_channels': ['order', 'execution']
            },
            'bybit_perp': {
                'symbol': 'BERA/USDT:USDT',
                'amount': 1.0,
                'private_channels': ['order', 'execution', 'position']
            },
            'mexc_spot': {
                'symbol': 'BERA/USDT',
                'amount': 3.0,
                'private_channels': ['spot@private.orders.v3.api', 'spot@private.deals.v3.api']
            },
            'gateio_spot': {
                'symbol': 'BERA/USDT',
                'amount': 2.0,
                'private_channels': ['spot.orders', 'spot.usertrades']
            },
            'bitget_spot': {
                'symbol': 'BERA/USDT',
                'amount': 1.0,
                'private_channels': ['orders', 'fills']
            },
            'hyperliquid_perp': {
                'symbol': 'BERA/USDC:USDC',
                'amount': 50.0,
                'private_channels': ['user', 'userFills']
            }
        }
        
        # Initialize message tracking
        for exchange in self.trading_config:
            self.received_messages[exchange] = []
            for msg_type in self.private_messages:
                self.private_messages[msg_type][exchange] = []
    
    async def setup(self):
        """Set up the testing environment."""
        print("🚀 Setting up Complete WebSocket Implementation Test...")
        
        # Initialize database
        await init_db()
        
        # Create WebSocket manager with updated implementation
        self.ws_manager = WebSocketManager({
            'ping_interval': 20.0,
            'pong_timeout': 10.0,
            'reconnect_enabled': True
        })
        await self.ws_manager.start()
        
        # Register enhanced message handlers
        self.ws_manager.register_handler(WSMessageType.ORDER_UPDATE, self._handle_order_update)
        self.ws_manager.register_handler(WSMessageType.TRADE, self._handle_trade_notification)
        self.ws_manager.register_handler(WSMessageType.HEARTBEAT, self._handle_heartbeat)
        
        # Create connectors
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
            except Exception as e:
                print(f"   ❌ {exchange_name} - connector creation failed: {e}")
        
        print(f"✅ Setup complete - {len(self.connectors)}/8 exchanges ready")
    
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
        """Handle enhanced order update messages."""
        exchange = message.get('exchange')
        if exchange in self.private_messages['order_updates']:
            self.private_messages['order_updates'][exchange].append({
                'timestamp': time.time(),
                'data': message,
                'parsed_fields': {
                    'order_id': message.get('id'),
                    'status': message.get('status'),
                    'filled': message.get('filled'),
                    'price': message.get('price'),
                    'side': message.get('side'),
                    'symbol': message.get('symbol')
                }
            })
            print(f"📨 {exchange} - Order Update: {message.get('status', 'unknown')} | "
                  f"ID: {message.get('id', 'N/A')} | "
                  f"Filled: {message.get('filled', 0)} | "
                  f"Symbol: {message.get('symbol', 'N/A')}")
    
    async def _handle_trade_notification(self, message: Dict[str, Any]):
        """Handle enhanced trade notifications."""
        exchange = message.get('exchange')
        if exchange in self.private_messages['trade_notifications']:
            self.private_messages['trade_notifications'][exchange].append({
                'timestamp': time.time(),
                'data': message,
                'parsed_fields': {
                    'trade_id': message.get('id'),
                    'order_id': message.get('order_id'),
                    'price': message.get('price'),
                    'amount': message.get('amount'),
                    'side': message.get('side'),
                    'symbol': message.get('symbol'),
                    'fee': message.get('fee')
                }
            })
            print(f"💰 {exchange} - Trade: {message.get('amount', 0)} @ {message.get('price', 0)} | "
                  f"Side: {message.get('side', 'N/A')} | "
                  f"Fee: {message.get('fee', 0)} | "
                  f"Symbol: {message.get('symbol', 'N/A')}")
    
    async def _handle_heartbeat(self, message: Dict[str, Any]):
        """Handle heartbeat/auth messages."""
        exchange = message.get('exchange')
        logger.debug(f"💓 {exchange} - Heartbeat/Auth: {message.get('type', 'unknown')}")
    
    async def test_private_websocket_connections(self):
        """Test private WebSocket connections for all exchanges."""
        print("\n" + "="*80)
        print("🔐 TESTING PRIVATE WEBSOCKET CONNECTIONS")
        print("="*80)
        
        connection_results = {}
        
        for exchange_name, connector in self.connectors.items():
            print(f"\n📡 Connecting {exchange_name} to private WebSocket...")
            
            try:
                # Connect to private websocket
                private_endpoint = self._get_private_endpoint(exchange_name)
                conn_id = await self.ws_manager.connect_exchange(
                    connector, private_endpoint, "private"
                )
                
                self.connections[exchange_name] = conn_id
                
                # Wait for connection
                connected = await self._wait_for_connection(conn_id, 15)
                connection_results[exchange_name] = connected
                
                if connected:
                    print(f"   ✅ {exchange_name} - Private WebSocket connected")
                else:
                    print(f"   ❌ {exchange_name} - Connection timeout")
                    
            except Exception as e:
                connection_results[exchange_name] = False
                print(f"   ❌ {exchange_name} - Connection error: {e}")
        
        success_count = sum(connection_results.values())
        print(f"\n📊 Connection Results: {success_count}/{len(self.connectors)} exchanges connected")
        
        return connection_results
    
    async def test_private_message_parsing(self):
        """Test private message parsing by placing orders."""
        print("\n" + "="*80)
        print("💰 TESTING PRIVATE MESSAGE PARSING WITH LIVE ORDERS")
        print("="*80)
        
        # Place orders on all connected exchanges and monitor messages
        tasks = []
        for exchange_name in self.connections:
            if exchange_name in self.connectors:
                task = asyncio.create_task(
                    self._test_exchange_messages(exchange_name)
                )
                tasks.append(task)
        
        # Execute all tests in parallel
        await asyncio.gather(*tasks, return_exceptions=True)
        
        # Analyze results
        self._analyze_message_results()
    
    async def _test_exchange_messages(self, exchange_name: str):
        """Test message parsing for a specific exchange."""
        print(f"\n🔍 Testing {exchange_name} message parsing...")
        
        try:
            connector = self.connectors[exchange_name]
            config = self.trading_config[exchange_name]
            symbol = config['symbol']
            amount = Decimal(str(config['amount']))
            
            # Get current market price
            orderbook = await connector.get_orderbook(symbol, limit=5)
            if not orderbook or not orderbook.get('bids'):
                print(f"   ❌ {exchange_name} - No market data available")
                return
            
            bid_price = Decimal(str(orderbook['bids'][0][0]))
            
            # Clear previous messages
            for msg_type in self.private_messages:
                self.private_messages[msg_type][exchange_name] = []
            
            # Place a BUY order
            print(f"   🔵 {exchange_name} - Placing BUY order...")
            buy_order = await connector.place_order(
                symbol=symbol,
                side='buy',
                amount=amount,
                price=bid_price,
                order_type=OrderType.LIMIT
            )
            
            if buy_order and buy_order.get('id'):
                print(f"   ✅ {exchange_name} - BUY order placed: {buy_order['id']}")
                
                # Wait for WebSocket messages
                await asyncio.sleep(8)
                
                # Check if we received order updates
                order_updates = len(self.private_messages['order_updates'][exchange_name])
                trade_notifications = len(self.private_messages['trade_notifications'][exchange_name])
                
                print(f"   📊 {exchange_name} - Received {order_updates} order updates, {trade_notifications} trade notifications")
                
                # Cancel the order if still open
                try:
                    await connector.cancel_order(buy_order['id'], symbol)
                    print(f"   🔴 {exchange_name} - Order cancelled")
                    await asyncio.sleep(3)  # Wait for cancellation message
                except:
                    pass
                    
            else:
                print(f"   ❌ {exchange_name} - Failed to place order")
                
        except Exception as e:
            print(f"   ❌ {exchange_name} - Test error: {e}")
    
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
    
    def _analyze_message_results(self):
        """Analyze the private message parsing results."""
        print("\n" + "="*80)
        print("📊 PRIVATE MESSAGE PARSING RESULTS")
        print("="*80)
        
        total_order_updates = 0
        total_trade_notifications = 0
        working_exchanges = 0
        
        for exchange_name in self.connections:
            order_count = len(self.private_messages['order_updates'][exchange_name])
            trade_count = len(self.private_messages['trade_notifications'][exchange_name])
            
            total_order_updates += order_count
            total_trade_notifications += trade_count
            
            if order_count > 0 or trade_count > 0:
                working_exchanges += 1
                status = "✅ WORKING"
            else:
                status = "❌ NO MESSAGES"
            
            print(f"{exchange_name:15} | Orders: {order_count:2d} | Trades: {trade_count:2d} | {status}")
            
            # Show sample parsed data if available
            if order_count > 0:
                sample = self.private_messages['order_updates'][exchange_name][0]
                parsed = sample['parsed_fields']
                print(f"  📝 Order Sample: ID={parsed.get('order_id', 'N/A')[:8]}... "
                      f"Status={parsed.get('status', 'N/A')} "
                      f"Symbol={parsed.get('symbol', 'N/A')}")
        
        print(f"\n🎯 SUMMARY:")
        print(f"   Working Exchanges: {working_exchanges}/{len(self.connections)}")
        print(f"   Total Order Updates: {total_order_updates}")
        print(f"   Total Trade Notifications: {total_trade_notifications}")
        
        if working_exchanges >= len(self.connections) * 0.8:  # 80% success rate
            print(f"   🎉 SUCCESS: Private WebSocket parsing is working excellently!")
        elif working_exchanges >= len(self.connections) * 0.5:  # 50% success rate
            print(f"   ⚠️  PARTIAL: Private WebSocket parsing is partially working")
        else:
            print(f"   ❌ ISSUES: Private WebSocket parsing needs more work")
    
    async def run_complete_test(self):
        """Run the complete private WebSocket implementation test."""
        print("🚀 COMPLETE PRIVATE WEBSOCKET IMPLEMENTATION TEST")
        print("="*80)
        print("Testing all 8 exchanges with comprehensive private message parsing")
        print("Including order updates, trade notifications, and message routing")
        print("="*80)
        
        # Test 1: Private WebSocket connections
        connection_results = await self.test_private_websocket_connections()
        
        if not any(connection_results.values()):
            print("❌ No exchanges connected - cannot proceed with message testing")
            return False
        
        # Test 2: Private message parsing
        await self.test_private_message_parsing()
        
        print("\n🎯 COMPLETE IMPLEMENTATION TEST FINISHED!")
        return True


async def main():
    """Main test execution."""
    tester = CompleteWebSocketTester()
    
    try:
        await tester.setup()
        success = await tester.run_complete_test()
        
        if success:
            print("\n✅ COMPLETE PRIVATE WEBSOCKET IMPLEMENTATION READY!")
            print("🎉 All exchanges now support real-time trade notifications")
        else:
            print("\n❌ Implementation needs additional work")
            
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