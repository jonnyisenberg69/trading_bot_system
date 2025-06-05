#!/usr/bin/env python
"""
Focused Private WebSocket Authentication Test

Tests private WebSocket authentication and message parsing
on 2 exchanges with minimal trading to debug the authentication flow.
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


class FocusedWebSocketTester:
    """Focused private WebSocket authentication tester."""
    
    def __init__(self):
        """Initialize the tester."""
        self.ws_manager = None
        self.connectors = {}
        self.connections = {}
        self.private_messages = {
            'order_updates': {},
            'trade_notifications': {},
            'raw_messages': {}
        }
        
        # Test only 2 exchanges with small amounts
        self.trading_config = {
            'mexc_spot': {
                'symbol': 'BERA/USDT',
                'amount': 1.0,  # Small amount for testing
                'private_channels': ['spot@private.orders.v3.api', 'spot@private.deals.v3.api']
            },
            'binance_spot': {
                'symbol': 'BERA/USDT',
                'amount': 1.0,  # Small amount for testing
                'private_channels': ['executionReport', 'outboundAccountPosition']
            }
        }
        
        # Initialize message tracking
        for exchange in self.trading_config:
            for msg_type in self.private_messages:
                self.private_messages[msg_type][exchange] = []
    
    async def setup(self):
        """Set up the testing environment."""
        print("🔍 Setting up Focused Private WebSocket Test...")
        
        # Initialize database
        await init_db()
        
        # Create WebSocket manager with detailed logging
        self.ws_manager = WebSocketManager({
            'ping_interval': 30.0,
            'pong_timeout': 15.0,
            'reconnect_enabled': True
        })
        await self.ws_manager.start()
        
        # Register message handlers with raw message logging
        self.ws_manager.register_handler(WSMessageType.ORDER_UPDATE, self._handle_order_update)
        self.ws_manager.register_handler(WSMessageType.TRADE, self._handle_trade_notification)
        self.ws_manager.register_handler(WSMessageType.HEARTBEAT, self._handle_heartbeat)
        
        # Create connectors for test exchanges
        print("📡 Creating connectors for focused test...")
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
        
        print(f"✅ Setup complete - {len(self.connectors)}/2 exchanges ready")
    
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
        """Handle order update messages with detailed logging."""
        exchange = message.get('exchange')
        if exchange in self.private_messages['order_updates']:
            self.private_messages['order_updates'][exchange].append(message)
            print(f"📨 {exchange} - ORDER UPDATE: {message.get('status', 'unknown')} | "
                  f"ID: {message.get('id', 'N/A')} | "
                  f"Symbol: {message.get('symbol', 'N/A')}")
            print(f"    Raw: {json.dumps(message.get('raw', {}), indent=2)[:200]}...")
    
    async def _handle_trade_notification(self, message: Dict[str, Any]):
        """Handle trade notifications with detailed logging."""
        exchange = message.get('exchange')
        if exchange in self.private_messages['trade_notifications']:
            self.private_messages['trade_notifications'][exchange].append(message)
            print(f"💰 {exchange} - TRADE: {message.get('amount', 0)} @ {message.get('price', 0)} | "
                  f"Side: {message.get('side', 'N/A')}")
            print(f"    Raw: {json.dumps(message.get('raw', {}), indent=2)[:200]}...")
    
    async def _handle_heartbeat(self, message: Dict[str, Any]):
        """Handle heartbeat/auth messages with detailed logging."""
        exchange = message.get('exchange')
        print(f"💓 {exchange} - HEARTBEAT/AUTH: {message.get('type', 'unknown')}")
        if exchange in self.private_messages['raw_messages']:
            self.private_messages['raw_messages'][exchange].append(message)
    
    async def test_private_authentication(self):
        """Test private WebSocket authentication in detail."""
        print("\n" + "="*60)
        print("🔐 TESTING PRIVATE WEBSOCKET AUTHENTICATION")
        print("="*60)
        
        # Connect to private websockets
        for exchange_name, connector in self.connectors.items():
            print(f"\n📡 Connecting {exchange_name} to private WebSocket...")
            
            try:
                # Connect to private websocket
                private_endpoint = self._get_private_endpoint(exchange_name)
                print(f"   Endpoint: {private_endpoint}")
                
                conn_id = await self.ws_manager.connect_exchange(
                    connector, private_endpoint, "private"
                )
                
                self.connections[exchange_name] = conn_id
                
                # Wait for connection
                connected = await self._wait_for_connection(conn_id, 10)
                
                if connected:
                    print(f"   ✅ {exchange_name} - Connected to private WebSocket")
                    
                    # Monitor for authentication messages
                    print(f"   ⏳ Monitoring for 10 seconds for authentication messages...")
                    await asyncio.sleep(10)
                    
                    # Check if we received any messages
                    raw_count = len(self.private_messages['raw_messages'].get(exchange_name, []))
                    print(f"   📊 {exchange_name} - Received {raw_count} raw messages during connection")
                    
                else:
                    print(f"   ❌ {exchange_name} - Connection timeout")
                    
            except Exception as e:
                print(f"   ❌ {exchange_name} - Connection error: {e}")
    
    async def test_minimal_trading(self):
        """Test with one very small trade per exchange."""
        print("\n" + "="*60)
        print("💰 TESTING WITH MINIMAL TRADING")
        print("="*60)
        
        for exchange_name in self.connections:
            if exchange_name not in self.connectors:
                continue
                
            print(f"\n🔍 Testing {exchange_name} with minimal trade...")
            
            try:
                connector = self.connectors[exchange_name]
                config = self.trading_config[exchange_name]
                symbol = config['symbol']
                amount = Decimal(str(config['amount']))
                
                # Get current market price
                orderbook = await connector.get_orderbook(symbol, limit=5)
                if not orderbook or not orderbook.get('bids'):
                    print(f"   ❌ {exchange_name} - No market data available")
                    continue
                
                bid_price = Decimal(str(orderbook['bids'][0][0]))
                
                # Clear previous messages
                for msg_type in self.private_messages:
                    self.private_messages[msg_type][exchange_name] = []
                
                # Place ONE small order
                print(f"   🔵 {exchange_name} - Placing 1 small BUY order: {amount} BERA @ {bid_price}")
                
                buy_order = await connector.place_order(
                    symbol=symbol,
                    side='buy',
                    amount=amount,
                    price=bid_price,
                    order_type=OrderType.LIMIT
                )
                
                if buy_order and buy_order.get('id'):
                    print(f"   ✅ {exchange_name} - Order placed: {buy_order['id']}")
                    
                    # Monitor for WebSocket messages
                    print(f"   ⏳ Monitoring for 15 seconds for private messages...")
                    start_time = time.time()
                    
                    while time.time() - start_time < 15:
                        await asyncio.sleep(1)
                        
                        order_count = len(self.private_messages['order_updates'][exchange_name])
                        trade_count = len(self.private_messages['trade_notifications'][exchange_name])
                        raw_count = len(self.private_messages['raw_messages'][exchange_name])
                        
                        if order_count > 0 or trade_count > 0:
                            print(f"   🎉 {exchange_name} - Received messages! Orders: {order_count}, Trades: {trade_count}")
                            break
                        
                        if raw_count > 0:
                            print(f"   📡 {exchange_name} - Raw messages: {raw_count}")
                    
                    # Final count
                    order_updates = len(self.private_messages['order_updates'][exchange_name])
                    trade_notifications = len(self.private_messages['trade_notifications'][exchange_name])
                    raw_messages = len(self.private_messages['raw_messages'][exchange_name])
                    
                    print(f"   📊 {exchange_name} - Final: {order_updates} orders, {trade_notifications} trades, {raw_messages} raw")
                    
                    # Cancel the order
                    try:
                        await connector.cancel_order(buy_order['id'], symbol)
                        print(f"   🔴 {exchange_name} - Order cancelled")
                        await asyncio.sleep(3)  # Wait for cancellation message
                    except Exception as e:
                        print(f"   ⚠️  {exchange_name} - Cancel error: {e}")
                        
                else:
                    print(f"   ❌ {exchange_name} - Failed to place order")
                    
            except Exception as e:
                print(f"   ❌ {exchange_name} - Test error: {e}")
    
    def _get_private_endpoint(self, exchange_name: str) -> str:
        """Get private WebSocket endpoint for exchange."""
        endpoints = {
            'mexc_spot': 'wss://wbs.mexc.com/ws',
            'binance_spot': 'wss://stream.binance.com:9443/ws'
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
        """Analyze the authentication test results."""
        print("\n" + "="*60)
        print("📊 AUTHENTICATION TEST RESULTS")
        print("="*60)
        
        for exchange_name in self.trading_config:
            order_count = len(self.private_messages['order_updates'].get(exchange_name, []))
            trade_count = len(self.private_messages['trade_notifications'].get(exchange_name, []))
            raw_count = len(self.private_messages['raw_messages'].get(exchange_name, []))
            
            status = "✅ WORKING" if (order_count > 0 or trade_count > 0) else "❌ NO PRIVATE MESSAGES"
            
            print(f"{exchange_name:12} | Orders: {order_count:2d} | Trades: {trade_count:2d} | Raw: {raw_count:2d} | {status}")
        
        # Show authentication recommendations
        print(f"\n🔧 NEXT STEPS:")
        print("1. Most exchanges require explicit authentication for private WebSocket channels")
        print("2. We need to implement listen keys (Binance) or API signature authentication")
        print("3. Current implementation connects but doesn't authenticate properly")
    
    async def run_focused_test(self):
        """Run the focused authentication test."""
        print("🎯 FOCUSED PRIVATE WEBSOCKET AUTHENTICATION TEST")
        print("="*60)
        print("Testing 2 exchanges with minimal trading to debug authentication")
        print("="*60)
        
        # Test 1: Authentication
        await self.test_private_authentication()
        
        if not self.connections:
            print("❌ No connections established - cannot proceed")
            return False
        
        # Test 2: Minimal trading
        await self.test_minimal_trading()
        
        # Test 3: Analysis
        self._analyze_results()
        
        return True


async def main():
    """Main test execution."""
    tester = FocusedWebSocketTester()
    
    try:
        await tester.setup()
        success = await tester.run_focused_test()
        
        if success:
            print("\n🎯 FOCUSED TEST COMPLETED!")
            print("Check results above for authentication insights")
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