#!/usr/bin/env python
"""
Complete Private WebSocket Authentication Test

Tests the newly implemented private WebSocket authentication
for all 8 exchanges with proper authentication flows.
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


class AuthenticationTester:
    """Test private WebSocket authentication implementation."""
    
    def __init__(self):
        """Initialize the tester."""
        self.ws_manager = None
        self.connectors = {}
        self.connections = {}
        self.auth_results = {}
        self.private_messages = {
            'order_updates': {},
            'trade_notifications': {},
            'heartbeat_messages': {}
        }
        
        # Test configuration with proper amounts that meet minimum notional requirements
        self.trading_config = {
            'binance_spot': {
                'symbol': 'BERA/USDT',
                'amount': 2.5,  # ~$6.50 (above $5 minimum)
                'test_auth': True
            },
            'binance_perp': {
                'symbol': 'BERA/USDT:USDT', 
                'amount': 2.5,  # ~$6.50 (above $5 minimum)
                'test_auth': True
            },
            'bybit_spot': {
                'symbol': 'BERA/USDT',
                'amount': 2.0,  # ~$5.20 (above $1 minimum)
                'test_auth': True
            },
            'bybit_perp': {
                'symbol': 'BERA/USDT:USDT',
                'amount': 2.0,  # ~$5.20 (above $1 minimum)
                'test_auth': True
            },
            'mexc_spot': {
                'symbol': 'BERA/USDT',
                'amount': 2.0,  # ~$5.20 (above $1 minimum)
                'test_auth': True
            },
            'gateio_spot': {
                'symbol': 'BERA/USDT',
                'amount': 2.0,  # ~$5.20 (above $1 minimum)
                'test_auth': True
            },
            'bitget_spot': {
                'symbol': 'BERA/USDT',
                'amount': 2.0,  # ~$5.20 (above $1 minimum)
                'test_auth': True
            },
            'hyperliquid_perp': {
                'symbol': 'BERA/USDC:USDC',
                'amount': 4.0,  # ~$10.40 (above $10 minimum)
                'test_auth': True
            }
        }
        
        # Initialize tracking
        for exchange in self.trading_config:
            self.auth_results[exchange] = {'status': 'pending', 'error': None}
            for msg_type in self.private_messages:
                self.private_messages[msg_type][exchange] = []
    
    async def setup(self):
        """Set up the testing environment."""
        print("🔐 Setting up Authentication Test...")
        
        # Initialize database
        await init_db()
        
        # Create WebSocket manager with authentication support
        self.ws_manager = WebSocketManager({
            'ping_interval': 30.0,
            'pong_timeout': 15.0,
            'reconnect_enabled': True
        })
        await self.ws_manager.start()
        
        # Register message handlers
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
        """Handle order update messages."""
        exchange = message.get('exchange')
        if exchange in self.private_messages['order_updates']:
            self.private_messages['order_updates'][exchange].append(message)
            print(f"📨 {exchange} - ORDER UPDATE: {message.get('status', 'unknown')} | ID: {message.get('id', 'N/A')}")
    
    async def _handle_trade_notification(self, message: Dict[str, Any]):
        """Handle trade notifications."""
        exchange = message.get('exchange')
        if exchange in self.private_messages['trade_notifications']:
            self.private_messages['trade_notifications'][exchange].append(message)
            print(f"💰 {exchange} - TRADE: {message.get('amount', 0)} @ {message.get('price', 0)}")
    
    async def _handle_heartbeat(self, message: Dict[str, Any]):
        """Handle heartbeat/auth messages."""
        exchange = message.get('exchange')
        if exchange in self.private_messages['heartbeat_messages']:
            self.private_messages['heartbeat_messages'][exchange].append(message)
            logger.debug(f"💓 {exchange} - Heartbeat/Auth message")
    
    async def test_authentication(self):
        """Test private WebSocket authentication for all exchanges."""
        print("\n" + "="*80)
        print("🔐 TESTING PRIVATE WEBSOCKET AUTHENTICATION")
        print("="*80)
        
        # Test authentication on all exchanges
        auth_tasks = []
        for exchange_name, connector in self.connectors.items():
            task = asyncio.create_task(
                self._test_exchange_authentication(exchange_name, connector)
            )
            auth_tasks.append(task)
        
        # Wait for all authentication tests
        await asyncio.gather(*auth_tasks, return_exceptions=True)
        
        # Analyze authentication results
        self._analyze_authentication_results()
    
    async def _test_exchange_authentication(self, exchange_name: str, connector):
        """Test authentication for a specific exchange."""
        print(f"\n🔐 Testing {exchange_name} authentication...")
        
        try:
            # Connect to private WebSocket with authentication
            private_endpoint = self._get_private_endpoint(exchange_name)
            
            conn_id = await self.ws_manager.connect_exchange(
                connector, private_endpoint, "private"
            )
            
            if not conn_id:
                self.auth_results[exchange_name]['status'] = 'failed'
                self.auth_results[exchange_name]['error'] = 'Failed to prepare authentication'
                print(f"   ❌ {exchange_name} - Authentication preparation failed")
                return
            
            self.connections[exchange_name] = conn_id
            
            # Wait for connection and authentication
            connected = await self._wait_for_connection(conn_id, 15)
            
            if connected:
                self.auth_results[exchange_name]['status'] = 'success'
                print(f"   ✅ {exchange_name} - Private WebSocket authenticated")
                
                # Monitor for messages for a few seconds
                print(f"   ⏳ Monitoring for 10 seconds for private messages...")
                await asyncio.sleep(10)
                
                # Check if we received any private messages
                order_count = len(self.private_messages['order_updates'].get(exchange_name, []))
                trade_count = len(self.private_messages['trade_notifications'].get(exchange_name, []))
                heartbeat_count = len(self.private_messages['heartbeat_messages'].get(exchange_name, []))
                
                print(f"   📊 {exchange_name} - Messages: {order_count} orders, {trade_count} trades, {heartbeat_count} heartbeats")
                
            else:
                self.auth_results[exchange_name]['status'] = 'failed'
                self.auth_results[exchange_name]['error'] = 'Connection timeout'
                print(f"   ❌ {exchange_name} - Connection timeout")
                
        except Exception as e:
            self.auth_results[exchange_name]['status'] = 'failed'
            self.auth_results[exchange_name]['error'] = str(e)
            print(f"   ❌ {exchange_name} - Authentication error: {e}")
    
    async def test_with_minimal_trading(self):
        """Test with minimal trading to verify private message reception."""
        print("\n" + "="*80)
        print("💰 TESTING WITH MINIMAL TRADING")
        print("="*80)
        
        # Test with one small order on authenticated exchanges
        successful_auths = [name for name, result in self.auth_results.items() 
                          if result['status'] == 'success']
        
        if not successful_auths:
            print("❌ No successfully authenticated exchanges for trading test")
            return
        
        # Test on first successfully authenticated exchange only (minimal trading)
        test_exchange = successful_auths[0]
        print(f"\n🔍 Testing minimal trading on {test_exchange}...")
        
        try:
            connector = self.connectors[test_exchange]
            config = self.trading_config[test_exchange]
            symbol = config['symbol']
            amount = Decimal(str(config['amount']))  # Use configured amount that meets minimum requirements
            
            # Get current market price
            orderbook = await connector.get_orderbook(symbol, limit=5)
            if not orderbook or not orderbook.get('bids'):
                print(f"   ❌ {test_exchange} - No market data available")
                return
            
            bid_price = Decimal(str(orderbook['bids'][0][0]))
            
            # Clear previous messages
            for msg_type in self.private_messages:
                self.private_messages[msg_type][test_exchange] = []
            
            # Place ONE order that meets minimum notional requirements
            print(f"   🔵 {test_exchange} - Placing BUY order: {amount} BERA @ {bid_price}")
            
            buy_order = await connector.place_order(
                symbol=symbol,
                side='buy',
                amount=amount,
                price=bid_price,
                order_type=OrderType.LIMIT
            )
            
            if buy_order and buy_order.get('id'):
                print(f"   ✅ {test_exchange} - Order placed: {buy_order['id']}")
                
                # Monitor for WebSocket messages
                print(f"   ⏳ Monitoring for 20 seconds for private messages...")
                start_time = time.time()
                
                while time.time() - start_time < 20:
                    await asyncio.sleep(2)
                    
                    order_count = len(self.private_messages['order_updates'][test_exchange])
                    trade_count = len(self.private_messages['trade_notifications'][test_exchange])
                    
                    if order_count > 0 or trade_count > 0:
                        print(f"   🎉 {test_exchange} - SUCCESS! Received private messages!")
                        print(f"       Orders: {order_count}, Trades: {trade_count}")
                        break
                    
                    print(f"   ⏳ Still monitoring... ({int(time.time() - start_time)}s)")
                
                # Final count
                order_updates = len(self.private_messages['order_updates'][test_exchange])
                trade_notifications = len(self.private_messages['trade_notifications'][test_exchange])
                
                if order_updates > 0 or trade_notifications > 0:
                    print(f"   🎉 AUTHENTICATION SUCCESS: Received {order_updates} order updates, {trade_notifications} trade notifications")
                else:
                    print(f"   ⚠️  No private messages received despite successful authentication")
                
                # Cancel the order
                try:
                    await connector.cancel_order(buy_order['id'], symbol)
                    print(f"   🔴 {test_exchange} - Order cancelled")
                    await asyncio.sleep(3)
                except Exception as e:
                    print(f"   ⚠️  {test_exchange} - Cancel error: {e}")
                    
            else:
                print(f"   ❌ {test_exchange} - Failed to place order")
                
        except Exception as e:
            print(f"   ❌ {test_exchange} - Trading test error: {e}")
    
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
    
    def _analyze_authentication_results(self):
        """Analyze authentication test results."""
        print("\n" + "="*80)
        print("📊 AUTHENTICATION RESULTS")
        print("="*80)
        
        successful_auths = 0
        failed_auths = 0
        
        for exchange_name, result in self.auth_results.items():
            status = result['status']
            if status == 'success':
                successful_auths += 1
                print(f"{exchange_name:15} | ✅ AUTHENTICATED")
            else:
                failed_auths += 1
                error = result.get('error', 'Unknown error')
                print(f"{exchange_name:15} | ❌ FAILED - {error}")
        
        print(f"\n🎯 SUMMARY:")
        print(f"   Successfully Authenticated: {successful_auths}/8")
        print(f"   Failed Authentication: {failed_auths}/8")
        
        if successful_auths >= 6:  # 75% success rate
            print(f"   🎉 EXCELLENT: Authentication implementation working well!")
        elif successful_auths >= 4:  # 50% success rate
            print(f"   ⚠️  PARTIAL: Some authentication issues to resolve")
        else:
            print(f"   ❌ ISSUES: Authentication needs significant work")
    
    async def run_authentication_test(self):
        """Run the complete authentication test."""
        print("🔐 PRIVATE WEBSOCKET AUTHENTICATION TEST")
        print("="*80)
        print("Testing complete authentication implementation for all 8 exchanges")
        print("="*80)
        
        # Test 1: Authentication
        await self.test_authentication()
        
        # Test 2: Minimal trading (if any exchanges authenticated)
        await self.test_with_minimal_trading()
        
        print("\n🎯 AUTHENTICATION TEST COMPLETED!")
        return True


async def main():
    """Main test execution."""
    tester = AuthenticationTester()
    
    try:
        await tester.setup()
        success = await tester.run_authentication_test()
        
        if success:
            print("\n✅ AUTHENTICATION TEST COMPLETED!")
            print("🔐 Private WebSocket authentication implementation tested")
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