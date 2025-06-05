#!/usr/bin/env python
"""
Comprehensive WebSocket Trade Message Testing

Tests real-time trade execution notifications via websockets across all exchanges.
Phase-by-phase testing ensuring all exchanges pass before proceeding.
"""

import asyncio
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Set
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


class WebSocketTradeTestFramework:
    """Comprehensive WebSocket trade message testing framework."""
    
    def __init__(self):
        """Initialize the testing framework."""
        self.ws_manager = None
        self.connectors = {}
        self.connections = {}
        self.received_messages = {}
        self.executed_trades = {}
        self.test_results = {}
        
        # Test configuration with BERA as base asset
        self.trading_config = {
            'binance_spot': {
                'symbol': 'BERA/USDT',
                'amount': 2.2,
                'private_endpoint': 'wss://stream.binance.com:9443/ws',
                'channels': ['executionReport', 'outboundAccountPosition']
            },
            'binance_perp': {
                'symbol': 'BERA/USDT:USDT', 
                'amount': 0.1,
                'private_endpoint': 'wss://fstream.binance.com/ws',
                'channels': ['ORDER_TRADE_UPDATE', 'ACCOUNT_UPDATE']
            },
            'bybit_spot': {
                'symbol': 'BERA/USDT',
                'amount': 2.2,
                'private_endpoint': 'wss://stream.bybit.com/v5/private',
                'channels': ['execution', 'order']
            },
            'bybit_perp': {
                'symbol': 'BERA/USDT:USDT',
                'amount': 1.0,
                'private_endpoint': 'wss://stream.bybit.com/v5/private',
                'channels': ['execution', 'order', 'position']
            },
            'mexc_spot': {
                'symbol': 'BERA/USDT',
                'amount': 3.0,
                'private_endpoint': 'wss://wbs.mexc.com/ws',
                'channels': ['spot@private.orders.v3.api', 'spot@private.deals.v3.api']
            },
            'gateio_spot': {
                'symbol': 'BERA/USDT',
                'amount': 2.0,
                'private_endpoint': 'wss://api.gateio.ws/ws/v4/',
                'channels': ['spot.orders', 'spot.usertrades']
            },
            'bitget_spot': {
                'symbol': 'BERA/USDT',
                'amount': 1.0,
                'private_endpoint': 'wss://ws.bitget.com/spot/v1/stream',
                'channels': ['orders', 'fills']
            },
            'hyperliquid_perp': {
                'symbol': 'BERA/USDC:USDC',
                'amount': 0.1,
                'private_endpoint': 'wss://api.hyperliquid.xyz/ws',
                'channels': ['user', 'allMids']
            }
        }
        
        # Initialize message tracking
        for exchange in self.trading_config:
            self.received_messages[exchange] = []
            self.executed_trades[exchange] = []
            self.test_results[exchange] = {
                'connection': {'status': 'pending', 'error': None},
                'authentication': {'status': 'pending', 'error': None},
                'order_notifications': {'status': 'pending', 'orders': []},
                'trade_notifications': {'status': 'pending', 'trades': []}
            }
    
    async def setup(self):
        """Set up the testing environment."""
        print("🔧 Setting up WebSocket Trade Testing Framework...")
        
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
        self.ws_manager.register_handler(WSMessageType.TRADE, self._handle_trade_message)
        
        # Create connectors
        print("📡 Creating exchange connectors...")
        for exchange_name in self.trading_config:
            try:
                config = get_exchange_config(exchange_name)
                connector = create_exchange_connector(exchange_name.split('_')[0], config)
                
                # Connect to REST API
                connected = await connector.connect()
                if connected:
                    self.connectors[exchange_name] = connector
                    print(f"   ✅ {exchange_name} - REST connector ready")
                else:
                    print(f"   ❌ {exchange_name} - REST connection failed")
                    self.test_results[exchange_name]['connection']['status'] = 'failed'
                    self.test_results[exchange_name]['connection']['error'] = 'REST connection failed'
            except Exception as e:
                print(f"   ❌ {exchange_name} - connector creation failed: {e}")
                self.test_results[exchange_name]['connection']['status'] = 'failed'
                self.test_results[exchange_name]['connection']['error'] = str(e)
        
        print(f"✅ Setup complete - {len(self.connectors)}/8 exchanges ready")
    
    async def cleanup(self):
        """Clean up resources."""
        print("🧹 Cleaning up resources...")
        
        # Close WebSocket connections
        if self.ws_manager:
            await self.ws_manager.stop()
        
        # Close REST connections
        for connector in self.connectors.values():
            try:
                await connector.disconnect()
            except Exception as e:
                logger.debug(f"Error disconnecting connector: {e}")
        
        # Close database
        await close_db()
        
        print("✅ Cleanup completed")
    
    async def _handle_order_update(self, message: Dict[str, Any]):
        """Handle order update messages from websockets."""
        exchange = message.get('exchange')
        if exchange in self.test_results:
            self.test_results[exchange]['order_notifications']['orders'].append({
                'timestamp': time.time(),
                'message': message
            })
            logger.info(f"📨 {exchange} - Order update: {message.get('status', 'unknown')}")
    
    async def _handle_trade_message(self, message: Dict[str, Any]):
        """Handle trade messages from websockets."""
        exchange = message.get('exchange')
        if exchange in self.test_results:
            self.test_results[exchange]['trade_notifications']['trades'].append({
                'timestamp': time.time(),
                'message': message
            })
            logger.info(f"💰 {exchange} - Trade notification: {message.get('amount', 0)} @ {message.get('price', 0)}")
    
    # PHASE 1 TESTS
    
    async def test_phase_1_private_websocket_connections(self):
        """Phase 1.1: Test private websocket connections for all exchanges."""
        print("\n" + "="*80)
        print("🔐 PHASE 1.1: Private WebSocket Connections")
        print("="*80)
        
        connection_tasks = []
        
        for exchange_name, connector in self.connectors.items():
            task = asyncio.create_task(
                self._test_private_connection(exchange_name, connector)
            )
            connection_tasks.append(task)
        
        # Wait for all connections to complete
        await asyncio.gather(*connection_tasks, return_exceptions=True)
        
        # Evaluate results
        passed = 0
        for exchange_name in self.trading_config:
            if self.test_results[exchange_name]['connection']['status'] == 'success':
                passed += 1
        
        print(f"\n📊 Phase 1.1 Results: {passed}/8 exchanges connected to private websockets")
        
        if passed < 8:
            print("❌ Phase 1.1 FAILED - Not all exchanges connected")
            self._print_detailed_results()
            return False
        
        print("✅ Phase 1.1 PASSED - All exchanges connected")
        return True
    
    async def _test_private_connection(self, exchange_name: str, connector):
        """Test private websocket connection for a specific exchange."""
        print(f"   🔗 {exchange_name} - Testing private websocket connection...")
        
        try:
            config = self.trading_config[exchange_name]
            endpoint = config['private_endpoint']
            
            # Connect to private websocket
            conn_id = await self.ws_manager.connect_exchange(
                connector, endpoint, "private"
            )
            
            self.connections[exchange_name] = conn_id
            
            # Wait for connection to establish
            max_wait = 15
            for _ in range(max_wait):
                status = self.ws_manager.get_connection_status(conn_id)
                if status.get('state') == WSState.CONNECTED:
                    self.test_results[exchange_name]['connection']['status'] = 'success'
                    print(f"   ✅ {exchange_name} - Private websocket connected")
                    return True
                await asyncio.sleep(1)
            
            # Connection timeout
            self.test_results[exchange_name]['connection']['status'] = 'failed'
            self.test_results[exchange_name]['connection']['error'] = 'Connection timeout'
            print(f"   ❌ {exchange_name} - Connection timeout")
            return False
            
        except Exception as e:
            self.test_results[exchange_name]['connection']['status'] = 'failed'
            self.test_results[exchange_name]['connection']['error'] = str(e)
            print(f"   ❌ {exchange_name} - Connection error: {e}")
            return False
    
    async def test_phase_1_authentication(self):
        """Phase 1.2: Test websocket authentication and subscription."""
        print("\n" + "="*80)
        print("🔑 PHASE 1.2: WebSocket Authentication & Subscriptions")
        print("="*80)
        
        auth_tasks = []
        
        for exchange_name in self.connections:
            task = asyncio.create_task(
                self._test_authentication(exchange_name)
            )
            auth_tasks.append(task)
        
        # Wait for all authentication tests
        await asyncio.gather(*auth_tasks, return_exceptions=True)
        
        # Evaluate results
        passed = 0
        for exchange_name in self.connections:
            if self.test_results[exchange_name]['authentication']['status'] == 'success':
                passed += 1
        
        print(f"\n📊 Phase 1.2 Results: {passed}/{len(self.connections)} exchanges authenticated")
        
        if passed < len(self.connections):
            print("❌ Phase 1.2 FAILED - Not all exchanges authenticated")
            self._print_detailed_results()
            return False
        
        print("✅ Phase 1.2 PASSED - All exchanges authenticated")
        return True
    
    async def _test_authentication(self, exchange_name: str):
        """Test authentication for private websocket."""
        print(f"   🔐 {exchange_name} - Testing authentication...")
        
        try:
            config = self.trading_config[exchange_name]
            conn_id = self.connections[exchange_name]
            
            # Subscribe to private channels based on exchange
            for channel in config['channels']:
                success = await self.ws_manager.subscribe(
                    conn_id, channel, config['symbol']
                )
                if not success:
                    logger.warning(f"{exchange_name} - Failed to subscribe to {channel}")
            
            # Wait for authentication confirmation (varies by exchange)
            await asyncio.sleep(3)
            
            # Check connection status
            status = self.ws_manager.get_connection_status(conn_id)
            if status.get('state') == WSState.CONNECTED:
                self.test_results[exchange_name]['authentication']['status'] = 'success'
                print(f"   ✅ {exchange_name} - Authentication successful")
                return True
            else:
                self.test_results[exchange_name]['authentication']['status'] = 'failed'
                self.test_results[exchange_name]['authentication']['error'] = 'Not connected after auth'
                print(f"   ❌ {exchange_name} - Authentication failed")
                return False
                
        except Exception as e:
            self.test_results[exchange_name]['authentication']['status'] = 'failed'
            self.test_results[exchange_name]['authentication']['error'] = str(e)
            print(f"   ❌ {exchange_name} - Authentication error: {e}")
            return False
    
    async def test_phase_1_trade_execution_notifications(self):
        """Phase 1.3: Test trade execution websocket notifications."""
        print("\n" + "="*80)
        print("💰 PHASE 1.3: Trade Execution WebSocket Notifications")
        print("="*80)
        
        # Execute trades and monitor websocket notifications
        trade_tasks = []
        
        for exchange_name in self.connections:
            task = asyncio.create_task(
                self._test_trade_notifications(exchange_name)
            )
            trade_tasks.append(task)
        
        # Wait for all trade notification tests
        await asyncio.gather(*trade_tasks, return_exceptions=True)
        
        # Evaluate results
        passed = 0
        for exchange_name in self.connections:
            order_status = self.test_results[exchange_name]['order_notifications']['status']
            trade_status = self.test_results[exchange_name]['trade_notifications']['status']
            
            if order_status == 'success' and trade_status == 'success':
                passed += 1
        
        print(f"\n📊 Phase 1.3 Results: {passed}/{len(self.connections)} exchanges received trade notifications")
        
        if passed < len(self.connections):
            print("❌ Phase 1.3 FAILED - Not all exchanges received notifications")
            self._print_detailed_results()
            return False
        
        print("✅ Phase 1.3 PASSED - All exchanges received trade notifications")
        return True
    
    async def _test_trade_notifications(self, exchange_name: str):
        """Test trade execution notifications for an exchange."""
        print(f"   📈 {exchange_name} - Testing trade notifications...")
        
        try:
            connector = self.connectors[exchange_name]
            config = self.trading_config[exchange_name]
            symbol = config['symbol']
            amount = Decimal(str(config['amount']))
            
            # Clear previous messages
            self.received_messages[exchange_name] = []
            
            # Get market price for limit order
            orderbook = await connector.get_orderbook(symbol, limit=5)
            if not orderbook or not orderbook.get('bids') or not orderbook.get('asks'):
                raise Exception("Unable to get market data")
            
            bid_price = Decimal(str(orderbook['bids'][0][0]))
            ask_price = Decimal(str(orderbook['asks'][0][0]))
            mid_price = (bid_price + ask_price) / 2
            
            # Place BUY order
            print(f"     🔵 {exchange_name} - Placing BUY order for {amount} {symbol}")
            buy_order = await connector.place_order(
                symbol=symbol,
                side='buy',
                amount=amount,
                price=bid_price,  # Use bid price for quick fill
                order_type=OrderType.LIMIT
            )
            
            if buy_order and buy_order.get('id'):
                self.executed_trades[exchange_name].append(buy_order)
                print(f"     ✅ {exchange_name} - BUY order placed: {buy_order['id']}")
                
                # Wait for websocket notifications
                await asyncio.sleep(5)
                
                # Check for order notifications
                order_messages = self.test_results[exchange_name]['order_notifications']['orders']
                if len(order_messages) > 0:
                    self.test_results[exchange_name]['order_notifications']['status'] = 'success'
                    print(f"     📨 {exchange_name} - Received {len(order_messages)} order notifications")
                else:
                    self.test_results[exchange_name]['order_notifications']['status'] = 'failed'
                    print(f"     ❌ {exchange_name} - No order notifications received")
                
                # Check for trade notifications
                trade_messages = self.test_results[exchange_name]['trade_notifications']['trades']
                if len(trade_messages) > 0:
                    self.test_results[exchange_name]['trade_notifications']['status'] = 'success'
                    print(f"     💰 {exchange_name} - Received {len(trade_messages)} trade notifications")
                else:
                    self.test_results[exchange_name]['trade_notifications']['status'] = 'failed'
                    print(f"     ❌ {exchange_name} - No trade notifications received")
                
                # Place SELL order to close position
                await asyncio.sleep(2)
                print(f"     🔴 {exchange_name} - Placing SELL order to close position")
                
                # Get current balance for sell amount
                balance = await connector.get_balance(symbol.split('/')[0])
                if balance:
                    sell_amount = min(amount, list(balance.values())[0] * Decimal('0.95'))
                    
                    sell_order = await connector.place_order(
                        symbol=symbol,
                        side='sell',
                        amount=sell_amount,
                        price=ask_price,  # Use ask price for quick fill
                        order_type=OrderType.LIMIT
                    )
                    
                    if sell_order and sell_order.get('id'):
                        self.executed_trades[exchange_name].append(sell_order)
                        print(f"     ✅ {exchange_name} - SELL order placed: {sell_order['id']}")
                    
                    # Wait for additional notifications
                    await asyncio.sleep(3)
            
            else:
                raise Exception("Failed to place buy order")
                
        except Exception as e:
            self.test_results[exchange_name]['order_notifications']['status'] = 'failed'
            self.test_results[exchange_name]['trade_notifications']['status'] = 'failed'
            print(f"     ❌ {exchange_name} - Trade notification test failed: {e}")
    
    def _print_detailed_results(self):
        """Print detailed test results for debugging."""
        print("\n" + "="*80)
        print("📋 DETAILED TEST RESULTS")
        print("="*80)
        
        for exchange_name, results in self.test_results.items():
            print(f"\n{exchange_name}:")
            print(f"  Connection: {results['connection']['status']}")
            if results['connection']['error']:
                print(f"    Error: {results['connection']['error']}")
            
            print(f"  Authentication: {results['authentication']['status']}")
            if results['authentication']['error']:
                print(f"    Error: {results['authentication']['error']}")
            
            print(f"  Order Notifications: {results['order_notifications']['status']}")
            print(f"    Messages: {len(results['order_notifications']['orders'])}")
            
            print(f"  Trade Notifications: {results['trade_notifications']['status']}")
            print(f"    Messages: {len(results['trade_notifications']['trades'])}")
    
    async def run_phase_1(self):
        """Run complete Phase 1 testing."""
        print("🚀 Starting Phase 1: Trade Execution WebSocket Integration")
        
        # Test 1.1: Private WebSocket Connections
        if not await self.test_phase_1_private_websocket_connections():
            return False
        
        # Test 1.2: Authentication
        if not await self.test_phase_1_authentication():
            return False
        
        # Test 1.3: Trade Execution Notifications
        if not await self.test_phase_1_trade_execution_notifications():
            return False
        
        print("\n🎉 PHASE 1 COMPLETED SUCCESSFULLY!")
        print("All exchanges are ready for real-time trade notifications")
        
        return True


async def main():
    """Main test execution."""
    framework = WebSocketTradeTestFramework()
    
    try:
        await framework.setup()
        
        # Run Phase 1
        success = await framework.run_phase_1()
        
        if success:
            print("\n✅ Phase 1 PASSED - Ready to proceed to Phase 2")
        else:
            print("\n❌ Phase 1 FAILED - Fix issues before proceeding")
            
    except KeyboardInterrupt:
        print("\n🛑 Test interrupted by user")
    except Exception as e:
        print(f"\n💥 Test failed with error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await framework.cleanup()


if __name__ == "__main__":
    asyncio.run(main()) 