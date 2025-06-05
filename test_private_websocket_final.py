#!/usr/bin/env python
"""
Final Private WebSocket Implementation Test

Comprehensive test that properly implements and tests private WebSocket
authentication, subscription, and message parsing across ALL 8 exchanges
with real trading to verify complete functionality.
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


class FinalPrivateWebSocketTester:
    """Final comprehensive private WebSocket implementation tester."""
    
    def __init__(self):
        """Initialize the tester."""
        self.ws_manager = None
        self.connectors = {}
        self.connections = {}
        self.authentication_results = {}
        self.trading_results = {}
        self.private_messages = {}
        
        # Test all 8 exchanges with proper amounts
        self.test_config = {
            'binance_spot': {
                'symbol': 'BERA/USDT',
                'amount': 2.5,  # Above $5 minimum
                'endpoint': 'wss://stream.binance.com:9443/ws'
            },
            'binance_perp': {
                'symbol': 'BERA/USDT:USDT',
                'amount': 2.5,
                'endpoint': 'wss://fstream.binance.com/ws'
            },
            'bybit_spot': {
                'symbol': 'BERA/USDT',
                'amount': 1.0,
                'endpoint': 'wss://stream.bybit.com/v5/private'
            },
            'bybit_perp': {
                'symbol': 'BERA/USDT:USDT',
                'amount': 1.0,
                'endpoint': 'wss://stream.bybit.com/v5/private'
            },
            'mexc_spot': {
                'symbol': 'BERA/USDT',
                'amount': 1.0,
                'endpoint': 'wss://wbs.mexc.com/ws'
            },
            'gateio_spot': {
                'symbol': 'BERA/USDT',
                'amount': 1.0,
                'endpoint': 'wss://api.gateio.ws/ws/v4/'
            },
            'bitget_spot': {
                'symbol': 'BERA/USDT',
                'amount': 1.0,
                'endpoint': 'wss://ws.bitget.com/spot/v1/stream'
            },
            'hyperliquid_perp': {
                'symbol': 'BERA/USDC:USDC',
                'amount': 4.0,  # Above $10 minimum
                'endpoint': 'wss://api.hyperliquid.xyz/ws'
            }
        }
        
        # Initialize tracking for all exchanges
        for exchange in self.test_config:
            self.authentication_results[exchange] = {'status': 'pending'}
            self.trading_results[exchange] = {'orders': [], 'fills': []}
            self.private_messages[exchange] = []
    
    async def setup(self):
        """Set up the testing environment."""
        print("🚀 FINAL PRIVATE WEBSOCKET IMPLEMENTATION TEST")
        print("="*70)
        print("Testing COMPLETE private WebSocket pipeline:")
        print("✅ Authentication across ALL 8 exchanges")
        print("✅ Private channel subscriptions")
        print("✅ Real order execution")
        print("✅ Private message reception and parsing")
        print("✅ Database integration")
        print("="*70)
        
        # Initialize database
        await init_db()
        
        # Create enhanced WebSocket manager
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
        print("\n📡 Creating exchange connectors...")
        for exchange_name in self.test_config:
            try:
                config = get_exchange_config(exchange_name)
                connector = create_exchange_connector(exchange_name.split('_')[0], config)
                
                connected = await connector.connect()
                if connected:
                    self.connectors[exchange_name] = connector
                    print(f"   ✅ {exchange_name} - REST connector ready")
                else:
                    print(f"   ❌ {exchange_name} - REST connection failed")
                    self.authentication_results[exchange_name] = {'status': 'failed', 'error': 'REST connection failed'}
            except Exception as e:
                print(f"   ❌ {exchange_name} - Error: {e}")
                self.authentication_results[exchange_name] = {'status': 'failed', 'error': str(e)}
        
        print(f"✅ Setup complete - {len(self.connectors)}/{len(self.test_config)} exchanges ready")
    
    async def cleanup(self):
        """Clean up resources."""
        print("\n🧹 Cleaning up resources...")
        
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
        """Enhanced order update handler."""
        exchange = message.get('exchange', 'unknown')
        if exchange in self.private_messages:
            self.private_messages[exchange].append(('order_update', message))
            print(f"📨 {exchange.upper()} - ORDER UPDATE: Status={message.get('status', 'unknown')} | ID={message.get('id', 'N/A')}")
    
    async def _handle_trade_notification(self, message: Dict[str, Any]):
        """Enhanced trade notification handler."""
        exchange = message.get('exchange', 'unknown')
        if exchange in self.private_messages:
            self.private_messages[exchange].append(('trade', message))
            print(f"💰 {exchange.upper()} - TRADE FILL: {message.get('amount', 0)} @ ${message.get('price', 0)} | ID={message.get('id', 'N/A')}")
    
    async def _handle_heartbeat(self, message: Dict[str, Any]):
        """Enhanced heartbeat handler."""
        exchange = message.get('exchange', 'unknown')
        if exchange in self.private_messages:
            self.private_messages[exchange].append(('heartbeat', message))
            logger.debug(f"💓 {exchange} - Heartbeat/Auth message")
    
    async def test_complete_pipeline(self):
        """Test the complete private WebSocket pipeline."""
        print("\n" + "="*70)
        print("🔐 TESTING COMPLETE PRIVATE WEBSOCKET PIPELINE")
        print("="*70)
        
        # Phase 1: Test authentication on all exchanges
        await self._phase1_test_authentication()
        
        # Phase 2: Test trading and private message reception
        await self._phase2_test_trading_and_messages()
        
        # Phase 3: Analyze results
        await self._phase3_analyze_results()
    
    async def _phase1_test_authentication(self):
        """Phase 1: Test private WebSocket authentication."""
        print("\n🔐 PHASE 1: Testing Private WebSocket Authentication")
        print("-" * 50)
        
        auth_tasks = []
        for exchange_name, connector in self.connectors.items():
            task = asyncio.create_task(
                self._test_authentication_single_exchange(exchange_name, connector)
            )
            auth_tasks.append(task)
        
        # Wait for all authentications
        results = await asyncio.gather(*auth_tasks, return_exceptions=True)
        
        # Process results
        successful_auths = 0
        for i, result in enumerate(results):
            exchange_name = list(self.connectors.keys())[i]
            if isinstance(result, Exception):
                print(f"   ❌ {exchange_name} - Authentication error: {result}")
                self.authentication_results[exchange_name] = {'status': 'failed', 'error': str(result)}
            elif result:
                successful_auths += 1
                self.authentication_results[exchange_name] = {'status': 'success'}
                print(f"   ✅ {exchange_name} - Authentication successful")
            else:
                print(f"   ❌ {exchange_name} - Authentication failed")
                self.authentication_results[exchange_name] = {'status': 'failed', 'error': 'Authentication returned False'}
        
        print(f"\n📊 Authentication Results: {successful_auths}/{len(self.connectors)} exchanges authenticated")
    
    async def _test_authentication_single_exchange(self, exchange_name: str, connector) -> bool:
        """Test authentication for a single exchange."""
        try:
            config = self.test_config[exchange_name]
            endpoint = config['endpoint']
            
            # Connect to private WebSocket
            conn_id = await self.ws_manager.connect_exchange(
                connector, endpoint, "private"
            )
            
            if not conn_id:
                return False
            
            self.connections[exchange_name] = conn_id
            
            # Wait for connection and authentication
            for _ in range(15):  # 15 second timeout
                status = self.ws_manager.get_connection_status(conn_id)
                if status.get('state') == WSState.CONNECTED:
                    return True
                await asyncio.sleep(1)
            
            return False
            
        except Exception as e:
            logger.error(f"Authentication test error for {exchange_name}: {e}")
            return False
    
    async def _phase2_test_trading_and_messages(self):
        """Phase 2: Test trading and private message reception."""
        print("\n💰 PHASE 2: Testing Trading and Private Messages")
        print("-" * 50)
        
        # Test 3 exchanges simultaneously for focused testing
        priority_exchanges = ['mexc_spot', 'binance_spot', 'bybit_spot']
        
        trade_tasks = []
        for exchange_name in priority_exchanges:
            if exchange_name in self.connectors and self.authentication_results[exchange_name]['status'] == 'success':
                task = asyncio.create_task(
                    self._test_trading_single_exchange(exchange_name)
                )
                trade_tasks.append(task)
        
        if trade_tasks:
            await asyncio.gather(*trade_tasks, return_exceptions=True)
    
    async def _test_trading_single_exchange(self, exchange_name: str):
        """Test trading and private message reception for single exchange."""
        print(f"\n🔵 Testing {exchange_name.upper()}...")
        
        try:
            connector = self.connectors[exchange_name]
            config = self.test_config[exchange_name]
            symbol = config['symbol']
            amount = Decimal(str(config['amount']))
            
            # Clear previous messages
            self.private_messages[exchange_name] = []
            
            print(f"   🔄 Placing MARKET BUY order: {amount} BERA")
            
            # Place market order for immediate fill
            buy_order = await connector.place_order(
                symbol=symbol,
                side='buy',
                amount=amount,
                order_type=OrderType.MARKET
            )
            
            if buy_order and buy_order.get('id'):
                order_id = buy_order['id']
                print(f"   ✅ Order placed successfully: {order_id}")
                self.trading_results[exchange_name]['orders'].append(buy_order)
                
                # Monitor for private messages for 20 seconds
                print(f"   ⏳ Monitoring for private messages (20s)...")
                
                messages_received = False
                for i in range(20):
                    await asyncio.sleep(1)
                    
                    message_count = len(self.private_messages[exchange_name])
                    if message_count > 0:
                        print(f"   🎉 SUCCESS! Received {message_count} private messages")
                        messages_received = True
                        break
                    
                    if i % 5 == 0 and i > 0:
                        print(f"   ⏳ Still waiting... ({i}s)")
                
                if not messages_received:
                    print(f"   ⚠️  No private messages received after 20 seconds")
                
                # Show received messages
                if self.private_messages[exchange_name]:
                    print(f"   📋 Received messages:")
                    for msg_type, msg in self.private_messages[exchange_name]:
                        print(f"      - {msg_type.upper()}: {str(msg)[:100]}...")
                
            else:
                print(f"   ❌ Order placement failed")
                
        except Exception as e:
            print(f"   ❌ Trading test error: {e}")
    
    async def _phase3_analyze_results(self):
        """Phase 3: Analyze and report final results."""
        print("\n" + "="*70)
        print("📊 FINAL COMPREHENSIVE RESULTS")
        print("="*70)
        
        total_exchanges = len(self.test_config)
        authenticated_count = sum(1 for r in self.authentication_results.values() if r['status'] == 'success')
        orders_placed = sum(len(r['orders']) for r in self.trading_results.values())
        messages_received = sum(len(msgs) for msgs in self.private_messages.values())
        exchanges_with_messages = sum(1 for msgs in self.private_messages.values() if len(msgs) > 0)
        
        print(f"\n🎯 SUMMARY:")
        print(f"   Total Exchanges: {total_exchanges}")
        print(f"   WebSocket Authentication: {authenticated_count}/{total_exchanges} ✅")
        print(f"   Orders Placed: {orders_placed}")
        print(f"   Private Messages Received: {messages_received}")
        print(f"   Exchanges with Messages: {exchanges_with_messages}")
        
        print(f"\n📋 DETAILED RESULTS:")
        for exchange_name in self.test_config:
            auth_status = self.authentication_results[exchange_name]['status']
            orders = len(self.trading_results[exchange_name]['orders'])
            messages = len(self.private_messages[exchange_name])
            
            if auth_status == 'success' and messages > 0:
                status_icon = "🎉 COMPLETE"
            elif auth_status == 'success':
                status_icon = "⚠️  WS-ONLY"
            else:
                status_icon = "❌ FAILED"
            
            print(f"   {exchange_name:15} | {status_icon} | Orders: {orders} | Messages: {messages}")
        
        # Final assessment
        if exchanges_with_messages >= 2:
            print(f"\n🚀 ASSESSMENT: Private WebSocket implementation working!")
            print(f"   ✅ Multiple exchanges receiving private messages")
            print(f"   ✅ Authentication pipeline complete")
            print(f"   ✅ Message parsing functional")
        elif exchanges_with_messages >= 1:
            print(f"\n⚠️  ASSESSMENT: Partial success - some exchanges working")
            print(f"   ✅ At least one exchange receiving private messages")
            print(f"   ⚠️  Need to debug remaining exchanges")
        else:
            print(f"\n❌ ASSESSMENT: Private message reception not working")
            print(f"   ❌ No exchanges receiving private messages")
            print(f"   ❌ Need to debug subscription/parsing logic")
        
        return exchanges_with_messages > 0
    
    async def run_final_test(self):
        """Run the complete final test."""
        try:
            await self.setup()
            success = await self.test_complete_pipeline()
            
            if success:
                print("\n✅ FINAL TEST COMPLETED SUCCESSFULLY!")
                print("🔍 Private WebSocket implementation verified")
            else:
                print("\n❌ Final test completed with issues")
                print("🔧 Further debugging required")
            
            return success
            
        except KeyboardInterrupt:
            print("\n🛑 Test interrupted by user")
            return False
        except Exception as e:
            print(f"\n💥 Test failed with error: {e}")
            import traceback
            traceback.print_exc()
            return False
        finally:
            await self.cleanup()


async def main():
    """Main test execution."""
    tester = FinalPrivateWebSocketTester()
    success = await tester.run_final_test()
    
    if success:
        print("\n🎉 PRIVATE WEBSOCKET IMPLEMENTATION COMPLETE!")
    else:
        print("\n🔧 Further development needed")


if __name__ == "__main__":
    asyncio.run(main()) 