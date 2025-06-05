#!/usr/bin/env python
"""
Simple Trade Endpoints Test

Comprehensive test of order placement and private WebSocket message reception
across all 8 exchanges to identify what's working and what needs fixing.
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

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SimpleTradeTest:
    def __init__(self):
        self.connectors = {}
        self.ws_manager = None
        self.private_messages = {
            'order_updates': [],
            'trades': [],
            'other': []
        }
        
        # Test configuration with proper amounts and correct connector names
        self.trading_config = {
            'binance': {
                'symbol': 'BERA/USDT',
                'amount': 2.5,  # ~$6.50 (above $5 minimum)
            },
            'bybit': {
                'symbol': 'BERA/USDT',
                'amount': 2.0,  # ~$5.20 (above $1 minimum)
            },
            'mexc': {
                'symbol': 'BERA/USDT',
                'amount': 2.0,  # ~$5.20 (above $1 minimum)
            },
            'gateio': {
                'symbol': 'BERA/USDT',
                'amount': 2.0,  # ~$5.20 (above $1 minimum)
            },
            'bitget': {
                'symbol': 'BERA/USDT',
                'amount': 2.0,  # ~$5.20 (above $1 minimum)
            },
            'hyperliquid': {
                'symbol': 'BERA/USDC:USDC',
                'amount': 15.0,  # ~$39.00 (above $10 minimum)
            }
        }

    async def run_comprehensive_test(self):
        """Run comprehensive test across all exchanges."""
        print("🚀 COMPREHENSIVE TRADE AND WEBSOCKET TEST")
        print("=" * 60)
        
        # Step 1: Initialize connectors
        print("\n📋 STEP 1: Initializing exchange connectors...")
        await self._initialize_connectors()
        
        # Step 2: Start WebSocket manager
        print("\n📋 STEP 2: Starting WebSocket manager...")
        await self._start_websocket_manager()
        
        # Step 3: Connect to private WebSockets
        print("\n📋 STEP 3: Connecting to private WebSockets...")
        await self._connect_private_websockets()
        
        # Step 4: Test order placement and message reception
        print("\n📋 STEP 4: Testing order placement and message reception...")
        await self._test_trading_and_messages()
        
        # Step 5: Summary
        print("\n📋 STEP 5: Test Summary...")
        await self._print_summary()
        
        # Cleanup
        await self._cleanup()

    async def _initialize_connectors(self):
        """Initialize all exchange connectors."""
        failed_connectors = []
        
        # Map connector names to config names
        config_mapping = {
            'binance': 'binance_spot',
            'bybit': 'bybit_spot', 
            'mexc': 'mexc_spot',
            'gateio': 'gateio_spot',
            'bitget': 'bitget_spot',
            'hyperliquid': 'hyperliquid_perp'
        }
        
        for exchange_name in self.trading_config.keys():
            try:
                print(f"   📡 Connecting to {exchange_name}...")
                
                # Get the correct config name
                config_name = config_mapping[exchange_name]
                config = get_exchange_config(config_name)
                
                if not config:
                    print(f"   ❌ {exchange_name} no config found for {config_name}")
                    failed_connectors.append(exchange_name)
                    continue
                
                # Create connector with config
                connector = create_exchange_connector(exchange_name, config)
                if not connector:
                    print(f"   ❌ {exchange_name} connector creation failed")
                    failed_connectors.append(exchange_name)
                    continue
                
                await connector.connect()
                self.connectors[exchange_name] = connector
                print(f"   ✅ {exchange_name} connected")
            except Exception as e:
                print(f"   ❌ {exchange_name} failed: {e}")
                failed_connectors.append(exchange_name)
        
        # Remove failed connectors from config
        for failed in failed_connectors:
            self.trading_config.pop(failed, None)
        
        print(f"\n📊 Connected to {len(self.connectors)}/6 exchanges")

    async def _start_websocket_manager(self):
        """Start WebSocket manager."""
        try:
            self.ws_manager = WebSocketManager()
            self.ws_manager.register_handler(WSMessageType.ORDER_UPDATE, self._handle_order_update)
            self.ws_manager.register_handler(WSMessageType.TRADE, self._handle_trade)
            await self.ws_manager.start()
            print("   ✅ WebSocket manager started")
        except Exception as e:
            print(f"   ❌ WebSocket manager failed: {e}")

    async def _connect_private_websockets(self):
        """Connect to private WebSockets for all exchanges."""
        auth_results = []
        
        for exchange_name, connector in self.connectors.items():
            try:
                print(f"   🔐 Authenticating {exchange_name}...")
                
                # Get private WebSocket endpoint
                endpoint = self._get_private_endpoint(exchange_name)
                
                # Connect to private WebSocket
                conn_id = await self.ws_manager.connect_exchange(
                    connector, endpoint, 'private'
                )
                
                # Wait for connection and authentication
                await asyncio.sleep(2)
                
                # Check connection status
                status = self.ws_manager.get_connection_status(conn_id)
                if status['state'] == WSState.CONNECTED:
                    print(f"   ✅ {exchange_name} private WebSocket authenticated")
                    auth_results.append((exchange_name, True))
                else:
                    print(f"   ❌ {exchange_name} authentication failed: {status.get('error', 'Unknown')}")
                    auth_results.append((exchange_name, False))
                    
            except Exception as e:
                print(f"   ❌ {exchange_name} WebSocket error: {e}")
                auth_results.append((exchange_name, False))
        
        successful_auths = len([r for r in auth_results if r[1]])
        print(f"\n📊 Private WebSocket authentication: {successful_auths}/{len(auth_results)} successful")

    async def _test_trading_and_messages(self):
        """Test order placement and private message reception."""
        test_results = []
        
        for exchange_name, connector in self.connectors.items():
            print(f"\n🔄 Testing {exchange_name}...")
            
            config = self.trading_config[exchange_name]
            symbol = config['symbol']
            amount = Decimal(str(config['amount']))
            
            try:
                # Clear previous messages
                self.private_messages = {'order_updates': [], 'trades': [], 'other': []}
                
                # Place market order
                print(f"   📝 Placing market order: {amount} {symbol}")
                order_result = await connector.place_order(
                    symbol=symbol,
                    side='buy',
                    order_type=OrderType.MARKET,
                    amount=amount
                )
                
                order_id = order_result.get('orderId') or order_result.get('id')
                print(f"   ✅ Order placed: {order_id}")
                
                # Wait for private messages
                print(f"   ⏳ Waiting for private messages (10 seconds)...")
                await asyncio.sleep(10)
                
                # Check for private messages
                total_messages = (len(self.private_messages['order_updates']) + 
                                len(self.private_messages['trades']) + 
                                len(self.private_messages['other']))
                
                print(f"   📨 Private messages received: {total_messages}")
                print(f"      Order updates: {len(self.private_messages['order_updates'])}")
                print(f"      Trade messages: {len(self.private_messages['trades'])}")
                print(f"      Other messages: {len(self.private_messages['other'])}")
                
                # Cancel order if still open (unlikely for market orders)
                try:
                    await connector.cancel_order(symbol=symbol, order_id=order_id)
                    print(f"   🗑️  Order cancelled")
                except:
                    pass  # Order might already be filled
                
                test_results.append({
                    'exchange': exchange_name,
                    'order_success': True,
                    'order_id': order_id,
                    'messages_received': total_messages
                })
                
            except Exception as e:
                print(f"   ❌ Trading test failed: {e}")
                test_results.append({
                    'exchange': exchange_name,
                    'order_success': False,
                    'error': str(e),
                    'messages_received': 0
                })
        
        self.test_results = test_results

    async def _print_summary(self):
        """Print comprehensive test summary."""
        print("\n🏁 COMPREHENSIVE TEST RESULTS")
        print("=" * 60)
        
        successful_orders = 0
        exchanges_with_messages = 0
        
        for result in self.test_results:
            exchange = result['exchange']
            order_success = result['order_success']
            messages = result['messages_received']
            
            order_icon = "✅" if order_success else "❌"
            message_icon = "✅" if messages > 0 else "❌"
            
            print(f"{order_icon} {exchange:15} | Order: {'SUCCESS' if order_success else 'FAILED':7} | Messages: {messages:2d} {message_icon}")
            
            if order_success:
                successful_orders += 1
            if messages > 0:
                exchanges_with_messages += 1
        
        print("\n📊 FINAL STATISTICS:")
        print(f"   Successful order placement: {successful_orders}/{len(self.test_results)}")
        print(f"   Exchanges receiving private messages: {exchanges_with_messages}/{len(self.test_results)}")
        
        if exchanges_with_messages == 0:
            print("\n🚨 CRITICAL ISSUE: NO EXCHANGES RECEIVING PRIVATE MESSAGES!")
            print("   This indicates a fundamental problem with the private WebSocket implementation.")
        elif exchanges_with_messages < len(self.test_results):
            print(f"\n⚠️  PARTIAL SUCCESS: {exchanges_with_messages}/{len(self.test_results)} exchanges receiving messages")
            print("   Some exchanges need private WebSocket debugging.")
        else:
            print("\n🎉 COMPLETE SUCCESS: All exchanges receiving private messages!")

    def _get_private_endpoint(self, exchange_name: str) -> str:
        """Get private WebSocket endpoint for exchange."""
        endpoints = {
            'binance': 'wss://stream.binance.com:9443/ws',
            'bybit': 'wss://stream.bybit.com/v5/private',
            'mexc': 'wss://wbs.mexc.com/ws',
            'gateio': 'wss://api.gateio.ws/ws/v4/',
            'bitget': 'wss://ws.bitget.com/spot/v1/stream',
            'hyperliquid': 'wss://api.hyperliquid.xyz/ws'
        }
        return endpoints.get(exchange_name, '')

    async def _handle_order_update(self, data: Dict[str, Any]):
        """Handle order update messages."""
        self.private_messages['order_updates'].append(data)
        print(f"📨 Order update: {data}")

    async def _handle_trade(self, data: Dict[str, Any]):
        """Handle trade messages."""
        self.private_messages['trades'].append(data)
        print(f"📨 Trade message: {data}")

    async def _cleanup(self):
        """Cleanup connections."""
        print("\n🧹 Cleaning up...")
        
        if self.ws_manager:
            await self.ws_manager.stop()
        
        for connector in self.connectors.values():
            try:
                await connector.disconnect()
            except:
                pass
        
        print("✅ Cleanup completed")

async def main():
    test = SimpleTradeTest()
    await test.run_comprehensive_test()

if __name__ == "__main__":
    asyncio.run(main()) 