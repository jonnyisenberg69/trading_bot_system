#!/usr/bin/env python
"""
Private Message Debug Script

Captures raw WebSocket messages to debug why private messages
aren't being parsed correctly despite successful authentication.
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

# Configure logging with debug level
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class PrivateMessageDebugger:
    """Debug private WebSocket messages."""
    
    def __init__(self):
        """Initialize the debugger."""
        self.ws_manager = None
        self.connectors = {}
        self.connections = {}
        self.raw_messages = {}
        self.parsed_messages = {}
        
        # Test only 2 exchanges for focused debugging
        self.test_exchanges = ['binance_spot', 'mexc_spot']
        
        for exchange in self.test_exchanges:
            self.raw_messages[exchange] = []
            self.parsed_messages[exchange] = {
                'order_updates': [],
                'trade_notifications': [],
                'heartbeat_messages': [],
                'unknown_messages': []
            }
    
    async def setup(self):
        """Set up the debugging environment."""
        print("🔧 Setting up Private Message Debugger...")
        
        # Create WebSocket manager with debug logging
        self.ws_manager = WebSocketManager({
            'ping_interval': 20.0,
            'pong_timeout': 10.0,
            'reconnect_enabled': True
        })
        await self.ws_manager.start()
        
        # Register ALL message handlers to capture everything
        self.ws_manager.register_handler(WSMessageType.ORDER_UPDATE, self._handle_order_update)
        self.ws_manager.register_handler(WSMessageType.TRADE, self._handle_trade_notification)
        self.ws_manager.register_handler(WSMessageType.HEARTBEAT, self._handle_heartbeat)
        self.ws_manager.register_handler(WSMessageType.ERROR, self._handle_error)
        self.ws_manager.register_handler(WSMessageType.ORDERBOOK, self._handle_orderbook)
        self.ws_manager.register_handler(WSMessageType.SUBSCRIBE, self._handle_subscribe)
        self.ws_manager.register_handler(WSMessageType.UNSUBSCRIBE, self._handle_unsubscribe)
        
        # Override the message processor to capture raw messages
        self._patch_message_processor()
        
        # Create connectors
        print("📡 Creating exchange connectors...")
        for exchange_name in self.test_exchanges:
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
        
        print(f"✅ Setup complete - {len(self.connectors)}/{len(self.test_exchanges)} exchanges ready")
    
    def _patch_message_processor(self):
        """Patch the WebSocket manager to capture raw messages."""
        original_process = self.ws_manager._process_message
        
        async def debug_process_message(conn_id: str, message: str) -> None:
            """Capture raw messages and call original processor."""
            # Extract exchange name from connection
            conn_info = self.ws_manager.connections.get(conn_id, {})
            exchange_name = 'unknown'
            
            # Try to determine exchange from connection info
            for test_exchange in self.test_exchanges:
                if test_exchange in str(conn_info.get('exchange', '')):
                    exchange_name = test_exchange
                    break
            
            # Try to determine from connector type
            if exchange_name == 'unknown':
                exchange = conn_info.get('exchange')
                if exchange:
                    class_name = exchange.__class__.__name__.lower()
                    if 'binance' in class_name:
                        exchange_name = 'binance_spot' if 'binance_spot' in self.test_exchanges else 'binance_perp'
                    elif 'mexc' in class_name:
                        exchange_name = 'mexc_spot'
            
            # Store raw message
            if exchange_name in self.raw_messages:
                try:
                    parsed_json = json.loads(message)
                    self.raw_messages[exchange_name].append({
                        'timestamp': time.time(),
                        'conn_id': conn_id,
                        'raw': message,
                        'parsed': parsed_json
                    })
                    
                    print(f"🔍 RAW MESSAGE from {exchange_name}:")
                    print(f"   {json.dumps(parsed_json, indent=2)[:200]}...")
                    
                except Exception as e:
                    print(f"⚠️  Failed to parse message from {exchange_name}: {e}")
                    self.raw_messages[exchange_name].append({
                        'timestamp': time.time(),
                        'conn_id': conn_id,
                        'raw': message,
                        'error': str(e)
                    })
            
            # Call original processor
            await original_process(conn_id, message)
        
        # Replace the method
        self.ws_manager._process_message = debug_process_message
    
    async def _handle_order_update(self, message: Dict[str, Any]):
        """Handle order update messages."""
        exchange = message.get('exchange', 'unknown')
        if exchange in self.parsed_messages:
            self.parsed_messages[exchange]['order_updates'].append(message)
            print(f"📨 PARSED ORDER UPDATE from {exchange}: {message.get('status', 'unknown')}")
    
    async def _handle_trade_notification(self, message: Dict[str, Any]):
        """Handle trade notifications."""
        exchange = message.get('exchange', 'unknown')
        if exchange in self.parsed_messages:
            self.parsed_messages[exchange]['trade_notifications'].append(message)
            print(f"💰 PARSED TRADE from {exchange}: {message.get('amount', 0)} @ {message.get('price', 0)}")
    
    async def _handle_heartbeat(self, message: Dict[str, Any]):
        """Handle heartbeat/auth messages."""
        exchange = message.get('exchange', 'unknown')
        if exchange in self.parsed_messages:
            self.parsed_messages[exchange]['heartbeat_messages'].append(message)
            print(f"💓 PARSED HEARTBEAT from {exchange}")
    
    async def _handle_error(self, message: Dict[str, Any]):
        """Handle error messages."""
        exchange = message.get('exchange', 'unknown')
        print(f"❌ ERROR MESSAGE from {exchange}: {message}")
    
    async def _handle_orderbook(self, message: Dict[str, Any]):
        """Handle orderbook messages."""
        exchange = message.get('exchange', 'unknown')
        print(f"📚 ORDERBOOK MESSAGE from {exchange} (ignoring)")
    
    async def _handle_subscribe(self, message: Dict[str, Any]):
        """Handle subscription messages."""
        exchange = message.get('exchange', 'unknown')
        print(f"📨 SUBSCRIPTION MESSAGE from {exchange}: {message}")
    
    async def _handle_unsubscribe(self, message: Dict[str, Any]):
        """Handle unsubscription messages."""
        exchange = message.get('exchange', 'unknown')
        print(f"📤 UNSUBSCRIPTION MESSAGE from {exchange}: {message}")
    
    async def debug_private_messages(self):
        """Debug private messages on selected exchanges."""
        print("\n" + "="*80)
        print("🔍 PRIVATE MESSAGE DEBUG SESSION")
        print("="*80)
        print("Connecting to private WebSockets and monitoring ALL messages")
        print("="*80)
        
        # Connect to private WebSockets
        for exchange_name, connector in self.connectors.items():
            print(f"\n🔧 Connecting {exchange_name} to private WebSocket...")
            
            try:
                # Connect to private WebSocket
                private_endpoint = self._get_private_endpoint(exchange_name)
                
                conn_id = await self.ws_manager.connect_exchange(
                    connector, private_endpoint, "private"
                )
                
                if conn_id:
                    self.connections[exchange_name] = conn_id
                    print(f"   ✅ {exchange_name} - Connected to private WebSocket")
                    
                    # Wait for authentication
                    await asyncio.sleep(3)
                    
                else:
                    print(f"   ❌ {exchange_name} - Failed to connect")
                    
            except Exception as e:
                print(f"   ❌ {exchange_name} - Connection error: {e}")
        
        # Execute a test trade to generate private messages
        if 'mexc_spot' in self.connectors:
            print(f"\n💰 Executing test trade on MEXC to generate private messages...")
            try:
                mexc_connector = self.connectors['mexc_spot']
                
                # Place a small market order to generate private messages
                buy_order = await mexc_connector.place_order(
                    symbol='BERA/USDT',
                    side='buy',
                    amount=Decimal('0.5'),  # Small amount
                    order_type=OrderType.MARKET
                )
                
                if buy_order:
                    print(f"   ✅ MEXC test order placed: {buy_order.get('id')}")
                else:
                    print(f"   ❌ MEXC test order failed")
                    
            except Exception as e:
                print(f"   ❌ MEXC test order error: {e}")
        
        # Monitor for 20 seconds
        print(f"\n⏳ Monitoring for 20 seconds to capture messages...")
        for i in range(20):
            await asyncio.sleep(1)
            if i % 5 == 0:
                raw_count = sum(len(msgs) for msgs in self.raw_messages.values())
                parsed_count = sum(
                    sum(len(msgs) for msgs in exchange_msgs.values()) 
                    for exchange_msgs in self.parsed_messages.values()
                )
                print(f"   📊 Progress: {i+1}s | Raw messages: {raw_count} | Parsed messages: {parsed_count}")
        
        # Analyze results
        self._analyze_debug_results()
    
    def _get_private_endpoint(self, exchange_name: str) -> str:
        """Get private WebSocket endpoint for exchange."""
        endpoints = {
            'binance_spot': 'wss://stream.binance.com:9443/ws',
            'mexc_spot': 'wss://wbs.mexc.com/ws'
        }
        return endpoints.get(exchange_name, '')
    
    def _analyze_debug_results(self):
        """Analyze the captured messages."""
        print("\n" + "="*80)
        print("📊 PRIVATE MESSAGE DEBUG ANALYSIS")
        print("="*80)
        
        for exchange_name in self.test_exchanges:
            print(f"\n🔍 {exchange_name.upper()} ANALYSIS:")
            
            raw_msgs = self.raw_messages.get(exchange_name, [])
            parsed_msgs = self.parsed_messages.get(exchange_name, {})
            
            print(f"   📨 Raw messages received: {len(raw_msgs)}")
            print(f"   🔧 Parsed order updates: {len(parsed_msgs.get('order_updates', []))}")
            print(f"   💰 Parsed trade notifications: {len(parsed_msgs.get('trade_notifications', []))}")
            print(f"   💓 Parsed heartbeats: {len(parsed_msgs.get('heartbeat_messages', []))}")
            
            # Show sample raw messages
            if raw_msgs:
                print(f"\n   📋 SAMPLE RAW MESSAGES:")
                for i, msg in enumerate(raw_msgs[:3]):  # Show first 3 messages
                    print(f"      Message {i+1}:")
                    if 'parsed' in msg:
                        print(f"        {json.dumps(msg['parsed'], indent=6)}")
                    else:
                        print(f"        RAW: {msg.get('raw', 'unknown')[:100]}...")
            else:
                print(f"   ❌ NO MESSAGES RECEIVED")
        
        # Overall analysis
        total_raw = sum(len(msgs) for msgs in self.raw_messages.values())
        total_parsed = sum(
            sum(len(msgs) for msgs in exchange_msgs.values()) 
            for exchange_msgs in self.parsed_messages.values()
        )
        
        print(f"\n🎯 OVERALL ANALYSIS:")
        print(f"   📨 Total raw messages: {total_raw}")
        print(f"   🔧 Total parsed messages: {total_parsed}")
        print(f"   📊 Parse rate: {(total_parsed/total_raw*100) if total_raw > 0 else 0:.1f}%")
        
        if total_raw == 0:
            print(f"\n❌ ISSUE: No messages received at all")
            print(f"   Possible causes:")
            print(f"   - WebSocket connection not working")
            print(f"   - Authentication failed")
            print(f"   - Wrong endpoints")
        elif total_parsed == 0:
            print(f"\n❌ ISSUE: Messages received but not parsed")
            print(f"   Possible causes:")
            print(f"   - Message type detection logic broken")
            print(f"   - Private message formats not recognized")
            print(f"   - Normalization logic failing")
        else:
            print(f"\n✅ SUCCESS: Messages received and parsed!")
    
    async def cleanup(self):
        """Clean up resources."""
        print("\n🧹 Cleaning up debug session...")
        
        if self.ws_manager:
            await self.ws_manager.stop()
        
        for connector in self.connectors.values():
            try:
                await connector.disconnect()
            except Exception as e:
                logger.debug(f"Error disconnecting connector: {e}")
        
        print("✅ Debug cleanup completed")


async def main():
    """Main debug execution."""
    debugger = PrivateMessageDebugger()
    
    try:
        await debugger.setup()
        await debugger.debug_private_messages()
        
    except KeyboardInterrupt:
        print("\n🛑 Debug session interrupted by user")
    except Exception as e:
        print(f"\n💥 Debug session failed with error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await debugger.cleanup()


if __name__ == "__main__":
    asyncio.run(main()) 