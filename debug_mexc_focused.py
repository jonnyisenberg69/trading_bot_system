#!/usr/bin/env python
"""
MEXC Private WebSocket Debug Script

Focused debugging for MEXC private WebSocket messages to identify
exactly what's being received and why it's not being parsed.
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

# Configure logging for detailed output
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class MEXCPrivateMessageDebugger:
    """Debug MEXC private WebSocket messages specifically."""
    
    def __init__(self):
        """Initialize the debugger."""
        self.ws_manager = None
        self.mexc_connector = None
        self.conn_id = None
        self.all_raw_messages = []
        self.parsed_messages = []
        self.message_processor_called = 0
        self.message_handlers_called = 0
        
    async def setup(self):
        """Set up MEXC debugging."""
        print("🔧 Setting up MEXC Private WebSocket Debugger...")
        
        # Create MEXC connector
        config = get_exchange_config('mexc_spot')
        self.mexc_connector = create_exchange_connector('mexc', config)
        
        connected = await self.mexc_connector.connect()
        if not connected:
            print("❌ Failed to connect MEXC REST API")
            return False
        
        print("✅ MEXC REST API connected")
        
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
        
        # Patch message processor to capture ALL messages
        self._patch_message_processor()
        
        print("✅ WebSocket manager ready")
        return True
    
    def _patch_message_processor(self):
        """Patch the message processor to capture everything."""
        original_process = self.ws_manager._process_message
        original_determine_type = self.ws_manager._determine_message_type
        
        async def debug_process_message(conn_id: str, message: str) -> None:
            """Capture and analyze every message."""
            self.message_processor_called += 1
            
            try:
                # Parse the raw message
                parsed = json.loads(message) if isinstance(message, str) else message
                
                # Store raw message
                self.all_raw_messages.append({
                    'timestamp': time.time(),
                    'raw': message,
                    'parsed': parsed
                })
                
                print(f"\n🔍 RAW MESSAGE #{self.message_processor_called}:")
                print(f"   Raw: {message}")
                print(f"   Parsed: {json.dumps(parsed, indent=2)}")
                
                # Test message type detection
                message_type = self.ws_manager._determine_message_type('mexc', parsed)
                print(f"   Detected Type: {message_type}")
                
                # Test specific MEXC patterns
                self._analyze_mexc_message_patterns(parsed)
                
            except Exception as e:
                print(f"⚠️ Error parsing message: {e}")
                self.all_raw_messages.append({
                    'timestamp': time.time(),
                    'raw': message,
                    'error': str(e)
                })
            
            # Call original processor
            await original_process(conn_id, message)
        
        def debug_determine_type(exchange: str, message: Any) -> Optional[str]:
            """Debug message type determination."""
            result = original_determine_type(exchange, message)
            print(f"   Type Detection - Exchange: {exchange}, Result: {result}")
            
            # Manual MEXC analysis
            if exchange.lower() == 'mexc' and isinstance(message, dict):
                print(f"   MEXC Analysis:")
                print(f"     Has 'c' key: {'c' in message}")
                print(f"     Has 'd' key: {'d' in message}")
                print(f"     Has 'method' key: {'method' in message}")
                
                if 'c' in message:
                    channel = message['c']
                    print(f"     Channel: {channel}")
                    print(f"     Private order channel: {'spot@private.orders' in channel}")
                    print(f"     Private deals channel: {'spot@private.deals' in channel}")
                
                if 'd' in message:
                    data = message['d']
                    print(f"     Data keys: {list(data.keys()) if isinstance(data, dict) else 'not dict'}")
            
            return result
        
        # Replace methods
        self.ws_manager._process_message = debug_process_message
        self.ws_manager._determine_message_type = debug_determine_type
    
    async def _handle_order_update(self, message: Dict[str, Any]):
        """Handle order updates."""
        self.message_handlers_called += 1
        self.parsed_messages.append(('order_update', message))
        print(f"📨 ORDER UPDATE HANDLER CALLED: {message}")
    
    async def _handle_trade_notification(self, message: Dict[str, Any]):
        """Handle trade notifications."""
        self.message_handlers_called += 1
        self.parsed_messages.append(('trade', message))
        print(f"💰 TRADE HANDLER CALLED: {message}")
    
    async def _handle_heartbeat(self, message: Dict[str, Any]):
        """Handle heartbeat messages."""
        self.message_handlers_called += 1
        self.parsed_messages.append(('heartbeat', message))
        print(f"💓 HEARTBEAT HANDLER CALLED: {message}")
    
    def _analyze_mexc_message_patterns(self, parsed_message: dict):
        """Analyze MEXC-specific message patterns."""
        print(f"   MEXC Pattern Analysis:")
        
        # Check for various MEXC message formats
        if 'method' in parsed_message:
            method = parsed_message['method']
            print(f"     Method: {method}")
            if 'private' in method:
                print(f"     ✅ PRIVATE METHOD DETECTED!")
        
        if 'c' in parsed_message and 'd' in parsed_message:
            channel = parsed_message['c']
            data = parsed_message['d']
            print(f"     Channel/Data format - Channel: {channel}")
            
            if 'private' in channel:
                print(f"     ✅ PRIVATE CHANNEL DETECTED!")
                if 'orders' in channel:
                    print(f"     ✅ ORDER CHANNEL DETECTED!")
                if 'deals' in channel:
                    print(f"     ✅ DEALS CHANNEL DETECTED!")
        
        # Check for authentication response
        if 'id' in parsed_message and 'code' in parsed_message:
            print(f"     Auth response - ID: {parsed_message['id']}, Code: {parsed_message['code']}")
    
    async def debug_mexc_private_messages(self):
        """Run comprehensive MEXC private message debugging."""
        print("\n" + "="*70)
        print("🔍 MEXC PRIVATE MESSAGE DEBUGGING SESSION")
        print("="*70)
        
        # Step 1: Connect to private WebSocket
        print("\n🔧 STEP 1: Connecting to MEXC private WebSocket...")
        
        self.conn_id = await self.ws_manager.connect_exchange(
            self.mexc_connector, 
            'wss://wbs.mexc.com/ws', 
            "private"
        )
        
        if not self.conn_id:
            print("❌ Failed to connect to MEXC WebSocket")
            return
        
        print(f"✅ Connected to MEXC WebSocket (conn_id: {self.conn_id})")
        
        # Step 2: Wait for authentication
        print("\n🔧 STEP 2: Waiting for authentication...")
        await asyncio.sleep(5)
        
        print(f"📊 Messages received during auth: {len(self.all_raw_messages)}")
        
        # Step 3: Place a test order
        print("\n🔧 STEP 3: Placing test order to generate private messages...")
        
        try:
            buy_order = await self.mexc_connector.place_order(
                symbol='BERA/USDT',
                side='buy',
                amount=Decimal('0.5'),
                order_type=OrderType.MARKET
            )
            
            if buy_order:
                print(f"✅ Order placed: {buy_order.get('id')}")
            else:
                print("❌ Order placement failed")
                
        except Exception as e:
            print(f"❌ Order error: {e}")
        
        # Step 4: Monitor for messages
        print("\n🔧 STEP 4: Monitoring for private messages...")
        
        initial_count = len(self.all_raw_messages)
        
        for i in range(15):
            await asyncio.sleep(1)
            current_count = len(self.all_raw_messages)
            new_messages = current_count - initial_count
            
            if new_messages > 0:
                print(f"📨 New messages received: {new_messages}")
            
            if i % 5 == 0:
                print(f"   ⏳ Monitoring... {i+1}s (Total messages: {current_count})")
        
        # Step 5: Analyze results
        self._analyze_mexc_results()
    
    def _analyze_mexc_results(self):
        """Analyze MEXC debugging results."""
        print("\n" + "="*70)
        print("📊 MEXC DEBUG ANALYSIS RESULTS")
        print("="*70)
        
        print(f"\n🔢 STATISTICS:")
        print(f"   Raw messages received: {len(self.all_raw_messages)}")
        print(f"   Message processor calls: {self.message_processor_called}")
        print(f"   Handler calls: {self.message_handlers_called}")
        print(f"   Parsed messages: {len(self.parsed_messages)}")
        
        print(f"\n📋 ALL RAW MESSAGES:")
        for i, msg in enumerate(self.all_raw_messages):
            print(f"\n   Message {i+1}:")
            if 'parsed' in msg:
                print(f"      Time: {msg['timestamp']}")
                print(f"      Raw: {msg['raw']}")
                print(f"      Parsed: {json.dumps(msg['parsed'], indent=6)}")
            else:
                print(f"      Error: {msg.get('error', 'Unknown')}")
                print(f"      Raw: {msg.get('raw', 'None')}")
        
        print(f"\n📋 PARSED MESSAGES:")
        for i, (msg_type, msg) in enumerate(self.parsed_messages):
            print(f"   {i+1}. Type: {msg_type}")
            print(f"      Content: {json.dumps(msg, indent=6)}")
        
        # Identify the issue
        if len(self.all_raw_messages) == 0:
            print(f"\n❌ ISSUE: No messages received at all")
        elif self.message_processor_called == 0:
            print(f"\n❌ ISSUE: Message processor never called")
        elif self.message_handlers_called == 0:
            print(f"\n❌ ISSUE: Messages received but handlers never called")
            print(f"   This means message type detection is failing")
        else:
            print(f"\n✅ SUCCESS: Messages received and processed!")
    
    async def cleanup(self):
        """Clean up resources."""
        print("\n🧹 Cleaning up MEXC debug session...")
        
        if self.ws_manager:
            await self.ws_manager.stop()
        
        if self.mexc_connector:
            await self.mexc_connector.disconnect()
        
        print("✅ MEXC debug cleanup completed")


async def main():
    """Main MEXC debug execution."""
    debugger = MEXCPrivateMessageDebugger()
    
    try:
        success = await debugger.setup()
        if success:
            await debugger.debug_mexc_private_messages()
        
    except KeyboardInterrupt:
        print("\n🛑 MEXC debug interrupted by user")
    except Exception as e:
        print(f"\n💥 MEXC debug failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await debugger.cleanup()


if __name__ == "__main__":
    asyncio.run(main()) 