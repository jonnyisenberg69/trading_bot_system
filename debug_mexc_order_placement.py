#!/usr/bin/env python
"""
MEXC Order Placement & Message Debug

Focused debugging to see exactly what private WebSocket messages
are being received for filled orders and why they're not being parsed.
"""

import asyncio
import json
import time
import logging
import hmac
import hashlib
from pathlib import Path
import sys
import websockets

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from exchanges.connectors import create_exchange_connector
from exchanges.base_connector import OrderType
from config.exchange_keys import get_exchange_config

# Configure verbose logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MEXCMessageDebugger:
    def __init__(self):
        self.messages = []
        self.auth_success = False
        self.order_id = None
        
    async def run_debug(self):
        """Run comprehensive MEXC debug test."""
        print("🔍 MEXC PRIVATE WEBSOCKET MESSAGE DEBUG")
        print("=" * 60)
        
        # Initialize MEXC connector
        config = get_exchange_config('mexc_spot')
        mexc = create_exchange_connector('mexc', config)
        await mexc.connect()
        
        # Start WebSocket monitoring
        ws_task = asyncio.create_task(self.monitor_websocket(config))
        
        # Wait for authentication
        await asyncio.sleep(3)
        
        if not self.auth_success:
            print("❌ Authentication failed, cannot continue")
            return
            
        print("✅ Authentication successful, placing order...")
        
        # Place an order that should generate messages
        try:
            order = await mexc.place_order(
                symbol='BERA/USDT',
                side='buy',
                order_type=OrderType.MARKET,
                amount=2.0
            )
            self.order_id = order.get('id')
            print(f"✅ Order placed: {self.order_id}")
        except Exception as e:
            print(f"❌ Order placement failed: {e}")
            return
        
        # Wait for messages
        print("⏳ Waiting for private messages (20 seconds)...")
        await asyncio.sleep(20)
        
        # Analyze results
        await self.analyze_messages()
        
        # Cleanup
        ws_task.cancel()
        await mexc.disconnect()
        
    async def monitor_websocket(self, config):
        """Monitor MEXC private WebSocket messages."""
        api_key = config['api_key']
        secret = config['secret']
        subscription_sent = False  # Prevent infinite loop
        
        try:
            async with websockets.connect('wss://wbs.mexc.com/ws') as ws:
                # Send authentication
                timestamp = int(time.time() * 1000)
                query_string = f"api_key={api_key}&req_time={timestamp}"
                signature = hmac.new(
                    secret.encode('utf-8'),
                    query_string.encode('utf-8'),
                    hashlib.sha256
                ).hexdigest()
                
                auth_msg = {
                    "method": "SUBSCRIPTION",
                    "params": ["spot@private.orders.v3.api", "spot@private.deals.v3.api"],
                    "api_key": api_key,
                    "req_time": timestamp,
                    "signature": signature
                }
                
                await ws.send(json.dumps(auth_msg))
                print("📤 Sent MEXC authentication + subscription message")
                
                # Listen for messages
                async for message in ws:
                    try:
                        data = json.loads(message)
                        self.messages.append({
                            'timestamp': time.time(),
                            'raw': message,
                            'parsed': data
                        })
                        
                        print(f"📨 Received: {message}")
                        
                        # Check for auth success (only once)
                        if 'code' in data and data.get('code') == 0 and not self.auth_success:
                            self.auth_success = True
                            print("✅ Authentication confirmed")
                            
                            # Send ONE additional subscription after auth
                            if not subscription_sent:
                                sub_msg = {
                                    "method": "SUBSCRIPTION",
                                    "params": ["spot@private.deals.v3.api"]
                                }
                                await ws.send(json.dumps(sub_msg))
                                print("📤 Sent additional subscription message")
                                subscription_sent = True
                            
                    except Exception as e:
                        print(f"❌ Error parsing message: {e}")
                        print(f"Raw message: {message}")
                        
        except Exception as e:
            print(f"❌ WebSocket error: {e}")
            
    async def analyze_messages(self):
        """Analyze all received messages."""
        print(f"\n📊 MESSAGE ANALYSIS")
        print("=" * 60)
        print(f"Total messages received: {len(self.messages)}")
        
        if not self.messages:
            print("❌ NO MESSAGES RECEIVED!")
            return
            
        for i, msg in enumerate(self.messages, 1):
            print(f"\n📨 Message {i}:")
            print(f"   Raw: {msg['raw']}")
            print(f"   Parsed: {json.dumps(msg['parsed'], indent=2)}")
            
            # Analyze message type
            data = msg['parsed']
            if 'c' in data:
                channel = data['c']
                print(f"   Channel: {channel}")
                if 'private.orders' in channel or 'private.deals' in channel:
                    print(f"   🎯 PRIVATE MESSAGE DETECTED!")
                    if 'd' in data:
                        print(f"   Data: {json.dumps(data['d'], indent=2)}")
                        
        print(f"\n🎯 ORDER PLACED: {self.order_id}")
        print(f"🔍 Expected: Private trade/order messages")
        print(f"📨 Received: {len(self.messages)} messages")
        
        # Check if any private messages
        private_msgs = [
            msg for msg in self.messages 
            if 'c' in msg['parsed'] and (
                'private.orders' in msg['parsed']['c'] or 
                'private.deals' in msg['parsed']['c']
            )
        ]
        
        print(f"🎯 Private messages: {len(private_msgs)}")
        
        if not private_msgs and self.order_id:
            print("🚨 CRITICAL: Order executed but NO private messages received!")
            print("🔍 This confirms the private WebSocket subscription issue.")

if __name__ == "__main__":
    debugger = MEXCMessageDebugger()
    asyncio.run(debugger.run_debug()) 