#!/usr/bin/env python

"""
MEXC Exact Error Debug Script

Tests the correct MEXC private WebSocket implementation using
the proper endpoint and subscription format.
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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MEXCPrivateDebugger:
    def __init__(self):
        self.messages_received = 0
        self.private_messages = 0
        
    async def test_mexc_private_correctly(self):
        """Test MEXC private WebSocket with correct format."""
        print("🔧 TESTING MEXC PRIVATE WEBSOCKET - CORRECT FORMAT")
        print("=" * 60)
        
        try:
            # Get MEXC configuration
            config = get_exchange_config('mexc_spot')
            if not config:
                print("❌ No MEXC configuration found")
                return
            
            # Create connector
            mexc = create_exchange_connector('mexc', config)
            
            print(f"✅ MEXC connector created: {mexc}")
            
            # Test with correct private WebSocket endpoint and format
            await self.connect_and_test_private(config, mexc)
            
        except Exception as e:
            print(f"❌ Error: {e}")
            import traceback
            traceback.print_exc()
    
    async def connect_and_test_private(self, config, mexc):
        """Connect to MEXC private WebSocket with correct format."""
        api_key = config['api_key']
        secret = config['secret']
        
        # Use correct MEXC private WebSocket endpoint (if different)
        endpoint = 'wss://wbs.mexc.com/ws'
        
        print(f"🔗 Connecting to: {endpoint}")
        
        try:
            async with websockets.connect(endpoint) as ws:
                print("✅ WebSocket connected")
                
                # Step 1: Authenticate with correct format
                await self.authenticate_mexc(ws, api_key, secret)
                
                # Step 2: Subscribe to private channels with correct format
                await self.subscribe_to_private_channels(ws)
                
                # Step 3: Place a small order to trigger private messages
                await self.place_test_order(mexc)
                
                # Step 4: Monitor for private messages
                await self.monitor_private_messages(ws, timeout=10)
                
        except Exception as e:
            print(f"❌ WebSocket error: {e}")
            import traceback
            traceback.print_exc()
    
    async def authenticate_mexc(self, ws, api_key, secret):
        """Authenticate with MEXC using correct format."""
        print("🔐 Authenticating...")
        
        timestamp = int(time.time() * 1000)
        
        # Create authentication message WITH subscription topics (this might be required)
        query_string = f"api_key={api_key}&req_time={timestamp}"
        signature = hmac.new(
            secret.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        
        # Try auth with subscription topics included
        auth_message = {
            "method": "SUBSCRIPTION",
            "params": ["spot@private.orders.v3.api", "spot@private.deals.v3.api"],  # Include topics in auth
            "api_key": api_key,
            "req_time": timestamp,
            "signature": signature
        }
        
        await ws.send(json.dumps(auth_message))
        print(f"📤 Sent auth with topics: {json.dumps(auth_message, indent=2)}")
        
        # Wait for auth response
        response = await ws.recv()
        print(f"📥 Auth response: {response}")
        
        return True
    
    async def subscribe_to_private_channels(self, ws):
        """Subscribe to private channels after authentication."""
        print("📡 Subscribing to private channels...")
        
        # Try different subscription formats
        subscription_formats = [
            # Format 1: Individual subscriptions
            {"method": "SUBSCRIPTION", "params": ["spot@private.orders.v3.api"]},
            {"method": "SUBSCRIPTION", "params": ["spot@private.deals.v3.api"]},
            
            # Format 2: Combined subscription  
            {"method": "SUBSCRIPTION", "params": ["spot@private.orders.v3.api", "spot@private.deals.v3.api"]},
            
            # Format 3: Alternative format
            {"method": "SUB", "params": ["spot@private.orders.v3.api", "spot@private.deals.v3.api"]},
        ]
        
        for i, sub_msg in enumerate(subscription_formats[:2]):  # Try first 2 formats
            print(f"📤 Subscription {i+1}: {json.dumps(sub_msg)}")
            await ws.send(json.dumps(sub_msg))
            await asyncio.sleep(0.5)  # Wait between subscriptions
            
            # Check for response
            try:
                response = await asyncio.wait_for(ws.recv(), timeout=1.0)
                print(f"📥 Subscription response {i+1}: {response}")
            except asyncio.TimeoutError:
                print(f"⏰ No immediate response to subscription {i+1}")
    
    async def place_test_order(self, mexc):
        """Place a small test order to trigger private messages."""
        print("💰 Placing test order...")
        
        try:
            # Get current price using async CCXT method
            ticker = await mexc.exchange.fetch_ticker('BERA/USDT')
            current_price = float(ticker['bid']) * 0.99  # Buy below market for quick fill
            
            print(f"📊 Current BERA price: ${ticker['last']:.4f}")
            
            # Place small market buy order using async CCXT
            order = await mexc.exchange.create_market_buy_order(
                'BERA/USDT',
                1.0  # 1 BERA
            )
            
            print(f"✅ Order placed: {order}")
            print(f"   Order ID: {order.get('id', 'N/A')}")
            print(f"   Status: {order.get('status', 'N/A')}")
            return order
            
        except Exception as e:
            print(f"❌ Order placement error: {e}")
            print("   Continuing to monitor for private messages anyway...")
            return None
    
    async def monitor_private_messages(self, ws, timeout=10):
        """Monitor for private messages."""
        print(f"👂 Monitoring for private messages ({timeout}s)...")
        
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                message = await asyncio.wait_for(ws.recv(), timeout=1.0)
                self.messages_received += 1
                
                print(f"📥 Message {self.messages_received}: {message}")
                
                # Parse message
                try:
                    msg_data = json.loads(message)
                    
                    # Check if this is a private message
                    if self.is_private_message(msg_data):
                        self.private_messages += 1
                        print(f"🎯 PRIVATE MESSAGE {self.private_messages}: {msg_data}")
                    
                except json.JSONDecodeError:
                    print(f"❌ Invalid JSON: {message}")
                
            except asyncio.TimeoutError:
                continue  # Continue monitoring
                
        print(f"\n📊 FINAL RESULTS:")
        print(f"   Total messages: {self.messages_received}")
        print(f"   Private messages: {self.private_messages}")
        
        if self.private_messages == 0:
            print("❌ NO PRIVATE MESSAGES RECEIVED!")
        else:
            print(f"✅ {self.private_messages} private messages received!")
    
    def is_private_message(self, message):
        """Check if message is a private trade/order message."""
        if not isinstance(message, dict):
            return False
            
        # Check for private message indicators
        if 'd' in message and 'c' in message:
            channel = message['c']
            if 'private.orders' in channel or 'private.deals' in channel:
                return True
                
        # Check for direct private data
        if 'data' in message:
            data = message['data']
            if isinstance(data, dict):
                if 'orderId' in data or 'tradeId' in data:
                    return True
                    
        return False

async def main():
    debugger = MEXCPrivateDebugger()
    await debugger.test_mexc_private_correctly()

if __name__ == "__main__":
    asyncio.run(main()) 