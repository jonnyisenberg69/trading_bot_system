#!/usr/bin/env python

"""
Binance Private WebSocket Debug Script

Focused debugging for Binance private WebSocket messages to test
if the private message reception works on a more reliable exchange.
"""

import asyncio
import json
import time
import logging
from pathlib import Path
import sys
import websockets
import requests

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from exchanges.connectors import create_exchange_connector
from exchanges.base_connector import OrderType
from config.exchange_keys import get_exchange_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BinancePrivateDebugger:
    def __init__(self):
        self.messages_received = 0
        self.private_messages = 0
        
    async def test_binance_private_websocket(self):
        """Test Binance private WebSocket with proper listenKey."""
        print("🔧 TESTING BINANCE PRIVATE WEBSOCKET")
        print("=" * 50)
        
        try:
            # Get Binance configuration
            config = get_exchange_config('binance_spot')
            if not config:
                print("❌ No Binance configuration found")
                return
            
            # Create connector
            binance = create_exchange_connector('binance', config)
            
            print(f"✅ Binance connector created: {binance}")
            
            # Test with Binance private WebSocket
            await self.connect_and_test_binance_private(config, binance)
            
        except Exception as e:
            print(f"❌ Error: {e}")
            import traceback
            traceback.print_exc()
    
    async def connect_and_test_binance_private(self, config, binance):
        """Connect to Binance private WebSocket."""
        api_key = config['api_key']
        secret = config['secret']
        
        print("🔑 Getting Binance listenKey...")
        
        try:
            # Get listenKey using REST API
            listen_key = await self.get_binance_listen_key(api_key, secret)
            if not listen_key:
                print("❌ Failed to get listenKey")
                return
                
            print(f"✅ Got listenKey: {listen_key[:16]}...")
            
            # Connect to private WebSocket with listenKey
            endpoint = f"wss://stream.binance.com:9443/ws/{listen_key}"
            print(f"🔗 Connecting to: {endpoint}")
            
            async with websockets.connect(endpoint) as ws:
                print("✅ WebSocket connected to Binance private stream")
                
                # No authentication needed - listenKey handles it
                print("✅ Authentication via listenKey (no additional auth needed)")
                
                # Place a test order
                await self.place_binance_test_order(binance)
                
                # Monitor for private messages
                await self.monitor_binance_private_messages(ws, timeout=15)
                
        except Exception as e:
            print(f"❌ WebSocket error: {e}")
            import traceback
            traceback.print_exc()
    
    async def get_binance_listen_key(self, api_key, secret):
        """Get Binance listenKey via REST API."""
        try:
            headers = {
                'X-MBX-APIKEY': api_key
            }
            
            # For spot trading, use the simple userDataStream endpoint without parameters
            endpoint = 'https://api.binance.com/api/v3/userDataStream'
            
            print(f"📡 Trying Binance spot endpoint: {endpoint}")
            response = requests.post(endpoint, headers=headers, timeout=10)
            
            print(f"   Status: {response.status_code}")
            print(f"   Response: {response.text}")
            
            if response.status_code == 200:
                data = response.json()
                return data.get('listenKey')
            else:
                print(f"❌ Failed to get listenKey: {response.text}")
                return None
            
        except Exception as e:
            print(f"❌ Failed to get listenKey: {e}")
            return None
    
    async def place_binance_test_order(self, binance):
        """Place a small test order on Binance."""
        print("💰 Placing Binance test order...")
        
        try:
            # Get current price
            ticker = await binance.exchange.fetch_ticker('BERA/USDT')
            print(f"📊 Current BERA price: ${ticker['last']:.4f}")
            
            # Place small market buy order
            order = await binance.exchange.create_market_buy_order(
                'BERA/USDT',
                2.5  # 2.5 BERA to meet minimum notional
            )
            
            print(f"✅ Binance order placed: {order}")
            print(f"   Order ID: {order.get('id', 'N/A')}")
            print(f"   Status: {order.get('status', 'N/A')}")
            return order
            
        except Exception as e:
            print(f"❌ Binance order placement error: {e}")
            print("   Continuing to monitor for private messages anyway...")
            return None
    
    async def monitor_binance_private_messages(self, ws, timeout=15):
        """Monitor for Binance private messages."""
        print(f"👂 Monitoring for Binance private messages ({timeout}s)...")
        
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
                    if self.is_binance_private_message(msg_data):
                        self.private_messages += 1
                        print(f"🎯 BINANCE PRIVATE MESSAGE {self.private_messages}: {msg_data}")
                    
                except json.JSONDecodeError:
                    print(f"❌ Invalid JSON: {message}")
                
            except asyncio.TimeoutError:
                continue  # Continue monitoring
                
        print(f"\n📊 BINANCE FINAL RESULTS:")
        print(f"   Total messages: {self.messages_received}")
        print(f"   Private messages: {self.private_messages}")
        
        if self.private_messages == 0:
            print("❌ NO BINANCE PRIVATE MESSAGES RECEIVED!")
        else:
            print(f"✅ {self.private_messages} Binance private messages received!")
    
    def is_binance_private_message(self, message):
        """Check if message is a Binance private trade/order message."""
        if not isinstance(message, dict):
            return False
            
        # Check for Binance private message types
        event_type = message.get('e')
        
        if event_type in ['executionReport', 'outboundAccountPosition', 'balanceUpdate']:
            return True
            
        # Check for other private indicators
        if 'orderId' in message or 'tradeId' in message:
            return True
            
        return False

async def main():
    debugger = BinancePrivateDebugger()
    await debugger.test_binance_private_websocket()

if __name__ == "__main__":
    asyncio.run(main()) 