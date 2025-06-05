#!/usr/bin/env python
"""
Bybit Private WebSocket Debug Script

Comprehensive test of Bybit private WebSocket authentication, order placement,
and private message reception to verify the complete trading flow.
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
from database import get_session
from database.models import Trade, Exchange
from sqlalchemy import select, desc, func

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from exchanges.connectors import create_exchange_connector
from exchanges.base_connector import OrderType
from config.exchange_keys import get_exchange_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BybitPrivateDebugger:
    def __init__(self):
        self.messages_received = 0
        self.private_messages = 0
        self.auth_confirmed = False
        self.order_id = None
        
    async def test_bybit_private_websocket(self):
        """Test Bybit private WebSocket with proper authentication."""
        print("🔧 TESTING BYBIT PRIVATE WEBSOCKET")
        print("=" * 50)
        
        try:
            # Get Bybit configuration
            config = get_exchange_config('bybit_spot')
            if not config:
                print("❌ No Bybit configuration found")
                return
            
            # Create connector
            bybit = create_exchange_connector('bybit', config)
            
            print(f"✅ Bybit connector created: {bybit}")
            
            # Test with Bybit private WebSocket
            await self.connect_and_test_bybit_private(config, bybit)
            
        except Exception as e:
            print(f"❌ Error: {e}")
            import traceback
            traceback.print_exc()
    
    async def connect_and_test_bybit_private(self, config, bybit):
        """Connect to Bybit private WebSocket with authentication."""
        api_key = config['api_key']
        secret = config['secret']
        
        # Bybit spot private WebSocket endpoint
        endpoint = 'wss://stream.bybit.com/v5/private'
        
        print(f"🔗 Connecting to: {endpoint}")
        
        try:
            async with websockets.connect(endpoint) as ws:
                print("✅ WebSocket connected to Bybit private stream")
                
                # Step 1: Authenticate
                await self.authenticate_bybit(ws, api_key, secret)
                
                # Step 2: Subscribe to private channels
                await self.subscribe_to_bybit_private_channels(ws)
                
                # Step 3: Place a test order
                await self.place_bybit_test_order(bybit)
                
                # Step 4: Monitor for private messages
                await self.monitor_bybit_private_messages(ws, timeout=15)
                
        except Exception as e:
            print(f"❌ WebSocket error: {e}")
            import traceback
            traceback.print_exc()
    
    async def authenticate_bybit(self, ws, api_key, secret):
        """Authenticate with Bybit using API signature."""
        print("🔐 Authenticating with Bybit...")
        
        try:
            # Generate Bybit authentication signature
            timestamp = int(time.time() * 1000)
            signature_payload = f"GET/realtime{timestamp}"
            signature = hmac.new(
                secret.encode('utf-8'),
                signature_payload.encode('utf-8'),
                hashlib.sha256
            ).hexdigest()
            
            auth_message = {
                "op": "auth",
                "args": [api_key, timestamp, signature]
            }
            
            await ws.send(json.dumps(auth_message))
            print(f"📤 Sent Bybit auth: {json.dumps(auth_message, indent=2)}")
            
            # Wait for auth response
            auth_response = await asyncio.wait_for(ws.recv(), timeout=5.0)
            print(f"📥 Auth response: {auth_response}")
            
            try:
                auth_data = json.loads(auth_response)
                if auth_data.get('success') == True:
                    self.auth_confirmed = True
                    print("✅ Bybit authentication successful")
                else:
                    print(f"❌ Bybit authentication failed: {auth_data}")
            except json.JSONDecodeError:
                print(f"❌ Invalid auth response: {auth_response}")
                
            return self.auth_confirmed
            
        except Exception as e:
            print(f"❌ Bybit authentication error: {e}")
            return False
    
    async def subscribe_to_bybit_private_channels(self, ws):
        """Subscribe to Bybit private channels after authentication."""
        print("📡 Subscribing to Bybit private channels...")
        
        if not self.auth_confirmed:
            print("❌ Cannot subscribe - authentication not confirmed")
            return False
            
        try:
            # Subscribe to various private channels
            subscription_messages = [
                {"op": "subscribe", "args": ["order"]},           # Order updates
                {"op": "subscribe", "args": ["execution"]},       # Trade executions
                {"op": "subscribe", "args": ["wallet"]},          # Wallet updates
                {"op": "subscribe", "args": ["position"]}         # Position updates
            ]
            
            for i, sub_msg in enumerate(subscription_messages):
                print(f"📤 Subscription {i+1}: {json.dumps(sub_msg)}")
                await ws.send(json.dumps(sub_msg))
                await asyncio.sleep(0.5)  # Wait between subscriptions
                
                # Check for subscription response
                try:
                    response = await asyncio.wait_for(ws.recv(), timeout=2.0)
                    print(f"📥 Subscription response {i+1}: {response}")
                except asyncio.TimeoutError:
                    print(f"⏰ No immediate response to subscription {i+1}")
            
            print("✅ Bybit private channel subscriptions sent")
            return True
            
        except Exception as e:
            print(f"❌ Bybit subscription error: {e}")
            return False
    
    async def place_bybit_test_order(self, bybit):
        """Place a small test order on Bybit to trigger private messages."""
        print("💰 Placing Bybit test order...")
        
        try:
            # Get current price
            ticker = await bybit.exchange.fetch_ticker('BERA/USDT')
            print(f"📊 Current BERA price: ${ticker['last']:.4f}")
            
            # Place small market buy order
            order = await bybit.exchange.create_market_buy_order(
                'BERA/USDT',
                2.0  # 2.0 BERA to meet minimum requirements
            )
            
            self.order_id = order.get('id')
            print(f"✅ Bybit order placed: {order}")
            print(f"   Order ID: {self.order_id}")
            print(f"   Status: {order.get('status', 'N/A')}")
            print(f"   Filled: {order.get('filled', 'N/A')}")
            return order
            
        except Exception as e:
            print(f"❌ Bybit order placement error: {e}")
            print("   Continuing to monitor for private messages anyway...")
            return None
    
    async def monitor_bybit_private_messages(self, ws, timeout=15):
        """Monitor for Bybit private messages."""
        print(f"👂 Monitoring for Bybit private messages ({timeout}s)...")
        
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
                    if self.is_bybit_private_message(msg_data):
                        self.private_messages += 1
                        print(f"🎯 BYBIT PRIVATE MESSAGE {self.private_messages}: {msg_data}")
                        
                        # Analyze the private message
                        self.analyze_bybit_private_message(msg_data)
                    
                except json.JSONDecodeError:
                    print(f"❌ Invalid JSON: {message}")
                
            except asyncio.TimeoutError:
                continue  # Continue monitoring
                
        print(f"\n📊 BYBIT FINAL RESULTS:")
        print(f"   Total messages: {self.messages_received}")
        print(f"   Private messages: {self.private_messages}")
        print(f"   Order placed: {self.order_id}")
        
        if self.private_messages == 0:
            print("❌ NO BYBIT PRIVATE MESSAGES RECEIVED!")
            if self.order_id:
                print("🚨 CRITICAL: Order executed but no private messages!")
        else:
            print(f"✅ {self.private_messages} Bybit private messages received!")
    
    def is_bybit_private_message(self, message):
        """Check if message is a Bybit private trade/order message."""
        if not isinstance(message, dict):
            return False
            
        # Check for Bybit private message indicators
        topic = message.get('topic', '')
        
        # Private channels: order, execution, wallet, position
        if any(channel in topic for channel in ['order', 'execution', 'wallet', 'position']):
            return True
            
        # Check for data structure with order/execution info
        if 'data' in message:
            data = message['data']
            if isinstance(data, list) and data:
                item = data[0]
                if any(key in item for key in ['orderId', 'orderLinkId', 'execId', 'execType']):
                    return True
                    
        return False
    
    def analyze_bybit_private_message(self, message):
        """Analyze Bybit private message content."""
        topic = message.get('topic', '')
        data = message.get('data', [])
        
        print(f"   📋 Analysis:")
        print(f"      Topic: {topic}")
        
        if data and isinstance(data, list):
            item = data[0]
            print(f"      Data keys: {list(item.keys())}")
            
            if 'order' in topic:
                print(f"      🔹 ORDER UPDATE:")
                print(f"         Order ID: {item.get('orderId', 'N/A')}")
                print(f"         Status: {item.get('orderStatus', 'N/A')}")
                print(f"         Symbol: {item.get('symbol', 'N/A')}")
                print(f"         Side: {item.get('side', 'N/A')}")
                print(f"         Quantity: {item.get('qty', 'N/A')}")
                print(f"         Filled: {item.get('cumExecQty', 'N/A')}")
                
            elif 'execution' in topic:
                print(f"      🔹 EXECUTION UPDATE:")
                print(f"         Exec ID: {item.get('execId', 'N/A')}")
                print(f"         Order ID: {item.get('orderId', 'N/A')}")
                print(f"         Price: {item.get('execPrice', 'N/A')}")
                print(f"         Quantity: {item.get('execQty', 'N/A')}")
                print(f"         Side: {item.get('side', 'N/A')}")
                print(f"         Symbol: {item.get('symbol', 'N/A')}")
                
            elif 'wallet' in topic:
                print(f"      🔹 WALLET UPDATE:")
                print(f"         Account Type: {item.get('accountType', 'N/A')}")
                print(f"         Coin: {item.get('coin', 'N/A')}")
                print(f"         Balance: {item.get('walletBalance', 'N/A')}")

async def main():
    debugger = BybitPrivateDebugger()
    await debugger.test_bybit_private_websocket()

async def debug_bybit_trades():
    async for session in get_session():
        # Check bybit_perp trades specifically
        result = await session.execute(
            select(Trade, Exchange.name).join(Exchange)
            .where(Exchange.name == 'bybit_perp')
            .order_by(desc(Trade.timestamp))
            .limit(10)
        )
        bybit_perp_trades = result.all()
        
        print(f'Bybit Perp trades in database: {len(bybit_perp_trades)}')
        for trade, exchange_name in bybit_perp_trades:
            print(f'  {trade.exchange_trade_id} - {trade.symbol} - {trade.timestamp} - {trade.side} {trade.amount}@{trade.price}')
        
        # Check all exchanges for comparison
        result = await session.execute(
            select(Exchange.name, func.count(Trade.id)).join(Trade).group_by(Exchange.name)
        )
        exchange_counts = result.all()
        
        print(f'\nTrade counts by exchange:')
        for exchange, count in exchange_counts:
            print(f'  {exchange}: {count} trades')
        
        # Check symbol formats used
        result = await session.execute(
            select(Exchange.name, Trade.symbol).join(Trade).distinct()
        )
        exchange_symbols = result.all()
        
        print(f'\nAll exchange/symbol combinations in database:')
        for exchange, symbol in exchange_symbols:
            print(f'  {exchange}: {symbol}')
        
        break

if __name__ == "__main__":
    asyncio.run(main())
    asyncio.run(debug_bybit_trades()) 