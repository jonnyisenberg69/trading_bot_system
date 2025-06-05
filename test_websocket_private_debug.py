#!/usr/bin/env python
"""
Debug Private WebSocket Messages

Simple test to see what private messages we're actually receiving
from the exchanges when orders are placed.
"""

import asyncio
import sys
import json
import time
import logging
from pathlib import Path
from typing import Dict, Any

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from exchanges.connectors import create_exchange_connector
from exchanges.websocket import WebSocketManager, WSState
from exchanges.base_connector import OrderType
from config.exchange_keys import get_exchange_config

# Configure detailed logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class PrivateMessageDebugger:
    """Debug private websocket messages."""
    
    def __init__(self):
        self.ws_manager = None
        self.connectors = {}
        self.connections = {}
        self.received_messages = {}
        
        # Test only working exchanges
        self.test_exchanges = {
            'binance_spot': {
                'symbol': 'BERA/USDT',
                'amount': 1.0,  # Small amount for testing
                'endpoint': 'wss://stream.binance.com:9443/ws'
            },
            'mexc_spot': {
                'symbol': 'BERA/USDT', 
                'amount': 1.5,
                'endpoint': 'wss://wbs.mexc.com/ws'
            }
        }
    
    async def setup(self):
        """Set up debugging environment."""
        print("🔧 Setting up Private Message Debugger...")
        
        # Create WebSocket manager
        self.ws_manager = WebSocketManager()
        await self.ws_manager.start()
        
        # Create connectors for working exchanges
        for exchange_name in self.test_exchanges:
            try:
                config = get_exchange_config(exchange_name)
                connector = create_exchange_connector(exchange_name.split('_')[0], config)
                
                connected = await connector.connect()
                if connected:
                    self.connectors[exchange_name] = connector
                    print(f"   ✅ {exchange_name} - REST connected")
                else:
                    print(f"   ❌ {exchange_name} - REST failed")
            except Exception as e:
                print(f"   ❌ {exchange_name} - error: {e}")
    
    async def test_raw_messages(self):
        """Test raw message reception."""
        print("\n" + "="*60)
        print("🔍 TESTING RAW PRIVATE WEBSOCKET MESSAGES")
        print("="*60)
        
        # Connect to private websockets
        for exchange_name, config in self.test_exchanges.items():
            if exchange_name not in self.connectors:
                continue
                
            print(f"\n📡 Connecting to {exchange_name} private websocket...")
            
            try:
                connector = self.connectors[exchange_name]
                endpoint = config['endpoint']
                
                # Connect
                conn_id = await self.ws_manager.connect_exchange(
                    connector, endpoint, "private"
                )
                
                self.connections[exchange_name] = conn_id
                self.received_messages[exchange_name] = []
                
                # Wait for connection
                await asyncio.sleep(3)
                
                # Check connection status
                status = self.ws_manager.get_connection_status(conn_id)
                print(f"   Connection status: {status.get('state')}")
                
                if status.get('state') == WSState.CONNECTED:
                    print(f"   ✅ {exchange_name} - WebSocket connected")
                else:
                    print(f"   ❌ {exchange_name} - WebSocket connection failed")
                    
            except Exception as e:
                print(f"   ❌ {exchange_name} - WebSocket error: {e}")
    
    async def monitor_messages_during_trade(self):
        """Monitor messages while placing a trade."""
        print("\n" + "="*60)
        print("💰 PLACING TRADE AND MONITORING MESSAGES")
        print("="*60)
        
        # Start message monitoring
        monitor_task = asyncio.create_task(self._message_monitor())
        
        # Place a trade on one working exchange
        exchange_name = 'mexc_spot'  # Known working exchange
        if exchange_name in self.connectors:
            try:
                connector = self.connectors[exchange_name]
                config = self.test_exchanges[exchange_name]
                symbol = config['symbol']
                amount = config['amount']
                
                print(f"\n📈 Placing trade on {exchange_name}...")
                
                # Get market price
                orderbook = await connector.get_orderbook(symbol, limit=5)
                if orderbook and orderbook.get('bids'):
                    bid_price = orderbook['bids'][0][0]
                    
                    print(f"   🔵 Placing BUY order: {amount} {symbol} @ {bid_price}")
                    
                    # Place order
                    order = await connector.place_order(
                        symbol=symbol,
                        side='buy',
                        amount=amount,
                        price=bid_price,
                        order_type=OrderType.LIMIT
                    )
                    
                    if order and order.get('id'):
                        print(f"   ✅ Order placed: {order['id']}")
                        
                        # Wait and monitor for messages
                        print("   ⏳ Monitoring for 15 seconds...")
                        await asyncio.sleep(15)
                        
                    else:
                        print("   ❌ Order placement failed")
                        
                else:
                    print("   ❌ Failed to get market data")
                    
            except Exception as e:
                print(f"   ❌ Trade error: {e}")
        
        # Stop monitoring
        monitor_task.cancel()
        
        # Show results
        self._show_message_results()
    
    async def _message_monitor(self):
        """Monitor all incoming websocket messages."""
        try:
            while True:
                for exchange_name, conn_id in self.connections.items():
                    # Get connection info
                    if conn_id in self.ws_manager.connections:
                        conn_info = self.ws_manager.connections[conn_id]
                        ws = conn_info.get('ws')
                        
                        if ws:
                            try:
                                # Check for messages (non-blocking)
                                message = await asyncio.wait_for(ws.recv(), timeout=0.1)
                                self.received_messages[exchange_name].append({
                                    'timestamp': time.time(),
                                    'message': message
                                })
                                print(f"📨 {exchange_name} - Received message: {message[:100]}...")
                            except asyncio.TimeoutError:
                                pass  # No message available
                            except Exception as e:
                                print(f"⚠️  {exchange_name} - Message error: {e}")
                
                await asyncio.sleep(0.1)  # Small delay
                
        except asyncio.CancelledError:
            pass
    
    def _show_message_results(self):
        """Show the results of message monitoring."""
        print("\n" + "="*60)
        print("📊 MESSAGE MONITORING RESULTS")
        print("="*60)
        
        for exchange_name, messages in self.received_messages.items():
            print(f"\n{exchange_name}:")
            print(f"  Total messages: {len(messages)}")
            
            if messages:
                print("  Sample messages:")
                for i, msg_data in enumerate(messages[:3]):  # Show first 3
                    print(f"    {i+1}. {msg_data['message'][:150]}...")
            else:
                print("  ❌ No messages received")
    
    async def cleanup(self):
        """Clean up resources."""
        print("\n🧹 Cleaning up...")
        
        if self.ws_manager:
            await self.ws_manager.stop()
        
        for connector in self.connectors.values():
            try:
                await connector.disconnect()
            except:
                pass
    
    async def run(self):
        """Run the debugging session."""
        try:
            await self.setup()
            await self.test_raw_messages()
            await self.monitor_messages_during_trade()
        except Exception as e:
            print(f"💥 Error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            await self.cleanup()


async def main():
    """Main function."""
    debugger = PrivateMessageDebugger()
    await debugger.run()


if __name__ == "__main__":
    asyncio.run(main()) 