#!/usr/bin/env python3
"""
Test script to verify WebSocket connection fixes.

This script tests the fixed WebSocket connection establishment
to ensure connections are properly created and don't timeout.
"""

import asyncio
import sys
import os
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from exchanges.websocket.ws_manager import WebSocketManager
from exchanges.connectors import create_exchange_connector
from config.settings import load_config
import structlog

logger = structlog.get_logger(__name__)


async def test_websocket_connection():
    """Test WebSocket connection establishment."""
    print("🔧 Testing WebSocket Connection Fixes")
    print("=" * 50)
    
    try:
        # Load config
        config = load_config()
        
        # Create a simple exchange connector for testing
        binance_config = {
            'api_key': 'test_key',
            'secret': 'test_secret',
            'sandbox': True,
            'market_type': 'spot'
        }
        
        connector = create_exchange_connector('binance', binance_config)
        if not connector:
            print("❌ Failed to create Binance connector")
            return False
        
        print(f"✅ Created Binance connector: {connector.name}")
        
        # Create WebSocket manager
        ws_manager = WebSocketManager({
            'ping_interval': 30.0,
            'mexc_ping_interval': 20.0,
            'mexc_connection_timeout': 10.0
        })
        
        await ws_manager.start()
        print("✅ WebSocket manager started")
        
        # Test connection establishment (this should not timeout now)
        print("🔗 Testing WebSocket connection establishment...")
        
        # Use public endpoint for testing (no auth required)
        endpoint = "wss://stream.binance.com:9443/ws"
        
        conn_id = await ws_manager.connect_exchange(
            exchange=connector,
            endpoint=endpoint,
            conn_type='public'
        )
        
        if conn_id:
            print(f"✅ Connection initiated: {conn_id}")
            
            # Wait a bit for connection to establish
            await asyncio.sleep(5)
            
            # Check connection status
            status = ws_manager.get_connection_status(conn_id)
            print(f"📊 Connection status: {status}")
            
            if status.get('state') == 'connected':
                print("🎉 SUCCESS: WebSocket connection established!")
                return True
            elif status.get('state') == 'connecting':
                print("⏳ Connection still in progress...")
                # Wait a bit more
                await asyncio.sleep(10)
                status = ws_manager.get_connection_status(conn_id)
                print(f"📊 Final status: {status}")
                
                if status.get('state') == 'connected':
                    print("🎉 SUCCESS: WebSocket connection established!")
                    return True
                else:
                    print(f"❌ Connection failed: {status.get('error', 'Unknown error')}")
                    return False
            else:
                print(f"❌ Connection failed: {status.get('error', 'Unknown error')}")
                return False
        else:
            print("❌ Failed to initiate connection")
            return False
            
    except Exception as e:
        print(f"❌ Test failed with exception: {e}")
        return False
    finally:
        if 'ws_manager' in locals():
            await ws_manager.stop()
            print("🛑 WebSocket manager stopped")


async def main():
    """Main test function."""
    print("🧪 WebSocket Connection Fix Test")
    print("This test verifies that WebSocket connections no longer timeout")
    print()
    
    success = await test_websocket_connection()
    
    print()
    if success:
        print("✅ ALL TESTS PASSED - WebSocket connections are working!")
        print("🚀 The strategy should now be able to establish WebSocket connections")
    else:
        print("❌ TESTS FAILED - WebSocket connections still have issues")
        print("🔧 Additional debugging may be needed")


if __name__ == "__main__":
    asyncio.run(main()) 