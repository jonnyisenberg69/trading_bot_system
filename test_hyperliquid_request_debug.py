#!/usr/bin/env python
"""
Hyperliquid Request/Response Debug

Debug the actual HTTP requests and responses to understand why trade fetching isn't working.
"""

import asyncio
import sys
import json
import aiohttp
from pathlib import Path
from datetime import datetime, timedelta
import traceback

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from exchanges.connectors import create_exchange_connector
from config.exchange_keys import get_exchange_config

async def debug_hyperliquid_requests():
    """Debug actual HTTP requests and responses for Hyperliquid."""
    
    print("🔍 HYPERLIQUID REQUEST/RESPONSE DEBUG")
    print("=" * 60)
    
    try:
        # Get config
        config = get_exchange_config('hyperliquid_perp')
        if not config:
            print("❌ No configuration found")
            return
            
        print("📋 Configuration:")
        print(f"   Wallet Address: {config.get('wallet_address', 'N/A')}")
        print(f"   Private Key: {config.get('private_key', 'N/A')[:10]}..." if config.get('private_key') else "   Private Key: None")
        print(f"   Sandbox: {config.get('sandbox', False)}")
        
        # Determine API URL
        api_url = "https://api.hyperliquid-testnet.xyz" if config.get('sandbox', False) else "https://api.hyperliquid.xyz"
        print(f"   API URL: {api_url}")
        
        # Create connector to test CCXT
        connector = create_exchange_connector('hyperliquid', config)
        if not connector:
            print("❌ Connector creation failed")
            return
            
        connected = await connector.connect()
        if not connected:
            print("❌ Connection failed")
            return
            
        print("✅ CCXT connector created and connected")
        
        # Test 1: Check what CCXT is actually calling
        print(f"\n🔧 TESTING CCXT RAW CALLS:")
        print("-" * 40)
        
        try:
            print("📋 Testing exchange.fetch_my_trades()...")
            
            # Monkey patch to see the actual HTTP requests
            original_request = connector.exchange.request
            
            async def debug_request(url, method='GET', headers=None, body=None, **kwargs):
                print(f"   🌐 HTTP Request:")
                print(f"      Method: {method}")
                print(f"      URL: {url}")
                print(f"      Headers: {json.dumps(dict(headers) if headers else {}, indent=8)}")
                print(f"      Body: {body}")
                
                response = await original_request(url, method, headers, body, **kwargs)
                
                print(f"   📥 HTTP Response:")
                print(f"      Status: {getattr(response, 'status', 'N/A')}")
                print(f"      Response: {json.dumps(response, indent=8, default=str)}")
                
                return response
            
            connector.exchange.request = debug_request
            
            # Now make the actual call
            trades = await connector.exchange.fetch_my_trades()
            print(f"   📊 CCXT Result: {len(trades) if trades else 0} trades")
            
        except Exception as e:
            print(f"   ❌ CCXT Error: {e}")
            print(f"   🔍 Traceback: {traceback.format_exc()}")
        
        # Test 2: Direct API calls using the official endpoints
        print(f"\n🔧 TESTING DIRECT API CALLS:")
        print("-" * 40)
        
        # Test user state endpoint
        await test_user_state_api(api_url, config)
        
        # Test user fills endpoint  
        await test_user_fills_api(api_url, config)
        
        # Test account info
        await test_account_info_api(api_url, config)
        
        # Clean up
        await connector.disconnect()
        
    except Exception as e:
        print(f"❌ Overall error: {e}")
        print(f"🔍 Traceback: {traceback.format_exc()}")

async def test_user_state_api(api_url: str, config: dict):
    """Test the user state API endpoint directly."""
    print("📋 Testing /info user state endpoint...")
    
    try:
        async with aiohttp.ClientSession() as session:
            url = f"{api_url}/info"
            
            payload = {
                "type": "userState",
                "user": config.get('wallet_address')
            }
            
            print(f"   🌐 Request:")
            print(f"      URL: {url}")
            print(f"      Method: POST")
            print(f"      Payload: {json.dumps(payload, indent=8)}")
            
            async with session.post(url, json=payload) as response:
                response_text = await response.text()
                print(f"   📥 Response:")
                print(f"      Status: {response.status}")
                print(f"      Headers: {dict(response.headers)}")
                print(f"      Body: {response_text}")
                
                if response.status == 200:
                    try:
                        data = json.loads(response_text)
                        print(f"   📊 Parsed response:")
                        print(f"      Keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
                        if isinstance(data, dict):
                            for key, value in data.items():
                                if isinstance(value, list):
                                    print(f"      {key}: {len(value)} items")
                                else:
                                    print(f"      {key}: {value}")
                    except json.JSONDecodeError as e:
                        print(f"   ❌ JSON decode error: {e}")
                        
    except Exception as e:
        print(f"   ❌ Error: {e}")

async def test_user_fills_api(api_url: str, config: dict):
    """Test the user fills (trade history) API endpoint directly."""
    print("\n📋 Testing /info user fills endpoint...")
    
    try:
        async with aiohttp.ClientSession() as session:
            url = f"{api_url}/info"
            
            payload = {
                "type": "userFills",
                "user": config.get('wallet_address')
            }
            
            print(f"   🌐 Request:")
            print(f"      URL: {url}")
            print(f"      Method: POST")
            print(f"      Payload: {json.dumps(payload, indent=8)}")
            
            async with session.post(url, json=payload) as response:
                response_text = await response.text()
                print(f"   📥 Response:")
                print(f"      Status: {response.status}")
                print(f"      Body: {response_text}")
                
                if response.status == 200:
                    try:
                        data = json.loads(response_text)
                        print(f"   📊 Parsed response:")
                        if isinstance(data, list):
                            print(f"      Trade count: {len(data)}")
                            for i, trade in enumerate(data[-3:]):  # Last 3 trades
                                print(f"      Trade {i+1}: {trade}")
                        else:
                            print(f"      Response type: {type(data)}")
                            print(f"      Response: {data}")
                    except json.JSONDecodeError as e:
                        print(f"   ❌ JSON decode error: {e}")
                        
    except Exception as e:
        print(f"   ❌ Error: {e}")

async def test_account_info_api(api_url: str, config: dict):
    """Test getting account/balance info."""
    print("\n📋 Testing account info...")
    
    try:
        async with aiohttp.ClientSession() as session:
            url = f"{api_url}/info"
            
            # Test clearinghouse state
            payload = {
                "type": "clearinghouseState",
                "user": config.get('wallet_address')
            }
            
            print(f"   🌐 Clearinghouse State Request:")
            print(f"      URL: {url}")
            print(f"      Payload: {json.dumps(payload, indent=8)}")
            
            async with session.post(url, json=payload) as response:
                response_text = await response.text()
                print(f"   📥 Clearinghouse Response:")
                print(f"      Status: {response.status}")
                print(f"      Body: {response_text}")
                
                if response.status == 200:
                    try:
                        data = json.loads(response_text)
                        print(f"   📊 Clearinghouse data:")
                        if isinstance(data, dict):
                            for key, value in data.items():
                                print(f"      {key}: {value}")
                    except json.JSONDecodeError as e:
                        print(f"   ❌ JSON decode error: {e}")
                        
    except Exception as e:
        print(f"   ❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(debug_hyperliquid_requests()) 