#!/usr/bin/env python3
"""
Test Bulk Credentials Feature

This script tests the new bulk credential application functionality.
"""

import requests
import json

API_BASE_URL = "http://localhost:8000"

def test_bulk_credentials():
    """Test the bulk credential application feature."""
    print("🧪 Testing Bulk Credential Application")
    print("=" * 50)
    
    # Test payload - simulated account with credentials for multiple exchanges
    test_payload = {
        "account_name": "Test Account",
        "test_connections": True,
        "credentials": {
            "binance_spot": {
                "api_key": "test_api_key_binance",
                "api_secret": "test_api_secret_binance",
                "wallet_address": "",
                "private_key": "",
                "passphrase": "",
                "testnet": True
            },
            "bybit_spot": {
                "api_key": "test_api_key_bybit",
                "api_secret": "test_api_secret_bybit",
                "wallet_address": "",
                "private_key": "",
                "passphrase": "",
                "testnet": True
            },
            "hyperliquid_perp": {
                "api_key": "0x1234567890abcdef",
                "api_secret": "test_private_key",
                "wallet_address": "0x1234567890abcdef",
                "private_key": "test_private_key",
                "passphrase": "",
                "testnet": True
            }
        }
    }
    
    try:
        print("📤 Sending bulk credential application request...")
        print(f"   Account: {test_payload['account_name']}")
        print(f"   Exchanges: {len(test_payload['credentials'])}")
        
        response = requests.post(
            f"{API_BASE_URL}/api/exchanges/bulk-credentials",
            json=test_payload,
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            print("\n✅ Bulk Application Results:")
            print(f"   Total Exchanges: {result['total_exchanges']}")
            print(f"   Successful: {result['successful']}")
            print(f"   Failed: {result['failed']}")
            print(f"   Message: {result['message']}")
            
            print("\n📋 Detailed Results:")
            for res in result.get('results', []):
                status = "✅" if res['success'] else "❌"
                test_status = ""
                if res.get('test_success') is False:
                    test_status = " (Test Failed)"
                elif res.get('test_success') is True:
                    test_status = " (Test Passed)"
                
                print(f"   {status} {res['exchange_name']} ({res.get('exchange_type', 'unknown')}){test_status}")
                if not res['success'] and res.get('error'):
                    print(f"      Error: {res['error']}")
                elif res.get('test_error'):
                    print(f"      Test Error: {res['test_error']}")
            
            return True
            
        else:
            print(f"❌ Request failed: HTTP {response.status_code}")
            try:
                error_detail = response.json()
                print(f"   Error: {error_detail.get('detail', 'Unknown error')}")
            except:
                print(f"   Raw response: {response.text}")
            return False
            
    except requests.exceptions.ConnectionError:
        print("❌ Cannot connect to API server")
        print("   Make sure the API is running: python api/main.py")
        return False
    except requests.exceptions.Timeout:
        print("❌ Request timed out")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

def test_exchange_status():
    """Test getting exchange status to verify endpoints exist."""
    print("\n🔍 Testing Exchange Status Endpoint...")
    
    try:
        response = requests.get(f"{API_BASE_URL}/api/exchanges/status", timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            connections = data.get('connections', [])
            print(f"✅ Found {len(connections)} exchange connections:")
            
            for conn in connections[:5]:  # Show first 5
                print(f"   • {conn.get('name', 'Unknown')} ({conn.get('exchange_type', 'unknown')}) - {conn.get('status', 'unknown')}")
            
            if len(connections) > 5:
                print(f"   ... and {len(connections) - 5} more")
                
            return True
        else:
            print(f"❌ Failed to get exchange status: HTTP {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Error getting exchange status: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Bulk Credentials Feature Test")
    print("=" * 50)
    
    # Test basic exchange status first
    if not test_exchange_status():
        print("\n❌ Basic API connection failed. Exiting.")
        exit(1)
    
    # Test bulk credentials feature
    success = test_bulk_credentials()
    
    print("\n" + "=" * 50)
    if success:
        print("🎉 Bulk credentials feature test completed!")
        print("\n📝 Notes:")
        print("• This test uses dummy credentials, so connections will likely fail")
        print("• The important part is that the API accepts the request and processes it")
        print("• Real credentials would be needed for actual connection testing")
    else:
        print("❌ Bulk credentials feature test failed!")
        print("\n🔧 Troubleshooting:")
        print("• Ensure API server is running: python api/main.py")
        print("• Check that all dependencies are installed")
        print("• Verify the bulk-credentials endpoint was added correctly") 