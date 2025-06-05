#!/usr/bin/env python3
"""
Test script to verify connection improvements are working.
This script tests the API endpoints and validates the symbol utilities.
"""

import sys
import time
import asyncio
import requests
from utils.symbol_utils import (
    SymbolMapper, 
    validate_symbol, 
    get_base_quote, 
    get_exchange_symbols
)

# Configuration
API_BASE_URL = "http://localhost:8000"
TEST_SYMBOLS = ["BERA/USDT", "BTC/USDT", "ETH/USDT", "SOL/USDT"]
TEST_EXCHANGES = ["binance", "bybit", "mexc", "gateio", "bitget", "hyperliquid"]

def test_symbol_utilities():
    """Test the symbol utility functions."""
    print("🔧 Testing Symbol Utilities...")
    
    for symbol in TEST_SYMBOLS:
        print(f"\n  Testing: {symbol}")
        
        # Test validation
        is_valid = validate_symbol(symbol)
        print(f"    ✓ Valid: {is_valid}")
        
        # Test base/quote extraction
        base, quote = get_base_quote(symbol)
        print(f"    ✓ Base: {base}, Quote: {quote}")
        
        # Test exchange formatting
        exchange_symbols = get_exchange_symbols(symbol, TEST_EXCHANGES)
        print(f"    ✓ Exchange formats:")
        for exchange, formatted in exchange_symbols.items():
            print(f"      - {exchange}: {formatted}")
    
    print("\n✅ Symbol utilities test completed!")

def test_api_endpoints():
    """Test key API endpoints."""
    print("\n🌐 Testing API Endpoints...")
    
    endpoints = [
        ("/api/system/health", "Health Check"),
        ("/api/system/dashboard", "Dashboard"),
        ("/api/exchanges/status", "Exchange Status"),
        ("/api/bots/", "Bot List"),
    ]
    
    for endpoint, name in endpoints:
        try:
            response = requests.get(f"{API_BASE_URL}{endpoint}", timeout=10)
            status = "✅" if response.status_code == 200 else "❌"
            print(f"  {status} {name}: HTTP {response.status_code}")
        except requests.exceptions.ConnectionError:
            print(f"  ❌ {name}: Connection failed (API server not running?)")
        except requests.exceptions.Timeout:
            print(f"  ⏰ {name}: Request timed out")
        except Exception as e:
            print(f"  ❌ {name}: {str(e)}")

def test_symbol_validation_api():
    """Test the symbol validation API endpoint."""
    print("\n🔍 Testing Symbol Validation API...")
    
    for symbol in TEST_SYMBOLS[:2]:  # Test first 2 symbols
        try:
            payload = {
                "symbol": symbol,
                "exchanges": ["binance", "hyperliquid"]
            }
            
            response = requests.post(
                f"{API_BASE_URL}/api/strategies/validate-symbol",
                json=payload,
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                print(f"  ✅ {symbol}: Valid={data.get('valid')}, Base={data.get('base_coin')}")
                if data.get('exchange_symbols'):
                    for ex, sym in data['exchange_symbols'].items():
                        print(f"    - {ex}: {sym}")
            else:
                print(f"  ❌ {symbol}: HTTP {response.status_code}")
                
        except requests.exceptions.ConnectionError:
            print(f"  ❌ {symbol}: Connection failed")
        except Exception as e:
            print(f"  ❌ {symbol}: {str(e)}")

def main():
    """Run all tests."""
    print("🚀 Testing Connection Improvements")
    print("=" * 50)
    
    # Test utilities first (no API required)
    test_symbol_utilities()
    
    # Test API endpoints
    test_api_endpoints()
    
    # Test symbol validation API
    test_symbol_validation_api()
    
    print("\n" + "=" * 50)
    print("🎯 Connection improvements test completed!")
    print("\nIf you see connection failures, make sure the API server is running:")
    print("  python api/main.py")

if __name__ == "__main__":
    main() 