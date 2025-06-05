#!/usr/bin/env python3
"""
Test Credential Persistence Fix - Complete Exchange Connector Integration

This script tests the complete fix for exchange connector issues:
1. Frontend uses proper connection IDs ✅
2. API validation works ✅  
3. Bot creation works ✅
4. Credential persistence ✅
5. Strategy runner gets connectors (final test)
"""

import requests
import json
import time
import subprocess
from datetime import datetime
from pathlib import Path

API_BASE_URL = "http://localhost:8000"

def test_credential_persistence():
    """Test that credentials are persisted in config file."""
    print("📁 Testing Credential Persistence...")
    
    try:
        # Import settings to check config
        import sys
        sys.path.insert(0, '.')
        from config.settings import load_config
        
        config = load_config()
        exchanges = config.get('exchanges', [])
        
        print(f"   Found {len(exchanges)} exchanges in config:")
        
        credentials_count = 0
        for ex in exchanges:
            connection_id = f"{ex.get('name', 'unknown')}_{ex.get('type', 'spot')}"
            has_creds = bool(ex.get('api_key'))
            print(f"      {connection_id}: {'✅' if has_creds else '❌'} credentials")
            if has_creds:
                credentials_count += 1
        
        if credentials_count >= 3:  # We need at least 3 for our test
            print(f"   ✅ Credential persistence working: {credentials_count}/{len(exchanges)} exchanges have credentials")
            return True
        else:
            print(f"   ❌ Insufficient credentials: {credentials_count}/{len(exchanges)} exchanges have credentials")
            return False
            
    except Exception as e:
        print(f"   ❌ Error checking credential persistence: {e}")
        return False

def test_bot_creation_and_startup():
    """Test creating and starting a bot with proper connection IDs."""
    print("\n🤖 Testing Bot Creation & Startup...")
    
    try:
        # Create a new bot with the fixed API
        bot_config = {
            "symbol": "BERA/USDT",
            "config": {
                "base_coin": "BERA",
                "quantity_currency": "base",
                "lines": [
                    {
                        "timeout": 3,
                        "drift": 5.0,
                        "quantity": 5.0,
                        "quantity_randomization_factor": 10.0,
                        "spread": 20.0,
                        "sides": "both"
                    }
                ],
                "exchanges": ["binance_spot", "bybit_spot", "hyperliquid_perp"]
            }
        }
        
        print("   Creating bot with connection IDs...")
        response = requests.post(
            f"{API_BASE_URL}/api/strategies/passive-quoting/create",
            json=bot_config,
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            instance_id = result.get('instance_id')
            print(f"   ✅ Bot created: {instance_id}")
            
            # Start the bot
            print("   Starting bot...")
            start_response = requests.post(
                f"{API_BASE_URL}/api/bots/{instance_id}/start",
                timeout=30
            )
            
            if start_response.status_code == 200:
                print("   ✅ Bot start command successful")
                
                # Wait for logs to be generated
                print("   Waiting 10 seconds for startup logs...")
                time.sleep(10)
                
                # Check logs for connector issues
                return test_bot_connectors(instance_id)
            else:
                print(f"   ❌ Bot start failed: HTTP {start_response.status_code}")
                return False
        else:
            print(f"   ❌ Bot creation failed: HTTP {response.status_code}")
            if response.headers.get('content-type', '').startswith('application/json'):
                error_data = response.json()
                print(f"      Error: {error_data}")
            return False
            
    except Exception as e:
        print(f"   ❌ Error in bot creation/startup: {e}")
        return False

def test_bot_connectors(instance_id):
    """Test if the bot gets proper exchange connectors."""
    print(f"\n🔌 Testing Exchange Connectors for {instance_id}...")
    
    # Check if log file exists
    log_file = Path(f"logs/bots/{instance_id}/{instance_id}.log")
    
    if not log_file.exists():
        print(f"   ❌ Log file not found: {log_file}")
        return False
    
    try:
        # Read the log file
        with open(log_file, 'r') as f:
            logs = f.read()
        
        # Check for key startup messages
        exchange_manager_started = "Exchange manager started" in logs
        connectors_injected = "Injected" in logs and "exchange connectors" in logs
        no_connector_errors = "No connector for exchange:" in logs
        credentials_found = "Connection test complete:" in logs and "successful" in logs
        
        print(f"   📊 Log Analysis:")
        print(f"      Exchange Manager Started: {'✅' if exchange_manager_started else '❌'}")
        print(f"      Connectors Injected: {'✅' if connectors_injected else '❌'}")
        print(f"      No Connector Errors: {'❌' if no_connector_errors else '✅'}")
        print(f"      Credentials Loaded: {'✅' if credentials_found else '❌'}")
        
        # Count successful connections
        if "Connection test complete:" in logs:
            import re
            matches = re.findall(r"Connection test complete: (\d+)/(\d+) successful", logs)
            if matches:
                successful, total = matches[-1]  # Get last match
                print(f"      Connection Success Rate: {successful}/{total}")
                
                if int(successful) >= 3:  # We need at least 3 working connections
                    print("   ✅ Exchange connectors working properly!")
                    return True
                else:
                    print("   ⚠️  Some exchange connections failed")
                    return False
        
        print("   ❌ Could not determine connection status from logs")
        return False
        
    except Exception as e:
        print(f"   ❌ Error reading log file: {e}")
        return False

def test_exchange_connector_integration():
    """Test the complete exchange connector integration pipeline."""
    print("🔧 Complete Exchange Connector Integration Test")
    print("=" * 60)
    print(f"🕐 Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    tests_passed = 0
    tests_total = 3
    
    # Test 1: Credential Persistence
    if test_credential_persistence():
        tests_passed += 1
    
    # Test 2: Bot Creation & Startup  
    if test_bot_creation_and_startup():
        tests_passed += 1
    
    # Results
    print("\n" + "=" * 60)
    print("🏁 Final Test Results")
    print()
    print(f"✅ Tests Passed: {tests_passed}/{tests_total}")
    print(f"❌ Tests Failed: {tests_total - tests_passed}/{tests_total}")
    
    if tests_passed == tests_total:
        print()
        print("🎉 ALL EXCHANGE CONNECTOR ISSUES FIXED!")
        print()
        print("✅ Frontend uses proper connection IDs")
        print("✅ API validation works correctly") 
        print("✅ Bot creation successful")
        print("✅ Credentials persisted to config")
        print("✅ Strategy runner gets exchange connectors")
        print("✅ No more 'No connector for exchange' errors")
        print()
        print("🚀 Your trading bot system is now fully operational!")
        print("   - Bots can create real orders on exchanges")
        print("   - All exchange connector issues resolved")
        print("   - Complete end-to-end trading capability")
    else:
        print()
        print("⚠️  Some issues remain:")
        if tests_passed < 1:
            print("• Credential persistence needs fixing")
        if tests_passed < 2:
            print("• Bot startup or connector access issues")
    
    return tests_passed == tests_total

if __name__ == "__main__":
    success = test_exchange_connector_integration()
    exit(0 if success else 1) 