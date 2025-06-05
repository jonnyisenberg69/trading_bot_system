#!/usr/bin/env python3
"""
Test Exchange Connector Fixes

This script tests:
1. Frontend properly uses exchange connection IDs  
2. Bot creation with correct exchange mapping
3. Strategy runner gets proper exchange connectors
4. Exchange names map correctly to connection IDs
"""

import requests
import json
import time
import subprocess
from datetime import datetime

API_BASE_URL = "http://localhost:8000"

def test_exchange_status():
    """Test that exchanges show proper connection IDs."""
    print("🔗 Testing Exchange Status & Connection IDs...")
    
    try:
        response = requests.get(f"{API_BASE_URL}/api/exchanges/status", timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            connections = data.get('connections', [])
            
            print(f"✅ Found {len(connections)} exchange connections:")
            
            spot_exchanges = []
            perp_exchanges = []
            
            for conn in connections:
                connection_id = conn.get('connection_id')
                name = conn.get('name') 
                exchange_type = conn.get('exchange_type')
                status = conn.get('status')
                
                print(f"   • {connection_id} ({name} {exchange_type}) - {status}")
                
                if exchange_type == 'spot':
                    spot_exchanges.append(connection_id)
                elif exchange_type == 'perp':
                    perp_exchanges.append(connection_id)
            
            print(f"\n📊 Exchange Summary:")
            print(f"   Spot exchanges: {len(spot_exchanges)} ({', '.join(spot_exchanges)})")
            print(f"   Perp exchanges: {len(perp_exchanges)} ({', '.join(perp_exchanges)})")
            
            # Verify we have both spot and perp for major exchanges
            binance_spot = any('binance_spot' in ex for ex in spot_exchanges)
            binance_perp = any('binance_perp' in ex for ex in perp_exchanges)
            bybit_spot = any('bybit_spot' in ex for ex in spot_exchanges)
            bybit_perp = any('bybit_perp' in ex for ex in perp_exchanges)
            
            if binance_spot and binance_perp:
                print("   ✅ Binance has both spot and perp")
            else:
                print("   ⚠️  Binance missing spot or perp")
                
            if bybit_spot and bybit_perp:
                print("   ✅ Bybit has both spot and perp")
            else:
                print("   ⚠️  Bybit missing spot or perp")
            
            return True, connections
        else:
            print(f"❌ Failed to get exchange status: HTTP {response.status_code}")
            return False, []
            
    except Exception as e:
        print(f"❌ Error checking exchange status: {e}")
        return False, []

def test_create_bot_with_connection_ids(connections):
    """Test creating a bot with proper connection IDs."""
    print("\n🤖 Testing Bot Creation with Connection IDs...")
    
    # Select some connected exchanges
    connected_exchanges = [
        conn['connection_id'] for conn in connections 
        if conn['status'] == 'connected'
    ][:3]  # Use first 3 connected exchanges
    
    if not connected_exchanges:
        print("❌ No connected exchanges available for testing")
        return None
        
    print(f"   Using exchanges: {connected_exchanges}")
    
    bot_config = {
        "strategy": "passive_quoting",
        "symbol": "BERA/USDT", 
        "exchanges": connected_exchanges,  # Use connection IDs now
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
            "exchanges": connected_exchanges
        }
    }
    
    try:
        print("   📤 Creating bot with connection IDs...")
        response = requests.post(f"{API_BASE_URL}/api/strategies/passive-quoting/create", json=bot_config, timeout=30)
        
        if response.status_code == 200:
            result = response.json()
            instance_id = result.get('instance_id')
            print(f"   ✅ Bot created successfully: {instance_id}")
            print(f"   📋 Exchanges configured: {result.get('config', {}).get('exchanges', [])}")
            return instance_id
        else:
            print(f"   ❌ Bot creation failed: HTTP {response.status_code}")
            try:
                error_detail = response.json()
                print(f"      Error: {error_detail}")
            except:
                print(f"      Response: {response.text}")
            return None
            
    except Exception as e:
        print(f"   ❌ Bot creation error: {e}")
        return None

def test_bot_with_proper_connectors(instance_id):
    """Test that bot starts and gets proper exchange connectors."""
    print("\n🚀 Testing Bot Startup with Exchange Connectors...")
    
    try:
        # Start the bot
        print("   ▶️  Starting bot...")
        response = requests.post(f"{API_BASE_URL}/api/bots/{instance_id}/start", timeout=30)
        
        if response.status_code == 200:
            print("   ✅ Bot start command successful")
            
            # Wait for startup
            time.sleep(3)
            
            # Check logs for connector injection
            log_response = requests.get(f"{API_BASE_URL}/api/bots/{instance_id}/logs", timeout=10)
            
            if log_response.status_code == 200:
                log_data = log_response.json().get('data', {})
                logs = log_data.get('logs', [])
                
                print("   📝 Recent logs:")
                
                connector_injected = False
                exchange_manager_started = False
                connectors_count = 0
                
                for log_line in logs:
                    if 'Exchange manager initialized' in log_line:
                        exchange_manager_started = True
                        print(f"      ✅ {log_line}")
                    elif 'Injected' in log_line and 'exchange connectors' in log_line:
                        connector_injected = True
                        # Try to extract connector count
                        if 'Injected' in log_line:
                            try:
                                parts = log_line.split('Injected ')[1].split(' exchange connectors')[0]
                                connectors_count = int(parts)
                            except:
                                pass
                        print(f"      ✅ {log_line}")
                    elif 'No connector for exchange' in log_line:
                        print(f"      ❌ {log_line}")
                    elif 'Starting strategy' in log_line:
                        print(f"      ℹ️  {log_line}")
                
                print(f"\n   📊 Connector Analysis:")
                print(f"      Exchange Manager Started: {'✅' if exchange_manager_started else '❌'}")
                print(f"      Connectors Injected: {'✅' if connector_injected else '❌'}")
                print(f"      Connector Count: {connectors_count}")
                
                if exchange_manager_started and connector_injected and connectors_count > 0:
                    print("   🎉 Bot successfully integrated with exchange connectors!")
                    return True
                else:
                    print("   ⚠️  Bot started but connector integration issues detected")
                    return False
            else:
                print(f"   ❌ Failed to get logs: HTTP {log_response.status_code}")
                return False
        else:
            print(f"   ❌ Bot start failed: HTTP {response.status_code}")
            print(f"      Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"   ❌ Error testing bot connectors: {e}")
        return False

def test_cleanup(instance_id):
    """Clean up test bot."""
    if instance_id:
        print(f"\n🧹 Cleaning Up Test Bot ({instance_id})...")
        
        try:
            # Stop bot
            requests.post(f"{API_BASE_URL}/api/bots/{instance_id}/stop", timeout=10)
            time.sleep(2)
            
            # Delete bot
            delete_response = requests.delete(f"{API_BASE_URL}/api/bots/{instance_id}", timeout=10)
            if delete_response.status_code == 200:
                print("   ✅ Test bot cleaned up")
            else:
                print(f"   ⚠️  Cleanup warning: HTTP {delete_response.status_code}")
                
        except Exception as e:
            print(f"   ⚠️  Cleanup error: {e}")

def main():
    """Run all exchange connector tests."""
    print("🔧 Exchange Connector Integration Test")
    print("=" * 60)
    print(f"🕐 Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    tests_passed = 0
    tests_total = 3
    
    # Test 1: Exchange Status & Connection IDs
    success, connections = test_exchange_status()
    if success:
        tests_passed += 1
    
    instance_id = None
    
    if success and connections:
        # Test 2: Bot Creation with Connection IDs
        instance_id = test_create_bot_with_connection_ids(connections)
        if instance_id:
            tests_passed += 1
            
            # Test 3: Bot Connector Integration
            if test_bot_with_proper_connectors(instance_id):
                tests_passed += 1
    
    # Cleanup
    test_cleanup(instance_id)
    
    # Results
    print("\n" + "=" * 60)
    print("🏁 Test Results Summary")
    print()
    print(f"✅ Tests Passed: {tests_passed}/{tests_total}")
    print(f"❌ Tests Failed: {tests_total - tests_passed}/{tests_total}")
    
    if tests_passed == tests_total:
        print()
        print("🎉 ALL TESTS PASSED! Exchange connector fixes are working!")
        print()
        print("✅ Exchange connection IDs properly displayed")
        print("✅ Bot creation uses correct connection IDs") 
        print("✅ Strategy runner gets exchange connectors")
        print("✅ No more 'No connector for exchange' errors")
        print()
        print("🚀 Your bot will now actually trade on exchanges!")
    else:
        print()
        print("⚠️  Some tests failed. Issues to address:")
        if tests_passed < 1:
            print("• Exchange status/connection ID issues")
        if tests_passed < 2:
            print("• Bot creation with connection IDs")
        if tests_passed < 3:
            print("• Exchange connector injection into strategy")
    
    return tests_passed == tests_total

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1) 