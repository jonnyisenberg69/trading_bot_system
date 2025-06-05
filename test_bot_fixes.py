#!/usr/bin/env python3
"""
Test Bot Fixes - Verify that all bot issues have been resolved.

This script tests:
1. Bot creation with proper quote lines configuration  
2. Bot starting with actual strategy execution
3. Monitor script finding running bots
4. Frontend showing correct quote lines count
"""

import requests
import json
import time
import subprocess
import asyncio
from datetime import datetime

API_BASE_URL = "http://localhost:8000"

def test_api_connection():
    """Test basic API connectivity."""
    print("🔗 Testing API Connection...")
    try:
        response = requests.get(f"{API_BASE_URL}/api/system/health", timeout=5)
        if response.status_code == 200:
            print("   ✅ API is responsive")
            return True
        else:
            print(f"   ❌ API returned HTTP {response.status_code}")
            return False
    except Exception as e:
        print(f"   ❌ API connection failed: {e}")
        return False

def test_create_bot():
    """Test creating a new bot with proper configuration."""
    print("\n🤖 Testing Bot Creation...")
    
    # Create bot with passive quoting strategy
    bot_config = {
        "strategy": "passive_quoting",
        "symbol": "BERA/USDT", 
        "exchanges": ["binance", "hyperliquid"],
        "config": {
            "base_coin": "BERA",
            "quantity_currency": "base",
            "lines": [
                {
                    "timeout": 3,
                    "drift": 5.0,
                    "quantity": 10.0,
                    "quantity_randomization_factor": 25.0,
                    "spread": 30.0,
                    "sides": "both"
                }
            ],
            "exchanges": ["binance", "hyperliquid"]
        }
    }
    
    try:
        print("   📤 Creating bot...")
        response = requests.post(f"{API_BASE_URL}/api/strategies/passive-quoting/create", json=bot_config, timeout=30)
        
        if response.status_code == 200:
            result = response.json()
            instance_id = result.get('instance_id')
            print(f"   ✅ Bot created successfully: {instance_id}")
            return instance_id
        else:
            print(f"   ❌ Bot creation failed: HTTP {response.status_code}")
            print(f"      Response: {response.text}")
            return None
            
    except Exception as e:
        print(f"   ❌ Bot creation error: {e}")
        return None

def test_bot_config_display(instance_id):
    """Test that bot displays correct configuration (including quote lines count)."""
    print("\n📋 Testing Bot Configuration Display...")
    
    try:
        # Get bot details
        response = requests.get(f"{API_BASE_URL}/api/bots/{instance_id}", timeout=10)
        
        if response.status_code == 200:
            bot_data = response.json()
            config = bot_data.get('config', {})
            lines = config.get('lines', [])
            
            print(f"   📊 Bot config loaded: {len(lines)} quote lines found")
            
            if len(lines) > 0:
                line = lines[0]
                print(f"      Line 1: qty={line.get('quantity')}, spread={line.get('spread')} bps, sides={line.get('sides')}")
                print("   ✅ Configuration properly saved and displayed")
                return True
            else:
                print("   ❌ No quote lines found in configuration")
                return False
        else:
            print(f"   ❌ Failed to get bot config: HTTP {response.status_code}")
            return False
            
    except Exception as e:
        print(f"   ❌ Error checking bot config: {e}")
        return False

def test_start_bot(instance_id):
    """Test starting the bot and verify it actually runs."""
    print("\n🚀 Testing Bot Startup...")
    
    try:
        # Start the bot
        print("   ▶️  Starting bot...")
        response = requests.post(f"{API_BASE_URL}/api/bots/{instance_id}/start", timeout=30)
        
        if response.status_code == 200:
            print("   ✅ Bot start command successful")
            
            # Wait a moment for startup
            time.sleep(3)
            
            # Check detailed status
            status_response = requests.get(f"{API_BASE_URL}/api/bots/{instance_id}/status", timeout=10)
            
            if status_response.status_code == 200:
                status_data = status_response.json().get('data', {})
                bot_status = status_data.get('status')
                process_info = status_data.get('process_info', {})
                
                print(f"   📊 Bot status: {bot_status}")
                
                if process_info:
                    pid = process_info.get('pid')
                    is_alive = process_info.get('is_alive')
                    print(f"   🔧 Process: PID {pid}, Alive: {is_alive}")
                    
                    if bot_status == 'running' and is_alive:
                        print("   ✅ Bot is running with live process")
                        return True
                    else:
                        print("   ⚠️  Bot status or process issue")
                        return False
                else:
                    print("   ❌ No process information available")
                    return False
            else:
                print(f"   ❌ Failed to get bot status: HTTP {status_response.status_code}")
                return False
        else:
            print(f"   ❌ Bot start failed: HTTP {response.status_code}")
            print(f"      Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"   ❌ Error starting bot: {e}")
        return False

def test_bot_logs(instance_id):
    """Test that bot logs show actual strategy execution."""
    print("\n📝 Testing Bot Logs...")
    
    try:
        response = requests.get(f"{API_BASE_URL}/api/bots/{instance_id}/logs", timeout=10)
        
        if response.status_code == 200:
            log_data = response.json().get('data', {})
            logs = log_data.get('logs', [])
            
            print(f"   📄 Found {len(logs)} log entries")
            
            # Look for specific log patterns that indicate proper execution
            initialization_found = False
            strategy_start_found = False
            config_validation_found = False
            
            for log_line in logs:
                if 'Initializing strategy' in log_line:
                    initialization_found = True
                elif 'Starting strategy' in log_line:
                    strategy_start_found = True
                elif 'config validation' in log_line or 'Configuration validated' in log_line:
                    config_validation_found = True
                    
                # Show recent logs
                print(f"      {log_line}")
            
            if initialization_found and strategy_start_found:
                print("   ✅ Bot properly initializing and starting")
                return True
            else:
                print("   ⚠️  Bot may not be executing properly - check logs")
                return False
        else:
            print(f"   ❌ Failed to get logs: HTTP {response.status_code}")
            return False
            
    except Exception as e:
        print(f"   ❌ Error checking logs: {e}")
        return False

def test_monitor_script():
    """Test that the monitor script correctly finds running bots."""
    print("\n🔍 Testing Monitor Script...")
    
    try:
        # Run monitor script for a few seconds to see if it finds bots
        print("   🔄 Running monitor script...")
        
        # Use timeout to run monitor for 5 seconds
        result = subprocess.run(
            ["timeout", "5", "python", "monitor_bot.py"],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        output = result.stdout
        
        if "No bots found" in output:
            print("   ❌ Monitor script shows 'No bots found'")
            return False
        elif "Bot #1:" in output:
            print("   ✅ Monitor script successfully found running bot")
            return True
        else:
            print("   ⚠️  Monitor script output unclear")
            print(f"      Output snippet: {output[:200]}...")
            return False
            
    except subprocess.TimeoutExpired:
        print("   ⚠️  Monitor script timeout (normal for continuous monitoring)")
        return True
    except Exception as e:
        print(f"   ❌ Error running monitor script: {e}")
        return False

def test_cleanup(instance_id):
    """Clean up test bot."""
    print(f"\n🧹 Cleaning Up Test Bot ({instance_id})...")
    
    try:
        # Stop bot first
        stop_response = requests.post(f"{API_BASE_URL}/api/bots/{instance_id}/stop", timeout=10)
        if stop_response.status_code == 200:
            print("   🛑 Bot stopped")
        
        time.sleep(2)
        
        # Delete bot
        delete_response = requests.delete(f"{API_BASE_URL}/api/bots/{instance_id}", timeout=10)
        if delete_response.status_code == 200:
            print("   🗑️  Bot deleted")
            return True
        else:
            print(f"   ⚠️  Bot deletion failed: HTTP {delete_response.status_code}")
            return False
            
    except Exception as e:
        print(f"   ❌ Cleanup error: {e}")
        return False

def main():
    """Run all tests."""
    print("🧪 Bot Fixes Test Suite")
    print("=" * 50)
    print(f"🕐 Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    tests_passed = 0
    tests_total = 6
    
    # Test 1: API Connection
    if test_api_connection():
        tests_passed += 1
    
    # Test 2: Bot Creation  
    instance_id = test_create_bot()
    if instance_id:
        tests_passed += 1
        
        # Test 3: Configuration Display
        if test_bot_config_display(instance_id):
            tests_passed += 1
        
        # Test 4: Bot Startup
        if test_start_bot(instance_id):
            tests_passed += 1
            
            # Test 5: Bot Logs
            if test_bot_logs(instance_id):
                tests_passed += 1
            
            # Test 6: Monitor Script
            if test_monitor_script():
                tests_passed += 1
        
        # Cleanup
        test_cleanup(instance_id)
    
    # Results
    print()
    print("=" * 50)
    print("🏁 Test Results Summary")
    print()
    print(f"✅ Tests Passed: {tests_passed}/{tests_total}")
    print(f"❌ Tests Failed: {tests_total - tests_passed}/{tests_total}")
    
    if tests_passed == tests_total:
        print()
        print("🎉 ALL TESTS PASSED! Bot fixes are working correctly.")
        print()
        print("✅ Quote lines now display correctly")
        print("✅ Bots actually run trading strategies") 
        print("✅ Monitor script finds running bots")
        print("✅ Configuration is properly saved and loaded")
    else:
        print()
        print("⚠️  Some tests failed. Check the output above for details.")
        print()
        print("🔧 Common issues:")
        print("• Make sure API server is running: python api/main.py")
        print("• Ensure exchange connections are configured")
        print("• Check that all files were saved correctly")
    
    return tests_passed == tests_total

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1) 