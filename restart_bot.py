#!/usr/bin/env python3
"""
Restart Bot Script - Restart existing bot to actually run strategy.

This script will stop and restart your bot so it uses the new strategy runner
instead of just simulating.
"""

import asyncio
import requests
import time

API_BASE_URL = "http://localhost:8000"

async def restart_bot():
    """Restart the user's bot to actually run the strategy."""
    print("🤖 Bot Restart Script")
    print("=" * 50)
    
    try:
        # Get list of current bots
        print("📋 Getting current bots...")
        response = requests.get(f"{API_BASE_URL}/api/bots/")
        
        if response.status_code != 200:
            print(f"❌ Failed to get bots: HTTP {response.status_code}")
            return
        
        bots = response.json().get('data', {}).get('instances', [])
        
        if not bots:
            print("ℹ️  No bots found. Create a bot first through the web interface.")
            return
        
        print(f"✅ Found {len(bots)} bot(s)")
        
        for bot in bots:
            instance_id = bot['instance_id']
            status = bot['status']
            symbol = bot['symbol']
            
            print(f"\n🔄 Processing bot: {instance_id}")
            print(f"   Symbol: {symbol}")
            print(f"   Current Status: {status}")
            
            if status in ['running', 'starting']:
                print("   ⏹️  Stopping bot...")
                stop_response = requests.post(f"{API_BASE_URL}/api/bots/{instance_id}/stop")
                
                if stop_response.status_code == 200:
                    print("   ✅ Bot stopped successfully")
                    
                    # Wait a moment for clean shutdown
                    print("   ⏳ Waiting 3 seconds for clean shutdown...")
                    time.sleep(3)
                else:
                    print(f"   ❌ Failed to stop bot: HTTP {stop_response.status_code}")
                    continue
            
            print("   ▶️  Starting bot with new strategy runner...")
            start_response = requests.post(f"{API_BASE_URL}/api/bots/{instance_id}/start")
            
            if start_response.status_code == 200:
                print("   ✅ Bot started successfully with real strategy!")
                
                # Wait a moment then check status
                time.sleep(2)
                
                status_response = requests.get(f"{API_BASE_URL}/api/bots/{instance_id}/status")
                if status_response.status_code == 200:
                    status_data = status_response.json().get('data', {})
                    
                    print(f"   📊 New Status: {status_data.get('status', 'unknown')}")
                    
                    if status_data.get('process_info'):
                        pid = status_data['process_info'].get('pid')
                        is_alive = status_data['process_info'].get('is_alive')
                        print(f"   🔗 Process ID: {pid}")
                        print(f"   💓 Process Alive: {is_alive}")
                        
                        if is_alive:
                            print("   🎉 SUCCESS: Bot is now actually running with real strategy!")
                        else:
                            print("   ⚠️  WARNING: Process not alive - check logs")
                    
            else:
                print(f"   ❌ Failed to start bot: HTTP {start_response.status_code}")
                
        print("\n" + "=" * 50)
        print("🎯 Bot restart complete!")
        print("\n📍 How to monitor your bot:")
        print(f"   • Check logs: GET {API_BASE_URL}/api/bots/{{instance_id}}/logs")
        print(f"   • Check status: GET {API_BASE_URL}/api/bots/{{instance_id}}/status")
        print(f"   • Web interface: http://localhost:3000/bots")
        print("\n📁 Log files location:")
        print(f"   • logs/bots/{{instance_id}}/{{instance_id}}.log")
        
    except Exception as e:
        print(f"❌ Error restarting bots: {e}")

if __name__ == "__main__":
    asyncio.run(restart_bot()) 