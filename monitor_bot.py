#!/usr/bin/env python3
"""
Bot Monitor - Monitor running bot activity in real-time.

This script continuously monitors your bot's status, logs, and activity.
"""

import asyncio
import requests
import time
from datetime import datetime
import os

API_BASE_URL = "http://localhost:8000"

def clear_screen():
    """Clear the terminal screen."""
    os.system('cls' if os.name == 'nt' else 'clear')

async def monitor_bots():
    """Monitor bot activity in real-time."""
    print("🔍 Bot Monitor - Real-time Activity")
    print("Press Ctrl+C to exit")
    print("=" * 60)
    
    try:
        while True:
            clear_screen()
            print("🤖 Trading Bot Monitor")
            print(f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("=" * 60)
            
            # Get all bots
            try:
                response = requests.get(f"{API_BASE_URL}/api/bots/", timeout=5)
                if response.status_code != 200:
                    print(f"❌ API Error: HTTP {response.status_code}")
                    await asyncio.sleep(5)
                    continue
                    
                bots = response.json().get('instances', [])
                
                if not bots:
                    print("ℹ️  No bots found")
                    await asyncio.sleep(5)
                    continue
                
                for i, bot in enumerate(bots):
                    instance_id = bot['instance_id']
                    status = bot['status']
                    symbol = bot['symbol']
                    exchanges = bot.get('exchanges', [])
                    
                    print(f"\n🤖 Bot #{i+1}: {instance_id}")
                    print(f"   Symbol: {symbol}")
                    print(f"   Exchanges: {', '.join(exchanges)}")
                    
                    # Status indicator
                    status_icon = {
                        'running': '🟢',
                        'stopped': '🔴', 
                        'starting': '🟡',
                        'stopping': '🟠',
                        'error': '💥'
                    }.get(status, '❓')
                    
                    print(f"   Status: {status_icon} {status.upper()}")
                    
                    if bot.get('start_time'):
                        start_time = datetime.fromisoformat(bot['start_time'].replace('Z', '+00:00'))
                        uptime = datetime.now().replace(tzinfo=start_time.tzinfo) - start_time
                        print(f"   Uptime: {str(uptime).split('.')[0]}")
                    
                    # Get detailed status
                    try:
                        detail_response = requests.get(f"{API_BASE_URL}/api/bots/{instance_id}/status", timeout=3)
                        if detail_response.status_code == 200:
                            detail_data = detail_response.json().get('data', {})
                            
                            if detail_data.get('process_info'):
                                proc_info = detail_data['process_info']
                                pid = proc_info.get('pid', 'N/A')
                                is_alive = proc_info.get('is_alive', False)
                                
                                print(f"   Process: PID {pid} {'✅ Alive' if is_alive else '❌ Dead'}")
                                
                                if not is_alive and status == 'running':
                                    print("   ⚠️  Status mismatch detected!")
                            
                            last_heartbeat = detail_data.get('last_heartbeat')
                            if last_heartbeat:
                                hb_time = datetime.fromisoformat(last_heartbeat.replace('Z', '+00:00'))
                                hb_ago = datetime.now().replace(tzinfo=hb_time.tzinfo) - hb_time
                                print(f"   Last Heartbeat: {hb_ago.total_seconds():.0f}s ago")
                    except:
                        print("   ⚠️  Could not get detailed status")
                    
                    # Get recent logs
                    if status in ['running', 'starting']:
                        try:
                            log_response = requests.get(f"{API_BASE_URL}/api/bots/{instance_id}/logs?lines=3", timeout=3)
                            if log_response.status_code == 200:
                                log_data = log_response.json().get('data', {})
                                logs = log_data.get('logs', [])
                                
                                if logs:
                                    print("   📋 Recent Logs:")
                                    for log_line in logs[-3:]:  # Show last 3 lines
                                        if log_line.strip():
                                            # Truncate long lines
                                            truncated = (log_line[:50] + '...') if len(log_line) > 50 else log_line
                                            print(f"      {truncated}")
                                else:
                                    print("   📋 No recent logs")
                        except:
                            print("   ⚠️  Could not fetch logs")
                
                print(f"\n{'='*60}")
                print("🔄 Refreshing in 5 seconds... (Ctrl+C to exit)")
                
            except requests.exceptions.ConnectionError:
                print("❌ Cannot connect to API server")
                print("   Make sure the API is running: python api/main.py")
            except Exception as e:
                print(f"❌ Error: {e}")
            
            await asyncio.sleep(5)
            
    except KeyboardInterrupt:
        print("\n\n👋 Monitor stopped by user")
    except Exception as e:
        print(f"\n❌ Monitor error: {e}")

def show_help():
    """Show monitoring help."""
    print("🔍 Bot Monitor Help")
    print("=" * 40)
    print("This script monitors your trading bots in real-time.")
    print("\nWhat you'll see:")
    print("• 🟢 Running - Bot is actively trading")
    print("• 🔴 Stopped - Bot is not running")
    print("• 🟡 Starting - Bot is starting up")
    print("• 💥 Error - Bot encountered an error")
    print("\nInformation displayed:")
    print("• Bot status and uptime")
    print("• Process ID and health")
    print("• Recent log entries")
    print("• Last heartbeat time")
    print("\nManual checks:")
    print("• View full logs: tail -f logs/bots/{instance_id}/{instance_id}.log")
    print("• Check process: ps aux | grep {instance_id}")
    print("• API status: curl http://localhost:8000/api/bots/{instance_id}/status")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] in ['--help', '-h']:
        show_help()
    else:
        asyncio.run(monitor_bots()) 