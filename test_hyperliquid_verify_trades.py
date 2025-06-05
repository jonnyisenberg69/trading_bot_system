#!/usr/bin/env python
"""
Hyperliquid Trade Verification

Verify if our supposedly executed trades actually exist on mainnet or testnet.
"""

import asyncio
import sys
import json
import aiohttp
from pathlib import Path
import traceback

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from config.exchange_keys import get_exchange_config

async def verify_hyperliquid_trades():
    """Verify if trades exist on mainnet or testnet."""
    
    print("🔍 HYPERLIQUID TRADE VERIFICATION")
    print("=" * 50)
    
    # Test both wallet addresses to find the funded one
    wallet_addresses = [
        {
            'name': 'Configured Wallet',
            'address': '0xFED666533DC4edca120fD5fD58fB309AD3E524d0',
            'source': 'config'
        },
        {
            'name': 'Alternative Wallet', 
            'address': '0x59a7eC7a658777225F7123B2c8420b6D5EC9D64d',
            'source': 'user_provided'
        }
    ]
    
    print(f"\n📋 Testing both wallet addresses:")
    for wallet in wallet_addresses:
        print(f"   {wallet['name']}: {wallet['address']}")
    
    # Test both mainnet and testnet for each wallet
    for env_name, api_url in [
        ("MAINNET", "https://api.hyperliquid.xyz"),
        ("TESTNET", "https://api.hyperliquid-testnet.xyz")
    ]:
        print(f"\n🌐 TESTING {env_name} ({api_url})")
        print("-" * 60)
        
        for wallet in wallet_addresses:
            print(f"\n   📋 {wallet['name']} ({wallet['address']}):")
            
            # Test account state
            await check_account_state(api_url, wallet['address'], f"{env_name}-{wallet['name']}")
            
            # Test trade history
            await check_trade_history(api_url, wallet['address'], f"{env_name}-{wallet['name']}")

async def check_account_state(api_url: str, wallet_address: str, env_name: str):
    """Check account state on given environment."""
    print(f"     📊 Account State:")
    
    try:
        async with aiohttp.ClientSession() as session:
            url = f"{api_url}/info"
            
            payload = {
                "type": "clearinghouseState",
                "user": wallet_address
            }
            
            async with session.post(url, json=payload) as response:
                if response.status == 200:
                    data = await response.json()
                    margin_summary = data.get('marginSummary', {})
                    positions = data.get('assetPositions', [])
                    
                    account_value = margin_summary.get('accountValue', '0')
                    withdrawable = data.get('withdrawable', '0')
                    
                    print(f"       Account Value: ${account_value}")
                    print(f"       Total Positions: {len(positions)}")
                    print(f"       Withdrawable: ${withdrawable}")
                    
                    if float(account_value) > 0:
                        print(f"       🎉 FOUND FUNDED WALLET!")
                        print(f"       💰 This wallet has ${account_value} in value")
                    
                    if positions:
                        print(f"       📊 Positions:")
                        for pos in positions:
                            print(f"         {pos}")
                else:
                    print(f"       ❌ Error {response.status}: {await response.text()}")
                    
    except Exception as e:
        print(f"       ❌ Error: {e}")

async def check_trade_history(api_url: str, wallet_address: str, env_name: str):
    """Check trade history on given environment."""
    print(f"     📈 Trade History:")
    
    try:
        async with aiohttp.ClientSession() as session:
            url = f"{api_url}/info"
            
            payload = {
                "type": "userFills",
                "user": wallet_address
            }
            
            async with session.post(url, json=payload) as response:
                if response.status == 200:
                    trades = await response.json()
                    print(f"       Total Trades: {len(trades)}")
                    
                    if trades:
                        print(f"       🎉 FOUND TRADES! ({len(trades)} total)")
                        print(f"       📊 Recent Trades:")
                        for i, trade in enumerate(trades[-5:]):  # Last 5 trades
                            print(f"         {i+1}. {trade}")
                    else:
                        print(f"       No trades found")
                else:
                    print(f"       ❌ Error {response.status}: {await response.text()}")
                    
    except Exception as e:
        print(f"       ❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(verify_hyperliquid_trades()) 