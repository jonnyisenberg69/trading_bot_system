#!/usr/bin/env python
"""
Debug Gate.io Trade Fetching

Systematic debugging to understand why Gate.io is not returning trades despite successful execution.
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime, timedelta
import json
import time
import logging

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from exchanges.connectors import create_exchange_connector
from config.exchange_keys import get_exchange_config

async def debug_gateio_trade_fetching():
    """Debug Gate.io trade fetching systematically."""
    
    print("🔍 GATE.IO TRADE FETCHING DEBUG")
    print("=" * 60)
    
    exchange_name = 'gateio_spot'
    
    print(f"\n📋 TESTING {exchange_name.upper()}")
    print("-" * 40)
    
    try:
        # Get config and create connector
        config = get_exchange_config(exchange_name)
        if not config:
            print(f"❌ No configuration for {exchange_name}")
            return
            
        connector = create_exchange_connector('gateio', config)
        if not connector:
            print(f"❌ Failed to create {exchange_name} connector")
            return
            
        connected = await connector.connect()
        if not connected:
            print(f"❌ Failed to connect to {exchange_name}")
            return
            
        print(f"✅ Connected to {exchange_name}")
        
        # Test different symbols
        symbols_to_test = ['BTC/USDT', 'ETH/USDT', 'BNB/USDT', 'BTC_USDT']  # Include Gate.io format
        
        for symbol in symbols_to_test:
            print(f"\n  🔧 Testing {symbol}:")
            
            # Test 1: Raw CCXT call with different parameters
            try:
                print(f"    📊 Raw CCXT fetch_my_trades()...")
                
                # Test different time ranges and parameter combinations
                for days_back in [1, 7, 30]:
                    since = datetime.utcnow() - timedelta(days=days_back)
                    since_ms = int(since.timestamp() * 1000)
                    
                    # Try different parameter combinations
                    param_combinations = [
                        {'since': since_ms, 'limit': 100},
                        {'since': since_ms, 'limit': 50},
                        {'limit': 100},  # No since parameter
                        {},  # No parameters
                    ]
                    
                    for i, params in enumerate(param_combinations):
                        try:
                            print(f"      Params {i+1}: {params}")
                            trades = await connector.exchange.fetch_my_trades(
                                symbol=symbol,
                                **params
                            )
                            
                            print(f"        {days_back} days: {len(trades)} trades")
                            
                            if trades:
                                # Show sample trade
                                sample = trades[-1]
                                print(f"        Sample: {sample.get('id')} - {sample.get('side')} {sample.get('amount')} @ ${sample.get('price')} at {sample.get('datetime')}")
                                break
                                
                        except Exception as e:
                            print(f"        Error with params {i+1}: {e}")
                    
                    if trades:
                        break
                
                if not trades:
                    print(f"      ⚠️  No trades found for {symbol}")
                    
            except Exception as e:
                print(f"      ❌ CCXT Error: {e}")
            
            # Test 2: Using our connector method
            try:
                print(f"    📊 Connector get_trade_history()...")
                
                for days_back in [1, 7, 30]:
                    since = datetime.utcnow() - timedelta(days=days_back)
                    
                    trades = await connector.get_trade_history(
                        symbol=symbol,
                        since=since,
                        limit=100
                    )
                    
                    print(f"      {days_back} days: {len(trades) if trades else 0} trades")
                    
                    if trades:
                        sample = trades[-1]
                        print(f"      Sample: {sample.get('id')} - {sample.get('side')} {sample.get('amount')} @ ${sample.get('price')} at {sample.get('datetime')}")
                        break
                
                if not trades:
                    print(f"      ⚠️  No trades found for {symbol}")
                    
            except Exception as e:
                print(f"      ❌ Connector Error: {e}")
            
            # Test 3: Check if symbol exists and has markets
            try:
                print(f"    📊 Market validation...")
                
                # Load markets
                await connector.exchange.load_markets()
                markets = connector.exchange.markets
                
                if symbol in markets:
                    market = markets[symbol]
                    print(f"      ✅ Market exists: {market.get('active', False)} (active)")
                    print(f"      Type: {market.get('type', 'unknown')}")
                    print(f"      Base: {market.get('base', 'unknown')}, Quote: {market.get('quote', 'unknown')}")
                    print(f"      Symbol ID: {market.get('id', 'unknown')}")
                else:
                    print(f"      ❌ Market {symbol} not found")
                    # Find similar symbols
                    similar_symbols = [s for s in markets.keys() if 'BTC' in s and 'USDT' in s][:10]
                    print(f"      Similar BTC/USDT symbols: {similar_symbols}")
                    
            except Exception as e:
                print(f"      ❌ Market validation error: {e}")
        
        # Test 4: Check recent orders/trades without symbol filter
        try:
            print(f"\n  🔧 Testing without symbol filter:")
            
            # Try fetching all recent trades
            for days_back in [1, 7, 30]:
                since = datetime.utcnow() - timedelta(days=days_back)
                since_ms = int(since.timestamp() * 1000)
                
                try:
                    trades = await connector.exchange.fetch_my_trades(
                        since=since_ms,
                        limit=100
                    )
                    
                    print(f"    All symbols ({days_back} days): {len(trades)} trades")
                    
                    if trades:
                        # Group by symbol
                        symbols_found = {}
                        for trade in trades:
                            sym = trade.get('symbol', 'unknown')
                            symbols_found[sym] = symbols_found.get(sym, 0) + 1
                        
                        print(f"    Symbols with trades:")
                        for sym, count in symbols_found.items():
                            print(f"      {sym}: {count} trades")
                            
                        # Show recent trades
                        print(f"    Recent trades:")
                        for i, trade in enumerate(trades[-3:]):
                            print(f"      {i+1}. {trade.get('id')} - {trade.get('symbol')} {trade.get('side')} {trade.get('amount')} @ ${trade.get('price')} at {trade.get('datetime')}")
                        break
                        
                except Exception as e:
                    print(f"    Error ({days_back} days): {e}")
            
        except Exception as e:
            print(f"    ❌ All trades fetch error: {e}")
        
        # Test 5: Check balance to ensure we have trading history
        try:
            print(f"\n  🔧 Testing balance and account info:")
            
            balance = await connector.get_balance()
            print(f"    Balance: {dict(balance) if balance else 'None'}")
            
            # Check for non-zero balances (indicates trading activity)
            non_zero_balances = {k: v for k, v in balance.items() if v > 0} if balance else {}
            if non_zero_balances:
                print(f"    Non-zero balances: {non_zero_balances}")
                print(f"    This suggests trading activity has occurred")
            else:
                print(f"    No non-zero balances found")
                
        except Exception as e:
            print(f"    ❌ Balance check error: {e}")
        
        # Test 6: Try alternative Gate.io specific methods if available
        try:
            print(f"\n  🔧 Testing Gate.io specific API methods:")
            
            # Check what methods are available
            exchange_methods = [method for method in dir(connector.exchange) if 'trade' in method.lower()]
            print(f"    Available trade methods: {exchange_methods[:10]}")
            
            # Try privateGetSpotMyTrades if available
            if hasattr(connector.exchange, 'privateGetSpotMyTrades'):
                print(f"    Trying direct API call...")
                response = await connector.exchange.privateGetSpotMyTrades()
                print(f"    Direct API response: {response}")
                
        except Exception as e:
            print(f"    ❌ Gate.io specific methods error: {e}")
        
        await connector.disconnect()
        
    except Exception as e:
        print(f"❌ Overall error for {exchange_name}: {e}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")

if __name__ == "__main__":
    asyncio.run(debug_gateio_trade_fetching()) 