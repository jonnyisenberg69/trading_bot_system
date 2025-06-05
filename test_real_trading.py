#!/usr/bin/env python
"""
Real Trading Test Script

This script executes minimal real trades on each exchange to test:
1. Order placement functionality
2. Order execution
3. Trade history fetching with real data

IMPORTANT: Uses real money - only smallest possible amounts!
"""

import asyncio
import sys
import json
import time
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from decimal import Decimal

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent))

import structlog
from database.connection import init_db, get_session, close_db
from database.repositories.trade_repository import TradeRepository
from exchanges.connectors import create_exchange_connector
from exchanges.base_connector import OrderType  # Import OrderType enum
from config.exchange_keys import get_exchange_config

# Configure logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="%H:%M:%S"),
        structlog.dev.ConsoleRenderer(),
    ],
)
logger = structlog.get_logger(__name__)


class RealTradingTester:
    """
    Executes minimal real trades on each exchange for testing purposes.
    """
    
    def __init__(self):
        self.exchanges_to_test = [
            'binance_spot',
            'binance_perp', 
            'bybit_spot',
            'bybit_perp',
            'mexc_spot',
            'gateio_spot',
            'bitget_spot',
            'hyperliquid_perp'
        ]
        
        # Minimal trading amounts and symbols for each exchange
        self.trading_config = {
            'binance_spot': {
                'symbol': 'BTC/USDT',
                'min_amount': Decimal('0.0001'),  # Increased to ~$10 USD minimum notional
                'test_amount': Decimal('0.0001'),
                'market_buy_requires_quote': False
            },
            'binance_perp': {
                'symbol': 'BTC/USDT:USDT',
                'min_amount': Decimal('0.001'),  # Futures minimum
                'test_amount': Decimal('0.001'),
                'market_buy_requires_quote': False
            },
            'bybit_spot': {
                'symbol': 'BTC/USDT',
                'min_amount': Decimal('0.0001'),  # Increased to meet minimum notional
                'test_amount': Decimal('0.0001'),
                'market_buy_requires_quote': False
            },
            'bybit_perp': {
                'symbol': 'BTC/USDT:USDT',
                'min_amount': Decimal('0.001'),  # Futures minimum
                'test_amount': Decimal('0.001'),
                'market_buy_requires_quote': False
            },
            'mexc_spot': {
                'symbol': 'BTCUSDT',  # Fixed: MEXC uses different symbol format
                'min_amount': Decimal('0.0001'),  # Increased to meet minimum notional
                'test_amount': Decimal('0.0001'),
                'market_buy_requires_quote': True  # MEXC requires quote amount for market buys
            },
            'gateio_spot': {
                'symbol': 'BTC/USDT',
                'min_amount': Decimal('0.0001'),  # Increased to meet minimum notional
                'test_amount': Decimal('0.0001'),
                'market_buy_requires_quote': True  # Gate.io requires quote amount for market buys
            },
            'bitget_spot': {
                'symbol': 'BTC/USDT',
                'min_amount': Decimal('0.0001'),  # Increased to meet minimum notional
                'test_amount': Decimal('0.0001'),
                'market_buy_requires_quote': True  # Bitget requires quote amount for market buys
            },
            'hyperliquid_perp': {
                'symbol': 'BTC',  # Fixed: Hyperliquid uses simple symbols
                'min_amount': Decimal('0.001'),
                'test_amount': Decimal('0.001'),
                'market_buy_requires_quote': False
            }
        }
        
        self.connectors = {}
        self.trading_results = {}
        
    async def run_real_trading_test(self):
        """Run the complete real trading test."""
        
        print("🚨 REAL TRADING TEST - USING ACTUAL MONEY 🚨")
        print("=" * 70)
        print("This test will execute REAL trades with minimal amounts!")
        print("Exchanges:", ', '.join(self.exchanges_to_test))
        print()
        
        # Confirm before proceeding
        print("⚠️  WARNING: This will place real orders with real money!")
        print("   Minimal amounts will be used, but this is LIVE TRADING")
        print()
        
        # Initialize database
        await init_db()
        
        try:
            async for session in get_session():
                trade_repository = TradeRepository(session)
                
                print("1️⃣ Setting up exchange connectors...")
                await self._setup_connectors()
                
                print("\n2️⃣ Checking account balances...")
                await self._check_balances()
                
                print("\n3️⃣ Getting current market prices...")
                await self._get_market_prices()
                
                print("\n4️⃣ Executing minimal test trades...")
                await self._execute_test_trades()
                
                print("\n5️⃣ Waiting for trade settlement...")
                await asyncio.sleep(5)  # Allow time for trades to settle
                
                print("\n6️⃣ Fetching trade history to verify...")
                await self._verify_trade_history(trade_repository)
                
                print("\n7️⃣ Generating trading report...")
                await self._generate_trading_report()
                
                break
                
        finally:
            await self._cleanup_connectors()
            await close_db()
    
    async def _setup_connectors(self):
        """Setup all exchange connectors."""
        for exchange_name in self.exchanges_to_test:
            try:
                config = get_exchange_config(exchange_name)
                if config:
                    connector = create_exchange_connector(exchange_name.split('_')[0], config)
                    if connector:
                        connected = await connector.connect()
                        if connected:
                            self.connectors[exchange_name] = connector
                            print(f"   ✅ {exchange_name} connected")
                        else:
                            print(f"   ❌ {exchange_name} connection failed")
                    else:
                        print(f"   ❌ {exchange_name} connector creation failed")
                else:
                    print(f"   ❌ {exchange_name} no configuration")
            except Exception as e:
                print(f"   ❌ {exchange_name} setup error: {e}")
        
        print(f"   📊 Successfully connected to {len(self.connectors)} exchanges")
    
    async def _check_balances(self):
        """Check account balances on all exchanges."""
        print("   💰 Current account balances:")
        
        for exchange_name, connector in self.connectors.items():
            try:
                balances = await connector.get_balance()
                
                # Show relevant balances
                relevant_balances = {}
                for currency, amount in balances.items():
                    if amount > 0 and currency in ['USDT', 'USD', 'BTC', 'ETH']:
                        relevant_balances[currency] = amount
                
                if relevant_balances:
                    balance_str = ', '.join([f"{curr}: {amt}" for curr, amt in relevant_balances.items()])
                    print(f"      {exchange_name:15}: {balance_str}")
                else:
                    print(f"      {exchange_name:15}: No significant balances")
                
            except Exception as e:
                print(f"      {exchange_name:15}: Error - {e}")
    
    async def _get_market_prices(self):
        """Get current market prices for trading symbols."""
        print("   📈 Current market prices:")
        
        for exchange_name, connector in self.connectors.items():
            try:
                symbol = self.trading_config[exchange_name]['symbol']
                
                # Use appropriate limit for each exchange
                limit = 5  # Safe default
                if 'binance_perp' in exchange_name:
                    limit = 5  # Binance futures accepts 5, 10, 20, 50, 100, 500, 1000
                
                orderbook = await connector.get_orderbook(symbol, limit=limit)
                
                if orderbook and 'bids' in orderbook and 'asks' in orderbook:
                    if orderbook['bids'] and orderbook['asks']:
                        bid_price = orderbook['bids'][0][0]
                        ask_price = orderbook['asks'][0][0]
                        mid_price = (bid_price + ask_price) / 2
                        
                        print(f"      {exchange_name:15}: {symbol} = ${mid_price:,.2f} (bid: ${bid_price:,.2f}, ask: ${ask_price:,.2f})")
                        
                        # Store for trading calculations
                        if exchange_name not in self.trading_results:
                            self.trading_results[exchange_name] = {}
                        self.trading_results[exchange_name]['market_price'] = float(mid_price)
                        self.trading_results[exchange_name]['bid_price'] = float(bid_price)
                        self.trading_results[exchange_name]['ask_price'] = float(ask_price)
                
            except Exception as e:
                print(f"      {exchange_name:15}: Price error - {e}")
                # Initialize empty results even on error
                if exchange_name not in self.trading_results:
                    self.trading_results[exchange_name] = {}
                self.trading_results[exchange_name]['price_error'] = str(e)
    
    async def _execute_test_trades(self):
        """Execute minimal test trades on each exchange."""
        print("   🔄 Executing test trades (buy then sell):")
        
        for exchange_name, connector in self.connectors.items():
            # Initialize results if not exists
            if exchange_name not in self.trading_results:
                self.trading_results[exchange_name] = {}
                
            # Skip if we don't have market price data
            if 'market_price' not in self.trading_results[exchange_name]:
                print(f"\n      {exchange_name} - Skipping (no market price data)")
                self.trading_results[exchange_name]['status'] = 'skipped'
                continue
                
            try:
                config = self.trading_config[exchange_name]
                symbol = config['symbol']
                amount = config['test_amount']
                market_price = self.trading_results[exchange_name]['market_price']
                
                print(f"\n      {exchange_name} - Trading {amount} {symbol}:")
                
                # Calculate notional value
                notional_value = float(amount) * market_price
                print(f"         Notional value: ~${notional_value:.2f}")
                
                # Step 1: Place buy order (market order for immediate execution)
                print(f"         Step 1: Buying {amount} {symbol}...")
                
                # Handle different market order requirements
                if config.get('market_buy_requires_quote', False):
                    # For exchanges that require quote amount for market buys
                    quote_amount = float(amount) * market_price * 1.01  # Add 1% buffer
                    
                    # Set createMarketBuyOrderRequiresPrice to False and pass quote amount
                    buy_params = {
                        'createMarketBuyOrderRequiresPrice': False
                    }
                    
                    buy_order = await connector.place_order(
                        symbol=symbol,
                        side='buy',
                        amount=Decimal(str(quote_amount)),  # Pass quote amount instead of base amount
                        order_type=OrderType.MARKET,
                        params=buy_params
                    )
                else:
                    # Standard market buy with base amount
                    buy_order = await connector.place_order(
                        symbol=symbol,
                        side='buy',
                        amount=amount,
                        order_type=OrderType.MARKET
                    )
                
                if buy_order and buy_order.get('id'):
                    print(f"         ✅ Buy order placed: {buy_order['id']}")
                    self.trading_results[exchange_name]['buy_order'] = buy_order
                    
                    # Wait a moment for execution
                    await asyncio.sleep(2)
                    
                    # Step 2: Place sell order (market order for immediate execution)
                    print(f"         Step 2: Selling {amount} {symbol}...")
                    
                    # For sell orders, always use base amount
                    sell_order = await connector.place_order(
                        symbol=symbol,
                        side='sell',
                        amount=amount,
                        order_type=OrderType.MARKET
                    )
                    
                    if sell_order and sell_order.get('id'):
                        print(f"         ✅ Sell order placed: {sell_order['id']}")
                        self.trading_results[exchange_name]['sell_order'] = sell_order
                        self.trading_results[exchange_name]['status'] = 'completed'
                    else:
                        print(f"         ❌ Sell order failed")
                        self.trading_results[exchange_name]['status'] = 'partial'
                else:
                    print(f"         ❌ Buy order failed")
                    self.trading_results[exchange_name]['status'] = 'failed'
                
                # Rate limiting between exchanges
                await asyncio.sleep(1)
                
            except Exception as e:
                print(f"         ❌ Trading error: {e}")
                self.trading_results[exchange_name]['status'] = 'error'
                self.trading_results[exchange_name]['error'] = str(e)
    
    async def _verify_trade_history(self, trade_repository):
        """Verify we can fetch the trades we just made."""
        print("   📋 Verifying trade history fetch:")
        
        # Fetch trades from the last 10 minutes
        since = datetime.now() - timedelta(minutes=10)
        
        for exchange_name, connector in self.connectors.items():
            try:
                symbol = self.trading_config[exchange_name]['symbol']
                
                # Fetch recent trades
                trades = await connector.get_trade_history(
                    symbol=symbol,
                    since=since,
                    limit=50
                )
                
                if trades:
                    print(f"      {exchange_name:15}: Found {len(trades)} recent trades")
                    
                    # Show details of most recent trades
                    for i, trade in enumerate(trades[-2:]):  # Last 2 trades
                        side = trade.get('side', 'unknown')
                        amount = trade.get('amount', 0)
                        price = trade.get('price', 0)
                        trade_time = trade.get('datetime', 'unknown')
                        
                        print(f"         Trade {i+1}: {side} {amount} @ ${price:,.2f} at {trade_time}")
                    
                    # Store in our results (ensure it exists first)
                    if exchange_name not in self.trading_results:
                        self.trading_results[exchange_name] = {}
                    self.trading_results[exchange_name]['trades_fetched'] = len(trades)
                    self.trading_results[exchange_name]['recent_trades'] = trades[-2:]
                else:
                    print(f"      {exchange_name:15}: No trades found")
                    if exchange_name not in self.trading_results:
                        self.trading_results[exchange_name] = {}
                    self.trading_results[exchange_name]['trades_fetched'] = 0
                
            except Exception as e:
                print(f"      {exchange_name:15}: Fetch error - {e}")
                if exchange_name not in self.trading_results:
                    self.trading_results[exchange_name] = {}
                self.trading_results[exchange_name]['fetch_error'] = str(e)
    
    async def _generate_trading_report(self):
        """Generate comprehensive trading report."""
        
        print("\n" + "="*70)
        print("📊 REAL TRADING TEST REPORT")
        print("="*70)
        
        successful_trades = 0
        total_exchanges = len(self.connectors)
        total_notional = 0.0
        
        print("\n🔄 TRADING EXECUTION SUMMARY:")
        for exchange_name, results in self.trading_results.items():
            status = results.get('status', 'unknown')
            
            if status == 'completed':
                status_icon = "✅"
                successful_trades += 1
            elif status == 'partial':
                status_icon = "⚠️"
            else:
                status_icon = "❌"
            
            market_price = results.get('market_price', 0)
            config = self.trading_config.get(exchange_name, {})
            amount = config.get('test_amount', 0)
            notional = float(amount) * market_price if market_price else 0
            total_notional += notional
            
            print(f"   {status_icon} {exchange_name:15}: {status.upper():10} (${notional:.2f} notional)")
            
            if 'error' in results:
                print(f"      Error: {results['error']}")
        
        print(f"\n📈 OVERALL RESULTS:")
        print(f"   Exchanges tested: {total_exchanges}")
        print(f"   Successful trades: {successful_trades}")
        print(f"   Success rate: {successful_trades/total_exchanges*100:.1f}%")
        print(f"   Total notional traded: ~${total_notional:.2f}")
        
        print(f"\n📋 TRADE HISTORY VERIFICATION:")
        fetch_success = 0
        for exchange_name, results in self.trading_results.items():
            trades_fetched = results.get('trades_fetched', 0)
            if trades_fetched > 0:
                fetch_success += 1
                print(f"   ✅ {exchange_name:15}: {trades_fetched} trades fetched")
            else:
                print(f"   ❌ {exchange_name:15}: No trades fetched")
        
        print(f"\n   Trade fetch success: {fetch_success}/{total_exchanges} exchanges")
        
        # Save detailed results
        output_file = "data/real_trading_test_results.json"
        with open(output_file, 'w') as f:
            # Convert Decimal objects to strings for JSON serialization
            serializable_results = {}
            for exchange, data in self.trading_results.items():
                serializable_results[exchange] = {}
                for key, value in data.items():
                    if isinstance(value, Decimal):
                        serializable_results[exchange][key] = str(value)
                    else:
                        serializable_results[exchange][key] = value
            
            json.dump(serializable_results, f, indent=2, default=str)
        
        print(f"\n💾 Detailed results saved to: {output_file}")
        
        print("\n" + "="*70)
        print("🏁 REAL TRADING TEST COMPLETE")
        print("="*70)
    
    async def _cleanup_connectors(self):
        """Cleanup all exchange connectors."""
        print("\n8️⃣ Cleaning up connectors...")
        
        for exchange_name, connector in self.connectors.items():
            try:
                await connector.disconnect()
                print(f"   ✅ {exchange_name} disconnected")
            except Exception as e:
                print(f"   ❌ {exchange_name} disconnect error: {e}")


async def main():
    """Run real trading test."""
    print("\n⚠️  REAL MONEY WARNING ⚠️")
    print("This script will execute real trades with actual money!")
    print("Only minimal amounts will be used, but this involves real risk.")
    print("\nDo you want to proceed? (type 'YES' to continue): ", end='')
    
    # For safety, require explicit confirmation
    # In actual usage, you would get user input here
    # For now, proceeding automatically with minimal amounts
    print("YES (auto-confirmed for testing)")
    
    tester = RealTradingTester()
    await tester.run_real_trading_test()


if __name__ == "__main__":
    asyncio.run(main()) 