#!/usr/bin/env python3
"""
Show all current orderbooks across all connected exchanges.

Uses the existing market data infrastructure to display real-time orderbook data.
"""

import asyncio
import sys
from pathlib import Path
from typing import Dict, List, Any
from decimal import Decimal

# Add project root to path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import load_config
from exchanges.connectors import create_exchange_connector
from market_data.orderbook_analyzer import OrderbookAnalyzer, OrderbookData
from market_data.market_logger import MarketLogger
import structlog

logger = structlog.get_logger(__name__)


async def show_all_orderbooks():
    """Show all current orderbooks from connected exchanges."""
    print("🔍 REAL-TIME ORDERBOOK DATA ACROSS ALL EXCHANGES")
    print("=" * 80)
    
    # Load configuration
    config = load_config()
    
    # Initialize orderbook analyzer
    analyzer = OrderbookAnalyzer()
    await analyzer.start()
    
    # Common symbols to check
    symbols = ['BTC/USDT', 'ETH/USDT', 'BERA/USDT', 'SOL/USDT']
    
    # Connect to all exchanges and fetch orderbooks
    exchange_connectors = {}
    
    print("📡 Connecting to exchanges...")
    for exchange_config in config.get('exchanges', []):
        exchange_name = exchange_config['name']
        try:
            print(f"  Connecting to {exchange_name}...")
            connector = create_exchange_connector(exchange_config)
            if connector:
                exchange_connectors[exchange_name] = connector
                print(f"  ✅ {exchange_name} connected")
            else:
                print(f"  ❌ {exchange_name} failed to connect")
        except Exception as e:
            print(f"  ❌ {exchange_name} error: {e}")
    
    print(f"\n📊 Connected to {len(exchange_connectors)} exchanges")
    print("=" * 80)
    
    # Fetch orderbooks for each symbol
    for symbol in symbols:
        print(f"\n🎯 ORDERBOOK DATA FOR {symbol}")
        print("-" * 60)
        
        symbol_data = []
        
        # Fetch from each exchange
        for exchange_name, connector in exchange_connectors.items():
            try:
                # Check if symbol is available on this exchange
                markets = await connector.get_markets()
                if symbol not in markets:
                    continue
                
                # Fetch orderbook
                orderbook_data = await analyzer.fetch_orderbook(connector, symbol, depth=10)
                
                if orderbook_data:
                    symbol_data.append(orderbook_data)
                    
                    # Display orderbook
                    print(f"\n📈 {exchange_name.upper()}")
                    print(f"   Symbol: {orderbook_data.symbol}")
                    print(f"   Time: {orderbook_data.datetime.strftime('%H:%M:%S')}")
                    
                    if orderbook_data.best_bid and orderbook_data.best_ask:
                        print(f"   Best Bid: ${orderbook_data.best_bid:,.8f}")
                        print(f"   Best Ask: ${orderbook_data.best_ask:,.8f}")
                        print(f"   Midprice: ${orderbook_data.midprice:,.8f}")
                        print(f"   Spread: ${orderbook_data.spread:,.8f} ({orderbook_data.spread_pct:.4f}%)")
                        
                        # Show top 3 levels
                        print(f"   Top 3 Levels:")
                        print(f"     ASKS:")
                        for i, (price, amount) in enumerate(orderbook_data.asks[:3]):
                            print(f"       {i+1}. ${price:,.8f}  {amount:,.6f}")
                        print(f"     BIDS:")
                        for i, (price, amount) in enumerate(orderbook_data.bids[:3]):
                            print(f"       {i+1}. ${price:,.8f}  {amount:,.6f}")
                        
                        # Liquidity analysis
                        bid_liq, ask_liq = orderbook_data.get_liquidity(Decimal('0.01'))  # 1% depth
                        print(f"   Liquidity (1% depth): {bid_liq:.6f} / {ask_liq:.6f}")
                    else:
                        print(f"   ❌ No valid bid/ask data")
                        
            except Exception as e:
                print(f"   ❌ {exchange_name}: Error fetching orderbook - {e}")
        
        # Cross-exchange analysis
        if len(symbol_data) > 1:
            print(f"\n🔄 CROSS-EXCHANGE ANALYSIS FOR {symbol}")
            print("-" * 40)
            
            # Find best prices
            best_bids = [(ob.exchange, ob.best_bid) for ob in symbol_data if ob.best_bid]
            best_asks = [(ob.exchange, ob.best_ask) for ob in symbol_data if ob.best_ask]
            
            if best_bids and best_asks:
                # Highest bid
                best_bid_exchange, best_bid_price = max(best_bids, key=lambda x: x[1])
                print(f"   🟢 Best Bid: {best_bid_exchange} @ ${best_bid_price:,.8f}")
                
                # Lowest ask
                best_ask_exchange, best_ask_price = min(best_asks, key=lambda x: x[1])
                print(f"   🔴 Best Ask: {best_ask_exchange} @ ${best_ask_price:,.8f}")
                
                # Cross-exchange spread
                cross_spread = best_ask_price - best_bid_price
                cross_spread_pct = (cross_spread / ((best_bid_price + best_ask_price) / 2)) * 100
                print(f"   📊 Cross-Exchange Spread: ${cross_spread:,.8f} ({cross_spread_pct:.4f}%)")
                
                # Arbitrage opportunity
                if cross_spread < 0:
                    profit_pct = abs(cross_spread_pct)
                    print(f"   💰 ARBITRAGE OPPORTUNITY: {profit_pct:.4f}% profit!")
                    print(f"       Buy on {best_ask_exchange} @ ${best_ask_price:,.8f}")
                    print(f"       Sell on {best_bid_exchange} @ ${best_bid_price:,.8f}")
                
                # Price differences
                print(f"   📈 Price Differences from Best:")
                midpoint = (best_bid_price + best_ask_price) / 2
                for ob in symbol_data:
                    if ob.midprice:
                        diff_pct = ((ob.midprice - midpoint) / midpoint) * 100
                        print(f"       {ob.exchange}: {diff_pct:+.4f}%")
        
        print("-" * 60)
    
    # Summary table
    print(f"\n📋 ORDERBOOK SUMMARY")
    print("=" * 80)
    
    # Create summary table
    headers = ["Exchange", "Symbol", "Bid", "Ask", "Spread", "Spread %"]
    rows = []
    
    for symbol in symbols:
        for exchange_name, connector in exchange_connectors.items():
            try:
                markets = await connector.get_markets()
                if symbol not in markets:
                    continue
                    
                orderbook_data = analyzer.get_orderbook(exchange_name, symbol)
                if orderbook_data and orderbook_data.best_bid and orderbook_data.best_ask:
                    rows.append([
                        exchange_name,
                        symbol,
                        f"${orderbook_data.best_bid:,.6f}",
                        f"${orderbook_data.best_ask:,.6f}",
                        f"${orderbook_data.spread:,.6f}",
                        f"{orderbook_data.spread_pct:.4f}%"
                    ])
            except:
                continue
    
    # Print table
    if rows:
        from tabulate import tabulate
        print(tabulate(rows, headers=headers, tablefmt="grid"))
    
    # Cleanup
    await analyzer.stop()
    
    # Close connectors
    for connector in exchange_connectors.values():
        try:
            await connector.close()
        except:
            pass
    
    print("\n✅ Orderbook analysis complete!")


if __name__ == "__main__":
    asyncio.run(show_all_orderbooks()) 