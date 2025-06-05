#!/usr/bin/env python
import asyncio
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from database.connection import init_db, get_session, close_db
from database.models import Trade, Exchange
from sqlalchemy import select, func, desc

async def calculate_positions():
    try:
        await init_db()
        print("📊 NET POSITION ANALYSIS")
        print("=" * 50)
        
        async for session in get_session():
            # Get exchange mappings
            stmt = select(Exchange)
            result = await session.execute(stmt)
            exchanges = result.scalars().all()
            
            exchange_map = {ex.id: ex.name for ex in exchanges}
            print(f"Exchange mappings: {exchange_map}")
            
            # Get all trades
            stmt = select(Trade).order_by(desc(Trade.timestamp))
            result = await session.execute(stmt)
            all_trades = result.scalars().all()
            
            print(f"\nTotal trades in database: {len(all_trades)}")
            
            # Calculate net positions by exchange and symbol
            positions = {}
            
            for trade in all_trades:
                exchange_name = exchange_map.get(trade.exchange_id, f"Unknown_{trade.exchange_id}")
                symbol = trade.symbol
                
                key = f"{exchange_name}_{symbol}"
                
                if key not in positions:
                    positions[key] = {
                        'exchange': exchange_name,
                        'symbol': symbol,
                        'volume': 0.0,
                        'cost': 0.0,
                        'buy_volume': 0.0,
                        'sell_volume': 0.0,
                        'buy_cost': 0.0,
                        'sell_cost': 0.0,
                        'trade_count': 0,
                        'avg_price': 0.0,
                        'last_trade': trade.timestamp
                    }
                
                pos = positions[key]
                pos['trade_count'] += 1
                
                if trade.side == 'buy':
                    pos['volume'] += trade.amount
                    pos['cost'] += trade.cost
                    pos['buy_volume'] += trade.amount
                    pos['buy_cost'] += trade.cost
                else:  # sell
                    pos['volume'] -= trade.amount
                    pos['cost'] -= trade.cost
                    pos['sell_volume'] += trade.amount
                    pos['sell_cost'] += trade.cost
                
                # Update last trade time
                if trade.timestamp > pos['last_trade']:
                    pos['last_trade'] = trade.timestamp
            
            # Calculate average prices and display results
            print(f"\n🔍 NET POSITIONS BY EXCHANGE:")
            print("-" * 50)
            
            total_net_usd = 0.0
            
            for key, pos in positions.items():
                if pos['buy_volume'] > 0:
                    avg_buy_price = pos['buy_cost'] / pos['buy_volume']
                else:
                    avg_buy_price = 0
                    
                if pos['sell_volume'] > 0:
                    avg_sell_price = pos['sell_cost'] / pos['sell_volume']
                else:
                    avg_sell_price = 0
                
                net_volume = pos['volume']
                net_cost = pos['cost']
                
                print(f"\n{pos['exchange']} - {pos['symbol']}:")
                print(f"  Net volume: {net_volume:.6f}")
                print(f"  Net cost: ${net_cost:.2f}")
                print(f"  Trades: {pos['trade_count']}")
                print(f"  Buy: {pos['buy_volume']:.6f} @ ${avg_buy_price:.2f}")
                print(f"  Sell: {pos['sell_volume']:.6f} @ ${avg_sell_price:.2f}")
                print(f"  Last trade: {pos['last_trade']}")
                
                total_net_usd += net_cost
            
            print(f"\n💰 TOTAL NET POSITION:")
            print(f"   Net USD value: ${total_net_usd:.2f}")
            
            # Get recent successful trading results
            print(f"\n🎯 RECENT TRADING ACTIVITY:")
            print("-" * 50)
            
            recent_trades = all_trades[:20]  # Last 20 trades
            for i, trade in enumerate(recent_trades, 1):
                exchange_name = exchange_map.get(trade.exchange_id, f"Unknown_{trade.exchange_id}")
                print(f"{i:2d}. {exchange_name} {trade.symbol} {trade.side.upper()} "
                      f"{trade.amount:.6f} @ ${trade.price:.2f} = ${trade.cost:.2f} "
                      f"({trade.timestamp.strftime('%m/%d %H:%M:%S')})")
                
            break
            
        await close_db()
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(calculate_positions()) 