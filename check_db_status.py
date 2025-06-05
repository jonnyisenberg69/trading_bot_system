#!/usr/bin/env python
import asyncio
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from database.connection import init_db, get_session, close_db
from database.repositories.trade_repository import TradeRepository
from database.models import Trade
from sqlalchemy import select, func, desc

async def check_db():
    try:
        await init_db()
        print("Database Status: CONNECTED (SQLite)")
        
        async for session in get_session():
            trade_repo = TradeRepository(session)
            
            # Get recent trades using direct query
            stmt = select(Trade).order_by(desc(Trade.timestamp)).limit(50)
            result = await session.execute(stmt)
            trades = result.scalars().all()
            
            print(f"Recent trades found: {len(trades)}")
            
            if trades:
                print("\nSample recent trades:")
                for i, trade in enumerate(trades[:10]):
                    print(f"  {i+1}. {trade.exchange_id} {trade.symbol} {trade.side} {trade.amount} @ {trade.price} on {trade.timestamp}")
            else:
                print("No trades found in database")
                
            # Check by exchange
            if trades:
                exchanges = set(trade.exchange_id for trade in trades)
                print(f"\nExchange IDs with trades: {list(exchanges)}")
                
                # Count by exchange
                exchange_counts = {}
                for trade in trades:
                    exchange_counts[trade.exchange_id] = exchange_counts.get(trade.exchange_id, 0) + 1
                
                print("Trade counts by exchange ID:")
                for exchange_id, count in exchange_counts.items():
                    print(f"  Exchange {exchange_id}: {count} trades")
                    
                # Get total count
                stmt = select(func.count(Trade.id))
                result = await session.execute(stmt)
                total_count = result.scalar_one()
                print(f"\nTotal trades in database: {total_count}")
                
                # Get statistics
                stats = await trade_repo.get_trade_statistics()
                print(f"\nStatistics:")
                print(f"  Total volume: {stats['total_volume']:.6f}")
                print(f"  Total cost: {stats['total_cost']:.2f}")
                print(f"  Trades by side: {stats['trades_by_side']}")
            
            break
            
        await close_db()
        
    except Exception as e:
        print(f"Database Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(check_db()) 