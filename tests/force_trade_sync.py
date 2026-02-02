import asyncio
import sys
import os
sys.path.append(os.getcwd())

from datetime import datetime, timezone, timedelta
from database.connection import init_db, get_session
from database.models import Exchange
from order_management.enhanced_trade_sync import EnhancedTradeSync
from sqlalchemy import select

async def force_sync_all_exchanges():
    """Force synchronization of trades for all exchanges."""
    print("🔄 Starting forced trade synchronization...")
    
    # Initialize database
    await init_db()
    
    # Get all active exchanges
    async for session in get_session():
        stmt = select(Exchange).where(Exchange.is_active == True)
        result = await session.execute(stmt)
        exchanges = result.scalars().all()
        
        print(f"Found {len(exchanges)} active exchanges")
        
        # Initialize enhanced trade sync
        trade_sync = EnhancedTradeSync()
        
        # Sync each exchange
        for exchange in exchanges:
            print(f"\n📡 Syncing {exchange.name}...")
            try:
                # Force sync from 2 hours ago to ensure we catch all recent trades
                since_time = datetime.now(timezone.utc) - timedelta(hours=2)
                
                result = await trade_sync.sync_exchange_trades(
                    exchange_name=exchange.name,
                    symbol="BERA/USDT",
                    since=since_time
                )
                
                if result:
                    new_trades, duplicates = result
                    print(f"✅ {exchange.name}: {new_trades} new trades, {duplicates} duplicates")
                else:
                    print(f"❌ {exchange.name}: Sync failed")
                    
            except Exception as e:
                print(f"❌ {exchange.name}: Error - {e}")
        
        break  # Only use first session
    
    print("\n🎯 Trade synchronization completed!")
    
    # Check updated trade counts
    print("\n📊 Updated trade counts:")
    async for session in get_session():
        from sqlalchemy import text
        result = await session.execute(text("""
            SELECT e.name, COUNT(*) as trade_count, MAX(t.timestamp) as last_trade 
            FROM trades t 
            JOIN exchanges e ON t.exchange_id = e.id 
            WHERE t.symbol = 'BERA/USDT' AND t.timestamp >= '2025-06-06 21:00:00' 
            GROUP BY e.name 
            ORDER BY last_trade DESC
        """))
        
        for row in result:
            print(f"  {row[0]}: {row[1]} trades, last: {row[2]}")
        break

if __name__ == "__main__":
    asyncio.run(force_sync_all_exchanges()) 