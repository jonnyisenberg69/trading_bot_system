#!/usr/bin/env python
import asyncio
from database import get_session
from database.models import Trade, Exchange
from sqlalchemy import select, desc

async def check_trade_ids():
    async for session in get_session():
        # Get all trade IDs and their exchanges
        result = await session.execute(
            select(Trade.exchange_trade_id, Exchange.name, Trade.symbol, Trade.timestamp)
            .join(Exchange)
            .order_by(desc(Trade.timestamp))
            .limit(20)
        )
        trades = result.all()
        
        print('Recent trade IDs by exchange:')
        for trade_id, exchange, symbol, timestamp in trades:
            print(f'  {exchange}: {trade_id} ({symbol}) - {timestamp}')
        
        # Check for duplicate trade IDs across exchanges
        result = await session.execute(
            select(Trade.exchange_trade_id)
            .group_by(Trade.exchange_trade_id)
            .having(func.count(Trade.exchange_trade_id) > 1)
        )
        duplicate_ids = result.scalars().all()
        
        if duplicate_ids:
            print(f'\nDuplicate trade IDs found across exchanges: {len(duplicate_ids)}')
            for dup_id in duplicate_ids:
                result = await session.execute(
                    select(Exchange.name, Trade.symbol, Trade.timestamp)
                    .join(Exchange)
                    .where(Trade.exchange_trade_id == dup_id)
                )
                exchanges = result.all()
                print(f'  {dup_id}: {exchanges}')
        else:
            print('\nNo duplicate trade IDs found across exchanges')
        
        break

if __name__ == "__main__":
    from sqlalchemy import func
    asyncio.run(check_trade_ids()) 