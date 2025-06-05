#!/usr/bin/env python
"""
Quick script to check database trades for debugging duplication issue.
"""

import asyncio
from database import get_session
from database.repositories import TradeRepository
from sqlalchemy import select, func, desc
from database.models import Trade, Exchange

async def check_trades():
    async for session in get_session():
        trade_repo = TradeRepository(session)
        
        # Check total trades
        result = await session.execute(select(func.count(Trade.id)))
        total_trades = result.scalar_one()
        print(f'Total trades in database: {total_trades}')
        
        if total_trades > 0:
            # Check recent trades
            result = await session.execute(
                select(Trade, Exchange.name).join(Exchange).order_by(desc(Trade.timestamp)).limit(10)
            )
            recent_trades = result.all()
            
            print(f'\nRecent trades:')
            for trade, exchange_name in recent_trades:
                print(f'  {exchange_name}: {trade.exchange_trade_id} - {trade.symbol} - {trade.timestamp} - {trade.side} {trade.amount}@{trade.price}')
        
        # Check unique exchange/symbol combinations
        combinations = await trade_repo.get_unique_exchange_symbols()
        print(f'\nUnique exchange/symbol combinations: {len(combinations)}')
        for exchange, symbol in combinations:
            print(f'  {exchange}: {symbol}')
        
        break

if __name__ == "__main__":
    asyncio.run(check_trades()) 