#!/usr/bin/env python
import asyncio
from database import get_session
from database.models import Trade, Exchange
from sqlalchemy import select, desc, func

async def debug_bybit_trades():
    async for session in get_session():
        result = await session.execute(select(Exchange.name, func.count(Trade.id)).join(Trade).group_by(Exchange.name))
        exchange_counts = result.all()
        print('Trade counts by exchange:')
        for exchange, count in exchange_counts:
            print(f'  {exchange}: {count} trades')
        
        result = await session.execute(select(Exchange.name, Trade.symbol).join(Trade).distinct())
        exchange_symbols = result.all()
        print('\nExchange/symbol combinations:')
        for exchange, symbol in exchange_symbols:
            print(f'  {exchange}: {symbol}')
        break

asyncio.run(debug_bybit_trades()) 