#!/usr/bin/env python
"""
Check and populate exchanges in the database.
"""

import asyncio
from database import get_session
from database.models import Exchange
from sqlalchemy import select

async def check_and_populate_exchanges():
    async for session in get_session():
        try:
            # Check existing exchanges
            result = await session.execute(select(Exchange))
            existing_exchanges = result.scalars().all()
            
            print(f"Existing exchanges in database: {len(existing_exchanges)}")
            for exchange in existing_exchanges:
                print(f"  {exchange.id}: {exchange.name} ({exchange.type})")
            
            # Define the exchanges we need
            required_exchanges = [
                {'name': 'binance_spot', 'type': 'spot'},
                {'name': 'binance_perp', 'type': 'perpetual'},
                {'name': 'bybit_spot', 'type': 'spot'},
                {'name': 'bybit_perp', 'type': 'perpetual'},
                {'name': 'mexc_spot', 'type': 'spot'},
                {'name': 'gateio_spot', 'type': 'spot'},
                {'name': 'bitget_spot', 'type': 'spot'},
                {'name': 'hyperliquid_perp', 'type': 'perpetual'},
            ]
            
            # Get existing exchange names
            existing_names = {ex.name for ex in existing_exchanges}
            
            # Add missing exchanges
            for req_exchange in required_exchanges:
                if req_exchange['name'] not in existing_names:
                    new_exchange = Exchange(
                        name=req_exchange['name'],
                        type=req_exchange['type']
                    )
                    session.add(new_exchange)
                    print(f"Adding exchange: {req_exchange['name']}")
            
            # Commit changes
            await session.commit()
            
            # Check again
            result = await session.execute(select(Exchange))
            all_exchanges = result.scalars().all()
            
            print(f"\nFinal exchanges in database: {len(all_exchanges)}")
            for exchange in all_exchanges:
                print(f"  {exchange.id}: {exchange.name} ({exchange.type})")
            
        except Exception as e:
            print(f"Error: {e}")
            import traceback
            traceback.print_exc()
            await session.rollback()
        finally:
            await session.close()
        break

if __name__ == "__main__":
    asyncio.run(check_and_populate_exchanges()) 