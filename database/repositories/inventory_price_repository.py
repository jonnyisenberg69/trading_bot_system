from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy import select, desc
from typing import Optional, List
from datetime import datetime
from ..models import InventoryPriceHistory

class InventoryPriceHistoryRepository:
    def __init__(self, session_factory: async_sessionmaker):
        self.session_factory = session_factory

    async def add_inventory_price(self, exchange_id: int, symbol: str, avg_price: Optional[float], size: float, bot_instance_id: Optional[int] = None, timestamp: Optional[datetime] = None):
        async with self.session_factory() as session:
            record = InventoryPriceHistory(
                exchange_id=exchange_id,
                symbol=symbol,
                avg_price=avg_price,
                size=size,
                bot_instance_id=bot_instance_id,
                timestamp=timestamp or datetime.utcnow()
            )
            session.add(record)
            await session.commit()
            return record

    async def get_inventory_prices(self, exchange_id: Optional[int] = None, symbol: Optional[str] = None, bot_instance_id: Optional[int] = None, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None) -> List[InventoryPriceHistory]:
        filters = []
        if exchange_id is not None:
            filters.append(InventoryPriceHistory.exchange_id == exchange_id)
        if symbol is not None:
            filters.append(InventoryPriceHistory.symbol == symbol)
        if bot_instance_id is not None:
            filters.append(InventoryPriceHistory.bot_instance_id == bot_instance_id)
        if start_time is not None:
            filters.append(InventoryPriceHistory.timestamp >= start_time)
        if end_time is not None:
            filters.append(InventoryPriceHistory.timestamp <= end_time)
        stmt = select(InventoryPriceHistory).where(*filters).order_by(desc(InventoryPriceHistory.timestamp))
        async with self.session_factory() as session:
            result = await session.execute(stmt)
            return result.scalars().all() 
