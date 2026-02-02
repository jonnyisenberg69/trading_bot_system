from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy import select, desc, and_, func
from typing import Optional, List
from datetime import datetime
from ..models import BalanceHistory

class BalanceHistoryRepository:
    def __init__(self, session_factory: async_sessionmaker):
        self.session_factory = session_factory

    async def add_balance(self, exchange_id: int, currency: str, total: float, free: Optional[float], used: Optional[float], bot_instance_id: Optional[int] = None, timestamp: Optional[datetime] = None):
        async with self.session_factory() as session:
            record = BalanceHistory(
                exchange_id=exchange_id,
                currency=currency,
                total=total,
                free=free,
                used=used,
                bot_instance_id=bot_instance_id,
                timestamp=timestamp or datetime.utcnow()
            )
            session.add(record)
            await session.commit()
            return record

    async def get_balances(self, exchange_id: Optional[int] = None, currency: Optional[str] = None, bot_instance_id: Optional[int] = None, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None) -> List[BalanceHistory]:
        filters = []
        if exchange_id is not None:
            filters.append(BalanceHistory.exchange_id == exchange_id)
        if currency is not None:
            filters.append(BalanceHistory.currency == currency)
        if bot_instance_id is not None:
            filters.append(BalanceHistory.bot_instance_id == bot_instance_id)
        if start_time is not None:
            filters.append(BalanceHistory.timestamp >= start_time)
        if end_time is not None:
            filters.append(BalanceHistory.timestamp <= end_time)
        stmt = select(BalanceHistory).where(*filters).order_by(desc(BalanceHistory.timestamp))
        async with self.session_factory() as session:
            result = await session.execute(stmt)
            return result.scalars().all()
            
    async def get_latest_balances(self, exchange_ids: Optional[List[int]] = None, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None) -> List[BalanceHistory]:
        """Get the latest balance for each exchange/currency combination, ignoring time ranges."""
        async with self.session_factory() as session:
            # Subquery to get max timestamp for each exchange/currency
            # This query should not be constrained by time, as we always want the *latest* balance.
            subquery = select(
                BalanceHistory.exchange_id,
                BalanceHistory.currency,
                func.max(BalanceHistory.timestamp).label('max_timestamp')
            )
            
            # Apply exchange_ids filter to the subquery if provided
            if exchange_ids is not None:
                subquery = subquery.where(BalanceHistory.exchange_id.in_(exchange_ids))
            
            # Group by exchange and currency
            subquery = subquery.group_by(
                BalanceHistory.exchange_id,
                BalanceHistory.currency
            ).subquery()
            
            # Main query to get the actual records
            stmt = select(BalanceHistory).join(
                subquery,
                and_(
                    BalanceHistory.exchange_id == subquery.c.exchange_id,
                    BalanceHistory.currency == subquery.c.currency,
                    BalanceHistory.timestamp == subquery.c.max_timestamp
                )
            )
            
            result = await session.execute(stmt)
        return result.scalars().all() 
