from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
from sqlalchemy import select, and_, func
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from ..models import RealizedPNL

class RealizedPNLRepository:
    """Repository for realized PNL operations."""

    def __init__(self, session_factory: async_sessionmaker):
        self.session_factory = session_factory

    async def add_pnl_event(self, exchange_id: int, symbol: str, realized_pnl: float, event_type: Optional[str], trade_id: Optional[int], bot_instance_id: Optional[int] = None, timestamp: Optional[datetime] = None):
        """Add realized PNL event."""
        async with self.session_factory() as session:
            # Use current time if not provided
            if timestamp is None:
                timestamp = datetime.now(timezone.utc).replace(tzinfo=None)
            elif timestamp.tzinfo is not None:
                # Make timestamp timezone-naive for DB
                timestamp = timestamp.replace(tzinfo=None)
                
            pnl_event = RealizedPNL(
                exchange_id=exchange_id,
                bot_instance_id=bot_instance_id,
                symbol=symbol,
                realized_pnl=realized_pnl,
                event_type=event_type,
                trade_id=trade_id,
                timestamp=timestamp
            )
            session.add(pnl_event)
            await session.commit()
            return pnl_event

    async def get_pnl_events(self, exchange_id: Optional[int] = None, symbol: Optional[str] = None, bot_instance_id: Optional[int] = None, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None, exchange_ids: Optional[List[int]] = None) -> List[RealizedPNL]:
        """Get PNL events filtered by criteria."""
        async with self.session_factory() as session:
            query = select(RealizedPNL)
            
            # Apply filters
            conditions = []
            if exchange_id is not None:
                conditions.append(RealizedPNL.exchange_id == exchange_id)
            if exchange_ids is not None:
                conditions.append(RealizedPNL.exchange_id.in_(exchange_ids))
            if symbol is not None:
                conditions.append(RealizedPNL.symbol == symbol)
            if bot_instance_id is not None:
                conditions.append(RealizedPNL.bot_instance_id == bot_instance_id)
            if start_time is not None:
                # Ensure timezone naive for DB
                if start_time.tzinfo is not None:
                    start_time = start_time.replace(tzinfo=None)
                conditions.append(RealizedPNL.timestamp >= start_time)
            if end_time is not None:
                # Ensure timezone naive for DB
                if end_time.tzinfo is not None:
                    end_time = end_time.replace(tzinfo=None)
                conditions.append(RealizedPNL.timestamp <= end_time)
            
            if conditions:
                query = query.where(and_(*conditions))
                
            query = query.order_by(RealizedPNL.timestamp)
            
            result = await session.execute(query)
            return result.scalars().all()

    async def get_pnl_by_time_range(self, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None) -> List[RealizedPNL]:
        """Get PNL events within a time range."""
        async with self.session_factory() as session:
            query = select(RealizedPNL)
            
            # Apply time filters
            conditions = []
            if start_time is not None:
                # Ensure timezone naive for DB
                if start_time.tzinfo is not None:
                    start_time = start_time.replace(tzinfo=None)
                conditions.append(RealizedPNL.timestamp >= start_time)
            if end_time is not None:
                # Ensure timezone naive for DB
                if end_time.tzinfo is not None:
                    end_time = end_time.replace(tzinfo=None)
                conditions.append(RealizedPNL.timestamp <= end_time)
            
            if conditions:
                query = query.where(and_(*conditions))
                
            # Sort by timestamp
            query = query.order_by(RealizedPNL.timestamp)
            
            result = await session.execute(query)
            return result.scalars().all()

    async def get_pnl_summary(self, exchange_id: Optional[int] = None, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None) -> Dict[str, Any]:
        """Get summarized PNL data aggregated by symbol."""
        async with self.session_factory() as session:
            # Build query to sum realized PNL grouped by symbol
            query = (
                select(
                    RealizedPNL.symbol,
                    func.sum(RealizedPNL.realized_pnl).label("total_pnl")
                )
                .group_by(RealizedPNL.symbol)
            )
            
            # Apply filters
            conditions = []
            if exchange_id is not None:
                conditions.append(RealizedPNL.exchange_id == exchange_id)
            if start_time is not None:
                # Ensure timezone naive for DB
                if start_time.tzinfo is not None:
                    start_time = start_time.replace(tzinfo=None)
                conditions.append(RealizedPNL.timestamp >= start_time)
            if end_time is not None:
                # Ensure timezone naive for DB
                if end_time.tzinfo is not None:
                    end_time = end_time.replace(tzinfo=None)
                conditions.append(RealizedPNL.timestamp <= end_time)
                
            if conditions:
                query = query.where(and_(*conditions))
                
            # Execute query
            result = await session.execute(query)
            
            # Build summary dict
            summary = {
                "total_pnl": 0.0,
                "by_symbol": {}
            }
            
            # Populate summary
            for symbol, total_pnl in result.all():
                summary["by_symbol"][symbol] = float(total_pnl)
                summary["total_pnl"] += float(total_pnl)
                
            return summary 
