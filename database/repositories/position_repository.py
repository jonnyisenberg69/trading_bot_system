"""
Position repository for handling position data in the database.
"""

from sqlalchemy import select, update, func, and_, or_, desc
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from decimal import Decimal

from ..models import Position, Exchange, BotInstance
import structlog

logger = structlog.get_logger(__name__)


class PositionRepository:
    """Repository for position operations."""
    
    def __init__(self, session: AsyncSession):
        """
        Initialize the repository.
        
        Args:
            session: SQLAlchemy async session
        """
        self.session = session
        self.logger = logger.bind(component="PositionRepository")
        
    async def get_position(
        self, 
        exchange_id: int, 
        symbol: str,
        bot_instance_id: Optional[int] = None
    ) -> Optional[Position]:
        """
        Get a position by exchange ID and symbol.
        
        Args:
            exchange_id: Exchange ID
            symbol: Trading symbol
            bot_instance_id: Bot instance ID (optional)
            
        Returns:
            Position if found, None otherwise
        """
        filters = [
            Position.exchange_id == exchange_id,
            Position.symbol == symbol
        ]
        
        if bot_instance_id is not None:
            filters.append(Position.bot_instance_id == bot_instance_id)
            
        stmt = select(Position).where(and_(*filters))
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()
        
    async def get_open_positions(
        self,
        exchange_id: Optional[int] = None,
        bot_instance_id: Optional[int] = None
    ) -> List[Position]:
        """
        Get all open positions, optionally filtered by exchange and/or bot instance.
        
        Args:
            exchange_id: Filter by exchange ID
            bot_instance_id: Filter by bot instance ID
            
        Returns:
            List of open positions
        """
        filters = [Position.is_open == True]
        
        if exchange_id is not None:
            filters.append(Position.exchange_id == exchange_id)
            
        if bot_instance_id is not None:
            filters.append(Position.bot_instance_id == bot_instance_id)
            
        stmt = select(Position).where(and_(*filters))
        result = await self.session.execute(stmt)
        return result.scalars().all()
        
    async def update_position(
        self, 
        exchange_id: int, 
        symbol: str,
        p1_delta: float,
        p2_delta: float,
        p1_fee_delta: float = 0,
        p2_fee_delta: float = 0,
        bot_instance_id: Optional[int] = None
    ) -> Position:
        """
        Update a position with trade data.
        
        Args:
            exchange_id: Exchange ID
            symbol: Trading symbol
            p1_delta: Change in base currency
            p2_delta: Change in quote currency
            p1_fee_delta: Change in base currency fee
            p2_fee_delta: Change in quote currency fee
            bot_instance_id: Bot instance ID (optional)
            
        Returns:
            Updated position
        """
        # Get existing position or create new one
        position = await self.get_position(exchange_id, symbol, bot_instance_id)
        
        if position:
            # Update existing position
            position.p1 += p1_delta
            position.p2 += p2_delta
            position.p1_fee += p1_fee_delta
            position.p2_fee += p2_fee_delta
        else:
            # Create new position
            position = Position(
                exchange_id=exchange_id,
                symbol=symbol,
                bot_instance_id=bot_instance_id,
                p1=p1_delta,
                p2=p2_delta,
                p1_fee=p1_fee_delta,
                p2_fee=p2_fee_delta
            )
            self.session.add(position)
            
        # Update position metadata
        position.last_update_time = datetime.utcnow()
        
        # Set entry time if position is being opened
        if position.entry_time is None and position.p1 != 0:
            position.entry_time = datetime.utcnow()
            
        # Update size and side
        position.size = position.p1
        if position.p1 > 0:
            position.side = 'long'
        elif position.p1 < 0:
            position.side = 'short'
        else:
            position.side = None
            
        # Update is_open flag
        position.is_open = position.p1 != 0
        
        # Calculate average price if position is open
        if position.is_open and position.p1 != 0:
            position.avg_price = -(position.p2 + position.p2_fee) / position.p1
        else:
            position.avg_price = None
            
        await self.session.commit()
        
        self.logger.info("Updated position", 
                         exchange_id=exchange_id, 
                         symbol=symbol,
                         size=position.size,
                         avg_price=position.avg_price)
        
        return position
        
    async def reset_position(
        self, 
        exchange_id: int, 
        symbol: str,
        bot_instance_id: Optional[int] = None
    ) -> bool:
        """
        Reset a position to zero.
        
        Args:
            exchange_id: Exchange ID
            symbol: Trading symbol
            bot_instance_id: Bot instance ID (optional)
            
        Returns:
            True if position was reset, False if not found
        """
        position = await self.get_position(exchange_id, symbol, bot_instance_id)
        
        if not position:
            return False
            
        # Reset position values
        position.p1 = 0
        position.p2 = 0
        position.p1_fee = 0
        position.p2_fee = 0
        position.avg_price = None
        position.size = 0
        position.side = None
        position.is_open = False
        position.last_update_time = datetime.utcnow()
        
        await self.session.commit()
        
        self.logger.info("Reset position", 
                         exchange_id=exchange_id, 
                         symbol=symbol)
        
        return True
        
    async def calculate_net_position(
        self,
        symbol: str,
        bot_instance_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Calculate net position across all exchanges for a symbol.
        
        Args:
            symbol: Trading symbol
            bot_instance_id: Filter by bot instance ID
            
        Returns:
            Dictionary with net position data
        """
        filters = [Position.symbol == symbol]
        
        if bot_instance_id is not None:
            filters.append(Position.bot_instance_id == bot_instance_id)
            
        # Calculate sums
        stmt = select(
            func.sum(Position.p1).label('p1'),
            func.sum(Position.p2).label('p2'),
            func.sum(Position.p1_fee).label('p1_fee'),
            func.sum(Position.p2_fee).label('p2_fee')
        ).where(and_(*filters))
        
        result = await self.session.execute(stmt)
        row = result.one_or_none()
        
        if not row or row.p1 is None:
            return {
                'symbol': symbol,
                'p1': 0,
                'p2': 0,
                'p1_fee': 0,
                'p2_fee': 0,
                'avg_price': None,
                'size': 0,
                'side': None,
                'is_open': False
            }
            
        # Calculate average price
        p1 = float(row.p1)
        p2 = float(row.p2)
        p1_fee = float(row.p1_fee)
        p2_fee = float(row.p2_fee)
        
        avg_price = None
        if p1 != 0:
            avg_price = -(p2 + p2_fee) / p1
            
        # Determine side
        side = None
        if p1 > 0:
            side = 'long'
        elif p1 < 0:
            side = 'short'
            
        return {
            'symbol': symbol,
            'p1': p1,
            'p2': p2,
            'p1_fee': p1_fee,
            'p2_fee': p2_fee,
            'avg_price': avg_price,
            'size': p1,
            'side': side,
            'is_open': p1 != 0
        }
        
    async def get_position_history(
        self,
        exchange_id: int,
        symbol: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        bot_instance_id: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Get position history for a specific exchange and symbol.
        
        Args:
            exchange_id: Exchange ID
            symbol: Trading symbol
            start_time: Filter by start time
            end_time: Filter by end time
            bot_instance_id: Filter by bot instance ID
            
        Returns:
            List of position history records
        """
        # This is a stub - in a real implementation, we would have a position_history table
        # For now, we just return the current position
        position = await self.get_position(exchange_id, symbol, bot_instance_id)
        
        if not position:
            return []
            
        return [{
            'timestamp': position.last_update_time,
            'p1': position.p1,
            'p2': position.p2,
            'p1_fee': position.p1_fee,
            'p2_fee': position.p2_fee,
            'avg_price': position.avg_price,
            'size': position.size,
            'side': position.side,
            'is_open': position.is_open
        }] 