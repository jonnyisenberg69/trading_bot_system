"""
Trade repository for handling trade data in the database.
"""

import json
from sqlalchemy import select, update, func, and_, or_, desc, delete
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from decimal import Decimal

from ..models import Trade, TradeSync, Exchange, BotInstance
import structlog

logger = structlog.get_logger(__name__)


def json_serializer(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")


class TradeRepository:
    """Repository for trade operations."""
    
    def __init__(self, session: AsyncSession):
        """
        Initialize the repository.
        
        Args:
            session: SQLAlchemy async session
        """
        self.session = session
        self.logger = logger.bind(component="TradeRepository")
        
    async def save_trade(self, trade_data: Dict[str, Any], exchange_id: int, bot_instance_id: Optional[int] = None) -> Optional[Trade]:
        """
        Save a trade to the database.
        
        Args:
            trade_data: Trade data dictionary
            exchange_id: Exchange ID
            bot_instance_id: Bot instance ID (optional)
            
        Returns:
            Created trade or None if failed
        """
        try:
            # Check if trade already exists
            exchange_trade_id = trade_data.get('id', '')
            if not exchange_trade_id:
                self.logger.warning("Trade data missing ID", trade_data=trade_data)
                return None
                
            stmt = select(Trade).where(
                and_(
                    Trade.exchange_id == exchange_id,
                    Trade.exchange_trade_id == exchange_trade_id
                )
            )
            result = await self.session.execute(stmt)
            existing_trade = result.scalar_one_or_none()
            
            if existing_trade:
                self.logger.debug("Trade already exists", trade_id=exchange_trade_id)
                return existing_trade
                
            # Extract trade data
            symbol = trade_data.get('symbol', '')
            side = trade_data.get('side', '').lower()
            amount = float(trade_data.get('amount', 0))
            price = float(trade_data.get('price', 0))
            cost = float(trade_data.get('cost', price * amount))
            
            # Extract fee data
            fee_cost = None
            fee_currency = None
            if 'fee' in trade_data and trade_data['fee']:
                fee_cost = float(trade_data['fee'].get('cost', 0))
                fee_currency = trade_data['fee'].get('currency', '')
                
            # Extract timestamp
            timestamp = trade_data.get('timestamp', None)
            if timestamp:
                if isinstance(timestamp, int):
                    timestamp = datetime.fromtimestamp(timestamp / 1000)
                elif isinstance(timestamp, str):
                    try:
                        timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    except:
                        timestamp = datetime.utcnow()
                elif not isinstance(timestamp, datetime):
                    timestamp = datetime.utcnow()
            else:
                timestamp = datetime.utcnow()
                
            # Calculate P1 and P2 delta
            p1_delta = amount if side == 'buy' else -amount
            p2_delta = -cost if side == 'buy' else cost
            
            # Prepare raw_data for JSON storage (handle datetime serialization)
            raw_data_serializable = json.loads(json.dumps(trade_data, default=json_serializer))
            
            # Create trade
            trade = Trade(
                exchange_id=exchange_id,
                bot_instance_id=bot_instance_id,
                exchange_trade_id=exchange_trade_id,
                symbol=symbol,
                side=side,
                amount=amount,
                price=price,
                cost=cost,
                fee_cost=fee_cost,
                fee_currency=fee_currency,
                timestamp=timestamp,
                order_id=trade_data.get('order', None),
                trade_type=trade_data.get('type', None),
                is_maker=trade_data.get('maker', False),
                is_liquidation=trade_data.get('liquidation', False),
                p1_delta=p1_delta,
                p2_delta=p2_delta,
                raw_data=raw_data_serializable,
                processed=False
            )
            
            self.session.add(trade)
            # DO NOT COMMIT HERE - let the caller handle batch commits
            
            self.logger.info("Added trade to session", 
                             exchange_id=exchange_id, 
                             symbol=symbol, 
                             trade_id=exchange_trade_id)
            
            return trade
            
        except Exception as e:
            self.logger.error(f"Error adding trade to session: {e}")
            # Rollback this session on error
            await self.session.rollback()
            return None
    
    async def commit_trades(self) -> bool:
        """
        Commit all pending trades in the current session.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            await self.session.commit()
            self.logger.info("Successfully committed trade batch")
            return True
        except Exception as e:
            self.logger.error(f"Error committing trade batch: {e}")
            await self.session.rollback()
            return False
        
    async def get_trade(self, trade_id: int) -> Optional[Trade]:
        """
        Get a trade by ID.
        
        Args:
            trade_id: Trade ID
            
        Returns:
            Trade if found, None otherwise
        """
        stmt = select(Trade).where(Trade.id == trade_id)
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()
        
    async def get_trade_by_exchange_id(self, exchange_id: int, exchange_trade_id: str) -> Optional[Trade]:
        """
        Get a trade by exchange trade ID.
        
        Args:
            exchange_id: Exchange ID
            exchange_trade_id: Exchange trade ID
            
        Returns:
            Trade if found, None otherwise
        """
        stmt = select(Trade).where(
            and_(
                Trade.exchange_id == exchange_id,
                Trade.exchange_trade_id == exchange_trade_id
            )
        )
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()
        
    async def get_trade_by_id_and_exchange(self, trade_id: str, exchange_name: str) -> Optional[Trade]:
        """
        Get a trade by trade ID and exchange name.
        
        Args:
            trade_id: Exchange trade ID
            exchange_name: Exchange name (connection ID)
            
        Returns:
            Trade if found, None otherwise
        """
        # Join with Exchange table to find by exchange name
        stmt = select(Trade).join(Exchange).where(
            and_(
                Trade.exchange_trade_id == trade_id,
                Exchange.name == exchange_name
            )
        )
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()
        
    async def get_trades_by_symbol(
        self, 
        symbol: str, 
        exchange_id: Optional[int] = None,
        bot_instance_id: Optional[int] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100
    ) -> List[Trade]:
        """
        Get trades for a specific symbol.
        
        Args:
            symbol: Trading symbol
            exchange_id: Filter by exchange ID
            bot_instance_id: Filter by bot instance ID
            start_time: Filter by start time
            end_time: Filter by end time
            limit: Maximum number of trades to return
            
        Returns:
            List of trades
        """
        filters = [Trade.symbol == symbol]
        
        if exchange_id is not None:
            filters.append(Trade.exchange_id == exchange_id)
            
        if bot_instance_id is not None:
            filters.append(Trade.bot_instance_id == bot_instance_id)
            
        if start_time is not None:
            filters.append(Trade.timestamp >= start_time)
            
        if end_time is not None:
            filters.append(Trade.timestamp <= end_time)
            
        stmt = select(Trade).where(and_(*filters)).order_by(desc(Trade.timestamp)).limit(limit)
        result = await self.session.execute(stmt)
        return result.scalars().all()

    async def get_trades_by_exchange_symbol(self, exchange_name: str, symbol: str) -> List[Trade]:
        """
        Get all trades for a specific exchange and symbol.
        
        Args:
            exchange_name: Exchange name (connection ID)
            symbol: Trading symbol
            
        Returns:
            List of trades
        """
        try:
            # Join with Exchange table to find by exchange name
            stmt = select(Trade).join(Exchange).where(
                and_(
                    Exchange.name == exchange_name,
                    Trade.symbol == symbol
                )
            ).order_by(Trade.timestamp)
            
            result = await self.session.execute(stmt)
            trades = result.scalars().all()
            
            self.logger.debug(f"Found {len(trades)} trades for {exchange_name} {symbol}")
            return trades
            
        except Exception as e:
            self.logger.error(f"Error getting trades for {exchange_name} {symbol}: {e}")
            return []
            
    async def get_unique_exchange_symbols(self) -> List[Tuple[str, str]]:
        """
        Get all unique exchange/symbol combinations.
        
        Returns:
            List of (exchange_name, symbol) tuples
        """
        try:
            stmt = select(Exchange.name, Trade.symbol).join(Exchange).distinct()
            result = await self.session.execute(stmt)
            combinations = [(row.name, row.symbol) for row in result.all()]
            
            self.logger.debug(f"Found {len(combinations)} unique exchange/symbol combinations")
            return combinations
            
        except Exception as e:
            self.logger.error(f"Error getting unique exchange/symbol combinations: {e}")
            return []
            
    async def get_unique_symbols(self) -> List[str]:
        """
        Get all unique symbols across all exchanges.
        
        Returns:
            List of unique symbols
        """
        try:
            stmt = select(Trade.symbol).distinct()
            result = await self.session.execute(stmt)
            symbols = [row.symbol for row in result.all()]
            
            self.logger.debug(f"Found {len(symbols)} unique symbols")
            return symbols
            
        except Exception as e:
            self.logger.error(f"Error getting unique symbols: {e}")
            return []
            
    async def get_symbols_by_exchange(self, exchange_name: str) -> List[str]:
        """
        Get all symbols for a specific exchange.
        
        Args:
            exchange_name: Exchange name (connection ID)
            
        Returns:
            List of symbols for the exchange
        """
        try:
            stmt = select(Trade.symbol).join(Exchange).where(
                Exchange.name == exchange_name
            ).distinct()
            
            result = await self.session.execute(stmt)
            symbols = [row.symbol for row in result.all()]
            
            self.logger.debug(f"Found {len(symbols)} symbols for {exchange_name}")
            return symbols
            
        except Exception as e:
            self.logger.error(f"Error getting symbols for {exchange_name}: {e}")
            return []
            
    async def delete_trades_by_exchange_symbol(self, exchange_name: str, symbol: str) -> int:
        """
        Delete all trades for a specific exchange and symbol.
        
        Args:
            exchange_name: Exchange name (connection ID)
            symbol: Trading symbol
            
        Returns:
            Number of trades deleted
        """
        try:
            # First get the exchange ID
            exchange_stmt = select(Exchange.id).where(Exchange.name == exchange_name)
            exchange_result = await self.session.execute(exchange_stmt)
            exchange_id = exchange_result.scalar_one_or_none()
            
            if not exchange_id:
                self.logger.warning(f"Exchange {exchange_name} not found")
                return 0
                
            # Delete trades
            stmt = delete(Trade).where(
                and_(
                    Trade.exchange_id == exchange_id,
                    Trade.symbol == symbol
                )
            )
            
            result = await self.session.execute(stmt)
            await self.session.commit()
            
            deleted_count = result.rowcount
            self.logger.info(f"Deleted {deleted_count} trades for {exchange_name} {symbol}")
            return deleted_count
            
        except Exception as e:
            self.logger.error(f"Error deleting trades for {exchange_name} {symbol}: {e}")
            await self.session.rollback()
            return 0
            
    async def delete_all_trades(self) -> int:
        """
        Delete all trades from the database.
        
        Returns:
            Number of trades deleted
        """
        try:
            stmt = delete(Trade)
            result = await self.session.execute(stmt)
            await self.session.commit()
            
            deleted_count = result.rowcount
            self.logger.info(f"Deleted all {deleted_count} trades from database")
            return deleted_count
            
        except Exception as e:
            self.logger.error(f"Error deleting all trades: {e}")
            await self.session.rollback()
            return 0
        
    async def get_unprocessed_trades(self, limit: int = 100) -> List[Trade]:
        """
        Get unprocessed trades for position calculation.
        
        Args:
            limit: Maximum number of trades to return
            
        Returns:
            List of unprocessed trades
        """
        stmt = select(Trade).where(Trade.processed == False).order_by(Trade.timestamp).limit(limit)
        result = await self.session.execute(stmt)
        return result.scalars().all()
        
    async def mark_trades_as_processed(self, trade_ids: List[int]) -> int:
        """
        Mark trades as processed.
        
        Args:
            trade_ids: List of trade IDs
            
        Returns:
            Number of trades updated
        """
        if not trade_ids:
            return 0
            
        stmt = update(Trade).where(Trade.id.in_(trade_ids)).values(processed=True)
        result = await self.session.execute(stmt)
        await self.session.commit()
        return result.rowcount
        
    async def get_last_trade_sync(self, exchange_id: int, symbol: str) -> Optional[TradeSync]:
        """
        Get the last trade synchronization record.
        
        Args:
            exchange_id: Exchange ID
            symbol: Trading symbol
            
        Returns:
            TradeSync record if found, None otherwise
        """
        stmt = select(TradeSync).where(
            and_(
                TradeSync.exchange_id == exchange_id,
                TradeSync.symbol == symbol
            )
        )
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()
        
    async def update_trade_sync(
        self, 
        exchange_id: int, 
        symbol: str,
        last_sync_time: datetime,
        last_trade_id: Optional[str] = None
    ) -> TradeSync:
        """
        Update the trade synchronization record.
        
        Args:
            exchange_id: Exchange ID
            symbol: Trading symbol
            last_sync_time: Last synchronization time
            last_trade_id: Last trade ID
            
        Returns:
            Updated TradeSync record
        """
        # Get existing record
        trade_sync = await self.get_last_trade_sync(exchange_id, symbol)
        
        if trade_sync:
            # Update existing record
            trade_sync.last_sync_time = last_sync_time
            if last_trade_id:
                trade_sync.last_trade_id = last_trade_id
            trade_sync.updated_at = datetime.utcnow()
        else:
            # Create new record
            trade_sync = TradeSync(
                exchange_id=exchange_id,
                symbol=symbol,
                last_sync_time=last_sync_time,
                last_trade_id=last_trade_id
            )
            self.session.add(trade_sync)
            
        await self.session.commit()
        return trade_sync
        
    async def get_trade_statistics(
        self,
        symbol: Optional[str] = None,
        exchange_id: Optional[int] = None,
        bot_instance_id: Optional[int] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Get trade statistics.
        
        Args:
            symbol: Filter by trading symbol
            exchange_id: Filter by exchange ID
            bot_instance_id: Filter by bot instance ID
            start_time: Filter by start time
            end_time: Filter by end time
            
        Returns:
            Dictionary with trade statistics
        """
        filters = []
        
        if symbol:
            filters.append(Trade.symbol == symbol)
            
        if exchange_id is not None:
            filters.append(Trade.exchange_id == exchange_id)
            
        if bot_instance_id is not None:
            filters.append(Trade.bot_instance_id == bot_instance_id)
            
        if start_time is not None:
            filters.append(Trade.timestamp >= start_time)
            
        if end_time is not None:
            filters.append(Trade.timestamp <= end_time)
            
        # Total trades
        stmt = select(func.count(Trade.id)).where(and_(*filters))
        result = await self.session.execute(stmt)
        total_trades = result.scalar_one() or 0
        
        # Total volume
        stmt = select(func.sum(Trade.amount)).where(and_(*filters))
        result = await self.session.execute(stmt)
        total_volume = result.scalar_one() or 0
        
        # Total cost
        stmt = select(func.sum(Trade.cost)).where(and_(*filters))
        result = await self.session.execute(stmt)
        total_cost = result.scalar_one() or 0
        
        # Trades by side
        stmt = select(
            Trade.side,
            func.count(Trade.id).label('count'),
            func.sum(Trade.amount).label('volume'),
            func.sum(Trade.cost).label('cost')
        ).where(and_(*filters)).group_by(Trade.side)
        result = await self.session.execute(stmt)
        trades_by_side = {row.side: {'count': row.count, 'volume': row.volume, 'cost': row.cost} 
                          for row in result.all()}
        
        return {
            'total_trades': total_trades,
            'total_volume': float(total_volume),
            'total_cost': float(total_cost),
            'trades_by_side': trades_by_side
        }
