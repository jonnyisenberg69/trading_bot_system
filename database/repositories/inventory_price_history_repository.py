from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, timezone, timedelta
from sqlalchemy import select, and_, or_, func, desc, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from ..models import InventoryPriceHistory, Exchange, Trade
from database.connection import init_db
import structlog

logger = structlog.get_logger(__name__)

class InventoryPriceHistoryRepository:
    """Repository for inventory price history operations."""

    def __init__(self, session_factory: async_sessionmaker):
        """Initialize the repository."""
        self.session_factory = session_factory
        self.logger = logger.bind(component="InventoryPriceHistoryRepository")

    async def add_inventory_price(self, exchange_id: int, symbol: str, avg_price: float, size: float, unrealized_pnl: float = 0, bot_instance_id: Optional[str] = None, timestamp: Optional[datetime] = None) -> InventoryPriceHistory:
        """Add an inventory price history entry."""
        # Skip very small positions
        if abs(size) < 1e-8:
            return None
            
        async with self.session_factory() as session:
            # Check if there's an existing entry with the same data to avoid duplication
            existing = await self._get_latest_entry(session, exchange_id, symbol)
            
            if existing and abs(existing.size - size) < 1e-8 and abs(existing.avg_price - avg_price) < 1e-8:
                # If data is essentially the same, don't create a new entry
                return existing
                
            # Create new entry
            entry = InventoryPriceHistory(
                exchange_id=exchange_id,
                symbol=symbol,
                avg_price=avg_price,
                size=size,
                unrealized_pnl=unrealized_pnl,
                bot_instance_id=bot_instance_id,
                timestamp=timestamp or datetime.utcnow()
            )
            session.add(entry)
            await session.commit()
            await session.refresh(entry)
            return entry
            
    async def _get_latest_entry(self, session: AsyncSession, exchange_id: int, symbol: str) -> Optional[InventoryPriceHistory]:
        """Get the latest inventory entry for a specific exchange and symbol."""
        query = (
            select(InventoryPriceHistory)
            .where(
                and_(
                    InventoryPriceHistory.exchange_id == exchange_id,
                    InventoryPriceHistory.symbol == symbol
                )
            )
            .order_by(desc(InventoryPriceHistory.timestamp))
            .limit(1)
        )
        result = await session.execute(query)
        return result.scalar_one_or_none()

    async def get_inventory_prices(self, exchange_id: Optional[int] = None, symbol: Optional[str] = None, bot_instance_id: Optional[int] = None, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None) -> List[InventoryPriceHistory]:
        """Get inventory price history filtered by criteria."""
        async with self.session_factory() as session:
            query = select(InventoryPriceHistory)
            
            # Apply filters
            conditions = []
            if exchange_id is not None:
                conditions.append(InventoryPriceHistory.exchange_id == exchange_id)
            if symbol is not None:
                conditions.append(InventoryPriceHistory.symbol == symbol)
            if bot_instance_id is not None:
                conditions.append(InventoryPriceHistory.bot_instance_id == bot_instance_id)
            if start_time is not None:
                conditions.append(InventoryPriceHistory.timestamp >= start_time)
            if end_time is not None:
                conditions.append(InventoryPriceHistory.timestamp <= end_time)
            
            if conditions:
                query = query.where(and_(*conditions))
                
            query = query.order_by(InventoryPriceHistory.timestamp)
            
            result = await session.execute(query)
            return result.scalars().all()

    async def get_latest_inventory_by_symbol(self, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None) -> List[InventoryPriceHistory]:
        """Get most recent inventory price for each exchange and symbol."""
        async with self.session_factory() as session:
            # This is a complex query that finds the latest inventory price entry for each exchange_id and symbol
            # First, get a subquery with the max timestamp for each group
            subquery = (
                select(
                    InventoryPriceHistory.exchange_id,
                    InventoryPriceHistory.symbol,
                    func.max(InventoryPriceHistory.timestamp).label("max_timestamp")
                )
                .group_by(
                    InventoryPriceHistory.exchange_id,
                    InventoryPriceHistory.symbol
                )
            )
            
            # Apply time filters to the subquery if provided
            conditions = []
            if start_time is not None:
                conditions.append(InventoryPriceHistory.timestamp >= start_time)
            if end_time is not None:
                conditions.append(InventoryPriceHistory.timestamp <= end_time)
            
            if conditions:
                subquery = subquery.where(and_(*conditions))
                
            # Use CTE (Common Table Expression) for clarity
            subquery_cte = subquery.cte("latest_timestamps")
            
            # Join with the original table to get the full records
            main_query = (
                select(InventoryPriceHistory)
                .join(
                    subquery_cte,
                    and_(
                        InventoryPriceHistory.exchange_id == subquery_cte.c.exchange_id,
                        InventoryPriceHistory.symbol == subquery_cte.c.symbol,
                        InventoryPriceHistory.timestamp == subquery_cte.c.max_timestamp
                    )
                )
            )
            
            # Skip entries with zero size for performance
            main_query = main_query.where(or_(
                InventoryPriceHistory.size > 1e-9,
                InventoryPriceHistory.size < -1e-9
            ))
            
            result = await session.execute(main_query)
            return result.scalars().all()

    async def get_current_positions(
        self, 
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        exchange_ids: Optional[List[int]] = None,
        min_position_size: float = 1e-8
    ) -> List[Dict[str, Any]]:
        """
        Get current positions from inventory price history.
        This method finds the latest inventory entry for each position.
        The time range filters are applied AFTER finding the latest record.
        """
        async with self.session_factory() as session:
            # Subquery to get the absolute latest timestamp for each position
            subquery = (
                select(
                    InventoryPriceHistory.exchange_id,
                    InventoryPriceHistory.symbol,
                    func.max(InventoryPriceHistory.timestamp).label("max_timestamp")
                )
                .group_by(
                    InventoryPriceHistory.exchange_id,
                    InventoryPriceHistory.symbol
                )
            )
            
            # Only filter by exchange_ids here, not time
            if exchange_ids:
                subquery = subquery.where(InventoryPriceHistory.exchange_id.in_(exchange_ids))
            
            subquery_cte = subquery.cte("latest_timestamps")
            
            # Main query to join with the original table and get the full records
            query = (
                select(InventoryPriceHistory, Exchange.name.label("exchange_name"))
                .join(
                    subquery_cte,
                    and_(
                        InventoryPriceHistory.exchange_id == subquery_cte.c.exchange_id,
                        InventoryPriceHistory.symbol == subquery_cte.c.symbol,
                        InventoryPriceHistory.timestamp == subquery_cte.c.max_timestamp
                    )
                )
                .join(Exchange, InventoryPriceHistory.exchange_id == Exchange.id)
            )
            
            # Now, apply time range filters to the final set of latest positions
            conditions = []
            if start_time:
                conditions.append(InventoryPriceHistory.timestamp >= start_time)
            if end_time:
                conditions.append(InventoryPriceHistory.timestamp <= end_time)
            
            if conditions:
                query = query.where(and_(*conditions))
            
            result = await session.execute(query)
            positions = []
            
            # Process results
            for row in result:
                inventory, exchange_name = row
                
                # Skip very small positions
                if abs(inventory.size) < min_position_size:
                    continue
                
                # Parse the symbol to get base/quote currencies
                original_symbol = inventory.symbol
                base_currency = quote_currency = None
                if '/' in original_symbol:
                    parts = original_symbol.split('/')
                    base_currency = parts[0]
                    quote_currency = parts[1]
                
                # Calculate value in quote currency
                value = float(inventory.size) * float(inventory.avg_price) if inventory.avg_price else 0
                
                positions.append({
                    "exchange": exchange_name,
                    "symbol": inventory.symbol.replace('/', '-'),  # Frontend format
                    "original_symbol": original_symbol,
                    "base_currency": base_currency,
                    "quote_currency": quote_currency,
                    "size": float(inventory.size),
                    "avg_price": float(inventory.avg_price) if inventory.avg_price else 0,
                    "value": value,
                    "unrealized_pnl": float(inventory.unrealized_pnl) if inventory.unrealized_pnl else 0,
                    "timestamp": inventory.timestamp.isoformat() if inventory.timestamp else None
                })
            
            return positions
    
    async def clean_old_entries(self, retention_days: int = 30) -> int:
        """Clean up old inventory entries, keeping only the latest for each position."""
        async with self.session_factory() as session:
            try:
                # Calculate cutoff date
                cutoff_date = datetime.utcnow() - timedelta(days=retention_days)
                
                # First, identify the positions we need to keep
                latest_entries = (
                    select(
                        InventoryPriceHistory.exchange_id,
                        InventoryPriceHistory.symbol,
                        func.max(InventoryPriceHistory.timestamp).label("max_timestamp")
                    )
                    .group_by(
                        InventoryPriceHistory.exchange_id,
                        InventoryPriceHistory.symbol
                    )
                ).cte("latest_timestamps")
                
                # Get entries to delete
                query = (
                    select(InventoryPriceHistory)
                    .where(
                        and_(
                            InventoryPriceHistory.timestamp < cutoff_date,
                            ~InventoryPriceHistory.id.in_(
                                select(InventoryPriceHistory.id)
                                .join(
                                    latest_entries,
                                    and_(
                                        InventoryPriceHistory.exchange_id == latest_entries.c.exchange_id,
                                        InventoryPriceHistory.symbol == latest_entries.c.symbol,
                                        InventoryPriceHistory.timestamp == latest_entries.c.max_timestamp
                                    )
                                )
                            )
                        )
                    )
                )
                
                result = await session.execute(query)
                entries_to_delete = result.scalars().all()
                
                # Delete the entries
                count = 0
                for entry in entries_to_delete:
                    await session.delete(entry)
                    count += 1
                
                await session.commit()
                self.logger.info(f"Cleaned up {count} old inventory entries")
                return count
                
            except Exception as e:
                self.logger.error(f"Error cleaning old inventory entries: {e}")
                return 0
    
    async def get_net_positions(
        self, 
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        exchange_ids: Optional[List[int]] = None,
        min_position_size: float = 1e-8
    ) -> Dict[str, Dict[str, Any]]:
        """
        Get net positions by symbol from inventory price history.
        This method aggregates the latest position for each exchange into a net position per symbol.
        """
        positions = await self.get_current_positions(
            start_time=start_time, 
            end_time=end_time, 
            exchange_ids=exchange_ids,
            min_position_size=min_position_size
        )
        
        net_positions = {}
        for pos in positions:
            symbol_key = pos["original_symbol"]
            
            # Normalize symbol for aggregation
            normalized_key = symbol_key
            if ':' in normalized_key:
                normalized_key = normalized_key.split(':')[0]
            if '-PERP' in normalized_key:
                normalized_key = normalized_key.replace('-PERP', '')

            if normalized_key not in net_positions:
                net_positions[normalized_key] = {
                    "symbol": normalized_key.replace('/', '-'),
                    "original_symbol": normalized_key,
                    "base_currency": pos["base_currency"],
                    "quote_currency": pos["quote_currency"],
                    "size": 0,
                    "value": 0,
                    "avg_price": 0,
                    "unrealized_pnl": 0,
                    "exchanges": [],
                    "is_open": False
                }
            
            net_pos = net_positions[normalized_key]
            net_pos["size"] += pos["size"]
            net_pos["value"] += pos["value"]
            net_pos["unrealized_pnl"] += pos["unrealized_pnl"]
            
            net_pos["exchanges"].append({
                "name": pos["exchange"],
                "size": pos["size"],
                "value": pos["value"],
                "avg_price": pos["avg_price"],
                "is_perpetual": "perp" in pos["exchange"].lower() or ':' in pos["original_symbol"] or '-PERP' in pos["original_symbol"]
            })
        
        for key, pos in net_positions.items():
            pos["is_open"] = abs(pos["size"]) > min_position_size
            if pos["is_open"]:
                pos["avg_price"] = pos["value"] / pos["size"] if pos["size"] != 0 else 0
            else:
                pos["avg_price"] = 0
        
        return net_positions
    
    async def get_inventory_history(
        self, 
        exchange_id: int,
        symbol: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """
        Get inventory history for a specific exchange and symbol.
        
        Returns a list of inventory entries.
        """
        async with self.session_factory() as session:
            query = (
                select(InventoryPriceHistory)
                .where(
                    and_(
                        InventoryPriceHistory.exchange_id == exchange_id,
                        InventoryPriceHistory.symbol == symbol
                    )
                )
                .order_by(desc(InventoryPriceHistory.timestamp))
            )
            
            # Apply time filters
            if start_time:
                query = query.where(InventoryPriceHistory.timestamp >= start_time)
            if end_time:
                query = query.where(InventoryPriceHistory.timestamp <= end_time)
                
            # Apply limit
            query = query.limit(limit)
            
            result = await session.execute(query)
            entries = result.scalars().all()
            
            history = []
            for entry in entries:
                history.append({
                    "timestamp": entry.timestamp.isoformat() if entry.timestamp else None,
                    "size": float(entry.size),
                    "avg_price": float(entry.avg_price) if entry.avg_price else 0,
                    "unrealized_pnl": float(entry.unrealized_pnl) if entry.unrealized_pnl else 0
                })
                
            return history
    
    async def get_position_summary(
        self, 
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        exchange_ids: Optional[List[int]] = None,
        min_position_size: float = 1e-8
    ) -> Dict[str, Any]:
        """
        Get position summary statistics.
        
        Returns a dictionary with summary statistics.
        """
        positions = await self.get_current_positions(
            start_time=start_time, 
            end_time=end_time, 
            exchange_ids=exchange_ids,
            min_position_size=min_position_size
        )
        
        # Calculate summary statistics
        long_value = 0
        short_value = 0
        long_positions = 0
        short_positions = 0
        positions_by_exchange = {}
        
        for pos in positions:
            exchange = pos["exchange"]
            size = pos["size"]
            value = pos["value"]
            
            # Count by direction
            if size > 0:
                long_value += value
                long_positions += 1
            elif size < 0:
                short_value += abs(value)
                short_positions += 1
                
            # Count by exchange
            if exchange not in positions_by_exchange:
                positions_by_exchange[exchange] = {
                    "count": 0,
                    "value": 0
                }
            positions_by_exchange[exchange]["count"] += 1
            positions_by_exchange[exchange]["value"] += abs(value)
        
        return {
            "long_value": long_value,
            "short_value": short_value,
            "net_exposure": long_value - short_value,
            "total_exposure": long_value + short_value,
            "long_positions": long_positions,
            "short_positions": short_positions,
            "open_positions": long_positions + short_positions,
            "positions_by_exchange": positions_by_exchange
        } 
