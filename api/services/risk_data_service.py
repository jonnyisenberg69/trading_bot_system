"""
Risk Data Service.

This service provides access to risk monitoring data from the database.
"""

from typing import Dict, List, Optional, Any
from datetime import datetime, timezone, timedelta
import structlog
from sqlalchemy import select, and_, func
from sqlalchemy.ext.asyncio import AsyncSession

from database.models import Exchange, InventoryPriceHistory, RealizedPNL
from database.repositories.inventory_price_history_repository import InventoryPriceHistoryRepository
from database.repositories.realized_pnl_repository import RealizedPNLRepository
from database.repositories.balance_history_repository import BalanceHistoryRepository

logger = structlog.get_logger(__name__)


class RiskDataService:
    """Service for accessing risk monitoring data."""
    
    def __init__(self, session_maker):
        self.session_maker = session_maker
        self.logger = logger.bind(component="RiskDataService")
        
        # Initialize repositories
        self.inventory_repo = InventoryPriceHistoryRepository(session_maker)
        self.pnl_repo = RealizedPNLRepository(session_maker)
        self.balance_repo = BalanceHistoryRepository(session_maker)
        
        # Cache for position data to reduce database queries
        self._position_cache = {}
        self._position_cache_time = {}
        self._cache_ttl = 15  # Increased from 5 to 15 seconds
        
        # Minimum position size to consider (filter out dust)
        self.min_position_size = 1e-8
    
    def _ensure_naive_datetime(self, dt):
        """Convert timezone-aware datetime to naive UTC datetime for database operations."""
        if dt is None:
            return None
        if dt.tzinfo is not None:
            return dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
        
    async def get_current_positions(
        self, 
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        exchange_ids: Optional[List[int]] = None,
        use_cache: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Get current positions from inventory price history.
        
        Returns a list of positions with exchange name, symbol, size, avg_price.
        """
        cache_key = f"positions_{start_time}_{end_time}_{exchange_ids}"
        
        # Check cache if enabled
        if use_cache and cache_key in self._position_cache:
            cache_time = self._position_cache_time.get(cache_key, 0)
            if (datetime.now().timestamp() - cache_time) < self._cache_ttl:
                cached_positions = self._position_cache[cache_key]
                self.logger.info(f"Using cached positions: {len(cached_positions)} positions found in cache")
                return cached_positions
        
        try:
            # Ensure naive datetimes for database queries
            naive_start_time = self._ensure_naive_datetime(start_time)
            naive_end_time = self._ensure_naive_datetime(end_time)
            
            self.logger.info(f"Querying positions from database with filters: start_time={naive_start_time}, end_time={naive_end_time}, exchange_ids={exchange_ids}")
            
            # Get positions from repository with minimum size filter
            positions = await self.inventory_repo.get_current_positions(
                start_time=naive_start_time,
                end_time=naive_end_time,
                exchange_ids=exchange_ids,
                min_position_size=self.min_position_size
            )
            
            self.logger.info(f"Retrieved {len(positions)} positions from database")
            
            # If no positions found and cache is available, use the cache
            if not positions and use_cache and cache_key in self._position_cache:
                cached_positions = self._position_cache[cache_key]
                self.logger.info(f"No positions in database, using {len(cached_positions)} cached positions")
                return cached_positions
            
            # Update cache
            if use_cache:
                self._position_cache[cache_key] = positions
                self._position_cache_time[cache_key] = datetime.now().timestamp()
            
            return positions
        except Exception as e:
            self.logger.error(f"Error getting current positions: {e}", exc_info=True)
            
            # If error occurred and cache is available, use the cache as fallback
            if use_cache and cache_key in self._position_cache:
                cached_positions = self._position_cache[cache_key]
                self.logger.info(f"Error occurred, using {len(cached_positions)} cached positions as fallback")
                return cached_positions
                
            return []
        
    async def get_net_positions(
        self, 
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        exchange_ids: Optional[List[int]] = None,
        use_cache: bool = True
    ) -> Dict[str, Dict[str, Any]]:
        """
        Get net positions by symbol from inventory price history.
        The repository now handles fetching the latest state, so time-based caching is removed.
        """
        try:
            # Ensure naive datetimes for database queries
            naive_start_time = self._ensure_naive_datetime(start_time)
            naive_end_time = self._ensure_naive_datetime(end_time)
            
            # Get net positions from repository
            net_positions = await self.inventory_repo.get_net_positions(
                start_time=naive_start_time,
                end_time=naive_end_time,
                exchange_ids=exchange_ids,
                min_position_size=self.min_position_size
            )
            
            return net_positions
        except Exception as e:
            self.logger.error(f"Error getting net positions: {e}", exc_info=True)
            return {}
        
    async def get_realized_pnl(
        self, 
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        exchange_ids: Optional[List[int]] = None
    ) -> Dict[str, Any]:
        """
        Get realized PNL data from the database.
        
        Returns a dictionary with total PNL and breakdown by exchange.
        """
        try:
            # Ensure naive datetimes for database queries
            naive_start_time = self._ensure_naive_datetime(start_time)
            naive_end_time = self._ensure_naive_datetime(end_time)
            
            # Get PNL events from repository - check if it supports exchange_ids parameter
            try:
                pnl_events = await self.pnl_repo.get_pnl_events(
                    start_time=naive_start_time,
                    end_time=naive_end_time,
                    exchange_ids=exchange_ids
                )
            except TypeError:
                # Fall back to using just start_time and end_time if exchange_ids is not supported
                pnl_events = await self.pnl_repo.get_pnl_events(
                    start_time=naive_start_time,
                    end_time=naive_end_time
                )
            
            # Calculate total PNL and breakdown by exchange
            total_pnl = 0
            pnl_by_exchange = {}
            pnl_by_symbol = {}
            
            # Pre-fetch exchange names to avoid N+1 queries
            exchange_names = {}
            async with self.session_maker() as session:
                if exchange_ids:
                    query = select(Exchange.id, Exchange.name).where(Exchange.id.in_(exchange_ids))
                else:
                    query = select(Exchange.id, Exchange.name)
                    
                result = await session.execute(query)
                for row in result:
                    exchange_names[row[0]] = row[1]
            
            # Process PNL events
            for event in pnl_events:
                pnl = float(event.realized_pnl)
                total_pnl += pnl
                
                # Get exchange name
                exchange_id = event.exchange_id
                exchange_name = exchange_names.get(exchange_id, f"Exchange {exchange_id}")
                
                # Add to exchange breakdown
                if exchange_name not in pnl_by_exchange:
                    pnl_by_exchange[exchange_name] = 0
                pnl_by_exchange[exchange_name] += pnl
                
                # Add to symbol breakdown
                symbol = event.symbol
                if symbol not in pnl_by_symbol:
                    pnl_by_symbol[symbol] = 0
                pnl_by_symbol[symbol] += pnl
            
            return {
                "total": total_pnl,
                "by_exchange": pnl_by_exchange,
                "by_symbol": pnl_by_symbol,
                "events_count": len(pnl_events)
            }
        except Exception as e:
            self.logger.error(f"Error getting realized PNL: {e}")
            return {"total": 0, "by_exchange": {}, "by_symbol": {}, "events_count": 0}
        
    async def get_summary_data(
        self, 
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        exchange_ids: Optional[List[int]] = None
    ) -> Dict[str, Any]:
        """
        Get summary data for risk monitoring.
        
        Returns a dictionary with summary statistics.
        """
        try:
            # Ensure naive datetimes for database queries
            naive_start_time = self._ensure_naive_datetime(start_time)
            naive_end_time = self._ensure_naive_datetime(end_time)
            
            # Get position summary
            position_summary = await self.inventory_repo.get_position_summary(
                start_time=naive_start_time,
                end_time=naive_end_time,
                exchange_ids=exchange_ids,
                min_position_size=self.min_position_size
            )
            
            # Get latest balances
            balances = await self.balance_repo.get_latest_balances(
                exchange_ids=exchange_ids
            )
            
            # Get realized PNL
            realized_pnl = await self.get_realized_pnl(
                start_time=naive_start_time,
                end_time=naive_end_time,
                exchange_ids=exchange_ids
            )
            
            # Combine data
            summary = {
                **position_summary,
                "balances": balances,
                "total_realized_pnl": realized_pnl["total"]
            }
            
            return summary
        except Exception as e:
            self.logger.error(f"Error getting summary data: {e}")
            return {
                "long_value": 0,
                "short_value": 0,
                "net_exposure": 0,
                "total_exposure": 0,
                "open_positions": 0,
                "positions_by_exchange": {},
                "balances": {},
                "total_realized_pnl": 0
            }
    
    async def clean_position_data(self) -> Dict[str, int]:
        """
        Clean up position data in the database.
        
        Returns a dictionary with counts of cleaned entries.
        """
        try:
            # Clean old inventory entries
            inventory_count = await self.inventory_repo.clean_old_entries(retention_days=30)
            
            # Clear cache
            self._position_cache = {}
            self._position_cache_time = {}
            
            return {
                "inventory_entries_cleaned": inventory_count
            }
        except Exception as e:
            self.logger.error(f"Error cleaning position data: {e}")
            return {"inventory_entries_cleaned": 0}
    
    async def consolidate_positions(
        self, 
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        exchange_ids: Optional[List[int]] = None
    ) -> Dict[str, int]:
        """
        Consolidate position data in the database.
        
        This will remove duplicate entries and ensure only the latest entry
        for each position is kept.
        
        Returns a dictionary with counts of consolidated entries.
        """
        try:
            # Ensure naive datetimes for database queries
            naive_start_time = self._ensure_naive_datetime(start_time)
            naive_end_time = self._ensure_naive_datetime(end_time)
            
            async with self.session_maker() as session:
                # For each exchange_id and symbol, find the latest entry
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
                
                # Apply filters
                conditions = []
                if naive_start_time:
                    conditions.append(InventoryPriceHistory.timestamp >= naive_start_time)
                if naive_end_time:
                    conditions.append(InventoryPriceHistory.timestamp <= naive_end_time)
                if exchange_ids:
                    conditions.append(InventoryPriceHistory.exchange_id.in_(exchange_ids))
                
                if conditions:
                    subquery = subquery.where(and_(*conditions))
                
                # Execute subquery to get the latest timestamps
                result = await session.execute(subquery)
                latest_entries = result.all()
                
                # Count of entries to keep
                keep_count = len(latest_entries)
                
                # Get all entries in the time range
                query = select(func.count(InventoryPriceHistory.id))
                if conditions:
                    query = query.where(and_(*conditions))
                
                result = await session.execute(query)
                total_count = result.scalar_one_or_none() or 0
                
                # Calculate entries to remove
                remove_count = total_count - keep_count
                
                # Clear cache
                self._position_cache = {}
                self._position_cache_time = {}
                
                return {
                    "total_entries": total_count,
                    "entries_to_keep": keep_count,
                    "entries_to_remove": remove_count
                }
        except Exception as e:
            self.logger.error(f"Error consolidating positions: {e}")
            return {
                "total_entries": 0,
                "entries_to_keep": 0,
                "entries_to_remove": 0
            } 
