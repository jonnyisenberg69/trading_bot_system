"""
Risk Table Processor Service.

This service processes trades from the database and populates the risk monitoring tables
(inventory_price_history, balance_history, realized_pnl) that are read by the RiskDataService.
"""

from typing import Dict, List, Optional, Any
from datetime import datetime, timezone, timedelta
from decimal import Decimal
import structlog
from sqlalchemy import select, and_, join, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload

from database.models import Trade, Exchange, InventoryPriceHistory
from database.repositories.inventory_price_history_repository import InventoryPriceHistoryRepository
from database.repositories.realized_pnl_repository import RealizedPNLRepository
from database.repositories.balance_history_repository import BalanceHistoryRepository

logger = structlog.get_logger(__name__)


class RiskTableProcessor:
    """Processes trades and populates risk monitoring tables."""
    
    def __init__(self, session_maker):
        self.session_maker = session_maker
        self.logger = logger.bind(component="RiskTableProcessor")
        
        # Position state tracking
        self._positions: Dict[str, Dict[str, float]] = {}  # {exchange_symbol: {amount, total_cost, p1_fee, p2_fee}}
        
        # Minimum time between inventory updates for the same position (in seconds)
        self.min_update_interval = 5
        
        # Last update timestamp per position
        self.last_update_time: Dict[str, datetime] = {}
        
    async def process_trades_to_risk_tables(
        self, 
        start_time: Optional[datetime] = None, 
        end_time: Optional[datetime] = None,
        exchange_ids: Optional[List[int]] = None
    ) -> Dict[str, Any]:
        """
        Process trades from the database and populate risk monitoring tables.
        
        Returns summary of processed data.
        """
        self.logger.info("Starting trade processing for risk tables", 
                        start_time=start_time, end_time=end_time)
        
        async with self.session_maker() as session:
            # Initialize repositories
            inventory_repo = InventoryPriceHistoryRepository(self.session_maker)
            pnl_repo = RealizedPNLRepository(self.session_maker)
            
            # Build query for trades with eager loading of the exchange relationship
            # This is critical to avoid the greenlet_spawn error
            query = select(Trade).options(joinedload(Trade.exchange))
            
            # Apply filters
            conditions = []
            if start_time:
                # Ensure timezone-naive for database query
                if start_time.tzinfo:
                    start_time = start_time.replace(tzinfo=None)
                conditions.append(Trade.timestamp >= start_time)
            if end_time:
                # Ensure timezone-naive for database query
                if end_time.tzinfo:
                    end_time = end_time.replace(tzinfo=None)
                conditions.append(Trade.timestamp <= end_time)
            if exchange_ids:
                conditions.append(Trade.exchange_id.in_(exchange_ids))
            
            if conditions:
                query = query.where(and_(*conditions))
            
            # Order by timestamp to process trades chronologically
            query = query.order_by(Trade.timestamp)
            
            result = await session.execute(query)
            trades = result.scalars().all()
            
            self.logger.info(f"Found {len(trades)} trades to process")
            
            # Clear existing position state
            self._positions.clear()
            self.last_update_time = {}
            
            # First, load the latest inventory entries to initialize position state
            await self._load_existing_positions(session, start_time, end_time, exchange_ids)
            
            trades_processed = 0
            positions_updated = 0
            pnl_events_created = 0
            
            # Process each trade
            for trade in trades:
                # The exchange relationship is now eagerly loaded
                exchange_name = trade.exchange.name
                symbol = trade.symbol
                
                # Process the trade and update position state
                pnl_event = await self._process_trade(
                    trade, exchange_name, inventory_repo, pnl_repo
                )
                
                trades_processed += 1
                positions_updated += 1
                
                if pnl_event:
                    pnl_events_created += 1
            
            # After processing all trades, persist final position states
            for pos_key, position in self._positions.items():
                # Skip very small positions (less than 1e-8)
                if abs(position['amount']) <= 1e-8:
                    continue
                
                exchange_name, symbol = pos_key.split('_', 1)
                
                # Get exchange ID - no need to query again since we have eagerly loaded exchange data
                exchange_id = None
                for trade in trades:
                    if trade.exchange.name == exchange_name:
                        exchange_id = trade.exchange_id
                        break
                
                if exchange_id:
                    # Calculate average entry price
                    avg_entry_price = (
                        (position['total_cost'] + position['p2_fee']) / 
                        (position['amount'] + position['p1_fee'])
                    ) if (position['amount'] + position['p1_fee']) != 0 else 0
                    
                    # Check if we should create a new inventory entry
                    should_update = True
                    
                    # Get the current time
                    now = datetime.utcnow()
                    
                    # Check if we've updated this position recently
                    last_update = self.last_update_time.get(pos_key)
                    if last_update and (now - last_update).total_seconds() < self.min_update_interval:
                        # Skip update if too recent
                        should_update = False
                    
                    if should_update:
                        # Add final inventory state
                        await inventory_repo.add_inventory_price(
                            exchange_id=exchange_id,
                            symbol=symbol,
                            avg_price=avg_entry_price,
                            size=position['amount'],
                            unrealized_pnl=0.0,  # Will be updated by price feeds
                            bot_instance_id=None,
                            timestamp=now
                        )
                        # Update last update time
                        self.last_update_time[pos_key] = now
            
            # Clean up old inventory entries to prevent duplication
            await self._consolidate_inventory_entries(session, start_time, end_time, exchange_ids)
            
            return {
                'trades_processed': trades_processed,
                'positions_updated': positions_updated,
                'pnl_events_created': pnl_events_created,
                'final_positions': len(self._positions)
            }
    
    async def _load_existing_positions(self, session: AsyncSession, start_time: Optional[datetime], end_time: Optional[datetime], exchange_ids: Optional[List[int]]):
        """Load the latest inventory entries to initialize position state."""
        try:
            # Build query to get the latest inventory entry for each exchange_id and symbol
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
            if exchange_ids:
                conditions.append(InventoryPriceHistory.exchange_id.in_(exchange_ids))
            
            if conditions:
                subquery = subquery.where(and_(*conditions))
            
            subquery_cte = subquery.cte("latest_timestamps")
            
            # Join with the original table to get the full records
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
            
            result = await session.execute(query)
            latest_entries = result.all()
            
            # Initialize position state from latest entries
            for entry, exchange_name in latest_entries:
                # Skip entries with zero or very small size
                if not entry.size or abs(entry.size) <= 1e-8:
                    continue
                
                pos_key = f"{exchange_name}_{entry.symbol}"
                self._positions[pos_key] = {
                    'amount': float(entry.size),
                    'total_cost': float(entry.size * entry.avg_price) if entry.avg_price else 0.0,
                    'p1_fee': 0.0,  # Reset fees since we don't have this info
                    'p2_fee': 0.0
                }
                
                # Set last update time to entry timestamp
                self.last_update_time[pos_key] = entry.timestamp
                
            self.logger.info(f"Loaded {len(self._positions)} existing positions")
            
        except Exception as e:
            self.logger.error(f"Error loading existing positions: {e}")
    
    async def _consolidate_inventory_entries(self, session: AsyncSession, start_time: Optional[datetime], end_time: Optional[datetime], exchange_ids: Optional[List[int]]):
        """Consolidate inventory entries to prevent duplication."""
        try:
            # For each exchange_id and symbol, keep only the latest entry
            # First, identify the latest entry for each group
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
            if start_time:
                conditions.append(InventoryPriceHistory.timestamp >= start_time)
            if end_time:
                conditions.append(InventoryPriceHistory.timestamp <= end_time)
            if exchange_ids:
                conditions.append(InventoryPriceHistory.exchange_id.in_(exchange_ids))
            
            if conditions:
                subquery = subquery.where(and_(*conditions))
            
            # Execute subquery to get the latest timestamps
            result = await session.execute(subquery)
            latest_entries = result.all()
            
            # For each group, delete all entries except the latest
            for exchange_id, symbol, max_timestamp in latest_entries:
                # Delete older entries for this exchange_id and symbol
                delete_query = (
                    select(InventoryPriceHistory)
                    .where(
                        and_(
                            InventoryPriceHistory.exchange_id == exchange_id,
                            InventoryPriceHistory.symbol == symbol,
                            InventoryPriceHistory.timestamp < max_timestamp
                        )
                    )
                )
                
                # Apply time range filters
                if start_time:
                    delete_query = delete_query.where(InventoryPriceHistory.timestamp >= start_time)
                if end_time:
                    delete_query = delete_query.where(InventoryPriceHistory.timestamp <= end_time)
                
                # Get the entries to delete
                delete_result = await session.execute(delete_query)
                entries_to_delete = delete_result.scalars().all()
                
                # Delete the entries
                for entry in entries_to_delete:
                    await session.delete(entry)
            
            # Commit the changes
            await session.commit()
            
            self.logger.info(f"Consolidated inventory entries for {len(latest_entries)} positions")
            
        except Exception as e:
            self.logger.error(f"Error consolidating inventory entries: {e}")
    
    async def _process_trade(
        self, 
        trade: Trade, 
        exchange_name: str,
        inventory_repo: InventoryPriceHistoryRepository,
        pnl_repo: RealizedPNLRepository
    ) -> Optional[Dict[str, Any]]:
        """
        Process a single trade and update position state.
        
        Returns realized PNL event if position was closed or flipped.
        """
        symbol = trade.symbol
        pos_key = f"{exchange_name}_{symbol}"
        
        # Get or create position
        position = self._positions.get(pos_key, {
            'amount': 0.0,
            'total_cost': 0.0,
            'p1_fee': 0.0,  # Base currency fee
            'p2_fee': 0.0   # Quote currency fee
        })
        
        # Store state before trade
        old_amount = position['amount']
        old_total_cost = position['total_cost']
        
        # Update position with trade
        trade_amount = float(trade.amount)
        trade_price = float(trade.price)
        trade_cost = float(trade.cost) if trade.cost else trade_amount * trade_price
        side_multiplier = 1 if trade.side == 'buy' else -1
        
        position['amount'] += (trade_amount * side_multiplier)
        position['total_cost'] += (trade_cost * side_multiplier)
        
        # Handle fees
        if trade.fee_cost and trade.fee_currency:
            fee_cost = float(trade.fee_cost)
            fee_currency = trade.fee_currency
            
            if '/' in symbol:
                is_spot = 'spot' in exchange_name
                base_currency, quote_currency = symbol.split('/')
                
                if fee_currency == base_currency:
                    position['p1_fee'] += fee_cost
                    # For spot trades, fee reduces actual amount held
                    if is_spot:
                        position['amount'] -= fee_cost
                elif fee_currency == quote_currency:
                    position['p2_fee'] += fee_cost
                    position['total_cost'] += fee_cost
        
        self._positions[pos_key] = position
        
        # Calculate average entry price for inventory update
        new_amount = position['amount']
        avg_entry_price = (
            (position['total_cost'] + position['p2_fee']) / 
            (new_amount + position['p1_fee'])
        ) if (new_amount + position['p1_fee']) != 0 else 0
        
        # Check if we should create a new inventory entry
        should_update = True
        
        # Skip very small positions
        if abs(new_amount) <= 1e-8:
            should_update = False
        
        # Check if we've updated this position recently
        last_update = self.last_update_time.get(pos_key)
        if last_update and (trade.timestamp - last_update).total_seconds() < self.min_update_interval:
            # Skip update if too recent, unless this is a significant change
            significant_change = abs(new_amount - old_amount) > abs(old_amount) * 0.05  # 5% change
            should_update = significant_change
        
        # Always update on position close or flip
        if old_amount != 0 and (
            new_amount == 0 or 
            (old_amount > 0 and new_amount < 0) or 
            (old_amount < 0 and new_amount > 0)
        ):
            should_update = True
        
        if should_update:
            # Persist to inventory price history
            await inventory_repo.add_inventory_price(
                exchange_id=trade.exchange_id,
                symbol=symbol,
                avg_price=avg_entry_price,
                size=new_amount,
                unrealized_pnl=0.0,  # Zero at trade time
                bot_instance_id=None,
                timestamp=trade.timestamp
            )
            # Update last update time
            self.last_update_time[pos_key] = trade.timestamp
        
        # Check for realized PNL (position closed or flipped)
        pnl_event = None
        if old_amount != 0 and (
            new_amount == 0 or 
            (old_amount > 0 and new_amount < 0) or 
            (old_amount < 0 and new_amount > 0)
        ):
            # Calculate realized PNL
            old_avg_price = (
                (old_total_cost + position['p2_fee']) / 
                (old_amount + position['p1_fee'])
            ) if (old_amount + position['p1_fee']) != 0 else 0
            
            closed_amount = -old_amount
            realized_pnl = (trade_price - old_avg_price) * closed_amount
            
            # Persist realized PNL event
            await pnl_repo.add_pnl_event(
                exchange_id=trade.exchange_id,
                symbol=symbol,
                realized_pnl=realized_pnl,
                event_type="close" if new_amount == 0 else "flip",
                trade_id=trade.id,
                bot_instance_id=None,
                timestamp=trade.timestamp
            )
            
            pnl_event = {
                'symbol': symbol,
                'realized_pnl': realized_pnl,
                'event_type': "close" if new_amount == 0 else "flip"
            }
            
            # If position flipped, reset cost basis
            if new_amount != 0:
                position['total_cost'] = new_amount * trade_price
                position['p1_fee'] = 0
                position['p2_fee'] = 0
                self._positions[pos_key] = position
            else:
                # Position fully closed
                self._positions.pop(pos_key, None)
                # Remove from last update time
                self.last_update_time.pop(pos_key, None)
        
        return pnl_event 
