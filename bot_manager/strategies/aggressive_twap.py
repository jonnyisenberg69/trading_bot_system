"""
Aggressive TWAP Strategy - Time-weighted average price with aggressive price targeting

This strategy aims to push a price to a target price by the end of total time.
It breaks the total time into intervals with intermediate goals and uses smart
order routing to target the biggest price moves with minimal liquidity.
"""

import asyncio
import time
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Optional, Any, Tuple
import logging
import redis.asyncio as redis
import contextlib
import hashlib
import os

from bot_manager.strategies.base_strategy import BaseStrategy
from market_data.redis_orderbook_manager import RedisOrderbookManager
from order_management.execution_engine import ExecutionEngine, ExecutionStrategy
from order_management.order_manager import OrderManager
from exchanges.base_connector import OrderType, OrderSide, OrderStatus
from order_management.tracking import PositionManager


class PositionCurrency(str, Enum):
    """Position currency options."""
    BASE = "base"
    USD = "usd"


class GranularityUnit(str, Enum):
    """Granularity unit options."""
    SECONDS = "seconds"
    MINUTES = "minutes"
    HOURS = "hours"


@dataclass
class PriceInterval:
    """Represents a price interval with target position."""
    interval_id: int
    start_time: datetime
    end_time: datetime
    target_price: Decimal
    target_position: Decimal
    current_position: Decimal = Decimal('0')
    completed: bool = False
    
    def get_remaining_position(self) -> Decimal:
        """Get remaining position for this interval."""
        return max(Decimal('0'), self.target_position - self.current_position)
    
    def is_price_reached(self, current_price: Decimal) -> bool:
        """Check if target price has been reached."""
        return current_price >= self.target_price
    
    def add_position(self, amount: Decimal) -> None:
        """Add to current position."""
        self.current_position += amount
        if self.current_position >= self.target_position:
            self.completed = True


class AggressiveTwapStrategy(BaseStrategy):
    """
    Aggressive TWAP strategy for pushing prices to targets.
    
    Features:
    - Time-based intervals with price targets
    - Smart order routing for minimal liquidity usage
    - Coordination with other strategies (cooldown mechanism)
    - Adaptive position management based on price movement
    """
    
    def __init__(
        self,
        instance_id: str,
        symbol: str,
        exchanges: List[str],
        config: Dict[str, Any],
        redis_url: str = "redis://localhost:6379"
    ):
        super().__init__(instance_id, symbol, exchanges, config)
        
        # Strategy-specific attributes
        self.base_coin: str = ""
        self.total_time_hours: float = 0
        self.granularity_value: int = 0
        self.granularity_unit: str = ""
        self.frequency_seconds: int = 0
        self.target_price: Decimal = Decimal('0')
        self.target_position_total: Decimal = Decimal('0')
        self.position_currency: str = "base"
        self.cooldown_period_seconds: int = 0
        self.configured_start_time: Optional[datetime] = None
        self.price_intervals: List[PriceInterval] = []
        self.current_interval_index: int = 0
        self.last_order_time: Optional[datetime] = None
        self.initial_midpoint: Optional[Decimal] = None
        self.position_log_interval = int(config.get('position_log_interval', 5))  # seconds
        self._position_logger_task: Optional[asyncio.Task] = None
        
        # Execution components
        self.redis_orderbook_manager = None
        self.position_manager = None
        self.execution_engine = None
        self.order_manager = None
        
        # Redis connection for strategy coordination
        self.redis_url = redis_url
        self.redis_client = None
        
        # Main task
        self.main_task: Optional[asyncio.Task] = None
        
        # Performance tracking
        self.orders_placed = 0
        # REMOVED: self.total_position_used - DATABASE IS THE ONLY SOURCE OF TRUTH
        self.price_moves_achieved = 0
        
    async def _validate_config(self) -> None:
        """Validate aggressive TWAP configuration."""
        # Defensive: flatten config if double-nested
        if 'config' in self.config and isinstance(self.config['config'], dict):
            self.logger.info("Detected double-nested config, flattening...")
            self.config = self.config['config']
        
        required_fields = [
            'total_time_hours', 'granularity_value', 'granularity_unit',
            'frequency_seconds', 'target_price', 'target_position_total',
            'position_currency', 'cooldown_period_seconds', 'intervals'
        ]
        
        for field in required_fields:
            if field not in self.config:
                raise ValueError(f"Missing required config field: {field}")
        
        # Validate and set basic parameters
        self.total_time_hours = float(self.config['total_time_hours'])
        self.granularity_value = int(self.config['granularity_value'])
        self.granularity_unit = GranularityUnit(self.config['granularity_unit'])
        self.frequency_seconds = int(self.config['frequency_seconds'])
        self.target_price = Decimal(str(self.config['target_price']))
        self.target_position_total = Decimal(str(self.config['target_position_total']))
        self.position_currency = PositionCurrency(self.config['position_currency'])
        self.cooldown_period_seconds = int(self.config['cooldown_period_seconds'])
        self.base_coin = self.config.get('base_coin', "")
        
        # Validate intervals
        intervals_config = self.config.get('intervals', [])
        if not intervals_config or not isinstance(intervals_config, list):
            raise ValueError("Intervals configuration must be a non-empty list")
        
        # Calculate interval duration
        if self.granularity_unit == GranularityUnit.SECONDS:
            interval_duration = timedelta(seconds=self.granularity_value)
        elif self.granularity_unit == GranularityUnit.MINUTES:
            interval_duration = timedelta(minutes=self.granularity_value)
        else:  # HOURS
            interval_duration = timedelta(hours=self.granularity_value)
        
        # Parse start_time from config - ALWAYS TREAT AS UTC
        start_time_str = self.config.get('start_time')
        self.logger.info(f"Raw start_time from config: {start_time_str} (type: {type(start_time_str)})")
        
        if start_time_str and start_time_str != '':
            try:
                if isinstance(start_time_str, str):
                    # Handle different datetime formats - ALWAYS interpret as UTC
                    if 'T' in start_time_str and start_time_str.endswith('Z'):
                        # ISO format with Z suffix: "2025-06-06T14:23:11.274574Z"
                        self.configured_start_time = datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))
                    elif 'T' in start_time_str and '+' in start_time_str:
                        # ISO format with timezone: "2025-06-06T14:23:11+00:00"
                        self.configured_start_time = datetime.fromisoformat(start_time_str)
                    elif 'T' in start_time_str:
                        # datetime-local format: "2025-06-06T14:23" - ALWAYS treat as UTC
                        naive_dt = datetime.fromisoformat(start_time_str)
                        self.configured_start_time = naive_dt.replace(tzinfo=timezone.utc)
                        self.logger.info(f"Interpreted datetime-local as UTC time: {self.configured_start_time}")
                    else:
                        # Try parsing as space-separated format: "2025-06-06 14:23" - ALWAYS treat as UTC
                        naive_dt = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M")
                        self.configured_start_time = naive_dt.replace(tzinfo=timezone.utc)
                        
                elif isinstance(start_time_str, datetime):
                    # Already a datetime object - ensure it's UTC
                    if start_time_str.tzinfo is None:
                        # Make it timezone-aware as UTC
                        self.configured_start_time = start_time_str.replace(tzinfo=timezone.utc)
                    else:
                        # Convert to UTC if it has a different timezone
                        self.configured_start_time = start_time_str.astimezone(timezone.utc)
                        
                # CRITICAL: Log the final interpreted time clearly
                self.logger.info(f"✅ FINAL start_time (UTC): {self.configured_start_time}")
                
            except (ValueError, TypeError) as e:
                self.logger.error(f"Could not parse start_time '{start_time_str}': {e}")
                self.configured_start_time = datetime.now(timezone.utc)
                self.logger.warning(f"Using current UTC time as start_time: {self.configured_start_time}")
        else:
            self.configured_start_time = datetime.now(timezone.utc)
            self.logger.warning("No start_time provided in config, using current UTC time")
        
        # Parse intervals - ALL times in UTC
        self.price_intervals = []
        start_time = self.configured_start_time
        
        for i, interval_config in enumerate(intervals_config):
            if 'target_price' not in interval_config or 'target_position' not in interval_config:
                raise ValueError(f"Interval {i}: Missing target_price or target_position")
            
            interval_start = start_time + (interval_duration * i)
            interval_end = interval_start + interval_duration
            
            price_interval = PriceInterval(
                interval_id=i,
                start_time=interval_start,
                end_time=interval_end,
                target_price=Decimal(str(interval_config['target_price'])),
                target_position=Decimal(str(interval_config['target_position']))
            )
            
            self.price_intervals.append(price_interval)
            self.logger.info(f"Interval {i}: {interval_start} to {interval_end} UTC, target {price_interval.target_position} {self.base_coin} @ {price_interval.target_price}")
        
        # Validate total position doesn't exceed limit
        total_interval_position = sum(interval.target_position for interval in self.price_intervals)
        if total_interval_position > self.target_position_total:
            raise ValueError(f"Total interval positions ({total_interval_position}) exceed total limit ({self.target_position_total})")
        
        self.logger.info(f"Validated config with {len(self.price_intervals)} intervals over {self.total_time_hours} hours")
        self.logger.info(f"Strategy will run from {self.configured_start_time} to {self.configured_start_time + timedelta(hours=self.total_time_hours)} UTC")
        
    async def _start_strategy(self) -> None:
        """Start the aggressive TWAP strategy."""
        self.logger.info("Starting aggressive TWAP strategy")
        
        # Initialize Redis connection for strategy coordination
        self.redis_client = redis.from_url(self.redis_url)
        
        # Start Redis orderbook manager
        self.redis_orderbook_manager = RedisOrderbookManager(self.redis_url)
        await self.redis_orderbook_manager.start()
        
        # Initialize execution engine and order manager
        await self._initialize_execution_components()
        
        # DISABLED: Trade sync is redundant with WebSocket real-time capture
        # The WebSocket system already captures all trades in real-time with deduplication
        # Running both systems creates duplicate trades  
        # await self._synchronize_all_trades()
        
        # Wait for initial orderbook data and set initial midpoint
        await asyncio.sleep(2)
        await self._set_initial_midpoint()
        
        # Publish initial interval target price
        await self._publish_interval_target_price()
        
        # Start main strategy loop
        self.main_task = asyncio.create_task(self._strategy_loop())
        
        # Start periodic position logger
        self._position_logger_task = asyncio.create_task(self._position_logging_loop())
        
    async def _stop_strategy(self) -> None:
        """Stop the aggressive TWAP strategy."""
        self.logger.info("Stopping aggressive TWAP strategy")
        
        if self.main_task:
            self.main_task.cancel()
            try:
                await self.main_task
            except asyncio.CancelledError:
                pass
        
        # Stop execution components
        if self.execution_engine:
            await self.execution_engine.stop()
        
        # Stop Redis components
        await self.redis_orderbook_manager.stop()
        if self.redis_client:
            await self.redis_client.close()
        
        # Stop position logger
        if self._position_logger_task:
            self._position_logger_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._position_logger_task
    
    async def _initialize_execution_components(self) -> None:
        """Initialize execution engine and order manager."""
        # Initialize position manager
        self.position_manager = PositionManager()
        await self.position_manager.start()
        
        # Set up trade repository for position calculations
        try:
            from database import get_session
            from database.repositories import TradeRepository
            
            # Get database session and set up trade repository
            async for session in get_session():
                trade_repository = TradeRepository(session)
                self.position_manager.set_trade_repository(trade_repository)
                self.logger.info("Trade repository set up for position calculations")
                break  # Only need the first session
        except Exception as e:
            self.logger.warning(f"Could not set up trade repository: {e}")
        
        # Initialize order manager
        self.order_manager = OrderManager(
            exchange_connectors=self.exchange_connectors,
            position_manager=self.position_manager
        )
        
        # Initialize execution engine
        self.execution_engine = ExecutionEngine(
            order_manager=self.order_manager,
            exchange_connectors=self.exchange_connectors
        )
        
        await self.execution_engine.start()
        
    async def _set_initial_midpoint(self) -> None:
        """Set the initial midpoint price for reference."""
        try:
            import asyncio
            # Get current midpoint from Redis orderbook
            if hasattr(self.redis_orderbook_manager, 'get_aggregated_orderbook'):
                orderbook_data = self.redis_orderbook_manager.get_aggregated_orderbook(self.symbol)
                if orderbook_data and 'midpoint' in orderbook_data:
                    self.initial_midpoint = Decimal(str(orderbook_data['midpoint']))
                    self.logger.info(f"Set initial midpoint: {self.initial_midpoint}")
                    return
            else:
                self.logger.warning("RedisOrderbookManager does not have get_aggregated_orderbook method")
            
            # Fallback to individual exchange midpoints
            midpoints = []
            for exchange in self.exchanges:
                try:
                    if hasattr(self.redis_orderbook_manager, 'get_midpoint'):
                        midpoint = self.redis_orderbook_manager.get_midpoint(exchange, self.symbol)
                        if midpoint:
                            midpoints.append(midpoint)
                except Exception as e:
                    self.logger.debug(f"Could not get midpoint from {exchange}: {e}")
                    continue
            
            if midpoints:
                # Use average of available midpoints
                self.initial_midpoint = sum(midpoints) / len(midpoints)
                self.logger.info(f"Set initial midpoint (fallback): {self.initial_midpoint}")
                return
            
            # Final fallback: get midpoints directly from exchanges concurrently
            self.logger.warning("Redis orderbook manager not available, trying concurrent direct exchange fallback...")
            
            # Create concurrent tasks for initial price fetching
            initial_price_tasks = []
            for exchange in self.exchanges:
                connector = self.exchange_connectors.get(exchange)
                if connector:
                    task = self._get_initial_exchange_midpoint(exchange, connector)
                    initial_price_tasks.append((exchange, task))
            
            if initial_price_tasks:
                # Execute all initial price fetching tasks concurrently
                results = await asyncio.gather(*[task for _, task in initial_price_tasks], return_exceptions=True)
                
                midpoints = []
                for i, result in enumerate(results):
                    exchange = initial_price_tasks[i][0]
                    if isinstance(result, Exception):
                        self.logger.debug(f"Could not get direct midpoint from {exchange}: {result}")
                    elif result is not None:
                        midpoints.append(result)
                        self.logger.debug(f"Got direct midpoint from {exchange}: {result}")
                
                if midpoints:
                    # Use average of available midpoints
                    self.initial_midpoint = sum(midpoints) / len(midpoints)
                    self.logger.info(f"✅ Set initial midpoint from concurrent direct exchange fallback: {self.initial_midpoint}")
            else:
                # WARNING: Do not use target price as initial midpoint - this causes false price comparisons
                self.logger.error("❌ Could not get initial midpoint from any exchange!")
                self.logger.error("❌ Strategy may not function correctly without real market data")
                self.initial_midpoint = None
        except Exception as e:
            self.logger.error(f"Error setting initial midpoint: {e}")
            # Do not use target price as fallback
            self.logger.error("❌ Strategy may not function correctly without real market data")
            self.initial_midpoint = None
    
    async def _strategy_loop(self) -> None:
        """Main strategy loop."""
        self.logger.info("Starting aggressive TWAP strategy loop")
        
        # Log initial status
        now = datetime.now(timezone.utc)
        if self.configured_start_time:
            start_time_aware = self.configured_start_time if self.configured_start_time.tzinfo else self.configured_start_time.replace(tzinfo=timezone.utc)
            if now < start_time_aware:
                time_until_start = start_time_aware - now
                self.logger.info(f"Strategy scheduled to start in {time_until_start} at {start_time_aware}")
            else:
                elapsed_time = now - start_time_aware
                self.logger.info(f"Strategy is active and running (configured start: {start_time_aware}, elapsed: {elapsed_time})")
        else:
            self.logger.info("Strategy starting immediately (no start time configured)")
        
        while True:
            try:
                # Check if strategy should be active
                if not await self._should_be_active():
                    await asyncio.sleep(self.frequency_seconds)
                    continue
                
                # Get current interval
                current_interval = self._get_current_interval()
                if not current_interval:
                    self.logger.info("All intervals completed or strategy time expired")
                    break
                
                # Check if interval changed
                if current_interval.interval_id != self.current_interval_index:
                    self.current_interval_index = current_interval.interval_id
                    self.logger.info(f"Moved to interval {self.current_interval_index}")
                    await self._publish_interval_target_price()
                
                # Check current price and determine action
                current_price = await self._get_current_midpoint()
                if current_price is None:
                    self.logger.warning("Could not get current price, waiting...")
                    await asyncio.sleep(self.frequency_seconds)
                    continue
                
                # Get current position before placing orders
                current_position = await self._get_current_total_position()
                
                # Execute interval logic
                await self._execute_interval_logic(current_interval, current_price, current_position)
                
                # Sleep until next execution
                await asyncio.sleep(self.frequency_seconds)
                
            except asyncio.CancelledError:
                self.logger.info("Strategy loop cancelled")
                break
            except Exception as e:
                self.logger.error(f"Error in strategy loop: {e}")
                await asyncio.sleep(5)  # Wait before retrying
    
    async def _should_be_active(self) -> bool:
        """Check if strategy should be active based on cooldown and other factors."""
        # Get current time in UTC to match parsed start_time timezone
        now = datetime.now(timezone.utc)
        
        # Check if we've reached the start time
        if self.configured_start_time:
            # Ensure start_time is timezone-aware
            if self.configured_start_time.tzinfo is None:
                start_time_aware = self.configured_start_time.replace(tzinfo=timezone.utc)
            else:
                start_time_aware = self.configured_start_time
            
            if now < start_time_aware:
                time_until_start = start_time_aware - now
                self.logger.debug(f"Strategy not yet active. Time until start: {time_until_start}")
                return False
            
            # Check if total time has expired
            elapsed_time = now - start_time_aware
            if elapsed_time.total_seconds() > (self.total_time_hours * 3600):
                self.logger.info(f"Strategy time expired. Elapsed: {elapsed_time}, Total allowed: {self.total_time_hours} hours")
                return False
        
        # Check if we're in cooldown period
        if self.last_order_time:
            # Ensure last_order_time is timezone-aware
            if self.last_order_time.tzinfo is None:
                last_order_time_aware = self.last_order_time.replace(tzinfo=timezone.utc)
            else:
                last_order_time_aware = self.last_order_time
            
            time_since_last_order = now - last_order_time_aware
            if time_since_last_order.total_seconds() < self.cooldown_period_seconds:
                self.logger.debug(f"In cooldown period. Time since last order: {time_since_last_order.total_seconds()}s")
                return False
        
        return True
    
    def _get_current_interval(self) -> Optional[PriceInterval]:
        """Get the current active interval."""
        # Get current time in UTC to match interval timezone
        now = datetime.now(timezone.utc)
        
        # Find the current interval based on time
        for i, interval in enumerate(self.price_intervals):
            # Ensure interval times are timezone-aware
            start_time = interval.start_time
            end_time = interval.end_time
            
            if start_time.tzinfo is None:
                start_time = start_time.replace(tzinfo=timezone.utc)
            if end_time.tzinfo is None:
                end_time = end_time.replace(tzinfo=timezone.utc)
            
            if start_time <= now <= end_time and not interval.completed:
                self.current_interval_index = i
                return interval
        
        # If no current interval, check if we need to fall back
        for i in range(len(self.price_intervals) - 1, -1, -1):
            interval = self.price_intervals[i]
            if not interval.completed and interval.get_remaining_position() > 0:
                self.current_interval_index = i
                return interval
        
        return None
    
    async def _get_current_midpoint(self) -> Optional[Decimal]:
        """Get current midpoint price."""
        try:
            import asyncio
            # Get aggregated midpoint from Redis orderbook
            if hasattr(self.redis_orderbook_manager, 'get_aggregated_orderbook'):
                orderbook_data = self.redis_orderbook_manager.get_aggregated_orderbook(self.symbol)
                if orderbook_data and 'midpoint' in orderbook_data:
                    current_price = Decimal(str(orderbook_data['midpoint']))
                    self.logger.debug(f"Got aggregated midpoint: {current_price}")
                    return current_price
            
            # Fallback to individual exchange midpoints
            midpoints = []
            for exchange in self.exchanges:
                try:
                    if hasattr(self.redis_orderbook_manager, 'get_midpoint'):
                        midpoint = self.redis_orderbook_manager.get_midpoint(exchange, self.symbol)
                        if midpoint:
                            midpoints.append(midpoint)
                            self.logger.debug(f"Got midpoint from {exchange}: {midpoint}")
                except Exception as e:
                    self.logger.debug(f"Could not get midpoint from {exchange}: {e}")
                    continue
            
            if midpoints:
                # Return average of available midpoints
                avg_price = sum(midpoints) / len(midpoints)
                self.logger.debug(f"Calculated average midpoint from {len(midpoints)} exchanges: {avg_price}")
                return avg_price
            
            # Fallback: Get price directly from exchange connectors concurrently
            self.logger.warning("Redis orderbook manager not available, trying concurrent direct exchange fallback...")
            
            # Create concurrent tasks for price fetching
            price_tasks = []
            for exchange in self.exchanges:
                connector = self.exchange_connectors.get(exchange)
                if connector:
                    task = self._get_exchange_midpoint(exchange, connector)
                    price_tasks.append((exchange, task))
            
            if price_tasks:
                # Execute all price fetching tasks concurrently
                results = await asyncio.gather(*[task for _, task in price_tasks], return_exceptions=True)
                
                direct_midpoints = []
                for i, result in enumerate(results):
                    exchange = price_tasks[i][0]
                    if isinstance(result, Exception):
                        self.logger.debug(f"Could not get direct price from {exchange}: {result}")
                    elif result is not None:
                        direct_midpoints.append(result)
                        self.logger.debug(f"Got direct midpoint from {exchange}: {result}")
                
                if direct_midpoints:
                    avg_direct_price = sum(direct_midpoints) / len(direct_midpoints)
                    self.logger.info(f"✅ Got price from concurrent direct exchange fallback: {avg_direct_price} (from {len(direct_midpoints)} exchanges)")
                    return avg_direct_price
            
            # WARNING: Do not use target price as current price - this causes false price targets
            self.logger.error("❌ No real-time price data available from any exchange!")
            self.logger.error("❌ Cannot determine current market price - this may cause incorrect strategy decisions")
            
            # Only use initial midpoint if it's significantly different from target price
            if self.initial_midpoint and abs(self.initial_midpoint - self.target_price) > Decimal('0.1'):
                self.logger.warning(f"Using initial midpoint as emergency fallback: {self.initial_midpoint} (target: {self.target_price})")
                return self.initial_midpoint
                
        except Exception as e:
            self.logger.error(f"Error getting current midpoint: {e}")
        
        # Return None to indicate no valid price data available
        self.logger.error("❌ No valid price data available - strategy will wait for next cycle")
        return None

    async def _get_exchange_midpoint(self, exchange: str, connector) -> Optional[Decimal]:
        """Get midpoint price from a single exchange."""
        try:
            # Get orderbook directly from exchange
            orderbook = await connector.get_orderbook(self.symbol, limit=5)
            if orderbook and 'bids' in orderbook and 'asks' in orderbook:
                bids = orderbook['bids']
                asks = orderbook['asks']
                
                if bids and asks and len(bids) > 0 and len(asks) > 0:
                    best_bid = Decimal(str(bids[0][0]))
                    best_ask = Decimal(str(asks[0][0]))
                    midpoint = (best_bid + best_ask) / 2
                    return midpoint
            return None
        except Exception as e:
            # Don't log here, let the caller handle logging
            raise e

    async def _get_initial_exchange_midpoint(self, exchange: str, connector) -> Optional[Decimal]:
        """Get initial midpoint price from a single exchange."""
        try:
            # Get orderbook directly from exchange (use fetch_order_book for initial setup)
            orderbook = await connector.fetch_order_book(self.symbol, limit=5)
            if orderbook and 'bids' in orderbook and 'asks' in orderbook:
                if orderbook['bids'] and orderbook['asks']:
                    best_bid = Decimal(str(orderbook['bids'][0][0]))
                    best_ask = Decimal(str(orderbook['asks'][0][0]))
                    midpoint = (best_bid + best_ask) / 2
                    return midpoint
            return None
        except Exception as e:
            # Don't log here, let the caller handle logging
            raise e

    async def _get_exchange_orderbook(self, exchange: str) -> Optional[Dict]:
        """Get orderbook data from a single exchange."""
        try:
            if hasattr(self.redis_orderbook_manager, 'get_orderbook'):
                orderbook = self.redis_orderbook_manager.get_orderbook(exchange, self.symbol)
                return orderbook
            return None
        except Exception as e:
            # Don't log here, let the caller handle logging
            raise e
    
    async def _get_current_total_position(self) -> Decimal:
        """Get current total position across all exchanges since strategy start time."""
        try:
            # Use hybrid approach: database trades + real-time order tracking
            database_position = await self._calculate_position_since_start_time()
            realtime_position = self._calculate_realtime_position_since_start()
            
            # Use database position as primary source of truth, with real-time adjustments
            if database_position is not None and realtime_position is not None:
                # Database position is the authoritative source, real-time tracks pending changes
                # For aggressive TWAP (buy strategy), we want the more conservative (higher) position
                # But we need to handle negative positions correctly
                if database_position >= 0 and realtime_position >= 0:
                    # Both positive: use the higher (more conservative for buy strategy)
                    final_position = max(database_position, realtime_position)
                elif database_position < 0 and realtime_position >= 0:
                    # Database negative, real-time positive: use real-time (more recent)
                    final_position = realtime_position
                    self.logger.warning(f"Database shows negative position ({database_position}), using real-time position ({realtime_position})")
                elif database_position >= 0 and realtime_position < 0:
                    # Database positive, real-time negative: use database (more reliable)
                    final_position = database_position
                    self.logger.warning(f"Real-time shows negative position ({realtime_position}), using database position ({database_position})")
                else:
                    # Both negative: use the less negative (closer to zero)
                    final_position = max(database_position, realtime_position)
                    self.logger.warning(f"Both positions negative - DB: {database_position}, RT: {realtime_position}, using: {final_position}")
                
                self.logger.info(f"Position calculation: DB={database_position}, RT={realtime_position}, Final={final_position} {self.base_coin}")
                
                # Update interval progress based on actual position
                await self._update_interval_progress(final_position)
                
                return final_position
            elif database_position is not None:
                self.logger.info(f"Using database position: {database_position} {self.base_coin}")
                await self._update_interval_progress(database_position)
                return database_position
            elif realtime_position is not None:
                self.logger.info(f"Using real-time position: {realtime_position} {self.base_coin}")
                await self._update_interval_progress(realtime_position)
                return realtime_position
            else:
                # NO FALLBACKS: Database is the only source of truth
                # If database calculation fails, return 0 and wait for next iteration
                self.logger.error("❌ Database position calculation failed - cannot determine position")
                self.logger.error("❌ Strategy will use 0 position and wait for next cycle")
                self.logger.warning("⚠️ This ensures database is always the source of truth")
                return Decimal('0')
                
        except Exception as e:
            self.logger.error(f"Error in position calculation: {e}")
            self.logger.error("❌ Database is the only source of truth - returning 0 position")
            return Decimal('0')  # No fallback to in-memory tracking

    async def _update_interval_progress(self, total_position: Decimal) -> None:
        """Update interval progress based on actual total position."""
        try:
            # Reset all interval positions first
            for interval in self.price_intervals:
                interval.current_position = Decimal('0')
                interval.completed = False
            
            # Distribute position across intervals in chronological order
            remaining_position = total_position
            now = datetime.now(timezone.utc)
            
            for interval in self.price_intervals:
                if remaining_position <= 0:
                    break
                
                # Check if this interval should have any position yet
                interval_start = interval.start_time if interval.start_time.tzinfo else interval.start_time.replace(tzinfo=timezone.utc)
                
                if now >= interval_start:
                    # This interval should have started, allocate position to it
                    position_for_interval = min(remaining_position, interval.target_position)
                    interval.current_position = position_for_interval
                    remaining_position -= position_for_interval
                    
                    # Mark as completed if target reached
                    if interval.current_position >= interval.target_position:
                        interval.completed = True
                    
                    self.logger.debug(f"Interval {interval.interval_id}: {interval.current_position}/{interval.target_position} {self.base_coin} "
                                    f"({'✅ completed' if interval.completed else f'{interval.get_remaining_position()} remaining'})")
            
            # Log overall progress
            completed_intervals = sum(1 for interval in self.price_intervals if interval.completed)
            total_intervals = len(self.price_intervals)
            total_progress = (total_position / self.target_position_total * 100) if self.target_position_total > 0 else 0
            
            self.logger.info(f"📊 Progress: {total_position}/{self.target_position_total} {self.base_coin} ({total_progress:.1f}%), "
                           f"Intervals: {completed_intervals}/{total_intervals} completed")
            
        except Exception as e:
            self.logger.error(f"Error updating interval progress: {e}")

    def _calculate_realtime_position_since_start(self) -> Optional[Decimal]:
        """Real-time position tracking is now replaced by database calculations."""
        try:
            if not self.configured_start_time:
                return None
                
            # DATABASE IS THE ONLY SOURCE OF TRUTH
            # Real-time position tracking has been removed in favor of database calculations
            # Return None to force use of database position
            pending_orders_count = len(self.active_orders)
            
            self.logger.debug(f"Real-time position tracking disabled - using database only ({pending_orders_count} pending orders)")
            return None
            
        except Exception as e:
            self.logger.debug(f"Error in real-time position calculation: {e}")
            return None

    async def _calculate_position_since_start_time(self) -> Optional[Decimal]:
        """
        Calculate total position based on trades since strategy start time.
        This includes ALL trades for the symbol, not just those from this bot instance.
        """
        try:
            from database import get_session, close_db, init_db
            from database.repositories import TradeRepository
            from sqlalchemy import text
            
            total_position = Decimal('0')
            total_trades_found = 0
            
            # ALTERNATIVE APPROACH: If session management alone doesn't work,
            # force a complete reconnection to ensure absolutely fresh state
            if hasattr(self, '_force_fresh_db_connection') and self._force_fresh_db_connection:
                self.logger.debug("Forcing fresh database connection")
                await close_db()
                await init_db()
            
            # Get a fresh session for position calculation
            async for session in get_session():
                try:
                    # CRITICAL: Begin a new transaction to ensure we see all committed data
                    # This forces the session to start fresh and see the latest commits
                    await session.begin()
                    
                    trade_repository = TradeRepository(session)
                    
                    # Convert timezone-aware datetime to timezone-naive for database query
                    start_time_for_db = self.configured_start_time
                    if start_time_for_db.tzinfo is not None:
                        # Convert to UTC and remove timezone info for database
                        start_time_for_db = start_time_for_db.astimezone(timezone.utc).replace(tzinfo=None)
                    
                    self.logger.debug(f"Calculating position since start time: {start_time_for_db}")
                    
                    # Get trades for each exchange since start time
                    for exchange in self.exchanges:
                        try:
                            # Get exchange ID from database
                            from database.models import Exchange
                            from sqlalchemy import select
                            
                            stmt = select(Exchange.id).where(Exchange.name == exchange)
                            result = await trade_repository.session.execute(stmt)
                            exchange_id = result.scalar_one_or_none()
                            
                            if not exchange_id:
                                self.logger.debug(f"Exchange {exchange} not found in database")
                                continue
                            
                            self.logger.debug(f"Querying trades for exchange {exchange} (ID: {exchange_id})")
                            
                            # Get ALL trades since start time for this exchange and symbol
                            # Don't filter by bot_instance_id - count everything
                            trades = await trade_repository.get_trades_by_symbol(
                                symbol=self.symbol,
                                exchange_id=exchange_id,
                                bot_instance_id=None,  # Explicitly set to None to get ALL trades
                                start_time=start_time_for_db,
                                limit=1000  # Get more trades to ensure we capture all
                            )
                            
                            # Calculate position from trades
                            exchange_position = Decimal('0')
                            exchange_trades = 0
                            
                            for trade in trades:
                                if trade.side.lower() == 'buy':
                                    exchange_position += Decimal(str(trade.amount))
                                elif trade.side.lower() == 'sell':
                                    exchange_position -= Decimal(str(trade.amount))
                                exchange_trades += 1
                                
                                # Log first few trades for debugging
                                if exchange_trades <= 3:
                                    self.logger.debug(
                                        f"Trade {exchange_trades}: {trade.timestamp} | {trade.side} {trade.amount} @ {trade.price} | "
                                        f"ID: {trade.exchange_trade_id}"
                                    )
                            
                            total_position += exchange_position
                            total_trades_found += exchange_trades
                            
                            if exchange_trades > 0:
                                self.logger.debug(f"Found {exchange_trades} trades since start time for {exchange}: position {exchange_position} {self.base_coin}")
                            
                        except Exception as e:
                            self.logger.debug(f"Error calculating position for {exchange}: {e}")
                            continue
                    
                    self.logger.info(f"Database position calculation: {total_trades_found} trades found, total position: {total_position} {self.base_coin}")
                    
                    # Commit the read-only transaction
                    await session.commit()
                    
                    return total_position
                    
                except Exception as e:
                    # Rollback on any error
                    await session.rollback()
                    raise e
                finally:
                    # Always close the session to return connection to pool
                    await session.close()
            
        except Exception as e:
            self.logger.error(f"Error calculating position since start time: {e}")
            return None
    
    async def _execute_interval_logic(self, interval: PriceInterval, current_price: Decimal, current_position: Decimal) -> None:
        """Execute the logic for the current interval."""
        # Check if target price is already reached
        if interval.is_price_reached(current_price):
            self.logger.info(f"Interval {interval.interval_id}: Target price {interval.target_price} reached (current: {current_price})")
            
            # If we've moved beyond target, adjust future targets
            price_overshoot = current_price - interval.target_price
            if price_overshoot > 0:
                await self._adjust_future_targets(price_overshoot)
            
            # Mark interval as completed
            interval.completed = True
            self.price_moves_achieved += 1
            return
        
        # Check if we have remaining position to use
        remaining_position = interval.get_remaining_position()
        if remaining_position <= 0:
            self.logger.info(f"Interval {interval.interval_id}: No remaining position")
            return
        
        # Log initial position state
        self.logger.info(f"📊 LIQUIDITY DISTRIBUTION - Initial State:")
        self.logger.info(f"  - Current Position: {current_position} {self.base_coin}")
        self.logger.info(f"  - Interval Target Position: {interval.target_position} {self.base_coin}")
        self.logger.info(f"  - Interval Current Position: {interval.current_position} {self.base_coin}")
        self.logger.info(f"  - Interval Remaining Position: {remaining_position} {self.base_coin}")
        self.logger.info(f"  - Total Position Limit: {self.target_position_total} {self.base_coin}")
        
        # Safety check: Handle negative positions properly
        if current_position < 0:
            self.logger.warning(f"⚠️ Current position is negative ({current_position} {self.base_coin})")
            self.logger.warning(f"⚠️ This indicates net selling has occurred - strategy will account for this")
            
            # For negative positions, we need to buy more to reach our target
            # The effective remaining position is larger because we need to overcome the negative position
            effective_remaining = self.target_position_total - current_position
            self.logger.info(f"Effective remaining position accounting for negative balance: {effective_remaining} {self.base_coin}")
            
            # Use the larger of interval remaining or effective remaining
            remaining_position = max(remaining_position, effective_remaining)
        
        # Check if we would exceed total position limit (accounting for negative positions)
        potential_new_position = current_position + remaining_position
        if current_position >= 0 and potential_new_position > self.target_position_total:
            # Only apply limit if current position is positive
            max_additional_position = self.target_position_total - current_position
            if max_additional_position <= 0:
                self.logger.info(f"Interval {interval.interval_id}: Would exceed total position limit")
                return
            self.logger.info(f"📊 Position limit adjustment: {remaining_position} -> {max_additional_position} {self.base_coin}")
            remaining_position = min(remaining_position, max_additional_position)
        elif current_position < 0:
            # For negative positions, allow larger orders to reach target
            max_order_size = self.target_position_total * Decimal('0.5')  # Allow up to 50% of target in one order
            self.logger.info(f"📊 Negative position recovery: max order size = {max_order_size} {self.base_coin}")
            remaining_position = min(remaining_position, max_order_size)
        
        # Calculate order size (don't use all remaining position at once)
        if current_position < 0:
            # For negative positions, use larger order sizes to recover faster
            order_percentage = Decimal('0.5')  # 50%
            order_size = min(remaining_position, remaining_position * order_percentage)
            self.logger.info(f"📊 Order sizing (negative position recovery): {order_percentage*100}% of {remaining_position} = {order_size} {self.base_coin}")
        else:
            # For positive positions, use conservative sizing
            order_percentage = Decimal('0.3')  # 30%
            order_size = min(remaining_position, remaining_position * order_percentage)
            self.logger.info(f"📊 Order sizing (normal): {order_percentage*100}% of {remaining_position} = {order_size} {self.base_coin}")
        
        self.logger.info(f"📊 FINAL ORDER DECISION: size={order_size}, current_pos={current_position}, remaining={remaining_position}, limit={self.target_position_total}")
        
        # Place aggressive order to push price toward target
        await self._place_aggressive_order(interval, current_price, order_size)
        
        # Publish updated interval target price after order placement
        await self._publish_interval_target_price()
        
        # Signal cooldown to other strategies
        await self._signal_strategy_cooldown()
    
    async def _adjust_future_targets(self, price_overshoot: Decimal) -> None:
        """Adjust future interval targets when price moves beyond current target."""
        for i in range(self.current_interval_index + 1, len(self.price_intervals)):
            interval = self.price_intervals[i]
            if not interval.completed:
                interval.target_price += price_overshoot
                self.logger.info(f"Adjusted interval {interval.interval_id} target price to {interval.target_price}")
    
    async def _place_aggressive_order(self, interval: PriceInterval, current_price: Decimal, order_size: Decimal) -> None:
        """Place an aggressive order to push price toward target using smart routing."""
        try:
            # Signal cooldown to other strategies
            await self._signal_strategy_cooldown()
            
            # Update last order time
            self.last_order_time = datetime.now(timezone.utc)
            
            # Determine order side (buy to push price up)
            side = OrderSide.BUY if interval.target_price > current_price else OrderSide.SELL
            
            self.logger.info(f"🚀 INITIATING SMART ROUTING ORDER CAMPAIGN:")
            self.logger.info(f"  - Interval: {interval.interval_id}")
            self.logger.info(f"  - Side: {side.value}")
            self.logger.info(f"  - Total Size: {order_size} {self.base_coin}")
            self.logger.info(f"  - Current Price: {current_price}")
            self.logger.info(f"  - Target Price: {interval.target_price}")
            self.logger.info(f"  - Price Movement Needed: {abs(interval.target_price - current_price)} ({abs((interval.target_price - current_price) / current_price * 100):.2f}%)")
            
            # Analyze orderbooks to determine optimal allocation
            exchange_allocations = await self._calculate_smart_routing_allocation(
                side, order_size, current_price, interval.target_price
            )
            
            if not exchange_allocations:
                self.logger.error("No valid exchange allocations calculated")
                return
            
            # Execute orders based on smart routing allocation
            order_tasks = []
            total_allocated_size = Decimal('0')
            
            self.logger.info(f"📋 PREPARING ORDERS FOR {len(exchange_allocations)} EXCHANGES:")
            
            for exchange, allocation in exchange_allocations.items():
                order_size_for_exchange = allocation['size']
                order_price = allocation['price']
                expected_impact = allocation['expected_impact']
                
                self.logger.info(
                    f"  📍 {exchange}:"
                )
                self.logger.info(
                    f"     - Size: {order_size_for_exchange} {self.base_coin}"
                )
                self.logger.info(
                    f"     - Price: {order_price} (current: {current_price})"
                )
                self.logger.info(
                    f"     - Expected Impact: {expected_impact*100:.4f}%"
                )
                self.logger.info(
                    f"     - Notional: ${order_size_for_exchange * order_price:.2f}"
                )
                
                task = self._place_single_aggressive_order(
                    exchange=exchange,
                    side=side,
                    amount=order_size_for_exchange,
                    price=order_price
                )
                order_tasks.append((exchange, task, order_size_for_exchange))
                total_allocated_size += order_size_for_exchange
            
            # Log allocation efficiency
            allocation_efficiency = (total_allocated_size / order_size * 100) if order_size > 0 else 0
            self.logger.info(f"📊 ALLOCATION EFFICIENCY: {total_allocated_size} / {order_size} = {allocation_efficiency:.1f}%")
            
            # Execute all orders simultaneously for maximum price impact
            self.logger.info(f"⚡ EXECUTING {len(order_tasks)} ORDERS SIMULTANEOUSLY...")
            start_time = datetime.now(timezone.utc)
            order_results = await asyncio.gather(*[task for _, task, _ in order_tasks], return_exceptions=True)
            execution_time = (datetime.now(timezone.utc) - start_time).total_seconds()
            
            # Count successful orders
            successful_orders = 0
            failed_orders = 0
            total_executed_size = Decimal('0')
            execution_summary = []
            
            for i, result in enumerate(order_results):
                exchange, _, exchange_order_size = order_tasks[i]
                if isinstance(result, Exception):
                    failed_orders += 1
                    execution_summary.append(f"  ❌ {exchange}: FAILED - {result}")
                    self.logger.warning(f"Smart-routed order failed on {exchange}: {result}")
                elif result:
                    successful_orders += 1
                    total_executed_size += exchange_order_size
                    execution_summary.append(f"  ✅ {exchange}: SUCCESS - Order ID: {result}")
                    self.logger.info(f"Smart-routed order placed successfully on {exchange}: {result}")
                else:
                    failed_orders += 1
                    execution_summary.append(f"  ❌ {exchange}: FAILED - No order ID returned")
            
            # Log execution summary
            self.logger.info(f"📊 EXECUTION SUMMARY (completed in {execution_time:.2f}s):")
            for summary in execution_summary:
                self.logger.info(summary)
            
            if successful_orders > 0:
                # **DO NOT UPDATE POSITION HERE**: Orders placed ≠ trades executed
                # Position updates will happen when actual trades are confirmed via WebSocket or trade sync
                self.orders_placed += successful_orders
                
                success_rate = (successful_orders / len(order_tasks) * 100) if order_tasks else 0
                execution_efficiency = (total_executed_size / order_size * 100) if order_size > 0 else 0
                
                self.logger.info(f"✅ SMART ROUTING CAMPAIGN COMPLETED:")
                self.logger.info(f"  - Success Rate: {successful_orders}/{len(order_tasks)} ({success_rate:.1f}%)")
                self.logger.info(f"  - Total Size Executed: {total_executed_size} {self.base_coin}")
                self.logger.info(f"  - Execution Efficiency: {execution_efficiency:.1f}%")
                self.logger.info(f"  - Failed Orders: {failed_orders}")
                self.logger.info(f"⚠️  Position will be updated when trades are confirmed, not when orders are placed")
            else:
                self.logger.error(f"❌ ALL ORDERS FAILED - No liquidity distributed")
            
        except Exception as e:
            self.logger.error(f"Error placing smart-routed aggressive order: {e}")

    async def _calculate_smart_routing_allocation(
        self, 
        side: OrderSide, 
        total_budget: Decimal, 
        current_price: Decimal, 
        target_price: Decimal
    ) -> Dict[str, Dict[str, Decimal]]:
        """Calculate optimal allocation across exchanges for maximum price impact."""
        try:
            allocations = {}
            
            self.logger.info(f"🔄 SMART ROUTING CALCULATION START:")
            self.logger.info(f"  - Side: {side.value}")
            self.logger.info(f"  - Total Budget: {total_budget} {self.base_coin}")
            self.logger.info(f"  - Current Price: {current_price}")
            self.logger.info(f"  - Target Price: {target_price}")
            self.logger.info(f"  - Price Gap: {abs(target_price - current_price)} ({abs((target_price - current_price) / current_price * 100):.2f}%)")
            
            # First, check which exchanges are actually connected and have sufficient balance
            available_exchanges = []
            balance_check_tasks = []
            
            for exchange in self.exchanges:
                connector = self.exchange_connectors.get(exchange)
                if connector:
                    # Check if connector is actually connected
                    if hasattr(connector, 'connected') and connector.connected:
                        task = self._check_exchange_balance(exchange, connector, side, total_budget, current_price)
                        balance_check_tasks.append((exchange, task))
                    else:
                        self.logger.warning(f"Exchange {exchange} is not connected, skipping")
                else:
                    self.logger.warning(f"No connector for {exchange}, skipping")
            
            # Execute balance checks concurrently
            if balance_check_tasks:
                results = await asyncio.gather(*[task for _, task in balance_check_tasks], return_exceptions=True)
                
                for i, result in enumerate(results):
                    exchange = balance_check_tasks[i][0]
                    if isinstance(result, Exception):
                        self.logger.warning(f"Could not check balance for {exchange}: {result}")
                    elif result:
                        available_exchanges.append(exchange)
                        self.logger.debug(f"Exchange {exchange} is available with sufficient balance")
                    else:
                        self.logger.warning(f"Exchange {exchange} has insufficient balance or is unavailable")
            
            if not available_exchanges:
                self.logger.error("No exchanges available with sufficient balance")
                return {}
            
            self.logger.info(f"📊 Available exchanges for smart routing: {available_exchanges}")
            
            # Get orderbook data for available exchanges concurrently
            orderbook_tasks = []
            for exchange in available_exchanges:
                task = self._get_exchange_orderbook(exchange)
                orderbook_tasks.append((exchange, task))
            
            # Execute all orderbook fetching tasks concurrently
            if orderbook_tasks:
                results = await asyncio.gather(*[task for _, task in orderbook_tasks], return_exceptions=True)
                
                exchange_orderbooks = {}
                for i, result in enumerate(results):
                    exchange = orderbook_tasks[i][0]
                    if isinstance(result, Exception):
                        self.logger.debug(f"Could not get orderbook for {exchange}: {result}")
                    elif result and 'bids' in result and 'asks' in result:
                        exchange_orderbooks[exchange] = result
            
            if not exchange_orderbooks:
                self.logger.warning("No orderbook data available, falling back to equal allocation among available exchanges")
                return await self._fallback_equal_allocation_available(side, total_budget, current_price, available_exchanges)
            
            self.logger.info(f"📊 Got orderbook data from {len(exchange_orderbooks)} exchanges")
            
            # Analyze each exchange's orderbook for price impact potential
            exchange_impacts = {}
            
            for exchange, orderbook in exchange_orderbooks.items():
                impact_analysis = self._analyze_price_impact_potential(
                    orderbook, side, current_price, target_price
                )
                if impact_analysis:
                    exchange_impacts[exchange] = impact_analysis
                    self.logger.info(f"📈 {exchange} impact analysis:")
                    self.logger.info(f"    - Expected Impact: {impact_analysis['expected_impact']*100:.4f}%")
                    self.logger.info(f"    - Efficiency Score: {impact_analysis['efficiency']:.6f} (higher = better for aggressive)")
                    self.logger.info(f"    - Optimal Price: {impact_analysis['optimal_price']}")
                    self.logger.info(f"    - Available Volume: {impact_analysis['available_volume']}")
            
            if not exchange_impacts:
                self.logger.warning("No valid impact analysis, falling back to equal allocation among available exchanges")
                return await self._fallback_equal_allocation_available(side, total_budget, current_price, available_exchanges)
            
            # Allocate budget based on price impact efficiency
            total_efficiency = sum(impact['efficiency'] for impact in exchange_impacts.values())
            self.logger.info(f"📊 Total efficiency score across exchanges: {total_efficiency:.6f}")
            
            allocation_details = []
            
            for exchange, impact in exchange_impacts.items():
                # Allocate budget proportional to efficiency
                efficiency_ratio = impact['efficiency'] / total_efficiency
                allocated_budget = total_budget * efficiency_ratio
                
                # Ensure minimum order size requirements
                precision = self._get_exchange_precision(exchange, self.symbol)
                min_notional = Decimal(str(precision['min_notional']))
                
                allocation_info = {
                    'exchange': exchange,
                    'efficiency_ratio': efficiency_ratio,
                    'allocated_budget': allocated_budget,
                    'min_notional': min_notional,
                    'optimal_price': impact['optimal_price'],
                    'notional_value': allocated_budget * impact['optimal_price']
                }
                allocation_details.append(allocation_info)
                
                # Calculate order size and price for this exchange
                if allocated_budget * impact['optimal_price'] >= min_notional:
                    allocations[exchange] = {
                        'size': allocated_budget,
                        'price': impact['optimal_price'],
                        'expected_impact': impact['expected_impact'],
                        'efficiency': impact['efficiency']
                    }
                    self.logger.info(f"✅ {exchange} allocation APPROVED:")
                    self.logger.info(f"    - Efficiency Ratio: {efficiency_ratio*100:.2f}%")
                    self.logger.info(f"    - Allocated Size: {allocated_budget} {self.base_coin}")
                    self.logger.info(f"    - Price: {impact['optimal_price']}")
                    self.logger.info(f"    - Notional: ${allocated_budget * impact['optimal_price']:.2f} (min: ${min_notional})")
                else:
                    self.logger.debug(f"❌ {exchange} allocation REJECTED:")
                    self.logger.debug(f"    - Allocated budget too small: ${allocated_budget * impact['optimal_price']:.2f} < ${min_notional}")
            
            # Log allocation summary
            if allocations:
                total_allocated = sum(alloc['size'] for alloc in allocations.values())
                self.logger.info(f"📊 ALLOCATION SUMMARY:")
                self.logger.info(f"  - Exchanges Used: {len(allocations)}/{len(exchange_impacts)}")
                self.logger.info(f"  - Total Allocated: {total_allocated} {self.base_coin} ({total_allocated/total_budget*100:.1f}% of budget)")
                
                # Show distribution percentages
                self.logger.info(f"📊 DISTRIBUTION BREAKDOWN:")
                for exchange, alloc in allocations.items():
                    percentage = alloc['size'] / total_allocated * 100
                    self.logger.info(f"  - {exchange}: {alloc['size']:.4f} {self.base_coin} ({percentage:.1f}%)")
            
            # If no allocations meet minimum requirements, redistribute
            if not allocations:
                self.logger.warning("No allocations meet minimum requirements, using largest available exchanges")
                return await self._fallback_largest_exchanges_available(side, total_budget, current_price, exchange_impacts, available_exchanges)
            
            self.logger.info(f"✅ Smart routing calculated allocations for {len(allocations)} exchanges")
            return allocations
            
        except Exception as e:
            self.logger.error(f"Error calculating smart routing allocation: {e}")
            # Fallback to equal allocation among strategy-configured exchanges only
            return await self._fallback_equal_allocation(side, total_budget, current_price)
    
    async def _check_exchange_balance(self, exchange: str, connector, side: OrderSide, required_amount: Decimal, price: Decimal) -> bool:
        """Check if exchange has sufficient balance for the order."""
        # BYPASS: Optimistically assume sufficient balance for all exchanges
        self.logger.debug(f"Balance check bypassed for {exchange} - assuming sufficient balance")
        return True
    
    async def _fallback_equal_allocation_available(
        self, 
        side: OrderSide, 
        total_budget: Decimal, 
        current_price: Decimal,
        available_exchanges: List[str]
    ) -> Dict[str, Dict[str, Decimal]]:
        """Fallback to equal allocation across available exchanges only."""
        allocations = {}
        
        self.logger.info(f"📊 FALLBACK: Using equal allocation strategy")
        self.logger.info(f"  - Reason: No orderbook data or impact analysis available")
        self.logger.info(f"  - Available Exchanges: {available_exchanges}")
        self.logger.info(f"  - Budget per Exchange: {total_budget / len(available_exchanges) if available_exchanges else 0} {self.base_coin}")
        
        if not available_exchanges:
            return allocations
        
        # Calculate aggressive price
        if side == OrderSide.BUY:
            aggressive_price = current_price * Decimal('1.01')  # 1% above
        else:
            aggressive_price = current_price * Decimal('0.99')  # 1% below
        
        self.logger.info(f"  - Aggressive Price: {aggressive_price} ({'+' if side == OrderSide.BUY else '-'}1% from {current_price})")
        
        # Equal allocation across available exchanges
        valid_exchanges = []
        for exchange in available_exchanges:
            precision = self._get_exchange_precision(exchange, self.symbol)
            min_notional = Decimal(str(precision['min_notional']))
            
            # Check if equal allocation would meet minimum
            equal_budget = total_budget / len(available_exchanges)
            notional = equal_budget * aggressive_price
            
            if notional >= min_notional:
                valid_exchanges.append(exchange)
                self.logger.debug(f"  ✅ {exchange}: ${notional:.2f} >= ${min_notional} (min)")
            else:
                self.logger.debug(f"  ❌ {exchange}: ${notional:.2f} < ${min_notional} (min)")
        
        if valid_exchanges:
            budget_per_exchange = total_budget / len(valid_exchanges)
            
            self.logger.info(f"📊 Equal allocation to {len(valid_exchanges)} exchanges:")
            
            for exchange in valid_exchanges:
                allocations[exchange] = {
                    'size': budget_per_exchange,
                    'price': aggressive_price,
                    'expected_impact': Decimal('0.01'),  # Estimated 1% impact
                    'efficiency': Decimal('1.0')  # Equal efficiency
                }
                self.logger.info(f"  - {exchange}: {budget_per_exchange} {self.base_coin} @ {aggressive_price}")
        
        return allocations
    
    async def _fallback_largest_exchanges_available(
        self, 
        side: OrderSide, 
        total_budget: Decimal, 
        current_price: Decimal,
        exchange_impacts: Dict,
        available_exchanges: List[str]
    ) -> Dict[str, Dict[str, Decimal]]:
        """Fallback to using largest available exchanges that meet minimum requirements."""
        allocations = {}
        
        self.logger.info(f"📊 FALLBACK: Using largest exchanges strategy")
        self.logger.info(f"  - Reason: Initial allocations didn't meet minimum requirements")
        self.logger.info(f"  - Total Budget: {total_budget} {self.base_coin}")
        
        # Filter impacts to only include available exchanges
        available_impacts = {
            exchange: impact 
            for exchange, impact in exchange_impacts.items() 
            if exchange in available_exchanges
        }
        
        if not available_impacts:
            return allocations
        
        # Sort exchanges by available volume (largest first)
        sorted_exchanges = sorted(
            available_impacts.items(),
            key=lambda x: x[1]['available_volume'],
            reverse=True
        )
        
        self.logger.info(f"📊 Exchanges sorted by available volume:")
        for exchange, impact in sorted_exchanges[:5]:  # Show top 5
            self.logger.info(f"  - {exchange}: {impact['available_volume']} volume available")
        
        # Try to allocate to largest exchanges first
        remaining_budget = total_budget
        
        for exchange, impact in sorted_exchanges:
            if remaining_budget <= 0:
                break
                
            precision = self._get_exchange_precision(exchange, self.symbol)
            min_notional = Decimal(str(precision['min_notional']))
            min_size = min_notional / impact['optimal_price']
            
            # Allocate at least minimum size, or remaining budget if smaller
            allocated_size = min(remaining_budget, max(min_size, remaining_budget / 2))
            notional = allocated_size * impact['optimal_price']
            
            if notional >= min_notional:
                allocations[exchange] = {
                    'size': allocated_size,
                    'price': impact['optimal_price'],
                    'expected_impact': impact['expected_impact'],
                    'efficiency': impact['efficiency']
                }
                remaining_budget -= allocated_size
                
                self.logger.info(f"✅ Allocated to {exchange}:")
                self.logger.info(f"    - Size: {allocated_size} {self.base_coin}")
                self.logger.info(f"    - Price: {impact['optimal_price']}")
                self.logger.info(f"    - Notional: ${notional:.2f}")
                self.logger.info(f"    - Remaining Budget: {remaining_budget} {self.base_coin}")
            else:
                self.logger.debug(f"❌ Skipped {exchange}: notional ${notional:.2f} < ${min_notional}")
        
        if allocations:
            total_allocated = sum(alloc['size'] for alloc in allocations.values())
            self.logger.info(f"📊 Largest exchanges allocation summary:")
            self.logger.info(f"  - Exchanges Used: {len(allocations)}")
            self.logger.info(f"  - Total Allocated: {total_allocated} {self.base_coin}")
            self.logger.info(f"  - Remaining Unallocated: {remaining_budget} {self.base_coin}")
        
        return allocations

    def _analyze_price_impact_potential(
        self, 
        orderbook: Dict, 
        side: OrderSide, 
        current_price: Decimal, 
        target_price: Decimal
    ) -> Optional[Dict[str, Decimal]]:
        """Analyze an exchange's orderbook for price impact potential."""
        try:
            # Choose the side of the orderbook we'll be hitting
            if side == OrderSide.BUY:
                # We're buying, so we hit the asks
                orders = orderbook.get('asks', [])
                direction = 1  # Price goes up
            else:
                # We're selling, so we hit the bids
                orders = orderbook.get('bids', [])
                direction = -1  # Price goes down
            
            if not orders:
                return None
            
            # Calculate how much volume is needed to reach target price
            cumulative_volume = Decimal('0')
            cumulative_cost = Decimal('0')
            levels_analyzed = 0
            
            self.logger.debug(f"📊 Analyzing orderbook depth to reach target price {target_price}")
            
            for price_str, volume_str in orders:
                price = Decimal(str(price_str))
                volume = Decimal(str(volume_str))
                
                # Check if this price level gets us to target
                if side == OrderSide.BUY and price >= target_price:
                    # We've reached our target price
                    self.logger.debug(f"  - Reached target at level {levels_analyzed}: {price} >= {target_price}")
                    break
                elif side == OrderSide.SELL and price <= target_price:
                    # We've reached our target price
                    self.logger.debug(f"  - Reached target at level {levels_analyzed}: {price} <= {target_price}")
                    break
                
                cumulative_volume += volume
                cumulative_cost += price * volume
                levels_analyzed += 1
                
                if levels_analyzed <= 5:  # Log first 5 levels for visibility
                    self.logger.debug(f"  - Level {levels_analyzed}: {volume} @ {price} (cumulative: {cumulative_volume})")
            
            if cumulative_volume == 0:
                self.logger.debug("  - No volume found to reach target price")
                return None
            
            # Calculate average price and efficiency metrics
            avg_execution_price = cumulative_cost / cumulative_volume
            price_impact = abs(avg_execution_price - current_price) / current_price
            
            # Calculate efficiency: INVERTED - higher impact = higher efficiency for aggressive strategy
            # We want to favor exchanges where we get MORE price impact
            efficiency = price_impact * cumulative_volume if cumulative_volume > 0 else Decimal('0')
            
            # Alternative efficiency calculations to consider:
            # 1. Pure price impact: efficiency = price_impact
            # 2. Impact per dollar: efficiency = price_impact / (cumulative_cost / cumulative_volume)
            # 3. Weighted by distance to target: efficiency = price_impact * (1 + abs(target_price - avg_execution_price) / target_price)
            
            # Optimal price for aggressive execution (slightly beyond average)
            if side == OrderSide.BUY:
                optimal_price = avg_execution_price * Decimal('1.001')  # 0.1% above avg
            else:
                optimal_price = avg_execution_price * Decimal('0.999')  # 0.1% below avg
            
            self.logger.debug(f"📊 Impact Analysis Results:")
            self.logger.debug(f"  - Levels Analyzed: {levels_analyzed}")
            self.logger.debug(f"  - Total Volume to Target: {cumulative_volume}")
            self.logger.debug(f"  - Average Execution Price: {avg_execution_price}")
            self.logger.debug(f"  - Price Impact: {price_impact*100:.4f}%")
            self.logger.debug(f"  - Efficiency Score: {efficiency:.6f} (higher = more impact)")
            self.logger.debug(f"  - Optimal Order Price: {optimal_price}")
            
            return {
                'expected_impact': price_impact,
                'efficiency': efficiency,
                'optimal_price': optimal_price,
                'available_volume': cumulative_volume
            }
            
        except Exception as e:
            self.logger.debug(f"Error analyzing price impact potential: {e}")
            return None

    async def _fallback_equal_allocation(
        self, 
        side: OrderSide, 
        total_budget: Decimal, 
        current_price: Decimal
    ) -> Dict[str, Dict[str, Decimal]]:
        """Fallback to equal allocation across exchanges."""
        allocations = {}
        
        # Calculate aggressive price
        if side == OrderSide.BUY:
            aggressive_price = current_price * Decimal('1.01')  # 1% above
        else:
            aggressive_price = current_price * Decimal('0.99')  # 1% below
        
        # Equal allocation across valid exchanges
        valid_exchanges = []
        for exchange in self.exchanges:
            precision = self._get_exchange_precision(exchange, self.symbol)
            min_notional = Decimal(str(precision['min_notional']))
            
            # Check if equal allocation would meet minimum
            equal_budget = total_budget / len(self.exchanges)
            if equal_budget * aggressive_price >= min_notional:
                valid_exchanges.append(exchange)
        
        if valid_exchanges:
            budget_per_exchange = total_budget / len(valid_exchanges)
            
            for exchange in valid_exchanges:
                allocations[exchange] = {
                    'size': budget_per_exchange,
                    'price': aggressive_price,
                    'expected_impact': Decimal('0.01'),  # Estimated 1% impact
                    'efficiency': Decimal('1.0')  # Equal efficiency
                }
        
        return allocations

    async def _fallback_largest_exchanges(
        self, 
        side: OrderSide, 
        total_budget: Decimal, 
        current_price: Decimal,
        exchange_impacts: Dict
    ) -> Dict[str, Dict[str, Decimal]]:
        """Fallback to using largest exchanges that meet minimum requirements."""
        allocations = {}
        
        # Sort exchanges by available volume (largest first)
        sorted_exchanges = sorted(
            exchange_impacts.items(),
            key=lambda x: x[1]['available_volume'],
            reverse=True
        )
        
        # Try to allocate to largest exchanges first
        remaining_budget = total_budget
        
        for exchange, impact in sorted_exchanges:
            if remaining_budget <= 0:
                break
                
            precision = self._get_exchange_precision(exchange, self.symbol)
            min_notional = Decimal(str(precision['min_notional']))
            min_size = min_notional / impact['optimal_price']
            
            # Allocate at least minimum size, or remaining budget if smaller
            allocated_size = min(remaining_budget, max(min_size, remaining_budget / 2))
            
            if allocated_size * impact['optimal_price'] >= min_notional:
                allocations[exchange] = {
                    'size': allocated_size,
                    'price': impact['optimal_price'],
                    'expected_impact': impact['expected_impact'],
                    'efficiency': impact['efficiency']
                }
                remaining_budget -= allocated_size
        
        return allocations
    
    async def _place_single_aggressive_order(self, exchange: str, side: OrderSide, amount: Decimal, price: Decimal) -> Optional[str]:
        """Place a single aggressive order on a specific exchange."""
        try:
            connector = self.exchange_connectors.get(exchange)
            if not connector:
                self.logger.error(f"No connector available for {exchange}")
                return None
            
            # Convert symbol for exchange
            exchange_symbol = self._normalize_symbol_for_exchange(self.symbol, exchange)
            if not exchange_symbol:
                self.logger.error(f"Cannot normalize symbol {self.symbol} for {exchange}")
                return None
            
            # Generate client order ID
            timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)
            client_order_id = self._generate_client_order_id(exchange, side.value, timestamp)
            
            # Get exchange-specific parameters
            params = self._get_exchange_specific_params(exchange, OrderType.LIMIT.value, side.value)
            if client_order_id:
                if exchange == 'mexc_spot':
                    params['clientOrderId'] = client_order_id
                elif exchange == 'gateio_spot':
                    params['clientOrderId'] = client_order_id
                else:
                    params['clientOrderId'] = client_order_id
            
            # Validate order size and price
            validated_amount, validated_price = self._validate_order_size(exchange, exchange_symbol, float(amount), float(price))
            
            # Log order details
            notional = validated_amount * validated_price
            self.logger.info(f"Placing validated order on {exchange}: {validated_amount:.2f} {exchange_symbol} @ {validated_price:.4f} (notional: ${notional:.2f})")
            
            # Place order
            self.logger.info(f"Placing {side.value} order on {exchange}: {validated_amount:.2f} {exchange_symbol} @ {validated_price:.4f}")
            self.logger.debug(f"Order details - Original symbol: {self.symbol}, Exchange symbol: {exchange_symbol}, Side: {side.value} -> {side.value}, Params: {params}, Original: {amount:.2f}@{price:.4f} -> Validated: {validated_amount:.2f}@{validated_price:.4f}")
            
            # Place the order
            order_result = await connector.place_order(
                symbol=exchange_symbol,
                side=side.value,
                amount=validated_amount,
                price=validated_price,
                order_type=OrderType.LIMIT.value,
                params=params
            )
            
            if order_result and 'id' in order_result:
                order_id = order_result['id']
                self.logger.info(f"✅ Successfully placed {side.value} order on {exchange}: {validated_amount:.2f} {exchange_symbol} @ {validated_price:.4f} (Order ID: {order_id})")
                
                # **DO NOT UPDATE POSITION HERE**: Only track orders, position updates happen when trades are confirmed
                # Store order info for tracking (but don't count as position yet)
                order_info = {
                    'id': order_id,
                    'instance_id': self.instance_id,  # ✅ CRITICAL: Track which instance placed this order
                    'exchange': exchange,
                    'exchange_symbol': exchange_symbol,
                    'side': side.value,
                    'amount': validated_amount,
                    'price': validated_price,
                    'status': 'open',
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'client_order_id': client_order_id,
                    'order_result': order_result,
                    'filled_amount': Decimal('0'),  # Track actual filled amount
                    'trade_confirmed': False  # Track if trade is confirmed
                }
                self.active_orders[order_id] = order_info
                
                self.logger.debug(f"Order placed but position NOT updated until trade confirmation: {order_id}")
                
                return order_id
            else:
                self.logger.error(f"❌ Failed to place order on {exchange}: Invalid response")
                return None
                
        except Exception as e:
            error_str = str(e).lower()
            
            # Enhanced error handling
            if 'minimum' in error_str and ('notional' in error_str or 'amount' in error_str or 'size' in error_str):
                self.logger.error(f"❌ Amount/size error on {exchange}: {e}")
                self.logger.info(f"Order amount: {amount}, Price: {price}")
                self.logger.error(f"Order details: {amount} {exchange_symbol} @ {price}")
            elif 'symbol' in error_str or 'market' in error_str:
                self.logger.error(f"❌ Symbol error on {exchange}: {e}")
                self.logger.info(f"Used symbol: {exchange_symbol} for {self.symbol}")
            elif 'balance' in error_str or 'insufficient' in error_str:
                self.logger.error(f"❌ Insufficient balance on {exchange}: {e}")
            else:
                self.logger.error(f"❌ Order placement failed on {exchange}: {e}")
            
            return None
    
    async def _signal_strategy_cooldown(self) -> None:
        """Signal other strategies to pause for the cooldown period."""
        try:
            from datetime import datetime, timedelta
            import json
            
            cooldown_key = f"strategy_cooldown:{self.symbol}"
            cooldown_until = datetime.now(timezone.utc) + timedelta(seconds=self.cooldown_period_seconds)
            
            # Create proper JSON data for cooldown signal
            cooldown_data = {
                'strategy': self.instance_id,
                'cooldown_until': cooldown_until.isoformat(),
                'cooldown_seconds': self.cooldown_period_seconds
            }
            
            await self.redis_client.setex(
                cooldown_key, 
                self.cooldown_period_seconds, 
                json.dumps(cooldown_data)
            )
            
            self.logger.info(f"Signaled strategy cooldown for {self.cooldown_period_seconds} seconds until {cooldown_until}")
        except Exception as e:
            self.logger.error(f"Error signaling strategy cooldown: {e}")
    
    async def _publish_interval_target_price(self) -> None:
        """Publish current interval target price to Redis for other strategies."""
        try:
            import json
            
            current_interval = self._get_current_interval()
            if not current_interval:
                self.logger.debug("No current interval to publish")
                return
            
            # Get current price and position
            current_price = await self._get_current_midpoint()
            current_position = await self._get_current_total_position()
            
            # Prepare interval data
            interval_data = {
                'strategy': self.instance_id,
                'symbol': self.symbol,
                'interval_id': current_interval.interval_id,
                'start_time': current_interval.start_time.isoformat(),
                'end_time': current_interval.end_time.isoformat(),
                'target_price': float(current_interval.target_price),
                'target_position': float(current_interval.target_position),
                'current_position': float(current_interval.current_position),
                'remaining_position': float(current_interval.get_remaining_position()),
                'current_price': float(current_price) if current_price else None,
                'actual_position': float(current_position) if current_position else 0,
                'completed': current_interval.completed,
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
            
            # Publish to Redis channel
            channel = f"interval_target_price:{self.symbol}"
            await self.redis_client.publish(channel, json.dumps(interval_data))
            
            # Also set a key with TTL for direct access
            key = f"current_interval_target:{self.symbol}"
            ttl = int((current_interval.end_time - datetime.now(timezone.utc)).total_seconds())
            if ttl > 0:
                await self.redis_client.setex(key, ttl, json.dumps(interval_data))
            
            self.logger.debug(f"Published interval target price: {current_interval.target_price} for interval {current_interval.interval_id}")
            
        except Exception as e:
            self.logger.error(f"Error publishing interval target price: {e}")
    
    def _generate_client_order_id(self, exchange: str, side: str, timestamp: int) -> str:
        """Generate exchange-compatible client order ID."""
        # Check if this is Hyperliquid
        if exchange == 'hyperliquid_perp' or exchange == 'hyperliquid':
            # Hyperliquid requires a 128-bit hex string (32 hex chars after 0x)
            # Include strategy identifier in the hex for later identification
            import hashlib
            import os
            
            # Create strategy prefix - "atwap" in hex (617477617) 
            strategy_hex = "61747761700000"  # "atwap" + padding to 14 chars
            
            # Generate unique part (18 hex chars to make total 32)
            unique_data = f"{timestamp}_{side}_{os.urandom(4).hex()}".encode()
            unique_hash = hashlib.md5(unique_data).hexdigest()[:18]  # Take first 18 chars
            
            # Combine: strategy identifier (14 chars) + unique part (18 chars) = 32 chars
            full_hex = strategy_hex + unique_hash
            
            # Return with 0x prefix as required by Hyperliquid
            return f"0x{full_hex}"
        
        # For other exchanges, use the existing logic
        elif exchange == 'gateio_spot':
            # Gate.io requires max 28 characters, no special rules for characters
            return f"atwap{side[0]}{timestamp}"[:28]
        elif exchange == 'mexc_spot':
            # MEXC requires alphanumeric + dash/underscore, but let's use only alphanumeric + dash
            return f"atwap-{side[0]}-{timestamp}"[:32]
        else:
            # Standard format for other exchanges
            return f"atwap_{side[0]}_{timestamp}"[:32]
    
    async def on_trade_confirmation(self, trade_data: Dict[str, Any]) -> None:
        """
        Callback function to handle confirmed trades from the connection pool.
        This method is called by the WebSocketConnectionPool.
        
        Includes instance ownership verification to prevent cross-instance contamination.
        """
        try:
            # CRITICAL: First call base class method for instance ownership verification
            await super().on_trade_confirmation(trade_data)
            
            # If we get here, the trade belongs to this instance and was processed by base class
            order_id = trade_data.get('order_id')
            if not order_id:
                return
            
            # The base class already removed the order from active_orders if it belonged to this instance
            # So we just need to do aggressive TWAP-specific processing
            trade_amount = Decimal(str(trade_data.get('amount', 0)))
            
            if trade_amount <= 0:
                self.logger.warning(f"Invalid trade amount for order {order_id}: {trade_amount}")
                return
            
            # Aggressive TWAP-specific processing
            # Position is recalculated from database on next iteration, no manual updates needed
            self.logger.info(f"✅ Aggressive TWAP trade confirmed for instance {self.instance_id}: order {order_id}, amount +{trade_amount:.2f} {self.base_coin}")
            
        except Exception as e:
            self.logger.error(f"Error in aggressive TWAP trade confirmation handler: {e}")
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get strategy performance statistics."""
        base_stats = super().get_performance_stats()
        
        # Calculate progress
        total_intervals = len(self.price_intervals)
        completed_intervals = sum(1 for interval in self.price_intervals if interval.completed)
        progress_percent = (completed_intervals / total_intervals * 100) if total_intervals > 0 else 0
        
        # Calculate position usage from interval tracking (synchronous)
        # Real-time position is calculated in strategy loop, we use interval tracking here
        total_position_used = sum(float(interval.current_position) for interval in self.price_intervals)
        position_usage_percent = (total_position_used / float(self.target_position_total) * 100) if self.target_position_total > 0 else 0
        
        aggressive_stats = {
            'orders_placed': self.orders_placed,
            'total_position_used': total_position_used,  # Real-time from database
            'position_usage_percent': float(position_usage_percent),
            'price_moves_achieved': self.price_moves_achieved,
            'intervals_completed': completed_intervals,
            'total_intervals': total_intervals,
            'progress_percent': float(progress_percent),
            'current_interval_index': self.current_interval_index,
            'target_price': float(self.target_price),
            'initial_midpoint': float(self.initial_midpoint) if self.initial_midpoint else None,
            'start_time': self.configured_start_time.isoformat() if self.configured_start_time else None,
            'base_coin': self.base_coin
        }
        
        return {**base_stats, **aggressive_stats}
    
    def get_interval_status(self) -> List[Dict[str, Any]]:
        """Get status of all price intervals."""
        return [
            {
                'interval_id': interval.interval_id,
                'start_time': interval.start_time.isoformat(),
                'end_time': interval.end_time.isoformat(),
                'target_price': float(interval.target_price),
                'target_position': float(interval.target_position),
                'current_position': float(interval.current_position),
                'remaining_position': float(interval.get_remaining_position()),
                'completed': interval.completed,
                'is_current': interval.interval_id == self.current_interval_index
            }
            for interval in self.price_intervals
        ]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert strategy to dictionary representation."""
        base_dict = super().to_dict()
        
        aggressive_dict = {
            'total_time_hours': self.total_time_hours,
            'granularity_value': self.granularity_value,
            'granularity_unit': self.granularity_unit,
            'frequency_seconds': self.frequency_seconds,
            'target_price': float(self.target_price),
            'target_position_total': float(self.target_position_total),
            'position_currency': self.position_currency,
            'cooldown_period_seconds': self.cooldown_period_seconds,
            'base_coin': self.base_coin,
            'start_time': self.configured_start_time.isoformat() if self.configured_start_time else None,
            'intervals': self.get_interval_status(),
            'performance': self.get_performance_stats()
        }
        
        return {**base_dict, **aggressive_dict}

    async def _synchronize_all_trades(self) -> None:
        """Fetch all trades from exchanges concurrently and insert missing ones to database using UTC time."""
        self.logger.info("🔄 Starting concurrent trade synchronization...")
        
        try:
            import asyncio
            
            # Create concurrent tasks for all exchanges
            sync_tasks = []
            for exchange in self.exchanges:
                task = self._sync_exchange_trades(exchange)
                sync_tasks.append(task)
            
            # Execute all trade synchronization tasks concurrently
            self.logger.info(f"🚀 Starting concurrent trade sync for {len(sync_tasks)} exchanges...")
            start_time = asyncio.get_event_loop().time()
            
            results = await asyncio.gather(*sync_tasks, return_exceptions=True)
            
            end_time = asyncio.get_event_loop().time()
            self.logger.info(f"⚡ Concurrent trade sync completed in {end_time - start_time:.2f} seconds")
            
            # Process results
            total_new_trades = 0
            total_existing_trades = 0
            
            for i, result in enumerate(results):
                exchange = self.exchanges[i]
                if isinstance(result, Exception):
                    self.logger.error(f"Trade sync failed for {exchange}: {result}")
                elif result:
                    new_trades, existing_trades = result
                    total_new_trades += new_trades
                    total_existing_trades += existing_trades
                    self.logger.info(f"📊 {exchange}: {new_trades} new trades inserted, {existing_trades} already existed")
            
            self.logger.info(f"🎯 Concurrent trade synchronization complete: {total_new_trades} new trades inserted, {total_existing_trades} already existed across all exchanges")
            
            # Log position summary after sync
            if total_new_trades > 0:
                await self._log_position_summary_after_sync()
                
        except Exception as e:
            self.logger.error(f"Error during trade synchronization: {e}")
            # Don't fail strategy start if sync fails
            self.logger.warning("Continuing strategy start despite sync error...")

    async def _sync_exchange_trades(self, exchange: str) -> tuple[int, int]:
        """Synchronize trades for a single exchange with its own database session."""
        try:
            from database import get_session
            from database.repositories import TradeRepository
            from database.models import Exchange, Trade
            from sqlalchemy import select, and_
            from datetime import datetime, timezone, timedelta
            
            connector = self.exchange_connectors.get(exchange)
            if not connector:
                self.logger.warning(f"No connector found for {exchange}")
                return 0, 0
            
            # Create a separate database session for this exchange
            async for session in get_session():
                trade_repository = TradeRepository(session)
                
                # Get exchange ID from database
                stmt = select(Exchange.id).where(Exchange.name == exchange)
                result = await trade_repository.session.execute(stmt)
                exchange_id = result.scalar_one_or_none()
                
                if not exchange_id:
                    self.logger.warning(f"Exchange {exchange} not found in database, skipping")
                    return 0, 0
                
                self.logger.debug(f"📥 Fetching trades from {exchange}...")
                
                # ---------------------------------------------------------
                # Optimisation: determine the last trade timestamp for this
                # exchange+symbol and request ONLY trades after that moment.
                # ---------------------------------------------------------

                last_trade_time: Optional[datetime] = None
                try:
                    # Look-up most recent trade timestamp directly via SQL
                    stmt_last = (
                        select(Trade.timestamp)
                        .where(
                            and_(
                                Trade.exchange_id == exchange_id,
                                Trade.symbol == self.symbol
                            )
                        )
                        .order_by(Trade.timestamp.desc())
                        .limit(1)
                    )
                    result_last = await trade_repository.session.execute(stmt_last)
                    last_trade_time = result_last.scalar_one_or_none()
                except Exception as e:
                    self.logger.warning(f"Could not query last trade time for {exchange}: {e}")

                if last_trade_time:
                    since_param = last_trade_time + timedelta(milliseconds=1)
                    self.logger.debug(
                        f"Using since={since_param.isoformat()} to minimise REST history load for {exchange}"
                    )
                else:
                    since_param = None  # Full fetch if no previous trade

                # Fetch trades – connector implementations expect a datetime for 'since'
                trades = await connector.get_trade_history(self.symbol, since=since_param)
                if not trades:
                    self.logger.debug(f"No trades found on {exchange}")
                    return 0, 0
                    
                self.logger.debug(f"Found {len(trades)} trades on {exchange}")
                
                new_trades_count = 0
                existing_trades_count = 0
                
                for trade in trades:
                    try:
                        # Convert trade timestamp to UTC datetime (timezone-naive for database)
                        if isinstance(trade.get('timestamp'), (int, float)):
                            # Timestamp in milliseconds - convert to timezone-naive UTC
                            trade_time = datetime.fromtimestamp(
                                trade['timestamp'] / 1000, 
                                tz=timezone.utc
                            ).replace(tzinfo=None)  # Remove timezone info for database
                        elif isinstance(trade.get('datetime'), str):
                            # ISO datetime string - convert to timezone-naive UTC
                            trade_time = datetime.fromisoformat(
                                trade['datetime'].replace('Z', '+00:00')
                            ).astimezone(timezone.utc).replace(tzinfo=None)  # Remove timezone info
                        else:
                            self.logger.warning(f"Invalid timestamp format in trade: {trade}")
                            continue
                    
                        # Check if trade already exists in database
                        existing_trade = await trade_repository.get_trade_by_exchange_id(
                            exchange_id, str(trade['id'])
                        )
                        
                        if existing_trade:
                            existing_trades_count += 1
                            continue
                    
                        # Prepare trade data with timezone-naive UTC timestamp
                        trade_data = trade.copy()
                        trade_data['timestamp'] = trade_time
                        
                        # Insert new trade using repository
                        new_trade = await trade_repository.save_trade(
                            trade_data=trade_data,
                            exchange_id=exchange_id,
                            bot_instance_id=None  # No specific bot instance for historical trades
                        )
                        
                        if new_trade:
                            new_trades_count += 1
                            self.logger.debug(f"✅ Inserted new trade: {exchange} {trade['side']} {trade['amount']} {trade['symbol']} @ {trade['price']} at {trade_time}")
                        
                    except Exception as e:
                        self.logger.error(f"Error processing trade {trade.get('id', 'unknown')} from {exchange}: {e}")
                        # Rollback the session to recover from any database errors
                        await trade_repository.session.rollback()
                        continue
                
                # Commit changes for this exchange
                success = await trade_repository.commit_trades()
                if not success:
                    self.logger.error(f"Failed to commit trades for {exchange}")
                    await trade_repository.session.rollback()
                
                # ---------------------------------------------------------
                # Update sync metadata (TradeSync table) so the next run can
                # start from the correct point in time.
                # ---------------------------------------------------------

                try:
                    from datetime import datetime as _dt
                    last_sync_time_val = _dt.utcnow()
                    last_trade_id_val = trades[-1]['id'] if trades else None
                    await trade_repository.update_trade_sync(
                        exchange_id=exchange_id,
                        symbol=self.symbol,
                        last_sync_time=last_sync_time_val,
                        last_trade_id=last_trade_id_val,
                    )
                except Exception as e:
                    self.logger.warning(f"Unable to update TradeSync metadata for {exchange}: {e}")

                return new_trades_count, existing_trades_count
            
            # If we get here, no session was available
            return 0, 0
            
        except Exception as e:
            self.logger.error(f"Error synchronizing trades for {exchange}: {e}")
            return 0, 0

    async def _log_position_summary_after_sync(self) -> None:
        """Log position summary after trade synchronization."""
        try:
            self.logger.info("📈 Position summary after trade synchronization:")
            
            # Calculate position since strategy start time if configured
            if self.configured_start_time:
                position_since_start = await self._calculate_position_since_start_time()
                if position_since_start is not None:
                    self.logger.info(f"Position since strategy start ({self.configured_start_time}): {position_since_start} {self.base_coin}")
            
            # Calculate total position across all time
            from database import get_session
            from database.repositories import TradeRepository
            
            async for session in get_session():
                trade_repository = TradeRepository(session)
                
                total_position = Decimal('0')
                for exchange in self.exchanges:
                    try:
                        exchange_trades = await trade_repository.get_trades_by_exchange_symbol(
                            exchange, self.symbol
                        )
                        
                        exchange_position = Decimal('0')
                        for trade in exchange_trades:
                            if trade.side.lower() == 'buy':
                                exchange_position += Decimal(str(trade.amount))
                            elif trade.side.lower() == 'sell':
                                exchange_position -= Decimal(str(trade.amount))
                        
                        total_position += exchange_position
                        
                        if exchange_position != 0:
                            self.logger.info(f"  {exchange}: {exchange_position} {self.base_coin}")
                            
                    except Exception as e:
                        self.logger.error(f"Error calculating position for {exchange}: {e}")
                        continue
                
                self.logger.info(f"📊 Total position across all exchanges: {total_position} {self.base_coin}")
                break
            
        except Exception as e:
            self.logger.error(f"Error logging position summary: {e}")

    async def _position_logging_loop(self) -> None:
        """Continuously log the live total position at a short interval."""
        try:
            while self.running:
                # Get the ACTUAL position from database, not internal tracking
                actual_position = await self._get_current_total_position()
                self.logger.info(f"[LIVE] Current total position: {actual_position} {self.base_coin}")
                await asyncio.sleep(self.position_log_interval)
        except asyncio.CancelledError:
            pass
