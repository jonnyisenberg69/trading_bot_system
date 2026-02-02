"""
Aggressive TWAP Strategy V2 - Time-weighted average price with sub-interval targeting

This strategy aims to push a price to a target price by the end of total time.
It breaks the total time into intervals and sub-intervals with intermediate goals
and uses a liquidity-based order routing system.
"""

import asyncio
import time
import json
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Optional, Any, Tuple, Union
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
class Budget:
    """Represents the budget for a sub-interval."""
    initial: Decimal = Decimal('0')
    remaining: Decimal = Decimal('0')
    consumed: Decimal = Decimal('0')
    currency: PositionCurrency = PositionCurrency.BASE

    def add_budget(self, amount: Decimal):
        """Add budget from a previous interval."""
        self.remaining += amount

    def consume_budget(self, amount: Decimal):
        """Consume budget for an order."""
        consumed_amount = min(amount, self.remaining)
        self.remaining -= consumed_amount
        self.consumed += consumed_amount
        return consumed_amount

@dataclass
class SubInterval:
    """Represents a sub-interval within a main PriceInterval."""
    sub_interval_id: int
    parent_interval_id: int
    start_time: datetime
    end_time: datetime
    target_price: Decimal
    completed: bool = False

@dataclass
class PriceInterval:
    """Represents a price interval with target position."""
    interval_id: int
    start_time: datetime
    end_time: datetime
    start_price: Decimal
    target_price: Decimal
    target_position: Decimal

    interval_budget: Budget
    move_budget: Budget  # Separate budget for BPS moves
    bps_move: Decimal    # Basis points move for fallback

    sub_intervals: List[SubInterval] = field(default_factory=list)
    current_position: Decimal = Decimal('0')
    completed: bool = False
    
    def get_remaining_position(self) -> Decimal:
        """Get remaining position for this interval."""
        return max(Decimal('0'), self.target_position - self.current_position)
    
    def add_position(self, amount: Decimal) -> None:
        """Add to current position."""
        self.current_position += amount
        if self.current_position >= self.target_position:
            self.completed = True


class AggressiveTwapV2Strategy(BaseStrategy):
    """
    Aggressive TWAP V2 strategy for pushing prices to targets.
    
    Features:
    - Time-based intervals and sub-intervals with price targets and budgets
    - Liquidity-based order routing
    - Coordination with other strategies (cooldown mechanism)
    - Dynamic budget reallocation based on performance
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
        self.current_sub_interval_index: int = 0
        self.last_order_time: Optional[datetime] = None
        self.initial_midpoint: Optional[Decimal] = None
        self._last_known_price: Optional[Decimal] = None  # Track current price for interval selection
        self.position_log_interval = int(config.get('position_log_interval', 5))  # seconds
        self._position_logger_task: Optional[asyncio.Task] = None
        self.intervals_config: List[Dict[str, Any]] = []
        self.interval_duration: Optional[timedelta] = None
        
        # Execution components
        self.redis_orderbook_manager = RedisOrderbookManager(redis_url)
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
        self.price_moves_achieved = 0
        
        # Strategy-specific attributes
        self.cooldown_period_seconds = int(self.config['cooldown_period_seconds'])
        self.base_coin = self.config.get('base_coin', "")
        
        self.intervals_config = self.config.get('intervals', [])
        if not self.intervals_config or not isinstance(self.intervals_config, list):
            raise ValueError("Intervals configuration must be a non-empty list")
        
        if self.granularity_unit == GranularityUnit.SECONDS:
            self.interval_duration = timedelta(seconds=self.granularity_value)
        elif self.granularity_unit == GranularityUnit.MINUTES:
            self.interval_duration = timedelta(minutes=self.granularity_value)
        else:
            self.interval_duration = timedelta(hours=self.granularity_value)

        start_time_str = self.config.get('start_time')
        self.configured_start_time = self._parse_start_time(start_time_str)

        # Interval generation has been moved to _start_strategy to ensure all components are initialized.
        
    async def _validate_config(self) -> None:
        """Validate aggressive TWAP V2 configuration."""
        self.logger.info(f"Raw config received: {json.dumps(self.config, indent=2)}")
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
        
        self.total_time_hours = float(self.config['total_time_hours'])
        self.granularity_value = int(self.config['granularity_value'])
        self.granularity_unit = GranularityUnit(self.config['granularity_unit'])
        self.frequency_seconds = int(self.config['frequency_seconds'])
        self.target_price = Decimal(str(self.config['target_price']))
        self.target_position_total = Decimal(str(self.config['target_position_total']))
        self.position_currency = PositionCurrency(self.config['position_currency'])
        self.cooldown_period_seconds = int(self.config['cooldown_period_seconds'])
        self.base_coin = self.config.get('base_coin', "")
        
        self.intervals_config = self.config.get('intervals', [])
        if not self.intervals_config or not isinstance(self.intervals_config, list):
            raise ValueError("Intervals configuration must be a non-empty list")
        
        if self.granularity_unit == GranularityUnit.SECONDS:
            self.interval_duration = timedelta(seconds=self.granularity_value)
        elif self.granularity_unit == GranularityUnit.MINUTES:
            self.interval_duration = timedelta(minutes=self.granularity_value)
        else:
            self.interval_duration = timedelta(hours=self.granularity_value)

        start_time_str = self.config.get('start_time')
        self.configured_start_time = self._parse_start_time(start_time_str)

        # Interval generation has been moved to _start_strategy to ensure all components are initialized.
        
    def _parse_start_time(self, start_time_str: Optional[Union[str, datetime]]) -> datetime:
        """Parse start_time from config, defaulting to now if invalid."""
        if start_time_str:
            try:
                if isinstance(start_time_str, str):
                    if 'T' in start_time_str and start_time_str.endswith('Z'):
                        return datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))
                    elif 'T' in start_time_str and '+' in start_time_str:
                        return datetime.fromisoformat(start_time_str)
                    else:
                        # Handles 'YYYY-MM-DDTHH:MM' and 'YYYY-MM-DD HH:MM'
                        return datetime.fromisoformat(start_time_str.replace(' ', 'T')).replace(tzinfo=timezone.utc)
                elif isinstance(start_time_str, datetime):
                    return start_time_str.astimezone(timezone.utc) if start_time_str.tzinfo else start_time_str.replace(tzinfo=timezone.utc)
            except (ValueError, TypeError) as e:
                self.logger.error(f"Could not parse start_time '{start_time_str}': {e}. Defaulting to now.")
        
        self.logger.warning("No valid start_time provided, using current UTC time.")
        return datetime.now(timezone.utc)

    async def _generate_intervals_and_sub_intervals(self, intervals_config: List[Dict], interval_duration: timedelta):
        """Generate main intervals and their sub-intervals."""
        self.price_intervals = []
        
        # Wait for initial midpoint price to be set
        await asyncio.sleep(2) # Allow connectors to initialize
        await self._set_initial_midpoint()
        if self.initial_midpoint is None:
             raise ValueError("Could not determine initial midpoint price. Cannot generate intervals.")

        last_price = self.initial_midpoint
        start_time = self.configured_start_time

        for i, interval_config in enumerate(intervals_config):
            if 'target_price' not in interval_config or 'target_position' not in interval_config:
                raise ValueError(f"Interval {i}: Missing target_price or target_position")

            interval_start = start_time + (interval_duration * i)
            interval_end = interval_start + interval_duration
            target_price = Decimal(str(interval_config['target_price']))
            target_position = Decimal(str(interval_config['target_position']))
            
            # Parse BPS move and move budget from config
            self.logger.debug(f"Interval {i} config: {interval_config}")
            bps_move = Decimal(str(interval_config.get('bps_move', '0')))
            move_position = Decimal(str(interval_config.get('move_position', '0')))
            self.logger.debug(f"Parsed BPS move: {bps_move}, Move position: {move_position}")
            
            # Create interval budget
            interval_budget = Budget(
                initial=target_position,
                remaining=target_position,
                currency=self.position_currency
            )
            
            # Create move budget (separate from interval budget)
            move_budget = Budget(
                initial=move_position,
                remaining=move_position,
                currency=self.position_currency
            )

            price_interval = PriceInterval(
                interval_id=i,
                start_time=interval_start,
                end_time=interval_end,
                start_price=last_price,
                target_price=target_price,
                target_position=target_position,
                interval_budget=interval_budget,
                move_budget=move_budget,
                bps_move=bps_move
            )

            price_interval.sub_intervals = self._generate_sub_intervals(price_interval)
            self.price_intervals.append(price_interval)
            last_price = target_price
            
            self.logger.info(f"Generated Interval {i}: {interval_start} to {interval_end} UTC, "
                           f"target {target_position} {self.base_coin} @ {target_price}, "
                           f"BPS move: {bps_move}, Move budget: {move_position} {self.base_coin}, "
                           f"with {len(price_interval.sub_intervals)} sub-intervals.")

        total_interval_position = sum(p.target_position for p in self.price_intervals)
        if total_interval_position > self.target_position_total:
            raise ValueError(f"Total interval positions ({total_interval_position}) exceed total limit ({self.target_position_total})")
        
        # Log which intervals are viable based on current price
        viable_count = 0
        for interval in self.price_intervals:
            if interval.target_price > self.initial_midpoint:
                viable_count += 1
        
        self.logger.info(f"📊 Interval Summary: {viable_count}/{len(self.price_intervals)} intervals have targets above current price ({self.initial_midpoint})")

    def _generate_sub_intervals(self, interval: PriceInterval) -> List[SubInterval]:
        """Generate sub-intervals for a given PriceInterval."""
        sub_intervals = []
        sub_interval_duration = timedelta(seconds=self.frequency_seconds)
        num_sub_intervals = (interval.end_time - interval.start_time) // sub_interval_duration
        
        if num_sub_intervals == 0:
            self.logger.warning(f"Interval {interval.interval_id} duration is less than frequency. Creating one sub-interval.")
            num_sub_intervals = 1

        price_step = (interval.target_price - interval.start_price) / num_sub_intervals

        for i in range(num_sub_intervals):
            sub_start_time = interval.start_time + (sub_interval_duration * i)
            sub_end_time = sub_start_time + sub_interval_duration
            sub_target_price = interval.start_price + (price_step * (i + 1))
            
            # Note: No budget allocation here - sub-intervals will use the full interval budget
            sub_interval = SubInterval(
                sub_interval_id=i,
                parent_interval_id=interval.interval_id,
                start_time=sub_start_time,
                end_time=sub_end_time,
                target_price=sub_target_price,
                completed=False
            )
            sub_intervals.append(sub_interval)
            
        return sub_intervals

    async def _start_strategy(self) -> None:
        """Start the aggressive TWAP V2 strategy."""
        self.logger.info("Starting aggressive TWAP V2 strategy")
        
        # Initialize Redis connection
        self.redis_client = redis.from_url(self.redis_url)
        
        # Start Redis orderbook manager
        await self.redis_orderbook_manager.start()
        
        # Initialize execution components
        await self._initialize_execution_components()
        
        # Generate intervals now that market data sources are available
        await self._generate_intervals_and_sub_intervals(self.intervals_config, self.interval_duration)
        
        # Find which interval and sub-interval we'll start with
        initial_interval = self._get_current_interval()
        if initial_interval:
            self.logger.info(f"🎯 Starting with Interval {initial_interval.interval_id} (target: {initial_interval.target_price}) based on current price: {self._last_known_price}")
            if initial_interval.interval_id > 0:
                self.logger.warning(f"⚠️ Skipping first {initial_interval.interval_id} intervals as their targets are below current price")
            
            # Also log which sub-interval we'll start with
            initial_sub = self._get_current_sub_interval()
            if initial_sub:
                self.logger.info(f"📍 Starting with Sub-interval {initial_sub.sub_interval_id} (target: {initial_sub.target_price})")
        
        # Publish initial sub-interval target price
        await self._publish_sub_interval_target_price()
        
        # Start main strategy loop
        self.main_task = asyncio.create_task(self._strategy_loop())
        
        # Start periodic position logger
        self._position_logger_task = asyncio.create_task(self._position_logging_loop())
        
    async def _stop_strategy(self) -> None:
        """Stop the aggressive TWAP V2 strategy."""
        self.logger.info("Stopping aggressive TWAP V2 strategy")
        
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
        """Set the initial midpoint price for reference, using the robust V1 logic."""
        try:
            # Correctly use RedisOrderbookManager as the primary source of truth for orderbook data
            midpoints = []
            for exchange in self.exchanges:
                try:
                    midpoint = self.redis_orderbook_manager.get_midpoint(exchange, self.symbol)
                    if midpoint:
                        midpoints.append(midpoint)
                except Exception as e:
                    self.logger.debug(f"Could not get midpoint from {exchange} via Redis: {e}")
                    continue
            
            if midpoints:
                self.initial_midpoint = sum(midpoints) / len(midpoints)
                self._last_known_price = self.initial_midpoint  # Store for interval selection
                self.logger.info(f"Set initial midpoint from Redis: {self.initial_midpoint}")
                return

            # Fallback: get midpoints directly from exchanges concurrently
            self.logger.warning("Redis orderbook manager has no data, trying concurrent direct exchange fallback...")
            
            initial_price_tasks = []
            for exchange in self.exchanges:
                connector = self.exchange_connectors.get(exchange)
                if connector:
                    task = self._get_initial_exchange_midpoint(exchange, connector)
                    initial_price_tasks.append((exchange, task))
            
            if initial_price_tasks:
                results = await asyncio.gather(*[task for _, task in initial_price_tasks], return_exceptions=True)
                
                direct_midpoints = []
                for i, result in enumerate(results):
                    exchange = initial_price_tasks[i][0]
                    if isinstance(result, Exception):
                        self.logger.warning(f"Could not get initial midpoint from {exchange}: {result}")
                    elif isinstance(result, Decimal):
                        direct_midpoints.append(result)
                    else:
                        self.logger.warning(f"Could not get initial midpoint from {exchange}: received None or invalid data")
                
                if direct_midpoints:
                    self.initial_midpoint = sum(direct_midpoints) / len(direct_midpoints)
                    self._last_known_price = self.initial_midpoint  # Store for interval selection
                    self.logger.info(f"✅ Set initial midpoint from direct exchange fallback: {self.initial_midpoint}")
                    return

            self.logger.error("❌ Could not get initial midpoint from any source!")
            self.initial_midpoint = None
        except Exception as e:
            self.logger.error(f"Error setting initial midpoint: {e}", exc_info=True)
            self.initial_midpoint = None
    
    async def _strategy_loop(self) -> None:
        """Main strategy loop for V2, operating on sub-intervals."""
        self.logger.info("Starting aggressive TWAP V2 strategy loop")
        
        while self.running:
            try:
                if not await self._should_be_active():
                    await asyncio.sleep(self.frequency_seconds)
                    continue

                # V2 logic: Get the current SUB-interval
                sub_interval = self._get_current_sub_interval()
                if not sub_interval:
                    self.logger.info("All sub-intervals completed or strategy time expired.")
                    break

                # V2 logic: Get current price
                current_price = await self._get_current_midpoint()
                if current_price is None:
                    self.logger.warning("Could not get current price, waiting...")
                    await asyncio.sleep(self.frequency_seconds)
                    continue
                
                # V2 logic: Get current position before executing logic
                current_position = await self._get_current_total_position()
                
                # V2 logic: Execute the logic for the SUB-interval
                self.logger.debug(f"Executing logic for sub-interval {sub_interval.sub_interval_id} - "
                                f"Current: {current_price}, Target: {sub_interval.target_price}, Position: {current_position}")
                await self._execute_sub_interval_logic(sub_interval, current_price, current_position)
                
                await asyncio.sleep(self.frequency_seconds)
                
            except asyncio.CancelledError:
                self.logger.info("Strategy loop cancelled")
                break
            except Exception as e:
                self.logger.error(f"Error in V2 strategy loop: {e}", exc_info=True)
                await asyncio.sleep(5)
    
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
        """Get the current active main interval based on price, not just time."""
        # First, check if we have a current price
        if not hasattr(self, '_last_known_price') or self._last_known_price is None:
            self.logger.warning("No current price available for interval selection")
            return None
        
        current_price = self._last_known_price
        
        # Find the first interval that:
        # 1. Has a target price above the current price
        # 2. Is not completed
        # 3. Has started (time-wise)
        now = datetime.now(timezone.utc)
        
        for i, interval in enumerate(self.price_intervals):
            # Skip completed intervals
            if interval.completed:
                continue
                
            # Check if interval has started time-wise
            start_time = interval.start_time.replace(tzinfo=timezone.utc) if interval.start_time.tzinfo is None else interval.start_time
            if now < start_time:
                self.logger.debug(f"Interval {i} hasn't started yet (starts at {start_time})")
                continue
            
            # This is the key change: select based on price target
            if interval.target_price > current_price:
                self.logger.info(f"Selected interval {i} - Current price: {current_price}, Target: {interval.target_price}")
                self.current_interval_index = i
                return interval
        
        # If all intervals have targets below current price, we might be done
        self.logger.warning(f"No suitable interval found. Current price {current_price} may have exceeded all targets")
        return None

    def _get_current_sub_interval(self) -> Optional[SubInterval]:
        """Get the current active sub-interval based on price, not just time."""
        main_interval = self._get_current_interval()
        if not main_interval:
            return None

        # Get current price
        if not hasattr(self, '_last_known_price') or self._last_known_price is None:
            self.logger.warning("No current price available for sub-interval selection")
            return None
        
        current_price = self._last_known_price
        now = datetime.now(timezone.utc)
        
        # PRICE-BASED SELECTION: Find the first uncompleted sub-interval whose target is above current price
        for i, sub_interval in enumerate(main_interval.sub_intervals):
            if sub_interval.completed:
                continue
            
            # Check if this sub-interval's target is above current price
            if sub_interval.target_price > current_price:
                # This is the sub-interval we should be working on
                self.current_sub_interval_index = i
                
                # Log if we're behind schedule time-wise
                end_time = sub_interval.end_time.replace(tzinfo=timezone.utc) if sub_interval.end_time.tzinfo is None else sub_interval.end_time
                if now >= end_time:
                    time_overdue = now - end_time
                    self.logger.warning(f"Sub-interval {i} is {time_overdue} overdue but still targeting price {sub_interval.target_price} (current: {current_price})")
                
                return sub_interval
        
        # If all sub-intervals have targets below current price, the interval might be done
        self.logger.warning(f"All sub-intervals in interval {main_interval.interval_id} have targets below current price {current_price}")
        return None
    
    async def _get_current_midpoint(self) -> Optional[Decimal]:
        """Get current midpoint price, using the robust V1 logic."""
        try:
            # Primary: use RedisOrderbookManager to get per-exchange midpoints
            midpoints = []
            for exchange in self.exchanges:
                try:
                    midpoint = self.redis_orderbook_manager.get_midpoint(exchange, self.symbol)
                    if midpoint:
                        midpoints.append(midpoint)
                except Exception as e:
                    self.logger.debug(f"Could not get midpoint from {exchange} via Redis: {e}")
            
            if midpoints:
                avg_price = sum(midpoints) / len(midpoints)
                self.logger.debug(f"Calculated average midpoint from Redis: {avg_price}")
                self._last_known_price = avg_price  # Store for interval selection
                return avg_price
            
            # Fallback: Direct concurrent exchange access
            self.logger.warning("Redis has no data, trying direct concurrent exchange access...")
            price_tasks = []
            for exchange in self.exchanges:
                connector = self.exchange_connectors.get(exchange)
                if connector:
                    task = self._get_exchange_midpoint(exchange, connector)
                    price_tasks.append(task)
            
            if price_tasks:
                results = await asyncio.gather(*price_tasks, return_exceptions=True)
                direct_midpoints = [res for res in results if isinstance(res, Decimal)]
                if direct_midpoints:
                    avg_direct_price = sum(direct_midpoints) / len(direct_midpoints)
                    self.logger.info(f"✅ Got price from direct exchange fallback: {avg_direct_price}")
                    self._last_known_price = avg_direct_price  # Store for interval selection
                    return avg_direct_price
            
            self.logger.error("❌ No real-time price data available from any source!")
            return None
        except Exception as e:
            self.logger.error(f"Error getting current midpoint: {e}", exc_info=True)
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
            exchange_symbol = self._normalize_symbol_for_exchange(self.symbol, exchange)
            self.logger.debug(f"Fetching initial orderbook for {exchange_symbol} on {exchange}")
            orderbook = await connector.fetch_order_book(exchange_symbol, limit=5)

            if not orderbook or not orderbook.get('bids') or not orderbook.get('asks'):
                self.logger.warning(f"No valid orderbook data for {exchange_symbol} on {exchange}. Response: {orderbook}")
                return None

            best_bid = Decimal(str(orderbook['bids'][0][0]))
            best_ask = Decimal(str(orderbook['asks'][0][0]))
            midpoint = (best_bid + best_ask) / 2
            self.logger.info(f"Got initial midpoint {midpoint} for {exchange_symbol} on {exchange}")
            return midpoint
        except Exception as e:
            self.logger.warning(f"Failed to fetch initial orderbook for {exchange_symbol} on {exchange}: {e}")
            raise e

    async def _get_exchange_orderbook(self, exchange: str) -> Optional[Dict]:
        """Get orderbook data from a single exchange, with fallback to direct API call."""
        try:
            # Primary: use Redis for speed
            if hasattr(self.redis_orderbook_manager, 'get_orderbook'):
                orderbook = self.redis_orderbook_manager.get_orderbook(exchange, self.symbol)
                if orderbook and orderbook.get('bids') and orderbook.get('asks') and orderbook['bids'] and orderbook['asks']:
                    self.logger.debug(f"Got orderbook for {exchange} from Redis.")
                    return orderbook
            
            # Fallback: direct exchange call if Redis fails or has no data
            self.logger.warning(f"No valid orderbook in Redis for {exchange}, falling back to direct API call.")
            connector = self.exchange_connectors.get(exchange)
            if connector:
                exchange_symbol = self._normalize_symbol_for_exchange(self.symbol, exchange)
                direct_orderbook = await connector.get_orderbook(exchange_symbol, limit=20)
                if direct_orderbook and direct_orderbook.get('bids') and direct_orderbook.get('asks'):
                    self.logger.info(f"Successfully fetched orderbook for {exchange} directly.")
                    return direct_orderbook
            
            self.logger.error(f"Failed to get orderbook for {exchange} from both Redis and direct API call.")
            return None

        except Exception as e:
            self.logger.error(f"Error getting exchange orderbook for {exchange}: {e}", exc_info=True)
            return None
    
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
    
    async def _execute_sub_interval_logic(self, sub_interval: SubInterval, current_price: Decimal, current_position: Decimal) -> None:
        """The core V2 state machine."""
        main_interval = self.price_intervals[sub_interval.parent_interval_id]

        # 1. Check for price reversion (fallback)
        if await self._handle_price_reversion(current_price):
            # Price reverted, and logic was handled. Restart logic with new sub-interval.
            return

        # 2. Check if sub-interval target price is reached (advance)
        if self.is_price_reached(current_price, sub_interval.target_price):
            await self._handle_sub_interval_advance(main_interval, sub_interval)
            return

        # 3. With price-based selection, we don't time out sub-intervals
        # They remain active until their price target is reached
        # This prevents the infinite loop issue
            
        await self._place_liquidity_based_order(sub_interval, current_position)
    
    async def _handle_price_reversion(self, current_price: Decimal) -> bool:
        """
        Handle market price falling back to a previous sub-interval's range.
        With price-based selection, this is now simplified.
        """
        # With price-based sub-interval selection, we don't need complex reversion logic
        # The _get_current_sub_interval will automatically select the right sub-interval
        # based on current price, so reversion is handled implicitly
        return False

    async def _handle_sub_interval_advance(self, main_interval: PriceInterval, current_sub: SubInterval):
        """Handle advancing to the next sub-interval after reaching a price target."""
        current_sub.completed = True
        self.price_moves_achieved += 1
        
        next_sub_index = self.current_sub_interval_index + 1
        if next_sub_index < len(main_interval.sub_intervals):
            # Simply advance to next sub-interval
            self.logger.info(f"Advanced to sub-interval {next_sub_index}")
            self.current_sub_interval_index = next_sub_index
        else:
            # This was the last sub-interval of the main interval
            self.logger.info(f"Main interval {main_interval.interval_id} completed.")
            main_interval.completed = True
            # The loop will naturally move to the next main interval
        
        await self._publish_sub_interval_target_price()

    async def _handle_sub_interval_timeout(self, main_interval: PriceInterval, current_sub: SubInterval):
        """Handle a sub-interval timing out before its price target is met."""
        # Mark as completed to prevent re-processing, but it wasn't successful
        current_sub.completed = True
        
        next_sub_index = self.current_sub_interval_index + 1
        if next_sub_index < len(main_interval.sub_intervals):
            # Simply move to next sub-interval
            self.logger.info(f"Timeout! Moving to sub-interval {next_sub_index}")
            self.current_sub_interval_index = next_sub_index
        else:
            # Last sub-interval of the main interval timed out
            self.logger.warning(f"Main interval {main_interval.interval_id} finished without reaching final target.")
            main_interval.completed = True
        
        await self._publish_sub_interval_target_price()

    async def _place_liquidity_based_order(self, sub_interval: SubInterval, current_position: Decimal) -> None:
        """Place orders based on available liquidity up to the target price."""
        try:
            main_interval = self.price_intervals[sub_interval.parent_interval_id]
            
            # Log current sub-interval and interval budgets
            self.logger.info(
                f"Sub-interval {sub_interval.parent_interval_id}-{sub_interval.sub_interval_id}: "
                f"Target Price: {sub_interval.target_price}, "
                f"Interval Budget: {main_interval.interval_budget.remaining:.4f}, "
                f"Move Budget: {main_interval.move_budget.remaining:.4f}"
            )

            current_price = await self._get_current_midpoint()
            if not current_price:
                self.logger.warning("Cannot place order without current price.")
                return

            side = OrderSide.BUY  # V2 is a buy-side strategy to push price up
            
            # First attempt: Try to reach sub-interval target price using interval budget
            self.logger.info(f"🚀 ATTEMPTING SUB-INTERVAL TARGET PRICE MOVE:")
            self.logger.info(f"  - Current Price: {current_price}")
            self.logger.info(f"  - Sub-Interval Target Price: {sub_interval.target_price}")
            
            success = await self._attempt_price_move(
                target_price=sub_interval.target_price,
                budget=main_interval.interval_budget,
                side=side,
                current_position=current_position,
                description="sub-interval target"
            )
            
            if success:
                return
            
            # Second attempt: Try BPS move if we have move budget
            if main_interval.bps_move > 0 and main_interval.move_budget.remaining > 0:
                # Calculate BPS target price
                bps_target_price = current_price * (1 + main_interval.bps_move / 10000)  # bps_move is in basis points
                
                self.logger.info(f"🎯 ATTEMPTING BPS MOVE:")
                self.logger.info(f"  - Current Price: {current_price}")
                self.logger.info(f"  - BPS Move: {main_interval.bps_move} bps")
                self.logger.info(f"  - BPS Target Price: {bps_target_price}")
                
                success = await self._attempt_price_move(
                    target_price=bps_target_price,
                    budget=main_interval.move_budget,
                    side=side,
                    current_position=current_position,
                    description="BPS move"
                )
                
                if success:
                    return
            
            # If neither worked, skip this round
            self.logger.info("⏭️ Skipping this round - neither sub-interval target nor BPS move achievable")
            
        except Exception as e:
            self.logger.error(f"Error placing liquidity-based order: {e}", exc_info=True)
    
    async def _attempt_price_move(
        self, 
        target_price: Decimal, 
        budget: Budget, 
        side: OrderSide,
        current_position: Decimal,
        description: str
    ) -> bool:
        """
        Attempt to move price to target using specified budget.
        
        Returns True if orders were placed, False if insufficient liquidity/budget.
        """
        price_key = 'asks' if side == OrderSide.BUY else 'bids'
        
        # Get allocations based on liquidity to reach the target price
        exchange_allocations = await self._calculate_liquidity_based_allocation(target_price, side)
        if not exchange_allocations:
            self.logger.warning(f"No valid exchange allocations for {description}")
            return False

        # Check if total required liquidity exceeds budget
        if self.position_currency == PositionCurrency.BASE:
            total_required_unit = sum(alloc['size'] for alloc in exchange_allocations.values())
            budget_unit_name = self.base_coin
        else:  # PositionCurrency.USD
            total_required_unit = sum(alloc['cost'] for alloc in exchange_allocations.values())
            budget_unit_name = "USD"
        
        if total_required_unit > budget.remaining:
            self.logger.warning(
                f"{description} requires {total_required_unit:.4f} {budget_unit_name} "
                f"but budget is {budget.remaining:.4f}. Cannot execute."
            )
            return False

        # Execute orders for the calculated volumes
        order_tasks = []
        for exchange, allocation in exchange_allocations.items():
            size_base = allocation['size']
            cost_quote = allocation['cost']
            
            # Safety check against total position limit (in base currency)
            order_size_base = size_base
            potential_position = current_position + order_size_base
            if potential_position > self.target_position_total:
                order_size_base = max(Decimal('0'), self.target_position_total - current_position)
            
            if order_size_base <= 0:
                self.logger.info(f"Order size for {exchange} is zero after position limit check.")
                continue
            
            # Determine final order size based on budget currency
            final_order_size_base = Decimal('0')
            if self.position_currency == PositionCurrency.BASE:
                final_order_size_base = budget.consume_budget(order_size_base)
            else:  # PositionCurrency.USD
                # Recalculate cost if size was capped by position limit
                avg_price = cost_quote / size_base if size_base > 0 else target_price
                order_cost_quote = order_size_base * avg_price
                
                consumed_budget_quote = budget.consume_budget(order_cost_quote)
                
                if avg_price > 0:
                    final_order_size_base = consumed_budget_quote / avg_price

            if final_order_size_base > 0:
                self.logger.info(f"Placing {description} order on {exchange}: "
                               f"size={final_order_size_base:.4f} at {target_price}")
                task = self._place_single_aggressive_order(exchange, side, final_order_size_base, target_price)
                order_tasks.append(task)
            else:
                self.logger.warning(f"Budget depleted for {exchange}. Cannot place order.")
        
        if order_tasks:
            self.logger.info(f"⚡ EXECUTING {len(order_tasks)} ORDERS FOR {description.upper()}...")
            await asyncio.gather(*order_tasks, return_exceptions=True)
            await self._signal_strategy_cooldown()
            await self._publish_sub_interval_target_price()
            return True
        else:
            self.logger.info(f"No orders to execute for {description}")
            return False

    async def _calculate_liquidity_based_allocation(self, target_price: Decimal, side: OrderSide) -> Dict[str, Dict[str, Decimal]]:
        """Calculate order size for each exchange based on liquidity up to the target price."""
        allocations = {}
        orderbook_side = 'asks' if side == OrderSide.BUY else 'bids'

        for exchange in self.exchanges:
            try:
                orderbook = await self._get_exchange_orderbook(exchange)
                if not orderbook or not orderbook.get(orderbook_side):
                    self.logger.warning(f"No orderbook data for {exchange}.")
                    continue

                volume_to_target, cost_to_target = self._calculate_liquidity_to_price(
                    orderbook[orderbook_side], 
                    target_price, 
                    side
                )

                if volume_to_target > 0:
                    allocations[exchange] = {
                        'size': volume_to_target,
                        'cost': cost_to_target,
                        'price': target_price  # Place order at the target price
                    }
                    self.logger.info(f"Liquidity analysis for {exchange}: "
                                   f"Volume to reach {target_price} is {volume_to_target:.4f}, costing {cost_to_target:.2f} USD")
                else:
                    self.logger.info(f"No liquidity found for {exchange} up to target price {target_price}.")

            except Exception as e:
                self.logger.error(f"Error calculating allocation for {exchange}: {e}")
        
        return allocations

    def _calculate_liquidity_to_price(self, orderbook_levels: List[List[str]], target_price: Decimal, side: OrderSide) -> Tuple[Decimal, Decimal]:
        """Sums the volume and cost of all order book levels up to the target price."""
        total_volume = Decimal('0')
        total_cost = Decimal('0')
        for price_str, volume_str in orderbook_levels:
            price = Decimal(price_str)
            volume = Decimal(volume_str)
            
            if side == OrderSide.BUY:
                if price <= target_price:
                    total_volume += volume
                    total_cost += volume * price
                else:
                    break # Orderbook is sorted, no need to continue
            else: # SELL
                if price >= target_price:
                    total_volume += volume
                    total_cost += volume * price
                else:
                    break
        return total_volume, total_cost
    
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
    
    async def _publish_sub_interval_target_price(self) -> None:
        """Publish current sub-interval target price to Redis for other strategies."""
        try:
            import json
            
            current_sub_interval = self._get_current_sub_interval()
            if not current_sub_interval:
                self.logger.debug("No current sub-interval to publish")
                return
            
            current_price = await self._get_current_midpoint()
            current_position = await self._get_current_total_position()
            
            # Get main interval for budget info
            main_interval = self.price_intervals[current_sub_interval.parent_interval_id]
            
            interval_data = {
                'strategy': self.instance_id,
                'symbol': self.symbol,
                'interval_id': current_sub_interval.parent_interval_id,
                'sub_interval_id': current_sub_interval.sub_interval_id,
                'start_time': current_sub_interval.start_time.isoformat(),
                'end_time': current_sub_interval.end_time.isoformat(),
                'target_price': float(current_sub_interval.target_price),
                'interval_budget_initial': float(main_interval.interval_budget.initial),
                'interval_budget_remaining': float(main_interval.interval_budget.remaining),
                'move_budget_initial': float(main_interval.move_budget.initial),
                'move_budget_remaining': float(main_interval.move_budget.remaining),
                'bps_move': float(main_interval.bps_move),
                'current_price': float(current_price) if current_price else None,
                'actual_position': float(current_position) if current_position else 0,
                'completed': current_sub_interval.completed,
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
            
            channel = f"interval_target_price:{self.symbol}"
            await self.redis_client.publish(channel, json.dumps(interval_data))
            
            key = f"current_interval_target:{self.symbol}"
            ttl = int((current_sub_interval.end_time - datetime.now(timezone.utc)).total_seconds())
            if ttl > 0:
                await self.redis_client.setex(key, ttl, json.dumps(interval_data))
            
            self.logger.debug(f"Published sub-interval target price: {current_sub_interval.target_price} for sub-interval {current_sub_interval.sub_interval_id}")
            
        except Exception as e:
            self.logger.error(f"Error publishing sub-interval target price: {e}")
    
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
            # So we just need to do aggressive TWAP v2-specific processing
            trade_amount = Decimal(str(trade_data.get('amount', 0)))
            
            if trade_amount <= 0:
                self.logger.warning(f"Invalid trade amount for order {order_id}: {trade_amount}")
                return
            
            # Aggressive TWAP v2-specific processing
            # Position is recalculated from database on next iteration, no manual updates needed
            self.logger.info(f"✅ Aggressive TWAP v2 trade confirmed for instance {self.instance_id}: order {order_id}, amount +{trade_amount:.2f} {self.base_coin}")
            
        except Exception as e:
            self.logger.error(f"Error in aggressive TWAP v2 trade confirmation handler: {e}")
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get strategy performance statistics."""
        base_stats = super().get_performance_stats()
        
        current_sub = self._get_current_sub_interval()
        if current_sub:
            main_interval = self.price_intervals[current_sub.parent_interval_id]
            total_sub_intervals = len(main_interval.sub_intervals)
            completed_sub_intervals = sum(1 for si in main_interval.sub_intervals if si.completed)
            progress_percent = (completed_sub_intervals / total_sub_intervals * 100) if total_sub_intervals > 0 else 0
        else:
            progress_percent = 0

        aggressive_stats = {
            'orders_placed': self.orders_placed,
            'price_moves_achieved': self.price_moves_achieved,
            'current_interval_index': self.current_interval_index,
            'current_sub_interval_index': self.current_sub_interval_index,
            'progress_percent': float(progress_percent),
            'target_price': float(self.target_price),
            'initial_midpoint': float(self.initial_midpoint) if self.initial_midpoint else None,
            'start_time': self.configured_start_time.isoformat() if self.configured_start_time else None,
            'base_coin': self.base_coin
        }
        
        return {**base_stats, **aggressive_stats}
    
    def get_interval_status(self) -> List[Dict[str, Any]]:
        """Get status of all price intervals and their sub-intervals."""
        interval_statuses = []
        for interval in self.price_intervals:
            sub_interval_statuses = []
            for sub in interval.sub_intervals:
                sub_interval_statuses.append({
                    'sub_interval_id': sub.sub_interval_id,
                    'start_time': sub.start_time.isoformat(),
                    'end_time': sub.end_time.isoformat(),
                    'target_price': float(sub.target_price),
                    'completed': sub.completed,
                })

            interval_statuses.append({
                'interval_id': interval.interval_id,
                'start_time': interval.start_time.isoformat(),
                'end_time': interval.end_time.isoformat(),
                'target_price': float(interval.target_price),
                'target_position': float(interval.target_position),
                'current_position': float(interval.current_position),
                'remaining_position': float(interval.get_remaining_position()),
                'interval_budget_initial': float(interval.interval_budget.initial),
                'interval_budget_remaining': float(interval.interval_budget.remaining),
                'interval_budget_consumed': float(interval.interval_budget.consumed),
                'move_budget_initial': float(interval.move_budget.initial),
                'move_budget_remaining': float(interval.move_budget.remaining),
                'move_budget_consumed': float(interval.move_budget.consumed),
                'bps_move': float(interval.bps_move),
                'completed': interval.completed,
                'sub_intervals': sub_interval_statuses
            })
        return interval_statuses
    
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

    def is_price_reached(self, current_price: Decimal, target_price: Decimal) -> bool:
        """Helper to check if target price has been reached."""
        # Assuming a buy-side strategy to push price up
        return current_price >= target_price
