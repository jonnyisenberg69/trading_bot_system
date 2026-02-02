"""
Volume-Weighted Top of Book Strategy - Dynamic rate adjustment based on exchange volume

This strategy extends the top of book strategy by dynamically adjusting execution rates
based on exchange-specific volume coefficients calculated from moving average data.
"""

import asyncio
import json
from datetime import datetime, timezone, timedelta
import time
from decimal import Decimal
from typing import Dict, List, Optional, Any, Tuple
import structlog
import redis.asyncio as redis
import aiohttp
import urllib.parse
from enum import Enum
from dataclasses import dataclass

from bot_manager.strategies.base_strategy import BaseStrategy
from order_management.execution_engine import ExecutionEngine
from order_management.order_manager import OrderManager
from order_management.tracking import PositionManager
from exchanges.base_connector import OrderType, OrderSide, OrderStatus

# Import market data collection components
from market_data_collection.database import TradeDataManager
from market_data_collection.moving_averages import MovingAverageCalculator, MovingAverageConfig, VolumeData
from market_data_collection.exchange_coefficients import SimpleExchangeCoefficientCalculator

# Import enhanced orderbook manager with cross-currency conversion
from market_data.enhanced_aggregated_orderbook_manager import EnhancedAggregatedOrderbookManager

# Import market data service components (like stacked_market_making.py)
from market_data.multi_symbol_market_data_service import (
    MultiSymbolMarketDataService, SymbolConfig
)


class SideOption(str, Enum):
    """Side options for quoting."""
    BOTH = "both"
    BID = "bid"
    OFFER = "offer"


@dataclass
class PriceLevelEntry:
    """Represents a single price level configuration for volume-weighted execution."""

    def __init__(
        self,
        side: str,  # "bid" or "offer"
        max_min_price: Decimal,  # Max price for bid, Min price for offer
        base_hourly_rate: Decimal,  # Base rate before coefficient adjustment
        rate_currency: str,  # "base" or "quote"
        drift_bps: float,
        timeout_seconds: int
    ):
        self.side = side
        self.max_min_price = max_min_price
        self.base_hourly_rate = base_hourly_rate
        self.rate_currency = rate_currency
        self.drift_bps = drift_bps
        self.timeout_seconds = timeout_seconds

        # Dynamic rate tracking
        self.current_hourly_rate: Decimal = base_hourly_rate  # Adjusted by coefficients
        self.coefficient_multiplier: float = 1.0

        # Per-exchange coefficient and rate tracking
        self.exchange_coefficients: Dict[str, float] = {}  # exchange -> coefficient
        self.exchange_hourly_rates: Dict[str, Decimal] = {}  # exchange -> adjusted hourly rate

        # Order tracking per exchange
        self.active_orders: Dict[str, str] = {}  # exchange -> order_id
        self.order_placed_at: Dict[str, datetime] = {}
        self.order_placed_price: Dict[str, Decimal] = {}

        # Time tracking - matching legacy top_of_book.py logic
        self.time_in_zone: float = 0.0
        self.last_zone_entry: Optional[datetime] = None
        self.zone_entry_execution_snapshot: Optional[Decimal] = None  # Execution level when entering zone

        # FIX 2: Track coefficient history for proper normalization - SIMPLIFIED
        # Instead of storing full history, just track sum and count for average
        self.coefficient_sum: float = 0.0
        self.coefficient_count: int = 0
        self.avg_coefficient_since_zone_entry: float = 1.0

    def apply_coefficient(self, exchange: str, coefficient: float) -> None:
        """Apply exchange coefficient to adjust the hourly rate for a specific exchange."""
        old_coefficient = self.exchange_coefficients.get(exchange, 1.0)
        old_avg_coefficient = self.avg_coefficient_since_zone_entry

        # Update per-exchange tracking
        self.exchange_coefficients[exchange] = coefficient
        self.exchange_hourly_rates[exchange] = self.base_hourly_rate * Decimal(str(coefficient))

        # Update global coefficient as weighted average for backward compatibility
        if self.exchange_coefficients:
            self.coefficient_multiplier = sum(self.exchange_coefficients.values()) / len(self.exchange_coefficients)
            self.current_hourly_rate = self.base_hourly_rate * Decimal(str(self.coefficient_multiplier))

        # FIX 2: Update coefficient tracking with simple numerical average
        if self.last_zone_entry is not None:
            # Only track coefficients while in a zone
            self.coefficient_sum += coefficient
            self.coefficient_count += 1
            # Calculate simple average
            self.avg_coefficient_since_zone_entry = self.coefficient_sum / self.coefficient_count if self.coefficient_count > 0 else coefficient

            # Enhanced logging for coefficient changes
            if abs(old_avg_coefficient - self.avg_coefficient_since_zone_entry) > 0.05:  # 5% change threshold
                structlog.get_logger().info(
                    "Coefficient tracking updated",
                    exchange=exchange,
                    side=self.side,
                    price_level=float(self.max_min_price),
                    old_coefficient=old_coefficient,
                    new_coefficient=coefficient,
                    old_avg_coefficient=old_avg_coefficient,
                    new_avg_coefficient=self.avg_coefficient_since_zone_entry,
                    coefficient_count=self.coefficient_count,
                    time_in_zone=self.time_in_zone
                )

    def __repr__(self):
        return (f"PriceLevelEntry(side={self.side}, max_min_price={self.max_min_price}, "
                f"base_rate={self.base_hourly_rate}, current_rate={self.current_hourly_rate}, "
                f"coefficient={self.coefficient_multiplier:.6f}, avg_coeff={self.avg_coefficient_since_zone_entry:.6f})")


class VolumeWeightedTopOfBookStrategy(BaseStrategy):
    """
    Volume-weighted top of book strategy that adjusts rates based on exchange volume analysis.

    Features:
    - Each bot instance calculates its own moving averages based on configured time periods
    - Dynamically adjusts quoting rates based on exchange volume patterns
    - Supports multiple price levels with individual rate adjustments
    - Configurable time periods for moving average analysis
    - Never crosses spread or self-matches
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
        self.quote_coin: str = ""
        self.start_time: Optional[datetime] = None  # User inputted start time for inventory calculation
        self.sides: SideOption = SideOption.BOTH
        self.target_inventory: Decimal = Decimal('0')
        self.excess_inventory_percentage: float = 0.0  # Percentage of excess inventory to trade
        self.spread_bps: int = 0
        self.taker_check: bool = True
        self.accounting_method: str = "FIFO"  # FIFO, LIFO, or AVERAGE_COST

        # Volume coefficient configuration
        self.time_periods: List[str] = []  # Moving average time periods to use
        self.coefficient_method: str = "min"  # min, max, or mid
        self.min_coefficient: float = 0.2  # Minimum allowed coefficient
        self.max_coefficient: float = 3.0  # Maximum allowed coefficient

        # Price level entries
        self.bid_levels: List[PriceLevelEntry] = []
        self.offer_levels: List[PriceLevelEntry] = []

        # Coefficient tracking
        self.exchange_coefficients: Dict[str, float] = {}  # exchange -> coefficient
        self.last_coefficient_update: Optional[datetime] = None

        # Moving average components (created per bot instance)
        self.ma_calculator: Optional[MovingAverageCalculator] = None
        self.coefficient_calculator: Optional[SimpleExchangeCoefficientCalculator] = None
        self.trade_db_manager: Optional[TradeDataManager] = None

        # Execution components
        self.position_manager = None
        self.execution_engine = None
        self.order_manager = None

        # Redis connection (for optional caching and performance data)
        self.redis_url = redis_url
        self.redis_client = None
        self.performance_key = f"bot_performance:{self.instance_id}"

        # Position tracking
        self.current_inventory: Decimal = Decimal('0')
        self.excess_inventory: Decimal = Decimal('0')
        self.target_excess_amount: Decimal = Decimal('0')  # Amount to trade based on percentage
        self.inventory_price: Optional[Decimal] = None

        # Delta target configuration (optional)
        self.target_inventory_source: str = "manual"  # "manual" or "delta_calc"
        self.delta_service_url: str = "http://127.0.0.1:8085"
        self.delta_refresh_seconds: float = 5.0
        self.delta_change_threshold_pct: float = 0.025
        self.delta_trade_fraction: float = 0.1
        self.delta_target_sign: float = -1.0
        self.prevent_unprofitable_trades: bool = False

        # Delta state
        self.last_delta_target: Optional[Decimal] = None
        self.last_delta_response: Optional[Dict[str, Any]] = None
        self.last_delta_updated_at: Optional[datetime] = None

        # Tasks
        self.main_task: Optional[asyncio.Task] = None
        self.ma_update_task: Optional[asyncio.Task] = None
        self.inventory_update_task: Optional[asyncio.Task] = None
        self.delta_update_task: Optional[asyncio.Task] = None

        # Performance tracking
        self.orders_placed = 0
        self.orders_filled = 0
        self.orders_skipped_inventory_protection = 0
        self.coefficient_updates_calculated = 0

        # MA tracking state
        self.last_processed_timestamp: Dict[str, datetime] = {}  # exchange -> last timestamp
        self.ma_update_interval = 10  # seconds between MA updates (fixed, not configurable)
        self.ma_update_timeout_seconds = 5  # per-exchange MA update timeout

        # Runtime tracking
        self.runtime_start_time: Optional[datetime] = None

        # Inventory update interval
        self.inventory_update_interval = 0.5  # FIX: Update every 500ms instead of 5 seconds
        # Throttle heavy inventory price recomputation to avoid event loop blocking
        self.inventory_price_recalc_interval_seconds: int = 30
        self.last_inventory_price_calc_at: Optional[datetime] = None
        # Throttle DB-based inventory fallback to avoid server overload
        self._last_db_inventory_calc_at: Optional[datetime] = None

        # Position tracking attributes
        self.position_cost: Optional[Decimal] = None
        self.position_size: Optional[Decimal] = None
        # Risk API base URL
        self.risk_api_url: str = "http://127.0.0.1:8080"
        self.risk_ws_url: str = "ws://127.0.0.1:8080/api/v2/risk/ws"
        self._ws_session: Optional[aiohttp.ClientSession] = None
        self._ws_task: Optional[asyncio.Task] = None
        self._ws_backoff_seconds: float = 1.0
        self._last_ws_inventory_update: Optional[datetime] = None
        self._ws_connected: bool = False
        self.ws_fresh_window_seconds: float = 8.0
        self._last_ws_refresh_request: Optional[datetime] = None
        self._ws_refresh_interval_seconds: float = 2.0

        # Initialize enhanced orderbook manager with cross-currency conversion
        self.enhanced_orderbook_manager = EnhancedAggregatedOrderbookManager(
            symbols=[self.symbol],
            exchanges=exchanges,
            redis_url=redis_url
        )
        self.redis_client: Optional[redis.Redis] = None
        
        # Market data service to publish orderbook data to Redis (like stacked_market_making.py)
        self.market_data_service = None

        # Exchange order locks to prevent race conditions
        self.exchange_order_locks: Dict[str, asyncio.Lock] = {
            exchange: asyncio.Lock() for exchange in exchanges
        }

        # GLOBAL: Track the single active order per exchange (across all levels)
        self.global_active_orders: Dict[str, Tuple[PriceLevelEntry, str]] = {}  # exchange -> (level, order_id)

        # Cached inventory data for faster access
        self.cached_inventory: Decimal = Decimal('0')
        self.cached_inventory_price: Optional[Decimal] = None
        self.cached_excess_inventory: Decimal = Decimal('0')
        self.last_inventory_update: datetime = datetime.now(timezone.utc)

        # Per-instance execution counters (since runtime start)
        self.executed_bid_total: Decimal = Decimal('0')
        self.executed_offer_total: Decimal = Decimal('0')

    async def _validate_config(self) -> None:
        """Validate and parse configuration."""
        self.logger.info(f"Validating config: {self.config}")

        # Parse start time
        self.start_time = None
        if 'start_time' in self.config:
            start_time_str = self.config['start_time']
            self.logger.debug(f"Original start_time in config: {start_time_str} (type: {type(start_time_str)})")

            try:
                if isinstance(start_time_str, str):
                    # Try parsing with timezone
                    try:
                        self.start_time = datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))
                        self.logger.debug(f"Parsed ISO format with timezone")
                    except:
                        # Fallback to standard format
                        naive_dt = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M")
                        self.start_time = naive_dt.replace(tzinfo=timezone.utc)
                        self.logger.debug("Parsed standard format")
                elif isinstance(start_time_str, datetime):
                    self.logger.debug(f"start_time is already a datetime object: {start_time_str}")
                    if start_time_str.tzinfo is None:
                        self.start_time = start_time_str.replace(tzinfo=timezone.utc)
                    else:
                        self.start_time = start_time_str.astimezone(timezone.utc)
                else:
                    self.logger.debug(f"Unexpected start_time type: {type(start_time_str)}")
                    self.start_time = datetime.now(timezone.utc)
            except Exception as e:
                self.logger.error(f"Could not parse start_time '{start_time_str}': {e}")
                self.start_time = datetime.now(timezone.utc)
        else:
            self.logger.debug("No start_time in config, using current time")
            self.start_time = datetime.now(timezone.utc)

        self.logger.info(f"Parsed start_time: {self.start_time} UTC")

        # Parse symbol components
        if '/' in self.symbol:
            self.base_coin, self.quote_coin = self.symbol.split('/')
        else:
            raise ValueError(f"Invalid symbol format: {self.symbol}")

        # Parse other configuration fields
        self.sides = SideOption(self.config['sides'])
        self.target_inventory = Decimal(str(self.config['target_inventory']))
        self.excess_inventory_percentage = float(self.config['excess_inventory_percentage'])
        self.spread_bps = int(self.config.get('spread_bps', 0))
        self.taker_check = bool(self.config['taker_check'])
        self.accounting_method = self.config.get('accounting_method', 'FIFO')  # Default to FIFO

        # Delta targeting safeguards
        self.target_inventory_source = self.config.get('target_inventory_source', 'manual')
        self.delta_service_url = self.config.get('delta_service_url', 'http://127.0.0.1:8085')
        self.delta_refresh_seconds = float(self.config.get('delta_refresh_seconds', 5))
        self.delta_change_threshold_pct = float(self.config.get('delta_change_threshold_pct', 0.025))
        self.delta_trade_fraction = float(self.config.get('delta_trade_fraction', 0.1))
        self.delta_target_sign = float(self.config.get('delta_target_sign', -1.0))
        self.prevent_unprofitable_trades = bool(self.config.get('prevent_unprofitable_trades', False))

        # Parse volume coefficient configuration
        self.time_periods = self.config.get('time_periods', ['5min', '15min', '30min'])
        self.coefficient_method = self.config.get('coefficient_method', 'min')
        self.min_coefficient = float(self.config.get('min_coefficient', 0.2))
        self.max_coefficient = float(self.config.get('max_coefficient', 3.0))

        # Parse price levels based on enabled sides
        bid_levels = self.config.get('bid_levels', [])
        offer_levels = self.config.get('offer_levels', [])

        # Validate based on sides configuration
        if self.sides in [SideOption.BOTH, SideOption.BID] and not bid_levels:
            raise ValueError("Bid levels required when bid side is enabled")

        if self.sides in [SideOption.BOTH, SideOption.OFFER] and not offer_levels:
            raise ValueError("Offer levels required when offer side is enabled")

        # Parse bid levels (only if bid side is enabled)
        if self.sides in [SideOption.BOTH, SideOption.BID]:
            for level_config in bid_levels:
                required_level_fields = ['max_price', 'base_hourly_rate', 'rate_currency', 'drift_bps', 'timeout_seconds']
                for field in required_level_fields:
                    if field not in level_config:
                        raise ValueError(f"Missing required field '{field}' in bid level")

                level_entry = PriceLevelEntry(
                    side="bid",
                    max_min_price=Decimal(str(level_config['max_price'])),
                    base_hourly_rate=Decimal(str(level_config['base_hourly_rate'])),
                    rate_currency=level_config['rate_currency'],
                    drift_bps=float(level_config['drift_bps']),
                    timeout_seconds=int(level_config['timeout_seconds'])
                )
                self.bid_levels.append(level_entry)

        # Parse offer levels (only if offer side is enabled)
        if self.sides in [SideOption.BOTH, SideOption.OFFER]:
            for level_config in offer_levels:
                required_level_fields = ['min_price', 'base_hourly_rate', 'rate_currency', 'drift_bps', 'timeout_seconds']
                for field in required_level_fields:
                    if field not in level_config:
                        raise ValueError(f"Missing required field '{field}' in offer level")

                level_entry = PriceLevelEntry(
                    side="offer",
                    max_min_price=Decimal(str(level_config['min_price'])),
                    base_hourly_rate=Decimal(str(level_config['base_hourly_rate'])),
                    rate_currency=level_config['rate_currency'],
                    drift_bps=float(level_config['drift_bps']),
                    timeout_seconds=int(level_config['timeout_seconds'])
                )
                self.offer_levels.append(level_entry)

        self.logger.info(f"Validated config with sides={self.sides.value}, taker_check={self.taker_check}, accounting={self.accounting_method}")
        self.logger.info(f"Enabled {len(self.bid_levels)} bid levels and {len(self.offer_levels)} offer levels")
        self.logger.info(f"Time periods for coefficient calculation: {self.time_periods}")
        self.logger.info(f"Coefficient bounds: [{self.min_coefficient}, {self.max_coefficient}]")
        self.logger.info(
            "Delta targeting: source=%s, refresh=%ss, threshold=%.4f, trade_fraction=%.3f, sign=%.1f, prevent_unprofitable=%s",
            self.target_inventory_source,
            self.delta_refresh_seconds,
            self.delta_change_threshold_pct,
            self.delta_trade_fraction,
            self.delta_target_sign,
            self.prevent_unprofitable_trades,
        )

    async def _start_strategy(self) -> None:
        """Start the volume-weighted top of book strategy."""
        self.logger.info("Starting volume-weighted top of book strategy using Redis")

        # Initialize Redis client for strategy coordination
        self.redis_client = redis.from_url("redis://localhost:6379")

        # Initialize and start market data service to publish orderbook data to Redis
        await self._initialize_market_data_service()

        # Start enhanced orderbook manager with cross-currency conversion (uses the published data)
        self.enhanced_orderbook_manager.exchange_connectors = self.exchange_connectors
        await self.enhanced_orderbook_manager.start()

        # Wait a moment for initial orderbook data and aggregation
        await asyncio.sleep(3)

        # Initialize execution components
        await self._initialize_execution_components()

        # Initialize moving average components
        await self._initialize_ma_components()

        # Initialize coefficients with default values
        for exchange in self.exchanges:
            self.exchange_coefficients[exchange] = 1.0

        # Set runtime start time
        self.runtime_start_time = datetime.now(timezone.utc)

        # Start MA update task
        self.ma_update_task = asyncio.create_task(self._ma_update_loop())

        # Start inventory update task (background)
        self.inventory_update_task = asyncio.create_task(self._inventory_update_loop())

        # Start Risk V2 WebSocket subscriber to receive pushed inventory/positions
        self._ws_task = asyncio.create_task(self._risk_ws_loop())

        # Start delta target update task (background)
        self.delta_update_task = asyncio.create_task(self._delta_target_update_loop())

        # Start exchange-specific monitoring tasks
        self.exchange_monitor_tasks = {}
        for exchange in self.exchanges:
            self.exchange_monitor_tasks[exchange] = asyncio.create_task(
                self._exchange_monitor_loop(exchange)
            )

        # Start main strategy loop
        self.main_task = asyncio.create_task(self._strategy_loop())

    async def _initialize_market_data_service(self) -> None:
        """Initialize market data service to publish orderbook data to Redis."""
        try:
            # Create symbol configuration for this strategy's symbol
            exchange_configs = []
            for exchange_id in self.exchanges:
                base_exchange = exchange_id.split('_')[0]
                exchange_type = 'perp' if '_perp' in exchange_id else 'spot'
                
                exchange_configs.append({
                    'name': base_exchange,
                    'type': exchange_type,
                    'symbol': self.symbol
                })
            
            symbol_config = SymbolConfig(
                base_symbol=self.symbol,
                exchanges=exchange_configs,
                priority=1,
                depth=100
            )
            
            # Initialize MultiSymbolMarketDataService to publish orderbook data to Redis
            self.market_data_service = MultiSymbolMarketDataService(
                symbol_configs=[symbol_config],
                exchange_connectors=self.exchange_connectors
            )
            await self.market_data_service.start()
            
            self.logger.info(f"Market data service started for {self.symbol} on {len(self.exchanges)} exchanges")
            
        except Exception as e:
            self.logger.error(f"Error initializing market data service: {e}")
            raise

    async def _stop_strategy(self) -> None:
        """Stop the volume-weighted top of book strategy."""
        self.logger.info("Stopping volume-weighted top of book strategy")

        # Cancel tasks
        tasks_to_cancel = [self.main_task, self.ma_update_task, self.inventory_update_task, self.delta_update_task]

        # Add exchange monitor tasks
        if hasattr(self, 'exchange_monitor_tasks'):
            tasks_to_cancel.extend(self.exchange_monitor_tasks.values())

        for task in tasks_to_cancel:
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        # Cancel all active orders
        await self._cancel_all_orders()

        # Clean up database connection
        if self.trade_db_manager:
            await self.trade_db_manager.close()

        # Stop market data service
        if self.market_data_service:
            await self.market_data_service.stop()

        # Stop enhanced orderbook manager
        await self.enhanced_orderbook_manager.stop()

    async def _fetch_delta_snapshot(self) -> Optional[Dict[str, Any]]:
        """Fetch delta snapshot from the delta service."""
        url = f"{self.delta_service_url.rstrip('/')}/delta"
        try:
            timeout = aiohttp.ClientTimeout(total=5)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url) as response:
                    if response.status != 200:
                        self.logger.warning(f"Delta service returned {response.status} for {url}")
                        return None
                    data = await response.json()
                    if not isinstance(data, dict) or "total_target_inventory" not in data:
                        self.logger.warning("Delta response missing total_target_inventory")
                        return None
                    return data
        except Exception as exc:
            self.logger.warning(f"Delta fetch failed: {exc}")
            return None

    async def _delta_target_update_loop(self) -> None:
        """Background loop to update target inventory from delta service with safeguards."""
        while self.running:
            try:
                if self.target_inventory_source != "delta_calc":
                    await asyncio.sleep(max(1.0, self.delta_refresh_seconds))
                    continue

                snapshot = await self._fetch_delta_snapshot()
                if snapshot:
                    await self._update_target_from_delta(snapshot)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                self.logger.warning(f"Delta update loop error: {exc}")

            await asyncio.sleep(max(1.0, self.delta_refresh_seconds))

    async def _update_target_from_delta(self, snapshot: Dict[str, Any]) -> None:
        """Apply delta target update with threshold + trade fraction safeguards."""
        try:
            raw_target = Decimal(str(snapshot.get("total_target_inventory", "0")))
            signed_target = raw_target * Decimal(str(self.delta_target_sign))

            base_amount = Decimal(str(snapshot.get("base_amount", "0")))
            premium_amount = Decimal(str(snapshot.get("premium_amount", "0")))
            total_option_size = base_amount + premium_amount
            if total_option_size <= 0:
                total_option_size = Decimal("0")

            threshold_units = total_option_size * Decimal(str(self.delta_change_threshold_pct))
            if self.last_delta_target is not None and threshold_units > 0:
                if abs(signed_target - self.last_delta_target) < threshold_units:
                    return

            trade_fraction = Decimal(str(self.delta_trade_fraction))
            trade_fraction = max(Decimal('0'), min(Decimal('1'), trade_fraction))
            current_inventory = self.current_inventory
            stepped_target = current_inventory + (signed_target - current_inventory) * trade_fraction

            self.target_inventory = stepped_target
            self.config['target_inventory'] = str(stepped_target)
            self.last_delta_target = signed_target
            self.last_delta_response = snapshot
            self.last_delta_updated_at = datetime.now(timezone.utc)

            self.logger.info(
                "Delta target update: raw=%s signed=%s threshold=%s stepped=%s current=%s",
                raw_target,
                signed_target,
                threshold_units,
                stepped_target,
                current_inventory,
            )
        except Exception as exc:
            self.logger.warning(f"Delta target update failed: {exc}")

    def _is_unprofitable_trade(self, side: str, price: Decimal) -> bool:
        """Return True if trade would lock in a loss based on avg entry price."""
        if not self.prevent_unprofitable_trades or self.inventory_price is None:
            return False
        if self.current_inventory < 0 and side == "bid" and price > self.inventory_price:
            return True
        if self.current_inventory > 0 and side == "offer" and price < self.inventory_price:
            return True
        return False

    async def _initialize_execution_components(self) -> None:
        """Initialize execution components."""
        # Initialize position manager
        self.position_manager = PositionManager()
        await self.position_manager.start()

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

        # Initialize Redis client (optional)
        try:
            self.redis_client = redis.from_url(self.redis_url)
            await self.redis_client.ping()
            self.logger.info("Connected to Redis for optional caching")
        except Exception as e:
            self.logger.warning(f"Could not connect to Redis (optional): {e}")
            self.redis_client = None

        # Wait for initial orderbook data
        await asyncio.sleep(2)

    async def _initialize_ma_components(self) -> None:
        """Initialize moving average components specific to this bot instance."""
        # Create MA configurations based on user-selected time periods
        ma_configs = self._create_ma_configs_from_time_periods()

        # Initialize MA calculator
        self.ma_calculator = MovingAverageCalculator(ma_configs)

        # Initialize coefficient calculator with bounds
        self.coefficient_calculator = SimpleExchangeCoefficientCalculator(
            ma_calculator=self.ma_calculator,
            calculation_method=self.coefficient_method,
            min_coefficient=self.min_coefficient,
            max_coefficient=self.max_coefficient
        )

        # Initialize database manager for trade data
        self.trade_db_manager = TradeDataManager()
        await self.trade_db_manager.initialize()

        self.logger.info(f"Initialized MA calculator with {len(ma_configs)} configurations")

    def _create_ma_configs_from_time_periods(self) -> List[MovingAverageConfig]:
        """Create MA configurations from user-selected time periods."""
        configs = []

        # Convert time period strings to data points
        for period_str in self.time_periods:
            period_points = self._convert_time_period_to_points(period_str)
            if period_points > 0:
                # Create both SMA and EWMA for each period, both base and quote volume
                configs.extend([
                    MovingAverageConfig(period=period_points, ma_type='sma', volume_type='quote'),
                    MovingAverageConfig(period=period_points, ma_type='sma', volume_type='base'),
                ])

                # Add EWMA for shorter periods only (to limit number of configs)
                if period_points <= 360:  # Up to 1 hour
                    configs.extend([
                        MovingAverageConfig(period=period_points, ma_type='ewma', volume_type='quote'),
                        MovingAverageConfig(period=period_points, ma_type='ewma', volume_type='base'),
                    ])

        return configs

    def _convert_time_period_to_points(self, period_str: str) -> int:
        """Convert time period string to number of data points."""
        # Assuming 10-second intervals for data collection
        conversions = {
            '30s': 3,
            '1min': 6,
            '5min': 30,
            '15min': 90,
            '30min': 180,
            '60min': 360,
            '240min': 1440,
            '480min': 2880,
            '720min': 4320,
            '1080min': 6480,
            '1440min': 8640,
            '2880min': 17280,
            '4320min': 25920,
            '10080min': 60480,
            '20160min': 120960,
            '43200min': 259200
        }

        return conversions.get(period_str, 0)

    async def _ma_update_loop(self) -> None:
        """Background task to update moving averages and coefficients."""
        self.logger.info("Starting MA update loop")

        while self.running:
            try:
                loop_start = time.perf_counter()
                # Update MAs for each exchange in parallel with timeouts
                tasks = [
                    asyncio.wait_for(
                        self._update_ma_for_exchange(exchange),
                        timeout=self.ma_update_timeout_seconds
                    ) for exchange in self.exchanges
                ]
                if tasks:
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    for idx, res in enumerate(results):
                        if isinstance(res, Exception):
                            self.logger.warning(f"MA update error for {self.exchanges[idx]}: {res}")

                # Calculate new coefficients
                await self._update_coefficients()
                loop_ms = (time.perf_counter() - loop_start) * 1000.0
                self.logger.debug(f"MA loop completed in {loop_ms:.1f}ms for {len(self.exchanges)} exchanges")

                # Wait before next update
                await asyncio.sleep(self.ma_update_interval)

            except (ValueError, ConnectionError, TimeoutError) as e:
                self.logger.error(f"Handled error in MA update loop: {e}")
                await asyncio.sleep(5)
            except Exception as e:
                self.logger.error(f"Critical error in MA update loop, re-raising: {e}")
                raise

    async def _update_ma_for_exchange(self, exchange: str) -> None:
        """Update moving averages for a specific exchange."""
        try:
            current_time = datetime.now(timezone.utc)

            # Get last processed timestamp
            last_timestamp = self.last_processed_timestamp.get(exchange)

            # Calculate time window
            if last_timestamp:
                window_start = last_timestamp
            else:
                # Start from 24 hours ago for initial load to ensure we get data
                window_start = current_time - timedelta(hours=24)

            # Get trades from database
            trades = await self._get_trades_for_ma_update(exchange, window_start, current_time)

            if not trades:
                return

            # Calculate volume data
            volume_data = self.ma_calculator.calculate_volume_data(trades)

            # Get database format for MA calculator
            db_exchange, db_symbol = self._get_db_exchange_and_symbol(exchange)

            # Update moving averages
            ma_values = self.ma_calculator.update_moving_averages(
                db_symbol, db_exchange, volume_data, current_time
            )

            # Update last processed timestamp
            self.last_processed_timestamp[exchange] = current_time

            self.logger.debug(f"Updated MAs for {exchange} (db: {db_exchange}/{db_symbol}): {len(trades)} trades processed")

        except Exception as e:
            self.logger.error(f"Error updating MA for {exchange}: {e}")

    def _get_db_exchange_and_symbol(self, exchange_id: str) -> Tuple[str, str]:
        """
        Convert exchange ID and symbol to database format.

        Args:
            exchange_id: Exchange ID like 'binance_spot', 'binance_perp', etc.

        Returns:
            Tuple of (db_exchange, db_symbol) for database queries
        """
        # Extract base exchange name (remove _spot, _perp suffixes)
        db_exchange = exchange_id.split('_')[0]

        # Determine correct symbol based on exchange type
        db_symbol = self.symbol  # Default to configured symbol

        if '_perp' in exchange_id:
            # Perpetual contracts use different symbol format
            base, quote = self.symbol.split('/') if '/' in self.symbol else (self.symbol, '')
            if db_exchange in ['binance', 'bybit']:
                db_symbol = f"{base}/{quote}:{quote}"
            elif db_exchange == 'hyperliquid':
                db_symbol = f"{base}/USDC:USDC"

        return db_exchange, db_symbol

    def _get_risk_exchange_id(self, exchange_id: str) -> Optional[int]:
        """Map exchange key to numeric risk_trades.exchange_id."""
        # Handle exchange IDs without _spot suffix for backwards compatibility
        if exchange_id in ['mexc', 'gateio', 'bitget']:
            exchange_id = f"{exchange_id}_spot"
        mapping = {
            'binance_spot': 33,
            'binance_perp': 34,
            'bybit_spot': 35,
            'bybit_perp': 36,
            'mexc_spot': 37,
            'gateio_spot': 38,
            'bitget_spot': 39,
            'hyperliquid_perp': 40,
        }
        return mapping.get(exchange_id)

    def _get_exchange_name_for_market_trades(self, exchange_id: str) -> Optional[str]:
        """Convert exchange ID to exchange name for market_trades table."""
        # Exchange name mapping for market_trades table
        exchange_name_mapping = {
            'binance_spot': 'binance',
            'binance_perp': 'binance',
            'bybit_spot': 'bybit',
            'bybit_perp': 'bybit',
            'mexc_spot': 'mexc',
            'gateio_spot': 'gateio',
            'bitget_spot': 'bitget',
            'hyperliquid_perp': 'hyperliquid'
        }
        return exchange_name_mapping.get(exchange_id)

    async def _get_trades_for_ma_update(self, exchange: str, start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
        """Get trades from market_trades table for MA update (correct MA system database)."""
        try:
            import asyncpg

            # Connect to trading_system_data database (where MA system stores data)
            conn = await asyncpg.connect('postgresql://jonnyisenberg:hello@localhost:5432/trading_system_data')

            try:
                # Convert exchange ID to exchange name for market_trades table
                exchange_name = self._get_exchange_name_for_market_trades(exchange)
                if not exchange_name:
                    self.logger.warning(f"Unknown exchange mapping for {exchange}, skipping MA fetch")
                    return []

                # Convert timezone-aware datetime to timezone-naive for database query
                start_time_for_db = start_time.astimezone(timezone.utc).replace(tzinfo=None) if start_time.tzinfo else start_time
                end_time_for_db = end_time.astimezone(timezone.utc).replace(tzinfo=None) if end_time.tzinfo else end_time

                self.logger.debug(f"MA query for {exchange}: exchange={exchange_name}, symbol={self.symbol}, start={start_time_for_db}, end={end_time_for_db}")

                # Query market_trades table (correct table for MA system)
                query = """
                    SELECT side, amount, price, timestamp
                    FROM market_trades
                    WHERE exchange = $1
                    AND symbol = $2
                    AND timestamp >= $3
                    AND timestamp <= $4
                    ORDER BY timestamp ASC
                """

                trades_data = await conn.fetch(query, exchange_name, self.symbol, start_time_for_db, end_time_for_db)

                # Convert to dict format expected by MA calculator
                filtered_trades: List[Dict[str, Any]] = []
                for trade in trades_data:
                    if trade['amount'] is None or trade['price'] is None:
                        continue
                    filtered_trades.append({
                        'side': trade['side'],
                        'amount': float(trade['amount']),
                        'price': float(trade['price']),
                        'cost': float(trade['amount']) * float(trade['price']),  # Calculate cost
                        'timestamp': trade['timestamp']
                    })

                self.logger.debug(f"Retrieved {len(filtered_trades)} trades between {start_time_for_db} and {end_time_for_db}")

                # If no trades found, check if there's any data for this symbol at all
                if len(filtered_trades) == 0:
                    diagnostic_query = """
                        SELECT COUNT(*) as total_count,
                               MIN(timestamp) as earliest_trade,
                               MAX(timestamp) as latest_trade
                        FROM market_trades
                        WHERE exchange = $1 AND symbol = $2
                    """
                    diag_result = await conn.fetchrow(diagnostic_query, exchange_name, self.symbol)
                    if diag_result:
                        self.logger.debug(f"Diagnostic for {exchange}: total_trades={diag_result['total_count']}, earliest={diag_result['earliest_trade']}, latest={diag_result['latest_trade']}")

                return filtered_trades

            finally:
                await conn.close()

        except Exception as e:
            self.logger.error(f"Error getting trades for MA update: {e}")
            return []

    async def _update_coefficients(self) -> None:
        """Update coefficients for all exchanges."""
        try:
            self.logger.info(f"Starting coefficient update for {len(self.exchanges)} exchanges")

            for exchange in self.exchanges:
                # Get database format for coefficient calculation (use market_trades format)
                db_exchange = self._get_exchange_name_for_market_trades(exchange)
                db_symbol = self.symbol

                if not db_exchange:
                    self.logger.warning(f"Unknown exchange mapping for {exchange}, using default coefficient")
                    await self._update_exchange_coefficient(exchange, 1.0)
                    continue

                coefficient = self.coefficient_calculator.calculate_coefficient(db_symbol, db_exchange)

                if coefficient is not None:
                    self.logger.info(f"Calculated coefficient for {exchange}: {coefficient:.4f} (db: {db_exchange}/{db_symbol})")
                    await self._update_exchange_coefficient(exchange, coefficient)
                else:
                    # Use default coefficient (1.0) when no MA data is available
                    default_coefficient = 1.0
                    self.logger.info(f"No MA data for {exchange} - using default coefficient {default_coefficient} (db: {db_exchange}/{db_symbol})")
                    await self._update_exchange_coefficient(exchange, default_coefficient)

            self.coefficient_updates_calculated += 1

        except Exception as e:
            self.logger.error(f"Error updating coefficients: {e}")

    async def _update_exchange_coefficient(self, exchange: str, coefficient: float) -> None:
        """Update coefficient for an exchange and adjust rates."""
        old_coefficient = self.exchange_coefficients.get(exchange, 1.0)
        self.exchange_coefficients[exchange] = coefficient
        self.last_coefficient_update = datetime.now(timezone.utc)

        # Apply coefficient to all price levels
        for level in self.bid_levels + self.offer_levels:
            level.apply_coefficient(exchange, coefficient)

        # Enhanced logging for rate changes - show both base and adjusted rates for each level
        for level in self.bid_levels + self.offer_levels:
            structlog.get_logger().info(
                "Exchange rate updated",
                exchange=exchange,
                side=level.side,
                price_level=float(level.max_min_price),
                base_hourly_rate=float(level.base_hourly_rate),
                old_coefficient=old_coefficient,
                new_coefficient=coefficient,
                adjusted_hourly_rate=float(level.current_hourly_rate),
                rate_currency=level.rate_currency
            )

        # Cancel and replace orders immediately if coefficient changed significantly
        if abs(old_coefficient - coefficient) > 0.05:  # 5% change threshold
            self.logger.info(f"Significant coefficient change for {exchange}, updating orders")
            # Cancel orders directly instead of using create_task
            await self._cancel_exchange_orders(exchange)

        # Optionally cache coefficient in Redis
        if self.redis_client:
            try:
                coefficient_data = {
                    'symbol': self.symbol,
                    'exchange': exchange,
                    'coefficient': coefficient,
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'bot_id': self.instance_id,
                    'calculation_method': self.coefficient_method
                }

                key = f"bot_coefficient:{self.instance_id}:{self.symbol}:{exchange}"
                await self.redis_client.setex(key, 300, json.dumps(coefficient_data))
            except Exception as e:
                self.logger.debug(f"Could not cache coefficient in Redis: {e}")

    async def _strategy_loop(self) -> None:
        """Main strategy execution loop."""
        self.logger.info("Starting strategy loop")

        # Wait for initial MA calculation
        await asyncio.sleep(self.ma_update_interval + 2)

        # Wait for initial inventory update
        await asyncio.sleep(2)

        # Track when we last processed each side to avoid duplicate processing
        last_processed = {
            "bid": datetime.now(timezone.utc) - timedelta(minutes=1),
            "offer": datetime.now(timezone.utc) - timedelta(minutes=1)
        }

        while self.running:
            try:
                now = datetime.now(timezone.utc)

                # Debug logging for strategy loop execution
                self.logger.debug(f"Strategy loop iteration: excess_inventory={self.excess_inventory}, should_quote={self._should_quote()}")

                # Check if we should quote based on inventory
                should_quote = self._should_quote()
                if not should_quote:
                    self.logger.debug(f"Should not quote: cached_excess_inventory={self.cached_excess_inventory}")
                    await asyncio.sleep(0.1)
                    continue
                if should_quote:
                    # Get current market price for level selection
                    current_midpoint = await self._get_current_midpoint()
                    self.logger.debug(f"Current midpoint: {current_midpoint}")
                    if not current_midpoint:
                        self.logger.warning("No current midpoint available, skipping iteration")
                        await asyncio.sleep(0.1)
                        continue

                    # Check if we have fresh data from at least one exchange
                    # Commented out data freshness check to improve performance
                    # has_fresh_data = False
                    # for exchange in self.exchanges:
                    #     if self._is_data_fresh(exchange, max_age_seconds=10.0):
                    #         has_fresh_data = True
                    #         break
                    #
                    # if not has_fresh_data:
                    #     self.logger.warning("No fresh orderbook data available, skipping strategy iteration")
                    #     await asyncio.sleep(0.1)
                    #     continue

                    # Process bid levels if enabled AND we're short
                    if self.sides in [SideOption.BOTH, SideOption.BID]:
                        if self.excess_inventory < 0:  # We're short, need to buy
                            # Only process if enough time has passed since last processing
                            if self.bid_levels and (now - last_processed["bid"]).total_seconds() >= 0.1:
                                # Find valid bid levels considering both price zone AND inventory price constraints
                                valid_bid_levels = []
                                for level in self.bid_levels:
                                    # Check if midpoint is in the level's zone
                                    if current_midpoint <= level.max_min_price:
                                        # Also check inventory price constraint
                                        if self._should_track_time_for_side("bid", current_midpoint):
                                            valid_bid_levels.append(level)

                                if valid_bid_levels:
                                    # CRITICAL FIX: Select the FIRST valid level (highest price for bids)
                                    # Sort by max_price descending and take the first one
                                    best_bid_level = max(valid_bid_levels, key=lambda x: x.max_min_price)

                                    # Log the selected level
                                    self.logger.info(
                                        f"Selected bid level with price {best_bid_level.max_min_price} "
                                        f"(current price: {current_midpoint}, inventory price: {self.inventory_price})"
                                    )

                                    await self._process_price_level(best_bid_level)
                                    last_processed["bid"] = now
                                else:
                                    self.logger.debug(f"No valid bid levels for current price {current_midpoint} with inventory constraints")
                        else:
                            # Cancel bid orders if we're long - do this directly
                            await self._cancel_side_orders("bid")

                    # Process offer levels if enabled AND we're long
                    if self.sides in [SideOption.BOTH, SideOption.OFFER]:
                        self.logger.debug(f"Checking offer levels: excess_inventory={self.excess_inventory}")
                        if self.excess_inventory > 0:  # We're long, need to sell
                            self.logger.debug(f"Should sell - have excess inventory. Time since last offer: {(now - last_processed['offer']).total_seconds():.3f}s")
                            # Only process if enough time has passed since last processing
                            if self.offer_levels and (now - last_processed["offer"]).total_seconds() >= 0.1:
                                self.logger.debug(f"Processing {len(self.offer_levels)} offer levels against current midpoint {current_midpoint}")
                                # Find valid offer levels considering both price zone AND inventory price constraints
                                valid_offer_levels = []
                                for level in self.offer_levels:
                                    # Check if midpoint is in the level's zone
                                    in_zone = current_midpoint >= level.max_min_price
                                    should_track = self._should_track_time_for_side("offer", current_midpoint)
                                    self.logger.debug(f"Level {level.max_min_price}: in_zone={in_zone}, should_track={should_track}, current={current_midpoint}, inventory_price={self.inventory_price}")
                                    if in_zone:
                                        # Also check inventory price constraint
                                        if should_track:
                                            valid_offer_levels.append(level)

                                if valid_offer_levels:
                                    # CRITICAL FIX: Select the HIGHEST valid level (highest price for offers)
                                    # Sort by max_min_price descending and take the first one
                                    best_offer_level = max(valid_offer_levels, key=lambda x: x.max_min_price)

                                    # Log the selected level
                                    self.logger.info(
                                        f"Selected offer level with price {best_offer_level.max_min_price} "
                                        f"(current price: {current_midpoint}, inventory price: {self.inventory_price})"
                                    )

                                    await self._process_price_level(best_offer_level)
                                    last_processed["offer"] = now
                                else:
                                    self.logger.debug(f"No valid offer levels for current price {current_midpoint} with inventory constraints")
                        else:
                            # Cancel offer orders if we're short - do this directly
                            await self._cancel_side_orders("offer")
                    else:
                        # Cancel all orders if we shouldn't quote - do this directly
                        await self._cancel_all_orders()

                    # Wait before next iteration - reduced to 0.001 seconds (1ms) for minimal latency
                    await asyncio.sleep(0.001)

            except Exception as e:
                self.logger.error(f"Error in strategy loop: {e}")
                await asyncio.sleep(1)  # Shorter sleep on error

    async def _inventory_update_loop(self) -> None:
        """Background task to update inventory and position data."""
        self.logger.info("Starting inventory update loop")

        while self.running:
            try:
                # Update current inventory and position
                await self._update_inventory_and_position()

                # Cache the results for faster access
                self.cached_inventory = self.current_inventory
                self.cached_inventory_price = self.inventory_price
                self.cached_excess_inventory = self.excess_inventory
                self.last_inventory_update = datetime.now(timezone.utc)

                # Wait before next update
                await asyncio.sleep(self.inventory_update_interval)

            except Exception as e:
                self.logger.error(f"Error in inventory update loop: {e}")
                await asyncio.sleep(5)  # Wait longer on error

    async def _update_inventory_and_position(self) -> None:
        """Update current inventory and position for each exchange."""
        try:
            # If we have a fresh WS-pushed inventory within the freshness window, skip HTTP/Redis/DB
            now_guard = datetime.now(timezone.utc)
            used_ws = (
                self._ws_connected and
                self._last_ws_inventory_update is not None and
                (now_guard - self._last_ws_inventory_update).total_seconds() <= self.ws_fresh_window_seconds
            )
            if not used_ws:
                # Strict WS-only mode: cancel all orders and wait for fresh WS inventory
                self.logger.warning(
                    f"WS inventory stale (last={self._last_ws_inventory_update}); cancelling all orders and waiting for fresh WS data"
                )
                try:
                    await self._cancel_all_orders()
                except Exception:
                    pass
                # Ensure WS is running and wait for freshness
                await self._ensure_ws_and_wait_for_fresh(timeout_seconds=30.0)
                # Re-check freshness after wait; if still stale, skip this cycle
                now_guard = datetime.now(timezone.utc)
                used_ws = (
                    self._ws_connected and
                    self._last_ws_inventory_update is not None and
                    (now_guard - self._last_ws_inventory_update).total_seconds() <= self.ws_fresh_window_seconds
                )
                if not used_ws:
                    # Keep previous cached values; do not fallback
                    self.logger.warning("Still no fresh WS inventory; skipping inventory update this cycle")
                    return

            # Calculate target amount to trade (WS provided current_inventory/price already)
            self.excess_inventory = self.current_inventory - self.target_inventory

            # Calculate the target amount we want to reduce from excess inventory
            self.target_excess_amount = abs(self.excess_inventory) * Decimal(str(self.excess_inventory_percentage / 100))

            self.logger.info(
                f"📊 Inventory update - Current: {self.current_inventory:.4f} {self.base_coin}, "
                f"Target: {self.target_inventory}, Excess: {self.excess_inventory:.4f}, "
                f"Target excess amount: {self.target_excess_amount:.4f} ({self.excess_inventory_percentage}% of excess)"
            )

            # Do NOT recalculate inventory price from the database.
            # Rely on WS/Redis-provided entry_price and cost_base to keep this in sync with the frontend.
            # If price is missing, keep None until the next WS/Redis update.

            if self.inventory_price:
                # Enhanced logging for inventory price calculation components
                self.logger.info(
                    f"📊 Inventory price: {self.inventory_price} {self.quote_coin} "
                    f"(position_cost: {self.position_cost} {self.quote_coin}, "
                    f"position_size: {self.position_size} {self.base_coin}, "
                    f"accounting_method: {self.accounting_method})"
                )
            else:
                self.logger.debug("📊 No inventory price available")

            # Publish performance data to Redis for API access
            await self._publish_performance_data()

        except Exception as e:
            self.logger.error(f"Error updating inventory: {e}")

    async def _calculate_current_inventory(self) -> Decimal:
        """Calculate current inventory from risk_trades table."""
        try:
            from database import get_session
            from sqlalchemy import text

            async for session in get_session():
                try:
                    # Begin fresh transaction
                    await session.begin()

                    # Get the start time for inventory calculations
                    start_time_for_db = self.get_inventory_start_time()
                    # Convert timezone-aware datetime to timezone-naive for database query
                    if start_time_for_db.tzinfo is not None:
                        start_time_for_db = start_time_for_db.astimezone(timezone.utc).replace(tzinfo=None)

                    self.logger.info(f"🔍 Calculating inventory for {self.symbol} since {start_time_for_db} using risk_trades table")

                    # Query risk_trades table including perpetual variants
                    query = text("""
                        SELECT side, amount, fee_cost, fee_currency
                        FROM risk_trades
                        WHERE (
                            symbol = :symbol
                            OR symbol = :symbol_perp
                            OR symbol = :symbol_usdt
                            OR symbol = :symbol_usdc
                            OR symbol = :symbol_current_colon
                            OR symbol LIKE :symbol_pattern
                            OR symbol LIKE :symbol_colon_pattern
                        )
                        AND timestamp >= :start_time
                        ORDER BY timestamp ASC
                    """)

                    # Prepare symbol variants (generalized for any pair)
                    base_coin, quote_coin = (self.base_coin, self.quote_coin)
                    base_symbol = f"{base_coin}/{quote_coin}"
                    symbol_perp = f"{base_symbol}-PERP"
                    symbol_colon_current = f"{base_coin}/{quote_coin}:{quote_coin}"
                    symbol_colon_usdt = f"{base_coin}/USDT:USDT"
                    symbol_colon_usdc = f"{base_coin}/USDC:USDC"
                    symbol_pattern = f"{base_coin}/%"
                    symbol_colon_pattern = f"{base_coin}/%:%"

                    result = await session.execute(
                        query,
                        {
                            "symbol": base_symbol,
                            "symbol_perp": symbol_perp,
                            "symbol_usdt": symbol_colon_usdt,
                            "symbol_usdc": symbol_colon_usdc,
                            "symbol_current_colon": symbol_colon_current,
                            "symbol_pattern": symbol_pattern,
                            "symbol_colon_pattern": symbol_colon_pattern,
                            "start_time": start_time_for_db
                        }
                    )
                    trades = result.fetchall()

                    # Calculate net inventory accounting for fees
                    inventory = Decimal('0')
                    fees_in_base = Decimal('0')
                    fees_in_quote = Decimal('0')

                    for trade in trades:
                        amount = Decimal(str(trade.amount))
                        side = trade.side.lower()

                        # Start with the trade amount
                        trade_amount = amount

                        # Process fees before adding to inventory
                        if trade.fee_cost and trade.fee_currency:
                            fee_amount = Decimal(str(trade.fee_cost))
                            fee_currency = trade.fee_currency.upper()

                            # Check if fee is in base coin
                            if self.base_coin and (fee_currency == self.base_coin.upper() or fee_currency == self.base_coin):
                                # Track base fees
                                fees_in_base += fee_amount

                                # Adjust trade amount for base fees
                                if side == 'buy':
                                    # When buying, base fees reduce the amount received
                                    trade_amount -= fee_amount
                                else:
                                    # When selling, base fees increase the amount sold (more base leaves inventory)
                                    trade_amount += fee_amount
                            elif fee_currency in ['USDT', 'USDC'] or (self.quote_coin and fee_currency == self.quote_coin.upper()):
                                # Track quote fees (but don't adjust trade amount)
                                fees_in_quote += fee_amount

                        # Add/subtract trade amount based on side
                        if side == 'buy':
                            inventory += trade_amount
                        else:  # sell
                            inventory -= trade_amount

                    self.logger.info(f"📊 Found {len(trades)} trades in risk_trades for {self.symbol} (including perps) since {start_time_for_db}")
                    self.logger.info(f"📊 Net inventory: {inventory} {self.base_coin} (fees in {self.base_coin}: {fees_in_base}, fees in quote: {fees_in_quote})")

                    # Commit the read-only transaction
                    await session.commit()
                    return inventory

                except Exception as e:
                    await session.rollback()
                    self.logger.error(f"Error in inventory calculation: {e}")
                    raise e
                finally:
                    await session.close()

        except Exception as e:
            self.logger.error(f"Error calculating inventory from risk_trades: {e}")
            return Decimal('0')

    async def _calculate_inventory_price(self) -> None:
        """Calculate inventory price from risk_trades table using average cost method."""
        try:
            from database import get_session
            from sqlalchemy import text

            # Only calculate if we have inventory
            if abs(self.current_inventory) < Decimal('0.00001'):
                self.inventory_price = None
                self.position_cost = None
                self.position_size = None
                self.total_base_fees = Decimal('0')
                self.total_quote_fees = Decimal('0')
                return

            # Initialize tracking variables for logging
            self.position_cost = None
            self.position_size = None
            self.total_base_fees = Decimal('0')
            self.total_quote_fees = Decimal('0')

            async for session in get_session():
                try:
                    # Begin fresh transaction
                    await session.begin()

                    # Get the start time for inventory calculations
                    start_time_for_db = self.get_inventory_start_time()
                    # Convert timezone-aware datetime to timezone-naive for database query
                    if start_time_for_db.tzinfo is not None:
                        start_time_for_db = start_time_for_db.astimezone(timezone.utc).replace(tzinfo=None)

                    # Query risk_trades table including perpetual variants to calculate average cost
                    query = text("""
                        SELECT side, amount, price, fee_cost, fee_currency, cost, symbol
                        FROM risk_trades
                        WHERE (
                            symbol = :symbol
                            OR symbol = :symbol_perp
                            OR symbol = :symbol_usdt
                            OR symbol = :symbol_usdc
                            OR symbol = :symbol_current_colon
                            OR symbol LIKE :symbol_pattern
                            OR symbol LIKE :symbol_colon_pattern
                        )
                        AND timestamp >= :start_time
                        ORDER BY timestamp ASC
                    """)

                    # Prepare symbol variants (generalized for any pair)
                    base_coin, quote_coin = (self.base_coin, self.quote_coin)
                    base_symbol = f"{base_coin}/{quote_coin}"
                    symbol_perp = f"{base_symbol}-PERP"
                    symbol_colon_current = f"{base_coin}/{quote_coin}:{quote_coin}"
                    symbol_colon_usdt = f"{base_coin}/USDT:USDT"
                    symbol_colon_usdc = f"{base_coin}/USDC:USDC"
                    symbol_pattern = f"{base_coin}/%"
                    symbol_colon_pattern = f"{base_coin}/%:%"

                    result = await session.execute(
                        query,
                        {
                            "symbol": base_symbol,
                            "symbol_perp": symbol_perp,
                            "symbol_usdt": symbol_colon_usdt,
                            "symbol_usdc": symbol_colon_usdc,
                            "symbol_current_colon": symbol_colon_current,
                            "symbol_pattern": symbol_pattern,
                            "symbol_colon_pattern": symbol_colon_pattern,
                            "start_time": start_time_for_db
                        }
                    )
                    trades = result.fetchall()

                    self.logger.info(f"Found {len(trades)} trades for inventory price calculation since {start_time_for_db}")

                    # Log first few trades to see the data structure
                    if trades:
                        for i, trade in enumerate(trades[:3]):
                            self.logger.debug(
                                f"Sample trade {i+1}: side={trade.side}, amount={trade.amount}, "
                                f"price={trade.price}, cost={getattr(trade, 'cost', 'NO COST FIELD')}, "
                                f"fee_cost={trade.fee_cost}, fee_currency={trade.fee_currency}"
                            )

                    # Choose calculation method based on accounting_method
                    if self.accounting_method == 'FIFO':
                        self._calculate_fifo_position(trades)
                    elif self.accounting_method == 'LIFO':
                        self._calculate_lifo_position(trades)
                    else:  # Default to AVERAGE_COST
                        self._calculate_average_cost_position(trades)

                    # Commit the read-only transaction
                    await session.commit()

                except Exception as e:
                    await session.rollback()
                    self.logger.error(f"Error in inventory price calculation: {e}")
                    self.inventory_price = None
                    self.position_cost = None
                    self.position_size = None
                    self.total_base_fees = Decimal('0')
                    self.total_quote_fees = Decimal('0')
                    raise e
                finally:
                    await session.close()

        except Exception as e:
            self.logger.error(f"Error calculating inventory price: {e}", exc_info=True)
            self.inventory_price = None
            self.position_cost = None
            self.position_size = None
            self.total_base_fees = Decimal('0')
            self.total_quote_fees = Decimal('0')

    def _calculate_average_cost_position(self, trades) -> None:
        """Calculate position using average cost method - matches risk system approach."""
        # EXACTLY MATCH RISK SYSTEM APPROACH
        # Initialize position tracking variables
        position_size = Decimal('0')
        position_cost = Decimal('0')
        total_base_fees = Decimal('0')
        total_quote_fees = Decimal('0')

        # Process trades chronologically - accumulate ALL trade costs
        for trade in trades:
            # Get basic trade info
            side = trade.side.lower()
            amount = Decimal(str(trade.amount))
            price = Decimal(str(trade.price))

            # Check if we have a cost field and use it, otherwise calculate
            if hasattr(trade, 'cost') and trade.cost is not None:
                # Use the cost from the database (like risk system does)
                base_cost = Decimal(str(trade.cost))
                trade_symbol = getattr(trade, 'symbol', None)
                base_cost = self._normalize_quote_amount(base_cost, trade_symbol)
                trade_cost = base_cost if side == 'buy' else -base_cost
            else:
                # Calculate trade cost if not provided
                trade_cost = amount * price if side == 'buy' else -(amount * price)
                trade_symbol = getattr(trade, 'symbol', None)
                trade_cost = self._normalize_quote_amount(trade_cost, trade_symbol)

            # Calculate trade size based on side
            trade_size = amount if side == 'buy' else -amount

            # Process fees exactly like the risk system
            if trade.fee_cost and trade.fee_cost > 0:
                fee_amount = Decimal(str(trade.fee_cost))
                fee_currency = trade.fee_currency.upper() if trade.fee_currency else ""

                # Handle base currency fees
                if fee_currency and (fee_currency == self.base_coin.upper() or fee_currency == self.base_coin):
                    # Track base fees separately
                    total_base_fees += fee_amount

                    # Adjust trade size based on side - EXACTLY like risk system
                    if side == 'buy':
                        # When buying, base fees reduce the amount received
                        trade_size -= fee_amount
                    else:
                        # When selling, base fees reduce the amount sold
                        # Add because trade_size is negative for sells
                        trade_size += fee_amount

                # Handle quote currency fees
                elif fee_currency and (fee_currency == self.quote_coin.upper() or fee_currency == self.quote_coin or
                                      fee_currency in ['USDT', 'USDC']):
                    # Track quote fees separately
                    fee_amount = self._normalize_quote_amount(fee_amount, getattr(trade, 'symbol', None))
                    total_quote_fees += fee_amount

                    # Adjust trade cost based on side
                    if side == 'buy':
                        # When buying, quote fees increase cost
                        trade_cost += fee_amount
                    else:
                        # When selling, quote fees reduce proceeds
                        trade_cost -= fee_amount
                else:
                    # Unknown fee currency, treat as quote fee
                    total_quote_fees += fee_amount
                    if side == 'buy':
                        trade_cost += fee_amount
                    else:
                        trade_cost -= fee_amount

            # Update position - CUMULATIVE COST OF ALL TRADES (like risk system)
            position_size += trade_size
            position_cost += trade_cost

        # Calculate average entry price using cumulative cost
        if abs(position_size) > Decimal('0.00001'):
            entry_price = abs(position_cost / position_size)
            self.inventory_price = entry_price

            # Store values for logging
            self.position_cost = position_cost
            self.position_size = position_size
            self.total_base_fees = total_base_fees
            self.total_quote_fees = total_quote_fees

            # Log inventory price calculation (reduced verbosity)
            self.logger.debug(
                f"📊 Average cost inventory: size={position_size} {self.base_coin}, "
                f"price={entry_price} {self.quote_coin}"
            )
        else:
            self.inventory_price = None
            self.position_cost = None
            self.position_size = None
            self.total_base_fees = Decimal('0')
            self.total_quote_fees = Decimal('0')
            self.logger.debug(f"📊 No inventory price available (zero position size)")

    def _calculate_fifo_position(self, trades) -> None:
        """Calculate position using FIFO method - net out realized P&L like Risk V2 system."""
        # Track individual lots for FIFO matching (oldest first)
        long_lots = []  # [(size, price, order_index, cost), ...]
        short_lots = []  # [(size, price, order_index, cost), ...]
        total_base_fees = Decimal('0')
        total_quote_fees = Decimal('0')
        total_realized_pnl = Decimal('0')  # Track realized P&L

        # Process ALL trades to track lots, fees, and realized P&L
        for trade_index, trade in enumerate(trades):
            side = trade.side.lower()
            amount = Decimal(str(trade.amount))
            price = Decimal(str(trade.price))

            # Get cost from database
            if hasattr(trade, 'cost') and trade.cost is not None:
                base_cost = Decimal(str(trade.cost))
                trade_symbol = getattr(trade, 'symbol', None)
                base_cost = self._normalize_quote_amount(base_cost, trade_symbol)
                trade_cost = base_cost if side == 'buy' else -base_cost
            else:
                trade_cost = amount * price if side == 'buy' else -(amount * price)
                trade_symbol = getattr(trade, 'symbol', None)
                trade_cost = self._normalize_quote_amount(trade_cost, trade_symbol)

            # Calculate trade size
            trade_size = amount if side == 'buy' else -amount

            # Process fees exactly like the risk system
            if trade.fee_cost and trade.fee_cost > 0:
                fee_amount = Decimal(str(trade.fee_cost))
                fee_currency = trade.fee_currency.upper() if trade.fee_currency else ""

                if fee_currency and (fee_currency == self.base_coin.upper() or fee_currency == self.base_coin):
                    total_base_fees += fee_amount
                    if side == 'buy':
                        trade_size -= fee_amount  # Less received
                    else:
                        trade_size -= fee_amount  # More base leaves on sells
                elif fee_currency and (fee_currency == self.quote_coin.upper() or fee_currency == self.quote_coin or
                                      fee_currency in ['USDT', 'USDC']):
                    total_quote_fees += fee_amount
                    if side == 'buy':
                        trade_cost += fee_amount
                    else:
                        trade_cost -= fee_amount
                else:
                    # Unknown fee currency, treat as quote fee
                    total_quote_fees += fee_amount
                    if side == 'buy':
                        trade_cost += fee_amount
                    else:
                        trade_cost -= fee_amount

            # Apply FIFO lot matching logic with realized P&L calculation
            if side == 'buy':
                # Check if we can close short positions (FIFO - oldest first)
                remaining_size = abs(trade_size)

                while remaining_size > 0 and short_lots:
                    short_lot = short_lots.pop(0)  # FIFO - take oldest (first in list)
                    lot_size, lot_price, lot_order_index, lot_cost = short_lot

                    if remaining_size >= lot_size:
                        # Close entire short lot - calculate realized P&L
                        # For short positions: P&L = (entry_price - exit_price) * size
                        realized_pnl = lot_size * (lot_price - price)
                        total_realized_pnl += realized_pnl
                        remaining_size -= lot_size

                        #self.logger.debug(f"FIFO: Closed short lot {lot_size} @ {lot_price} with buy @ {price}, P&L: {realized_pnl}")
                    else:
                        # Partial close of short lot
                        realized_pnl = remaining_size * (lot_price - price)
                        total_realized_pnl += realized_pnl

                        # Put back remaining portion at the beginning (it's still the oldest)
                        remaining_lot_size = lot_size - remaining_size
                        remaining_lot_cost = lot_cost * (remaining_lot_size / lot_size)
                        short_lots.insert(0, (remaining_lot_size, lot_price, lot_order_index, remaining_lot_cost))

                        #self.logger.debug(f"FIFO: Partially closed short lot {remaining_size} @ {lot_price} with buy @ {price}, P&L: {realized_pnl}")
                        remaining_size = 0

                # Add remaining as long position (append to end - newest)
                if remaining_size > 0:
                    # Calculate the cost for this remaining portion
                    cost_per_unit = abs(trade_cost) / abs(trade_size) if abs(trade_size) > 0 else Decimal('0')
                    remaining_cost = remaining_size * cost_per_unit
                    long_lots.append((remaining_size, price, trade_index, remaining_cost))

            else:  # sell
                # Check if we can close long positions (FIFO - oldest first)
                remaining_size = abs(trade_size)

                while remaining_size > 0 and long_lots:
                    long_lot = long_lots.pop(0)  # FIFO - take oldest (first in list)
                    lot_size, lot_price, lot_order_index, lot_cost = long_lot

                    if remaining_size >= lot_size:
                        # Close entire long lot - calculate realized P&L
                        # For long positions: P&L = (exit_price - entry_price) * size
                        realized_pnl = lot_size * (price - lot_price)
                        total_realized_pnl += realized_pnl
                        remaining_size -= lot_size

                        #self.logger.debug(f"FIFO: Closed long lot {lot_size} @ {lot_price} with sell @ {price}, P&L: {realized_pnl}")
                    else:
                        # Partial close of long lot
                        realized_pnl = remaining_size * (price - lot_price)
                        total_realized_pnl += realized_pnl

                        # Put back remaining portion at the beginning (it's still the oldest)
                        remaining_lot_size = lot_size - remaining_size
                        remaining_lot_cost = lot_cost * (remaining_lot_size / lot_size)
                        long_lots.insert(0, (remaining_lot_size, lot_price, lot_order_index, remaining_lot_cost))

                        #self.logger.debug(f"FIFO: Partially closed long lot {remaining_size} @ {lot_price} with sell @ {price}, P&L: {realized_pnl}")
                        remaining_size = 0

                # Add remaining as short position (append to end - newest)
                if remaining_size > 0:
                    # Calculate the cost for this remaining portion
                    cost_per_unit = abs(trade_cost) / abs(trade_size) if abs(trade_size) > 0 else Decimal('0')
                    remaining_cost = remaining_size * cost_per_unit
                    short_lots.append((remaining_size, price, trade_index, remaining_cost))

        # Calculate position from remaining lots only (like Risk V2)
        if long_lots:
            position_size = sum(lot[0] for lot in long_lots)
            position_cost = sum(lot[3] for lot in long_lots)  # Cost of remaining lots only
            entry_price = position_cost / position_size if position_size > 0 else Decimal('0')
        elif short_lots:
            position_size = -sum(lot[0] for lot in short_lots)
            position_cost = -sum(lot[3] for lot in short_lots)  # Negative for short positions
            entry_price = abs(position_cost / position_size) if position_size != 0 else Decimal('0')
        else:
            position_size = Decimal('0')
            position_cost = Decimal('0')
            entry_price = None

        # Store values - position_cost is cost of remaining lots (netted for realized P&L)
        self.position_size = position_size
        self.position_cost = position_cost  # This is P2 - cost of remaining lots only (like Risk V2)
        self.total_base_fees = total_base_fees
        self.total_quote_fees = total_quote_fees
        self.inventory_price = entry_price

        if entry_price:
            self.logger.debug(
                f"📊 FIFO inventory: size={position_size} {self.base_coin}, "
                f"price={entry_price} {self.quote_coin}, realized_pnl={total_realized_pnl} {self.quote_coin}"
            )
        else:
            self.inventory_price = None
            self.position_cost = None
            self.position_size = None
            self.total_base_fees = Decimal('0')
            self.total_quote_fees = Decimal('0')
            self.logger.debug(f"📊 No inventory price available (FIFO - no position)")

    def _calculate_lifo_position(self, trades) -> None:
        """Calculate position using LIFO method - net out realized P&L like Risk V2 system."""
        # Track individual lots for LIFO matching
        long_lots = []  # [(size, price, order_index, cost), ...]
        short_lots = []  # [(size, price, order_index, cost), ...]
        total_base_fees = Decimal('0')
        total_quote_fees = Decimal('0')
        total_realized_pnl = Decimal('0')  # Track realized P&L

        # Process ALL trades to track lots, fees, and realized P&L
        for trade_index, trade in enumerate(trades):
            side = trade.side.lower()
            amount = Decimal(str(trade.amount))
            price = Decimal(str(trade.price))

            # Get cost from database
            if hasattr(trade, 'cost') and trade.cost is not None:
                base_cost = Decimal(str(trade.cost))
                trade_symbol = getattr(trade, 'symbol', None)
                base_cost = self._normalize_quote_amount(base_cost, trade_symbol)
                trade_cost = base_cost if side == 'buy' else -base_cost
            else:
                trade_cost = amount * price if side == 'buy' else -(amount * price)
                trade_symbol = getattr(trade, 'symbol', None)
                trade_cost = self._normalize_quote_amount(trade_cost, trade_symbol)

            # Calculate trade size
            trade_size = amount if side == 'buy' else -amount

            # Process fees exactly like the risk system
            if trade.fee_cost and trade.fee_cost > 0:
                fee_amount = Decimal(str(trade.fee_cost))
                fee_currency = trade.fee_currency.upper() if trade.fee_currency else ""

                if fee_currency and (fee_currency == self.base_coin.upper() or fee_currency == self.base_coin):
                    total_base_fees += fee_amount
                    if side == 'buy':
                        trade_size -= fee_amount  # Less received
                    else:
                        trade_size -= fee_amount  # More base leaves on sells
                elif fee_currency and (fee_currency == self.quote_coin.upper() or fee_currency == self.quote_coin or
                                      fee_currency in ['USDT', 'USDC']):
                    total_quote_fees += fee_amount
                    if side == 'buy':
                        trade_cost += fee_amount
                    else:
                        trade_cost -= fee_amount
                else:
                    # Unknown fee currency, treat as quote fee
                    total_quote_fees += fee_amount
                    if side == 'buy':
                        trade_cost += fee_amount
                    else:
                        trade_cost -= fee_amount

            # Apply LIFO lot matching logic with realized P&L calculation
            if side == 'buy':
                # Check if we can close short positions (LIFO - most recent first)
                remaining_size = abs(trade_size)

                while remaining_size > 0 and short_lots:
                    short_lot = short_lots.pop()  # LIFO - take most recent
                    lot_size, lot_price, lot_order_index, lot_cost = short_lot

                    if remaining_size >= lot_size:
                        # Close entire short lot - calculate realized P&L
                        # For short positions: P&L = (entry_price - exit_price) * size
                        realized_pnl = lot_size * (lot_price - price)
                        total_realized_pnl += realized_pnl
                        remaining_size -= lot_size

                        #self.logger.debug(f"LIFO: Closed short lot {lot_size} @ {lot_price} with buy @ {price}, P&L: {realized_pnl}")
                    else:
                        # Partial close of short lot
                        realized_pnl = remaining_size * (lot_price - price)
                        total_realized_pnl += realized_pnl

                        # Put back remaining portion
                        remaining_lot_size = lot_size - remaining_size
                        remaining_lot_cost = lot_cost * (remaining_lot_size / lot_size)
                        short_lots.append((remaining_lot_size, lot_price, lot_order_index, remaining_lot_cost))

                        #self.logger.debug(f"LIFO: Partially closed short lot {remaining_size} @ {lot_price} with buy @ {price}, P&L: {realized_pnl}")
                        remaining_size = 0

                # Add remaining as long position
                if remaining_size > 0:
                    # Calculate the cost for this remaining portion
                    cost_per_unit = abs(trade_cost) / abs(trade_size) if abs(trade_size) > 0 else Decimal('0')
                    remaining_cost = remaining_size * cost_per_unit
                    long_lots.append((remaining_size, price, trade_index, remaining_cost))

            else:  # sell
                # Check if we can close long positions (LIFO - most recent first)
                remaining_size = abs(trade_size)

                while remaining_size > 0 and long_lots:
                    long_lot = long_lots.pop()  # LIFO - take most recent
                    lot_size, lot_price, lot_order_index, lot_cost = long_lot

                    if remaining_size >= lot_size:
                        # Close entire long lot - calculate realized P&L
                        # For long positions: P&L = (exit_price - entry_price) * size
                        realized_pnl = lot_size * (price - lot_price)
                        total_realized_pnl += realized_pnl
                        remaining_size -= lot_size

                        #self.logger.debug(f"LIFO: Closed long lot {lot_size} @ {lot_price} with sell @ {price}, P&L: {realized_pnl}")
                    else:
                        # Partial close of long lot
                        realized_pnl = remaining_size * (price - lot_price)
                        total_realized_pnl += realized_pnl

                        # Put back remaining portion
                        remaining_lot_size = lot_size - remaining_size
                        remaining_lot_cost = lot_cost * (remaining_lot_size / lot_size)
                        long_lots.append((remaining_lot_size, lot_price, lot_order_index, remaining_lot_cost))

                        #self.logger.debug(f"LIFO: Partially closed long lot {remaining_size} @ {lot_price} with sell @ {price}, P&L: {realized_pnl}")
                        remaining_size = 0

                # Add remaining as short position
                if remaining_size > 0:
                    # Calculate the cost for this remaining portion
                    cost_per_unit = abs(trade_cost) / abs(trade_size) if abs(trade_size) > 0 else Decimal('0')
                    remaining_cost = remaining_size * cost_per_unit
                    short_lots.append((remaining_size, price, trade_index, remaining_cost))

        # Calculate position from remaining lots only (like Risk V2)
        if long_lots:
            position_size = sum(lot[0] for lot in long_lots)
            position_cost = sum(lot[3] for lot in long_lots)  # Cost of remaining lots only
            entry_price = position_cost / position_size if position_size > 0 else Decimal('0')
        elif short_lots:
            position_size = -sum(lot[0] for lot in short_lots)
            position_cost = -sum(lot[3] for lot in short_lots)  # Negative for short positions
            entry_price = abs(position_cost / position_size) if position_size != 0 else Decimal('0')
        else:
            position_size = Decimal('0')
            position_cost = Decimal('0')
            entry_price = None

        # Store values - position_cost is cost of remaining lots (netted for realized P&L)
        self.position_size = position_size
        self.position_cost = position_cost  # This is P2 - cost of remaining lots only (like Risk V2)
        self.total_base_fees = total_base_fees
        self.total_quote_fees = total_quote_fees
        self.inventory_price = entry_price

        if entry_price:
            self.logger.debug(
                f"📊 LIFO inventory: size={position_size} {self.base_coin}, "
                f"price={entry_price} {self.quote_coin}, realized_pnl={total_realized_pnl} {self.quote_coin}"
            )
        else:
            self.inventory_price = None
            self.position_cost = None
            self.position_size = None
            self.total_base_fees = Decimal('0')
            self.total_quote_fees = Decimal('0')
            self.logger.debug(f"📊 No inventory price available (LIFO - no position)")

    def _calculate_fifo_entry_price_like_risk_system(self, trades: List[Dict]) -> float:
        """Calculate FIFO entry price exactly like the risk system."""
        # Track individual lots for FIFO matching
        long_lots = []  # [(size, price, timestamp), ...]
        short_lots = []  # [(size, price, timestamp), ...]

        for trade in trades:
            trade_size = trade['amount']
            trade_price = trade['price']

            if trade['side'] == 'buy':
                # Check if we can close short positions (FIFO - oldest first)
                remaining_size = trade_size

                while remaining_size > 0 and short_lots:
                    short_lot = short_lots.pop(0)  # FIFO - take oldest
                    lot_size, lot_price, lot_timestamp = short_lot

                    if remaining_size >= lot_size:
                        # Close entire short lot
                        remaining_size -= lot_size
                    else:
                        # Partial close of short lot
                        short_lots.insert(0, (lot_size - remaining_size, lot_price, lot_timestamp))
                        remaining_size = 0

                # Add remaining as long position
                if remaining_size > 0:
                    long_lots.append((remaining_size, trade_price, trade['timestamp']))

            else:  # sell
                # Check if we can close long positions (FIFO - oldest first)
                remaining_size = trade_size

                while remaining_size > 0 and long_lots:
                    long_lot = long_lots.pop(0)  # FIFO - take oldest
                    lot_size, lot_price, lot_timestamp = long_lot

                    if remaining_size >= lot_size:
                        # Close entire long lot
                        remaining_size -= lot_size
                    else:
                        # Partial close of long lot
                        long_lots.insert(0, (lot_size - remaining_size, lot_price, lot_timestamp))
                        remaining_size = 0

                # Add remaining as short position
                if remaining_size > 0:
                    short_lots.append((remaining_size, trade_price, trade['timestamp']))

        # Calculate weighted average price of remaining lots
        if long_lots:
            total_size = sum(lot[0] for lot in long_lots)
            total_cost = sum(lot[0] * lot[1] for lot in long_lots)
            return total_cost / total_size if total_size > 0 else 0
        elif short_lots:
            total_size = sum(lot[0] for lot in short_lots)
            total_cost = sum(lot[0] * lot[1] for lot in short_lots)
            return total_cost / total_size if total_size > 0 else 0
        else:
            return 0.0

    def _calculate_lifo_entry_price_like_risk_system(self, trades: List[Dict]) -> float:
        """Calculate LIFO entry price exactly like the risk system."""
        # Track individual lots for LIFO matching
        long_lots = []  # [(size, price, timestamp), ...]
        short_lots = []  # [(size, price, timestamp), ...]

        for trade in trades:
            trade_size = trade['amount']
            trade_price = trade['price']

            if trade['side'] == 'buy':
                # Check if we can close short positions (LIFO - most recent first)
                remaining_size = trade_size

                while remaining_size > 0 and short_lots:
                    short_lot = short_lots.pop()  # LIFO - take most recent
                    lot_size, lot_price, lot_timestamp = short_lot

                    if remaining_size >= lot_size:
                        # Close entire short lot
                        remaining_size -= lot_size
                    else:
                        # Partial close of short lot
                        short_lots.append((lot_size - remaining_size, lot_price, lot_timestamp))
                        remaining_size = 0

                # Add remaining as long position
                if remaining_size > 0:
                    long_lots.append((remaining_size, trade_price, trade['timestamp']))

            else:  # sell
                # Check if we can close long positions (LIFO - most recent first)
                remaining_size = trade_size

                while remaining_size > 0 and long_lots:
                    long_lot = long_lots.pop()  # LIFO - take most recent
                    lot_size, lot_price, lot_timestamp = long_lot

                    if remaining_size >= lot_size:
                        # Close entire long lot
                        remaining_size -= lot_size
                    else:
                        # Partial close of long lot
                        long_lots.append((lot_size - remaining_size, lot_price, lot_timestamp))
                        remaining_size = 0

                # Add remaining as short position
                if remaining_size > 0:
                    short_lots.append((remaining_size, trade_price, trade['timestamp']))

        # Calculate weighted average price of remaining lots
        if long_lots:
            total_size = sum(lot[0] for lot in long_lots)
            total_cost = sum(lot[0] * lot[1] for lot in long_lots)
            return total_cost / total_size if total_size > 0 else 0
        elif short_lots:
            total_size = sum(lot[0] for lot in short_lots)
            total_cost = sum(lot[0] * lot[1] for lot in short_lots)
            return total_cost / total_size if total_size > 0 else 0
        else:
            return 0.0

    def _should_quote(self) -> bool:
        """Check if we should quote based on inventory."""
        # Quote if we have excess inventory to manage
        result = abs(self.cached_excess_inventory) > Decimal('0.00001')
        self.logger.debug(f"Should quote check: cached_excess={self.cached_excess_inventory}, abs={abs(self.cached_excess_inventory)}, result={result}")
        return result

    async def _process_price_level(self, level: PriceLevelEntry) -> None:
        """Process a single price level."""
        try:
            # Get current market data
            best_bid, best_offer = await self._get_best_bid_offer()

            if not best_bid or not best_offer:
                return

            current_midpoint = (best_bid + best_offer) / 2

            # Check if price is in valid zone for this level
            in_zone = False
            if level.side == "bid":
                in_zone = current_midpoint <= level.max_min_price
            else:  # offer
                in_zone = current_midpoint >= level.max_min_price

            if in_zone:
                # Update time tracking
                self._update_time_in_zone(level, current_midpoint)

                # FIX: Take a snapshot of execution levels when entering a zone
                # This ensures we have a snapshot before calculating order size
                if level.zone_entry_execution_snapshot is None and level.last_zone_entry is not None:
                    await self._set_zone_entry_execution_snapshot(level)

                # Manage orders for this level
                await self._manage_level_orders(level, best_bid, best_offer)
            else:
                # Reset time tracking if out of zone
                if level.last_zone_entry:
                    level.time_in_zone = 0.0
                    level.last_zone_entry = None

                # Cancel any active orders for this level
                await self._cancel_level_orders(level)

        except Exception as e:
            self.logger.error(f"Error processing price level {level}: {e}")

    def _update_time_in_zone(self, level: PriceLevelEntry, current_price: Decimal) -> None:
        """Update time spent in valid price zone for the price level - matching legacy logic."""
        now = datetime.now(timezone.utc)

        # Reset time tracking for other levels when switching to this level
        # Change to use asyncio.create_task to handle the async call from a sync method
        if self.running:  # Only create task if strategy is running
            asyncio.create_task(self._reset_inactive_level_time_tracking(level, now))

        # Check if we should be tracking time based on inventory price and spread
        should_track_time = self._should_track_time_for_side(level.side, current_price)

        if level.last_zone_entry is None and should_track_time:
            # Fresh entry into this price zone - reset everything for a clean start
            # This ensures we don't carry over time from previous entries into this zone
            level.time_in_zone = 0.0  # Reset time to start fresh
            level.zone_entry_execution_snapshot = None  # Will be set below

            # FIX 2: Reset coefficient tracking for fresh zone entry - SIMPLIFIED
            level.coefficient_sum = level.coefficient_multiplier  # Start with current coefficient
            level.coefficient_count = 1  # Start with count of 1
            level.avg_coefficient_since_zone_entry = level.coefficient_multiplier

            if self.runtime_start_time and now >= self.runtime_start_time:
                level.last_zone_entry = now  # Use current time, not runtime start

                # We'll set the snapshot in _process_price_level after this method returns
                # to avoid the race condition

                # Enhanced logging for zone entry
                structlog.get_logger().info(
                    "Entering price zone",
                    side=level.side,
                    price_level=float(level.max_min_price),
                    current_price=float(current_price),
                    current_coefficient=level.coefficient_multiplier,
                    inventory_price=float(self.inventory_price) if self.inventory_price else None,
                    current_inventory=float(self.current_inventory),
                    target_inventory=float(self.target_inventory),
                    excess_inventory=float(self.excess_inventory)
                )
            else:
                # Strategy runtime hasn't started yet, don't track time
                return
        else:
            if should_track_time:
                # Continue tracking time in current zone
                elapsed_hours = (now - level.last_zone_entry).total_seconds() / 3600

                # Only add positive elapsed time (in case of clock adjustments)
                if elapsed_hours > 0:
                    level.time_in_zone += elapsed_hours

                    # Log time updates at regular intervals (every 0.1 hours or 6 minutes)
                    if int(level.time_in_zone * 10) > int((level.time_in_zone - elapsed_hours) * 10):
                        structlog.get_logger().debug(
                            "Time in zone updated",
                            side=level.side,
                            price_level=float(level.max_min_price),
                            time_in_zone=level.time_in_zone,
                            elapsed_since_last=elapsed_hours,
                            zone_entry_time=level.last_zone_entry.isoformat(),
                            avg_coefficient=level.avg_coefficient_since_zone_entry
                        )

                level.last_zone_entry = now
            else:
                # We should not be tracking time but were previously in the zone
                if level.last_zone_entry is not None:
                    structlog.get_logger().info(
                        "Pausing time tracking due to price conditions",
                        side=level.side,
                        price_level=float(level.max_min_price),
                        current_price=float(current_price),
                        inventory_price=float(self.inventory_price) if self.inventory_price else None,
                        time_in_zone=level.time_in_zone
                    )
                return

        self.logger.debug(
            f"Time in zone updated for {level.side} level (price: {level.max_min_price}): {level.time_in_zone:.4f} hours "
            f"(zone entry: {level.last_zone_entry}, avg_coeff: {level.avg_coefficient_since_zone_entry:.6f})"
        )

    def _should_track_time_for_side(self, side: str, current_price: Decimal) -> bool:
        """
        Determine if we should track time for the given side based on current price vs inventory price.

        For bid (buy) side: Track time when current price is BELOW inventory price minus spread
        (we want to buy below our cost basis)

        For offer (sell) side: Track time when current price is ABOVE inventory price plus spread
        (we want to sell above our cost basis)
        
        IMPORTANT: When inventory_price is None (no position), we should ALWAYS track time
        because we need to be able to build a position (either long or short) to reach our target.
        """
        if not self.inventory_price:
            # No inventory price available - this means we have no position yet.
            # We should ALWAYS track time so we can build a position to reach our target.
            # - If target is negative, we need to sell (go short)
            # - If target is positive, we need to buy (go long)
            self.logger.debug(f"No inventory price available for {side} - allowing time tracking to build position")
            return True

        spread_decimal = Decimal(str(self.spread_bps / 10000))

        if side == "bid":
            # For bid: track time when current price is below inventory price minus spread
            # This means we can buy below our cost basis (profitable)
            threshold_price = self.inventory_price * (Decimal('1') - spread_decimal)
            result = current_price <= threshold_price
            self.logger.debug(f"Bid time tracking: current={current_price}, threshold={threshold_price}, inventory={self.inventory_price}, spread_bps={self.spread_bps}, result={result}")
            return result
        else:  # offer
            # For offer: track time when current price is above inventory price plus spread
            # This means we can sell above our cost basis (profitable)
            threshold_price = self.inventory_price * (Decimal('1') + spread_decimal)
            result = current_price >= threshold_price
            self.logger.debug(f"Offer time tracking: current={current_price}, threshold={threshold_price}, inventory={self.inventory_price}, spread_bps={self.spread_bps}, result={result}")
            return result

    async def _set_zone_entry_execution_snapshot(self, level: PriceLevelEntry) -> None:
        """Set execution snapshot when entering a zone to track progress only since zone entry."""
        try:
            # Use local per-instance counters instead of querying DB
            if level.side == "bid":
                level.zone_entry_execution_snapshot = self.executed_bid_total
            else:  # offer
                level.zone_entry_execution_snapshot = self.executed_offer_total

            self.logger.info(
                f"📊 Set execution snapshot for {level.side} zone (price: {level.max_min_price}): "
                f"{level.zone_entry_execution_snapshot}"
            )
        except Exception as e:
            self.logger.error(f"Error setting zone entry execution snapshot: {e}")
            level.zone_entry_execution_snapshot = Decimal('0')

    async def _reset_inactive_level_time_tracking(self, active_level: PriceLevelEntry, current_time: datetime) -> None:
        """Reset time tracking for levels that are not currently active."""
        # Get all levels for the same side
        levels = self.bid_levels if active_level.side == "bid" else self.offer_levels

        for level in levels:
            if level != active_level and level.last_zone_entry is not None:
                # This level was previously active but is no longer the selected level
                # Log the final state before resetting
                structlog.get_logger().info(
                    "Exiting price zone",
                    side=level.side,
                    price_level=float(level.max_min_price),
                    time_in_zone=level.time_in_zone,
                    zone_entry_time=level.last_zone_entry.isoformat() if level.last_zone_entry else None,
                    avg_coefficient=level.avg_coefficient_since_zone_entry,
                    coefficient_count=level.coefficient_count,
                    active_orders_count=len(level.active_orders),
                    new_active_level=float(active_level.max_min_price)
                )

                # Complete reset when leaving a zone - no time accumulation across entries
                level.last_zone_entry = None
                level.time_in_zone = 0.0  # Reset time completely
                level.zone_entry_execution_snapshot = None  # Reset execution snapshot

                # FIX 2: Reset coefficient tracking when leaving a zone - SIMPLIFIED
                level.coefficient_sum = level.coefficient_multiplier
                level.coefficient_count = 1
                level.avg_coefficient_since_zone_entry = level.coefficient_multiplier

                # Cancel any active orders for this inactive level immediately
                if level.active_orders:
                    structlog.get_logger().info(
                        "Cancelling orders for inactive level",
                        side=level.side,
                        price_level=float(level.max_min_price),
                        active_orders=list(level.active_orders.keys())
                    )
                    # Cancel directly instead of creating a task
                    await self._cancel_level_orders(level)

    async def _calculate_order_size(self, level: PriceLevelEntry, exchange: str) -> Decimal:
        """Calculate order size based on rate and time in zone with zone-specific execution tracking."""
        if level.time_in_zone <= 0:
            return Decimal('0')

        # Calculate how much we should have executed by now based on time in THIS zone
        # Use base hourly rate (without coefficient) for the target calculation
        base_target_executed_in_zone = level.base_hourly_rate * Decimal(str(level.time_in_zone))

        # Get actual executed amounts since entering THIS specific zone using local counters
        if level.zone_entry_execution_snapshot is not None:
            # Determine which side we're processing and read local totals
            if level.side == "bid":
                current_total_executed = self.executed_bid_total
            else:  # offer
                current_total_executed = self.executed_offer_total

            # Calculate execution progress since entering this zone
            actual_executed_in_zone = current_total_executed - level.zone_entry_execution_snapshot

            # FIX 2: Normalize actual execution by the average coefficient used in this zone
            # This accounts for the fact that executed amounts already include coefficient adjustments
            if level.avg_coefficient_since_zone_entry > 0:
                normalized_actual_executed = actual_executed_in_zone / Decimal(str(level.avg_coefficient_since_zone_entry))

                # Enhanced logging for normalization
                structlog.get_logger().info(
                    "Normalizing executed amount",
                    side=level.side,
                    price_level=float(level.max_min_price),
                    raw_executed=float(actual_executed_in_zone),
                    avg_coefficient=level.avg_coefficient_since_zone_entry,
                    normalized_executed=float(normalized_actual_executed),
                    time_in_zone=level.time_in_zone
                )
            else:
                normalized_actual_executed = actual_executed_in_zone
        else:
            # Fallback if snapshot not set yet (shouldn't happen in normal operation)
            self.logger.warning(f"Zone entry execution snapshot not set for {level.side} level, using zero")
            normalized_actual_executed = Decimal('0')

        # Check if we still have excess inventory to manage
        current_excess = abs(self.excess_inventory)

        # Calculate how much more we need to execute in base units (without coefficient)
        base_remaining_to_execute = base_target_executed_in_zone - normalized_actual_executed

        # CRITICAL FIX: Ensure we don't stop selling when we still have inventory
        # If we have excess inventory but base_remaining_to_execute is negative,
        # it means we're ahead of schedule but should still continue selling at a reduced rate
        if base_remaining_to_execute <= Decimal('0'):
            if current_excess > Decimal('0.00001'):
                # We still have excess inventory to manage
                # Allow execution at a reduced rate (25% of hourly rate) to continue making progress
                reduced_rate = level.base_hourly_rate * Decimal('0.25')
                base_remaining_to_execute = reduced_rate * Decimal(str(level.time_in_zone % 1.0 + 0.1))

                # Enhanced logging for reduced rate execution
                structlog.get_logger().info(
                    "Continuing execution at reduced rate despite being ahead of schedule",
                    side=level.side,
                    price_level=float(level.max_min_price),
                    base_target=float(base_target_executed_in_zone),
                    normalized_actual=float(normalized_actual_executed),
                    excess_inventory=float(current_excess),
                    reduced_rate=float(reduced_rate),
                    base_remaining=float(base_remaining_to_execute)
                )
            else:
                # Nothing more to execute in this zone
                structlog.get_logger().debug(
                    "No more execution needed in zone",
                    side=level.side,
                    price_level=float(level.max_min_price),
                    base_target=float(base_target_executed_in_zone),
                    normalized_actual=float(normalized_actual_executed),
                    time_in_zone=level.time_in_zone,
                    avg_coefficient=level.avg_coefficient_since_zone_entry,
                    current_excess=float(current_excess)
                )
                return Decimal('0')

        # Apply current coefficient to the remaining amount for this order
        # Use exchange-specific coefficient if available, otherwise fall back to global
        exchange_coefficient = level.exchange_coefficients.get(exchange, level.coefficient_multiplier)
        remaining_to_execute = base_remaining_to_execute * Decimal(str(exchange_coefficient))

        # Enhanced logging for order size calculation
        structlog.get_logger().info(
            "Order size calculation",
            exchange=exchange,
            side=level.side,
            price_level=float(level.max_min_price),
            base_rate=float(level.base_hourly_rate),
            exchange_coefficient=exchange_coefficient,
            current_coefficient=level.coefficient_multiplier,
            avg_coefficient=level.avg_coefficient_since_zone_entry,
            base_target=float(base_target_executed_in_zone),
            normalized_actual=float(normalized_actual_executed),
            base_remaining=float(base_remaining_to_execute),
            adjusted_remaining=float(remaining_to_execute),
            current_excess=float(current_excess),
            time_in_zone=level.time_in_zone
        )

        # FIX: Safety check to prevent runaway execution
        # If the coefficient is extremely high, limit the execution to a reasonable amount
        # based on the current excess inventory
        if remaining_to_execute > current_excess:
            structlog.get_logger().warning(
                "Safety limit applied to prevent runaway execution",
                exchange=exchange,
                side=level.side,
                price_level=float(level.max_min_price),
                original_size=float(remaining_to_execute),
                limited_size=float(current_excess),
                current_excess=float(current_excess),
                coefficient=exchange_coefficient
            )
            remaining_to_execute = current_excess

        # Ensure we don't execute more than what would push us beyond our target
        # NEGATIVE TARGET SUPPORT: The logic needs to handle both positive and negative targets
        # - For buying: we're trying to increase inventory towards target (or above if target is negative)
        # - For selling: we're trying to decrease inventory towards target (or below if target is positive)
        if level.side == "bid":  # buying
            # We're buying because excess_inventory < 0 (current < target)
            # Max we can buy is the amount needed to reach target
            amount_to_reach_target = self.target_inventory - self.current_inventory
            if amount_to_reach_target > Decimal('0'):
                # We need to buy to reach target
                if remaining_to_execute > amount_to_reach_target:
                    structlog.get_logger().info(
                        "Limiting buy size to avoid exceeding target inventory",
                        side=level.side,
                        original_size=float(remaining_to_execute),
                        limited_size=float(amount_to_reach_target),
                        current_inventory=float(self.current_inventory),
                        target_inventory=float(self.target_inventory)
                    )
                    remaining_to_execute = amount_to_reach_target
            else:
                # We shouldn't be buying if current >= target
                structlog.get_logger().debug(
                    "No buying needed - already at or above target",
                    current_inventory=float(self.current_inventory),
                    target_inventory=float(self.target_inventory)
                )
                remaining_to_execute = Decimal('0')
        else:  # selling
            # We're selling because excess_inventory > 0 (current > target)
            # Max we can sell is the amount needed to reach target
            amount_to_reach_target = self.current_inventory - self.target_inventory
            if amount_to_reach_target > Decimal('0'):
                # We need to sell to reach target
                if remaining_to_execute > amount_to_reach_target:
                    structlog.get_logger().info(
                        "Limiting sell size to avoid falling below target inventory",
                        side=level.side,
                        original_size=float(remaining_to_execute),
                        limited_size=float(amount_to_reach_target),
                        current_inventory=float(self.current_inventory),
                        target_inventory=float(self.target_inventory)
                    )
                    remaining_to_execute = amount_to_reach_target
            else:
                # We shouldn't be selling if current <= target
                structlog.get_logger().debug(
                    "No selling needed - already at or below target",
                    current_inventory=float(self.current_inventory),
                    target_inventory=float(self.target_inventory)
                )
                remaining_to_execute = Decimal('0')



        return remaining_to_execute

    async def _manage_level_orders(
        self,
        level: PriceLevelEntry,
        best_bid: Decimal,
        best_offer: Decimal
    ) -> None:
        """Manage orders for a price level across all exchanges."""
        # DON'T cancel all orders upfront - let each exchange handle its own lifecycle
        # This prevents race conditions and stale order references

        # Process each exchange in parallel with fresh market data
        exchange_tasks = []
        for exchange in self.exchanges:
            # Create task for each exchange
            task = self._process_exchange_order(exchange, level, best_bid, best_offer)
            exchange_tasks.append(task)

        # Run all exchange tasks in parallel
        if exchange_tasks:
            await asyncio.gather(*exchange_tasks)

    def _has_order_on_exchange(self, exchange: str) -> bool:
        """Check if ANY level has an active order on this exchange."""
        for level in self.bid_levels + self.offer_levels:
            if exchange in level.active_orders:
                order_id = level.active_orders[exchange]
                # Verify it's still in base strategy tracking
                if order_id in self.active_orders:
                    return True
        return False

    async def _process_exchange_order(
        self,
        exchange: str,
        level: PriceLevelEntry,
        global_best_bid: Decimal,
        global_best_offer: Decimal
    ) -> None:
        """Process order placement for a single exchange."""
        try:
            # Calculate exchange-specific size
            size = await self._calculate_order_size(level, exchange)
            if size <= 0:
                self.logger.debug(f"Zero size calculated for {exchange}, skipping order placement")
                return

            # Use exchange-specific lock to prevent race conditions
            async with self.exchange_order_locks.get(exchange, asyncio.Lock()):
                try:
                    # CRITICAL: Ensure we only have one order per exchange across all levels
                    if self._has_order_on_exchange(exchange):
                        self.logger.debug(f"Already have an order on {exchange}, skipping placement for {level.side} level")
                        return

                    # Check if we already have an active order for this exchange
                    if exchange in level.active_orders:
                        order_id = level.active_orders[exchange]
                        # Verify the order is still in base strategy tracking
                        if order_id in self.active_orders:
                            # Order exists and is active - skip placing a new one
                            self.logger.debug(f"Order {order_id} already active on {exchange} for {level.side} level")
                            return
                        else:
                            # Order is in our tracking but not in base strategy - clean it up
                            self.logger.warning(f"Cleaning up stale order reference {order_id} on {exchange}")
                            del level.active_orders[exchange]
                            level.order_placed_at.pop(exchange, None)
                            level.order_placed_price.pop(exchange, None)

                    # Get fresh market data for THIS exchange using enhanced manager
                    exchange_best_bid, exchange_best_offer = await self.enhanced_orderbook_manager.get_best_bid_ask_direct(exchange, self.symbol)
                    
                    # If enhanced manager fails, fallback to direct exchange connector
                    if not exchange_best_bid or not exchange_best_offer:
                        self.logger.debug(f"Enhanced manager data not available for {exchange}, using direct connector")
                        exchange_best_bid, exchange_best_offer = await self._get_direct_exchange_prices(exchange)
                    
                    if not exchange_best_bid or not exchange_best_offer:
                        self.logger.warning(f"No market data available for {exchange} (tried enhanced + direct), skipping")
                        return

                    # Get exchange coefficient for logging
                    coefficient = self.exchange_coefficients.get(exchange, 1.0)

                    # Enhanced logging with exchange-specific coefficient and rates
                    structlog.get_logger().debug(
                        "Exchange market data",
                        exchange=exchange,
                        best_bid=float(exchange_best_bid),
                        best_offer=float(exchange_best_offer),
                        side=level.side,
                        base_hourly_rate=float(level.base_hourly_rate),
                        exchange_coefficient=coefficient,
                        adjusted_hourly_rate=float(level.current_hourly_rate)
                    )

                    # INVENTORY PROTECTION: Check if we should quote based on best prices vs inventory
                    if self.inventory_price:
                        spread_decimal = Decimal(str(self.spread_bps / 10000))

                        if level.side == "bid":
                            # Don't quote bids if best bid > inventory price - spread
                            max_allowed_best_bid = self.inventory_price * (Decimal('1') - spread_decimal)
                            if exchange_best_bid > max_allowed_best_bid:
                                self.logger.info(
                                    f"🛡️ INVENTORY PROTECTION [{exchange}]: Best bid {exchange_best_bid} > max allowed {max_allowed_best_bid} "
                                    f"(inventory price {self.inventory_price} - {self.spread_bps} bps spread). "
                                    f"Skipping bid order to avoid buying above inventory price."
                                )
                                self.orders_skipped_inventory_protection += 1
                                return
                        else:  # offer
                            # Don't quote offers if best offer < inventory price + spread
                            min_allowed_best_offer = self.inventory_price * (Decimal('1') + spread_decimal)
                            if exchange_best_offer < min_allowed_best_offer:
                                self.logger.info(
                                    f"🛡️ INVENTORY PROTECTION [{exchange}]: Best offer {exchange_best_offer} < min allowed {min_allowed_best_offer} "
                                    f"(inventory price {self.inventory_price} + {self.spread_bps} bps spread). "
                                    f"Skipping offer order to avoid selling below inventory price."
                                )
                                self.orders_skipped_inventory_protection += 1
                                return

                    # Calculate quote price - Place AT best bid/offer for top-of-book strategy
                    if level.side == "bid":
                        quote_price = exchange_best_bid
                    else:  # offer
                        quote_price = exchange_best_offer

                    # Apply taker check if enabled (should rarely trigger since we're at best)
                    if self.taker_check:
                        original_quote_price = quote_price
                        quote_price = await self._apply_taker_check_for_exchange(
                            quote_price, level.side, exchange_best_bid, exchange_best_offer
                        )
                        if quote_price is None:
                            self.logger.warning(f"Taker check failed for {level.side} order on {exchange}, skipping order placement")
                            return
                        if quote_price != original_quote_price:
                            self.logger.info(f"Taker check adjusted {level.side} price from {original_quote_price} to {quote_price} on {exchange}")

                    # Prevent unprofitable trades if enabled
                    if self._is_unprofitable_trade(level.side, quote_price):
                        self.logger.info(
                            "Unprofitable trade blocked: side=%s price=%s inventory_price=%s inventory=%s",
                            level.side,
                            quote_price,
                            self.inventory_price,
                            self.current_inventory,
                        )
                        return

                    # Enforce level min/max by skipping out-of-bounds quotes (TOB only)
                    if level.side == "bid" and level.max_min_price and quote_price > level.max_min_price:
                        self.logger.info(
                            f"Skipping bid on {exchange}: price {quote_price} > max {level.max_min_price}"
                        )
                        return
                    if level.side == "offer" and level.max_min_price and quote_price < level.max_min_price:
                        self.logger.info(
                            f"Skipping offer on {exchange}: price {quote_price} < min {level.max_min_price}"
                        )
                        return

                    # Size already includes exchange-specific coefficient from _calculate_order_size
                    if size > 0:
                        await self._place_level_order(level, exchange, size, quote_price)

                except Exception as e:
                    self.logger.error(f"Error processing exchange {exchange} for level {level.side}: {e}")

        except Exception as e:
            self.logger.error(f"Error processing exchange {exchange} for level {level.side}: {e}")

    async def _apply_taker_check(self, quote_price: Decimal, side: str) -> Decimal:
        """Apply taker check to ensure we don't cross the spread."""
        try:
            best_bid, best_offer = await self._get_best_bid_offer()

            if not best_bid or not best_offer:
                self.logger.warning(f"No best bid/offer available for taker check, using original price: {quote_price}")
                return quote_price

            original_price = quote_price

            if side == "bid":
                # For bid orders, ensure we don't exceed best offer (become taker)
                if quote_price >= best_offer:
                    # Adjust to be just below best offer
                    quote_price = best_offer - self._get_min_tick_size()
                    self.logger.info(f"🛡️ TAKER CHECK: Bid price {original_price} >= best offer {best_offer}, adjusted to {quote_price}")

                    # Additional safety check: ensure adjusted price doesn't exceed best bid
                    if quote_price > best_bid:
                        quote_price = best_bid - self._get_min_tick_size()
                        self.logger.info(f"🛡️ TAKER CHECK: Further adjusted bid price to {quote_price} to stay below best bid")

            else:  # offer
                # For offer orders, ensure we don't go below best bid (become taker)
                if quote_price <= best_bid:
                    # Adjust to be just above best bid
                    quote_price = best_bid + self._get_min_tick_size()
                    self.logger.info(f"🛡️ TAKER CHECK: Offer price {original_price} <= best bid {best_bid}, adjusted to {quote_price}")

                    # Additional safety check: ensure adjusted price doesn't go below best offer
                    if quote_price < best_offer:
                        quote_price = best_offer + self._get_min_tick_size()
                        self.logger.info(f"🛡️ TAKER CHECK: Further adjusted offer price to {quote_price} to stay above best offer")

            # Final validation: ensure we haven't crossed the spread
            if side == "bid" and quote_price >= best_offer:
                self.logger.error(f"🚨 TAKER CHECK FAILED: Final bid price {quote_price} still >= best offer {best_offer}")
                return None  # Don't place order
            elif side == "offer" and quote_price <= best_bid:
                self.logger.error(f"🚨 TAKER CHECK FAILED: Final offer price {quote_price} still <= best bid {best_bid}")
                return None  # Don't place order

            return quote_price

        except Exception as e:
            self.logger.error(f"Error in taker check: {e}")
            return quote_price

    def _get_min_tick_size(self) -> Decimal:
        """Get minimum tick size for the symbol."""
        # Default tick size, should be overridden based on exchange/symbol
        return Decimal('0.000001')

    async def _apply_taker_check_for_exchange(
        self,
        quote_price: Decimal,
        side: str,
        best_bid: Decimal,
        best_offer: Decimal
    ) -> Optional[Decimal]:
        """
        Apply taker check using exchange-specific best bid/offer.
        This version doesn't need to fetch prices again since they're passed in.
        """
        try:
            if not best_bid or not best_offer:
                self.logger.warning(f"No best bid/offer available for taker check, using original price: {quote_price}")
                return quote_price

            original_price = quote_price
            min_tick_size = self._get_min_tick_size()

            if side == "bid":
                # For bid orders, ensure we don't exceed best offer (become taker)
                if quote_price >= best_offer:
                    # Adjust to be just below best offer
                    quote_price = best_offer - min_tick_size
                    self.logger.info(f"🛡️ TAKER CHECK: Bid price {original_price} >= best offer {best_offer}, adjusted to {quote_price}")

                    # Additional safety check: ensure adjusted price doesn't exceed best bid
                    if quote_price > best_bid:
                        quote_price = best_bid - min_tick_size
                        self.logger.info(f"🛡️ TAKER CHECK: Further adjusted bid price to {quote_price} to stay below best bid")

            else:  # offer
                # For offer orders, ensure we don't go below best bid (become taker)
                if quote_price <= best_bid:
                    # Adjust to be just above best bid
                    quote_price = best_bid + min_tick_size
                    self.logger.info(f"🛡️ TAKER CHECK: Offer price {original_price} <= best bid {best_bid}, adjusted to {quote_price}")

                    # Additional safety check: ensure adjusted price doesn't go below best offer
                    if quote_price < best_offer:
                        quote_price = best_offer + min_tick_size
                        self.logger.info(f"🛡️ TAKER CHECK: Further adjusted offer price to {quote_price} to stay above best offer")

            # Final validation: ensure we haven't crossed the spread
            if side == "bid" and quote_price >= best_offer:
                self.logger.error(f"🚨 TAKER CHECK FAILED: Final bid price {quote_price} still >= best offer {best_offer}")
                return None  # Don't place order
            elif side == "offer" and quote_price <= best_bid:
                self.logger.error(f"🚨 TAKER CHECK FAILED: Final offer price {quote_price} still <= best bid {best_bid}")
                return None  # Don't place order

            return quote_price

        except Exception as e:
            self.logger.error(f"Error in taker check: {e}")
            return quote_price

    async def _place_level_order(
        self,
        level: PriceLevelEntry,
        exchange: str,
        size: Decimal,
        price: Decimal
    ) -> None:
        """Place an order for a price level on a specific exchange, enforcing only one order per exchange globally."""
        try:
            # Require fresh WS inventory before placing any order to strictly respect target
            now_guard = datetime.now(timezone.utc)
            ws_fresh = (
                self._ws_connected and
                self._last_ws_inventory_update is not None and
                (now_guard - self._last_ws_inventory_update).total_seconds() <= self.ws_fresh_window_seconds
            )
            if not ws_fresh:
                self.logger.warning(
                    f"Skipping order on {exchange} for level {level.side} due to stale WS inventory. "
                    f"last_ws_update={self._last_ws_inventory_update}, window={self.ws_fresh_window_seconds}s"
                )
                return

            # GLOBAL CHECK: If there is already an order for this exchange, cancel it first (regardless of level)
            if exchange in self.global_active_orders:
                prev_level, prev_order_id = self.global_active_orders[exchange]
                structlog.get_logger().info(
                    "Global order enforcement: Cancelling previous order before placing new one",
                    exchange=exchange,
                    prev_level_side=prev_level.side,
                    prev_level_price=float(prev_level.max_min_price),
                    prev_order_id=prev_order_id
                )
                # Cancel the previous order and clean up tracking
                await self._cancel_exchange_level_order(prev_level, exchange)
                # Remove from global tracking
                self.global_active_orders.pop(exchange, None)

            # Convert side: 'bid' -> 'buy', 'offer' -> 'sell'
            order_side = 'buy' if level.side == 'bid' else 'sell'

            # Get coefficient for detailed logging
            coefficient = self.exchange_coefficients.get(exchange, 1.0)

            # Enhanced logging for quoting decision with clear rate information
            structlog.get_logger().info(
                "Preparing to place order",
                exchange=exchange,
                side=order_side,
                size=float(size),
                price=float(price),
                level_price=float(level.max_min_price),
                base_hourly_rate=float(level.base_hourly_rate),
                exchange_coefficient=coefficient,
                adjusted_hourly_rate=float(level.current_hourly_rate),
                rate_currency=level.rate_currency,
                current_inventory=float(self.current_inventory),
                target_inventory=float(self.target_inventory),
                excess_inventory=float(self.excess_inventory)
            )

            # FIX: Check if this order would push us beyond our inventory target
            # NEGATIVE TARGET SUPPORT: Handle both positive and negative targets correctly
            # For buy orders: limit to amount needed to reach target (if current < target)
            # For sell orders: limit to amount needed to reach target (if current > target)
            if order_side == 'buy':
                # Calculate how much we can buy to reach target
                amount_to_reach_target = self.target_inventory - self.current_inventory
                if amount_to_reach_target <= Decimal('0'):
                    # We're already at or above target, shouldn't be buying
                    structlog.get_logger().info(
                        "Skipping buy order - already at or above target inventory",
                        exchange=exchange,
                        side=order_side,
                        original_size=float(size),
                        current_inventory=float(self.current_inventory),
                        target_inventory=float(self.target_inventory)
                    )
                    return
                elif size > amount_to_reach_target:
                    # Adjust size to exactly hit target
                    structlog.get_logger().info(
                        "Adjusting buy size to avoid exceeding target inventory",
                        exchange=exchange,
                        side=order_side,
                        original_size=float(size),
                        adjusted_size=float(amount_to_reach_target),
                        current_inventory=float(self.current_inventory),
                        target_inventory=float(self.target_inventory)
                    )
                    size = amount_to_reach_target
            elif order_side == 'sell':
                # Calculate how much we can sell to reach target
                amount_to_reach_target = self.current_inventory - self.target_inventory
                if amount_to_reach_target <= Decimal('0'):
                    # We're already at or below target, shouldn't be selling
                    structlog.get_logger().info(
                        "Skipping sell order - already at or below target inventory",
                        exchange=exchange,
                        side=order_side,
                        original_size=float(size),
                        current_inventory=float(self.current_inventory),
                        target_inventory=float(self.target_inventory)
                    )
                    return
                elif size > amount_to_reach_target:
                    # Adjust size to exactly hit target
                    structlog.get_logger().info(
                        "Adjusting sell size to avoid falling below target inventory",
                        exchange=exchange,
                        side=order_side,
                        original_size=float(size),
                        adjusted_size=float(amount_to_reach_target),
                        current_inventory=float(self.current_inventory),
                        target_inventory=float(self.target_inventory)
                    )
                    size = amount_to_reach_target
            
            # Final check - ensure size is still valid after adjustments
            if size <= Decimal('0.00001'):
                structlog.get_logger().info(
                    "Skipping order - size too small after target adjustment",
                    exchange=exchange,
                    side=order_side,
                    size=float(size),
                    current_inventory=float(self.current_inventory),
                    target_inventory=float(self.target_inventory)
                )
                return

            # Log order details before placement with clear rate information
            structlog.get_logger().info(
                "Placing order",
                exchange=exchange,
                side=order_side,
                size=float(size),
                price=float(price),
                level_price=float(level.max_min_price),
                base_hourly_rate=float(level.base_hourly_rate),
                exchange_coefficient=coefficient,
                adjusted_hourly_rate=float(level.current_hourly_rate),
                avg_coefficient=level.avg_coefficient_since_zone_entry,
                time_in_zone=level.time_in_zone
            )

            # DEBUG: Additional context for Hyperliquid debugging
            if exchange == 'hyperliquid_perp':
                self.logger.error(f"=== VOLUME STRATEGY HYPERLIQUID DEBUG ===")
                self.logger.error(f"Strategy instance: {self.instance_id}")
                self.logger.error(f"About to call _place_order for hyperliquid_perp")
                self.logger.error(f"  exchange: {exchange}")
                self.logger.error(f"  side: {order_side}")  
                self.logger.error(f"  amount: {float(size)}")
                self.logger.error(f"  price: {float(price)}")
                self.logger.error(f"  symbol (original): {self.symbol}")
                self.logger.error(f"  level price: {level.max_min_price}")
                connector = self.exchange_connectors.get(exchange)
                if connector:
                    self.logger.error(f"  connector available: {type(connector)}")
                    self.logger.error(f"  connector wallet: {getattr(connector, 'wallet_address', 'N/A')}")
                    self.logger.error(f"  connector private key len: {len(getattr(connector, 'private_key', '') or '')}")
                else:
                    self.logger.error(f"  NO CONNECTOR AVAILABLE!")
                self.logger.error(f"=== END VOLUME STRATEGY DEBUG ===")

            # Use base strategy's order placement method which handles all exchanges properly
            order_id = await self._place_order(
                exchange=exchange,
                side=order_side,
                amount=float(size),
                price=float(price)
            )

            if order_id:
                level.active_orders[exchange] = order_id
                level.order_placed_at[exchange] = datetime.now(timezone.utc)
                level.order_placed_price[exchange] = price
                self.orders_placed += 1
                # GLOBAL: Track this as the only active order for this exchange
                self.global_active_orders[exchange] = (level, order_id)

                structlog.get_logger().info(
                    "Order placed successfully",
                    exchange=exchange,
                    side=order_side,
                    size=float(size),
                    price=float(price),
                    order_id=order_id,
                    base_hourly_rate=float(level.base_hourly_rate),
                    exchange_coefficient=coefficient,
                    adjusted_hourly_rate=float(level.current_hourly_rate)
                )

                # ENFORCE: If there are ever two or more orders for this exchange, cancel all and wait
                active_orders_for_exchange = [oid for ex, (lvl, oid) in self.global_active_orders.items() if ex == exchange]
                if len(active_orders_for_exchange) > 1:
                    structlog.get_logger().warning(
                        "INVARIANT VIOLATION: More than one active order for exchange! Cancelling all orders for this exchange.",
                        exchange=exchange,
                        active_orders=active_orders_for_exchange
                    )
                    await self._cancel_exchange_orders(exchange)
                    # Wait until there are no active orders for this exchange
                    while any(ex == exchange for ex in self.global_active_orders):
                        await asyncio.sleep(0.05)

        except Exception as e:
            structlog.get_logger().error(
                "Error placing order",
                exchange=exchange,
                side=level.side,
                size=float(size) if isinstance(size, Decimal) else size,
                price=float(price) if isinstance(price, Decimal) else price,
                error=str(e)
            )

    async def _check_order_timeouts_and_drift(self) -> None:
        """Check all active orders for timeouts and price drift - each exchange independently and in parallel."""
        current_time = datetime.now(timezone.utc)

        # Create a single flat list of all (level, exchange) pairs that need checking
        check_tasks = []
        for level in self.bid_levels + self.offer_levels:
            for exchange in list(level.active_orders.keys()):
                check_tasks.append(self._check_exchange_order_timeout_drift(level, exchange, current_time))

        # Process all checks in parallel
        if check_tasks:
            await asyncio.gather(*check_tasks)

    async def _check_exchange_order_timeout_drift(self, level: PriceLevelEntry, exchange: str, current_time: datetime) -> None:
        """Check a specific order for timeout and drift."""
        try:
            # Check timeout first (simpler check)
            if exchange in level.order_placed_at:
                time_elapsed = (current_time - level.order_placed_at[exchange]).total_seconds()
                if time_elapsed > level.timeout_seconds:
                    self.logger.info(f"Order timeout on {exchange} for level {level}: {time_elapsed:.1f}s > {level.timeout_seconds}s")
                    await self._cancel_exchange_level_order(level, exchange)
                    return  # Skip to next exchange after cancelling

            # Check drift with fresh market data for THIS exchange
            if exchange in level.order_placed_price:
                # Get fresh orderbook for this specific exchange
                connector = self.exchange_connectors.get(exchange)
                if not connector:
                    self.logger.warning(f"No connector for {exchange}, skipping drift check")
                    return

                orderbook = await connector.get_orderbook(self.symbol)
                if not orderbook or not orderbook.get('bids') or not orderbook.get('asks'):
                    self.logger.warning(f"No valid orderbook for {exchange}, skipping drift check")
                    return

                # Calculate current midpoint for this exchange
                if len(orderbook['bids']) > 0 and len(orderbook['asks']) > 0:
                    exchange_best_bid = Decimal(str(orderbook['bids'][0][0]))
                    exchange_best_offer = Decimal(str(orderbook['asks'][0][0]))
                    current_price = (exchange_best_bid + exchange_best_offer) / 2

                    placed_price = level.order_placed_price[exchange]
                    drift_bps = abs(current_price - placed_price) / placed_price * 10000

                    if drift_bps > level.drift_bps:
                        self.logger.info(
                            f"Price drift exceeded on {exchange} for level {level}: {drift_bps:.2f} bps > {level.drift_bps} bps "
                            f"(current: {current_price}, placed: {placed_price})"
                        )
                        await self._cancel_exchange_level_order(level, exchange)

        except Exception as e:
            self.logger.error(f"Error checking order status for {exchange}, level {level}: {e}")

    async def _cancel_level_orders(self, level: PriceLevelEntry) -> None:
        """Cancel all orders for a price level in parallel."""
        exchange_tasks = []
        for exchange in list(level.active_orders.keys()):
            task = self._cancel_exchange_level_order(level, exchange)
            exchange_tasks.append(task)

        if exchange_tasks:
            await asyncio.gather(*exchange_tasks)

    async def _cancel_exchange_level_order(self, level: PriceLevelEntry, exchange: str) -> None:
        """Cancel a specific order for a level on an exchange, and update global tracking."""
        try:
            order_id = level.active_orders.get(exchange)
            if order_id:
                # First check if the order is still in base strategy's tracking
                if order_id not in self.active_orders:
                    self.logger.warning(f"Order {order_id} not in base strategy tracking - likely filled")
                    del level.active_orders[exchange]
                    level.order_placed_at.pop(exchange, None)
                    level.order_placed_price.pop(exchange, None)
                    # Remove from global tracking if present
                    if exchange in self.global_active_orders and self.global_active_orders[exchange][1] == order_id:
                        self.global_active_orders.pop(exchange, None)
                    return
                success = await self._cancel_order(order_id, exchange)
                if success:
                    self.logger.debug(f"Cancelled order {order_id} on {exchange}")
                    del level.active_orders[exchange]
                    level.order_placed_at.pop(exchange, None)
                    level.order_placed_price.pop(exchange, None)
                    # Remove from global tracking if present
                    if exchange in self.global_active_orders and self.global_active_orders[exchange][1] == order_id:
                        self.global_active_orders.pop(exchange, None)
                else:
                    self.logger.warning(f"Failed to cancel order {order_id} on {exchange} - might have been filled")
        except Exception as e:
            self.logger.error(f"Error cancelling order on {exchange}: {e}")
            if exchange in level.active_orders:
                del level.active_orders[exchange]
                level.order_placed_at.pop(exchange, None)
                level.order_placed_price.pop(exchange, None)
                # Remove from global tracking if present
                if exchange in self.global_active_orders and self.global_active_orders[exchange][1] == order_id:
                    self.global_active_orders.pop(exchange, None)

    async def _cancel_exchange_orders(self, exchange: str) -> None:
        """Cancel all orders on a specific exchange."""
        for level in self.bid_levels + self.offer_levels:
            if exchange in level.active_orders:
                await self._cancel_exchange_level_order(level, exchange)

    async def _cancel_all_orders(self) -> None:
        """Cancel all active orders."""
        # Process each level independently
        cancel_tasks = []
        for level in self.bid_levels + self.offer_levels:
            if level.active_orders:  # Only process levels with active orders
                cancel_tasks.append(self._cancel_level_orders(level))

        # Wait for all cancellations to complete
        if cancel_tasks:
            await asyncio.gather(*cancel_tasks)

    async def on_trade_confirmation(self, trade_data: Dict[str, Any]) -> None:
        """Handle trade confirmation from WebSocket and update global order tracking."""
        try:
            # Only count fills for orders that belong to this instance
            order_id = trade_data.get('order_id')
            if not order_id or not self._order_belongs_to_instance(order_id):
                return

            side = trade_data.get('side', '').lower()
            quantity = Decimal(str(trade_data.get('quantity', 0)))

            # Increment local per-instance execution counters
            if quantity and quantity > 0:
                if side == 'buy':
                    self.executed_bid_total += quantity
                elif side == 'sell':
                    self.executed_offer_total += quantity

            # Spawn task to call base class method for instance ownership bookkeeping
            asyncio.create_task(super().on_trade_confirmation(trade_data))

            # Update filled counter
            self.orders_filled += 1

            price = trade_data.get('price', 0)

            self.logger.info(
                f"✅ Volume-weighted trade confirmed: "
                f"{side.upper()} {quantity} {self.base_coin} @ {price} "
                f"(Order ID: {order_id})"
            )

            # Remove order from active tracking in this strategy's level maps
            for level in self.bid_levels + self.offer_levels:
                for exchange, active_order_id in list(level.active_orders.items()):
                    if active_order_id == order_id:
                        # Clean up tracking
                        del level.active_orders[exchange]
                        level.order_placed_at.pop(exchange, None)
                        level.order_placed_price.pop(exchange, None)
                        self.logger.debug(f"Removed filled order {order_id} from level tracking")

                        # Publish updated performance data after trade
                        await self._publish_performance_data()
                        return

        except Exception as e:
            self.logger.error(f"Error handling trade confirmation: {e}")

    async def _get_best_bid_offer(self) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        """Get aggregated best bid and offer with proper cross-currency conversion."""
        try:
            # Use enhanced orderbook manager's aggregated book which handles cross-currency conversion
            aggregated_book = self.enhanced_orderbook_manager.get_aggregated_orderbook(self.symbol)
            
            if aggregated_book:
                best_bid_level = aggregated_book.get_best_bid()
                best_ask_level = aggregated_book.get_best_ask()
                
                best_bid = best_bid_level.price if best_bid_level else None
                best_offer = best_ask_level.price if best_ask_level else None
                
                if best_bid and best_offer:
                    self.logger.debug(f"Aggregated book prices (with conversion): bid={best_bid}, offer={best_offer}")
                    return best_bid, best_offer
            
            # Fallback to manual aggregation only if enhanced manager fails
            self.logger.warning("Enhanced orderbook manager has no data, falling back to manual aggregation")
            return await self._get_fallback_aggregated_prices()
            
        except Exception as e:
            self.logger.error(f"Error getting aggregated prices: {e}")
            return await self._get_fallback_aggregated_prices()

    async def _get_fallback_aggregated_prices(self) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        """Fallback method for manual price aggregation (avoid currency mixing)."""
        best_bid = None
        best_offer = None

        # Only aggregate prices from exchanges with the same quote currency (USDT)
        usdt_exchanges = []
        for exchange in self.exchanges:
            if exchange != 'hyperliquid_perp':  # Skip hyperliquid since it uses USDC
                usdt_exchanges.append(exchange)

        for exchange in usdt_exchanges:
            try:
                exchange_bid, exchange_offer = await self._get_direct_exchange_prices(exchange)
                
                self.logger.debug(f"Exchange {exchange} USDT orderbook: bid={exchange_bid}, offer={exchange_offer}")

                if exchange_bid and exchange_offer:
                    if best_bid is None or exchange_bid > best_bid:
                        best_bid = exchange_bid
                    if best_offer is None or exchange_offer < best_offer:
                        best_offer = exchange_offer
            except Exception as e:
                self.logger.error(f"Error getting orderbook for {exchange}: {e}")
                continue

        self.logger.debug(f"Fallback aggregated USDT prices: bid={best_bid}, offer={best_offer}")
        return best_bid, best_offer

    async def _get_direct_exchange_prices(self, exchange: str) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        """Get prices directly from exchange connector with USDT conversion for non-USDT quotes."""
        try:
            connector = self.exchange_connectors.get(exchange)
            if not connector:
                self.logger.warning(f"No connector available for {exchange}")
                return None, None

            # Get orderbook from exchange directly using proper symbol normalization
            exchange_symbol = self._normalize_symbol_for_exchange(self.symbol, exchange)
            orderbook = await connector.get_orderbook(exchange_symbol)
            if not orderbook or not orderbook.get('bids') or not orderbook.get('asks'):
                self.logger.warning(f"No orderbook data from {exchange} connector")
                return None, None

            best_bid = Decimal(str(orderbook['bids'][0][0])) if len(orderbook['bids']) > 0 else None
            best_offer = Decimal(str(orderbook['asks'][0][0])) if len(orderbook['asks']) > 0 else None

            # CRITICAL FIX: For non-USDT quotes, convert to USDT before aggregating
            if exchange == 'hyperliquid_perp' and self.symbol == 'BERA/USDT':
                # Hyperliquid trades BERA/USDC but we need USDT prices for aggregation
                # USDC ≈ USDT (treat as 1:1 for now, should use actual conversion rate)
                usdc_to_usdt_rate = Decimal('1.0')  # Simplified 1:1 conversion
                
                if best_bid:
                    best_bid = best_bid * usdc_to_usdt_rate
                if best_offer:
                    best_offer = best_offer * usdc_to_usdt_rate
                    
                self.logger.info(f"Converted {exchange} USDC prices to USDT: bid={best_bid}, offer={best_offer}")
            else:
                self.logger.info(f"Direct exchange prices for {exchange}: bid={best_bid}, offer={best_offer}")
            
            return best_bid, best_offer

        except Exception as e:
            self.logger.error(f"Error getting direct exchange prices for {exchange}: {e}")
            return None, None

    async def _get_current_midpoint(self) -> Optional[Decimal]:
        """Get current midpoint price."""
        best_bid, best_offer = await self._get_best_bid_offer()
        if best_bid and best_offer:
            return (best_bid + best_offer) / 2
        return None

    def _is_data_fresh(self, exchange: str, max_age_seconds: float = 10.0) -> bool:
        """Check if orderbook data is fresh for a given exchange."""
        # Enhanced manager doesn't have is_data_fresh method, assume data is fresh if available
        aggregated_book = self.enhanced_orderbook_manager.get_aggregated_orderbook(self.symbol)
        return aggregated_book is not None

    def get_performance_stats(self) -> Dict[str, Any]:
        """Get strategy performance statistics."""
        stats = {
            'strategy': 'volume_weighted_top_of_book_delta',
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'orders_placed': self.orders_placed,
            'orders_filled': self.orders_filled,
            'orders_skipped_inventory_protection': self.orders_skipped_inventory_protection,
            'coefficient_updates': self.coefficient_updates_calculated,
            'current_inventory': float(self.current_inventory),
            'target_inventory': float(self.target_inventory),
            'excess_inventory': float(self.excess_inventory),
            'excess_inventory_percentage': self.excess_inventory_percentage,
            'target_excess_amount': float(self.target_excess_amount),
            'inventory_price': float(self.inventory_price) if self.inventory_price else None,
            'exchange_coefficients': {
                exchange: coeff for exchange, coeff in self.exchange_coefficients.items()
            },
            'coefficient_method': self.coefficient_method,
            'coefficient_bounds': {
                'min': self.min_coefficient,
                'max': self.max_coefficient
            },
            'sides': self.sides.value,
            'taker_check': self.taker_check,
            'accounting_method': self.accounting_method,
            'bid_levels': len(self.bid_levels),
            'offer_levels': len(self.offer_levels),
            'last_coefficient_update': self.last_coefficient_update.isoformat() if self.last_coefficient_update else None,
            'time_periods': self.time_periods,
            'available_exchanges': len(self.enhanced_orderbook_manager.exchanges),
            'orderbook_status': self.enhanced_orderbook_manager.get_statistics(),
            'target_inventory_source': self.target_inventory_source,
            'delta_change_threshold_pct': self.delta_change_threshold_pct,
            'delta_trade_fraction': self.delta_trade_fraction,
            'delta_target_sign': self.delta_target_sign,
            'prevent_unprofitable_trades': self.prevent_unprofitable_trades,
            'last_delta_target': float(self.last_delta_target) if self.last_delta_target is not None else None,
            'last_delta_updated_at': self.last_delta_updated_at.isoformat() if self.last_delta_updated_at else None,
        }

        # Add level-specific stats
        for i, level in enumerate(self.bid_levels):
            stats[f'bid_level_{i}_rate'] = float(level.current_hourly_rate)
            stats[f'bid_level_{i}_coefficient'] = level.coefficient_multiplier
            stats[f'bid_level_{i}_time_in_zone'] = level.time_in_zone
            stats[f'bid_level_{i}_zone_entry_snapshot'] = float(level.zone_entry_execution_snapshot) if level.zone_entry_execution_snapshot else None
            stats[f'bid_level_{i}_is_active'] = level.last_zone_entry is not None
            # Add per-exchange coefficients and rates
            stats[f'bid_level_{i}_exchange_coefficients'] = {ex: coef for ex, coef in level.exchange_coefficients.items()}
            stats[f'bid_level_{i}_exchange_rates'] = {ex: float(rate) for ex, rate in level.exchange_hourly_rates.items()}

        for i, level in enumerate(self.offer_levels):
            stats[f'offer_level_{i}_rate'] = float(level.current_hourly_rate)
            stats[f'offer_level_{i}_coefficient'] = level.coefficient_multiplier
            stats[f'offer_level_{i}_time_in_zone'] = level.time_in_zone
            stats[f'offer_level_{i}_zone_entry_snapshot'] = float(level.zone_entry_execution_snapshot) if level.zone_entry_execution_snapshot else None
            stats[f'offer_level_{i}_is_active'] = level.last_zone_entry is not None
            # Add per-exchange coefficients and rates
            stats[f'offer_level_{i}_exchange_coefficients'] = {ex: coef for ex, coef in level.exchange_coefficients.items()}
            stats[f'offer_level_{i}_exchange_rates'] = {ex: float(rate) for ex, rate in level.exchange_hourly_rates.items()}

        return stats

    async def _publish_performance_data(self) -> None:
        """Publish current performance data to Redis for API access."""
        try:
            if not self.redis_client:
                self.logger.debug("Redis client not available - performance data will not be published to frontend")
                return

            # Get current performance stats
            performance_data = self.get_performance_stats()

            # Add basic instance info
            full_data = {
                "instance_id": self.instance_id,
                "strategy": "volume_weighted_top_of_book_delta",  # Use the strategy key for frontend
                "symbol": self.symbol,
                "exchanges": self.exchanges,
                "config": self.config,
                "status": "running" if self.running else "stopped",
                "started_at": self.start_time.isoformat() if self.start_time else None,
                "last_updated": datetime.now(timezone.utc).isoformat(),
                "performance": performance_data
            }

            # Store in Redis with 60 second expiry
            await self.redis_client.setex(
                self.performance_key,
                60,  # Expire after 60 seconds
                json.dumps(full_data, default=str)
            )

        except Exception as e:
            self.logger.error(f"Error publishing performance data to Redis: {e}")

    def to_dict(self) -> Dict[str, Any]:
        """Convert strategy state to dictionary."""
        base_dict = {
            'instance_id': self.instance_id,
            'symbol': self.symbol,
            'exchanges': self.exchanges,
            'status': self.status.value,
            'config': self.config,
            'performance': self.get_performance_stats()
        }

        # Add enhanced orderbook manager status
        enhanced_dict = {
            'strategy_type': 'volume_weighted_top_of_book_delta',
            'orderbook_manager_status': self.enhanced_orderbook_manager.get_statistics()
        }

        base_dict.update(enhanced_dict)
        return base_dict

    async def _cancel_side_orders(self, side: str) -> None:
        """Cancel all orders for a specific side."""
        levels = self.bid_levels if side == "bid" else self.offer_levels
        cancel_tasks = []

        # Create tasks for each level to cancel orders independently
        for level in levels:
            if level.active_orders:  # Only process levels with active orders
                cancel_tasks.append(self._cancel_level_orders(level))

        # Wait for all cancellations to complete
        if cancel_tasks:
            await asyncio.gather(*cancel_tasks)
            cancelled_count = sum(len(level.active_orders) for level in levels)
            self.logger.debug(f"Cancelled {cancelled_count} {side} orders")

    async def _calculate_executed_since_runtime_start(self) -> Dict[str, Decimal]:
        """Calculate how much we've executed since this strategy instance started running."""
        try:
            from database import get_session
            from sqlalchemy import text

            if not self.runtime_start_time:
                return {"bid_executed": Decimal('0'), "offer_executed": Decimal('0')}

            async for session in get_session():
                try:
                    # Begin fresh transaction
                    await session.begin()

                    # Convert timezone-aware datetime to timezone-naive for database query
                    runtime_start_for_db = self.runtime_start_time
                    if runtime_start_for_db.tzinfo is not None:
                        runtime_start_for_db = runtime_start_for_db.astimezone(timezone.utc).replace(tzinfo=None)

                    self.logger.debug(f"🔍 Calculating executed amounts since strategy runtime start: {runtime_start_for_db}")

                    # Query all trades for this symbol since strategy runtime start
                    # Don't filter by strategy - track all trades like other strategies do
                    query = text("""
                        SELECT side, SUM(amount) as total_amount
                        FROM risk_trades
                        WHERE (
                            symbol = :symbol
                            OR symbol = :symbol_perp
                            OR symbol = :symbol_usdt
                            OR symbol = :symbol_usdc
                            OR symbol LIKE :symbol_pattern
                        )
                        AND timestamp >= :start_time
                        GROUP BY side
                    """)

                    # Prepare symbol variants (generalized for any pair)
                    base_coin, quote_coin = (self.base_coin, self.quote_coin)
                    base_symbol = f"{base_coin}/{quote_coin}"
                    symbol_perp = f"{base_symbol}-PERP"
                    symbol_colon_current = f"{base_coin}/{quote_coin}:{quote_coin}"
                    symbol_colon_usdt = f"{base_coin}/USDT:USDT"
                    symbol_colon_usdc = f"{base_coin}/USDC:USDC"
                    symbol_pattern = f"{base_coin}/%"
                    symbol_colon_pattern = f"{base_coin}/%:%"

                    result = await session.execute(
                        query,
                        {
                            "symbol": base_symbol,
                            "symbol_perp": symbol_perp,
                            "symbol_usdt": symbol_colon_usdt,
                            "symbol_usdc": symbol_colon_usdc,
                            "symbol_current_colon": symbol_colon_current,
                            "symbol_pattern": symbol_pattern,
                            "symbol_colon_pattern": symbol_colon_pattern,
                            "start_time": runtime_start_for_db
                        }
                    )
                    rows = result.fetchall()

                    bid_executed = Decimal('0')
                    offer_executed = Decimal('0')

                    for row in rows:
                        amount = Decimal(str(row.total_amount))
                        if row.side.lower() == 'buy':
                            bid_executed = amount
                        else:  # sell
                            offer_executed = amount

                    # Commit the read-only transaction
                    await session.commit()

                    self.logger.debug(
                        f"📊 Executed since runtime start: bid={bid_executed}, offer={offer_executed}"
                    )

                    return {"bid_executed": bid_executed, "offer_executed": offer_executed}

                except Exception as e:
                    await session.rollback()
                    self.logger.error(f"Error in executed amounts calculation: {e}")
                    raise e
                finally:
                    await session.close()

        except Exception as e:
            self.logger.error(f"Error calculating executed amounts since runtime start: {e}")
            return {"bid_executed": Decimal('0'), "offer_executed": Decimal('0')}

    async def _exchange_monitor_loop(self, exchange: str) -> None:
        """
        Independent monitoring loop for a specific exchange.
        Checks for order timeouts and price drift conditions and immediately replaces orders.
        """
        self.logger.info(f"Starting exchange monitor loop for {exchange}")

        while self.running:
            try:
                current_time = datetime.now(timezone.utc)

                # First check if we have any active orders for this exchange before fetching orderbook
                has_active_orders = False
                active_order_info = None
                for level in self.bid_levels + self.offer_levels:
                    if exchange in level.active_orders:
                        has_active_orders = True
                        order_id = level.active_orders[exchange]
                        placed_at = level.order_placed_at.get(exchange)
                        active_order_info = f"order_id={order_id}, placed_at={placed_at}, side={level.side}"
                        break

                if not has_active_orders:
                    # No active orders for this exchange, sleep briefly and continue
                    await asyncio.sleep(0.05)
                    continue
                
                # DEBUG: Log that we found active orders
                self.logger.debug(f"Monitor loop [{exchange}]: Found active order - {active_order_info}")

                # Get fresh orderbook for this exchange using enhanced manager
                exchange_best_bid, exchange_best_offer = await self.enhanced_orderbook_manager.get_best_bid_ask_direct(exchange, self.symbol)
                
                # CRITICAL FIX: If enhanced manager has no data, fallback to direct exchange prices
                # Without this, the timeout check would NEVER run when enhanced manager has no data!
                if not exchange_best_bid or not exchange_best_offer:
                    try:
                        exchange_best_bid, exchange_best_offer = await self._get_direct_exchange_prices(exchange)
                    except Exception as e:
                        self.logger.warning(f"Monitor loop [{exchange}]: Failed to get direct prices: {e}")
                
                # If we STILL don't have orderbook data, we can't check price drift but we CAN still check timeout!
                # Don't skip the timeout check just because we don't have orderbook data
                has_orderbook_data = exchange_best_bid and exchange_best_offer
                if has_orderbook_data:
                    current_midpoint = (exchange_best_bid + exchange_best_offer) / 2
                else:
                    current_midpoint = None
                    self.logger.debug(f"Monitor loop [{exchange}]: No orderbook data, will skip price checks but still check timeout")

                # Check all active orders for this exchange across all levels
                for level in self.bid_levels + self.offer_levels:
                    if exchange in level.active_orders:
                        order_id = level.active_orders[exchange]
                        
                        # Check if order is still at the top of the book (only if we have orderbook data)
                        if has_orderbook_data:
                            if not await self._is_order_at_best_price(level, exchange, exchange_best_bid, exchange_best_offer):
                                self.logger.info(f"Order on {exchange} for {level.side} level no longer at top of book, replacing")
                                await self._cancel_and_replace_order(level, exchange)
                                continue  # Skip other checks after cancelling

                        # CRITICAL: Check timeout ALWAYS, even without orderbook data
                        if exchange in level.order_placed_at:
                            time_elapsed = (current_time - level.order_placed_at[exchange]).total_seconds()
                            self.logger.debug(f"Monitor [{exchange}]: order {order_id} age={time_elapsed:.1f}s, timeout={level.timeout_seconds}s")
                            if time_elapsed > level.timeout_seconds:
                                self.logger.info(f"⏰ Order timeout on {exchange} for {level.side} level: {time_elapsed:.1f}s > {level.timeout_seconds}s, cancelling and replacing")
                                await self._cancel_and_replace_order(level, exchange)
                                continue  # Skip drift check after cancelling
                        else:
                            self.logger.warning(f"Monitor [{exchange}]: order {order_id} has NO order_placed_at timestamp!")

                        # Check drift with fresh market data (only if we have orderbook data)
                        if has_orderbook_data and current_midpoint and exchange in level.order_placed_price:
                            placed_price = level.order_placed_price[exchange]
                            drift_bps = abs(current_midpoint - placed_price) / placed_price * 10000

                            if drift_bps > level.drift_bps:
                                self.logger.info(
                                    f"Price drift exceeded on {exchange} for level {level}: {drift_bps:.2f} bps > {level.drift_bps} bps "
                                    f"(current: {current_midpoint}, placed: {placed_price})"
                                )
                                await self._cancel_and_replace_order(level, exchange)

                # Sleep briefly before next check - use a short interval for responsive monitoring
                await asyncio.sleep(0.05)  # 50ms check interval for low latency

            except Exception as e:
                self.logger.error(f"Error in exchange monitor loop for {exchange}: {e}")
                await asyncio.sleep(0.5)  # Brief sleep on error

    async def _is_order_at_best_price(self, level: PriceLevelEntry, exchange: str, best_bid: Decimal, best_offer: Decimal) -> bool:
        """
        Check if an order is still at the best price (top of book).

        Args:
            level: The price level entry
            exchange: The exchange to check
            best_bid: Current best bid price
            best_offer: Current best offer price

        Returns:
            True if the order is still at the best price, False otherwise
        """
        if exchange not in level.order_placed_price:
            self.logger.debug(f"No placed price found for {exchange} in {level.side} level")
            return False

        placed_price = level.order_placed_price[exchange]

        # For bid orders, check if our price is still at the best bid
        if level.side == "bid":
            is_at_best = placed_price >= best_bid
            if not is_at_best:
                structlog.get_logger().debug(
                    "Order no longer at top of book",
                    exchange=exchange,
                    side=level.side,
                    placed_price=float(placed_price),
                    current_best_bid=float(best_bid),
                    price_difference=float(best_bid - placed_price)
                )
            return is_at_best

        # For offer orders, check if our price is still at the best offer
        else:  # offer
            is_at_best = placed_price <= best_offer
            if not is_at_best:
                structlog.get_logger().debug(
                    "Order no longer at top of book",
                    exchange=exchange,
                    side=level.side,
                    placed_price=float(placed_price),
                    current_best_offer=float(best_offer),
                    price_difference=float(placed_price - best_offer)
                )
            return is_at_best

    async def _cancel_and_replace_order(self, level: PriceLevelEntry, exchange: str) -> None:
        """
        Cancel an order and immediately place a new one if conditions are still valid.
        This ensures minimal delay between cancellation and replacement.
        """
        try:
            start_time = datetime.now(timezone.utc)

            # Log the start of the replacement process
            structlog.get_logger().info(
                "Starting order cancel and replace",
                exchange=exchange,
                side=level.side,
                price_level=float(level.max_min_price),
                order_id=level.active_orders.get(exchange, "unknown")
            )

            # Cancel the existing order
            await self._cancel_exchange_level_order(level, exchange)

            cancel_time = datetime.now(timezone.utc)
            cancel_duration_ms = (cancel_time - start_time).total_seconds() * 1000

            # Check if we should still be quoting
            if not self._should_quote():
                structlog.get_logger().info(
                    "Skipping replacement - should not quote",
                    exchange=exchange,
                    side=level.side,
                    cancel_duration_ms=cancel_duration_ms
                )
                return

            # Check if this side should be active based on inventory
            if (level.side == "bid" and self.excess_inventory >= 0) or (level.side == "offer" and self.excess_inventory <= 0):
                structlog.get_logger().info(
                    "Skipping replacement - wrong inventory direction",
                    exchange=exchange,
                    side=level.side,
                    excess_inventory=float(self.excess_inventory),
                    cancel_duration_ms=cancel_duration_ms
                )
                return

            # Get current market data for this exchange using enhanced manager
            exchange_best_bid, exchange_best_offer = await self.enhanced_orderbook_manager.get_best_bid_ask_direct(exchange, self.symbol)
            if not exchange_best_bid or not exchange_best_offer:
                structlog.get_logger().warning(
                    "Skipping replacement - no market data from Redis",
                    exchange=exchange,
                    side=level.side,
                    cancel_duration_ms=cancel_duration_ms
                )
                return

                            # Commented out data freshness check to improve performance
                # if not self._is_data_fresh(exchange, max_age_seconds=10.0):
                #     structlog.get_logger().warning(
                #         "Skipping replacement - stale market data",
                #         exchange=exchange,
                #         side=level.side,
                #         cancel_duration_ms=cancel_duration_ms
                #     )
                #     return

            current_midpoint = (exchange_best_bid + exchange_best_offer) / 2

            # Check if price is in valid zone for this level
            in_zone = False
            if level.side == "bid":
                in_zone = current_midpoint <= level.max_min_price
            else:  # offer
                in_zone = current_midpoint >= level.max_min_price

            if not in_zone:
                structlog.get_logger().info(
                    "Skipping replacement - price not in zone",
                    exchange=exchange,
                    side=level.side,
                    price_level=float(level.max_min_price),
                    current_midpoint=float(current_midpoint),
                    cancel_duration_ms=cancel_duration_ms
                )
                return

            # Calculate order size
            size = await self._calculate_order_size(level, exchange)
            if size <= 0:
                structlog.get_logger().info(
                    "Skipping replacement - zero size",
                    exchange=exchange,
                    side=level.side,
                    cancel_duration_ms=cancel_duration_ms
                )
                return

            # Log before placement
            structlog.get_logger().info(
                "Placing replacement order",
                exchange=exchange,
                side=level.side,
                size=float(size),
                best_bid=float(exchange_best_bid),
                best_offer=float(exchange_best_offer),
                cancel_duration_ms=cancel_duration_ms
            )

            # Process the order placement - use global best prices as expected by the method
            await self._process_exchange_order(exchange, level, exchange_best_bid, exchange_best_offer)

            # Calculate total replacement time
            end_time = datetime.now(timezone.utc)
            total_duration_ms = (end_time - start_time).total_seconds() * 1000

            structlog.get_logger().info(
                "Completed order replacement",
                exchange=exchange,
                side=level.side,
                price_level=float(level.max_min_price),
                cancel_duration_ms=cancel_duration_ms,
                total_duration_ms=total_duration_ms
            )

        except Exception as e:
            self.logger.error(f"Error in cancel and replace for {exchange}, level {level}: {e}")

    def _extract_quote_from_symbol(self, symbol: str) -> Optional[str]:
        """Extract the quote currency from a trade symbol string."""
        try:
            if not symbol:
                return None
            # Examples:
            #  - BASE/USDT -> quote USDT
            #  - BASE/USDT:USDT -> quote USDT (after colon)
            #  - BASE/USDC:USDC -> quote USDC
            #  - BASE/USDT-PERP -> quote USDT
            if ':' in symbol:
                after_colon = symbol.split(':')[1]
                return after_colon
            if '/' in symbol:
                rhs = symbol.split('/')[1]
                # Handle -PERP suffix
                if '-' in rhs:
                    rhs = rhs.split('-')[0]
                return rhs
            return None
        except Exception:
            return None

    def _normalize_quote_amount(self, amount: Decimal, trade_symbol: Optional[str]) -> Decimal:
        """Normalize a quote-denominated amount to the strategy's quote coin.
        Currently treats USDT and USDC at parity (1:1)."""
        try:
            trade_quote = self._extract_quote_from_symbol(trade_symbol) if trade_symbol else None
            target_quote = (self.quote_coin or '').upper()
            if not trade_quote or not target_quote:
                return amount
            trade_quote = trade_quote.upper()
            if trade_quote == target_quote:
                return amount
            # Treat USDT and USDC as parity for normalization
            stable_set = {"USDT", "USDC"}
            if trade_quote in stable_set and target_quote in stable_set:
                return amount
            # Unknown conversion: return original and log once
            try:
                structlog.get_logger().warning(
                    "Quote normalization skipped (no converter)",
                    trade_symbol=trade_symbol,
                    trade_quote=trade_quote,
                    target_quote=target_quote,
                    amount=float(amount)
                )
            except Exception:
                pass
            return amount
        except Exception:
            return amount

    async def _calculate_inventory_price_background(self) -> None:
        """Run _calculate_inventory_price asynchronously in a thread-safe way."""
        try:
            def runner():
                import asyncio as _asyncio
                _asyncio.run(self._calculate_inventory_price())
            await asyncio.to_thread(runner)
        except Exception as e:
            self.logger.error(f"Background inventory price calc failed: {e}")

    def _get_candidate_redis_symbols(self) -> List[str]:
        """Return candidate Redis symbols for this strategy symbol across spot/perp variants."""
        try:
            if '/' not in self.symbol:
                return [self.symbol]
            base, quote = self.symbol.split('/')
            candidates = [self.symbol]
            # Perp-style colon symbol (e.g., BASE/QUOTE:QUOTE)
            candidates.append(f"{base}/{quote}:{quote}")
            # Hyperliquid/USDC-style colon symbol if primary quote is USDT
            if quote.upper() == 'USDT':
                candidates.append(f"{base}/USDC:USDC")
            return candidates
        except Exception:
            return [self.symbol]

    async def _scan_redis_keys(self, pattern: str) -> List[str]:
        """Scan Redis for keys matching pattern, returning decoded strings."""
        keys: List[str] = []
        try:
            cursor = 0
            while True:
                cursor, results = await self.redis_client.scan(cursor=cursor, match=pattern, count=200)
                for k in results or []:
                    if isinstance(k, (bytes, bytearray)):
                        keys.append(k.decode())
                    else:
                        keys.append(k)
                if cursor == 0:
                    break
        except Exception as e:
            self.logger.error(f"Error scanning Redis with pattern {pattern}: {e}")
        return keys

    async def _fetch_position_from_risk_redis(self) -> Optional[Dict[str, Decimal]]:
        """Aggregate position size and entry price from risk_v2 Redis across all exchanges for this symbol.
        Returns dict with keys: size (signed), entry_price (Decimal or None), cost_base (absolute), method (str).
        """
        try:
            if not self.redis_client:
                return None

            # Try requested method first, then fall back to AVERAGE_COST, FIFO, LIFO
            primary_method = (self.accounting_method or 'AVERAGE_COST').upper()
            method_candidates = []
            for m in [primary_method, 'AVERAGE_COST', 'FIFO', 'LIFO']:
                if m not in method_candidates:
                    method_candidates.append(m)
            candidates = self._get_candidate_redis_symbols()

            for method in method_candidates:
                # Collect all matching keys across candidates
                all_keys: List[str] = []
                for sym in candidates:
                    pattern = f"position:*:{sym}:{method}"
                    keys = await self._scan_redis_keys(pattern)
                    all_keys.extend(keys)

                if not all_keys:
                    continue

                # Pipeline fetch fields for efficiency
                pipe = self.redis_client.pipeline()
                for key in all_keys:
                    pipe.hget(key, 'size')
                    pipe.hget(key, 'p2')
                    pipe.hget(key, 'avg_price')
                    pipe.hget(key, 'entry_price')
                results = await pipe.execute()

                total_size = Decimal('0')
                total_p2 = Decimal('0')

                # Parse results in chunks of 4 per key
                for i in range(0, len(results), 4):
                    raw_size, raw_p2, raw_avg_price, raw_entry_price = results[i:i+4]

                    def _to_decimal(val) -> Optional[Decimal]:
                        if val is None:
                            return None
                        try:
                            if isinstance(val, (bytes, bytearray)):
                                return Decimal(val.decode())
                            return Decimal(str(val))
                        except Exception:
                            return None

                    size_dec = _to_decimal(raw_size) or Decimal('0')
                    p2_dec = _to_decimal(raw_p2)
                    avg_price_dec = _to_decimal(raw_avg_price)
                    entry_price_dec = _to_decimal(raw_entry_price)

                    # Prefer p2 if present; otherwise derive from size * avg_price/entry_price
                    if p2_dec is None:
                        if avg_price_dec is not None:
                            p2_dec = -(size_dec * avg_price_dec)
                        elif entry_price_dec is not None:
                            p2_dec = -(size_dec * entry_price_dec)
                        else:
                            p2_dec = Decimal('0')

                    total_size += size_dec
                    total_p2 += p2_dec

                # If we found any position, return it
                if abs(total_size) >= Decimal('0.00000001'):
                    entry_price = abs(total_p2) / abs(total_size)
                    cost_base = abs(total_p2)
                    return {
                        'size': total_size,
                        'entry_price': entry_price,
                        'cost_base': cost_base,
                        'method': method
                    }
            # No data across any method
            return None
        except Exception as e:
            self.logger.error(f"Error fetching position from risk Redis: {e}")
            return None

    async def _fetch_position_from_risk_api(self) -> Optional[Dict[str, Decimal]]:
        """Fetch aggregated position from Risk V2 HTTP API for this symbol (AVERAGE_COST)."""
        try:
            # Build URL with symbol filter (API filters by substring match)
            symbol_q = urllib.parse.quote(self.symbol)
            url = f"{self.risk_api_url}/api/v2/risk/positions?symbol={symbol_q}"
            timeout = aiohttp.ClientTimeout(total=1.5)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.json()
                    positions = (data or {}).get('data', {}).get('positions', [])
                    if not positions:
                        return None
                    total_size = Decimal('0')
                    total_p2 = Decimal('0')
                    for pos in positions:
                        try:
                            size = Decimal(str(pos.get('size', 0)))
                            side = (pos.get('side') or '').lower()
                            signed = size if side == 'long' else (-size if side == 'short' else size)
                            entry = pos.get('entry_price')
                            if entry is None:
                                entry = pos.get('avg_price')
                            entry_dec = Decimal(str(entry)) if entry is not None else None
                            total_size += signed
                            if entry_dec is not None:
                                total_p2 += -(signed * entry_dec)
                        except Exception:
                            continue
                    if abs(total_size) < Decimal('0.00000001'):
                        return {'size': Decimal('0'), 'entry_price': None, 'cost_base': Decimal('0')}
                    entry_price = abs(total_p2) / abs(total_size)
                    return {'size': total_size, 'entry_price': entry_price, 'cost_base': abs(total_p2)}
        except Exception:
            return None

    async def _risk_ws_loop(self) -> None:
        """Subscribe to Risk V2 WS and keep local inventory in sync from pushed positions."""
        while self.running:
            try:
                if not self._ws_session:
                    self._ws_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
                self.logger.info(f"Connecting to Risk V2 WS at {self.risk_ws_url}")
                async with self._ws_session.ws_connect(self.risk_ws_url) as ws:
                    self._ws_connected = True
                    # Set accounting method for this connection
                    try:
                        method = (self.accounting_method or 'AVERAGE_COST').upper()
                        await ws.send_str(json.dumps({"type": "set_method", "method": method}))
                    except Exception:
                        pass
                    self._ws_backoff_seconds = 1.0
                    while self.running:
                        try:
                            msg = await ws.receive(timeout=30)
                        except asyncio.TimeoutError:
                            try:
                                await ws.send_str("ping")
                            except Exception:
                                break
                            continue
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                payload = json.loads(msg.data)
                                await self._handle_risk_ws_message(payload)
                            except Exception:
                                pass
                        elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSING, aiohttp.WSMsgType.CLOSED):
                            break
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            break
            except Exception as e:
                self.logger.warning(f"WS subscriber error: {e}")
            finally:
                self._ws_connected = False
            # Backoff before reconnect
            await asyncio.sleep(self._ws_backoff_seconds)
            self._ws_backoff_seconds = min(self._ws_backoff_seconds * 2.0, 10.0)

    def _ws_inventory_is_fresh(self) -> bool:
        """Return True if WS inventory has updated within freshness window."""
        try:
            if not self._ws_connected or self._last_ws_inventory_update is None:
                return False
            now_guard = datetime.now(timezone.utc)
            age = (now_guard - self._last_ws_inventory_update).total_seconds()
            return age <= self.ws_fresh_window_seconds
        except Exception:
            return False

    async def _ensure_ws_and_wait_for_fresh(self, timeout_seconds: float = 30.0) -> bool:
        """Ensure WS subscriber is running and wait up to timeout for fresh inventory."""
        try:
            # Ensure WS task is running
            if (self._ws_task is None) or self._ws_task.done():
                try:
                    # Close prior session if any
                    if self._ws_session:
                        try:
                            await self._ws_session.close()
                        except Exception:
                            pass
                        self._ws_session = None
                    self._ws_task = asyncio.create_task(self._risk_ws_loop())
                except Exception as e:
                    self.logger.warning(f"Failed to start WS loop: {e}")
            # Wait for freshness
            deadline = time.time() + max(0.0, float(timeout_seconds))
            while time.time() < deadline:
                if self._ws_inventory_is_fresh():
                    return True
                await asyncio.sleep(0.1)
            return self._ws_inventory_is_fresh()
        except Exception as e:
            self.logger.warning(f"WS wait failed: {e}")
            return False

    async def _handle_risk_ws_message(self, payload: Dict[str, Any]) -> None:
        """Process WS message and update local inventory from pushed positions."""
        try:
            msg_type = payload.get('type')
            positions = None
            metrics = None
            if msg_type == 'update':
                data = (payload.get('data') or {})
                positions = data.get('positions')
                metrics = data.get('metrics')
            elif msg_type in ('positions_update', 'pnl_update'):
                positions = payload.get('positions') or ((payload.get('data') or {}).get('positions'))
                metrics = payload.get('metrics') or ((payload.get('data') or {}).get('metrics'))

            # Brief frame summary
            try:
                self.logger.info(
                    f"WS frame received: type={msg_type}, has_metrics={bool(metrics)}, has_positions={bool(positions)}"
                )
            except Exception:
                pass

            # Prefer frontend totals from metrics.positions_by_symbol when available
            size_from_metrics = None
            if metrics and isinstance(metrics, dict):
                agg_m = self._aggregate_inventory_from_metrics(metrics)
                if agg_m and agg_m.get('size') is not None:
                    size_from_metrics = agg_m['size']

            # Use positions list to refresh entry_price/cost, but don't override metrics-based size
            entry_price = None
            cost_base = None
            size_from_positions = None
            if positions:
                agg_p = self._aggregate_inventory_from_positions(positions)
                if agg_p:
                    entry_price = agg_p.get('entry_price')
                    cost_base = agg_p.get('cost_base')
                    if agg_p.get('size') is not None:
                        size_from_positions = agg_p['size']
                    # If metrics wasn't present, we can fall back to size from positions
                    if size_from_metrics is None and size_from_positions is not None:
                        size_from_metrics = size_from_positions

            # Log aggregation summary before applying
            try:
                self.logger.info(
                    f"WS inventory aggregate: metrics_size={size_from_metrics}, positions_size={size_from_positions}, entry_price={entry_price}"
                )
            except Exception:
                pass

            # CRITICAL FIX: If we have no size data but received a valid position WS frame,
            # treat this as zero inventory (no positions = zero position).
            # NOTE: balance_update messages should NOT update inventory - they don't contain position data.
            if size_from_metrics is None:
                # Valid position-related message types that indicate we should treat missing data as zero
                if msg_type in ('update', 'positions_update', 'pnl_update', 'initialization_progress'):
                    self.logger.info(f"No position data in WS frame (type={msg_type}), treating as zero inventory")
                    size_from_metrics = Decimal('0')
                else:
                    # Unknown message type, don't assume anything
                    return

            # Apply updates
            self.current_inventory = size_from_metrics
            self.position_size = size_from_metrics
            
            # Handle inventory price based on position size:
            # - If size is 0 (or near zero), clear inventory price (no position = no entry price)
            # - If size is non-zero (positive or negative), use the entry price
            if abs(size_from_metrics) < Decimal('0.00000001'):
                # Zero position - clear inventory price
                self.inventory_price = None
                self.position_cost = Decimal('0')
                self.logger.info("Zero inventory - clearing inventory price")
            elif entry_price is not None:
                # Non-zero position with entry price - use it (works for both positive and negative positions)
                self.inventory_price = entry_price
                self.last_inventory_price_calc_at = datetime.now(timezone.utc)
                if cost_base is not None:
                    self.position_cost = cost_base
                self.logger.info(f"Updated inventory price to {entry_price} for position size {size_from_metrics}")
            
            self._last_ws_inventory_update = datetime.now(timezone.utc)
        except Exception:
            pass



    def _aggregate_inventory_from_positions(self, positions: List[Dict[str, Any]]) -> Optional[Dict[str, Decimal]]:
        """Aggregate signed size and derive entry price from avg across ALL quotes for our base coin."""
        try:
            base_coin = (self.base_coin or '').upper()
            if not base_coin:
                return None
            total_size = Decimal('0')
            total_p2 = Decimal('0')
            matched = False
            matched_count = 0
            for pos in positions:
                sym = (pos.get('symbol') or '')
                if not sym or '/' not in sym:
                    continue
                if sym.split('/')[0].upper() != base_coin:
                    continue
                size = Decimal(str(pos.get('size', 0)))
                side = (pos.get('side') or '').lower()
                signed = size if side == 'long' else (-size if side == 'short' else size)
                avg = pos.get('entry_price')
                if avg is None:
                    avg = pos.get('avg_price')
                avg_dec = Decimal(str(avg)) if avg is not None else None
                total_size += signed
                if avg_dec is not None:
                    total_p2 += -(signed * avg_dec)
                matched = True
                matched_count += 1
                try:
                    self.logger.debug(f"Positions agg [{sym}]: size={size}, side={side}, signed={signed}, avg={avg_dec}")
                except Exception:
                    pass
            # IMPORTANT: If no positions matched for our base coin, treat as zero position (not "no data")
            # This allows the strategy to work when there are no trades/positions yet
            if not matched:
                try:
                    self.logger.info(f"Positions agg result: base={base_coin}, no positions matched - treating as zero position")
                except Exception:
                    pass
                return {'size': Decimal('0'), 'entry_price': None, 'cost_base': Decimal('0')}
            if abs(total_size) < Decimal('0.00000001'):
                try:
                    self.logger.info(f"Positions agg result: base={base_coin}, symbols_matched={matched_count}, total_size=0")
                except Exception:
                    pass
                return {'size': total_size, 'entry_price': None, 'cost_base': Decimal('0')}
            entry_price = abs(total_p2) / abs(total_size)
            try:
                self.logger.info(f"Positions agg result: base={base_coin}, symbols_matched={matched_count}, total_size={total_size}, entry={entry_price}")
            except Exception:
                pass
            return {'size': total_size, 'entry_price': entry_price, 'cost_base': abs(total_p2)}
        except Exception:
            return None

    def _aggregate_inventory_from_metrics(self, metrics: Dict[str, Any]) -> Optional[Dict[str, Decimal]]:
        """Aggregate signed size using the frontend's metrics across ALL quotes for our base coin.
        Prefers a signed/net field if present; otherwise derives sign from side.
        Returns {'size': Decimal} when available.
        """
        try:
            pbs = metrics.get('positions_by_symbol')
            if not pbs or not isinstance(pbs, dict):
                return None
            base_coin = (self.base_coin or '').upper()
            if not base_coin:
                return None
            total_size = Decimal('0')
            matched = False
            matched_count = 0
            for sym, data in pbs.items():
                if not sym or '/' not in sym:
                    continue
                if sym.split('/')[0].upper() != base_coin:
                    continue
                data = data or {}
                net_val = data.get('net_size') or data.get('signed_total_size')
                if net_val is not None:
                    contrib = Decimal(str(net_val))
                    total_size += contrib
                    matched = True
                    matched_count += 1
                    try:
                        self.logger.debug(f"Metrics agg [{sym}]: using net={contrib}")
                    except Exception:
                        pass
                    continue
                size_val = data.get('total_size')
                if size_val is None:
                    continue
                side_str = (data.get('side') or '').lower()
                sign = Decimal('1') if side_str == 'long' else (Decimal('-1') if side_str == 'short' else Decimal('0'))
                contrib = Decimal(str(size_val)) * sign
                total_size += contrib
                matched = True
                matched_count += 1
                try:
                    self.logger.debug(f"Metrics agg [{sym}]: total={size_val}, side={side_str}, contrib={contrib}")
                except Exception:
                    pass
            # IMPORTANT: If no positions matched for our base coin, treat as zero position (not "no data")
            # This allows the strategy to work when there are no trades/positions yet
            if not matched:
                try:
                    self.logger.info(f"Metrics agg result: base={base_coin}, no positions matched - treating as zero position")
                except Exception:
                    pass
                return {'size': Decimal('0')}  # Zero position, not None
            try:
                self.logger.info(f"Metrics agg result: base={base_coin}, symbols_matched={matched_count}, total_size={total_size}")
            except Exception:
                pass
            return {'size': total_size}
        except Exception:
            return None