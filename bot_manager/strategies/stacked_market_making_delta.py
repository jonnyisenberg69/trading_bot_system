"""
Stacked Market Making Strategy - Implementation of the NEW_BOT.txt strategy requirements.

This strategy implements the advanced market making approach described in NEW_BOT.txt:
- Dual line types: Top of Book (TOB) lines and Passive lines
- Inventory-based coefficient adjustments
- Volume coefficient modulation
- Smart pricing selection between aggregated and component venues
- Cross-currency conversion support
"""

import asyncio
import time
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Optional, Any, Set, Tuple
from collections import deque
import logging
import redis.asyncio as redis
import aiohttp
import json

from .base_strategy import BaseStrategy
from .inventory_manager import InventoryManager, InventoryConfig, InventoryPriceMethod
from market_data.multi_symbol_market_data_service import (
    MultiSymbolMarketDataService, SymbolConfig
)
from market_data.multi_reference_pricing_engine import (
    MultiReferencePricingEngine, PricingContext, PricingMethod
)

# Import existing proven coefficient technology
from market_data_collection.moving_averages import MovingAverageCalculator, MovingAverageConfig
from market_data_collection.exchange_coefficients import SimpleExchangeCoefficientCalculator
from market_data_collection.database import TradeDataManager

logger = logging.getLogger(__name__)


class LineType(str, Enum):
    """Types of market making lines."""
    TOP_OF_BOOK = "tob"  # Top of book lines - local TOB pricing
    PASSIVE = "passive"   # Passive lines - aggregated book pricing


class PricingSourceType(str, Enum):
    """Pricing source options for smart pricing."""
    AGGREGATED = "aggregated"  # Use combined book from all exchanges
    COMPONENT_VENUES = "component"  # Use selected component venues only
    LIQUIDITY_WEIGHTED = "liquidity_weighted"  # Weight by per-venue depth share
    SINGLE_VENUE = "single_venue"  # Use single venue (e.g., "Gate only")


@dataclass
class TOBLineConfig:
    """Configuration for Top of Book line."""
    line_id: int
    min_price: Optional[Decimal] = None  # Theoretical minimum price
    max_price: Optional[Decimal] = None  # Theoretical maximum price
    hourly_quantity: Decimal = Decimal('100')  # Hourly quantity (quote or base)
    quantity_currency: str = "base"  # 'base' or 'quote'
    sides: str = "both"  # 'both', 'bid', 'offer'
    spread_from_inventory: bool = True  # Whether to use spread from inventory price
    spread_bps: Decimal = Decimal('50')  # Base spread in bps
    drift_bps: Decimal = Decimal('30')  # Drift threshold in bps
    timeout_seconds: int = 30  # Order timeout
    coefficient_method: str = "inventory"  # 'inventory' or 'volume' or 'both'
    max_coefficient: Decimal = Decimal('2.0')
    min_coefficient: Decimal = Decimal('0.1')


@dataclass
class PassiveLineConfig:
    """Configuration for Passive line."""
    line_id: int
    mid_spread_bps: Decimal  # Base spread from mid
    quantity: Decimal  # Base quantity amount
    sides: str = "both"  # 'both', 'bid', 'offer'
    spread_coefficient_method: str = "inventory"  # 'inventory', 'volume', 'both'
    quantity_coefficient_method: str = "volume"  # 'inventory', 'volume', 'both', 'none'
    min_spread_bps: Decimal = Decimal('10')
    max_spread_bps: Decimal = Decimal('200')
    drift_bps: Decimal = Decimal('20')
    timeout_seconds: int = 60
    randomization_factor: Decimal = Decimal('0.05')  # 5% quantity randomization


@dataclass
class StackedMarketMakingConfig:
    """Complete configuration for stacked market making strategy."""
    # Global settings
    base_coin: str
    quote_currencies: List[str]  # e.g., ['USDT', 'BTC', 'ETH']
    exchanges: List[str]

    # Inventory configuration
    inventory: InventoryConfig

    # Line configurations
    tob_lines: List[TOBLineConfig] = field(default_factory=list)
    passive_lines: List[PassiveLineConfig] = field(default_factory=list)

    # Moving averages and timing
    moving_average_periods: List[int] = field(default_factory=lambda: [5, 15, 60])  # minutes
    leverage: Decimal = Decimal('1.0')

    # Advanced settings
    taker_check: bool = True
    smart_pricing_source: PricingSourceType = PricingSourceType.AGGREGATED
    hedging_enabled: bool = True  # For NON-USDT pairs
    hedging_targets: Dict[str, str] = field(default_factory=dict)  # currency -> target amount (string)

    # Volume coefficient settings (using existing proven system)
    time_periods: List[str] = field(default_factory=lambda: ['5min', '15min', '30min'])
    coefficient_method: str = 'min'  # 'min', 'max', or 'mid'
    min_coefficient: float = 0.2
    max_coefficient: float = 3.0

    # Delta targeting safeguards
    target_inventory_source: str = "manual"  # "manual" or "delta_calc"
    delta_service_url: str = "http://127.0.0.1:8085"
    delta_refresh_seconds: float = 5.0
    delta_change_threshold_pct: float = 0.025
    delta_trade_fraction: float = 0.1
    delta_target_sign: float = -1.0
    prevent_unprofitable_trades: bool = False


@dataclass
class ActiveTOBLine:
    """Runtime state for an active TOB line."""
    config: TOBLineConfig
    # Multi-symbol tracking: symbol -> exchange -> side -> order_id
    active_orders: Dict[str, Dict[str, Dict[str, str]]] = field(default_factory=dict)
    last_prices: Dict[str, Dict[str, Dict[str, Decimal]]] = field(default_factory=dict)
    placed_at: Dict[str, Dict[str, Dict[str, datetime]]] = field(default_factory=dict)
    current_coefficient: Decimal = Decimal('1.0')
    # Per-exchange midpoint tracking at placement time: symbol -> exchange -> midpoint
    last_midpoint_per_exchange: Dict[str, Dict[str, Decimal]] = field(default_factory=dict)


@dataclass
class ActivePassiveLine:
    """Runtime state for an active passive line."""
    config: PassiveLineConfig
    # Multi-symbol tracking: symbol -> exchange -> side -> order_id
    active_orders: Dict[str, Dict[str, Dict[str, str]]] = field(default_factory=dict)
    last_reference_price: Dict[str, Optional[Decimal]] = field(default_factory=dict)  # symbol -> price
    last_spread: Dict[str, Dict[str, Decimal]] = field(default_factory=dict)  # symbol -> side -> spread_bps
    placed_at: Dict[str, Dict[str, Dict[str, datetime]]] = field(default_factory=dict)
    # Per-exchange midpoint tracking: symbol -> exchange -> midpoint
    last_midpoint_per_exchange: Dict[str, Dict[str, Decimal]] = field(default_factory=dict)
    current_spread_coefficient: Decimal = Decimal('1.0')
    current_quantity_coefficient: Decimal = Decimal('1.0')


class StackedMarketMakingStrategy(BaseStrategy):
    """
    Stacked Market Making Strategy - Advanced multi-line market making with inventory management.

    Features from NEW_BOT.txt:
    - Dual line types: TOB lines (local pricing) and Passive lines (aggregated pricing)
    - Inventory coefficient management with position risk controls
    - Volume coefficient modulation for dynamic sizing
    - Smart price selection between aggregated and component venue pricing
    - Cross-currency hedging for non-USDT pairs
    - Per-level liquidity share tracking for intelligent routing
    """

    def __init__(
        self,
        instance_id: str,
        symbol: str,
        exchanges: List[str],
        config: Dict[str, Any]
    ):
        super().__init__(instance_id, symbol, exchanges, config)

        # Parse configuration
        self.strategy_config: Optional[StackedMarketMakingConfig] = None
        self.inventory_manager: Optional[InventoryManager] = None

        # Market data components
        self.market_data_service: Optional[MultiSymbolMarketDataService] = None
        self.pricing_engine: Optional[MultiReferencePricingEngine] = None

        # Existing proven coefficient system integration
        self.ma_calculator: Optional[MovingAverageCalculator] = None
        self.coefficient_calculator: Optional[SimpleExchangeCoefficientCalculator] = None
        self.trade_db_manager: Optional[TradeDataManager] = None
        self.exchange_coefficients: Dict[str, float] = {}  # exchange -> coefficient
        self.last_coefficient_update: Optional[datetime] = None

        # Active line tracking
        self.active_tob_lines: Dict[int, ActiveTOBLine] = {}
        self.active_passive_lines: Dict[int, ActivePassiveLine] = {}

        # Background tasks
        self.main_task: Optional[asyncio.Task] = None
        self.coefficient_update_task: Optional[asyncio.Task] = None
        self.delta_update_task: Optional[asyncio.Task] = None

        # Performance tracking
        self.orders_placed_tob = 0
        self.orders_placed_passive = 0
        self.orders_cancelled_timeout = 0
        self.orders_cancelled_drift = 0
        self.orders_cancelled_replace = 0
        self.coefficient_adjustments = 0
        self.smart_pricing_decisions = 0

        # Redis performance publishing (for frontend details panel)
        self.performance_key = f"bot_performance:{self.instance_id}"
        self._last_performance_publish = 0.0

        # Moving average tracking
        self.moving_averages: Dict[str, Dict[int, Decimal]] = {}  # symbol -> period_minutes -> ma_value
        self.price_history: Dict[str, deque] = {}  # symbol -> price history

        # Inventory calculation (like volume weighted strategy)
        self.real_current_inventory: Decimal = Decimal('0')
        self.real_inventory_price: Optional[Decimal] = None

        # Delta target state
        self.last_delta_target: Optional[Decimal] = None
        self.last_delta_response: Optional[Dict[str, Any]] = None
        self.last_delta_updated_at: Optional[datetime] = None

        # Hedging state: track per-exchange quote exposures from our fills
        # Keyed by (exchange, currency) -> Decimal exposure in currency units
        self.quote_exposures: Dict[Tuple[str, str], Decimal] = {}

        # Risk V2 Redis client for inventory
        self.redis_client: Optional[redis.Redis] = None
        self._last_db_inventory_calc_at: Optional[datetime] = None
        # Risk V2 WS subscriber state
        self.risk_ws_url: str = "ws://127.0.0.1:8080/api/v2/risk/ws"
        self._ws_session: Optional[aiohttp.ClientSession] = None
        self._ws_task: Optional[asyncio.Task] = None
        self._ws_backoff_seconds: float = 1.0
        # WS inventory freshness tracking (require fresh inventory before TOB placement)
        self._last_ws_inventory_update: Optional[datetime] = None
        self.ws_fresh_window_seconds: float = 5.0

    async def _validate_config(self) -> None:
        """Validate stacked market making configuration."""
        try:
            # Parse configuration from dict
            self.strategy_config = self._parse_config_dict(self.config)

            # Validate required fields
            if not self.strategy_config.base_coin:
                raise ValueError("base_coin is required")

            if not self.strategy_config.quote_currencies:
                raise ValueError("quote_currencies list is required")

            if not self.strategy_config.tob_lines and not self.strategy_config.passive_lines:
                raise ValueError("At least one TOB line or passive line must be configured")

            # Create inventory manager
            self.inventory_manager = InventoryManager(self.strategy_config.inventory)

            self.logger.info(
                f"Validated stacked market making config: "
                f"{len(self.strategy_config.tob_lines)} TOB lines, "
                f"{len(self.strategy_config.passive_lines)} passive lines, "
                f"inventory target: {self.strategy_config.inventory.target_inventory}"
            )

        except Exception as e:
            self.logger.error(f"Configuration validation failed: {e}")
            raise

    def _parse_config_dict(self, config_dict: Dict[str, Any]) -> StackedMarketMakingConfig:
        """Parse configuration dictionary into typed configuration object."""
        # Flatten double-nested config if needed
        if 'config' in config_dict and isinstance(config_dict['config'], dict):
            config_dict = config_dict['config']

        # Parse inventory config
        inventory_config_dict = config_dict.get('inventory', {})

        # Safe parsing with None checks
        target_inv_raw = inventory_config_dict.get('target_inventory', '0')
        self.logger.info(f"🔧 Parsing target_inventory: raw='{target_inv_raw}', type={type(target_inv_raw)}")

        target_inventory_value = Decimal(str(target_inv_raw)) if target_inv_raw is not None else Decimal('0')

        inventory_config = InventoryConfig(
            target_inventory=target_inventory_value,
            max_inventory_deviation=Decimal(str(inventory_config_dict.get('max_inventory_deviation', '1000'))),
            inventory_price_method=InventoryPriceMethod(
                inventory_config_dict.get('inventory_price_method', 'accounting')
            ),
            manual_inventory_price=(
                Decimal(str(inventory_config_dict['manual_inventory_price']))
                if inventory_config_dict.get('manual_inventory_price') else None
            ),
            start_time=(
                datetime.fromisoformat(inventory_config_dict['start_time'])
                if inventory_config_dict.get('start_time') else None
            )
        )

        self.logger.info(f"✅ Inventory config created: target={target_inventory_value}, max_dev={inventory_config.max_inventory_deviation}")

        # Parse coefficient system configuration (using existing proven system)
        self.time_periods = config_dict.get('time_periods', ['1min', '5min', '15min'])  # Shorter periods for faster responsiveness
        self.coefficient_method = config_dict.get('coefficient_method', 'min')
        self.min_coefficient = float(config_dict.get('min_coefficient', 0.2))
        self.max_coefficient = float(config_dict.get('max_coefficient', 3.0))

        # Parse TOB lines
        tob_lines = []
        for i, tob_config in enumerate(config_dict.get('tob_lines', [])):
            tob_line = TOBLineConfig(
                line_id=i,
                min_price=Decimal(str(tob_config['min_price'])) if tob_config.get('min_price') else None,
                max_price=Decimal(str(tob_config['max_price'])) if tob_config.get('max_price') else None,
                hourly_quantity=Decimal(str(tob_config.get('hourly_quantity', '100'))),
                quantity_currency=tob_config.get('quantity_currency', 'base'),
                sides=tob_config.get('sides', 'both'),
                spread_from_inventory=tob_config.get('spread_from_inventory', True),
                spread_bps=Decimal(str(tob_config.get('spread_bps', '50'))),
                drift_bps=Decimal(str(tob_config.get('drift_bps', '30'))),
                timeout_seconds=int(tob_config.get('timeout_seconds', 30)),
                coefficient_method=tob_config.get('coefficient_method', 'inventory'),
                max_coefficient=Decimal(str(tob_config.get('max_coefficient', '2.0'))),
                min_coefficient=Decimal(str(tob_config.get('min_coefficient', '0.1')))
            )
            tob_lines.append(tob_line)

        # Parse passive lines
        passive_lines = []
        for i, passive_config in enumerate(config_dict.get('passive_lines', [])):
            passive_line = PassiveLineConfig(
                line_id=i,
                mid_spread_bps=Decimal(str(passive_config.get('mid_spread_bps', '20'))),
                quantity=Decimal(str(passive_config.get('quantity', '100'))),
                sides=passive_config.get('sides', 'both'),
                spread_coefficient_method=passive_config.get('spread_coefficient_method', 'inventory'),
                quantity_coefficient_method=passive_config.get('quantity_coefficient_method', 'volume'),
                min_spread_bps=Decimal(str(passive_config.get('min_spread_bps', '10'))),
                max_spread_bps=Decimal(str(passive_config.get('max_spread_bps', '200'))),
                drift_bps=Decimal(str(passive_config.get('drift_bps', '20'))),
                timeout_seconds=int(passive_config.get('timeout_seconds', 60)),
                randomization_factor=Decimal(str(passive_config.get('randomization_factor', '0.05')))
            )
            passive_lines.append(passive_line)

        # No need for separate volume config - using existing proven system

        # Deduplicate quote currencies to avoid duplicate symbols
        quote_currencies = config_dict.get('quote_currencies', ['USDT'])
        unique_quote_currencies = list(dict.fromkeys(quote_currencies))  # Preserves order, removes duplicates

        if len(unique_quote_currencies) != len(quote_currencies):
            self.logger.info(f"Deduplicated quote currencies: {quote_currencies} -> {unique_quote_currencies}")

        return StackedMarketMakingConfig(
            base_coin=config_dict['base_coin'],
            quote_currencies=unique_quote_currencies,
            exchanges=self.exchanges,
            inventory=inventory_config,
            tob_lines=tob_lines,
            passive_lines=passive_lines,
            moving_average_periods=config_dict.get('moving_average_periods', [5, 15, 60]),
            leverage=Decimal(str(config_dict.get('leverage', '1.0'))),
            taker_check=config_dict.get('taker_check', True),
            smart_pricing_source=PricingSourceType(config_dict.get('smart_pricing_source', 'aggregated')),
            hedging_enabled=config_dict.get('hedging_enabled', True),
            hedging_targets={k.upper(): str(v) for k, v in (config_dict.get('hedging_targets', {}) or {}).items() if k and k.upper() != 'USDT'},
            # Using existing proven coefficient system instead of separate volume_config
            target_inventory_source=config_dict.get('target_inventory_source', 'manual'),
            delta_service_url=config_dict.get('delta_service_url', 'http://127.0.0.1:8085'),
            delta_refresh_seconds=float(config_dict.get('delta_refresh_seconds', 5.0)),
            delta_change_threshold_pct=float(config_dict.get('delta_change_threshold_pct', 0.025)),
            delta_trade_fraction=float(config_dict.get('delta_trade_fraction', 0.1)),
            delta_target_sign=float(config_dict.get('delta_target_sign', -1.0)),
            prevent_unprofitable_trades=bool(config_dict.get('prevent_unprofitable_trades', False)),
        )

    async def _start_strategy(self) -> None:
        """Start the stacked market making strategy."""
        self.logger.info("Starting Stacked Market Making Strategy")

        # Initialize market data components
        await self._initialize_market_data()

        # Initialize Redis client (optional) for inventory from risk_v2
        try:
            self.redis_client = redis.from_url("redis://localhost:6379")
            await self.redis_client.ping()
            self.logger.info("Connected to Redis for inventory (Risk V2)")
        except Exception as e:
            self.logger.warning(f"Could not connect to Redis (inventory optional): {e}")
            self.redis_client = None

        # Initialize active lines
        self._initialize_active_lines()

        # Start main strategy loop
        self.main_task = asyncio.create_task(self._strategy_loop())

        # Start coefficient update loop using existing proven system
        self.coefficient_update_task = asyncio.create_task(self._coefficient_update_loop())

        # Start Risk V2 WS subscriber for pushed positions/metrics
        self._ws_task = asyncio.create_task(self._risk_ws_loop())

        # Start delta target update loop
        self.delta_update_task = asyncio.create_task(self._delta_target_update_loop())

        self.logger.info(
            f"Stacked Market Making Strategy started with {len(self.active_tob_lines)} TOB lines "
            f"and {len(self.active_passive_lines)} passive lines"
        )

    async def _stop_strategy(self) -> None:
        """Stop the stacked market making strategy."""
        self.logger.info("Stopping Stacked Market Making Strategy")

        # Cancel background tasks
        if self.main_task:
            self.main_task.cancel()
        if self.coefficient_update_task:
            self.coefficient_update_task.cancel()
        if self.delta_update_task:
            self.delta_update_task.cancel()
        if self._ws_task:
            self._ws_task.cancel()

        tasks = [t for t in [self.main_task, self.coefficient_update_task] if t]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        # Wait for WS task and close session
        if self._ws_task:
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass
        if self._ws_session:
            try:
                await self._ws_session.close()
            except Exception:
                pass

        # Cancel all line-tracked orders first (per-line/per-exchange)
        await self._cancel_all_line_orders()

        # Belt-and-suspenders: cancel any remaining instance-tracked orders via base order tracker
        try:
            remaining = len(self.active_orders)
            if remaining:
                self.logger.info(f"Cancelling {remaining} remaining active orders via base tracker before shutdown")
            await self._cancel_all_orders()
        except Exception as e:
            self.logger.warning(f"Error during base-tracker shutdown cancellation: {e}")

        # Stop market data services
        if hasattr(self, 'pricing_engine') and self.pricing_engine:
            await self.pricing_engine.stop()

        if hasattr(self, 'market_data_service') and self.market_data_service:
            await self.market_data_service.stop()

        # Stop Redis orderbook manager
        if hasattr(self, 'redis_orderbook_manager') and self.redis_orderbook_manager:
            await self.redis_orderbook_manager.stop()

    async def _initialize_market_data(self):
        """Initialize multi-symbol market data service and pricing engine."""
        # Get all symbols we need to track (cache to avoid repeated calls)
        if not hasattr(self, '_cached_required_symbols'):
            self._cached_required_symbols = self._get_required_symbols()
        all_symbols = self._cached_required_symbols

        # Create symbol configurations for MultiSymbolMarketDataService
        symbol_configs = []
        for symbol in all_symbols:
            # Create exchange configurations for this symbol
            exchange_configs = []
            for exchange_id in self.exchanges:
                base_exchange = exchange_id.split('_')[0]
                exchange_type = 'perp' if '_perp' in exchange_id else 'spot'

                # Don't normalize here - let the MultiSymbolMarketDataService handle it
                # Just pass the standard symbol format
                exchange_configs.append({
                    'name': base_exchange,
                    'type': exchange_type,
                    'symbol': symbol  # Use original symbol format
                })

            symbol_config = SymbolConfig(
                base_symbol=symbol,
                exchanges=exchange_configs,
                priority=1,
                depth=100
            )
            symbol_configs.append(symbol_config)

        # Log symbol configurations being created
        self.logger.info(f"🔧 Created {len(symbol_configs)} symbol configurations for MultiSymbolMarketDataService")
        for config in symbol_configs:
            exchange_details = []
            for ex in config.exchanges:
                exchange_details.append(f"{ex['name']}_{ex['type']}")
            self.logger.info(f"  📊 {config.base_symbol} -> {len(config.exchanges)} exchanges: {', '.join(exchange_details)}")

        # Initialize MultiSymbolMarketDataService to publish orderbook data to Redis
        self.market_data_service = MultiSymbolMarketDataService(
            symbol_configs=symbol_configs,
            exchange_connectors=self.exchange_connectors  # Use existing connectors
        )
        await self.market_data_service.start()

        # Initialize multi-reference pricing engine
        self.pricing_engine = MultiReferencePricingEngine(self.market_data_service)
        await self.pricing_engine.start()

        # Also initialize Redis orderbook manager for backward compatibility
        from market_data.redis_orderbook_manager import RedisOrderbookManager

        self.redis_orderbook_manager = RedisOrderbookManager()
        await self.redis_orderbook_manager.start()

        # Wait for initial orderbook data to be published
        await asyncio.sleep(5)

        # Log market data status
        available_exchanges = self.redis_orderbook_manager.get_available_exchanges()
        orderbook_status = self.redis_orderbook_manager.get_status()

        self.logger.info(f"📊 MultiSymbolMarketDataService started with {len(symbol_configs)} symbol configs")
        self.logger.info(f"📊 Redis orderbook manager initialized")
        self.logger.info(f"📋 Available exchanges: {available_exchanges}")
        self.logger.info(f"📊 Orderbook status: {orderbook_status}")

        if not available_exchanges:
            self.logger.warning("⚠️ No exchanges available in Redis orderbook manager - waiting for data...")

        # Initialize existing proven coefficient system
        await self._initialize_coefficient_system()

        # Do NOT calculate inventory from the database on startup.
        # Inventory will come from Risk V2 WS (metrics/positions) and Redis.
        # await self._calculate_real_inventory_from_database()

    async def _fetch_delta_snapshot(self) -> Optional[Dict[str, Any]]:
        """Fetch delta snapshot from the delta service."""
        url = f"{self.strategy_config.delta_service_url.rstrip('/')}/delta"
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
                if not self.strategy_config or self.strategy_config.target_inventory_source != "delta_calc":
                    await asyncio.sleep(1.0)
                    continue

                snapshot = await self._fetch_delta_snapshot()
                if snapshot:
                    await self._update_target_from_delta(snapshot)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                self.logger.warning(f"Delta update loop error: {exc}")

            await asyncio.sleep(max(1.0, float(self.strategy_config.delta_refresh_seconds)))

    async def _update_target_from_delta(self, snapshot: Dict[str, Any]) -> None:
        """Apply delta target update with threshold + trade fraction safeguards."""
        try:
            raw_target = Decimal(str(snapshot.get("total_target_inventory", "0")))
            signed_target = raw_target * Decimal(str(self.strategy_config.delta_target_sign))

            base_amount = Decimal(str(snapshot.get("base_amount", "0")))
            premium_amount = Decimal(str(snapshot.get("premium_amount", "0")))
            total_option_size = base_amount + premium_amount
            if total_option_size <= 0:
                total_option_size = Decimal("0")

            threshold_units = total_option_size * Decimal(str(self.strategy_config.delta_change_threshold_pct))
            if self.last_delta_target is not None and threshold_units > 0:
                if abs(signed_target - self.last_delta_target) < threshold_units:
                    return

            trade_fraction = Decimal(str(self.strategy_config.delta_trade_fraction))
            trade_fraction = max(Decimal('0'), min(Decimal('1'), trade_fraction))
            current_inventory = self.real_current_inventory if self.real_current_inventory is not None else Decimal('0')
            stepped_target = current_inventory + (signed_target - current_inventory) * trade_fraction

            if self.strategy_config and self.strategy_config.inventory:
                self.strategy_config.inventory.target_inventory = stepped_target
            if self.inventory_manager:
                self.inventory_manager.config.target_inventory = stepped_target
                self.inventory_manager._calculate_inventory_state()

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
        if not self.strategy_config or not self.strategy_config.prevent_unprofitable_trades:
            return False
        if self.real_inventory_price is None:
            return False
        current_inventory = self.real_current_inventory or Decimal('0')
        if current_inventory < 0 and side in ("bid", "buy") and price > self.real_inventory_price:
            return True
        if current_inventory > 0 and side in ("ask", "offer", "sell") and price < self.real_inventory_price:
            return True
        return False

    def _get_required_symbols(self) -> List[str]:
        """Get all symbols required for this strategy including cross-conversion pairs."""
        symbols = set()

        # Filter out quote currencies that don't have reference pairs available
        available_quote_currencies = []
        known_reference_pairs = {
            'BTC': 'BTC/USDT',
            'ETH': 'ETH/USDT',
            'BNB': 'BNB/USDT',
            'USDC': 'USDC/USDT',
            'FDUSD': 'FDUSD/USDT',  # FDUSD support added
            'TRY': 'USDT/TRY',  # TRY trades as USDT/TRY (not TRY/USDT)
            'USDT': None,  # USDT doesn't need conversion
        }

        for quote_currency in self.strategy_config.quote_currencies:
            if quote_currency == 'USDT':
                available_quote_currencies.append(quote_currency)
            elif quote_currency in known_reference_pairs:
                available_quote_currencies.append(quote_currency)
            else:
                self.logger.warning(f"⏭️ Skipping quote currency {quote_currency} - no reference pair available")

        # Primary symbols for each available quote currency
        for quote_currency in available_quote_currencies:
            symbols.add(f"{self.strategy_config.base_coin}/{quote_currency}")

        # Only add base/USDT for conversion if we have non-USDT quote currencies that need conversion
        non_usdt_quotes = [q for q in available_quote_currencies if q != 'USDT']
        if non_usdt_quotes:
            base_usdt_symbol = f"{self.strategy_config.base_coin}/USDT"
            symbols.add(base_usdt_symbol)
            self.logger.info(f"🔧 CONVERSION SUPPORT: Added {base_usdt_symbol} for cross-currency conversion")

            # Cross-conversion symbols (NON-USDT/USDT) - only for available currencies
            for quote_currency in non_usdt_quotes:
                if quote_currency in known_reference_pairs:
                    ref_pair = known_reference_pairs[quote_currency]
                    if ref_pair:
                        symbols.add(ref_pair)
                        self.logger.info(f"🔧 CONVERSION SUPPORT: Added {ref_pair} for {quote_currency} conversion")

        symbol_list = list(symbols)

        # Log all symbols being tracked
        primary_symbols = [s for s in symbol_list if s.startswith(self.strategy_config.base_coin)]
        cross_symbols = [s for s in symbol_list if not s.startswith(self.strategy_config.base_coin)]

        self.logger.info(f"📈 Tracking {len(primary_symbols)} primary pairs: {', '.join(sorted(primary_symbols))}")
        self.logger.info(f"🔄 Tracking {len(cross_symbols)} cross-conversion pairs: {', '.join(sorted(cross_symbols))}")

        # Special note for TRY handling
        if 'USDT/TRY' in cross_symbols:
            self.logger.info("💰 TRY support: Using USDT/TRY (correct direction for Turkish Lira)")

        self.logger.info(f"📊 Total symbols for multi-reference pricing: {len(symbol_list)}")

        return symbol_list

    def _build_exchange_symbol_map(self, primary_symbols: List[str]) -> Dict[str, List[str]]:
        """Build mapping of which symbols are supported on each exchange (like volume weighted strategy)."""
        exchange_symbol_map = {}

        # For each exchange, assume all primary symbols are supported on spot exchanges
        # The strategy config already defines which exchanges to use, so we trust that config
        # This is more flexible than hardcoding specific symbols per exchange
        for exchange in self.exchanges:
            # Spot exchanges generally support most major symbols
            # We'll assign all primary symbols to each exchange and let the exchange reject if not supported
            if '_spot' in exchange:
                # For spot exchanges, assign all primary USDT pairs
                available_symbols = [s for s in primary_symbols if '/USDT' in s]
            elif '_perp' in exchange:
                # For perp exchanges, also support USDT pairs
                available_symbols = [s for s in primary_symbols if '/USDT' in s]
            else:
                available_symbols = primary_symbols

            exchange_symbol_map[exchange] = available_symbols
            self.logger.info(f"📊 {exchange}: {len(available_symbols)} symbols -> {available_symbols}")

        return exchange_symbol_map

    def _initialize_active_lines(self):
        """Initialize active line tracking."""
        # Initialize TOB lines
        for tob_config in self.strategy_config.tob_lines:
            self.active_tob_lines[tob_config.line_id] = ActiveTOBLine(config=tob_config)

        # Initialize passive lines
        for passive_config in self.strategy_config.passive_lines:
            self.active_passive_lines[passive_config.line_id] = ActivePassiveLine(config=passive_config)

    async def _strategy_loop(self) -> None:
        """Main strategy loop processing all line types across all primary symbols."""
        self.logger.info("Starting stacked market making strategy loop")

        # Get symbols that are actually available per exchange type (like volume weighted strategy)
        # Use hasattr/getattr pattern to avoid evaluating _get_required_symbols() when cache exists
        if not hasattr(self, '_cached_required_symbols'):
            self._cached_required_symbols = self._get_required_symbols()
        all_symbols = self._cached_required_symbols
        all_primary_symbols = [s for s in all_symbols if s.startswith(self.strategy_config.base_coin)]

        # Filter to only symbols we actually want to trade (exclude BERA/USDT if not in config)
        config_quote_currencies = self.strategy_config.quote_currencies
        trading_symbols = []
        for symbol in all_primary_symbols:
            base, quote = symbol.split('/')
            if quote in config_quote_currencies:
                trading_symbols.append(symbol)

        self.logger.info(f"🎯 Trading on {len(trading_symbols)} configured symbols: {', '.join(trading_symbols)}")
        if len(all_primary_symbols) > len(trading_symbols):
            conversion_symbols = [s for s in all_primary_symbols if s not in trading_symbols]
            self.logger.info(f"📊 Tracking {len(conversion_symbols)} conversion symbols: {', '.join(conversion_symbols)}")

        # Build exchange-specific symbol mapping for trading symbols only
        exchange_symbol_map = self._build_exchange_symbol_map(trading_symbols)

        while self.running:
            try:
                start_time = time.perf_counter()

                # First, cancel stale or drifting orders so we can replace in the same cycle
                await self._check_all_order_timeouts_and_drift()

                # Process each exchange in its own async task (exchange-by-exchange concurrency)
                exchange_tasks = []
                for exchange in self.exchanges:
                    supported_symbols = exchange_symbol_map.get(exchange, [])
                    if supported_symbols:
                        exchange_tasks.append(self._process_exchange_cycle(exchange, supported_symbols))

                # Debug: Log line counts periodically
                if hasattr(self, '_last_line_count_log'):
                    if time.time() - self._last_line_count_log > 30:  # Every 30 seconds
                        self.logger.info(f"🎯 Trading {len(trading_symbols)} symbols with {len(self.active_tob_lines)} TOB, {len(self.active_passive_lines)} passive lines each")
                        self._last_line_count_log = time.time()
                else:
                    self._last_line_count_log = time.time()
                    self.logger.info(f"🎯 Trading {len(trading_symbols)} symbols with {len(self.active_tob_lines)} TOB, {len(self.active_passive_lines)} passive lines each")

                # Execute exchange processing concurrently
                if exchange_tasks:
                    await asyncio.gather(*exchange_tasks, return_exceptions=True)

                # Update moving averages
                await self._update_moving_averages()

                # Cycle time measurement
                cycle_time = (time.perf_counter() - start_time) * 1000

                # Log performance periodically
                if hasattr(self, '_last_performance_log'):
                    if time.time() - self._last_performance_log > 10:  # Every 10 seconds
                        self._log_performance_summary(cycle_time)
                        self._last_performance_log = time.time()
                else:
                    self._last_performance_log = time.time()

                # Publish performance snapshot to Redis for frontend details
                if time.time() - self._last_performance_publish > 1.0:
                    await self._publish_performance_data()
                    self._last_performance_publish = time.time()

                # Fast cycle for market making
                await asyncio.sleep(0.002)  # 2ms cycle time

            except asyncio.CancelledError:
                self.logger.info("Strategy loop cancelled")
                break
            except Exception as e:
                self.logger.error(f"Error in strategy loop: {e}")
                await asyncio.sleep(1)

    async def _process_exchange_cycle(self, exchange: str, supported_symbols: List[str]) -> None:
        """Process TOB and Passive lines for a single exchange."""
        try:
            for symbol in supported_symbols:
                line_tasks = []
                # Process TOB lines for this symbol on this exchange
                for line in self.active_tob_lines.values():
                    line_tasks.append(self._process_tob_line_for_exchange_symbol(line, exchange, symbol))

                # Process Passive lines for this symbol on this exchange
                for line in self.active_passive_lines.values():
                    line_tasks.append(self._process_passive_line_for_exchange_symbol(line, exchange, symbol))

                if line_tasks:
                    await asyncio.gather(*line_tasks, return_exceptions=True)
        except Exception as e:
            self.logger.error(f"Error processing exchange cycle for {exchange}: {e}")

    async def _initialize_coefficient_system(self):
        """Initialize the existing proven coefficient system."""
        try:
            # Create MA configurations from time periods (like volume_weighted_top_of_book.py)
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

            # Initialize coefficients with default values
            for exchange in self.exchanges:
                self.exchange_coefficients[exchange] = 1.0

            self.logger.info(f"Initialized existing proven coefficient system with {len(ma_configs)} MA configurations")

        except Exception as e:
            self.logger.error(f"Error initializing coefficient system: {e}")
            raise

    async def _calculate_real_inventory_from_database(self) -> None:
        """Calculate real inventory from database (exactly matching risk system calculation)."""
        try:
            from database import get_session
            from sqlalchemy import text

            # Use June 18, 2025 11:27 AM UTC as specified
            start_time = datetime(2025, 6, 18, 11, 27, tzinfo=timezone.utc)
            start_time_for_db = start_time.replace(tzinfo=None)

            # DEBUG: Log datetime operations
            self.logger.info(f"🔧 DATETIME DEBUG: start_time={start_time} (type: {type(start_time)})")
            self.logger.info(f"🔧 DATETIME DEBUG: start_time_for_db={start_time_for_db} (type: {type(start_time_for_db)})")

            async for session in get_session():
                try:
                    await session.begin()

                    self.logger.info(f"🔍 Calculating real inventory since {start_time_for_db} (matching risk system)")

                    # Query ALL BERA trades (like risk system does)
                    query = text('''
                        SELECT
                            symbol,
                            side,
                            SUM(amount) as total_amount,
                            SUM(CASE WHEN side = 'buy' THEN amount ELSE -amount END) as net_amount
                        FROM risk_trades
                        WHERE symbol LIKE '%BERA%'
                        AND timestamp >= :start_time
                        GROUP BY symbol, side
                        ORDER BY symbol, side
                    ''')

                    result = await session.execute(query, {'start_time': start_time_for_db})
                    rows = result.fetchall()

                    # Calculate net inventory by symbol (like risk system)
                    symbol_nets = {}
                    for row in rows:
                        symbol = row.symbol
                        side = row.side
                        total = Decimal(str(row.total_amount))

                        if symbol not in symbol_nets:
                            symbol_nets[symbol] = Decimal('0')

                        if side == 'buy':
                            symbol_nets[symbol] += total
                        else:
                            symbol_nets[symbol] -= total

                    # Sum all symbol variants (like risk system does)
                    total_inventory = sum(symbol_nets.values())

                    self.real_current_inventory = total_inventory

                    # Calculate weighted average price
                    total_cost = Decimal('0')
                    total_amount = Decimal('0')

                    cost_query = text('''
                        SELECT side, amount, price
                        FROM risk_trades
                        WHERE symbol LIKE '%BERA%'
                        AND timestamp >= :start_time
                        ORDER BY timestamp ASC
                    ''')

                    cost_result = await session.execute(cost_query, {'start_time': start_time_for_db})
                    cost_rows = cost_result.fetchall()

                    for row in cost_rows:
                        # DEBUG: Log before processing database row
                        #  self.logger.info(f"🔧 DB ROW DEBUG: amount={row.amount} (type: {type(row.amount)}), price={row.price} (type: {type(row.price)}), side={row.side}")

                        # Safe conversion with None checks
                        if row.amount is None or row.price is None:
                            self.logger.warning(f"⚠️ Skipping row with None values: amount={row.amount}, price={row.price}")
                            continue

                        amount = Decimal(str(row.amount))
                        price = Decimal(str(row.price))
                        side = row.side.lower()

                        # DEBUG: Log before arithmetic operations
                        #self.logger.info(f"🔧 DB ARITHMETIC DEBUG: amount={amount}, price={price}, side={side}")

                        if side == 'buy':
                            #self.logger.info(f"🔧 DB BUY DEBUG: total_cost += {amount} * {price}, total_amount += {amount}")
                            total_cost += amount * price
                            total_amount += amount
                        else:
                            #self.logger.info(f"🔧 DB SELL DEBUG: total_cost -= {amount} * {price}, total_amount -= {amount}")
                            # This could be the problematic subtraction if amount or price is somehow None
                            total_cost -= amount * price
                            total_amount -= amount

                    if abs(total_amount) > Decimal('0.001'):
                        self.real_inventory_price = abs(total_cost / total_amount)
                    else:
                        self.real_inventory_price = None

                    # DEBUG: Log before database inventory subtraction
                    target_decimal = Decimal('4244000')
                    self.logger.info(f"🔧 DB INVENTORY DEBUG: total_inventory={total_inventory} (type: {type(total_inventory)})")
                    self.logger.info(f"🔧 DB INVENTORY DEBUG: target_decimal={target_decimal} (type: {type(target_decimal)})")
                    self.logger.info(f"🔧 DB INVENTORY SUBTRACTION DEBUG: total_inventory - target = {total_inventory} - {target_decimal}")

                    excess_bera = total_inventory - target_decimal

                    self.logger.info(f"📊 REAL INVENTORY CALCULATION (matching risk system):")
                    self.logger.info(f"  Current: {total_inventory:,.2f} BERA")
                    self.logger.info(f"  Target: 4,244,000 BERA")
                    self.logger.info(f"  Excess: {excess_bera:,.2f} BERA")
                    if self.real_inventory_price:
                        self.logger.info(f"  Price: {self.real_inventory_price:.6f}")
                    else:
                        self.logger.info(f"  Price: N/A")

                    await session.commit()

                except Exception as e:
                    await session.rollback()
                    raise e
                finally:
                    await session.close()

        except Exception as e:
            self.logger.error(f"Error calculating real inventory: {e}")
            self.real_current_inventory = Decimal('0')
            self.real_inventory_price = None
        finally:
            self._last_db_inventory_calc_at = datetime.now(timezone.utc)

    def _create_ma_configs_from_time_periods(self) -> List[MovingAverageConfig]:
        """Create MA configurations from time periods (same as volume_weighted_top_of_book.py)."""
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

                # Add EWMA for shorter periods only
                if period_points <= 360:  # Up to 1 hour
                    configs.extend([
                        MovingAverageConfig(period=period_points, ma_type='ewma', volume_type='quote'),
                        MovingAverageConfig(period=period_points, ma_type='ewma', volume_type='base'),
                    ])

        return configs

    def _convert_time_period_to_points(self, period_str: str) -> int:
        """Convert time period string to number of data points (same as volume_weighted_top_of_book.py)."""
        conversions = {
            '30s': 3, '1min': 6, '5min': 30, '15min': 90, '30min': 180, '60min': 360,
            '240min': 1440, '480min': 2880, '720min': 4320, '1080min': 6480,
            '1440min': 8640, '2880min': 17280, '4320min': 25920, '10080min': 60480,
            '20160min': 120960, '43200min': 259200
        }
        return conversions.get(period_str, 0)

    async def _coefficient_update_loop(self) -> None:
        """Background loop for updating coefficients using existing proven system."""
        while self.running:
            try:
                # Update MA and coefficients for each exchange (like volume_weighted_top_of_book.py)
                for exchange in self.exchanges:
                    await self._update_ma_for_exchange(exchange)

                # Calculate new coefficients using existing proven system
                await self._update_coefficients()

                # Update line coefficients based on calculated exchange coefficients
                self._apply_coefficients_to_lines()

                await asyncio.sleep(10)  # Update every 10 seconds like existing system

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in coefficient update loop: {e}")
                await asyncio.sleep(5)

    async def _update_ma_for_exchange(self, exchange: str) -> None:
        """Update moving averages for exchange (adapted from volume_weighted_top_of_book.py)."""
        try:
            current_time = datetime.now(timezone.utc)

            # Calculate time window for MA update
            window_start = current_time - timedelta(hours=1)  # 1 hour window

            # Get trades from database using existing system
            trades = await self._get_trades_for_ma_update(exchange, window_start, current_time)

            if not trades:
                return

            # Calculate volume data using existing system
            volume_data = self.ma_calculator.calculate_volume_data(trades)

            # Get database format
            db_exchange_id, db_symbol = self._get_db_exchange_and_symbol(exchange)

            if not db_exchange_id:
                return

            # Update moving averages using existing proven system
            ma_values = self.ma_calculator.update_moving_averages(
                db_symbol, db_exchange_id, volume_data, current_time
            )

            self.logger.info(f"Updated MAs for {exchange}: {len(trades)} trades processed")

        except Exception as e:
            self.logger.error(f"Error updating MA for {exchange}: {e}")

    async def _update_coefficients(self) -> None:
        """Update coefficients using existing proven system."""
        try:
            for exchange in self.exchanges:
                db_exchange_id, db_symbol = self._get_db_exchange_and_symbol(exchange)

                if not db_exchange_id:
                    continue

                coefficient = self.coefficient_calculator.calculate_coefficient(db_symbol, db_exchange_id)

                if coefficient is not None:
                    old_coefficient = self.exchange_coefficients.get(exchange, 1.0)
                    self.exchange_coefficients[exchange] = coefficient

                    self.logger.info(f"Updated coefficient for {exchange}: {old_coefficient:.4f} → {coefficient:.4f}")

            self.last_coefficient_update = datetime.now(timezone.utc)

        except Exception as e:
            self.logger.error(f"Error updating coefficients: {e}")

    def _apply_coefficients_to_lines(self):
        """Apply calculated exchange coefficients to all active lines."""
        for line in self.active_tob_lines.values():
            # Apply exchange-specific coefficients to TOB lines
            for exchange in self.exchanges:
                coefficient = self.exchange_coefficients.get(exchange, 1.0)
                # Apply to TOB line based on coefficient method
                if line.config.coefficient_method in ['volume', 'both']:
                    line.current_coefficient = Decimal(str(coefficient))

        for line in self.active_passive_lines.values():
            # Apply coefficients to passive lines
            valid_coefficients = [coeff for coeff in self.exchange_coefficients.values() if coeff is not None]
            if valid_coefficients:
                avg_coefficient = sum(valid_coefficients) / len(valid_coefficients)
            else:
                avg_coefficient = 1.0  # Default if no valid coefficients

            if line.config.quantity_coefficient_method in ['volume', 'both']:
                line.current_quantity_coefficient = Decimal(str(avg_coefficient))
            elif line.config.quantity_coefficient_method in ['none', 'off', 'disabled']:
                line.current_quantity_coefficient = Decimal('1.0')

    async def _get_trades_for_ma_update(self, exchange: str, start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
        """Get trades for MA update using risk_trades table (same as volume_weighted_top_of_book.py)."""
        try:
            from database import get_session
            from sqlalchemy import text

            db_exchange_id, db_symbol = self._get_db_exchange_and_symbol(exchange)

            if not db_exchange_id:
                return []

            async for session in get_session():
                try:
                    await session.begin()

                    # Convert timezone-aware datetime to timezone-naive for database query
                    start_time_for_db = start_time.astimezone(timezone.utc).replace(tzinfo=None) if start_time.tzinfo else start_time
                    end_time_for_db = end_time.astimezone(timezone.utc).replace(tzinfo=None) if end_time.tzinfo else end_time

                    # Query risk_trades table directly like volume_weighted_top_of_book.py
                    query = text("""
                        SELECT side, amount, price, cost, fee_cost, fee_currency, timestamp
                        FROM risk_trades
                        WHERE exchange_id = :exchange_id
                        AND symbol = :symbol
                        AND timestamp >= :start_time
                        AND timestamp <= :end_time
                        ORDER BY timestamp ASC
                    """)

                    result = await session.execute(query, {
                        "exchange_id": db_exchange_id,
                        "symbol": db_symbol,
                        "start_time": start_time_for_db,
                        "end_time": end_time_for_db
                    })

                    trades_data = result.fetchall()

                    # Convert to dict format expected by MA calculator
                    filtered_trades = []
                    for trade in trades_data:
                        filtered_trades.append({
                            'side': trade.side,
                            'amount': float(trade.amount),
                            'price': float(trade.price),
                            'cost': float(trade.cost) if trade.cost else float(trade.amount) * float(trade.price),
                            'timestamp': trade.timestamp
                        })

                    await session.commit()

                    if filtered_trades:
                        self.logger.info(f"Retrieved {len(filtered_trades)} trades for {db_exchange_id} {db_symbol} from risk_trades table")

                    return filtered_trades

                except Exception as e:
                    await session.rollback()
                    raise e
                finally:
                    await session.close()

        except Exception as e:
            self.logger.error(f"Error getting trades for MA update: {e}")
            return []

    def _get_db_exchange_and_symbol(self, exchange_id: str) -> Tuple[int, str]:
        """Convert exchange ID to database format with numeric exchange_id."""
        # Exchange ID mapping from risk system
        EXCHANGE_ID_MAP = {
            'binance_spot': 33,
            'binance_perp': 34,
            'bybit_spot': 35,
            'bybit_perp': 36,
            'mexc_spot': 37,
            'gateio_spot': 38,
            'bitget_spot': 39,
            'hyperliquid_perp': 40
        }

        # Handle exchange IDs without _spot suffix for backwards compatibility
        if exchange_id in ['mexc', 'gateio', 'bitget']:
            exchange_id = f"{exchange_id}_spot"

        db_exchange_id = EXCHANGE_ID_MAP.get(exchange_id)
        if not db_exchange_id:
            self.logger.error(f"Unknown exchange_id: {exchange_id}")
            return None, None

        db_symbol = self.symbol

        if '_perp' in exchange_id:
            if exchange_id.startswith('binance') and self.symbol == 'BERA/USDT':
                db_symbol = 'BERA/USDT:USDT'
            elif exchange_id.startswith('bybit') and self.symbol == 'BERA/USDT':
                db_symbol = 'BERA/USDT:USDT'
            elif exchange_id == 'hyperliquid_perp' and self.symbol == 'BERA/USDT':
                db_symbol = 'BERA/USDC:USDC'

        return db_exchange_id, db_symbol

    async def _process_tob_line_for_symbol(self, line: ActiveTOBLine, symbol: str) -> None:
        """
        Process a Top of Book line for a specific symbol.

        From NEW_BOT.txt: "base quotes on local TOB (bid + min tick for bids; ask − min tick for offers)"
        """
        try:
            config = line.config

            # Check timeouts and drift for this symbol
            await self._check_tob_timeouts_for_symbol(line, symbol)
            await self._check_tob_drift_for_symbol(line, symbol)

            # Place missing orders on each exchange for this symbol
            for exchange in self.exchanges:
                await self._place_missing_tob_orders_for_symbol(line, exchange, symbol)

        except Exception as e:
            self.logger.error(f"Error processing TOB line {line.config.line_id} for {symbol}: {e}")

    async def _process_passive_line_for_symbol(self, line: ActivePassiveLine, symbol: str) -> None:
        """
        Process a Passive line for a specific symbol.

        From NEW_BOT.txt: "Maintain passive quote ladders derived from the combined BASE/USDT book"
        """
        try:
            config = line.config

            # Check timeouts and drift for this symbol
            await self._check_passive_timeouts_for_symbol(line, symbol)
            await self._check_passive_drift_for_symbol(line, symbol)

            # Place missing orders using aggregated pricing for this symbol
            await self._place_missing_passive_orders_for_symbol(line, symbol)

        except Exception as e:
            self.logger.error(f"Error processing passive line {line.config.line_id} for {symbol}: {e}")

    async def _process_tob_line_for_exchange_symbol(self, line: ActiveTOBLine, exchange: str, symbol: str) -> None:
        """Process a TOB line for a specific exchange and symbol."""
        try:
            # Only process if this exchange-symbol combination is valid
            await self._place_missing_tob_orders_for_symbol(line, exchange, symbol)
        except Exception as e:
            self.logger.error(f"Error processing TOB line {line.config.line_id} for {symbol} on {exchange}: {e}")

    async def _process_passive_line_for_exchange_symbol(self, line: ActivePassiveLine, exchange: str, symbol: str) -> None:
        """Process a Passive line for a specific exchange and symbol."""
        try:
            # Check timeouts and drift for this exchange-symbol combination
            await self._check_passive_timeouts_for_exchange_symbol(line, exchange, symbol)
            await self._check_passive_drift_for_exchange_symbol(line, exchange, symbol)

            # Only process if this exchange-symbol combination is valid
            await self._place_missing_passive_orders_for_exchange_symbol(line, exchange, symbol)
        except Exception as e:
            self.logger.error(f"Error processing passive line {line.config.line_id} for {symbol} on {exchange}: {e}")

    async def _place_missing_passive_orders_for_exchange_symbol(self, line: ActivePassiveLine, exchange: str, symbol: str):
        """Place missing passive orders for a specific exchange and symbol."""
        try:
            config = line.config

            # Parse symbol for cross-currency logic
            base_currency, quote_currency = symbol.split('/')

            # Get aggregated pricing based on smart pricing configuration
            aggregated_price = await self._get_smart_pricing(symbol, 'mid', Decimal('100'))

            if not aggregated_price:
                self.logger.debug(f"Passive Line {config.line_id}: No aggregated price for {symbol} on {exchange}, skipping")
                return

            # Apply ASYMMETRIC inventory-based spread adjustment
            # Get inventory coefficient for asymmetric spread calculation
            current_inventory = self.real_current_inventory if self.real_current_inventory is not None else Decimal('0')
            target_inventory = self.strategy_config.inventory.target_inventory if (self.strategy_config and self.strategy_config.inventory) else Decimal('0')
            
            # Calculate inventory coefficient using abs(target) for correct sign handling
            if target_inventory != 0:
                inv_coeff = (current_inventory - target_inventory) / abs(target_inventory)
                # Clamp to [-1, 1]
                inv_coeff = max(Decimal('-1'), min(Decimal('1'), inv_coeff))
            else:
                inv_coeff = Decimal('0')
            
            # Calculate asymmetric spreads based on inventory position
            # Logic: interpolate between min_spread and max_spread based on inventory coefficient
            # - The "push away" side (unwanted direction) goes from mid_spread toward max_spread
            # - The "pull toward" side (wanted direction) goes from mid_spread toward min_spread
            base_spread = config.mid_spread_bps
            min_spread = config.min_spread_bps
            max_spread = config.max_spread_bps
            inv_abs = abs(inv_coeff)
            
            # Interpolate spreads: at |inv_coeff|=0: both at base_spread; at |inv_coeff|=1: max deviation
            # Wide side: base_spread -> max_spread as |inv_coeff| goes 0 -> 1
            # Tight side: base_spread -> min_spread as |inv_coeff| goes 0 -> 1
            wide_spread = base_spread + (max_spread - base_spread) * inv_abs
            tight_spread = base_spread - (base_spread - min_spread) * inv_abs
            
            if inv_coeff < 0:
                # Below target (MORE SHORT than target, need to BUY to reduce short)
                # -> tighten bids (closer to mid to encourage buying)
                # -> widen asks (further from mid to discourage selling)
                bid_spread_bps = tight_spread
                ask_spread_bps = wide_spread
            elif inv_coeff > 0:
                # Above target (LESS SHORT than target, need to SELL to increase short)
                # -> widen bids (further from mid to discourage buying)
                # -> tighten asks (closer to mid to encourage selling)
                bid_spread_bps = wide_spread
                ask_spread_bps = tight_spread
            else:
                # At target - symmetric spreads
                bid_spread_bps = base_spread
                ask_spread_bps = base_spread
            
            # Enforce strict spread bounds from config (never breach min/max)
            bid_spread_bps = max(min_spread, min(max_spread, bid_spread_bps))
            ask_spread_bps = max(min_spread, min(max_spread, ask_spread_bps))
            
            # Calculate bid and ask prices with asymmetric spreads
            bid_spread_decimal = bid_spread_bps / Decimal('10000')
            ask_spread_decimal = ask_spread_bps / Decimal('10000')
            passive_bid_price = aggregated_price * (Decimal('1') - bid_spread_decimal)
            passive_ask_price = aggregated_price * (Decimal('1') + ask_spread_decimal)
            
            # Log the asymmetric spread calculation
            self.logger.info(
                f"Passive Line {config.line_id}: inv_coeff={float(inv_coeff):.3f}, "
                f"bid_spread={float(bid_spread_bps):.1f}bps (min={float(min_spread):.1f}), "
                f"ask_spread={float(ask_spread_bps):.1f}bps (max={float(max_spread):.1f})"
            )

            # Apply quantity adjustments
            base_quantity = config.quantity * line.current_quantity_coefficient

            # Apply randomization
            randomization = config.randomization_factor
            import random
            qty_multiplier = Decimal('1') + Decimal(str(random.uniform(-float(randomization), float(randomization))))
            final_quantity = base_quantity * qty_multiplier

            # Apply taker check per exchange - use appropriate market data based on what we're actually trading
            base_currency, quote_currency = symbol.split('/')

            # For pairs that don't exist natively, use USDT market data for taker check
            if not self._exchange_has_native_pair(exchange, symbol):
                # Use BERA/USDT market data for taker check since that's what we're trading
                resolved_symbol = self._resolve_symbol_for_exchange_and_symbol(exchange, 'BERA/USDT')
                self.logger.info(f"🔧 TAKER CHECK DEBUG: Using USDT market data for {symbol} conversion trading on {exchange}")
            else:
                # Use native market data for direct pairs (USDT everywhere, USDC only on hyperliquid_perp)
                resolved_symbol = self._resolve_symbol_for_exchange_and_symbol(exchange, symbol)
                self.logger.info(f"🔧 TAKER CHECK DEBUG: Using native market data for {symbol} on {exchange}")

            self.logger.info(f"🔧 SYMBOL RESOLUTION DEBUG: {symbol} -> {resolved_symbol} for {exchange}")

            local_best_bid, local_best_ask = self._get_best_bid_ask_from_service(exchange, resolved_symbol)
            self.logger.debug(f"🔧 MARKET DATA DEBUG (Redis): {exchange} {resolved_symbol} -> bid={local_best_bid}, ask={local_best_ask}")
            
            # If Redis has no data, try direct connector
            if not local_best_bid or not local_best_ask:
                try:
                    connector = self.exchange_connectors.get(exchange)
                    if connector:
                        exchange_symbol = self._normalize_symbol_for_exchange(symbol, exchange)
                        orderbook = await connector.get_orderbook(exchange_symbol)
                        if orderbook and orderbook.get('bids') and orderbook.get('asks'):
                            local_best_bid = Decimal(str(orderbook['bids'][0][0])) if len(orderbook['bids']) > 0 else None
                            local_best_ask = Decimal(str(orderbook['asks'][0][0])) if len(orderbook['asks']) > 0 else None
                            self.logger.debug(f"🔧 MARKET DATA DEBUG (Direct): {exchange} {exchange_symbol} -> bid={local_best_bid}, ask={local_best_ask}")
                except Exception as e:
                    self.logger.debug(f"Error getting direct market data for {exchange}: {e}")

            if local_best_bid and local_best_ask:
                # Track per-exchange midpoint used for drift checks later
                if symbol not in line.last_midpoint_per_exchange:
                    line.last_midpoint_per_exchange[symbol] = {}
                line.last_midpoint_per_exchange[symbol][exchange] = (local_best_bid + local_best_ask) / Decimal('2')
                # Apply taker check
                exchange_bid_price = self._apply_taker_check_price(
                    passive_bid_price, 'bid', local_best_bid, local_best_ask
                )
                exchange_ask_price = self._apply_taker_check_price(
                    passive_ask_price, 'ask', local_best_bid, local_best_ask
                )

                # Place orders (only if taker check passed)
                if (config.sides in ['both', 'bid'] and
                    exchange_bid_price is not None and
                    not self._has_active_passive_order_for_symbol(line, exchange, symbol, 'bid')):
                    if self._is_unprofitable_trade('bid', exchange_bid_price):
                        self.logger.info(
                            f"Passive Line {line.config.line_id}: Skipping bid - unprofitable at price {exchange_bid_price} "
                            f"(inventory_price={self.real_inventory_price}, inventory={self.real_current_inventory})"
                        )
                    else:
                        await self._place_passive_order_for_symbol(
                            line, exchange, symbol, 'bid', exchange_bid_price, final_quantity
                        )
                elif exchange_bid_price is None and config.sides in ['both', 'bid']:
                    self.logger.warning(f"Passive Line {line.config.line_id}: Bid taker check failed for {symbol} on {exchange}")

                if (config.sides in ['both', 'offer'] and
                    exchange_ask_price is not None and
                    not self._has_active_passive_order_for_symbol(line, exchange, symbol, 'ask')):
                    if self._is_unprofitable_trade('ask', exchange_ask_price):
                        self.logger.info(
                            f"Passive Line {line.config.line_id}: Skipping ask - unprofitable at price {exchange_ask_price} "
                            f"(inventory_price={self.real_inventory_price}, inventory={self.real_current_inventory})"
                        )
                    else:
                        await self._place_passive_order_for_symbol(
                            line, exchange, symbol, 'ask', exchange_ask_price, final_quantity
                        )
                elif exchange_ask_price is None and config.sides in ['both', 'offer']:
                    self.logger.warning(f"Passive Line {line.config.line_id}: Ask taker check failed for {symbol} on {exchange}")

            # Update line state with asymmetric spreads
            line.last_reference_price[symbol] = aggregated_price
            if symbol not in line.last_spread:
                line.last_spread[symbol] = {}
            line.last_spread[symbol]['bid'] = bid_spread_bps
            line.last_spread[symbol]['ask'] = ask_spread_bps

        except Exception as e:
            self.logger.error(f"Error placing passive orders for line {line.config.line_id} and {symbol} on {exchange}: {e}")

    # Legacy methods for backward compatibility
    async def _process_tob_line(self, line: ActiveTOBLine) -> None:
        """Legacy method - process TOB line for main symbol only."""
        await self._process_tob_line_for_symbol(line, self.symbol)

    async def _process_passive_line(self, line: ActivePassiveLine) -> None:
        """Legacy method - process passive line for main symbol only."""
        await self._process_passive_line_for_symbol(line, self.symbol)

    async def _place_missing_tob_orders_for_symbol(self, line: ActiveTOBLine, exchange: str, symbol: str):
        """Place missing TOB orders for a specific exchange and symbol using local TOB pricing."""
        try:
            config = line.config

            # Enforce fresh WS inventory for TOB on every iteration
            now_ts = datetime.now(timezone.utc)
            if not self._last_ws_inventory_update or (now_ts - self._last_ws_inventory_update).total_seconds() > self.ws_fresh_window_seconds:
                self.logger.warning(
                    f"TOB Line {line.config.line_id}: Skipping TOB placement due to stale WS inventory (last={self._last_ws_inventory_update})"
                )
                return
            # Ensure we have a current inventory value
            current_inventory = self.real_current_inventory
            if current_inventory is None:
                self.logger.warning(f"TOB Line {line.config.line_id}: No WS inventory value available; skipping TOB placement")
                return

            # Get local TOB for this exchange and symbol - use appropriate market data for taker check
            base_currency, quote_currency = symbol.split('/')

            # For pairs that don't exist natively, use USDT market data
            if not self._exchange_has_native_pair(exchange, symbol):
                # Use BERA/USDT market data for taker check since that's what we're trading
                resolved_symbol = self._resolve_symbol_for_exchange_and_symbol(exchange, 'BERA/USDT')
                self.logger.info(f"🔧 TOB TAKER CHECK DEBUG: Using USDT market data for {symbol} conversion trading on {exchange}")
            else:
                # Use native market data for direct pairs (USDT everywhere, USDC only on hyperliquid_perp)
                resolved_symbol = self._resolve_symbol_for_exchange_and_symbol(exchange, symbol)
                self.logger.info(f"🔧 TOB TAKER CHECK DEBUG: Using native market data for {symbol} on {exchange}")

            if not self.market_data_service:
                self.logger.warning(f"TOB Line {line.config.line_id}: Market data service not initialized")
                return

            best_bid, best_ask = self._get_best_bid_ask_from_service(exchange, resolved_symbol)
            self.logger.info(f"🔧 TOB MARKET DATA DEBUG: {exchange} {resolved_symbol} -> bid={best_bid}, ask={best_ask}")

            if not best_bid or not best_ask:
                self.logger.info(f"TOB Line {line.config.line_id}: No market data for {exchange} {resolved_symbol} (bid={best_bid}, ask={best_ask})")
                return

            # Calculate current hourly quantity (convert from hourly rate)
            seconds_per_hour = 3600
            current_quantity = config.hourly_quantity / Decimal(str(seconds_per_hour)) * Decimal('2')  # 2s worth

            # Apply coefficient adjustments
            adjusted_quantity = current_quantity * line.current_coefficient

            # Use REAL inventory price from WS for logic
            inventory_price = self.real_inventory_price

            min_tick = Decimal('0.000001')  # Minimum tick size

            # DEBUG: Log before TOB price calculations
            self.logger.info(f"🔧 TOB PRICE DEBUG: best_bid={best_bid} (type: {type(best_bid)}), best_ask={best_ask} (type: {type(best_ask)})")
            self.logger.info(f"🔧 TOB PRICE DEBUG: min_tick={min_tick} (type: {type(min_tick)})")

            # TOB bid pricing (local bid + min tick)
            self.logger.info(f"🔧 TOB ADDITION DEBUG: best_bid + min_tick = {best_bid} + {min_tick}")
            tob_bid_price = best_bid + min_tick
            # TOB ask pricing (local ask - min tick)
            self.logger.info(f"🔧 TOB SUBTRACTION DEBUG: best_ask - min_tick = {best_ask} - {min_tick}")
            tob_ask_price = best_ask - min_tick

            # Apply inventory-based spread adjustment with ASYMMETRY
            should_place_bid = True
            should_place_offer = True

            if config.spread_from_inventory and inventory_price:
                # Compute excess and inventory coefficient [-1,1]
                target_inventory = self.strategy_config.inventory.target_inventory if (self.strategy_config and self.strategy_config.inventory) else Decimal('0')
                inv_coeff = Decimal('0')
                if target_inventory and target_inventory != 0:
                    try:
                        inv_coeff = (current_inventory - target_inventory) / abs(target_inventory)
                    except Exception:
                        inv_coeff = Decimal('0')
                    # Clamp
                    inv_coeff = max(Decimal('-1'), min(Decimal('1'), inv_coeff))
                inv_abs = abs(inv_coeff)
                # Scale factors: widen the "unwanted" side, tighten the preferred side
                # alpha in [1,2]; beta in [0.5,1]
                alpha = Decimal('1') + inv_abs
                beta = Decimal('1') - (inv_abs * Decimal('0.5'))
                base_spread_bps = config.spread_bps
                # Compute per-side spreads in bps
                if inv_coeff < 0:
                    # Short relative to target -> push offers away, bids closer
                    bid_spread_bps = base_spread_bps * beta
                    ask_spread_bps = base_spread_bps * alpha
                elif inv_coeff > 0:
                    # Long relative to target -> push bids away, asks closer
                    bid_spread_bps = base_spread_bps * alpha
                    ask_spread_bps = base_spread_bps * beta
                else:
                    bid_spread_bps = base_spread_bps
                    ask_spread_bps = base_spread_bps
                bid_spread_decimal = bid_spread_bps / Decimal('10000')
                ask_spread_decimal = ask_spread_bps / Decimal('10000')
                # Inventory-based bounds per side
                inventory_based_bid = inventory_price * (Decimal('1') - bid_spread_decimal)
                inventory_based_ask = inventory_price * (Decimal('1') + ask_spread_decimal)
                # Use the more conservative (wider) side-specific bounds
                tob_bid_price = min(tob_bid_price, inventory_based_bid)
                tob_ask_price = max(tob_ask_price, inventory_based_ask)

                # Placement conditions should reflect the asymmetric bounds
                # Only place ask if it's sufficiently above inventory_based_ask
                if tob_ask_price <= inventory_based_ask:
                    should_place_offer = False
                    self.logger.info(
                        f"TOB Line {line.config.line_id}: Not placing ask - price check failed (ask={tob_ask_price}, inv_ask_bound={inventory_based_ask})"
                    )
                # Only place bid if it's sufficiently below inventory_based_bid
                if tob_bid_price >= inventory_based_bid:
                    should_place_bid = False
                    self.logger.info(
                        f"TOB Line {line.config.line_id}: Not placing bid - price check failed (bid={tob_bid_price}, inv_bid_bound={inventory_based_bid})"
                    )

            self.logger.info(f"TOB Line {line.config.line_id}: Order placement decision - should_place_bid={should_place_bid}, should_place_offer={should_place_offer}, inventory_price={inventory_price}")

            # Final taker check after all adjustments
            final_bid_price = self._apply_taker_check_price(tob_bid_price, 'bid', best_bid, best_ask)
            final_ask_price = self._apply_taker_check_price(tob_ask_price, 'ask', best_bid, best_ask)

            # Prevent unprofitable trades if enabled
            if should_place_bid and final_bid_price is not None and self._is_unprofitable_trade('bid', final_bid_price):
                should_place_bid = False
                self.logger.info(
                    f"TOB Line {line.config.line_id}: Skipping bid - unprofitable at price {final_bid_price} "
                    f"(inventory_price={self.real_inventory_price}, inventory={self.real_current_inventory})"
                )
            if should_place_offer and final_ask_price is not None and self._is_unprofitable_trade('ask', final_ask_price):
                should_place_offer = False
                self.logger.info(
                    f"TOB Line {line.config.line_id}: Skipping ask - unprofitable at price {final_ask_price} "
                    f"(inventory_price={self.real_inventory_price}, inventory={self.real_current_inventory})"
                )

            # Enforce price bounds for TOB by skipping out-of-bounds quotes
            if config.max_price and final_bid_price is not None and final_bid_price > config.max_price:
                should_place_bid = False
                self.logger.info(
                    f"TOB Line {line.config.line_id}: Skipping bid - price {final_bid_price} > max {config.max_price}"
                )
            if config.min_price and final_ask_price is not None and final_ask_price < config.min_price:
                should_place_offer = False
                self.logger.info(
                    f"TOB Line {line.config.line_id}: Skipping ask - price {final_ask_price} < min {config.min_price}"
                )

            # Skip order placement if taker check failed
            if final_bid_price is None:
                should_place_bid = False
                self.logger.warning(f"TOB Line {line.config.line_id}: Bid taker check failed, skipping bid order")
            if final_ask_price is None:
                should_place_offer = False
                self.logger.warning(f"TOB Line {line.config.line_id}: Ask taker check failed, skipping ask order")

            # Per-order headroom checks against target inventory (every order, every iteration)
            target_inventory = self.strategy_config.inventory.target_inventory if (self.strategy_config and self.strategy_config.inventory) else Decimal('0')
            headroom_up = (target_inventory - current_inventory) if target_inventory is not None else Decimal('0')
            headroom_down = (current_inventory - target_inventory) if target_inventory is not None else Decimal('0')
            min_exec_qty = Decimal('0.00001')
            bid_quantity = adjusted_quantity
            ask_quantity = adjusted_quantity
            # For bids (buying increases inventory)
            if should_place_bid:
                if headroom_up <= Decimal('0'):
                    should_place_bid = False
                    self.logger.info(f"TOB Line {line.config.line_id}: Skipping bid - no upward headroom (current={current_inventory}, target={target_inventory})")
                elif bid_quantity > headroom_up:
                    adj = headroom_up
                    if adj < min_exec_qty:
                        should_place_bid = False
                        self.logger.info(f"TOB Line {line.config.line_id}: Skipping bid - tiny adjusted qty (headroom_up={headroom_up})")
                    else:
                        self.logger.info(f"TOB Line {line.config.line_id}: Adjusting bid qty from {bid_quantity} to {adj} to avoid exceeding target")
                        bid_quantity = adj
            # For asks (selling decreases inventory)
            if should_place_offer:
                if headroom_down <= Decimal('0'):
                    should_place_offer = False
                    self.logger.info(f"TOB Line {line.config.line_id}: Skipping ask - no downward headroom (current={current_inventory}, target={target_inventory})")
                elif ask_quantity > headroom_down:
                    adj = headroom_down
                    if adj < min_exec_qty:
                        should_place_offer = False
                        self.logger.info(f"TOB Line {line.config.line_id}: Skipping ask - tiny adjusted qty (headroom_down={headroom_down})")
                    else:
                        self.logger.info(f"TOB Line {line.config.line_id}: Adjusting ask qty from {ask_quantity} to {adj} to avoid going below target")
                        ask_quantity = adj

            # Place orders based on sides configuration and inventory logic (with taker check validation)
            if (config.sides in ['both', 'bid'] and
                should_place_bid and
                final_bid_price is not None and
                not self._has_active_tob_order_for_symbol(line, exchange, symbol, 'bid')):

                await self._place_tob_order_for_symbol(line, exchange, symbol, 'bid', final_bid_price, bid_quantity)

            if (config.sides in ['both', 'offer'] and
                should_place_offer and
                final_ask_price is not None and
                not self._has_active_tob_order_for_symbol(line, exchange, symbol, 'ask')):

                await self._place_tob_order_for_symbol(line, exchange, symbol, 'ask', final_ask_price, ask_quantity)

        except Exception as e:
            self.logger.error(f"Error placing TOB orders for line {line.config.line_id} on {exchange} for {symbol}: {e}")

    # Legacy method for backward compatibility
    async def _place_missing_tob_orders(self, line: ActiveTOBLine, exchange: str):
        """Legacy method - place TOB orders for main symbol only."""
        await self._place_missing_tob_orders_for_symbol(line, exchange, self.symbol)

    async def _place_missing_passive_orders(self, line: ActivePassiveLine):
        """Legacy method - place passive orders for main symbol only."""
        await self._place_missing_passive_orders_for_symbol(line, self.symbol)

    async def _place_missing_passive_orders_for_symbol(self, line: ActivePassiveLine, symbol: str):
        """Place missing passive orders for a specific symbol using aggregated book pricing."""
        try:
            config = line.config

            # Parse symbol for cross-currency logic
            base_currency, quote_currency = symbol.split('/')

            # Get aggregated pricing based on smart pricing configuration
            aggregated_price = await self._get_smart_pricing(symbol, 'mid', Decimal('100'))

            if not aggregated_price:
                return

            # Apply ASYMMETRIC inventory-based spread adjustment (LEGACY METHOD)
            # Get inventory coefficient for asymmetric spread calculation
            current_inventory = self.real_current_inventory if self.real_current_inventory is not None else Decimal('0')
            target_inventory = self.strategy_config.inventory.target_inventory if (self.strategy_config and self.strategy_config.inventory) else Decimal('0')
            
            # Calculate inventory coefficient using abs(target) for correct sign handling
            if target_inventory != 0:
                inv_coeff = (current_inventory - target_inventory) / abs(target_inventory)
                # Clamp to [-1, 1]
                inv_coeff = max(Decimal('-1'), min(Decimal('1'), inv_coeff))
            else:
                inv_coeff = Decimal('0')
            
            # Calculate asymmetric spreads based on inventory position
            # Logic: interpolate between min_spread and max_spread based on inventory coefficient
            # - The "push away" side (unwanted direction) goes from mid_spread toward max_spread
            # - The "pull toward" side (wanted direction) goes from mid_spread toward min_spread
            base_spread = config.mid_spread_bps
            min_spread = config.min_spread_bps
            max_spread = config.max_spread_bps
            inv_abs = abs(inv_coeff)
            
            # Interpolate spreads: at |inv_coeff|=0: both at base_spread; at |inv_coeff|=1: max deviation
            # Wide side: base_spread -> max_spread as |inv_coeff| goes 0 -> 1
            # Tight side: base_spread -> min_spread as |inv_coeff| goes 0 -> 1
            wide_spread = base_spread + (max_spread - base_spread) * inv_abs
            tight_spread = base_spread - (base_spread - min_spread) * inv_abs
            
            if inv_coeff < 0:
                # Below target (MORE SHORT than target, need to BUY to reduce short)
                # -> tighten bids (closer to mid to encourage buying)
                # -> widen asks (further from mid to discourage selling)
                bid_spread_bps = tight_spread
                ask_spread_bps = wide_spread
            elif inv_coeff > 0:
                # Above target (LESS SHORT than target, need to SELL to increase short)
                # -> widen bids (further from mid to discourage buying)
                # -> tighten asks (closer to mid to encourage selling)
                bid_spread_bps = wide_spread
                ask_spread_bps = tight_spread
            else:
                # At target - symmetric spreads
                bid_spread_bps = base_spread
                ask_spread_bps = base_spread
            
            # Enforce strict spread bounds from config (never breach min/max)
            bid_spread_bps = max(min_spread, min(max_spread, bid_spread_bps))
            ask_spread_bps = max(min_spread, min(max_spread, ask_spread_bps))
            
            # Calculate bid and ask prices with asymmetric spreads
            bid_spread_decimal = bid_spread_bps / Decimal('10000')
            ask_spread_decimal = ask_spread_bps / Decimal('10000')
            passive_bid_price = aggregated_price * (Decimal('1') - bid_spread_decimal)
            passive_ask_price = aggregated_price * (Decimal('1') + ask_spread_decimal)
            
            # Log the asymmetric spread calculation
            self.logger.info(
                f"Passive Line {config.line_id} (LEGACY): inv_coeff={float(inv_coeff):.3f}, "
                f"bid_spread={float(bid_spread_bps):.1f}bps (min={float(min_spread):.1f}), "
                f"ask_spread={float(ask_spread_bps):.1f}bps (max={float(max_spread):.1f})"
            )

            # Apply quantity adjustments
            base_quantity = config.quantity * line.current_quantity_coefficient

            # Apply randomization
            randomization = config.randomization_factor
            import random
            qty_multiplier = Decimal('1') + Decimal(str(random.uniform(-float(randomization), float(randomization))))
            final_quantity = base_quantity * qty_multiplier

            # Place orders on all exchanges
            for exchange in self.exchanges:
                # Apply taker check per exchange - use appropriate market data
                if not self._exchange_has_native_pair(exchange, symbol):
                    # Use BERA/USDT market data for taker check since that's what we're trading
                    resolved_symbol = self._resolve_symbol_for_exchange_and_symbol(exchange, 'BERA/USDT')
                    self.logger.info(f"🔧 LEGACY TAKER CHECK DEBUG: Using USDT market data for {symbol} conversion trading on {exchange}")
                else:
                    # Use native market data for direct pairs
                    resolved_symbol = self._resolve_symbol_for_exchange_and_symbol(exchange, symbol)
                    self.logger.info(f"🔧 LEGACY TAKER CHECK DEBUG: Using native market data for {symbol} on {exchange}")

                local_best_bid, local_best_ask = self._get_best_bid_ask_from_service(exchange, resolved_symbol)

                if local_best_bid and local_best_ask:
                    # Apply taker check
                    exchange_bid_price = self._apply_taker_check_price(
                        passive_bid_price, 'bid', local_best_bid, local_best_ask
                    )
                    exchange_ask_price = self._apply_taker_check_price(
                        passive_ask_price, 'ask', local_best_bid, local_best_ask
                    )

                    # Place orders (only if taker check passed)
                    if (config.sides in ['both', 'bid'] and
                        exchange_bid_price is not None and
                        not self._has_active_passive_order_for_symbol(line, exchange, symbol, 'bid')):

                        # For non-native pairs, place the order using BERA/USDT and convert quantity
                        if not self._exchange_has_native_pair(exchange, symbol):
                            converted_quantity = await self._convert_quantity_for_usdt_trading(symbol, final_quantity)
                            if converted_quantity:
                                self.logger.info(f"🔧 CONVERSION ORDER: Placing USDT bid for {symbol} -> qty={converted_quantity}")
                                await self._place_passive_order_for_symbol(
                                    line, exchange, 'BERA/USDT', 'bid', exchange_bid_price, converted_quantity
                                )
                        else:
                            self.logger.info(f"🔧 NATIVE ORDER: Placing native bid for {symbol} -> qty={final_quantity}")
                            await self._place_passive_order_for_symbol(
                                line, exchange, symbol, 'bid', exchange_bid_price, final_quantity
                            )
                    elif exchange_bid_price is None and config.sides in ['both', 'bid']:
                        self.logger.warning(f"Passive Line {line.config.line_id}: Bid taker check failed for {symbol} on {exchange}")

                    if (config.sides in ['both', 'offer'] and
                        exchange_ask_price is not None and
                        not self._has_active_passive_order_for_symbol(line, exchange, symbol, 'ask')):

                        # For non-native pairs, place the order using BERA/USDT and convert quantity
                        if not self._exchange_has_native_pair(exchange, symbol):
                            converted_quantity = await self._convert_quantity_for_usdt_trading(symbol, final_quantity)
                            if converted_quantity:
                                self.logger.info(f"🔧 CONVERSION ORDER: Placing USDT ask for {symbol} -> qty={converted_quantity}")
                                await self._place_passive_order_for_symbol(
                                    line, exchange, 'BERA/USDT', 'ask', exchange_ask_price, converted_quantity
                                )
                        else:
                            self.logger.info(f"🔧 NATIVE ORDER: Placing native ask for {symbol} -> qty={final_quantity}")
                            await self._place_passive_order_for_symbol(
                                line, exchange, symbol, 'ask', exchange_ask_price, final_quantity
                            )
                    elif exchange_ask_price is None and config.sides in ['both', 'offer']:
                        self.logger.warning(f"Passive Line {line.config.line_id}: Ask taker check failed for {symbol} on {exchange}")

            # Update line state
            line.last_reference_price[symbol] = aggregated_price
            if symbol not in line.last_spread:
                line.last_spread[symbol] = {}
            line.last_spread[symbol]['bid'] = final_spread
            line.last_spread[symbol]['ask'] = final_spread

        except Exception as e:
            self.logger.error(f"Error placing passive orders for line {line.config.line_id} and {symbol}: {e}")


    async def _get_smart_pricing(
        self,
        symbol: str,
        side: str,
        quantity: Decimal
    ) -> Optional[Decimal]:
        """
        Get aggregated pricing with cross-currency conversion support.
        """
        if not hasattr(self, 'pricing_engine') or not self.pricing_engine:
            # Fallback to Redis orderbook manager
            return await self._get_smart_pricing_fallback(symbol, side, quantity)

        self.smart_pricing_decisions += 1

        try:
            # Check if this is a pair that needs cross-currency conversion
            base_currency, quote_currency = symbol.split('/')

            # Only attempt cross-currency conversion for BASE=BERA and QUOTE!=USDT when no native pair exists
            is_base_coin_pair = (base_currency == self.strategy_config.base_coin)
            # For BERA/QUOTE where QUOTE != USDT, ALWAYS anchor to USDT combined midpoint
            # then convert back to QUOTE for pricing/placement.
            needs_conversion = (
                is_base_coin_pair and
                quote_currency != 'USDT'
            )

            if needs_conversion:
                self.logger.info(f"🔧 USDT-ANCHOR PRICING: {symbol} (anchor on {self.strategy_config.base_coin}/USDT, then convert back)")
                converted_price = await self._get_converted_pricing_via_usdt(symbol, side, quantity)
                if converted_price is not None:
                    self.logger.info(f"✅ Smart pricing (converted) for {symbol} {side}: {converted_price}")
                    return converted_price
                else:
                    self.logger.warning(f"❌ Conversion pricing failed for {symbol}, falling back to direct multi-exchange pricing")

            # Prefer direct pricing for everything else (including BTC/USDT, ETH/USDT, etc.)
            pricing_result = await self.pricing_engine.get_price(
                    symbol=symbol,
                    side=side,
                quantity=quantity
            )

            if pricing_result and pricing_result.price:
                self.logger.info(
                    f"Smart pricing (direct) for {symbol} {side}: {pricing_result.price} "
                                f"(confidence: {pricing_result.confidence:.3f}, "
                                f"exchanges: {len(pricing_result.contributing_exchanges)}) "
                    f"from exchanges: {pricing_result.contributing_exchanges}"
                )
                return pricing_result.price

            self.logger.debug(f"Smart pricing: pricing_engine returned no price for {symbol}, trying fallback")
            return await self._get_smart_pricing_fallback(symbol, side, quantity)

        except Exception as e:
            self.logger.error(f"Error getting smart pricing for {symbol}: {e}")
            # Fallback to simple Redis approach
            return await self._get_smart_pricing_fallback(symbol, side, quantity)

    async def _get_converted_pricing_via_usdt(
        self,
        symbol: str,
        side: str,
        quantity: Decimal
    ) -> Optional[Decimal]:
        """
        Get pricing for non-USDT pairs using BERA/USDT + quote/USDT conversion.

        Example: To get BERA/BTC price:
        1. Get BERA/USDT price (~2.39)
        2. Get BTC/USDT price (~67000)
        3. Convert: BERA/BTC = BERA/USDT ÷ BTC/USDT = 2.39 ÷ 67000 = ~0.000036
        """
        try:
            base_currency, quote_currency = symbol.split('/')

            # Guardrail: Only convert for base coin pairs (e.g., BERA/QUOTE). Otherwise, use direct pricing.
            if base_currency != self.strategy_config.base_coin:
                self.logger.warning(f"⏭️ Skipping conversion for non-{self.strategy_config.base_coin} symbol {symbol}")
                return None

            # Get BERA/USDT price (the main liquidity source)
            bera_usdt_result = await self.pricing_engine.get_price('BERA/USDT', side, quantity)
            if not bera_usdt_result or not bera_usdt_result.price:
                self.logger.warning(f"No BERA/USDT price available for {symbol} conversion")
                return None

            bera_usdt_price = bera_usdt_result.price

            # Get quote currency to USDT conversion rate
            if quote_currency == 'USDC':
                conversion_symbol = 'USDC/USDT'
            elif quote_currency == 'TRY':
                conversion_symbol = 'USDT/TRY'  # TRY trades as USDT/TRY
            else:
                conversion_symbol = f"{quote_currency}/USDT"

            conversion_result = await self.pricing_engine.get_price(conversion_symbol, 'mid', quantity)
            if not conversion_result or not conversion_result.price:
                self.logger.warning(f"No {conversion_symbol} rate available for {symbol} conversion")
                return None

            quote_usdt_rate = conversion_result.price

            # Handle USDT/TRY direction (rate needs to be inverted)
            if conversion_symbol == 'USDT/TRY':
                quote_usdt_rate = Decimal('1') / quote_usdt_rate
                self.logger.info(f"🔧 TRY CONVERSION: Inverted USDT/TRY rate {conversion_result.price} -> {quote_usdt_rate}")

            # Convert: BERA/quote = BERA/USDT ÷ quote/USDT
            converted_price = bera_usdt_price / quote_usdt_rate

            self.logger.info(
                f"Cross-currency conversion for {symbol}: "
                f"BERA/USDT={bera_usdt_price} ÷ {conversion_symbol}={quote_usdt_rate} = {converted_price}"
            )

            return converted_price

        except Exception as e:
            self.logger.error(f"Error in cross-currency conversion for {symbol}: {e}")
            return None

    def _exchange_has_native_pair(self, exchange: str, symbol: str) -> bool:
        """Check if an exchange has a native trading pair (not requiring conversion)."""
        # For USDT pairs, assume all spot and perp exchanges support them natively
        # This is more flexible than hardcoding specific symbols
        base, quote = symbol.split('/')
        
        if quote == 'USDT':
            # Most exchanges support USDT pairs natively
            return True
        elif quote == 'USDC':
            # USDC pairs are supported on some exchanges
            usdc_exchanges = ['binance_spot', 'bybit_spot', 'hyperliquid_perp']
            return exchange in usdc_exchanges
        else:
            # Other quote currencies may need conversion
            return False

    def _has_any_native_pair_available(self, symbol: str) -> bool:
        """Check if a symbol has native pairs available on any exchange."""
        for exchange in self.exchanges:
            if self._exchange_has_native_pair(exchange, symbol):
                return True
        return False

    async def _convert_quantity_for_usdt_trading(self, target_symbol: str, target_quantity: Decimal) -> Optional[Decimal]:
        """
        Convert quantity from target quote currency to USDT for cross-currency trading.

        Example: If we want to trade 100 BERA worth in BTC terms, but need to place USDT order:
        - target_symbol = 'BERA/BTC', target_quantity = 100 BERA
        - BTC/USDT rate = 67000
        - USDT quantity = 100 * (67000/1) = 6,700,000 USDT worth
        """
        try:
            base_currency, quote_currency = target_symbol.split('/')

            if quote_currency == 'USDT':
                return target_quantity  # No conversion needed

            # Get quote/USDT rate for conversion
            if quote_currency == 'USDC':
                conversion_symbol = 'USDC/USDT'
            elif quote_currency == 'TRY':
                conversion_symbol = 'USDT/TRY'  # TRY trades as USDT/TRY
            else:
                conversion_symbol = f"{quote_currency}/USDT"

            conversion_result = await self.pricing_engine.get_price(conversion_symbol, 'mid', Decimal('100'))

            if not conversion_result or not conversion_result.price:
                self.logger.warning(f"No {conversion_symbol} rate for quantity conversion")
                return None

            quote_usdt_rate = conversion_result.price

            # Handle USDT/TRY direction (rate needs to be inverted for quantity calc)
            if conversion_symbol == 'USDT/TRY':
                quote_usdt_rate = Decimal('1') / quote_usdt_rate
                self.logger.info(f"🔧 TRY QUANTITY CONVERSION: Inverted USDT/TRY rate for quantity calc")

            # Convert quantity: USDT_quantity = target_quantity * quote_usdt_rate
            usdt_quantity = target_quantity * quote_usdt_rate

            self.logger.info(f"Quantity conversion for {target_symbol}: "
                           f"{target_quantity} -> {usdt_quantity} USDT (rate: {quote_usdt_rate})")

            return usdt_quantity

        except Exception as e:
            self.logger.error(f"Error converting quantity for {target_symbol}: {e}")
            return None

    async def _get_smart_pricing_fallback(
        self,
        symbol: str,
        side: str,
        quantity: Decimal
    ) -> Optional[Decimal]:
        """
        Fallback pricing using Redis orderbook manager (like volume weighted strategy).
        """
        if not hasattr(self, 'redis_orderbook_manager') or not self.redis_orderbook_manager:
            return None

        try:
            # Get aggregated price across all exchanges (like volume weighted strategy)
            best_bid = None
            best_ask = None

            for exchange in self.exchanges:
                exchange_bid, exchange_ask = self._get_best_bid_ask_from_service(exchange, symbol)

                if exchange_bid and exchange_ask:
                    if best_bid is None or exchange_bid > best_bid:
                        best_bid = exchange_bid
                    if best_ask is None or exchange_ask < best_ask:
                        best_ask = exchange_ask

            if best_bid and best_ask:
                if side == 'mid':
                    return (best_bid + best_ask) / Decimal('2')
                elif side in ['bid', 'buy']:
                    return best_bid
                elif side in ['ask', 'sell']:
                    return best_ask

            self.logger.debug(f"Fallback pricing (Redis): No bid/ask found for {symbol} across exchanges (bid={best_bid}, ask={best_ask})")
            
            # Last resort: fetch directly from exchange connectors
            self.logger.debug(f"Trying direct exchange connector fallback for {symbol}")
            direct_price = await self._get_direct_connector_pricing(symbol, side, quantity)
            if direct_price:
                return direct_price
                
            return None

        except Exception as e:
            self.logger.error(f"Error getting fallback pricing for {symbol}: {e}")
            return None
    
    async def _get_direct_connector_pricing(
        self,
        symbol: str,
        side: str,
        quantity: Decimal
    ) -> Optional[Decimal]:
        """
        Last resort pricing: fetch directly from exchange connectors.
        This bypasses Redis entirely and gets live orderbook data from exchanges.
        """
        try:
            best_bid = None
            best_ask = None
            
            for exchange in self.exchanges:
                try:
                    connector = self.exchange_connectors.get(exchange)
                    if not connector:
                        continue
                    
                    # Normalize symbol for exchange
                    exchange_symbol = self._normalize_symbol_for_exchange(symbol, exchange)
                    
                    # Get orderbook directly from exchange
                    orderbook = await connector.get_orderbook(exchange_symbol)
                    if not orderbook or not orderbook.get('bids') or not orderbook.get('asks'):
                        continue
                    
                    exchange_bid = Decimal(str(orderbook['bids'][0][0])) if len(orderbook['bids']) > 0 else None
                    exchange_ask = Decimal(str(orderbook['asks'][0][0])) if len(orderbook['asks']) > 0 else None
                    
                    if exchange_bid and exchange_ask:
                        self.logger.debug(f"Direct connector pricing from {exchange}: bid={exchange_bid}, ask={exchange_ask}")
                        
                        if best_bid is None or exchange_bid > best_bid:
                            best_bid = exchange_bid
                        if best_ask is None or exchange_ask < best_ask:
                            best_ask = exchange_ask
                            
                except Exception as e:
                    self.logger.debug(f"Error getting direct pricing from {exchange}: {e}")
                    continue
            
            if best_bid and best_ask:
                self.logger.info(f"✅ Direct connector pricing for {symbol}: bid={best_bid}, ask={best_ask}")
                if side == 'mid':
                    return (best_bid + best_ask) / Decimal('2')
                elif side in ['bid', 'buy']:
                    return best_bid
                elif side in ['ask', 'sell']:
                    return best_ask
            
            self.logger.warning(f"❌ Direct connector pricing failed for {symbol}: no valid bid/ask from any exchange")
            return None
            
        except Exception as e:
            self.logger.error(f"Error in direct connector pricing for {symbol}: {e}")
            return None

    def _calculate_line_coefficient(
        self,
        method: str,
        min_coeff: Decimal,
        max_coeff: Decimal,
        exchange: Optional[str] = None
    ) -> Decimal:
        """Calculate coefficient for a line based on method using existing proven system."""
        if method == 'inventory':
            coeff = self.inventory_manager.get_inventory_coefficient()
            # Convert from [-1, +1] to [min_coeff, max_coeff]
            normalized = (coeff + Decimal('1')) / Decimal('2')  # Convert to [0, 1]
            return min_coeff + normalized * (max_coeff - min_coeff)

        elif method == 'volume':
            # Use existing proven coefficient system
            if exchange and self.coefficient_calculator:
                db_exchange_id, db_symbol = self._get_db_exchange_and_symbol(exchange)
                if db_exchange_id:
                    volume_coeff = self.coefficient_calculator.calculate_coefficient(db_symbol, db_exchange_id)
                    if volume_coeff is not None:
                        safe_volume_coeff = max(float(min_coeff), min(float(max_coeff), volume_coeff))
                        return Decimal(str(safe_volume_coeff))
                    else:
                        self.logger.info(f"Volume coefficient returned None for {exchange}, using default 1.0")
                        return Decimal('1.0')
            else:
                # Use average of all exchange coefficients
                if self.exchange_coefficients:
                    valid_coefficients = [coeff for coeff in self.exchange_coefficients.values() if coeff is not None]
                    if valid_coefficients:
                        avg_coeff = sum(valid_coefficients) / len(valid_coefficients)
                        safe_avg_coeff = max(float(min_coeff), min(float(max_coeff), avg_coeff))
                        return Decimal(str(safe_avg_coeff))
                    else:
                        return Decimal('1.0')  # Default if no valid coefficients

        elif method == 'both':
            # Combine inventory and volume coefficients
            inv_coeff = self._calculate_line_coefficient('inventory', min_coeff, max_coeff, exchange)
            vol_coeff = self._calculate_line_coefficient('volume', min_coeff, max_coeff, exchange)
            combined = (inv_coeff + vol_coeff) / Decimal('2')
            return max(min_coeff, min(max_coeff, combined))

        return Decimal('1.0')  # Default coefficient

    def _apply_inventory_spread_adjustment(self, base_spread_bps: Decimal) -> Decimal:
        """Apply inventory-based spread adjustment."""
        if not self.inventory_manager:
            return base_spread_bps

        # Use inventory manager's spread adjustment method
        max_spread = base_spread_bps * Decimal('2')  # 2x base spread as max
        return self.inventory_manager.get_spread_adjustment(base_spread_bps, max_spread)

    def _apply_taker_check_price(
        self,
        quote_price: Decimal,
        side: str,
        best_bid: Decimal,
        best_ask: Decimal
    ) -> Optional[Decimal]:
        """Apply robust taker check to ensure quotes don't cross the spread (like volume weighted strategy)."""
        if not self.strategy_config.taker_check:
            return quote_price

        # DEBUG: Log all input values for subtraction debugging
        self.logger.info(f"🔧 TAKER CHECK DEBUG: quote_price={quote_price} (type: {type(quote_price)}), side={side}")
        self.logger.info(f"🔧 TAKER CHECK DEBUG: best_bid={best_bid} (type: {type(best_bid)}), best_ask={best_ask} (type: {type(best_ask)})")

        if not best_bid or not best_ask:
            self.logger.warning(f"No best bid/ask available for taker check, using original price: {quote_price}")
            return quote_price

        try:
            original_price = quote_price
            min_tick_size = Decimal('0.000001')  # Default minimum tick size

            # DEBUG: Log before any arithmetic operations
            self.logger.info(f"🔧 ARITHMETIC DEBUG: About to do calculations with best_bid={best_bid}, best_ask={best_ask}, min_tick={min_tick_size}")

            if side == "bid":
                # For bid orders, ensure we don't exceed best ask (become taker)
                if quote_price >= best_ask:
                    # DEBUG: Log before subtraction
                    self.logger.info(f"🔧 SUBTRACTION DEBUG: best_ask - min_tick_size = {best_ask} - {min_tick_size}")
                    # Adjust to be just below best ask
                    quote_price = best_ask - min_tick_size
                    self.logger.info(f"🛡️ TAKER CHECK: Bid price {original_price} >= best ask {best_ask}, adjusted to {quote_price}")

                    # Additional safety check: ensure adjusted price doesn't exceed best bid
                    if quote_price > best_bid:
                        # DEBUG: Log before second subtraction
                        self.logger.info(f"🔧 SUBTRACTION DEBUG: best_bid - min_tick_size = {best_bid} - {min_tick_size}")
                        quote_price = best_bid - min_tick_size
                        self.logger.info(f"🛡️ TAKER CHECK: Further adjusted bid price to {quote_price} to stay below best bid")

            else:  # ask
                # For ask orders, ensure we don't go below best bid (become taker)
                if quote_price <= best_bid:
                    # Adjust to be just above best bid
                    quote_price = best_bid + min_tick_size
                    self.logger.info(f"🛡️ TAKER CHECK: Ask price {original_price} <= best bid {best_bid}, adjusted to {quote_price}")

                    # Additional safety check: ensure adjusted price doesn't go below best ask
                    if quote_price < best_ask:
                        quote_price = best_ask + min_tick_size
                        self.logger.info(f"🛡️ TAKER CHECK: Further adjusted ask price to {quote_price} to stay above best ask")

            # Final validation: ensure we haven't crossed the spread
            if side == "bid" and quote_price >= best_ask:
                self.logger.error(f"🚨 TAKER CHECK FAILED: Final bid price {quote_price} still >= best ask {best_ask}")
                return None  # Don't place order
            elif side == "ask" and quote_price <= best_bid:
                self.logger.error(f"🚨 TAKER CHECK FAILED: Final ask price {quote_price} still <= best bid {best_bid}")
                return None  # Don't place order

            return quote_price

        except Exception as e:
            self.logger.error(f"Error in taker check: {e}")
        return quote_price

    # Helper methods for order management

    def _has_active_tob_order(self, line: ActiveTOBLine, exchange: str, side: str) -> bool:
        """Check if TOB line has active order on exchange for side (legacy method for main symbol)."""
        return self._has_active_tob_order_for_symbol(line, exchange, self.symbol, side)

    def _has_active_passive_order(self, line: ActivePassiveLine, exchange: str, side: str) -> bool:
        """Check if passive line has active order on exchange for side (legacy method for main symbol)."""
        return self._has_active_passive_order_for_symbol(line, exchange, self.symbol, side)

    async def _place_tob_order(
        self,
        line: ActiveTOBLine,
        exchange: str,
        side: str,
        price: Decimal,
        quantity: Decimal
    ):
        """Place a TOB order (legacy method for main symbol)."""
        await self._place_tob_order_for_symbol(line, exchange, self.symbol, side, price, quantity)

    async def _place_passive_order(
        self,
        line: ActivePassiveLine,
        exchange: str,
        side: str,
        price: Decimal,
        quantity: Decimal
    ):
        """Place a passive order (legacy method for main symbol)."""
        await self._place_passive_order_for_symbol(line, exchange, self.symbol, side, price, quantity)

    def _resolve_symbol_for_exchange(self, exchange: str) -> str:
        """Resolve symbol for specific exchange."""
        # Use same logic as base strategy
        return self._normalize_symbol_for_exchange(self.symbol, exchange)

    def _resolve_symbol_for_exchange_and_symbol(self, exchange: str, symbol: str) -> str:
        """Resolve any symbol for specific exchange."""
        return self._normalize_symbol_for_exchange(symbol, exchange)

    def _has_active_tob_order_for_symbol(self, line: ActiveTOBLine, exchange: str, symbol: str, side: str) -> bool:
        """Check if TOB line has active order on exchange for symbol and side."""
        return (symbol in line.active_orders and
                exchange in line.active_orders[symbol] and
                side in line.active_orders[symbol][exchange] and
                line.active_orders[symbol][exchange][side] in self.active_orders)

    async def _place_tob_order_for_symbol(
        self,
        line: ActiveTOBLine,
        exchange: str,
        symbol: str,
        side: str,
        price: Decimal,
        quantity: Decimal
    ):
        """Place a TOB order for a specific symbol after cancelling any existing order."""
        try:
            # CRITICAL: Cancel any existing order for this line/exchange/symbol/side first
            if (symbol in line.active_orders and
                exchange in line.active_orders[symbol] and
                side in line.active_orders[symbol][exchange]):

                existing_order_id = line.active_orders[symbol][exchange][side]
                self.logger.info(f"TOB Line {line.config.line_id}: Cancelling existing {side} order {existing_order_id} on {exchange} for {symbol}")
                await self._cancel_tob_order_for_symbol(line, exchange, symbol, side, 'replace')

            order_side = 'buy' if side == 'bid' else 'sell'

            # Capture local midpoint for this exchange at placement time for drift tracking
            try:
                resolved_symbol_for_mid = self._resolve_symbol_for_exchange_and_symbol(exchange, symbol)
                mid_bid, mid_ask = self._get_best_bid_ask_from_service(exchange, resolved_symbol_for_mid)
                if mid_bid and mid_ask:
                    if symbol not in line.last_midpoint_per_exchange:
                        line.last_midpoint_per_exchange[symbol] = {}
                    line.last_midpoint_per_exchange[symbol][exchange] = (mid_bid + mid_ask) / Decimal('2')
            except Exception:
                # Non-fatal if we can't seed mid; drift check will seed on next cycle
                pass

            # Generate short client order ID for exchanges with limits (Gate.io max 28 chars)
            timestamp_suffix = str(int(time.time() * 1000))[-6:]  # Last 6 digits
            symbol_short = symbol.replace('/', '')[:4]  # First 4 chars of symbol
            client_order_id = f"t{line.config.line_id}{symbol_short}{side[0]}{timestamp_suffix}"  # Max ~15 chars

            # Use the specific symbol for order placement
            order_id = await self._place_order_for_symbol(
                exchange=exchange,
                symbol=symbol,
                side=order_side,
                amount=float(quantity),
                price=float(price),
                client_order_id=client_order_id
            )

            if order_id:
                # Track order in line state
                if symbol not in line.active_orders:
                    line.active_orders[symbol] = {}
                if exchange not in line.active_orders[symbol]:
                    line.active_orders[symbol][exchange] = {}
                if symbol not in line.placed_at:
                    line.placed_at[symbol] = {}
                if exchange not in line.placed_at[symbol]:
                    line.placed_at[symbol][exchange] = {}
                if symbol not in line.last_prices:
                    line.last_prices[symbol] = {}
                if exchange not in line.last_prices[symbol]:
                    line.last_prices[symbol][exchange] = {}

                line.active_orders[symbol][exchange][side] = order_id
                line.placed_at[symbol][exchange][side] = datetime.now(timezone.utc)
                line.last_prices[symbol][exchange][side] = price

                self.orders_placed_tob += 1

                self.logger.info(
                    f"TOB Line {line.config.line_id}: Placed {side} order for {symbol} @ {price:.4f} "
                    f"qty={quantity:.8f} on {exchange} (coeff: {line.current_coefficient:.3f})"
                )

        except Exception as e:
            self.logger.error(f"Error placing TOB order for {symbol}: {e}")

    async def _place_order_for_symbol(
        self,
        exchange: str,
        symbol: str,
        side: str,
        amount: float,
        price: float,
        client_order_id: str
    ) -> Optional[str]:
        """Place order for any symbol by directly using connector."""
        try:
            # Get exchange connector
            connector = self.exchange_connectors.get(exchange)
            if not connector:
                self.logger.warning(f"No connector available for {exchange}")
                return None

            # Normalize symbol for this exchange
            exchange_symbol = self._normalize_symbol_for_exchange(symbol, exchange)

            # Enhanced logging for order placement debugging
            self.logger.info(f"🔧 ORDER PLACEMENT DEBUG: {exchange} {symbol} -> {exchange_symbol}, "
                           f"side={side}, amount={amount:.8f}, price={price:.8f}")

            # Validate price ranges to avoid exchange filter failures
            if price <= 0:
                self.logger.error(f"❌ Invalid price {price} for {exchange_symbol} on {exchange}")
                return None

            # Check for extreme prices that might hit exchange limits
            if exchange == 'binance_spot':
                if price > 1000000:  # Binance max price check
                    self.logger.error(f"❌ Price {price} too high for Binance {exchange_symbol}")
                    return None
                if price < 0.00000001:  # Binance min price check
                    self.logger.error(f"❌ Price {price} too low for Binance {exchange_symbol}")
                    return None

            # Use connector directly (like base strategy does)
            from decimal import Decimal
            order_result = await connector.place_order(
                symbol=exchange_symbol,
                side=side,
                amount=Decimal(str(amount)),
                price=Decimal(str(price)),
                order_type='limit',
                params={'clientOrderId': client_order_id} if client_order_id else {}
            )

            if order_result and order_result.get('id'):
                order_id = order_result['id']

                # Add to active orders tracking
                self.active_orders[order_id] = {
                    'id': order_id,
                    'symbol': symbol,  # Store the original symbol
                    'exchange': exchange,
                    'exchange_symbol': exchange_symbol,
                    'side': side,
                    'amount': amount,
                    'filled_amount': 0.0,
                    'price': price,
                    'status': 'open',
                    'client_order_id': client_order_id,
                    'created_at': datetime.now(timezone.utc),
                    'instance_id': self.instance_id
                }

                return order_id

        except Exception as e:
            error_msg = str(e)

            # Handle specific exchange errors with better messaging
            if "insufficient balance" in error_msg.lower():
                self.logger.warning(f"⚠️ Insufficient balance for {symbol} on {exchange}: {e}")
            elif "symbol not support" in error_msg.lower():
                self.logger.warning(f"⚠️ Symbol {symbol} not supported on {exchange}: {e}")
            elif "price is over" in error_msg.lower() or "maximum price" in error_msg.lower():
                self.logger.warning(f"⚠️ Price too high for {symbol} on {exchange}: {e}")
            elif "rate limited" in error_msg.lower() or "429" in error_msg:
                self.logger.warning(f"⚠️ Rate limited on {exchange}: {e}")
            elif "deserialize" in error_msg.lower() or "422" in error_msg:
                self.logger.warning(f"⚠️ Order format error on {exchange}: {e}")
            else:
                self.logger.error(f"Error placing order for {symbol} on {exchange}: {e}")

            return None

    async def _check_tob_timeouts_for_symbol(self, line: ActiveTOBLine, symbol: str):
        """Check for TOB line order timeouts for a specific symbol (batched version - legacy)."""
        now = datetime.now(timezone.utc)
        timeout_delta = timedelta(seconds=line.config.timeout_seconds)

        if symbol not in line.active_orders:
            return

        # Batch all timeout cancellations
        cancel_tasks = []
        for exchange in list(line.active_orders[symbol].keys()):
            for side in ['bid', 'ask']:
                placed_time = line.placed_at.get(symbol, {}).get(exchange, {}).get(side)
                if placed_time and (now - placed_time) > timeout_delta:
                    self.logger.info(
                        f"TOB Line {line.config.line_id}: Cancelling {side} on {exchange} for {symbol} due to TIMEOUT. "
                        f"Age={(now - placed_time).total_seconds():.1f}s > {line.config.timeout_seconds}s"
                    )
                    cancel_tasks.append(self._cancel_tob_order_for_symbol(line, exchange, symbol, side, 'timeout'))
        
        if cancel_tasks:
            await asyncio.gather(*cancel_tasks, return_exceptions=True)

    async def _check_tob_timeouts_for_symbol_sequential(self, line: ActiveTOBLine, symbol: str):
        """Check for TOB line order timeouts - cancel and replace ONE at a time to maintain liquidity."""
        now = datetime.now(timezone.utc)
        timeout_delta = timedelta(seconds=line.config.timeout_seconds)

        if symbol not in line.active_orders:
            return

        for exchange in list(line.active_orders[symbol].keys()):
            for side in ['bid', 'ask']:
                placed_time = line.placed_at.get(symbol, {}).get(exchange, {}).get(side)
                if placed_time and (now - placed_time) > timeout_delta:
                    self.logger.info(
                        f"TOB Line {line.config.line_id}: Cancelling {side} on {exchange} for {symbol} due to TIMEOUT. "
                        f"Age={(now - placed_time).total_seconds():.1f}s > {line.config.timeout_seconds}s"
                    )
                    # Cancel this single order
                    await self._cancel_tob_order_for_symbol(line, exchange, symbol, side, 'timeout')
                    # Small delay to allow replacement order to be placed in next cycle
                    await asyncio.sleep(0.001)  # 1ms

    async def _check_tob_drift_for_symbol(self, line: ActiveTOBLine, symbol: str):
        """Check TOB drift per exchange using the last per-exchange midpoint stored at placement time."""
        if symbol not in line.active_orders:
            return

        for exchange in list(line.active_orders[symbol].keys()):
            resolved_symbol = self._resolve_symbol_for_exchange_and_symbol(exchange, symbol)
            best_bid, best_ask = self._get_best_bid_ask_from_service(exchange, resolved_symbol)
            if not best_bid or not best_ask:
                continue

            current_local_mid = (best_bid + best_ask) / Decimal('2')

            last_mid = line.last_midpoint_per_exchange.get(symbol, {}).get(exchange)
            if last_mid is None:
                # Seed midpoint and skip cancellation this cycle
                if symbol not in line.last_midpoint_per_exchange:
                    line.last_midpoint_per_exchange[symbol] = {}
                line.last_midpoint_per_exchange[symbol][exchange] = current_local_mid
                continue

            price_diff = current_local_mid - last_mid
            drift_pct = abs(price_diff / last_mid) * 100
            drift_bps = drift_pct * 100

            if drift_bps > line.config.drift_bps:
                self.logger.info(
                    f"TOB Line {line.config.line_id}: Cancelling orders on {exchange} for {symbol} due to LOCAL DRIFT. "
                    f"Drift: {drift_bps:.2f} bps (allowed: {line.config.drift_bps:.2f}), "
                    f"current_local_mid={current_local_mid}, last_mid={last_mid}"
                )
                # Batch cancellations for both sides
                cancel_tasks = []
                for side in ['bid', 'ask']:
                    if side in line.active_orders.get(symbol, {}).get(exchange, {}):
                        cancel_tasks.append(self._cancel_tob_order_for_symbol(line, exchange, symbol, side, 'drift'))
                if cancel_tasks:
                    await asyncio.gather(*cancel_tasks, return_exceptions=True)
                # Update midpoint after cancellation to avoid repeated cancels
                line.last_midpoint_per_exchange[symbol][exchange] = current_local_mid

    async def _check_tob_competitiveness_for_symbol(self, line: ActiveTOBLine, symbol: str):
        """Cancel TOB orders per exchange if no longer at the intended top-of-book price."""
        try:
            if symbol not in line.active_orders:
                return
            min_tick = Decimal('0.000001')
            for exchange in list(line.active_orders[symbol].keys()):
                resolved_symbol = self._resolve_symbol_for_exchange_and_symbol(exchange, symbol)
                best_bid, best_ask = self._get_best_bid_ask_from_service(exchange, resolved_symbol)
                if not best_bid or not best_ask:
                    continue
                intended_bid = best_bid + min_tick
                intended_ask = best_ask - min_tick

                # Compare against last placed prices stored on the line
                last_bid_price = line.last_prices.get(symbol, {}).get(exchange, {}).get('bid')
                last_ask_price = line.last_prices.get(symbol, {}).get(exchange, {}).get('ask')

                # Batch cancellations for competitiveness
                cancel_tasks = []
                if last_bid_price is not None and abs(last_bid_price - intended_bid) > Decimal('0'):
                    self.logger.info(
                        f"TOB Line {line.config.line_id}: Replacing BID on {exchange} for {symbol} (last={last_bid_price}, intended={intended_bid})"
                    )
                    cancel_tasks.append(self._cancel_tob_order_for_symbol(line, exchange, symbol, 'bid', 'replace'))

                if last_ask_price is not None and abs(last_ask_price - intended_ask) > Decimal('0'):
                    self.logger.info(
                        f"TOB Line {line.config.line_id}: Replacing ASK on {exchange} for {symbol} (last={last_ask_price}, intended={intended_ask})"
                    )
                    cancel_tasks.append(self._cancel_tob_order_for_symbol(line, exchange, symbol, 'ask', 'replace'))
                
                if cancel_tasks:
                    await asyncio.gather(*cancel_tasks, return_exceptions=True)
        except Exception as e:
            self.logger.error(f"Error in TOB competitiveness check for {symbol}: {e}")

    async def _check_passive_timeouts_for_symbol(self, line: ActivePassiveLine, symbol: str):
        """Check for passive line order timeouts for a specific symbol (batched version - legacy)."""
        now = datetime.now(timezone.utc)
        timeout_delta = timedelta(seconds=line.config.timeout_seconds)

        if symbol not in line.active_orders:
            return

        # Batch all timeout cancellations
        cancel_tasks = []
        for exchange in list(line.active_orders[symbol].keys()):
            for side in ['bid', 'ask']:
                placed_time = line.placed_at.get(symbol, {}).get(exchange, {}).get(side)
                if placed_time and (now - placed_time) > timeout_delta:
                    self.logger.info(
                        f"Passive Line {line.config.line_id}: Cancelling {side} on {exchange} for {symbol} due to TIMEOUT. "
                        f"Age={(now - placed_time).total_seconds():.1f}s > {line.config.timeout_seconds}s"
                    )
                    cancel_tasks.append(self._cancel_passive_order_for_symbol(line, exchange, symbol, side, 'timeout'))
        
        if cancel_tasks:
            await asyncio.gather(*cancel_tasks, return_exceptions=True)

    async def _check_passive_timeouts_for_symbol_sequential(self, line: ActivePassiveLine, symbol: str):
        """Check for passive line order timeouts - cancel and replace ONE at a time to maintain liquidity."""
        now = datetime.now(timezone.utc)
        timeout_delta = timedelta(seconds=line.config.timeout_seconds)

        if symbol not in line.active_orders:
            return

        for exchange in list(line.active_orders[symbol].keys()):
            for side in ['bid', 'ask']:
                placed_time = line.placed_at.get(symbol, {}).get(exchange, {}).get(side)
                if placed_time and (now - placed_time) > timeout_delta:
                    self.logger.info(
                        f"Passive Line {line.config.line_id}: Cancelling {side} on {exchange} for {symbol} due to TIMEOUT. "
                        f"Age={(now - placed_time).total_seconds():.1f}s > {line.config.timeout_seconds}s"
                    )
                    # Cancel this single order
                    await self._cancel_passive_order_for_symbol(line, exchange, symbol, side, 'timeout')
                    # Small delay to allow replacement order to be placed in next cycle
                    await asyncio.sleep(0.001)  # 1ms

    async def _check_passive_timeouts_for_exchange_symbol(self, line: ActivePassiveLine, exchange: str, symbol: str):
        """Check passive line order timeouts for a specific exchange and symbol."""
        now = datetime.now(timezone.utc)
        timeout_delta = timedelta(seconds=line.config.timeout_seconds)

        placed_times = line.placed_at.get(symbol, {}).get(exchange, {})
        if not placed_times:
            return

        for side in ['bid', 'ask']:
            placed_time = placed_times.get(side)
            if placed_time and (now - placed_time) > timeout_delta:
                self.logger.info(
                    f"Passive Line {line.config.line_id}: Cancelling {side} on {exchange} for {symbol} due to TIMEOUT. "
                    f"Age={(now - placed_time).total_seconds():.1f}s > {line.config.timeout_seconds}s"
                )
                await self._cancel_passive_order_for_symbol(line, exchange, symbol, side, 'timeout')

    async def _check_passive_drift_for_exchange_symbol(self, line: ActivePassiveLine, exchange: str, symbol: str):
        """Check passive drift for a specific exchange and symbol using local midpoint."""
        try:
            if symbol not in line.active_orders:
                return

            resolved_symbol = self._resolve_symbol_for_exchange_and_symbol(exchange, symbol)
            best_bid, best_ask = self._get_best_bid_ask_from_service(exchange, resolved_symbol)
            if not best_bid or not best_ask:
                return

            current_local_mid = (best_bid + best_ask) / Decimal('2')

            last_mid = line.last_midpoint_per_exchange.get(symbol, {}).get(exchange)
            if last_mid is None:
                if symbol not in line.last_midpoint_per_exchange:
                    line.last_midpoint_per_exchange[symbol] = {}
                line.last_midpoint_per_exchange[symbol][exchange] = current_local_mid
                return

            price_diff = current_local_mid - last_mid
            drift_pct = abs(price_diff / last_mid) * 100
            drift_bps = drift_pct * 100

            if drift_bps > line.config.drift_bps:
                self.logger.info(
                    f"Passive Line {line.config.line_id}: Cancelling orders on {exchange} for {symbol} due to DRIFT. "
                    f"Drift: {drift_bps:.2f} bps (allowed: {line.config.drift_bps:.2f}), "
                    f"current_local_mid={current_local_mid}, last_mid={last_mid}"
                )
                cancel_tasks = []
                for side in ['bid', 'ask']:
                    if side in line.active_orders.get(symbol, {}).get(exchange, {}):
                        cancel_tasks.append(self._cancel_passive_order_for_symbol(line, exchange, symbol, side, 'drift'))
                if cancel_tasks:
                    await asyncio.gather(*cancel_tasks, return_exceptions=True)
                line.last_midpoint_per_exchange[symbol][exchange] = current_local_mid
        except Exception as e:
            self.logger.error(f"Error in passive drift check for {symbol} on {exchange}: {e}")

    async def _check_passive_drift_for_symbol(self, line: ActivePassiveLine, symbol: str):
        """Check passive drift per exchange using the last per-exchange midpoint stored at placement time."""
        try:
            if symbol not in line.active_orders:
                return
            # Compute current aggregated mid used for reference logging (optional)
            current_price = await self._get_smart_pricing(symbol, 'mid', Decimal('100'))
            last_ref_price = line.last_reference_price.get(symbol)
            if current_price and last_ref_price:
                self.logger.info(
                    f"🔧 PASSIVE DRIFT DEBUG (agg): current={current_price}, last_ref={last_ref_price}"
                )

            # Iterate per exchange and compare current local mid against stored mid for that exchange
            for exchange in list(line.active_orders.get(symbol, {}).keys()):
                # Determine resolved symbol and current local mid
                resolved_symbol = self._resolve_symbol_for_exchange_and_symbol(exchange, symbol)
                best_bid, best_ask = self._get_best_bid_ask_from_service(exchange, resolved_symbol)
                if not best_bid or not best_ask:
                    continue
                current_local_mid = (best_bid + best_ask) / Decimal('2')

                # Get last stored local midpoint for this exchange
                last_mid = line.last_midpoint_per_exchange.get(symbol, {}).get(exchange)
                if last_mid is None:
                    # If missing, seed it now and skip cancellation this cycle
                    if symbol not in line.last_midpoint_per_exchange:
                        line.last_midpoint_per_exchange[symbol] = {}
                    line.last_midpoint_per_exchange[symbol][exchange] = current_local_mid
                    continue

                # Compute drift in bps vs last mid used during placement
                price_diff = current_local_mid - last_mid
                drift_pct = abs(price_diff / last_mid) * 100
                drift_bps = drift_pct * 100

                if drift_bps > line.config.drift_bps:
                    self.logger.info(
                        f"Passive Line {line.config.line_id}: Cancelling orders on {exchange} for {symbol} due to DRIFT. "
                        f"Drift: {drift_bps:.2f} bps (allowed: {line.config.drift_bps:.2f}), "
                        f"current_local_mid={current_local_mid}, last_mid={last_mid}"
                    )
                    # Cancel only this exchange's passive orders for the symbol - batch cancellations
                    cancel_tasks = []
                    for side in ['bid', 'ask']:
                        if side in line.active_orders.get(symbol, {}).get(exchange, {}):
                            cancel_tasks.append(self._cancel_passive_order_for_symbol(line, exchange, symbol, side, 'drift'))
                    if cancel_tasks:
                        await asyncio.gather(*cancel_tasks, return_exceptions=True)
                    # Update reference midpoint after cancellation to avoid repeated cancels
                    line.last_midpoint_per_exchange[symbol][exchange] = current_local_mid
        except Exception as e:
            self.logger.error(f"Error in passive drift check for {symbol}: {e}")

    async def _check_all_drift_batched(self, trading_symbols: List[str]):
        """
        Check all orders for drift and cancel in batches of 5, prioritizing orders 
        closest to mid (smallest spread) since those are most at risk of being taken.
        
        This maintains liquidity by never removing more than 5 orders at a time.
        """
        try:
            # Collect all drift candidates with their spread distance from mid
            drift_candidates = []  # List of (spread_bps, line, exchange, symbol, side, line_type, current_mid)
            
            # Check TOB lines for drift
            for line in self.active_tob_lines.values():
                for symbol in trading_symbols:
                    if symbol not in line.active_orders:
                        continue
                    
                    for exchange in list(line.active_orders[symbol].keys()):
                        resolved_symbol = self._resolve_symbol_for_exchange_and_symbol(exchange, symbol)
                        best_bid, best_ask = self._get_best_bid_ask_from_service(exchange, resolved_symbol)
                        if not best_bid or not best_ask:
                            continue
                        
                        current_local_mid = (best_bid + best_ask) / Decimal('2')
                        last_mid = line.last_midpoint_per_exchange.get(symbol, {}).get(exchange)
                        
                        if last_mid is None:
                            # Seed midpoint
                            if symbol not in line.last_midpoint_per_exchange:
                                line.last_midpoint_per_exchange[symbol] = {}
                            line.last_midpoint_per_exchange[symbol][exchange] = current_local_mid
                            continue
                        
                        price_diff = current_local_mid - last_mid
                        drift_pct = abs(price_diff / last_mid) * 100
                        drift_bps = drift_pct * 100
                        
                        if drift_bps > line.config.drift_bps:
                            # Calculate spread from mid for each side
                            for side in ['bid', 'ask']:
                                if side in line.active_orders.get(symbol, {}).get(exchange, {}):
                                    # Get order price to calculate spread
                                    order_price = line.last_prices.get(symbol, {}).get(exchange, {}).get(side)
                                    if order_price:
                                        spread_from_mid = abs(order_price - current_local_mid)
                                        spread_bps = (spread_from_mid / current_local_mid) * 10000
                                    else:
                                        spread_bps = Decimal('9999')  # Unknown spread, process last
                                    
                                    drift_candidates.append((
                                        spread_bps, line, exchange, symbol, side, 'tob', current_local_mid, drift_bps
                                    ))
            
            if not drift_candidates:
                return
            
            # Sort by spread (smallest first - closest to mid, highest risk)
            drift_candidates.sort(key=lambda x: x[0])
            
            self.logger.info(f"🔄 DRIFT: Found {len(drift_candidates)} orders needing cancellation, processing in batches of 5")
            
            # Process in batches of 5
            batch_size = 5
            for i in range(0, len(drift_candidates), batch_size):
                batch = drift_candidates[i:i + batch_size]
                cancel_tasks = []
                
                for spread_bps, line, exchange, symbol, side, line_type, current_mid, drift_bps in batch:
                    self.logger.info(
                        f"DRIFT batch {i//batch_size + 1}: Cancelling {line_type} {side} on {exchange} for {symbol} "
                        f"(spread={float(spread_bps):.1f}bps, drift={float(drift_bps):.1f}bps)"
                    )
                    
                    if line_type == 'tob':
                        cancel_tasks.append(self._cancel_tob_order_for_symbol(line, exchange, symbol, side, 'drift'))
                    
                    # Update midpoint after scheduling cancellation
                    if symbol not in line.last_midpoint_per_exchange:
                        line.last_midpoint_per_exchange[symbol] = {}
                    line.last_midpoint_per_exchange[symbol][exchange] = current_mid
                
                # Execute this batch
                if cancel_tasks:
                    await asyncio.gather(*cancel_tasks, return_exceptions=True)
                    # Small delay between batches to allow replacements
                    if i + batch_size < len(drift_candidates):
                        await asyncio.sleep(0.005)  # 5ms between batches
                        
        except Exception as e:
            self.logger.error(f"Error in batched drift check: {e}")

    def _has_active_passive_order_for_symbol(self, line: ActivePassiveLine, exchange: str, symbol: str, side: str) -> bool:
        """Check if passive line has active order on exchange for symbol and side."""
        return (symbol in line.active_orders and
                exchange in line.active_orders[symbol] and
                side in line.active_orders[symbol][exchange] and
                line.active_orders[symbol][exchange][side] in self.active_orders)

    async def _place_passive_order_for_symbol(
        self,
        line: ActivePassiveLine,
        exchange: str,
        symbol: str,
        side: str,
        price: Decimal,
        quantity: Decimal
    ):
        """Place a passive order for a specific symbol after cancelling any existing order."""
        try:
            # CRITICAL: Cancel any existing order for this line/exchange/symbol/side first
            if (symbol in line.active_orders and
                exchange in line.active_orders[symbol] and
                side in line.active_orders[symbol][exchange]):

                existing_order_id = line.active_orders[symbol][exchange][side]
                self.logger.info(f"Passive Line {line.config.line_id}: Cancelling existing {side} order {existing_order_id} on {exchange} for {symbol}")
                await self._cancel_passive_order_for_symbol(line, exchange, symbol, side, 'replace')

            order_side = 'buy' if side == 'bid' else 'sell'

            # Generate short client order ID for exchanges with limits (Gate.io max 28 chars)
            timestamp_suffix = str(int(time.time() * 1000))[-6:]  # Last 6 digits
            symbol_short = symbol.replace('/', '')[:4]  # First 4 chars of symbol
            client_order_id = f"p{line.config.line_id}{symbol_short}{side[0]}{timestamp_suffix}"  # Max ~15 chars

            order_id = await self._place_order_for_symbol(
                exchange=exchange,
                symbol=symbol,
                side=order_side,
                amount=float(quantity),
                price=float(price),
                client_order_id=client_order_id
            )

            if order_id:
                # Track order in line state
                if symbol not in line.active_orders:
                    line.active_orders[symbol] = {}
                if exchange not in line.active_orders[symbol]:
                    line.active_orders[symbol][exchange] = {}
                if symbol not in line.placed_at:
                    line.placed_at[symbol] = {}
                if exchange not in line.placed_at[symbol]:
                    line.placed_at[symbol][exchange] = {}

                line.active_orders[symbol][exchange][side] = order_id
                line.placed_at[symbol][exchange][side] = datetime.now(timezone.utc)

                self.orders_placed_passive += 1

                self.logger.info(
                    f"Passive Line {line.config.line_id}: Placed {side} order for {symbol} @ {price:.4f} "
                    f"qty={quantity:.8f} on {exchange} (spread_coeff: {line.current_spread_coefficient:.3f}, "
                    f"qty_coeff: {line.current_quantity_coefficient:.3f})"
                )

        except Exception as e:
            self.logger.error(f"Error placing passive order for {symbol}: {e}")

    async def _cancel_tob_order_for_symbol(self, line: ActiveTOBLine, exchange: str, symbol: str, side: str, reason: str):
        """Cancel specific TOB order for a symbol with proper cleanup."""
        try:
            order_id = line.active_orders.get(symbol, {}).get(exchange, {}).get(side)
            if order_id:
                # Attempt cancellation regardless of base tracking to avoid stranded orders
                if order_id not in self.active_orders:
                    self.logger.warning(f"TOB order {order_id} not tracked in base; attempting direct cancel on {exchange}")
                success = await self._cancel_order(order_id, exchange)
                if success:
                    self.logger.info(f"TOB Line {line.config.line_id}: Cancelled {side} order {order_id} on {exchange} for {symbol} ({reason})")
                    self._cleanup_tob_order_tracking(line, exchange, symbol, side)
                    # Track cancellation stats
                    if reason == 'timeout':
                        self.orders_cancelled_timeout += 1
                    elif reason == 'drift':
                        self.orders_cancelled_drift += 1
                    elif reason == 'replace':
                        self.orders_cancelled_replace += 1
                else:
                    self.logger.warning(f"Failed to cancel TOB order {order_id} on {exchange} - might have been filled")

        except Exception as e:
            self.logger.error(f"Error cancelling TOB order on {exchange} for {symbol}: {e}")
            # Clean up tracking even on error
            self._cleanup_tob_order_tracking(line, exchange, symbol, side)

    def _cleanup_tob_order_tracking(self, line: ActiveTOBLine, exchange: str, symbol: str, side: str):
        """Clean up TOB order tracking for a specific order."""
        try:
            if (symbol in line.active_orders and
                exchange in line.active_orders[symbol] and
                side in line.active_orders[symbol][exchange]):
                del line.active_orders[symbol][exchange][side]

            if (symbol in line.placed_at and
                exchange in line.placed_at[symbol] and
                side in line.placed_at[symbol][exchange]):
                del line.placed_at[symbol][exchange][side]

            if (symbol in line.last_prices and
                exchange in line.last_prices[symbol] and
                side in line.last_prices[symbol][exchange]):
                del line.last_prices[symbol][exchange][side]

        except KeyError:
            # Already cleaned up or never existed
            pass

    async def _cancel_passive_order_for_symbol(self, line: ActivePassiveLine, exchange: str, symbol: str, side: str, reason: str):
        """Cancel specific passive order for a symbol with proper cleanup."""
        try:
            order_id = line.active_orders.get(symbol, {}).get(exchange, {}).get(side)
            if order_id:
                # Attempt cancellation regardless of base tracking to avoid stranded orders
                if order_id not in self.active_orders:
                    self.logger.warning(f"Passive order {order_id} not tracked in base; attempting direct cancel on {exchange}")
                success = await self._cancel_order(order_id, exchange)
                if success:
                    self.logger.info(f"Passive Line {line.config.line_id}: Cancelled {side} order {order_id} on {exchange} for {symbol} ({reason})")
                    self._cleanup_passive_order_tracking(line, exchange, symbol, side)
                    # Track cancellation stats
                    if reason == 'timeout':
                        self.orders_cancelled_timeout += 1
                    elif reason == 'drift':
                        self.orders_cancelled_drift += 1
                    elif reason == 'replace':
                        self.orders_cancelled_replace += 1
                else:
                    self.logger.warning(f"Failed to cancel passive order {order_id} on {exchange} - might have been filled")

        except Exception as e:
            self.logger.error(f"Error cancelling passive order on {exchange} for {symbol}: {e}")
            # Clean up tracking even on error
            self._cleanup_passive_order_tracking(line, exchange, symbol, side)

    def _cleanup_passive_order_tracking(self, line: ActivePassiveLine, exchange: str, symbol: str, side: str):
        """Clean up passive order tracking for a specific order."""
        try:
            if (symbol in line.active_orders and
                exchange in line.active_orders[symbol] and
                side in line.active_orders[symbol][exchange]):
                del line.active_orders[symbol][exchange][side]

            if (symbol in line.placed_at and
                exchange in line.placed_at[symbol] and
                side in line.placed_at[symbol][exchange]):
                del line.placed_at[symbol][exchange][side]

        except KeyError:
            # Already cleaned up or never existed
            pass

    async def _check_all_order_timeouts_and_drift(self):
        """Check all active orders for timeouts and drift.
        
        LIQUIDITY PRESERVATION STRATEGY:
        - Timeouts: Cancel and replace ONE order at a time to maintain liquidity
        - Drift: Cancel in batches of 5, prioritizing orders closest to mid (highest risk)
        
        This ensures we never remove all liquidity from the book at once.
        """
        try:
            # Get trading symbols only (exclude conversion-only symbols)
            if not hasattr(self, '_cached_required_symbols'):
                self._cached_required_symbols = self._get_required_symbols()
            all_symbols = self._cached_required_symbols
            all_primary_symbols = [s for s in all_symbols if s.startswith(self.strategy_config.base_coin)]

            # Filter to only symbols we actually trade
            config_quote_currencies = self.strategy_config.quote_currencies
            trading_symbols = []
            for symbol in all_primary_symbols:
                base, quote = symbol.split('/')
                if quote in config_quote_currencies:
                    trading_symbols.append(symbol)

            # TIMEOUTS: Process one at a time (cancel + immediate replace)
            # This maintains liquidity by never removing more than one order at a time
            for line in self.active_tob_lines.values():
                for symbol in trading_symbols:
                    await self._check_tob_timeouts_for_symbol_sequential(line, symbol)
                    await self._check_tob_competitiveness_for_symbol(line, symbol)

            for line in self.active_passive_lines.values():
                for symbol in trading_symbols:
                    # Passive line timeouts/drift are handled per-exchange in the exchange cycle
                    # to avoid batch cancels and keep orders in the market longer.
                    continue

            # DRIFT: Collect all drift candidates, sort by spread (smallest first), 
            # then cancel in batches of 5
            await self._check_all_drift_batched(trading_symbols)

        except Exception as e:
            self.logger.error(f"Error checking order timeouts and drift: {e}")

    async def _cancel_passive_line_orders_for_symbol(self, line: ActivePassiveLine, symbol: str, reason: str):
        """Cancel all orders for a passive line and specific symbol."""
        if symbol not in line.active_orders:
            return

        cancel_tasks = []

        for exchange in list(line.active_orders[symbol].keys()):
            for side in ['bid', 'ask']:
                if side in line.active_orders[symbol][exchange]:
                    cancel_tasks.append(self._cancel_passive_order_for_symbol(line, exchange, symbol, side, reason))

        if cancel_tasks:
            await asyncio.gather(*cancel_tasks, return_exceptions=True)

    async def _check_tob_timeouts(self, line: ActiveTOBLine):
        """Check for TOB line order timeouts."""
        now = datetime.now(timezone.utc)
        timeout_delta = timedelta(seconds=line.config.timeout_seconds)

        for exchange in list(line.active_orders.keys()):
            for side in ['bid', 'ask']:
                placed_time = line.placed_at.get(exchange, {}).get(side)
                if placed_time and (now - placed_time) > timeout_delta:
                    await self._cancel_tob_order(line, exchange, side, 'timeout')

    async def _check_passive_timeouts(self, line: ActivePassiveLine):
        """Check for passive line order timeouts."""
        now = datetime.now(timezone.utc)
        timeout_delta = timedelta(seconds=line.config.timeout_seconds)

        for exchange in list(line.active_orders.keys()):
            for side in ['bid', 'ask']:
                placed_time = line.placed_at.get(exchange, {}).get(side)
                if placed_time and (now - placed_time) > timeout_delta:
                    await self._cancel_passive_order(line, exchange, side, 'timeout')

    async def _check_tob_drift(self, line: ActiveTOBLine):
        """Check for TOB line price drift."""
        # TOB lines use local exchange pricing, so drift is per-exchange
        for exchange in list(line.active_orders.keys()):
            resolved_symbol = self._resolve_symbol_for_exchange(exchange)
            best_bid, best_ask = self._get_best_bid_ask_from_service(exchange, resolved_symbol)

            if not best_bid or not best_ask:
                continue

            current_mid = (best_bid + best_ask) / Decimal('2')

            for side in ['bid', 'ask']:
                last_price = line.last_prices.get(exchange, {}).get(side)
                if last_price:
                    # DEBUG: Log before legacy drift calculation
                    self.logger.info(f"🔧 LEGACY DRIFT DEBUG: current_mid={current_mid} (type: {type(current_mid)}), last_price={last_price} (type: {type(last_price)})")
                    self.logger.info(f"🔧 LEGACY DRIFT SUBTRACTION DEBUG: current_mid - last_price = {current_mid} - {last_price}")

                    price_diff = current_mid - last_price
                    drift_pct = abs(price_diff / last_price) * 100
                    drift_bps = drift_pct * 100

                    if drift_bps > line.config.drift_bps:
                        await self._cancel_tob_order(line, exchange, side, 'drift')

    async def _check_passive_drift(self, line: ActivePassiveLine):
        """Check for passive line price drift."""
        # Passive lines use aggregated pricing, so check against aggregated reference
        current_price = await self._get_smart_pricing(self.symbol, 'mid', Decimal('100'))

        if not current_price or not line.last_reference_price:
            line.last_reference_price = current_price
            return

        # DEBUG: Log before legacy passive drift calculation
        self.logger.info(f"🔧 LEGACY PASSIVE DRIFT DEBUG: current_price={current_price} (type: {type(current_price)}), last_reference_price={line.last_reference_price} (type: {type(line.last_reference_price)})")
        self.logger.info(f"🔧 LEGACY PASSIVE DRIFT SUBTRACTION DEBUG: current_price - last_reference_price = {current_price} - {line.last_reference_price}")

        price_diff = current_price - line.last_reference_price
        drift_pct = abs(price_diff / line.last_reference_price) * 100
        drift_bps = drift_pct * 100

        if drift_bps > line.config.drift_bps:
            self.logger.info(
                f"Passive Line {line.config.line_id}: Cancelling orders due to DRIFT. "
                f"Drift: {drift_bps:.2f} bps (allowed: {line.config.drift_bps:.2f})"
            )
            # Cancel all orders for this line
            await self._cancel_passive_line_orders(line, 'drift')

    async def _cancel_tob_order(self, line: ActiveTOBLine, exchange: str, side: str, reason: str):
        """Cancel specific TOB order."""
        order_id = line.active_orders.get(exchange, {}).get(side)
        if order_id and order_id in self.active_orders:
            await self._cancel_order(order_id, exchange)

            # Clean up line state
            if exchange in line.active_orders and side in line.active_orders[exchange]:
                del line.active_orders[exchange][side]
            if exchange in line.placed_at and side in line.placed_at[exchange]:
                del line.placed_at[exchange][side]
            if exchange in line.last_prices and side in line.last_prices[exchange]:
                del line.last_prices[exchange][side]

    async def _cancel_passive_order(self, line: ActivePassiveLine, exchange: str, side: str, reason: str):
        """Cancel specific passive order."""
        order_id = line.active_orders.get(exchange, {}).get(side)
        if order_id and order_id in self.active_orders:
            await self._cancel_order(order_id, exchange)

            # Clean up line state
            if exchange in line.active_orders and side in line.active_orders[exchange]:
                del line.active_orders[exchange][side]
            if exchange in line.placed_at and side in line.placed_at[exchange]:
                del line.placed_at[exchange][side]

    async def _cancel_passive_line_orders(self, line: ActivePassiveLine, reason: str):
        """Cancel all orders for a passive line."""
        cancel_tasks = []

        for exchange in list(line.active_orders.keys()):
            for side in ['bid', 'ask']:
                if side in line.active_orders[exchange]:
                    cancel_tasks.append(self._cancel_passive_order(line, exchange, side, reason))

        if cancel_tasks:
            await asyncio.gather(*cancel_tasks, return_exceptions=True)

    async def _cancel_all_line_orders(self):
        """Cancel all orders for all lines."""
        cancel_tasks = []

        # Cancel TOB line orders
        for line in self.active_tob_lines.values():
            for exchange in list(line.active_orders.keys()):
                for side in ['bid', 'ask']:
                    if side in line.active_orders[exchange]:
                        cancel_tasks.append(self._cancel_tob_order(line, exchange, side, 'strategy_stop'))

        # Cancel passive line orders
        for line in self.active_passive_lines.values():
            cancel_tasks.append(self._cancel_passive_line_orders(line, 'strategy_stop'))

        if cancel_tasks:
            await asyncio.gather(*cancel_tasks, return_exceptions=True)

    async def _update_moving_averages(self):
        """Update moving averages for all symbols (including conversion symbols)."""
        # This would implement moving average calculation for volume and price data
        # For now, we'll keep it simple and just track current prices

        # Use ALL symbols including conversion symbols for price history
        # Use hasattr/getattr pattern to avoid evaluating _get_required_symbols() when cache exists
        if not hasattr(self, '_cached_required_symbols'):
            self._cached_required_symbols = self._get_required_symbols()
        all_symbols = self._cached_required_symbols
        for symbol in all_symbols:
            if symbol not in self.price_history:
                self.price_history[symbol] = deque(maxlen=1000)

            # Get current aggregated price (including for conversion symbols like BERA/USDT)
            current_price = await self._get_smart_pricing(symbol, 'mid', Decimal('100'))
            if current_price:
                self.price_history[symbol].append(current_price)

    def _get_best_bid_ask_from_service(self, exchange: str, symbol: str) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        """Get best bid/ask from Redis orderbook manager (like volume weighted strategy)."""
        if not hasattr(self, 'redis_orderbook_manager') or not self.redis_orderbook_manager:
            return None, None

        try:
            # Use Redis orderbook manager like volume weighted strategy
            normalized_symbol = self._normalize_symbol_for_exchange(symbol, exchange)
            best_bid, best_ask = self.redis_orderbook_manager.get_best_bid_ask(exchange, normalized_symbol)

            if not best_bid or not best_ask:
                # Try with original symbol format as fallback
                best_bid, best_ask = self.redis_orderbook_manager.get_best_bid_ask(exchange, symbol)

            return best_bid, best_ask

        except Exception as e:
            self.logger.info(f"Error getting best bid/ask for {exchange} {symbol}: {e}")
            return None, None

    async def _on_price_update(self, exchange: str, symbol: str, orderbook: Dict[str, Any]):
        """Handle price updates from market data service."""
        # This could trigger immediate re-evaluation of lines
        # For now, the main loop will pick up changes naturally
        pass

    def _log_performance_summary(self, last_cycle_time_ms: float):
        """Log performance summary."""
        # Count orders across all symbols
        total_tob_orders = 0
        total_passive_orders = 0

        if self.active_tob_lines:
            for line in self.active_tob_lines.values():
                for symbol_orders in line.active_orders.values():
                    total_tob_orders += sum(len(exchange_orders) for exchange_orders in symbol_orders.values())

        if self.active_passive_lines:
            for line in self.active_passive_lines.values():
                for symbol_orders in line.active_orders.values():
                    total_passive_orders += sum(len(exchange_orders) for exchange_orders in symbol_orders.values())

        # Use REAL inventory values for logging (not inventory manager's wrong values)
        target_inv = float(self.strategy_config.inventory.target_inventory) if self.strategy_config and self.strategy_config.inventory else 4244000
        current_inv = float(self.real_current_inventory) if self.real_current_inventory is not None else 0.0

        # DEBUG: Log before inventory subtraction
        self.logger.info(f"🔧 PERFORMANCE LOG DEBUG: target_inv={target_inv} (type: {type(target_inv)}), current_inv={current_inv} (type: {type(current_inv)})")
        self.logger.info(f"🔧 PERFORMANCE SUBTRACTION DEBUG: current_inv - target_inv = {current_inv} - {target_inv}")

        excess_inv = current_inv - target_inv

        # Calculate real coefficient
        inv_coeff = 0.0
        if target_inv != 0:
            coefficient = excess_inv / abs(target_inv)
            inv_coeff = max(-1.0, min(1.0, coefficient))

        self.logger.info(
            f"📊 Performance Summary: cycle={last_cycle_time_ms:.1f}ms, "
            f"tob_orders={total_tob_orders}, passive_orders={total_passive_orders}, "
            f"current_inv={current_inv:.0f}, target_inv={target_inv:.0f}, excess_inv={excess_inv:.0f}, "
            f"inventory_coeff={inv_coeff:.3f}, "
            f"orders_placed_tob={self.orders_placed_tob}, orders_placed_passive={self.orders_placed_passive}, "
            f"cancelled_timeout={self.orders_cancelled_timeout}, cancelled_drift={self.orders_cancelled_drift}, "
            f"cancelled_replace={self.orders_cancelled_replace}"
        )

    async def _publish_performance_data(self) -> None:
        """Publish current strategy state to Redis for frontend details."""
        try:
            if not self.redis_client:
                return

            state = self.to_dict()
            # Ensure frontend receives the expected strategy key
            state["strategy"] = "stacked_market_making_delta"
            state["status"] = "running" if self.running else "stopped"
            state["last_updated"] = datetime.now(timezone.utc).isoformat()

            await self.redis_client.setex(
                self.performance_key,
                60,  # Expire after 60 seconds
                json.dumps(state, default=str)
            )
        except Exception as e:
            self.logger.error(f"Error publishing performance data to Redis: {e}")

    async def on_trade_confirmation(self, trade_data: Dict[str, Any]) -> None:
        """Handle trade confirmation and update inventory."""
        try:
            # Call base class for order tracking
            await super().on_trade_confirmation(trade_data)

            # Clean up our line tracking when orders are filled (like volume weighted strategy does)
            order_id = trade_data.get('order_id')
            if order_id and order_id not in self.active_orders:
                await self._cleanup_filled_order_from_lines(order_id)

            # Update inventory manager
            exchange = trade_data.get('exchange', '')
            side = trade_data.get('side', '')
            amount = Decimal(str(trade_data.get('amount', 0)))
            price = Decimal(str(trade_data.get('price', 0)))

            # Extract fee information
            fee_cost = None
            fee_currency = None

            if isinstance(trade_data.get('fee'), dict):
                fee_cost = Decimal(str(trade_data['fee'].get('cost', 0)))
                fee_currency = trade_data['fee'].get('currency')

            # Update inventory
            self.inventory_manager.update_position(
                exchange, side, amount, price, fee_cost, fee_currency
            )

            self.logger.info(
                f"✅ Stacked trade confirmed: {side.upper()} {amount} {trade_data.get('symbol', 'unknown')} @ {price} "
                f"on {exchange} (Order ID: {order_id})"
            )

            # The existing coefficient system automatically picks up trades from the database
            # No need to manually update - trades are automatically processed by the MA system

            # Handle hedging for non-USDT pairs if enabled (same-exchange only)
            if self.strategy_config.hedging_enabled and exchange:
                await self._handle_hedging_for_fill(exchange, trade_data)

        except Exception as e:
            self.logger.error(f"Error in trade confirmation handler: {e}")

    async def _cleanup_filled_order_from_lines(self, order_id: str):
        """Clean up order tracking when an order is filled (like volume weighted strategy)."""
        try:
            # Remove order from TOB line tracking
            for line in self.active_tob_lines.values():
                for symbol in list(line.active_orders.keys()):
                    for exchange in list(line.active_orders[symbol].keys()):
                        for side in list(line.active_orders[symbol][exchange].keys()):
                            if line.active_orders[symbol][exchange][side] == order_id:
                                self.logger.info(f"TOB Line {line.config.line_id}: Cleaning up filled order {order_id} ({side} {symbol} on {exchange})")
                                self._cleanup_tob_order_tracking(line, exchange, symbol, side)
                                return

            # Remove order from Passive line tracking
            for line in self.active_passive_lines.values():
                for symbol in list(line.active_orders.keys()):
                    for exchange in list(line.active_orders[symbol].keys()):
                        for side in list(line.active_orders[symbol][exchange].keys()):
                            if line.active_orders[symbol][exchange][side] == order_id:
                                self.logger.info(f"Passive Line {line.config.line_id}: Cleaning up filled order {order_id} ({side} {symbol} on {exchange})")
                                self._cleanup_passive_order_tracking(line, exchange, symbol, side)
                                return

        except Exception as e:
            self.logger.error(f"Error cleaning up filled order {order_id}: {e}")

    async def _handle_hedging_for_fill(self, exchange: str, trade_data: Dict[str, Any]):
        """Update quote exposure and place hedge orders to move toward per-currency targets.

        Rules:
        - Never hedge USDT; controlled by base BERA inventory.
        - Use same symbols as normal quoting when fetching from Redis.
        - TRY uses USDT/TRY for hedging (directional mapping); all other use QUOTE/USDT.
        - Use WAPQ if available; otherwise best bid/ask depending on direction.
        - Place limit hedge order intended as taker; bypass taker check for hedge orders only.
        """
        try:
            symbol = trade_data.get('symbol') or self.symbol
            side = (trade_data.get('side') or '').lower()
            amount = Decimal(str(trade_data.get('amount', '0')))
            price = Decimal(str(trade_data.get('price', '0')))

            if '/' not in symbol or amount <= 0:
                return

            base, quote = symbol.split('/')
            # Only hedge when symbol is BASE/QUOTE for our configured BASE and QUOTE != USDT
            if base != self.strategy_config.base_coin:
                return
            if quote.upper() == 'USDT':
                return

            # Ensure there is a target defined for this quote currency; absent means default 0 target
            targets = getattr(self.strategy_config, 'hedging_targets', {}) or {}
            target_str = targets.get(quote.upper(), '0')
            try:
                target_qty = Decimal(str(target_str))
            except Exception:
                target_qty = Decimal('0')

            # Update per-exchange quote exposure from our fill
            # Buy BASE/QUOTE spends QUOTE (negative exposure); Sell BASE/QUOTE receives QUOTE (positive exposure)
            delta_quote = amount * price
            exposure_key = (exchange, quote.upper())
            current_exposure = self.quote_exposures.get(exposure_key, Decimal('0'))
            if side == 'buy':
                current_exposure -= delta_quote
            elif side == 'sell':
                current_exposure += delta_quote
            else:
                return

            self.quote_exposures[exposure_key] = current_exposure

            # Compute hedge needed to move toward target
            excess = current_exposure - target_qty
            if excess == 0:
                return

            # Derive hedge instrument and direction
            # For non-TRY: hedge on QUOTE/USDT
            # For TRY: hedge on USDT/TRY with inverted sides
            if quote.upper() == 'TRY':
                hedge_symbol = 'USDT/TRY'
                # If we are long TRY (excess > 0), we want to sell TRY -> buy USDT/TRY (side='buy')
                # If we are short TRY (excess < 0), we want to buy TRY -> sell USDT/TRY (side='sell')
                hedge_side = 'buy' if excess > 0 else 'sell'
                hedge_qty_in_quote = abs(excess)  # TRY quantity to unwind
            else:
                hedge_symbol = f"{quote.upper()}/USDT"
                # If long QUOTE (excess > 0), sell QUOTE (side='sell')
                # If short QUOTE (excess < 0), buy QUOTE (side='buy')
                hedge_side = 'sell' if excess > 0 else 'buy'
                hedge_qty_in_quote = abs(excess)

            # Resolve symbol key used in Redis for this exchange
            resolved_symbol = self._resolve_symbol_for_exchange_and_symbol(exchange, hedge_symbol)

            # Get hedge price: prefer WAPQ consuming the correct side/qty; fallback to best bid/ask
            hedge_price = await self._get_exchange_wapq_or_best(exchange, resolved_symbol, hedge_side, hedge_qty_in_quote)
            if hedge_price is None or hedge_price <= 0:
                self.logger.warning(f"HEDGE: No price for {exchange} {resolved_symbol}; skipping hedge")
                return

            # Determine amount in base units of hedge market
            # For QUOTE/USDT: base is QUOTE; amount = hedge_qty_in_quote converted to base via price if needed
            # For USDT/TRY: base is USDT; to sell/buy TRY notionals, convert TRY to USDT amount via price
            if resolved_symbol.upper().startswith('USDT/TRY'):
                # price = USDT per TRY; TRY notional * price = USDT amount
                order_amount = (hedge_qty_in_quote * hedge_price).quantize(Decimal('0.00000001'))
            else:
                # QUOTE/USDT market base is QUOTE; amount in QUOTE units
                order_amount = hedge_qty_in_quote.quantize(Decimal('0.00000001'))

            # Place limit hedge order designed to take: push one tick across in the intended direction
            min_tick = Decimal('0.000001')
            if hedge_side == 'buy':
                place_price = hedge_price + min_tick
            else:
                place_price = hedge_price - min_tick

            # Bypass taker check for hedge orders by placing directly via low-level method
            client_order_id = f"hedge_{int(time.time()*1000)}"
            order_id = await self._place_order_for_symbol(
                exchange=exchange,
                symbol=resolved_symbol,
                side=hedge_side,
                amount=float(order_amount),
                price=float(place_price),
                client_order_id=client_order_id
            )

            if order_id:
                self.logger.info(
                    f"HEDGE: Placed {hedge_side.upper()} {order_amount} {resolved_symbol} @ {place_price} on {exchange} "
                    f"(exposure {current_exposure:.8f} {quote.upper()}, target {target_qty:.8f})"
                )
            else:
                self.logger.warning(
                    f"HEDGE: Failed to place {hedge_side.upper()} {resolved_symbol} on {exchange}"
                )

        except Exception as e:
            self.logger.error(f"Error in hedge handler: {e}")

    async def _get_exchange_wapq_or_best(self, exchange: str, symbol: str, side: str, quantity: Decimal) -> Optional[Decimal]:
        """Try per-exchange WAPQ for side/quantity; fallback to best bid/ask from Redis."""
        try:
            # If pricing engine exists, ask for exchange-specific WAPQ via direct orderbook
            # Reuse internal _get_exchange_orderbook path by mimicking a one-exchange calc
            # We have a helper below to fetch orderbook.
            orderbook = self._get_exchange_orderbook_from_service(exchange, symbol)
            if orderbook and orderbook.get('bids') and orderbook.get('asks'):
                # Compute WAPQ manually using the same logic as pricing engine
                if side in ('buy', 'bid'):
                    levels = orderbook.get('asks', [])
                else:
                    levels = orderbook.get('bids', [])
                remaining = quantity
                total_cost = Decimal('0')
                total_qty = Decimal('0')
                for price_str, amount_str in levels:
                    px = Decimal(str(price_str))
                    avail = Decimal(str(amount_str))
                    if remaining <= 0:
                        break
                    take = min(avail, remaining)
                    total_cost += take * px
                    total_qty += take
                    remaining -= take
                if total_qty > 0:
                    return (total_cost / total_qty)

            # Fallback to best-of-book
            best_bid, best_ask = self._get_best_bid_ask_from_service(exchange, symbol)
            if best_bid and best_ask:
                return best_ask if side in ('buy', 'bid') else best_bid
            return None
        except Exception:
            return None

    def _get_exchange_orderbook_from_service(self, exchange: str, symbol: str) -> Optional[Dict[str, Any]]:
        """Fetch orderbook published for exchange/symbol from market_data_service cache (same keys as quoting)."""
        try:
            if not hasattr(self, 'market_data_service') or not self.market_data_service:
                return None
            # market_data_service.orderbooks keys are (exchange, symbol) in original format
            # Ensure we try both normalized and original
            normalized_symbol = self._normalize_symbol_for_exchange(symbol, exchange)
            ob = self.market_data_service.orderbooks.get((exchange, normalized_symbol))
            if ob:
                return ob
            return self.market_data_service.orderbooks.get((exchange, symbol))
        except Exception:
            return None

    def get_strategy_state(self) -> Dict[str, Any]:
        """Get comprehensive strategy state."""
        state = self.to_dict()

        # Add stacked strategy specific information
        inventory_state_data = self.inventory_manager.get_inventory_state() if self.inventory_manager else None

        # Calculate total active orders per line (across all symbols)
        tob_line_data = []
        for line in self.active_tob_lines.values():
            total_orders = 0
            for symbol_orders in line.active_orders.values():
                for exchange_orders in symbol_orders.values():
                    total_orders += len(exchange_orders)

            tob_line_data.append({
                    'line_id': line.config.line_id,
                'active_orders': total_orders,
                    'current_coefficient': float(line.current_coefficient),
                    'sides': line.config.sides,
                'spread_bps': float(line.config.spread_bps),
                'timeout_seconds': line.config.timeout_seconds,
                'min_price': float(line.config.min_price) if line.config.min_price else None,
                'max_price': float(line.config.max_price) if line.config.max_price else None
            })

        passive_line_data = []
        for line in self.active_passive_lines.values():
            total_orders = 0
            for symbol_orders in line.active_orders.values():
                for exchange_orders in symbol_orders.values():
                    total_orders += len(exchange_orders)

            passive_line_data.append({
                    'line_id': line.config.line_id,
                'active_orders': total_orders,
                    'current_spread_coefficient': float(line.current_spread_coefficient),
                    'current_quantity_coefficient': float(line.current_quantity_coefficient),
                    'sides': line.config.sides,
                'mid_spread_bps': float(line.config.mid_spread_bps),
                'timeout_seconds': line.config.timeout_seconds,
                'quantity': float(line.config.quantity)
            })

        # Use REAL inventory values from database (not inventory manager's wrong values)
        target_inventory = float(self.strategy_config.inventory.target_inventory) if self.strategy_config and self.strategy_config.inventory else 4244000
        current_inventory = float(self.real_current_inventory) if self.real_current_inventory is not None else 0.0

        # DEBUG: Log before strategy state inventory subtraction
        self.logger.info(f"🔧 STRATEGY STATE DEBUG: target_inventory={target_inventory} (type: {type(target_inventory)}), current_inventory={current_inventory} (type: {type(current_inventory)})")
        self.logger.info(f"🔧 STRATEGY STATE SUBTRACTION DEBUG: current_inventory - target_inventory = {current_inventory} - {target_inventory}")

        excess_inventory = current_inventory - target_inventory

        # Calculate correct coefficient (like risk system)
        # NEGATIVE TARGET SUPPORT: Handle both positive and negative targets
        inventory_coefficient = 0.0
        if target_inventory != 0:
            # For positive target: excess > 0 means we're long (coeff > 0), excess < 0 means short (coeff < 0)
            # For negative target: excess > 0 means we need to sell more (coeff > 0 relative to abs(target))
            coefficient = excess_inventory / abs(target_inventory)
            inventory_coefficient = max(-1.0, min(1.0, coefficient))  # Clamp to [-1, 1]

        enhanced_inventory_state = {
            'current_inventory': current_inventory,
            'target_inventory': target_inventory,
            'excess_inventory': excess_inventory,
            'excess_inventory_percentage': 100.0,  # Default to 100% like volume weighted strategy
            'target_excess_amount': abs(excess_inventory),
            'inventory_coefficient': inventory_coefficient,  # CORRECT coefficient from real data
            'inventory_price': float(self.real_inventory_price) if self.real_inventory_price else None,
            'max_inventory_deviation': float(self.strategy_config.inventory.max_inventory_deviation) if self.strategy_config else 0,
            'inventory_price_method': self.strategy_config.inventory.inventory_price_method.value if self.strategy_config else 'accounting'
        }

        strategy_state = {
            'tob_lines': tob_line_data,
            'passive_lines': passive_line_data,
            'inventory_state': enhanced_inventory_state,
            'delta_state': {
                'target_inventory_source': self.strategy_config.target_inventory_source if self.strategy_config else 'manual',
                'delta_change_threshold_pct': self.strategy_config.delta_change_threshold_pct if self.strategy_config else 0.0,
                'delta_trade_fraction': self.strategy_config.delta_trade_fraction if self.strategy_config else 0.0,
                'delta_target_sign': self.strategy_config.delta_target_sign if self.strategy_config else -1.0,
                'prevent_unprofitable_trades': self.strategy_config.prevent_unprofitable_trades if self.strategy_config else False,
                'last_delta_target': float(self.last_delta_target) if self.last_delta_target is not None else None,
                'last_delta_updated_at': self.last_delta_updated_at.isoformat() if self.last_delta_updated_at else None,
            },
            'performance': {
                'orders_placed_tob': self.orders_placed_tob,
                'orders_placed_passive': self.orders_placed_passive,
                'orders_cancelled_timeout': self.orders_cancelled_timeout,
                'orders_cancelled_drift': self.orders_cancelled_drift,
                'orders_cancelled_replace': self.orders_cancelled_replace,
                'coefficient_adjustments': self.coefficient_adjustments,
                'smart_pricing_decisions': self.smart_pricing_decisions,
                'last_updated': datetime.now(timezone.utc).isoformat()
            },
            'multi_symbol_config': {
                'base_coin': self.strategy_config.base_coin if self.strategy_config else 'BERA',
                'quote_currencies': self.strategy_config.quote_currencies if self.strategy_config else [],
                'smart_pricing_source': self.strategy_config.smart_pricing_source.value if self.strategy_config else 'aggregated',
                'hedging_enabled': self.strategy_config.hedging_enabled if self.strategy_config else False,
                'taker_check': self.strategy_config.taker_check if self.strategy_config else True,
                'hedging_targets': getattr(self.strategy_config, 'hedging_targets', {}) if self.strategy_config else {}
            },
            'coefficient_config': {
                'time_periods': getattr(self.strategy_config, 'time_periods', []) if self.strategy_config else [],
                'coefficient_method': getattr(self.strategy_config, 'coefficient_method', 'min') if self.strategy_config else 'min',
                'min_coefficient': getattr(self.strategy_config, 'min_coefficient', 0.2) if self.strategy_config else 0.2,
                'max_coefficient': getattr(self.strategy_config, 'max_coefficient', 3.0) if self.strategy_config else 3.0,
                'exchange_coefficients': self.exchange_coefficients,
                'last_coefficient_update': self.last_coefficient_update.isoformat() if self.last_coefficient_update else None
            }
        }

        state.update(strategy_state)
        return state

    def to_dict(self) -> Dict[str, Any]:
        """Convert strategy to dictionary for API responses."""
        # Use the base class to_dict method to avoid recursion
        state = super().to_dict()
        state["strategy"] = "stacked_market_making_delta"

        # Add stacked strategy specific information
        inventory_state_data = self.inventory_manager.get_inventory_state() if self.inventory_manager else None

        # Calculate total active orders per line (across all symbols)
        tob_line_data = []
        for line in self.active_tob_lines.values():
            total_orders = 0
            for symbol_orders in line.active_orders.values():
                for exchange_orders in symbol_orders.values():
                    total_orders += len(exchange_orders)

            tob_line_data.append({
                    'line_id': line.config.line_id,
                'active_orders': total_orders,
                    'current_coefficient': float(line.current_coefficient),
                    'sides': line.config.sides,
                'spread_bps': float(line.config.spread_bps),
                'timeout_seconds': line.config.timeout_seconds,
                'min_price': float(line.config.min_price) if line.config.min_price else None,
                'max_price': float(line.config.max_price) if line.config.max_price else None
            })

        passive_line_data = []
        for line in self.active_passive_lines.values():
            total_orders = 0
            for symbol_orders in line.active_orders.values():
                for exchange_orders in symbol_orders.values():
                    total_orders += len(exchange_orders)

            passive_line_data.append({
                    'line_id': line.config.line_id,
                'active_orders': total_orders,
                    'current_spread_coefficient': float(line.current_spread_coefficient),
                    'current_quantity_coefficient': float(line.current_quantity_coefficient),
                    'sides': line.config.sides,
                'mid_spread_bps': float(line.config.mid_spread_bps),
                'timeout_seconds': line.config.timeout_seconds,
                'quantity': float(line.config.quantity)
            })

        # Use REAL inventory values from database (not inventory manager's wrong values)
        target_inventory = float(self.strategy_config.inventory.target_inventory) if self.strategy_config and self.strategy_config.inventory else 4244000
        current_inventory = float(self.real_current_inventory) if self.real_current_inventory is not None else 0.0

        excess_inventory = current_inventory - target_inventory

        # Calculate correct coefficient (like risk system)
        # NEGATIVE TARGET SUPPORT: Handle both positive and negative targets
        inventory_coefficient = 0.0
        if target_inventory != 0:
            # For positive target: excess > 0 means we're long (coeff > 0), excess < 0 means short (coeff < 0)
            # For negative target: excess > 0 means we need to sell more (coeff > 0 relative to abs(target))
            coefficient = excess_inventory / abs(target_inventory)
            inventory_coefficient = max(-1.0, min(1.0, coefficient))  # Clamp to [-1, 1]

        enhanced_inventory_state = {
            'current_inventory': current_inventory,
            'target_inventory': target_inventory,
            'excess_inventory': excess_inventory,
            'excess_inventory_percentage': 100.0,  # Default to 100% like volume weighted strategy
            'target_excess_amount': abs(excess_inventory),
            'inventory_coefficient': inventory_coefficient,  # CORRECT coefficient from real data
            'inventory_price': float(self.real_inventory_price) if self.real_inventory_price else None,
            'max_inventory_deviation': float(self.strategy_config.inventory.max_inventory_deviation) if self.strategy_config else 0,
            'inventory_price_method': self.strategy_config.inventory.inventory_price_method.value if self.strategy_config else 'accounting'
        }

        strategy_state = {
            'tob_lines': tob_line_data,
            'passive_lines': passive_line_data,
            'inventory_state': enhanced_inventory_state,
            'performance': {
                'orders_placed_tob': self.orders_placed_tob,
                'orders_placed_passive': self.orders_placed_passive,
                'orders_cancelled_timeout': self.orders_cancelled_timeout,
                'orders_cancelled_drift': self.orders_cancelled_drift,
                'orders_cancelled_replace': self.orders_cancelled_replace,
                'coefficient_adjustments': self.coefficient_adjustments,
                'smart_pricing_decisions': self.smart_pricing_decisions,
                'last_updated': datetime.now(timezone.utc).isoformat()
            },
            'multi_symbol_config': {
                'base_coin': self.strategy_config.base_coin if self.strategy_config else 'BERA',
                'quote_currencies': self.strategy_config.quote_currencies if self.strategy_config else [],
                'smart_pricing_source': self.strategy_config.smart_pricing_source.value if self.strategy_config else 'aggregated',
                'hedging_enabled': self.strategy_config.hedging_enabled if self.strategy_config else False,
                'taker_check': self.strategy_config.taker_check if self.strategy_config else True,
                'hedging_targets': getattr(self.strategy_config, 'hedging_targets', {}) if self.strategy_config else {}
            },
            'coefficient_config': {
                'time_periods': getattr(self.strategy_config, 'time_periods', []) if self.strategy_config else [],
                'coefficient_method': getattr(self.strategy_config, 'coefficient_method', 'min') if self.strategy_config else 'min',
                'min_coefficient': getattr(self.strategy_config, 'min_coefficient', 0.2) if self.strategy_config else 0.2,
                'max_coefficient': getattr(self.strategy_config, 'max_coefficient', 3.0) if self.strategy_config else 3.0,
                'exchange_coefficients': self.exchange_coefficients,
                'last_coefficient_update': self.last_coefficient_update.isoformat() if self.last_coefficient_update else None
            }
        }

        state.update(strategy_state)
        return state

    async def _fetch_inventory_from_risk_redis(self) -> Optional[Dict[str, Decimal]]:
        """Read inventory size and entry price from Risk V2 Redis across methods with fallbacks."""
        try:
            if not self.redis_client:
                return None
            candidates: List[str] = [self.symbol]
            # Try colon notation too for perps-style keys if applicable
            try:
                if '/' in self.symbol:
                    base, quote = self.symbol.split('/')
                    candidates.append(f"{base}/{quote}:{quote}")
                    if quote.upper() == 'USDT':
                        candidates.append(f"{base}/USDC:USDC")
            except Exception:
                pass
            method_candidates = ['AVERAGE_COST', 'FIFO', 'LIFO']
            for method in method_candidates:
                all_keys: List[str] = []
                cursor = 0
                for sym in candidates:
                    pattern = f"position:*:{sym}:{method}"
                    # SCAN loop
                    while True:
                        cursor, keys = await self.redis_client.scan(cursor=cursor, match=pattern, count=200)
                        if keys:
                            for k in keys:
                                key_str = k.decode() if isinstance(k, (bytes, bytearray)) else k
                                all_keys.append(key_str)
                        if cursor == 0:
                            break
                if not all_keys:
                    continue
                pipe = self.redis_client.pipeline()
                for key in all_keys:
                    pipe.hget(key, 'size')
                    pipe.hget(key, 'p2')
                    pipe.hget(key, 'avg_price')
                    pipe.hget(key, 'entry_price')
                results = await pipe.execute()
                total_size = Decimal('0')
                total_p2 = Decimal('0')
                for i in range(0, len(results), 4):
                    raw_size, raw_p2, raw_avg_price, raw_entry_price = results[i:i+4]
                    def _to_dec(v):
                        if v is None:
                            return None
                        try:
                            if isinstance(v, (bytes, bytearray)):
                                return Decimal(v.decode())
                            return Decimal(str(v))
                        except Exception:
                            return None
                    size_dec = _to_dec(raw_size) or Decimal('0')
                    p2_dec = _to_dec(raw_p2)
                    avg_dec = _to_dec(raw_avg_price)
                    entry_dec = _to_dec(raw_entry_price)
                    if p2_dec is None:
                        if avg_dec is not None:
                            p2_dec = -(size_dec * avg_dec)
                        elif entry_dec is not None:
                            p2_dec = -(size_dec * entry_dec)
                        else:
                            p2_dec = Decimal('0')
                    total_size += size_dec
                    total_p2 += p2_dec
                if abs(total_size) >= Decimal('0.00000001'):
                    entry_price = abs(total_p2) / abs(total_size)
                    return {
                        'size': total_size,
                        'entry_price': entry_price,
                        'cost_base': abs(total_p2)
                    }
            return None
        except Exception as e:
            self.logger.error(f"Redis inventory fetch failed: {e}")
            return None

    async def _risk_ws_loop(self) -> None:
        """Subscribe to Risk V2 WS and update real inventory from pushed positions."""
        while self.running:
            try:
                if not self._ws_session:
                    self._ws_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
                self.logger.info(f"Connecting to Risk V2 WS at {self.risk_ws_url}")
                async with self._ws_session.ws_connect(self.risk_ws_url) as ws:
                    # Prefer AVERAGE_COST for cross-quote aggregation
                    try:
                        await ws.send_str(json.dumps({"type": "set_method", "method": "AVERAGE_COST"}))
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
                self.logger.warning(f"Risk V2 WS subscriber error: {e}")
            await asyncio.sleep(self._ws_backoff_seconds)
            self._ws_backoff_seconds = min(self._ws_backoff_seconds * 2.0, 10.0)

    async def _handle_risk_ws_message(self, payload: Dict[str, Any]) -> None:
        """Process WS message and aggregate base inventory across all quote pairs."""
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

            # Frame summary
            try:
                self.logger.info(
                    f"WS frame received: type={msg_type}, has_metrics={bool(metrics)}, has_positions={bool(positions)}"
                )
            except Exception:
                pass

            size_from_metrics = None
            if metrics:
                m = self._aggregate_base_inventory_from_metrics(metrics)
                if m and m.get('size') is not None:
                    size_from_metrics = m['size']

            entry_price = None
            size_from_positions = None
            if positions:
                agg = self._aggregate_base_inventory_from_positions(positions)
                if agg:
                    entry_price = agg.get('entry_price')
                    if agg.get('size') is not None:
                        size_from_positions = agg['size']
                    if size_from_metrics is None and size_from_positions is not None:
                        size_from_metrics = size_from_positions

            # Aggregation summary
            try:
                self.logger.info(
                    f"WS inventory aggregate: metrics_size={size_from_metrics}, positions_size={size_from_positions}, entry_price={entry_price}"
                )
            except Exception:
                pass

            # CRITICAL FIX: If we have no size data but received a valid POSITION-related WS frame,
            # treat this as zero inventory (no positions = zero position).
            # This allows the strategy to work when there are no trades/positions yet.
            # NOTE: balance_update messages should NOT update inventory - they don't contain position data
            if size_from_metrics is None:
                # Only position-related message types should update inventory when no data is present
                # balance_update is explicitly excluded - it's for balance changes, not position changes
                if msg_type in ('update', 'positions_update', 'pnl_update', 'initialization_progress'):
                    self.logger.info(f"No position data in WS frame (type={msg_type}), treating as zero inventory")
                    size_from_metrics = Decimal('0')
                else:
                    # balance_update and unknown message types should not update inventory
                    self.logger.debug(f"Ignoring WS frame type={msg_type} for inventory update (no position data)")
                    return

            # Use WS-pushed totals as canonical source (aligns with frontend)
            self.real_current_inventory = size_from_metrics
            
            # Handle inventory price based on position size:
            # - If size is 0 (or near zero), clear inventory price (no position = no entry price)
            # - If size is non-zero (positive or negative), use the entry price
            if abs(size_from_metrics) < Decimal('0.00000001'):
                # Zero position - clear inventory price
                self.real_inventory_price = None
                self.logger.info("Zero inventory - clearing inventory price")
            elif entry_price is not None:
                # Non-zero position with entry price - use it (works for both positive and negative positions)
                self.real_inventory_price = entry_price
                self.logger.info(f"Updated inventory price to {entry_price} for position size {size_from_metrics}")
            
            # Mark freshness
            self._last_ws_inventory_update = datetime.now(timezone.utc)
        except Exception:
            pass

    def _aggregate_base_inventory_from_positions(self, positions: List[Dict[str, Any]]) -> Optional[Dict[str, Decimal]]:
        """Aggregate signed base size across all symbols for our base_coin; derive entry as |p2|/|size|."""
        try:
            base_coin = (self.strategy_config.base_coin if self.strategy_config else '').upper()
            if not base_coin:
                return None
            total_size = Decimal('0')
            total_p2 = Decimal('0')
            matched = False
            matched_count = 0
            for pos in positions:
                sym = (pos.get('symbol') or '').upper()
                if not sym or '/' not in sym:
                    continue
                base = sym.split('/')[0]
                if base != base_coin:
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
            
            # IMPORTANT: If no positions matched for our base coin, treat as zero position (not "no data")
            # This allows the strategy to work when there are no trades/positions yet
            if not matched:
                self.logger.info(f"Positions agg result: base={base_coin}, no positions matched - treating as zero position")
                return {'size': Decimal('0'), 'entry_price': None, 'cost_base': Decimal('0')}
            
            # Zero or near-zero position - no entry price
            if abs(total_size) < Decimal('0.00000001'):
                self.logger.info(f"Positions agg result: base={base_coin}, symbols_matched={matched_count}, total_size=0 - no entry price")
                return {'size': Decimal('0'), 'entry_price': None, 'cost_base': Decimal('0')}
            
            # Non-zero position (positive or negative) - calculate entry price using absolute values
            entry_price = abs(total_p2) / abs(total_size)
            self.logger.info(f"Positions agg result: base={base_coin}, symbols_matched={matched_count}, total_size={total_size}, entry={entry_price}")
            return {'size': total_size, 'entry_price': entry_price, 'cost_base': abs(total_p2)}
        except Exception as e:
            self.logger.error(f"Error in _aggregate_base_inventory_from_positions: {e}")
            return None

    def _aggregate_base_inventory_from_metrics(self, metrics: Dict[str, Any]) -> Optional[Dict[str, Decimal]]:
        """Aggregate signed base size using metrics.positions_by_symbol across all quotes for our base coin.
        Prefers net/signed fields when available; else uses side sign with totals.
        """
        try:
            pbs = metrics.get('positions_by_symbol')
            if not pbs or not isinstance(pbs, dict):
                return None
            base_coin = (self.strategy_config.base_coin if self.strategy_config else '').upper()
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
                    total_size += Decimal(str(net_val))
                    matched = True
                    matched_count += 1
                    continue
                size_val = data.get('total_size')
                if size_val is None:
                    continue
                side_str = (data.get('side') or '').lower()
                sign = Decimal('1') if side_str == 'long' else (Decimal('-1') if side_str == 'short' else Decimal('0'))
                total_size += Decimal(str(size_val)) * sign
                matched = True
                matched_count += 1
            
            # IMPORTANT: If no positions matched for our base coin, treat as zero position (not "no data")
            # This allows the strategy to work when there are no trades/positions yet
            if not matched:
                self.logger.info(f"Metrics agg result: base={base_coin}, no positions matched - treating as zero position")
                return {'size': Decimal('0')}
            
            self.logger.info(f"Metrics agg result: base={base_coin}, symbols_matched={matched_count}, total_size={total_size}")
            return {'size': total_size}
        except Exception as e:
            self.logger.error(f"Error in _aggregate_base_inventory_from_metrics: {e}")
            return None