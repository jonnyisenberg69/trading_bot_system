"""
Targeted Sellbot Strategy - Maker-only selling strategy with dynamic pricing

This strategy aims to sell BASE coins as a maker at optimal prices, integrating
with aggressive TWAP for interval target prices and using various pricing algorithms.
"""

import asyncio
import json
import time
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Dict, List, Optional, Any, Tuple
import logging
import structlog
import uuid

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_
from sqlalchemy.orm import joinedload

import redis.asyncio as redis
import contextlib

from bot_manager.strategies.base_strategy import BaseStrategy
from market_data.redis_orderbook_manager import RedisOrderbookManager
from order_management.execution_engine import ExecutionEngine
from order_management.order_manager import OrderManager
from exchanges.base_connector import OrderType, OrderSide, OrderStatus
from order_management.tracking import PositionManager


class TargetCurrency(str, Enum):
    """Target currency options."""
    BASE = "base"
    QUOTE = "quote"


class TrackingMode(str, Enum):
    """Tracking mode options."""
    PERCENTAGE = "percentage"
    TARGET = "target"


class PricingAlgorithm(str, Enum):
    """Pricing algorithm options."""
    GREATER_OF = "greater_of"
    LESSER_OF = "lesser_of"
    INVENTORY_PLUS_SPREAD = "inventory_plus_spread"
    INTERVAL_PLUS_SPREAD = "interval_plus_spread"


class PriceComparisonMode(str, Enum):
    """Price comparison mode for determining which price to use."""
    GREATER = "greater"  # Use MAX(inventory, interval, target)
    LESSER = "lesser"    # Use MIN(inventory, interval, target)
    INVENTORY_ONLY = "inventory_only"  # Use only inventory price
    INTERVAL_ONLY = "interval_only"    # Use only interval price
    TARGET_ONLY = "target_only"        # Use only target price


class SellMode(str, Enum):
    """Selling mode options."""
    PERCENT_INVENTORY = "percent_inventory"  # Sell a percentage of total inventory
    HOURLY_RATE = "hourly_rate"             # Sell at a fixed hourly rate


class TargetedSellbotStrategy(BaseStrategy):
    """
    Targeted Sellbot strategy for selling BASE coins as a maker.
    
    Features:
    - Maker-only selling at optimal prices
    - Integration with aggressive TWAP for interval target prices
    - Custom client order ID tracking with "sellbot_" prefix
    - Dynamic pricing algorithms with multiple options
    - Taker check to avoid crossing the spread
    - Percentage-based selling: Sell X% of total inventory over Y hours
    - Hourly rate selling: Sell fixed amount per hour
    - Minimum sell price configuration
    - Linear selling through time with catch-up mechanism
    
    Configuration:
    - For percentage mode: sell_target_value = percentage of TOTAL inventory to sell
      Example: sell_target_value=50 means sell 50% of inventory over total_time_hours
    - For backward compatibility: hourly_percentage also means TOTAL percentage (not per hour)
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
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None  # New: explicit end time
        self.total_time_hours: float = 0
        self.spread_bps: int = 0
        self.timeout_seconds: int = 0
        self.drift_bps: int = 0
        self.tracking_mode: TrackingMode = TrackingMode.PERCENTAGE
        self.hourly_percentage: Optional[float] = None  # Legacy: Actually TOTAL % to sell (not per hour)
        self.target_amount: Optional[Decimal] = None
        self.target_currency: TargetCurrency = TargetCurrency.BASE
        self.taker_check: bool = True
        self.pricing_algorithm: PricingAlgorithm = PricingAlgorithm.GREATER_OF
        
        # New configuration parameters
        self.minimum_sell_price: Optional[Decimal] = None
        self.price_comparison_mode: PriceComparisonMode = PriceComparisonMode.GREATER
        self.sell_mode: SellMode = SellMode.PERCENT_INVENTORY
        self.sell_target_value: Optional[Decimal] = None  # TOTAL % to sell over total_time_hours
        self.initial_inventory: Optional[Decimal] = None  # Inventory at start time
        
        # Execution components
        self.redis_orderbook_manager = None
        self.position_manager = None
        self.execution_engine = None
        self.order_manager = None
        
        # Redis connection for strategy coordination
        self.redis_url = redis_url
        self.redis_client = None
        self.redis_pubsub = None
        self.interval_subscription_task = None
        
        # Current interval target price from aggressive TWAP
        self.current_interval_target_price: Optional[Decimal] = None
        self.last_interval_update: Optional[datetime] = None
        
        # Position tracking
        self.inventory_price: Optional[Decimal] = None
        self.total_sold_since_start: Decimal = Decimal('0')
        self.hourly_sold_amounts: Dict[int, Decimal] = {}  # hour -> amount sold
        
        # Enhanced position tracking for different target scenarios
        self.current_net_base_position: Decimal = Decimal('0')  # P1 - net base inventory
        self.total_quote_value_from_sells: Decimal = Decimal('0')  # Total quote received from sells
        self.last_inventory_calculation_time: Optional[datetime] = None
        
        # Main task
        self.main_task: Optional[asyncio.Task] = None
        
        # Performance tracking
        self.orders_placed = 0
        self.orders_filled = 0
        # REMOVED: total_sold_amount, total_sold_value - DATABASE IS THE ONLY SOURCE OF TRUTH
        # These values are now calculated fresh from database on every iteration
        
        # Cooldown tracking
        self.in_cooldown: bool = False
        self.cooldown_until: Optional[datetime] = None
        
    async def _validate_config(self) -> None:
        """Validate Targeted Sellbot configuration."""
        required_fields = [
            'start_time', 'total_time_hours', 'spread_bps', 'timeout_seconds',
            'drift_bps', 'taker_check'
        ]
        
        for field in required_fields:
            if field not in self.config and field != 'pricing_algorithm' and field != 'tracking_mode': # pricing_algorithm is optional
                raise ValueError(f"Missing required config field: {field}")
        
        self.total_time_hours = float(self.config['total_time_hours'])
        self.spread_bps = int(self.config['spread_bps'])
        self.timeout_seconds = int(self.config['timeout_seconds'])
        self.drift_bps = int(self.config['drift_bps'])
        self.taker_check = bool(self.config['taker_check'])
        
        if '/' in self.symbol:
            self.base_coin, self.quote_coin = self.symbol.split('/')
        else:
            raise ValueError(f"Invalid symbol format: {self.symbol}")

        # Prioritize new sell_mode configuration
        if 'sell_mode' in self.config:
            self.sell_mode = SellMode(self.config['sell_mode'])
            
            sell_target = self.config.get('sell_target_value')
            if sell_target is None or str(sell_target).strip() == '':
                raise ValueError("sell_target_value is missing or empty for the selected sell mode")

            try:
                self.sell_target_value = Decimal(str(sell_target))
            except InvalidOperation:
                raise ValueError(f"Invalid sell_target_value '{sell_target}'")

            if self.sell_mode == SellMode.PERCENT_INVENTORY:
                self.tracking_mode = TrackingMode.PERCENTAGE
                self.logger.info(f"Sell mode: {self.sell_target_value}% of TOTAL inventory over {self.total_time_hours} hours")
            elif self.sell_mode == SellMode.HOURLY_RATE:
                self.tracking_mode = TrackingMode.TARGET
                self.target_currency = TargetCurrency(self.config.get('target_currency', 'base'))
                # Calculate the total target amount from the hourly rate and duration
                self.target_amount = self.sell_target_value * Decimal(str(self.total_time_hours))
                self.logger.info(f"Sell mode: {self.sell_target_value} {self.target_currency.value}/hour. Total target: {self.target_amount}")
        
        # Fallback to legacy configuration if sell_mode is not present
        else:
            self.logger.warning("Falling back to legacy tracking_mode. It's recommended to update the bot configuration to use sell_mode.")
            if 'tracking_mode' not in self.config:
                raise ValueError("Missing required config field: tracking_mode (for legacy configs)")
            
            self.tracking_mode = TrackingMode(self.config['tracking_mode'])
            if self.tracking_mode == TrackingMode.PERCENTAGE:
                if 'hourly_percentage' not in self.config:
                    raise ValueError("hourly_percentage required when tracking_mode is 'percentage'")
                self.hourly_percentage = float(self.config['hourly_percentage'])
                self.sell_target_value = Decimal(str(self.hourly_percentage)) # For internal consistency
                self.sell_mode = SellMode.PERCENT_INVENTORY
            else:  # TARGET mode
                if 'target_amount' not in self.config:
                    raise ValueError("target_amount required when tracking_mode is 'target'")
                if 'target_currency' not in self.config:
                    raise ValueError("target_currency required when tracking_mode is 'target'")
                self.target_amount = Decimal(str(self.config['target_amount']))
                self.target_currency = TargetCurrency(self.config['target_currency'])
                self.sell_target_value = self.target_amount # For internal consistency
                self.sell_mode = SellMode.HOURLY_RATE # Approximate mapping

        # Parse pricing algorithm (may not be present in new configs)
        if 'pricing_algorithm' in self.config:
            self.pricing_algorithm = PricingAlgorithm(self.config.get('pricing_algorithm', 'greater_of'))
        else:
            self.pricing_algorithm = PricingAlgorithm.GREATER_OF
        
        if 'minimum_sell_price' in self.config and self.config['minimum_sell_price'] is not None and str(self.config['minimum_sell_price']).strip() != '':
            try:
                self.minimum_sell_price = Decimal(str(self.config['minimum_sell_price']))
                self.logger.info(f"Minimum sell price set to: {self.minimum_sell_price}")
            except InvalidOperation:
                 raise ValueError(f"Invalid minimum_sell_price: {self.config['minimum_sell_price']}")
        
        if 'price_comparison_mode' in self.config:
            self.price_comparison_mode = PriceComparisonMode(self.config['price_comparison_mode'])
        else:
            if self.pricing_algorithm == PricingAlgorithm.GREATER_OF:
                self.price_comparison_mode = PriceComparisonMode.GREATER
            elif self.pricing_algorithm == PricingAlgorithm.LESSER_OF:
                self.price_comparison_mode = PriceComparisonMode.LESSER
            elif self.pricing_algorithm == PricingAlgorithm.INVENTORY_PLUS_SPREAD:
                self.price_comparison_mode = PriceComparisonMode.INVENTORY_ONLY
            elif self.pricing_algorithm == PricingAlgorithm.INTERVAL_PLUS_SPREAD:
                self.price_comparison_mode = PriceComparisonMode.INTERVAL_ONLY

        start_time_str = self.config.get('start_time')
        if start_time_str:
            try:
                if isinstance(start_time_str, str):
                    if 'T' in start_time_str and start_time_str.endswith('Z'):
                        self.start_time = datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))
                    elif 'T' in start_time_str and '+' in start_time_str:
                        self.start_time = datetime.fromisoformat(start_time_str)
                    elif 'T' in start_time_str:
                        naive_dt = datetime.fromisoformat(start_time_str)
                        self.start_time = naive_dt.replace(tzinfo=timezone.utc)
                    else:
                        naive_dt = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M")
                        self.start_time = naive_dt.replace(tzinfo=timezone.utc)
                elif isinstance(start_time_str, datetime):
                    if start_time_str.tzinfo is None:
                        self.start_time = start_time_str.replace(tzinfo=timezone.utc)
                    else:
                        self.start_time = start_time_str.astimezone(timezone.utc)
            except Exception as e:
                self.logger.error(f"Could not parse start_time '{start_time_str}': {e}")
                self.start_time = datetime.now(timezone.utc)
        else:
            self.start_time = datetime.now(timezone.utc)
        
        self.end_time = self.start_time + timedelta(hours=self.total_time_hours)
        
        self.logger.info(f"Validated Targeted Sellbot config. Sell mode: {self.sell_mode.value}, Price comparison: {self.price_comparison_mode.value}")
        self.logger.info(f"Strategy will run from {self.start_time} to {self.end_time} UTC")
    
    async def _start_strategy(self) -> None:
        """Start the Targeted Sellbot strategy."""
        self.logger.info("Starting Targeted Sellbot strategy")
        
        # Initialize Redis connection (async version)
        self.redis_client = redis.from_url(self.redis_url)
        
        # Start Redis orderbook manager
        self.redis_orderbook_manager = RedisOrderbookManager(self.redis_url)
        await self.redis_orderbook_manager.start()
        
        # Initialize execution components
        await self._initialize_execution_components()
        
        # Calculate initial position and inventory price (P1 & P2 needed for target tracking)
        try:
            await self._calculate_current_position()
            await self._calculate_inventory_price()
        except Exception as e:
            self.logger.error(f"Error during initial position/inventory calculation: {e}")
            # Set defaults to avoid blocking the strategy
            self.current_net_base_position = Decimal('0')
            self.inventory_price = None
        
        # Capture initial inventory for percentage-based selling
        # This is the net base position at start time (only from trades since start)
        self.initial_inventory = self.current_net_base_position if self.current_net_base_position and self.current_net_base_position > 0 else Decimal('0')
        self.logger.info(f"Initial inventory at start: {self.initial_inventory} {self.base_coin} (from trades since {self.start_time})")
        
        # Subscribe to interval target prices from aggressive TWAP
        self.interval_subscription_task = asyncio.create_task(self._subscribe_to_interval_prices())
        
        # Subscribe to cooldown signals
        asyncio.create_task(self._subscribe_to_cooldown_signals())
        
        # Start main strategy loop
        self.main_task = asyncio.create_task(self._strategy_loop())
        
    async def _stop_strategy(self) -> None:
        """Stop the Targeted Sellbot strategy."""
        self.logger.info("Stopping Targeted Sellbot strategy")
        
        # Cancel main task
        if self.main_task:
            self.main_task.cancel()
            try:
                await self.main_task
            except asyncio.CancelledError:
                pass
        
        # Cancel subscription tasks
        if self.interval_subscription_task:
            self.interval_subscription_task.cancel()
            try:
                await self.interval_subscription_task
            except asyncio.CancelledError:
                pass
        
        # Stop execution components
        if self.execution_engine:
            await self.execution_engine.stop()
        
        # Stop Redis components
        if self.redis_orderbook_manager:
            await self.redis_orderbook_manager.stop()
        if self.redis_pubsub:
            await self.redis_pubsub.unsubscribe()
            await self.redis_pubsub.close()
        if self.redis_client:
            await self.redis_client.close()
    
    async def _initialize_execution_components(self) -> None:
        """Initialize execution engine and order manager."""
        # Initialize position manager
        self.position_manager = PositionManager()
        await self.position_manager.start()
        
        # Set up trade repository for position calculations
        try:
            from database import get_session
            from database.repositories import TradeRepository
            
            async for session in get_session():
                trade_repository = TradeRepository(session)
                self.position_manager.set_trade_repository(trade_repository)
                self.logger.info("Trade repository set up for position calculations")
                break
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
        
    async def _calculate_current_position(self) -> Decimal:
        """Calculate current position including ALL sell trades since start time."""
        try:
            from database import get_session
            from database.repositories import TradeRepository
            from database.models import Trade, Exchange
            
            total_sold = Decimal('0')
            total_trades_found = 0
            total_sells = 0
            
            # Reset quote value tracking since we're recalculating
            self.total_quote_value_from_sells = Decimal('0')
            
            async for session in get_session():
                try:
                    # Begin fresh transaction
                    await session.begin()
                    
                    # Get the start time for inventory calculations
                    start_time_for_db = self.get_inventory_start_time()
                    # Convert timezone-aware datetime to timezone-naive for database query
                    if start_time_for_db.tzinfo is not None:
                        start_time_for_db = start_time_for_db.astimezone(timezone.utc).replace(tzinfo=None)
                    
                    self.logger.info(f"🔍 Calculating total sold since start time: {start_time_for_db}")
                    
                    # Get ALL exchange IDs from database, not just configured ones
                    stmt = select(Exchange.id, Exchange.name)
                    result = await session.execute(stmt)
                    all_exchanges = result.all()
                    
                    exchange_ids = [ex.id for ex in all_exchanges]
                    
                    if not exchange_ids:
                        self.logger.warning("No exchanges found in database")
                        return Decimal('0')
                    
                    self.logger.info(f"🔍 Calculating total sold since start time: {start_time_for_db} from {len(exchange_ids)} exchanges")
                    
                    # Query ALL trades for this symbol since start time across all exchanges at once
                    # Eagerly load the exchange relationship to avoid lazy loading issues
                    stmt = select(Trade).options(
                        joinedload(Trade.exchange)
                    ).where(
                        and_(
                            Trade.exchange_id.in_(exchange_ids),
                            Trade.symbol == self.symbol,
                            Trade.timestamp >= start_time_for_db
                        )
                    ).order_by(Trade.timestamp.asc())
                    
                    result = await session.execute(stmt)
                    trades = result.scalars().unique().all()
                    
                    self.logger.info(f"📊 Found {len(trades)} trades for {self.symbol} since {start_time_for_db}")
                    
                    # Process all trades
                    for trade in trades:
                        # Only count sell trades for amount sold tracking
                        if trade.side.lower() == 'sell':
                            amount = Decimal(str(trade.amount))
                            price = Decimal(str(trade.price))
                            quote_value = amount * price
                            
                            total_sold += amount
                            self.total_quote_value_from_sells += quote_value
                            total_sells += 1
                            
                            # Get exchange name from our mapping to avoid lazy loading
                            exchange_name = exchange_names.get(trade.exchange_id, f"Exchange_{trade.exchange_id}")
                            
                            # Log first few sell trades for debugging
                            if total_sells <= 3:
                                self.logger.debug(
                                    f"Sell trade {total_sells}: {trade.timestamp} | {exchange_name} | "
                                    f"SELL {amount} @ {price} = ${quote_value:.2f} | ID: {trade.exchange_trade_id}"
                                )
                        
                        total_trades_found += 1
                    
                    # Update tracking
                    self.total_sold_since_start = total_sold
                    
                    self.logger.info(f"📊 TOTAL SOLD CALCULATION SUMMARY:")
                    self.logger.info(f"   Total trades found: {total_trades_found}")
                    self.logger.info(f"   Total sells: {total_sells}")
                    self.logger.info(f"   Total sold since start: {self.total_sold_since_start:.4f} {self.base_coin}")
                    self.logger.info(f"   Total quote value from sells: ${self.total_quote_value_from_sells:.2f}")
                    
                    # Update hourly sold amounts
                    await self._update_hourly_sold_amounts_optimized(trades)
                    
                    # Commit the read-only transaction
                    await session.commit()
                    break
                    
                except Exception as e:
                    await session.rollback()
                    self.logger.error(f"Error in position calculation: {e}")
                    raise e
                finally:
                    await session.close()
                
            return total_sold
            
        except Exception as e:
            self.logger.error(f"Error calculating current position: {e}")
            return Decimal('0')
    
    async def _update_hourly_sold_amounts_optimized(self, trades: List[Any]) -> None:
        """Update hourly sold amounts from provided trades list."""
        try:
            self.hourly_sold_amounts.clear()
            inventory_start_time = self.get_inventory_start_time()
            
            # Process only sell trades for hourly breakdown
            for trade in trades:
                if trade.side.lower() == 'sell':
                    # Calculate hour since start
                    trade_time = trade.timestamp
                    if trade_time.tzinfo is None:
                        trade_time = trade_time.replace(tzinfo=timezone.utc)
                        
                    hours_since_start = int((trade_time - inventory_start_time).total_seconds() / 3600)
                    
                    if hours_since_start not in self.hourly_sold_amounts:
                        self.hourly_sold_amounts[hours_since_start] = Decimal('0')
                        
                    self.hourly_sold_amounts[hours_since_start] += Decimal(str(trade.amount))
                    
        except Exception as e:
            self.logger.error(f"Error updating hourly sold amounts: {e}")
    
    def _update_hourly_sold_amounts(self, trades: List[Any]) -> None:
        """Legacy method - now replaced by _update_hourly_sold_amounts_optimized."""
        self.logger.debug("Using optimized hourly tracking method instead of legacy approach")
        # This method is now replaced by the more comprehensive method above
    
    async def _calculate_inventory_price(self) -> None:
        """Calculate inventory price from trades since start time only."""
        try:
            from database import get_session
            from database.repositories import TradeRepository
            from database.models import Trade, Exchange
            
            p1_total = Decimal('0')  # Buy side base amount
            p2_total = Decimal('0')  # Buy side quote amount (negative)
            p1_fees_total = Decimal('0')  # Fees in base currency
            p2_fees_total = Decimal('0')  # Fees in quote currency
            
            total_trades_found = 0
            total_buys = 0
            total_sells = 0
            
            async for session in get_session():
                try:
                    # Begin fresh transaction
                    await session.begin()
                    
                    # Get the start time for inventory calculations
                    start_time_for_db = self.get_inventory_start_time()
                    # Convert timezone-aware datetime to timezone-naive for database query
                    if start_time_for_db.tzinfo is not None:
                        start_time_for_db = start_time_for_db.astimezone(timezone.utc).replace(tzinfo=None)
                    
                    self.logger.info(f"🔍 Calculating inventory price for {self.symbol} since {start_time_for_db}")
                    
                    # Get ALL exchange IDs from database, not just configured ones
                    stmt = select(Exchange.id, Exchange.name)
                    result = await session.execute(stmt)
                    all_exchanges = result.all()
                    
                    exchange_ids = [ex.id for ex in all_exchanges]
                    
                    if not exchange_ids:
                        self.logger.warning("No exchanges found in database")
                        return
                    
                    # Query ALL trades for this symbol since start time across all exchanges at once
                    # Eagerly load the exchange relationship to avoid lazy loading issues
                    stmt = select(Trade).options(
                        joinedload(Trade.exchange)
                    ).where(
                        and_(
                            Trade.exchange_id.in_(exchange_ids),
                            Trade.symbol == self.symbol,
                            Trade.timestamp >= start_time_for_db
                        )
                    ).order_by(Trade.timestamp.asc())
                    
                    result = await session.execute(stmt)
                    trades = result.scalars().unique().all()
                    
                    self.logger.info(f"📊 Found {len(trades)} trades since {start_time_for_db}")
                    
                    # Process all trades
                    for trade in trades:
                        amount = Decimal(str(trade.amount))
                        price = Decimal(str(trade.price))
                        fee_cost = Decimal(str(trade.fee_cost)) if trade.fee_cost else Decimal('0')
                        fee_currency = trade.fee_currency
                        
                        # Get exchange name from our mapping to avoid lazy loading
                        exchange_name = exchange_names.get(trade.exchange_id, f"Exchange_{trade.exchange_id}")
                        
                        # Log first few trades for debugging
                        if total_trades_found < 5:
                            self.logger.debug(
                                f"Trade {total_trades_found + 1}: {trade.timestamp} | {exchange_name} | "
                                f"{trade.side} {amount} @ {price} | Fee: {fee_cost} {fee_currency or 'UNKNOWN'}"
                            )
                        
                        if trade.side.lower() == 'buy':
                            # Buy: acquire base, spend quote
                            p1_total += amount
                            p2_total -= (amount * price)
                            
                            # Handle fees based on currency
                            if fee_currency and fee_currency.upper() == self.base_coin.upper():
                                # Fee paid in base currency (reduce base amount received)
                                p1_fees_total += fee_cost
                            else:
                                # Fee paid in quote currency (increase quote amount spent)
                                # If fee_currency is missing, assume quote currency
                                p2_fees_total += fee_cost
                            
                            total_buys += 1
                        else:  # sell
                            # Sell: lose base, gain quote
                            p1_total -= amount
                            p2_total += (amount * price)
                            
                            # Handle fees based on currency
                            if fee_currency and fee_currency.upper() == self.base_coin.upper():
                                # Fee paid in base currency (additional base lost)
                                p1_fees_total += fee_cost
                            else:
                                # Fee paid in quote currency (reduce quote received)
                                # If fee_currency is missing, assume quote currency
                                p2_fees_total += fee_cost
                            
                            total_sells += 1
                        
                        total_trades_found += 1
                    
                    # Calculate inventory price
                    self.logger.info(f"🧮 Inventory calculation (trades since {self.start_time}):")
                    self.logger.info(f"   Total trades: {total_trades_found} ({total_buys} buys, {total_sells} sells)")
                    self.logger.info(f"   P1 (base amount): {p1_total}")
                    self.logger.info(f"   P2 (quote amount): {p2_total}")
                    self.logger.info(f"   P1 fees (base): {p1_fees_total}")
                    self.logger.info(f"   P2 fees (quote): {p2_fees_total}")
                    
                    # Net base position after fees
                    net_base_position = p1_total - p1_fees_total
                    
                    # Store net base position for inventory tracking
                    self.current_net_base_position = net_base_position
                    self.last_inventory_calculation_time = datetime.now(timezone.utc)
                    
                    if net_base_position > 0:
                        # Calculate average entry price (excluding unrealized PnL)
                        # Total quote spent (including fees)
                        net_quote_cost = -(p2_total + p2_fees_total)
                        self.inventory_price = net_quote_cost / net_base_position
                        self.logger.info(f"✅ Calculated inventory price: {self.inventory_price:.4f} {self.quote_coin}")
                        self.logger.info(f"   Formula: {net_quote_cost:.4f} / {net_base_position:.4f} = {self.inventory_price:.4f}")
                        self.logger.info(f"   📊 Net base inventory: {self.current_net_base_position:.4f} {self.base_coin}")
                    else:
                        self.logger.warning(f"❌ No net positive position to calculate inventory price")
                        self.logger.warning(f"   Net base position: {net_base_position:.4f} (need > 0)")
                        self.inventory_price = None
                        self.logger.info(f"   📊 Net base inventory: {self.current_net_base_position:.4f} {self.base_coin}")
                    
                    # Commit the read-only transaction
                    await session.commit()
                    break
                    
                except Exception as e:
                    await session.rollback()
                    self.logger.error(f"Error in inventory calculation: {e}")
                    raise e
                finally:
                    await session.close()
                
        except Exception as e:
            self.logger.error(f"Error calculating inventory price: {e}")
            self.inventory_price = None
    
    async def _subscribe_to_interval_prices(self) -> None:
        """Subscribe to interval target prices from aggressive TWAP."""
        try:
            self.redis_pubsub = self.redis_client.pubsub()
            channel = f"interval_target_price:{self.symbol}"
            
            await self.redis_pubsub.subscribe(channel)
            self.logger.info(f"Subscribed to interval target prices on channel: {channel}")
            
            # Use get_message with timeout to avoid blocking
            while self.running:
                try:
                    message = await self.redis_pubsub.get_message(ignore_subscribe_messages=True, timeout=0.1)
                    if message and message['type'] == 'message':
                        try:
                            data = json.loads(message['data'])
                            self.current_interval_target_price = Decimal(str(data['target_price']))
                            self.last_interval_update = datetime.now(timezone.utc)
                            
                            self.logger.debug(f"Received interval target price: {self.current_interval_target_price}")
                            
                        except Exception as e:
                            self.logger.error(f"Error processing interval price message: {e}")
                except asyncio.TimeoutError:
                    # No message received, continue
                    pass
                except Exception as e:
                    self.logger.error(f"Error getting message: {e}")
                    
                # Small sleep to prevent tight loop
                await asyncio.sleep(0.1)
                        
        except asyncio.CancelledError:
            self.logger.info("Interval price subscription cancelled")
        except Exception as e:
            self.logger.error(f"Error in interval price subscription: {e}")
    
    async def _subscribe_to_cooldown_signals(self) -> None:
        """Subscribe to cooldown signals from aggressive TWAP."""
        try:
            cooldown_key = f"strategy_cooldown:{self.symbol}"
            
            while self.running:
                # Check for cooldown signal
                cooldown_data = await self.redis_client.get(cooldown_key)
                
                if cooldown_data:
                    try:
                        data = json.loads(cooldown_data)
                        cooldown_until = datetime.fromisoformat(data['cooldown_until'])
                        
                        # Check if cooldown is still active
                        if cooldown_until > datetime.now(timezone.utc):
                            self.in_cooldown = True
                            self.cooldown_until = cooldown_until
                            self.logger.info(f"Entering cooldown until {cooldown_until}")
                        else:
                            self.in_cooldown = False
                            self.cooldown_until = None
                    except Exception as e:
                        self.logger.error(f"Error processing cooldown signal: {e}")
                else:
                    self.in_cooldown = False
                    self.cooldown_until = None
                
                await asyncio.sleep(1)  # Check every second
                
        except asyncio.CancelledError:
            self.logger.info("Cooldown subscription cancelled")
        except Exception as e:
            self.logger.error(f"Error in cooldown subscription: {e}")
        
    async def _strategy_loop(self) -> None:
        """Main strategy loop."""
        self.logger.info("Starting Targeted Sellbot main loop")
        
        # Track if we've reached target to allow resuming
        target_reached_previously = False
        
        while self.running:
            try:
                # Check if we've reached the end time
                current_time = datetime.now(timezone.utc)
                if current_time >= self.end_time:
                    self.logger.info(f"Strategy end time reached ({self.end_time}). Stopping strategy.")
                    await self.stop()
                    break
                
                # Check if in cooldown
                if self.in_cooldown and self.cooldown_until:
                    if current_time < self.cooldown_until:
                        await asyncio.sleep(1)
                        continue
                    else:
                        self.in_cooldown = False
                        self.cooldown_until = None
                        self.logger.info("Cooldown period ended, resuming trading")
                
                # Recalculate position and inventory price fresh from database
                await self._calculate_current_position()
                await self._calculate_inventory_price()
                
                # Check current inventory
                current_inventory = self._get_current_inventory()
                
                # Check if target reached
                is_target_reached = self._is_target_reached()
                
                if is_target_reached:
                    if not target_reached_previously:
                        self.logger.info("Target reached - pausing quotes")
                        target_reached_previously = True
                    
                    # If we have no inventory, continue waiting
                    if current_inventory <= 0:
                        self.logger.debug("Target reached and no inventory available")
                        await asyncio.sleep(5)
                        continue
                    
                    # If we have new inventory after reaching target, resume
                    self.logger.info(f"Target was reached but new inventory detected ({current_inventory:.4f} {self.base_coin}) - resuming quotes")
                    # Don't stop - continue to try selling the new inventory
                else:
                    # Target not reached, reset flag
                    if target_reached_previously:
                        self.logger.info("Target no longer reached (new inventory accumulated) - resuming normal operation")
                        target_reached_previously = False
                
                # If no inventory available, wait, unless in a mode that doesn't depend on inventory
                if self.price_comparison_mode != PriceComparisonMode.TARGET_ONLY and current_inventory <= 0:
                    self.logger.debug(f"No inventory to sell (current: {current_inventory:.4f} {self.base_coin})")
                    await asyncio.sleep(5)
                    continue
                
                # Cancel stale orders
                await self._cancel_stale_orders()
                
                # Place new quotes
                await self._place_quotes()
                
                # Sleep before next iteration
                await asyncio.sleep(5)  # 5 second frequency
                
            except asyncio.CancelledError:
                self.logger.info("Strategy loop cancelled")
                break
            except Exception as e:
                self.logger.error(f"Error in strategy loop: {e}")
                await asyncio.sleep(5)
    
    def _is_target_reached(self) -> bool:
        """Check if the selling target has been reached."""
        if self.tracking_mode == TrackingMode.PERCENTAGE:
            # For percentage mode, check if we've sold the target percentage of initial inventory
            if not self.initial_inventory or self.initial_inventory <= 0:
                # If no initial inventory, check if we have current inventory
                current_inventory = self._get_current_inventory()
                if current_inventory > 0:
                    # We have inventory now but no initial inventory recorded
                    # Don't consider target reached - we need to start selling
                    self.logger.debug("No initial inventory recorded yet, but have current inventory - target not reached")
                    return False
                else:
                    # No inventory at all - can't sell anything
                    self.logger.debug("No initial inventory and no current inventory - no target to reach")
                    return False
            
            # Calculate total target amount to sell based on initial inventory
            if self.sell_mode == SellMode.PERCENT_INVENTORY and self.sell_target_value:
                # Use sell_target_value for new config style
                total_target = self.initial_inventory * (self.sell_target_value / Decimal('100'))
                target_percentage = self.sell_target_value
            else:
                # Use hourly_percentage for backward compatibility
                total_target = self.initial_inventory * (Decimal(str(self.hourly_percentage)) / Decimal('100'))
                target_percentage = Decimal(str(self.hourly_percentage))
            
            # Check if we've sold the target amount
            progress = (self.total_sold_since_start / total_target * 100) if total_target > 0 else 100
            
            self.logger.debug(f"Percentage mode progress: {self.total_sold_since_start:.4f}/{total_target:.4f} {self.base_coin} "
                            f"({progress:.1f}% of {target_percentage}% target)")
            
            return self.total_sold_since_start >= total_target
        else:  # TARGET mode
            if self.target_currency == TargetCurrency.BASE:
                # Target amount in base coin - use total sold amount
                if self.target_amount is None or self.target_amount <= 0:
                    self.logger.warning("Invalid target_amount in BASE target mode")
                    return True  # Consider target reached if invalid
                progress = (self.total_sold_since_start / self.target_amount * 100) if self.target_amount > 0 else 0
                self.logger.debug(f"Base target mode progress: {self.total_sold_since_start:.4f}/{self.target_amount:.4f} {self.base_coin} ({progress:.1f}%)")
                return self.total_sold_since_start >= self.target_amount
            else:  # QUOTE currency - target amount in dollars
                # Use total quote value received from sells
                if self.target_amount is None or self.target_amount <= 0:
                    self.logger.warning("Invalid target_amount in QUOTE target mode")
                    return True  # Consider target reached if invalid
                progress = (self.total_quote_value_from_sells / self.target_amount * 100) if self.target_amount > 0 else 0
                self.logger.debug(f"Quote target mode progress: ${self.total_quote_value_from_sells:.2f}/${self.target_amount:.2f} ({progress:.1f}%)")
                return self.total_quote_value_from_sells >= self.target_amount
    
    def _get_current_inventory(self) -> Decimal:
        """Get current BASE coin inventory from net position calculation (P1)."""
        try:
            # Use the actual net base position calculated from inventory price calculation
            # This is now recalculated fresh on each iteration
            if self.current_net_base_position is None:
                self.logger.warning("No inventory calculation available yet")
                return Decimal('0')
            
            self.logger.debug(f"Current inventory (net base position): {self.current_net_base_position:.4f} {self.base_coin}")
            return max(Decimal('0'), self.current_net_base_position)  # Return 0 if negative (no inventory to sell)
            
        except Exception as e:
            self.logger.error(f"Error getting current inventory: {e}")
            return Decimal('0')
    
    async def _cancel_stale_orders(self) -> None:
        """Cancel orders that have timed out or drifted too far."""
        try:
            current_midpoint = await self._get_current_midpoint()
            if not current_midpoint:
                return
            
            for order_id, order_info in list(self.active_orders.items()):
                # Skip orders that don't belong to this instance
                if not self._order_belongs_to_instance(order_id):
                    continue
                
                order_age = (datetime.now(timezone.utc) - datetime.fromisoformat(order_info['timestamp'])).total_seconds()
                exchange = order_info.get('exchange')
                
                if not exchange:
                    self.logger.warning(f"Order {order_id} missing exchange info, skipping")
                    continue
                
                # Check timeout
                if order_age > self.timeout_seconds:
                    self.logger.info(f"Cancelling order {order_id} due to timeout")
                    await self._cancel_order(order_id, exchange)
                    continue
                
                # Check drift
                order_price = Decimal(str(order_info['price']))
                price_drift_bps = abs((order_price - current_midpoint) / current_midpoint) * Decimal('10000')
                
                if price_drift_bps > self.drift_bps:
                    self.logger.info(f"Cancelling order {order_id} due to drift ({price_drift_bps:.0f} bps)")
                    await self._cancel_order(order_id, exchange)
                    
        except Exception as e:
            self.logger.error(f"Error cancelling stale orders: {e}")
    
    async def _place_quotes(self) -> None:
        """Place sell quotes based on pricing algorithm."""
        try:
            # Calculate base quote price
            base_price = await self._calculate_quote_price()
            if not base_price:
                self.logger.warning("Could not calculate base quote price")
                return
            
            # Apply minimum sell price if configured
            if self.minimum_sell_price:
                if base_price < self.minimum_sell_price:
                    self.logger.debug(f"Base price {base_price} below minimum {self.minimum_sell_price}, using minimum")
                    base_price = self.minimum_sell_price
            
            # Calculate quote size
            quote_size = await self._calculate_quote_size()
            if quote_size <= 0:
                self.logger.debug("No size to quote")
                return
            
            # Place orders on all exchanges at their best offer
            for exchange in self.exchanges:
                try:
                    # Get exchange orderbook
                    orderbook = await self._get_exchange_orderbook(exchange)
                    if not orderbook or 'bids' not in orderbook or 'asks' not in orderbook:
                        self.logger.warning(f"No orderbook available for {exchange}")
                        continue
                    
                    # Get best bid and ask for this exchange
                    best_bid = Decimal(str(orderbook['bids'][0][0])) if orderbook['bids'] else None
                    best_ask = Decimal(str(orderbook['asks'][0][0])) if orderbook['asks'] else None
                    
                    if not best_bid:
                        self.logger.warning(f"No best bid available for {exchange}")
                        continue
                    
                    # Check if base price is above best bid
                    if base_price > best_bid:
                        quote_price = base_price
                    else:
                        # If the best bid is better than our minimum price, take it
                        quote_price = best_bid
                    
                    # If quoting at best offer, adjust to be just below it to ensure it's a maker order
                    if best_ask and quote_price >= best_ask:
                        quote_price = best_ask * (Decimal('1') - Decimal('0.0001')) # 1 bps below
                    
                    # Apply taker check if enabled
                    if self.taker_check and best_bid and quote_price <= best_bid:
                        self.logger.debug(f"{exchange}: Taker check failed - quote {quote_price} <= bid {best_bid}, adjusting")
                        # Adjust to be slightly above best bid
                        quote_price = best_bid * (Decimal('1') + Decimal('0.0001'))
                    
                    # Final check against minimum sell price
                    if self.minimum_sell_price and quote_price < self.minimum_sell_price:
                        self.logger.warning(f"Final quote price {quote_price} is below minimum sell price {self.minimum_sell_price}. Skipping.")
                        continue
                    
                    # Place the order
                    await self._place_single_quote(exchange, quote_size, quote_price)
                    
                except Exception as e:
                    self.logger.error(f"Error placing quote on {exchange}: {e}")
                    
        except Exception as e:
            self.logger.error(f"Error placing quotes: {e}")
    
    async def _calculate_quote_price(self) -> Optional[Decimal]:
        """Calculate quote price based on selected pricing algorithm."""
        try:
            # Get base prices
            inventory_price = self.inventory_price
            interval_target_price = self.current_interval_target_price
            target_price = self.minimum_sell_price  # Use minimum sell price as target price
            
            # Convert spread from bps to decimal
            spread = Decimal(str(self.spread_bps)) / Decimal('10000')
            
            # Collect available prices
            prices = []
            
            if self.price_comparison_mode == PriceComparisonMode.INVENTORY_ONLY:
                if inventory_price:
                    return inventory_price * (Decimal('1') + spread)
                else:
                    self.logger.warning("Inventory price not available for INVENTORY_ONLY mode")
                    return await self._fallback_to_midpoint_price(spread)
                    
            elif self.price_comparison_mode == PriceComparisonMode.INTERVAL_ONLY:
                if interval_target_price:
                    return interval_target_price * (Decimal('1') + spread)
                else:
                    self.logger.warning("Interval price not available for INTERVAL_ONLY mode")
                    return await self._fallback_to_midpoint_price(spread)
                    
            elif self.price_comparison_mode == PriceComparisonMode.TARGET_ONLY:
                if target_price:
                    return target_price  # Target price already includes desired level
                else:
                    self.logger.warning("Target price not set for TARGET_ONLY mode")
                    return await self._fallback_to_midpoint_price(spread)
            
            # For GREATER and LESSER modes, collect all available prices
            if inventory_price:
                prices.append(inventory_price * (Decimal('1') + spread))
            if interval_target_price:
                prices.append(interval_target_price * (Decimal('1') + spread))
            if target_price:
                prices.append(target_price)
                
            if not prices:
                self.logger.warning("No prices available for comparison")
                return await self._fallback_to_midpoint_price(spread)
            
            # Apply comparison mode
            if self.price_comparison_mode == PriceComparisonMode.GREATER:
                selected_price = max(prices)
                self.logger.debug(f"GREATER mode: selected {selected_price} from {prices}")
                return selected_price
            elif self.price_comparison_mode == PriceComparisonMode.LESSER:
                selected_price = min(prices)
                self.logger.debug(f"LESSER mode: selected {selected_price} from {prices}")
                return selected_price
            else:
                # Backward compatibility - use old pricing algorithm logic
                if self.pricing_algorithm == PricingAlgorithm.GREATER_OF:
                    if inventory_price and interval_target_price:
                        # Both prices available - use greater of the two
                        inventory_plus_spread = inventory_price * (Decimal('1') + spread)
                        interval_plus_spread = interval_target_price * (Decimal('1') + spread)
                        return max(inventory_plus_spread, interval_plus_spread)
                    elif inventory_price:
                        # Only inventory price available
                        self.logger.debug("Using inventory price as fallback for GREATER_OF")
                        return inventory_price * (Decimal('1') + spread)
                    elif interval_target_price:
                        # Only interval price available
                        self.logger.debug("Using interval target price as fallback for GREATER_OF")
                        return interval_target_price * (Decimal('1') + spread)
                    else:
                        # Neither available - fall back to market midpoint
                        return await self._fallback_to_midpoint_price(spread)
                    
                elif self.pricing_algorithm == PricingAlgorithm.LESSER_OF:
                    if inventory_price and interval_target_price:
                        # Both prices available - use lesser of the two
                        inventory_plus_spread = inventory_price * (Decimal('1') + spread)
                        interval_plus_spread = interval_target_price * (Decimal('1') + spread)
                        return min(inventory_plus_spread, interval_plus_spread)
                    elif inventory_price:
                        # Only inventory price available
                        self.logger.debug("Using inventory price as fallback for LESSER_OF")
                        return inventory_price * (Decimal('1') + spread)
                    elif interval_target_price:
                        # Only interval price available
                        self.logger.debug("Using interval target price as fallback for LESSER_OF")
                        return interval_target_price * (Decimal('1') + spread)
                    else:
                        # Neither available - fall back to market midpoint
                        return await self._fallback_to_midpoint_price(spread)
                    
                elif self.pricing_algorithm == PricingAlgorithm.INVENTORY_PLUS_SPREAD:
                    if inventory_price:
                        return inventory_price * (Decimal('1') + spread)
                    else:
                        # Fall back to interval price or midpoint
                        if interval_target_price:
                            self.logger.debug("Using interval target price as fallback for INVENTORY_PLUS_SPREAD")
                            return interval_target_price * (Decimal('1') + spread)
                        else:
                            return await self._fallback_to_midpoint_price(spread)
                    
                elif self.pricing_algorithm == PricingAlgorithm.INTERVAL_PLUS_SPREAD:
                    if interval_target_price:
                        return interval_target_price * (Decimal('1') + spread)
                    else:
                        # Fall back to inventory price or midpoint
                        if inventory_price:
                            self.logger.debug("Using inventory price as fallback for INTERVAL_PLUS_SPREAD")
                            return inventory_price * (Decimal('1') + spread)
                        else:
                            return await self._fallback_to_midpoint_price(spread)
                
        except Exception as e:
            self.logger.error(f"Error calculating quote price: {e}")
            return None
    
    async def _fallback_to_midpoint_price(self, spread: Decimal) -> Optional[Decimal]:
        """Fallback to market midpoint + spread when other prices unavailable."""
        try:
            midpoint = await self._get_current_midpoint()
            if midpoint:
                fallback_price = midpoint * (Decimal('1') + spread)
                self.logger.info(f"Using market midpoint as fallback: {midpoint} -> {fallback_price}")
                return fallback_price
            else:
                self.logger.warning("No midpoint price available for fallback")
                return None
        except Exception as e:
            self.logger.error(f"Error calculating fallback midpoint price: {e}")
            return None
    
    async def _calculate_quote_size(self) -> Decimal:
        """Calculate size to quote based on tracking mode and progress."""
        try:
            current_time = datetime.now(timezone.utc)
            
            # Check if we're past end time
            if current_time >= self.end_time:
                return Decimal('0')
            
            # Calculate time elapsed and remaining
            time_elapsed = (current_time - self.start_time).total_seconds()
            total_duration = (self.end_time - self.start_time).total_seconds()
            time_remaining = total_duration - time_elapsed
            
            if time_remaining <= 0:
                return Decimal('0')
            
            # Calculate base quantity per iteration (5 second frequency)
            # This is the steady-state rate we need to maintain
            base_quantity_per_second = Decimal('1') / Decimal('720')  # Roughly 0.5 BERA per 5-second cycle at steady state
            
            # Calculate based on sell mode
            if self.sell_mode == SellMode.PERCENT_INVENTORY:
                # Ensure we have a sell_target_value
                if not self.sell_target_value:
                    # Fall back to hourly_percentage for backward compatibility
                    if self.hourly_percentage:
                        self.sell_target_value = Decimal(str(self.hourly_percentage))
                        self.logger.info(f"Using legacy hourly_percentage ({self.hourly_percentage}%) as total percentage to sell")
                    else:
                        self.logger.error("No sell_target_value or hourly_percentage configured")
                        return Decimal('0')
                
                # Use initial inventory for percentage calculation
                if not self.initial_inventory or self.initial_inventory <= 0:
                    # If no initial inventory but we have current inventory, use current inventory as base
                    current_inventory = self._get_current_inventory()
                    if current_inventory > 0:
                        self.logger.info(f"No initial inventory recorded, using current inventory ({current_inventory:.4f} {self.base_coin}) as base for percentage calculation")
                        # Update initial inventory for future calculations
                        self.initial_inventory = current_inventory
                    else:
                        self.logger.warning("No initial inventory and no current inventory for percentage-based selling")
                        return Decimal('0')
                
                # Calculate total target amount to sell based on initial inventory
                total_target = self.initial_inventory * (self.sell_target_value / Decimal('100'))
                
                self.logger.debug(f"Calculating quote size: initial_inventory={self.initial_inventory:.4f}, sell_target_value={self.sell_target_value}%, total_target={total_target:.4f}")
                
                # Calculate progress ratios
                time_progress = Decimal(str(time_elapsed)) / Decimal(str(total_duration))
                sell_progress = self.total_sold_since_start / total_target if total_target > 0 else Decimal('0')
                
                # If we've sold everything we aimed to sell, stop
                if self.total_sold_since_start >= total_target:
                    self.logger.debug(f"Target amount sold: {self.total_sold_since_start:.4f} >= {total_target:.4f}")
                    return Decimal('0')
                
                # If we're ahead of schedule, stop quoting
                if sell_progress >= time_progress:
                    self.logger.debug(
                        f"Ahead of schedule - pausing quotes "
                        f"(sold: {sell_progress:.1%}, time: {time_progress:.1%})"
                    )
                    return Decimal('0')
                
                # Calculate base quantity for this mode
                base_quantity = total_target * base_quantity_per_second * Decimal('5')  # 5 second cycles
                
                # Calculate size based on progress difference
                # The more behind we are, the larger the order
                progress_diff = time_progress - sell_progress
                quote_size = (progress_diff * total_target) + base_quantity
                
                self.logger.debug(
                    f"Percent inventory mode: {quote_size:.6f} {self.base_coin} "
                    f"(time: {time_progress:.1%}, sold: {sell_progress:.1%}, "
                    f"diff: {progress_diff:.1%}, base: {base_quantity:.6f})"
                )
                
            elif self.sell_mode == SellMode.HOURLY_RATE:
                # Fixed hourly rate mode
                if not self.sell_target_value or self.sell_target_value <= 0:
                    self.logger.warning("Invalid hourly rate target")
                    return Decimal('0')
                
                # Calculate total target based on duration
                total_hours = Decimal(str(total_duration)) / Decimal('3600')
                total_target = self.sell_target_value * total_hours
                
                # Handle base vs quote currency targets
                if self.target_currency == TargetCurrency.BASE:
                    # Target is in base currency
                    actual_sold = self.total_sold_since_start
                else:
                    # Target is in quote currency - need to convert
                    actual_sold = self.total_quote_value_from_sells
                    
                    # Use current price to estimate base amount needed
                    reference_price = self.inventory_price or await self._get_current_midpoint()
                    if not reference_price:
                        self.logger.warning("No reference price for quote->base conversion")
                        return Decimal('0')
                    
                    # Convert total target to base for calculation
                    total_target_base = total_target / reference_price
                    actual_sold = self.total_sold_since_start
                    total_target = total_target_base
                
                # Calculate progress ratios
                time_progress = Decimal(str(time_elapsed)) / Decimal(str(total_duration))
                sell_progress = actual_sold / total_target if total_target > 0 else Decimal('0')
                
                # If we're ahead of schedule, stop quoting
                if sell_progress >= time_progress:
                    self.logger.debug(
                        f"Ahead of schedule - pausing quotes "
                        f"(sold: {sell_progress:.1%}, time: {time_progress:.1%})"
                    )
                    return Decimal('0')
                
                # If we've sold everything, stop
                if actual_sold >= total_target:
                    return Decimal('0')
                
                # Calculate base quantity for hourly rate
                hourly_rate_per_second = self.sell_target_value / Decimal('3600')
                base_quantity = hourly_rate_per_second * Decimal('5')  # 5 second cycles
                
                # If quote currency, convert base quantity to base currency
                if self.target_currency == TargetCurrency.QUOTE:
                    reference_price = self.inventory_price or await self._get_current_midpoint()
                    if reference_price:
                        base_quantity = base_quantity / reference_price
                
                # Calculate size based on progress difference
                progress_diff = time_progress - sell_progress
                quote_size = (progress_diff * total_target) + base_quantity
                
                self.logger.debug(
                    f"Hourly rate mode: {quote_size:.6f} {self.base_coin} "
                    f"(time: {time_progress:.1%}, sold: {sell_progress:.1%}, "
                    f"diff: {progress_diff:.1%}, base: {base_quantity:.6f})"
                )
                
            else:
                # Fallback to old logic for backward compatibility
                return await self._calculate_quote_size_legacy()
            
            # Ensure we don't exceed available inventory, unless in a mode that doesn't require it
            if self.price_comparison_mode != PriceComparisonMode.TARGET_ONLY:
                current_inventory = self._get_current_inventory()
                if quote_size > current_inventory:
                    self.logger.debug(f"Quote size {quote_size} exceeds current inventory {current_inventory}. Adjusting.")
                    quote_size = current_inventory
            
            # Ensure minimum order size (adjust based on exchange requirements)
            min_order_size = Decimal('0.1')  # Minimum 0.1 BERA
            if quote_size < min_order_size and quote_size > 0:
                quote_size = min_order_size
                
            return max(Decimal('0'), quote_size)
                
        except Exception as e:
            self.logger.error(f"Error calculating quote size: {e}")
            return Decimal('0')
    
    async def _calculate_quote_size_legacy(self) -> Decimal:
        """Legacy quote size calculation for backward compatibility."""
        try:
            if self.tracking_mode == TrackingMode.PERCENTAGE:
                # Use initial inventory for percentage calculation
                if not self.initial_inventory or self.initial_inventory <= 0:
                    # If no initial inventory but we have current inventory, use current inventory as base
                    current_inventory = self._get_current_inventory()
                    if current_inventory > 0:
                        self.logger.info(f"No initial inventory recorded, using current inventory ({current_inventory:.4f} {self.base_coin}) as base for percentage calculation")
                        # Update initial inventory for future calculations
                        self.initial_inventory = current_inventory
                    else:
                        self.logger.warning("No initial inventory and no current inventory for percentage tracking")
                        return Decimal('0')
                
                # Calculate total target amount
                total_target = self.initial_inventory * (Decimal(str(self.hourly_percentage)) / Decimal('100'))
                remaining = total_target - self.total_sold_since_start
                
                if remaining <= 0:
                    self.logger.debug(f"Target already reached: {self.total_sold_since_start:.4f}/{total_target:.4f} {self.base_coin}")
                    return Decimal('0')
                
                # Calculate time remaining
                time_elapsed = (datetime.now(timezone.utc) - self.start_time).total_seconds()
                time_remaining = (self.total_time_hours * 3600) - time_elapsed
                
                if time_remaining <= 0:
                    self.logger.debug("Strategy time window expired")
                    return Decimal('0')
                
                # Divide remaining by number of quote cycles
                cycles_remaining = Decimal(str(time_remaining)) / Decimal('5')  # Assuming 5 second frequency
                size_per_cycle = remaining / cycles_remaining
                
                self.logger.debug(f"Percentage mode quote size: {size_per_cycle:.6f} {self.base_coin} "
                                f"(remaining: {remaining:.4f}, time: {time_remaining:.0f}s)")
                
                return max(Decimal('0'), size_per_cycle)
                
            else:  # TARGET mode
                if self.target_currency == TargetCurrency.BASE:
                    # Calculate based on fixed target in base currency
                    if self.target_amount is None or self.target_amount <= 0:
                        self.logger.warning("Invalid target_amount in BASE target mode for quote size calculation")
                        return Decimal('0')
                    remaining = self.target_amount - self.total_sold_since_start
                elif self.target_currency == TargetCurrency.QUOTE:
                    # Calculate based on fixed target in quote currency
                    if self.target_amount is None or self.target_amount <= 0:
                        self.logger.warning("Invalid target_amount in QUOTE target mode for quote size calculation")
                        return Decimal('0')
                    # Convert remaining quote value to base amount using current pricing
                    remaining_quote_value = self.target_amount - self.total_quote_value_from_sells
                    if remaining_quote_value <= 0:
                        return Decimal('0')
                    
                    # Use inventory price or current midpoint to estimate base amount needed
                    reference_price = self.inventory_price or await self._get_current_midpoint()
                    if not reference_price:
                        self.logger.warning("No reference price available for quote target calculation")
                        return Decimal('0')
                    
                    remaining = remaining_quote_value / reference_price
                    self.logger.debug(f"Quote target mode: ${remaining_quote_value:.2f} remaining = {remaining:.4f} {self.base_coin} @ {reference_price:.4f}")
                else:
                    self.logger.error(f"Unknown target currency: {self.target_currency}")
                    return Decimal('0')
                
                if remaining <= 0:
                    self.logger.debug(f"Target already reached in {self.target_currency.value} mode")
                    return Decimal('0')
                
                # Calculate time remaining
                time_elapsed = (datetime.now(timezone.utc) - self.start_time).total_seconds()
                time_remaining = (self.total_time_hours * 3600) - time_elapsed
                
                if time_remaining <= 0:
                    self.logger.debug("Strategy time window expired")
                    return Decimal('0')
                
                # Divide remaining by number of quote cycles
                cycles_remaining = Decimal(str(time_remaining)) / Decimal('5')  # Assuming 5 second frequency
                size_per_cycle = remaining / cycles_remaining
                
                self.logger.debug(f"Target mode quote size: {size_per_cycle:.6f} {self.base_coin} "
                                f"(remaining: {remaining:.4f}, time: {time_remaining:.0f}s)")
                
                return max(Decimal('0'), size_per_cycle)
                
        except Exception as e:
            self.logger.error(f"Error in legacy quote size calculation: {e}")
            return Decimal('0')
    
    async def _get_current_midpoint(self) -> Optional[Decimal]:
        """Get current midpoint price from Redis orderbook."""
        try:
            if hasattr(self.redis_orderbook_manager, 'get_aggregated_orderbook'):
                orderbook_data = self.redis_orderbook_manager.get_aggregated_orderbook(self.symbol)
                if orderbook_data and 'midpoint' in orderbook_data:
                    return Decimal(str(orderbook_data['midpoint']))
            
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
            
            if midpoints:
                return sum(midpoints) / Decimal(str(len(midpoints)))
                
            return None
            
        except Exception as e:
            self.logger.error(f"Error getting current midpoint: {e}")
            return None
    
    async def _get_best_bid(self) -> Optional[Decimal]:
        """Get best bid price from orderbook."""
        try:
            if hasattr(self.redis_orderbook_manager, 'get_aggregated_orderbook'):
                orderbook_data = self.redis_orderbook_manager.get_aggregated_orderbook(self.symbol)
                if orderbook_data and 'bids' in orderbook_data and orderbook_data['bids']:
                    return Decimal(str(orderbook_data['bids'][0][0]))
            
            # Fallback to individual exchange best bids
            best_bids = []
            for exchange in self.exchanges:
                try:
                    orderbook = await self._get_exchange_orderbook(exchange)
                    if orderbook and 'bids' in orderbook and orderbook['bids']:
                        best_bids.append(Decimal(str(orderbook['bids'][0][0])))
                except Exception as e:
                    self.logger.debug(f"Could not get best bid from {exchange}: {e}")
            
            if best_bids:
                return max(best_bids)  # Return highest bid
                
            return None
            
        except Exception as e:
            self.logger.error(f"Error getting best bid: {e}")
            return None
    
    async def _get_exchange_orderbook(self, exchange: str) -> Optional[Dict]:
        """Get orderbook for a specific exchange."""
        try:
            connector = self.exchange_connectors.get(exchange)
            if connector:
                return await connector.get_orderbook(self.symbol)
            return None
        except Exception as e:
            self.logger.error(f"Error getting orderbook from {exchange}: {e}")
            return None
    
    # Removed custom _cancel_order method - now uses base class method with instance verification
    
    async def _place_single_quote(self, exchange: str, size: Decimal, price: Decimal) -> Optional[str]:
        """Place a single sell quote on an exchange."""
        try:
            connector = self.exchange_connectors.get(exchange)
            if not connector:
                self.logger.warning(f"No connector for {exchange}")
                return None
            
            # Generate client order ID
            client_order_id = self._generate_client_order_id(exchange, int(time.time() * 1000))
            
            # Get exchange-specific parameters (like aggressive TWAP does)
            params = self._get_exchange_specific_params(exchange, OrderType.LIMIT.value, OrderSide.SELL.value)
            
            # Add client order ID to params (following base strategy pattern)
            if client_order_id and exchange.split('_')[0] != 'hyperliquid':
                params['clientOrderId'] = client_order_id
            
            # Get exchange-specific symbol
            exchange_symbol = connector.normalize_symbol(self.symbol)
            
            # Validate and round order size according to exchange requirements
            validated_quantity, validated_price = self._validate_order_size(
                exchange, self.symbol, float(size), float(price)
            )
            
            # Log order details
            notional = validated_quantity * validated_price
            self.logger.info(f"Placing validated sell order on {exchange}: {validated_quantity:.2f} {exchange_symbol} @ {validated_price:.4f} (notional: ${notional:.2f})")
            
            # Place sell order
            order_result = await connector.place_order(
                symbol=exchange_symbol,
                side=OrderSide.SELL.value,
                amount=Decimal(str(validated_quantity)),
                price=Decimal(str(validated_price)),
                order_type=OrderType.LIMIT.value,
                params=params
            )
            
            if order_result and 'id' in order_result:
                order_id = order_result['id']
                
                # Store order info
                self.active_orders[order_id] = {
                    'id': order_id,
                    'instance_id': self.instance_id,  # ✅ CRITICAL: Track which instance placed this order
                    'exchange': exchange,
                    'exchange_symbol': exchange_symbol,
                    'side': OrderSide.SELL.value,
                    'amount': validated_quantity,
                    'price': validated_price,
                    'status': 'open',
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'client_order_id': client_order_id,
                    'order_result': order_result
                }
                
                self.orders_placed += 1
                self.logger.info(f"✅ Placed sell quote on {exchange}: {validated_quantity:.2f} {self.base_coin} @ {validated_price:.4f} (Order ID: {order_id})")
                
                return order_id
            else:
                self.logger.error(f"Failed to place order on {exchange}: Invalid response")
                return None
                
        except Exception as e:
            self.logger.error(f"Error placing quote on {exchange}: {e}")
            return None
    
    def _generate_client_order_id(self, exchange: str, timestamp: int) -> str:
        """Generate sellbot-specific client order ID."""
        # Exchange-specific formatting
        if exchange == 'hyperliquid_perp' or exchange == 'hyperliquid':
            # Hyperliquid requires a 128-bit hex string (32 hex chars after 0x)
            # Include strategy identifier in the hex for later identification
            import hashlib
            import os
            
            # Create strategy prefix - "sellbot" padded to exactly 14 chars
            strategy_hex = "73656c6c626f7400"  # "sellbot" + padding to 14 chars
            
            # Generate unique part (18 hex chars to make total 32)
            unique_data = f"{timestamp}_{os.urandom(4).hex()}".encode()
            unique_hash = hashlib.md5(unique_data).hexdigest()[:18]  # Take first 18 chars
            
            # Combine: strategy identifier (14 chars) + unique part (18 chars) = 32 chars
            full_hex = strategy_hex + unique_hash
            
            # Return with 0x prefix as required by Hyperliquid
            return f"0x{full_hex}"
        
        # For other exchanges
        elif exchange == 'gateio_spot':
            return f"sellbot{timestamp}"[:28]
        elif exchange == 'mexc_spot':
            return f"sellbot-{timestamp}"[:32]
        else:
            return f"sellbot_{exchange[:3]}_{timestamp}"[:32]
    
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
            # So we just need to do sellbot-specific processing
            trade_amount = Decimal(str(trade_data.get('amount', 0)))
            trade_price = Decimal(str(trade_data.get('price', 0)))
            
            if trade_amount <= 0:
                return
            
            # Sellbot-specific processing
            self.orders_filled += 1
            self.logger.info(f"✅ Sellbot trade confirmed for instance {self.instance_id}: Sold {trade_amount} {self.base_coin} @ {trade_price} - position will be recalculated from database")
            
        except Exception as e:
            self.logger.error(f"Error in sellbot trade confirmation handler: {e}")
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get strategy performance statistics."""
        base_stats = super().get_performance_stats()
        
        # Calculate progress based on tracking mode and target currency
        if self.sell_mode == SellMode.PERCENT_INVENTORY:
            # Progress based on percentage of initial inventory
            if self.initial_inventory and self.initial_inventory > 0:
                total_target = self.initial_inventory * (self.sell_target_value / Decimal('100'))
                progress_percent = float(self.total_sold_since_start / total_target * Decimal('100')) if total_target > 0 else 0
            else:
                progress_percent = 0
        elif self.sell_mode == SellMode.HOURLY_RATE:
            # Progress based on hourly rate target
            time_elapsed = (datetime.now(timezone.utc) - self.start_time).total_seconds()
            total_duration = (self.end_time - self.start_time).total_seconds()
            total_hours = total_duration / 3600
            total_target = self.sell_target_value * Decimal(str(total_hours))
            
            if self.target_currency == TargetCurrency.BASE:
                progress_percent = float(self.total_sold_since_start / total_target * Decimal('100')) if total_target > 0 else 0
            else:
                progress_percent = float(self.total_quote_value_from_sells / total_target * Decimal('100')) if total_target > 0 else 0
        else:
            # Backward compatibility
            if self.tracking_mode == TrackingMode.PERCENTAGE:
                # Use initial inventory for percentage calculation
                if self.initial_inventory and self.initial_inventory > 0:
                    total_target = self.initial_inventory * (Decimal(str(self.hourly_percentage)) / Decimal('100'))
                    progress_percent = float(self.total_sold_since_start / total_target * Decimal('100')) if total_target > 0 else 0
                else:
                    progress_percent = 0
            else:  # TARGET mode
                if self.target_currency == TargetCurrency.BASE:
                    progress_percent = float(self.total_sold_since_start / self.target_amount * Decimal('100')) if self.target_amount > 0 else 0
                else:  # QUOTE currency
                    progress_percent = float(self.total_quote_value_from_sells / self.target_amount * Decimal('100')) if self.target_amount > 0 else 0
        
        sellbot_stats = {
            'orders_placed': self.orders_placed,
            'orders_filled': self.orders_filled,
            'total_sold_amount': float(self.total_sold_since_start),  # Real-time from database
            'total_sold_value': float(self.total_quote_value_from_sells),  # Real-time from database
            'total_sold_since_start': float(self.total_sold_since_start),
            'total_quote_value_from_sells': float(self.total_quote_value_from_sells),
            'current_net_base_position': float(self.current_net_base_position),
            'current_inventory': float(self._get_current_inventory()),
            'initial_inventory': float(self.initial_inventory) if self.initial_inventory else None,
            'progress_percent': progress_percent,
            'tracking_mode': self.tracking_mode.value,
            'target_currency': self.target_currency.value if hasattr(self, 'target_currency') else None,
            'target_amount': float(self.target_amount) if self.target_amount else None,
            'hourly_percentage': self.hourly_percentage,
            'inventory_price': float(self.inventory_price) if self.inventory_price else None,
            'current_interval_target_price': float(self.current_interval_target_price) if self.current_interval_target_price else None,
            'minimum_sell_price': float(self.minimum_sell_price) if self.minimum_sell_price else None,
            'price_comparison_mode': self.price_comparison_mode.value,
            'sell_mode': self.sell_mode.value,
            'sell_target_value': float(self.sell_target_value) if self.sell_target_value else None,
            'in_cooldown': self.in_cooldown,
            'hourly_sold_amounts': {k: float(v) for k, v in self.hourly_sold_amounts.items()},
            'last_inventory_calculation_time': self.last_inventory_calculation_time.isoformat() if self.last_inventory_calculation_time else None,
            'time_remaining': max(0, (self.end_time - datetime.now(timezone.utc)).total_seconds())
        }
        
        return {**base_stats, **sellbot_stats}
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert strategy to dictionary representation."""
        base_dict = super().to_dict()
        
        sellbot_dict = {
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'total_time_hours': self.total_time_hours,
            'spread_bps': self.spread_bps,
            'timeout_seconds': self.timeout_seconds,
            'drift_bps': self.drift_bps,
            'tracking_mode': self.tracking_mode.value,
            'hourly_percentage': self.hourly_percentage,
            'target_amount': float(self.target_amount) if self.target_amount else None,
            'target_currency': self.target_currency.value if self.target_currency else None,
            'taker_check': self.taker_check,
            'pricing_algorithm': self.pricing_algorithm.value,
            'minimum_sell_price': float(self.minimum_sell_price) if self.minimum_sell_price else None,
            'price_comparison_mode': self.price_comparison_mode.value,
            'sell_mode': self.sell_mode.value,
            'sell_target_value': float(self.sell_target_value) if self.sell_target_value else None,
            'performance': self.get_performance_stats()
        }
        
        return {**base_dict, **sellbot_dict} 
