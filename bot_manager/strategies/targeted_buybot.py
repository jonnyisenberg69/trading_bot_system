"""
Targeted Buybot Strategy - Maker-only buying strategy with dynamic pricing

This strategy aims to buy BASE coins as a maker at optimal prices, integrating
with aggressive TWAP for interval target prices and using various pricing algorithms.
"""

import asyncio
import json
import time
from datetime import datetime, timezone, timedelta
from decimal import Decimal
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
    LESSER_OF = "lesser_of"
    GREATER_OF = "greater_of"
    INVENTORY_MINUS_SPREAD = "inventory_minus_spread"
    INTERVAL_MINUS_SPREAD = "interval_minus_spread"


class PriceComparisonMode(str, Enum):
    """Price comparison mode for determining which price to use."""
    LESSER = "lesser"    # Use MIN(inventory, interval, target)
    GREATER = "greater"  # Use MAX(inventory, interval, target)
    INVENTORY_ONLY = "inventory_only"  # Use only inventory price
    INTERVAL_ONLY = "interval_only"    # Use only interval price
    TARGET_ONLY = "target_only"        # Use only target price


class BuyMode(str, Enum):
    """Buying mode options."""
    PERCENT_OF_TARGET = "percent_of_target"  # Buy a percentage of total target
    HOURLY_RATE = "hourly_rate"             # Buy at a fixed hourly rate


class TargetedBuybotStrategy(BaseStrategy):
    """
    Targeted Buybot strategy for buying BASE coins as a maker.
    
    Features:
    - Maker-only buying at optimal prices
    - Integration with aggressive TWAP for interval target prices
    - Custom client order ID tracking with "buybot_" prefix
    - Dynamic pricing algorithms with multiple options
    - Taker check to avoid crossing the spread
    - Percentage-based buying: Buy X% of total target over Y hours
    - Hourly rate buying: Buy fixed amount per hour
    - Maximum buy price configuration
    - Linear buying through time with catch-up mechanism
    
    Configuration:
    - For percentage mode: buy_target_value = percentage of TOTAL target to buy
      Example: buy_target_value=50 means buy 50% of target over total_time_hours
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
        self.end_time: Optional[datetime] = None
        self.total_time_hours: float = 0
        self.spread_bps: int = 0
        self.timeout_seconds: int = 0
        self.drift_bps: int = 0
        self.tracking_mode: TrackingMode = TrackingMode.PERCENTAGE # Kept for compatibility if needed
        self.target_amount: Optional[Decimal] = None
        self.target_currency: TargetCurrency = TargetCurrency.BASE
        self.taker_check: bool = True
        self.pricing_algorithm: PricingAlgorithm = PricingAlgorithm.LESSER_OF
        
        # New configuration parameters
        self.maximum_buy_price: Optional[Decimal] = None
        self.price_comparison_mode: PriceComparisonMode = PriceComparisonMode.LESSER
        self.buy_mode: BuyMode = BuyMode.PERCENT_OF_TARGET
        self.buy_target_value: Optional[Decimal] = None  # TOTAL % of target to buy over total_time_hours
        self.total_buy_target_amount: Optional[Decimal] = None # This will be the absolute target amount to acquire
        
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
        self.total_bought_since_start: Decimal = Decimal('0')
        self.hourly_bought_amounts: Dict[int, Decimal] = {}  # hour -> amount bought
        
        self.current_net_base_position: Decimal = Decimal('0')
        self.total_quote_value_for_buys: Decimal = Decimal('0')
        self.last_inventory_calculation_time: Optional[datetime] = None
        
        self.main_task: Optional[asyncio.Task] = None
        
        self.orders_placed = 0
        self.orders_filled = 0
        
        self.in_cooldown: bool = False
        self.cooldown_until: Optional[datetime] = None
        
    async def _validate_config(self) -> None:
        """Validate Targeted Buybot configuration."""
        required_fields = [
            'start_time', 'total_time_hours', 'spread_bps', 'timeout_seconds',
            'drift_bps', 'taker_check'
        ]
        
        for field in required_fields:
            if field not in self.config:
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

        if 'tracking_mode' in self.config:
            self.tracking_mode = TrackingMode(self.config['tracking_mode'])

        if 'buy_mode' in self.config:
            self.buy_mode = BuyMode(self.config['buy_mode'])
            if 'buy_target_value' not in self.config:
                raise ValueError("buy_target_value required")
            self.buy_target_value = Decimal(str(self.config['buy_target_value']))
            if self.buy_mode == BuyMode.PERCENT_OF_TARGET:
                 if 'total_buy_target_amount' not in self.config:
                    raise ValueError("total_buy_target_amount is required for percent_of_target mode")
                 self.total_buy_target_amount = Decimal(str(self.config['total_buy_target_amount']))
                 self.logger.info(f"Buy mode: {self.buy_target_value}% of TOTAL target {self.total_buy_target_amount} over {self.total_time_hours} hours")
            elif self.buy_mode == BuyMode.HOURLY_RATE:
                self.logger.info(f"Buy mode: {self.buy_target_value} {self.target_currency.value}/hour")
        else:
             raise ValueError("buy_mode is a required config field")

        if 'target_currency' in self.config:
            self.target_currency = TargetCurrency(self.config['target_currency'])
        
        if 'maximum_buy_price' in self.config and self.config['maximum_buy_price'] is not None:
            self.maximum_buy_price = Decimal(str(self.config['maximum_buy_price']))
            self.logger.info(f"Maximum buy price set to: {self.maximum_buy_price}")
        
        if 'price_comparison_mode' in self.config:
            self.price_comparison_mode = PriceComparisonMode(self.config['price_comparison_mode'])
        else:
            self.price_comparison_mode = PriceComparisonMode.LESSER

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
        
        self.logger.info(f"Validated Targeted Buybot config: {self.price_comparison_mode.value} pricing")
        self.logger.info(f"Strategy will run from {self.start_time} to {self.end_time} UTC")

    async def _start_strategy(self) -> None:
        """Start the Targeted Buybot strategy."""
        self.logger.info("Starting Targeted Buybot strategy")
        
        self.redis_client = redis.from_url(self.redis_url)
        self.redis_orderbook_manager = RedisOrderbookManager(self.redis_url)
        await self.redis_orderbook_manager.start()
        
        await self._initialize_execution_components()
        
        self.interval_subscription_task = asyncio.create_task(self._subscribe_to_interval_prices())
        asyncio.create_task(self._subscribe_to_cooldown_signals())
        self.main_task = asyncio.create_task(self._strategy_loop())
        
    async def _stop_strategy(self) -> None:
        """Stop the Targeted Buybot strategy."""
        self.logger.info("Stopping Targeted Buybot strategy")
        
        if self.main_task:
            self.main_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.main_task
        
        if self.interval_subscription_task:
            self.interval_subscription_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.interval_subscription_task

        if self.execution_engine:
            await self.execution_engine.stop()
        
        if self.redis_orderbook_manager:
            await self.redis_orderbook_manager.stop()
        if self.redis_pubsub:
            await self.redis_pubsub.unsubscribe()
            await self.redis_pubsub.close()
        if self.redis_client:
            await self.redis_client.close()
    
    async def _initialize_execution_components(self) -> None:
        """Initialize execution engine and order manager."""
        self.position_manager = PositionManager()
        await self.position_manager.start()
        
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
        
        self.order_manager = OrderManager(
            exchange_connectors=self.exchange_connectors,
            position_manager=self.position_manager
        )
        
        self.execution_engine = ExecutionEngine(
            order_manager=self.order_manager,
            exchange_connectors=self.exchange_connectors
        )
        
        await self.execution_engine.start()
        
    async def _calculate_current_position(self) -> Decimal:
        """Calculate current position including ALL buy trades since start time."""
        try:
            from database import get_session
            from database.repositories import TradeRepository
            from database.models import Trade, Exchange
            
            total_bought = Decimal('0')
            self.total_quote_value_for_buys = Decimal('0')
            
            async for session in get_session():
                try:
                    await session.begin()
                    # Get the start time for inventory calculations
                    start_time_for_db = self.get_inventory_start_time()
                    # Convert timezone-aware datetime to timezone-naive for database query
                    if start_time_for_db.tzinfo is not None:
                        start_time_for_db = start_time_for_db.astimezone(timezone.utc).replace(tzinfo=None)
                    
                    # Get ALL exchange IDs from database, not just configured ones
                    stmt = select(Exchange.id, Exchange.name)
                    result = await session.execute(stmt)
                    all_exchanges = result.all()
                    
                    exchange_ids = [ex.id for ex in all_exchanges]
                    
                    if not exchange_ids:
                        self.logger.warning("No exchanges found in database")
                        return Decimal('0')
                    
                    self.logger.info(f"🔍 Calculating total bought since start time: {start_time_for_db} from {len(exchange_ids)} exchanges")
                    
                    stmt = select(Trade).options(joinedload(Trade.exchange)).where(
                        and_(
                            Trade.exchange_id.in_(exchange_ids),
                            Trade.symbol == self.symbol,
                            Trade.timestamp >= start_time_for_db,
                            Trade.side.ilike('buy')
                        )
                    )
                    
                    result = await session.execute(stmt)
                    trades = result.scalars().unique().all()
                    
                    for trade in trades:
                        amount = Decimal(str(trade.amount))
                        price = Decimal(str(trade.price))
                        total_bought += amount
                        self.total_quote_value_for_buys += amount * price
                    
                    self.total_bought_since_start = total_bought
                    await self._update_hourly_bought_amounts_optimized(trades)
                    await session.commit()
                    break
                except Exception as e:
                    await session.rollback()
                    self.logger.error(f"Error in position calculation: {e}")
                    raise
                finally:
                    await session.close()
            return total_bought
        except Exception as e:
            self.logger.error(f"Error calculating current position: {e}")
            return Decimal('0')
    
    async def _update_hourly_bought_amounts_optimized(self, trades: List[Any]) -> None:
        """Update hourly bought amounts from provided trades list."""
        self.hourly_bought_amounts.clear()
        inventory_start_time = self.get_inventory_start_time()
        for trade in trades:
            trade_time = trade.timestamp.replace(tzinfo=timezone.utc)
            hours_since_start = int((trade_time - inventory_start_time).total_seconds() / 3600)
            self.hourly_bought_amounts.setdefault(hours_since_start, Decimal('0'))
            self.hourly_bought_amounts[hours_since_start] += Decimal(str(trade.amount))

    async def _calculate_inventory_price(self) -> None:
        """Calculate inventory price from trades since start time only."""
        try:
            from database import get_session
            from database.repositories import TradeRepository
            from database.models import Trade, Exchange
            
            p1_total = Decimal('0')
            p2_total = Decimal('0')
            p1_fees_total = Decimal('0')
            p2_fees_total = Decimal('0')
            
            async for session in get_session():
                try:
                    await session.begin()
                    # Get the start time for inventory calculations
                    start_time_for_db = self.get_inventory_start_time()
                    # Convert timezone-aware datetime to timezone-naive for database query
                    if start_time_for_db.tzinfo is not None:
                        start_time_for_db = start_time_for_db.astimezone(timezone.utc).replace(tzinfo=None)
                    
                    # Get ALL exchange IDs from database, not just configured ones
                    stmt = select(Exchange.id, Exchange.name)
                    result = await session.execute(stmt)
                    all_exchanges = result.all()
                    
                    exchange_ids = [ex.id for ex in all_exchanges]
                    
                    if not exchange_ids:
                        self.logger.warning("No exchanges found in database")
                        return

                    stmt = select(Trade).options(joinedload(Trade.exchange)).where(
                        and_(
                            Trade.exchange_id.in_(exchange_ids),
                            Trade.symbol == self.symbol,
                            Trade.timestamp >= start_time_for_db
                        )
                    )
                    result = await session.execute(stmt)
                    trades = result.scalars().unique().all()
                    
                    for trade in trades:
                        amount = Decimal(str(trade.amount))
                        price = Decimal(str(trade.price))
                        fee_cost = Decimal(str(trade.fee_cost or '0'))
                        fee_currency = trade.fee_currency
                        
                        if trade.side.lower() == 'buy':
                            p1_total += amount
                            p2_total -= (amount * price)
                            if fee_currency and fee_currency.upper() == self.base_coin.upper():
                                p1_fees_total += fee_cost
                            else:
                                p2_fees_total += fee_cost
                        else:  # sell
                            p1_total -= amount
                            p2_total += (amount * price)
                            if fee_currency and fee_currency.upper() == self.base_coin.upper():
                                p1_fees_total += fee_cost
                            else:
                                p2_fees_total += fee_cost
                    
                    net_base_position = p1_total - p1_fees_total
                    self.current_net_base_position = net_base_position
                    self.last_inventory_calculation_time = datetime.now(timezone.utc)
                    
                    if net_base_position < 0: # Interested in short positions
                        net_quote_cost = -(p2_total + p2_fees_total)
                        self.inventory_price = net_quote_cost / abs(net_base_position)
                    else:
                        self.inventory_price = None

                    await session.commit()
                    break
                except Exception as e:
                    await session.rollback()
                    raise
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
            
            while self.running:
                try:
                    message = await self.redis_pubsub.get_message(ignore_subscribe_messages=True, timeout=0.1)
                    if message and message['type'] == 'message':
                        data = json.loads(message['data'])
                        self.current_interval_target_price = Decimal(str(data['target_price']))
                        self.last_interval_update = datetime.now(timezone.utc)
                except asyncio.TimeoutError:
                    continue
                await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            self.logger.info("Interval price subscription cancelled")
        except Exception as e:
            self.logger.error(f"Error in interval price subscription: {e}")
    
    async def _subscribe_to_cooldown_signals(self) -> None:
        """Subscribe to cooldown signals from aggressive TWAP."""
        cooldown_key = f"strategy_cooldown:{self.symbol}"
        while self.running:
            cooldown_data = await self.redis_client.get(cooldown_key)
            if cooldown_data:
                data = json.loads(cooldown_data)
                cooldown_until = datetime.fromisoformat(data['cooldown_until'])
                if cooldown_until > datetime.now(timezone.utc):
                    self.in_cooldown = True
                    self.cooldown_until = cooldown_until
                else:
                    self.in_cooldown = False
            else:
                self.in_cooldown = False
            await asyncio.sleep(1)
        
    async def _strategy_loop(self) -> None:
        """Main strategy loop."""
        self.logger.info("Starting Targeted Buybot main loop")
        while self.running:
            try:
                # Recalculate position and inventory price fresh from database
                await self._calculate_current_position()
                await self._calculate_inventory_price()

                current_time = datetime.now(timezone.utc)
                if current_time >= self.end_time:
                    await self.stop()
                    break

                if self.in_cooldown and self.cooldown_until and current_time < self.cooldown_until:
                    await asyncio.sleep(1)
                    continue
                else:
                    self.in_cooldown = False
                
                if self._is_target_reached():
                    self.logger.info("Target reached, pausing quotes.")
                    await asyncio.sleep(5)
                    continue

                await self._cancel_stale_orders()
                await self._place_quotes()

                # Log performance stats every loop
                stats = self.get_performance_stats()
                self.logger.info("Buybot Performance Stats", **stats)

                await asyncio.sleep(5)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in strategy loop: {e}")
                await asyncio.sleep(5)
    
    def _is_target_reached(self) -> bool:
        """Check if the buying target has been reached."""
        if self.buy_mode == BuyMode.PERCENT_OF_TARGET:
            if not self.total_buy_target_amount or self.total_buy_target_amount <= 0:
                return True # No target set
            
            target_to_buy = self.total_buy_target_amount * (self.buy_target_value / Decimal('100'))
            return self.total_bought_since_start >= target_to_buy
        
        elif self.buy_mode == BuyMode.HOURLY_RATE:
            # This logic assumes target is in base currency for now
            total_hours = self.total_time_hours
            total_target = self.buy_target_value * Decimal(total_hours)
            if self.target_currency == TargetCurrency.QUOTE:
                return self.total_quote_value_for_buys >= total_target
            else:
                return self.total_bought_since_start >= total_target
        return False

    def _get_current_inventory_to_buy(self) -> Decimal:
        """Get current short position to determine how much we can still buy."""
        # We want to close a short position, so we look for negative net base position.
        # If position is positive, we don't need to buy.
        return max(Decimal('0'), -self.current_net_base_position)

    async def _cancel_stale_orders(self) -> None:
        """Cancel orders that have timed out or drifted too far."""
        current_midpoint = await self._get_current_midpoint()
        if not current_midpoint:
            return
        
        for order_id, order_info in list(self.active_orders.items()):
            # Skip orders that don't belong to this instance
            if not self._order_belongs_to_instance(order_id):
                continue
            
            order_age = (datetime.now(timezone.utc) - datetime.fromisoformat(order_info['timestamp'])).total_seconds()
            order_price = Decimal(str(order_info['price']))
            price_drift_bps = abs((order_price - current_midpoint) / current_midpoint) * Decimal('10000')
            exchange = order_info.get('exchange')
            
            if not exchange:
                self.logger.warning(f"Order {order_id} missing exchange info, skipping")
                continue

            if order_age > self.timeout_seconds or price_drift_bps > self.drift_bps:
                self.logger.info(f"Cancelling order {order_id} due to timeout or drift")
                await self._cancel_order(order_id, exchange)

    async def _place_quotes(self) -> None:
        """Place buy quotes based on pricing algorithm."""
        base_price = await self._calculate_quote_price()
        if not base_price:
            return
        
        quote_size = await self._calculate_quote_size()
        if quote_size <= 0:
            return
        
        for exchange in self.exchanges:
            try:
                orderbook = await self._get_exchange_orderbook(exchange)
                if not orderbook or not orderbook.get('asks'):
                    continue

                best_ask = Decimal(str(orderbook['asks'][0][0]))
                best_bid = Decimal(str(orderbook['bids'][0][0])) if orderbook.get('bids') else None

                # Start with the calculated base price
                quote_price = base_price
                
                # If the base price is below the best ask, we can potentially place a maker order
                if base_price < best_ask:
                    # Place at best bid to be a maker
                    if best_bid:
                        quote_price = best_bid
                    # If no best bid, we can still use our base_price if it's not aggressive
                
                # Taker check - if our price is now at or above the best ask, it would be a taker order
                if self.taker_check and quote_price >= best_ask:
                    # Adjust to be just below the best ask to ensure it's a maker order
                    quote_price = best_ask - (best_ask * Decimal('0.0001')) # 1 bps below

                # FINAL CHECK: Never place an order above the configured maximum price
                if self.maximum_buy_price and quote_price > self.maximum_buy_price:
                    self.logger.warning(f"Quote price {quote_price} exceeds maximum buy price {self.maximum_buy_price}. Skipping order placement.")
                    continue

                await self._place_single_quote(exchange, quote_size, quote_price)
            except Exception as e:
                self.logger.error(f"Error placing quote on {exchange}: {e}", exc_info=True)

    async def _calculate_quote_price(self) -> Optional[Decimal]:
        """Calculate quote price based on selected pricing algorithm."""
        inventory_price = self.inventory_price
        interval_target_price = self.current_interval_target_price
        target_price = self.maximum_buy_price
        spread = Decimal(str(self.spread_bps)) / Decimal('10000')
        
        prices = []
        if self.price_comparison_mode == PriceComparisonMode.INVENTORY_ONLY and inventory_price:
            return inventory_price * (Decimal('1') - spread)
        if self.price_comparison_mode == PriceComparisonMode.INTERVAL_ONLY and interval_target_price:
            return interval_target_price * (Decimal('1') - spread)
        if self.price_comparison_mode == PriceComparisonMode.TARGET_ONLY and target_price:
            return target_price

        if inventory_price: prices.append(inventory_price * (Decimal('1') - spread))
        if interval_target_price: prices.append(interval_target_price * (Decimal('1') - spread))
        if target_price: prices.append(target_price)
            
        if not prices:
            return await self._fallback_to_midpoint_price(spread)
        
        if self.price_comparison_mode == PriceComparisonMode.LESSER:
            return min(prices)
        elif self.price_comparison_mode == PriceComparisonMode.GREATER:
            return max(prices)
        
        return await self._fallback_to_midpoint_price(spread)

    async def _fallback_to_midpoint_price(self, spread: Decimal) -> Optional[Decimal]:
        """Fallback to market midpoint - spread."""
        midpoint = await self._get_current_midpoint()
        return midpoint * (Decimal('1') - spread) if midpoint else None

    async def _calculate_quote_size(self) -> Decimal:
        """Calculate size to quote based on progress."""
        current_time = datetime.now(timezone.utc)
        if current_time >= self.end_time: return Decimal('0')

        time_elapsed = (current_time - self.start_time).total_seconds()
        total_duration = (self.end_time - self.start_time).total_seconds()
        if time_elapsed >= total_duration: return Decimal('0')
        
        time_progress = Decimal(time_elapsed) / Decimal(total_duration)

        if self.buy_mode == BuyMode.PERCENT_OF_TARGET:
            if not self.total_buy_target_amount or self.total_buy_target_amount <= 0:
                return Decimal('0')
            total_target = self.total_buy_target_amount * (self.buy_target_value / Decimal('100'))
        elif self.buy_mode == BuyMode.HOURLY_RATE:
            total_target = self.buy_target_value * Decimal(self.total_time_hours)
        else:
            return Decimal('0')

        buy_progress = self.total_bought_since_start / total_target if total_target > 0 else Decimal('1')

        if buy_progress >= time_progress:
            return Decimal('0') # Ahead of schedule

        remaining_to_buy = total_target - self.total_bought_since_start
        if remaining_to_buy <= 0: return Decimal('0')
        
        progress_diff = time_progress - buy_progress
        quote_size = (progress_diff * total_target)

        inventory_to_buy = self._get_current_inventory_to_buy()
        
        # We can buy up to our target, or what's needed to close short.
        # But for this strategy, we are acquiring a position, not closing one, so we just check against the target.
        final_size = min(quote_size, remaining_to_buy)

        # Ensure minimum order size
        min_order_size = Decimal('0.1')
        if final_size > 0 and final_size < min_order_size:
            final_size = min_order_size
            
        return max(Decimal('0'), final_size)
        
    async def _get_current_midpoint(self) -> Optional[Decimal]:
        """Get current midpoint price from Redis orderbook."""
        orderbook_data = self.redis_orderbook_manager.get_aggregated_orderbook(self.symbol)
        return Decimal(str(orderbook_data['midpoint'])) if orderbook_data and 'midpoint' in orderbook_data else None

    async def _get_exchange_orderbook(self, exchange: str) -> Optional[Dict]:
        """Get orderbook for a specific exchange."""
        connector = self.exchange_connectors.get(exchange)
        return await connector.get_orderbook(self.symbol) if connector else None

    # Removed custom _cancel_order method - now uses base class method with instance verification

    async def _place_single_quote(self, exchange: str, size: Decimal, price: Decimal) -> Optional[str]:
        """Place a single buy quote on an exchange."""
        try:
            connector = self.exchange_connectors.get(exchange)
            if not connector:
                self.logger.warning(f"No connector for {exchange}")
                return None
            
            client_order_id = self._generate_client_order_id(exchange, int(time.time() * 1000))
            params = self._get_exchange_specific_params(exchange, OrderType.LIMIT.value, OrderSide.BUY.value)
            if client_order_id and 'hyperliquid' not in exchange:
                params['clientOrderId'] = client_order_id
                
            exchange_symbol = connector.normalize_symbol(self.symbol)
            validated_quantity, validated_price = self._validate_order_size(exchange, self.symbol, float(size), float(price))

            self.logger.info(f"Placing buy order on {exchange}: {validated_quantity:.4f} {exchange_symbol} @ {validated_price:.4f}")

            order_result = await connector.place_order(
                symbol=exchange_symbol,
                side=OrderSide.BUY.value,
                amount=Decimal(str(validated_quantity)),
                price=Decimal(str(validated_price)),
                order_type=OrderType.LIMIT.value,
                params=params
            )
            
            if order_result and 'id' in order_result:
                order_id = order_result['id']
                self.active_orders[order_id] = {
                    'id': order_id,
                    'instance_id': self.instance_id,
                    'exchange': exchange, 
                    'price': validated_price,
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'client_order_id': client_order_id
                }
                self.orders_placed += 1
                self.logger.info(f"Successfully placed buy order {order_id} on {exchange} (instance: {self.instance_id})")
                return order_id
            else:
                self.logger.error(f"Failed to place order on {exchange}, response: {order_result}")
                return None
        except Exception as e:
            self.logger.error(f"Error placing quote on {exchange}: {e}", exc_info=True)
            return None

    def _generate_client_order_id(self, exchange: str, timestamp: int) -> str:
        """Generate buybot-specific client order ID."""
        prefix = "buybot"
        if exchange.startswith('hyperliquid'):
            strategy_hex = prefix.encode().hex().ljust(14, '0')
            unique_hash = uuid.uuid4().hex[:18]
            return f"0x{strategy_hex}{unique_hash}"
        return f"{prefix}_{exchange[:3]}_{timestamp}"[:32]

    async def on_trade_confirmation(self, trade_data: Dict[str, Any]) -> None:
        """
        Handle confirmed trades from the connection pool.
        
        Includes instance ownership verification to prevent cross-instance contamination.
        """
        try:
            # CRITICAL: First call base class method for instance ownership verification
            await super().on_trade_confirmation(trade_data)
            
            # If we get here, the trade belongs to this instance and was processed by base class
            order_id = trade_data.get('order_id') or trade_data.get('id')
            if not order_id:
                return
            
            # The base class already removed the order from active_orders if it belonged to this instance
            # So we just need to do buybot-specific processing
            
            # Look for client_order_id for additional validation
            client_order_id = trade_data.get('client_order_id')

            # The raw message might contain the clientOid
            if not client_order_id and 'raw' in trade_data and isinstance(trade_data['raw'], dict):
                raw_data = trade_data['raw'].get('data', [])
                if isinstance(raw_data, list) and raw_data:
                    client_order_id = raw_data[0].get('clientOid') or raw_data[0].get('cOid')
                    order_id = order_id or raw_data[0].get('orderId') or raw_data[0].get('ordId')

            # Buybot-specific processing
            self.orders_filled += 1
            self.logger.info(f"✅ Buybot trade confirmed for instance {self.instance_id}: order {order_id}")

        except Exception as e:
            self.logger.error(f"Error in buybot trade confirmation handler: {e}", exc_info=True)
            self.logger.debug(f"Problematic trade_data: {trade_data}")

    def get_performance_stats(self) -> Dict[str, Any]:
        """Get strategy performance statistics."""
        base_stats = super().get_performance_stats()
        
        total_target = Decimal('0')
        if self.buy_mode == BuyMode.PERCENT_OF_TARGET and self.total_buy_target_amount and self.buy_target_value:
             total_target = self.total_buy_target_amount * (self.buy_target_value / Decimal('100'))
        elif self.buy_mode == BuyMode.HOURLY_RATE and self.buy_target_value:
            total_target = self.buy_target_value * Decimal(self.total_time_hours)
        
        progress_percent = float(self.total_bought_since_start / total_target * 100) if total_target > 0 else 0

        buybot_stats = {
            'orders_placed': self.orders_placed,
            'orders_filled': self.orders_filled,
            'total_bought_amount': float(self.total_bought_since_start),
            'total_quote_value_for_buys': float(self.total_quote_value_for_buys),
            'current_net_base_position': float(self.current_net_base_position),
            'inventory_price': float(self.inventory_price) if self.inventory_price else None,
            'maximum_buy_price': float(self.maximum_buy_price) if self.maximum_buy_price else None,
            'price_comparison_mode': self.price_comparison_mode.value,
            'buy_mode': self.buy_mode.value,
            'buy_target_value': float(self.buy_target_value) if self.buy_target_value else None,
            'total_buy_target_amount': float(self.total_buy_target_amount) if self.total_buy_target_amount else None,
            'progress_percent': progress_percent,
        }
        return {**base_stats, **buybot_stats}

    def to_dict(self) -> Dict[str, Any]:
        """Convert strategy to dictionary representation."""
        base_dict = super().to_dict()
        buybot_dict = {
            'performance': self.get_performance_stats(),
            # Include other relevant state for representation
        }
        return {**base_dict, **buybot_dict}
