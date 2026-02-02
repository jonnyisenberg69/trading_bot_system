"""
Passive Quoting Strategy - Redis-based implementation
"""
import asyncio
import random
import time
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Optional, Any
import logging
import redis.asyncio as redis
import json

from bot_manager.strategies.base_strategy import BaseStrategy
from market_data.redis_orderbook_manager import RedisOrderbookManager


class QuoteSide(str, Enum):
    """Quote side options."""
    BOTH = "both"
    BID = "bid"
    OFFER = "offer"


class QuantityCurrency(str, Enum):
    """Quantity currency options."""
    BASE = "base"
    QUOTE = "quote"


@dataclass
class QuoteLine:
    """Represents a single quote line configuration."""
    line_id: int
    timeout_seconds: int
    drift_bps: float
    quantity: float
    quantity_randomization_factor: float
    spread_bps: float
    sides: QuoteSide
    
    # Runtime state - track orders per exchange
    bid_orders: Dict[str, str] = field(default_factory=dict)  # exchange -> order_id
    ask_orders: Dict[str, str] = field(default_factory=dict)  # exchange -> order_id
    last_midpoint_per_exchange: Dict[str, Decimal] = field(default_factory=dict)  # exchange -> last_midpoint
    bid_placed_at: Dict[str, datetime] = field(default_factory=dict)  # exchange -> datetime
    ask_placed_at: Dict[str, datetime] = field(default_factory=dict)  # exchange -> datetime
    bid_placed_price: Optional[Decimal] = None
    ask_placed_price: Optional[Decimal] = None
    
    def get_active_bid_count(self) -> int:
        """Get count of active bid orders."""
        return len(self.bid_orders)
    
    def get_active_ask_count(self) -> int:
        """Get count of active ask orders."""
        return len(self.ask_orders)
    
    def has_bid_on_exchange(self, exchange: str) -> bool:
        """Check if there's an active bid order on this exchange."""
        return exchange in self.bid_orders
    
    def has_ask_on_exchange(self, exchange: str) -> bool:
        """Check if there's an active ask order on this exchange."""
        return exchange in self.ask_orders


class PassiveQuotingStrategy(BaseStrategy):
    """
    Passive quoting strategy using Redis for market data.
    
    Features:
    - Places passive orders at specified spreads
    - Cancels orders on timeout or drift
    - Uses Redis pub/sub for real-time market data
    - Supports multiple quote lines with different parameters
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
        self.quote_lines: List[QuoteLine] = []
        self.quantity_currency: QuantityCurrency = QuantityCurrency.BASE
        self.main_task: Optional[asyncio.Task] = None
        
        # Redis orderbook manager
        self.redis_orderbook_manager = RedisOrderbookManager(redis_url)
        
        # Redis client for strategy coordination
        self.redis_client: Optional[redis.Redis] = None
        
        # Performance tracking
        self.orders_placed = 0
        self.orders_cancelled_timeout = 0
        self.orders_cancelled_drift = 0
        
    async def _validate_config(self) -> None:
        """Validate passive quoting configuration."""
        # Defensive: flatten config if double-nested
        if 'config' in self.config and isinstance(self.config['config'], dict):
            self.logger.info("Detected double-nested config, flattening...")
            self.config = self.config['config']
        
        required_fields = [
            'base_coin', 'lines', 'quantity_currency'
        ]
        
        for field in required_fields:
            if field not in self.config:
                raise ValueError(f"Missing required config field: {field}")
        
        # Validate quantity currency
        qty_currency = self.config.get('quantity_currency', 'base').lower()
        if qty_currency not in ['base', 'quote']:
            raise ValueError(f"Invalid quantity_currency: {qty_currency}")
        self.quantity_currency = QuantityCurrency(qty_currency)
        
        # Validate lines configuration
        lines_config = self.config.get('lines', [])
        if not lines_config or not isinstance(lines_config, list):
            raise ValueError("Lines configuration must be a non-empty list")
        
        # Parse and validate each line
        self.quote_lines = []
        for i, line_config in enumerate(lines_config):
            required_line_fields = [
                'timeout', 'drift', 'quantity', 'quantity_randomization_factor',
                'spread', 'sides'
            ]
            
            for field in required_line_fields:
                if field not in line_config:
                    raise ValueError(f"Line {i}: Missing required field '{field}'")
            
            # Validate sides
            sides = line_config['sides'].lower()
            if sides not in ['both', 'bid', 'offer']:
                raise ValueError(f"Line {i}: Invalid sides '{sides}'")
            
            quote_line = QuoteLine(
                line_id=i,
                timeout_seconds=int(line_config['timeout']),
                drift_bps=float(line_config['drift']),
                quantity=float(line_config['quantity']),
                quantity_randomization_factor=float(line_config['quantity_randomization_factor']),
                spread_bps=float(line_config['spread']),
                sides=QuoteSide(sides)
            )
            
            self.quote_lines.append(quote_line)
        
        self.logger.info(f"Validated config with {len(self.quote_lines)} quote lines")
        
    async def _start_websocket_monitoring(self) -> None:
        """Override base class WebSocket monitoring - we use Redis instead."""
        self.logger.info("Skipping WebSocket monitoring - using Redis for market data")
        self._websocket_subscribed = False
        
    async def _stop_websocket_monitoring(self) -> None:
        """Override base class WebSocket monitoring - we use Redis instead."""
        self.logger.info("No WebSocket monitoring to stop - using Redis for market data")
        
    async def _start_strategy(self) -> None:
        """Start the passive quoting strategy."""
        self.logger.info("Starting passive quoting strategy using Redis")
        
        # Initialize Redis client for strategy coordination
        self.redis_client = redis.from_url(self.redis_orderbook_manager.redis_url)
        
        # Start Redis orderbook manager
        await self.redis_orderbook_manager.start()
        
        # Wait a moment for initial orderbook data
        await asyncio.sleep(2)
        
        # Log symbol mappings for debugging
        for exchange in self.exchanges:
            resolved_symbol = self._resolve_symbol_for_exchange(exchange)
            if resolved_symbol != self.symbol:
                self.logger.info(f"Symbol mapping for {exchange}: {self.symbol} -> {resolved_symbol}")
        
        # Start main strategy loop
        self.main_task = asyncio.create_task(self._strategy_loop())
        
    async def _stop_strategy(self) -> None:
        """Stop the passive quoting strategy."""
        self.logger.info("Stopping passive quoting strategy")
        
        if self.main_task:
            self.main_task.cancel()
            try:
                await self.main_task
            except asyncio.CancelledError:
                pass
        
        # Cancel all active orders
        for line in self.quote_lines:
            if line.bid_orders:
                await self._cancel_line_order(line, 'bid', 'strategy_stop')
            if line.ask_orders:
                await self._cancel_line_order(line, 'ask', 'strategy_stop')
        
        # Stop Redis components
        await self.redis_orderbook_manager.stop()
        if self.redis_client:
            await self.redis_client.close()
    
    def _resolve_symbol_for_exchange(self, exchange: str) -> str:
        """Resolve and cache the actual symbol key published for a given exchange in Redis."""
        if not hasattr(self, "_cached_exchange_symbols"):
            self._cached_exchange_symbols = {}
        if exchange in self._cached_exchange_symbols and self._cached_exchange_symbols[exchange]:
            return self._cached_exchange_symbols[exchange]
        
        candidates = []
        base_quote = self.symbol.split('/') if '/' in self.symbol else [self.symbol, 'USDT0']
        base = base_quote[0]
        quote = base_quote[1] if len(base_quote) > 1 else 'USDT0'
        candidates.append(self.symbol)
        
        # Add various quote currency variants
        quote_variants = {quote}
        quote_variants.update({quote.upper(), quote.capitalize()})
        quote_variants.update({'USD', 'USDT', 'USDC', 'USDT0'})
        base_variants = {base, base.upper(), base.capitalize()}
        if base.upper() == 'XAUT':
            base_variants.update({'XAUT', 'XAUt', 'XAUT0'})
        
        for b in base_variants:
            for q in quote_variants:
                candidates.append(f"{b}/{q}")
        
        # Hyperliquid-specific handling
        if exchange.startswith('hyperliquid'):
            for b in base_variants:
                if 'spot' in exchange:
                    # Spot uses USDT0 without settlement suffix
                    candidates.append(f"{b}/USDT0")
                    # Special case for XAUT on Hyperliquid spot
                    if b.upper() == 'XAUT':
                        candidates.append('XAUT0/USDT0')
                else:
                    # Perp uses USDC:USDC
                    candidates.append(f"{b}/USDT:USDT")
        
        # Bitfinex-specific handling
        elif exchange.startswith('bitfinex'):
            base_variants = {base, base.upper(), base.capitalize()}
            if base.upper() == 'XAUT':
                base_variants.update({'XAUT', 'XAUt'})
            quote_variants = {quote, quote.upper(), quote.capitalize()}
            quote_variants.update({'USD', 'USDT', 'USDT0'})
            for b in base_variants:
                for q in quote_variants:
                    candidates.append(f"{b}/{q}")
        
        # Check which candidate is available in Redis
        available = set()
        if hasattr(self.redis_orderbook_manager, 'orderbooks') and exchange in self.redis_orderbook_manager.orderbooks:
            available = set(self.redis_orderbook_manager.orderbooks[exchange].keys())
        
        for cand in candidates:
            if cand in available:
                self._cached_exchange_symbols[exchange] = cand
                return cand
        
        # If no match found, log warning and return original symbol
        self.logger.debug(f"No symbol match found for {exchange}. Candidates: {candidates}, Available: {available}")
        return self.symbol

    async def _strategy_loop(self) -> None:
        """Main strategy loop."""
        self.logger.info("Starting passive quoting strategy loop with 100ms cycle time")
        
        loop_count = 0
        last_performance_log = time.time()
        
        while True:
            try:
                loop_start = time.time()
                
                # Check if we should pause due to aggressive TWAP cooldown
                if await self._should_pause_for_cooldown():
                    await asyncio.sleep(0.1)  # Even cooldown checks should be fast
                    continue
                
                # Process all quote lines concurrently
                if self.quote_lines:
                    line_tasks = [self._process_quote_line(line) for line in self.quote_lines]
                    await asyncio.gather(*line_tasks, return_exceptions=True)
                
                loop_end = time.time()
                loop_duration = (loop_end - loop_start) * 1000  # Convert to milliseconds
                
                # Log performance every 100 loops (every ~10 seconds)
                loop_count += 1
                if loop_count % 100 == 0:
                    avg_cycle_time = (time.time() - last_performance_log) * 1000 / 100
                    self.logger.info(f"⚡ Performance: Last loop {loop_duration:.1f}ms, Avg cycle {avg_cycle_time:.1f}ms over 100 loops")
                    last_performance_log = time.time()
                
                # Warn if loop takes too long
                if loop_duration > 50:  # Warn if over 50ms
                    self.logger.warning(f"⚠️ Slow loop detected: {loop_duration:.1f}ms (target: <50ms)")
                
                # Sleep between iterations - use milliseconds for market making
                await asyncio.sleep(0.1)  # 100ms for responsive market making
                
            except asyncio.CancelledError:
                self.logger.info("Strategy loop cancelled")
                break
            except Exception as e:
                self.logger.error(f"Error in strategy loop: {e}")
                await asyncio.sleep(1)  # Reduced error retry time
    
    async def _should_pause_for_cooldown(self) -> bool:
        """Check if strategy should pause due to aggressive TWAP cooldown."""
        try:
            if not self.redis_client:
                return False
            
            cooldown_key = f"strategy_cooldown:{self.symbol}"
            
            # Use a timeout to prevent Redis from blocking the loop
            try:
                cooldown_data = await asyncio.wait_for(
                    self.redis_client.get(cooldown_key), 
                    timeout=0.01  # 10ms timeout for Redis call
                )
            except asyncio.TimeoutError:
                # If Redis is slow, don't block the strategy
                return False
            
            if cooldown_data:
                try:
                    # Parse cooldown data using JSON instead of eval() for security
                    import json
                    cooldown_info = json.loads(cooldown_data.decode('utf-8'))
                    cooldown_until = datetime.fromisoformat(cooldown_info['cooldown_until'])
                    
                    if datetime.now(timezone.utc) < cooldown_until:
                        self.logger.info(f"Cooldown active from {cooldown_info['strategy']} until {cooldown_until}")
                        
                        # Cancel all existing orders when cooldown is detected
                        await self._cancel_all_orders_for_cooldown()
                        
                        return True
                except json.JSONDecodeError as e:
                    self.logger.warning(f"Invalid JSON in cooldown data: {e}")
                except Exception as e:
                    self.logger.warning(f"Error parsing cooldown data: {e}")
            
            return False
            
        except Exception as e:
            self.logger.debug(f"Error checking cooldown: {e}")  # Reduced to debug to avoid spam
            return False

    async def _cancel_all_orders_for_cooldown(self) -> None:
        """Cancel all active orders due to cooldown signal."""
        try:
            self.logger.info("Cancelling all passive quoting orders due to cooldown signal")
            
            # Cancel all orders for all quote lines
            for line in self.quote_lines:
                # Cancel all bid orders
                for exchange in list(line.bid_orders.keys()):
                    await self._cancel_line_order(line, 'bid', 'cooldown', exchange)
                
                # Cancel all ask orders  
                for exchange in list(line.ask_orders.keys()):
                    await self._cancel_line_order(line, 'ask', 'cooldown', exchange)
            
            self.logger.info("All passive quoting orders cancelled due to cooldown")
            
        except Exception as e:
            self.logger.error(f"Error cancelling orders for cooldown: {e}")
                
    async def _process_quote_line(self, line: QuoteLine) -> None:
        """Process a single quote line."""
        try:
            # Check for timeouts
            await self._check_timeouts(line)
            
            # Check for drift
            await self._check_drift(line)
            
            # Place missing orders
            await self._place_missing_orders(line)
            
        except Exception as e:
            self.logger.error(f"Error processing line {line.line_id}: {e}")
            
    async def _check_timeouts(self, line: QuoteLine) -> None:
        """Check and cancel orders that have timed out (per exchange)."""
        now = datetime.now(timezone.utc)
        timeout_delta = timedelta(seconds=line.timeout_seconds)
        # Check bid timeouts per exchange
        for exchange in list(line.bid_orders.keys()):
            placed_at = line.bid_placed_at.get(exchange)
            if placed_at and (now - placed_at) > timeout_delta:
                self.logger.info(
                    f"Line {line.line_id}: Cancelling BID order on {exchange} due to TIMEOUT. "
                    f"Order age: {(now - placed_at).total_seconds():.2f}s > {line.timeout_seconds}s timeout."
                )
                await self._cancel_line_order(line, 'bid', 'timeout', exchange)
        # Check ask timeouts per exchange
        for exchange in list(line.ask_orders.keys()):
            placed_at = line.ask_placed_at.get(exchange)
            if placed_at and (now - placed_at) > timeout_delta:
                self.logger.info(
                    f"Line {line.line_id}: Cancelling ASK order on {exchange} due to TIMEOUT. "
                    f"Order age: {(now - placed_at).total_seconds():.2f}s > {line.timeout_seconds}s timeout."
                )
                await self._cancel_line_order(line, 'ask', 'timeout', exchange)
            
    async def _check_drift(self, line: QuoteLine) -> None:
        """Check and cancel orders that have drifted too far from current midpoint (per exchange)."""
        current_midpoints = self.redis_orderbook_manager.get_all_midpoints()
        for exchange in set(list(line.bid_orders.keys()) + list(line.ask_orders.keys())):
            resolved_symbol = self._resolve_symbol_for_exchange(exchange)
            midpoint = current_midpoints.get(exchange, {}).get(resolved_symbol)
            if midpoint is None:
                continue
            last_midpoint = line.last_midpoint_per_exchange.get(exchange)
            if last_midpoint is None:
                continue
            drift_pct = abs((midpoint - last_midpoint) / last_midpoint) * 100
            drift_bps = drift_pct * 100
            if drift_bps > line.drift_bps:
                # Log drift details before cancelling
                self.logger.info(
                    f"Line {line.line_id}: Cancelling orders on {exchange} due to DRIFT. "
                    f"Drift: {drift_bps:.2f} bps (allowed: {line.drift_bps:.2f} bps), "
                    f"Current midpoint: {midpoint}, Last midpoint: {last_midpoint}"
                )
                # Cancel only the orders for this exchange due to drift
                if line.has_bid_on_exchange(exchange):
                    await self._cancel_line_order(line, 'bid', 'drift', exchange)
                if line.has_ask_on_exchange(exchange):
                    await self._cancel_line_order(line, 'ask', 'drift', exchange)
            
    async def _place_missing_orders(self, line: QuoteLine) -> None:
        """Place missing orders for a quote line."""
        # Get current midpoints for all exchanges and symbols
        current_midpoints = self.redis_orderbook_manager.get_all_midpoints()
        
        # Calculate average midpoint from available exchanges
        valid_midpoints = []
        for exchange in self.exchanges:
            resolved_symbol = self._resolve_symbol_for_exchange(exchange)
            if exchange in current_midpoints and resolved_symbol in current_midpoints[exchange]:
                midpoint = current_midpoints[exchange][resolved_symbol]
                if midpoint is not None:
                    valid_midpoints.append(midpoint)
        
        if not valid_midpoints:
            self.logger.debug(f"Line {line.line_id}: No valid midpoints available")
            return
            
        avg_midpoint = sum(valid_midpoints) / len(valid_midpoints)
        
        # Calculate spread in price terms
        spread_decimal = Decimal(str(line.spread_bps)) / Decimal('10000')
        spread_amount = avg_midpoint * spread_decimal
        
        # Calculate initial bid and ask prices based on midpoint
        initial_bid_price = avg_midpoint - spread_amount
        initial_ask_price = avg_midpoint + spread_amount
        
        # Collect order placement tasks for concurrent execution
        order_tasks = []
        
        for exchange in self.exchanges:
            resolved_symbol = self._resolve_symbol_for_exchange(exchange)
            midpoint = current_midpoints.get(exchange, {}).get(resolved_symbol)
            if midpoint is None:
                continue
                
            # Get best bid/ask for this exchange for taker check
            best_bid, best_ask = self.redis_orderbook_manager.get_best_bid_ask(exchange, resolved_symbol)
            
            if not best_bid or not best_ask:
                self.logger.debug(f"Line {line.line_id}: No best bid/ask available for {exchange} (symbol: {resolved_symbol})")
                continue
            
            # Update last midpoint for this exchange
            line.last_midpoint_per_exchange[exchange] = midpoint
            
            # Apply taker check to prices for this exchange
            exchange_bid_price = self._apply_taker_check(initial_bid_price, 'bid', best_bid, best_ask)
            exchange_ask_price = self._apply_taker_check(initial_ask_price, 'ask', best_bid, best_ask)
            
            # Create bid order task if needed and price is valid
            if (line.sides in [QuoteSide.BOTH, QuoteSide.BID] and 
                not line.has_bid_on_exchange(exchange) and
                exchange_bid_price is not None):
                task = self._place_line_order_on_exchange(
                    line, 'bid', exchange_bid_price, avg_midpoint, exchange
                )
                order_tasks.append(task)
            
            # Create ask order task if needed and price is valid
            if (line.sides in [QuoteSide.BOTH, QuoteSide.OFFER] and 
                not line.has_ask_on_exchange(exchange) and
                exchange_ask_price is not None):
                task = self._place_line_order_on_exchange(
                    line, 'ask', exchange_ask_price, avg_midpoint, exchange
                )
                order_tasks.append(task)
        
        # Execute all order placement tasks concurrently
        if order_tasks:
            await asyncio.gather(*order_tasks, return_exceptions=True)
            
    def _apply_taker_check(self, quote_price: Decimal, side: str, best_bid: Decimal, best_ask: Decimal) -> Optional[Decimal]:
        """Apply taker check to ensure we don't cross the spread."""
        min_tick_size = Decimal('0.000001')  # Default minimum tick size
        
        if side == 'bid':
            # For bid orders, ensure we don't exceed best ask (become taker)
            if quote_price >= best_ask:
                # Adjust to be just below best ask
                adjusted_price = best_ask - min_tick_size
                self.logger.debug(f"Taker check: Bid price {quote_price} >= best ask {best_ask}, adjusted to {adjusted_price}")
                return adjusted_price
            return quote_price
        else:  # ask
            # For ask orders, ensure we don't go below best bid (become taker)
            if quote_price <= best_bid:
                # Adjust to be just above best bid
                adjusted_price = best_bid + min_tick_size
                self.logger.debug(f"Taker check: Ask price {quote_price} <= best bid {best_bid}, adjusted to {adjusted_price}")
                return adjusted_price
            return quote_price
            
    async def _place_line_order_on_exchange(
        self, 
        line: QuoteLine, 
        side: str, 
        price: Decimal, 
        midpoint: Decimal, 
        exchange: str
    ) -> None:
        """Place an order for a specific line and side on a specific exchange."""
        try:
            # Calculate randomized quantity
            base_qty = Decimal(str(line.quantity))
            randomization = Decimal(str(line.quantity_randomization_factor)) / Decimal('100')
            qty_multiplier = Decimal('1') + Decimal(str(random.uniform(-float(randomization), float(randomization))))
            randomized_qty = base_qty * qty_multiplier
            
            # Convert quantity if needed
            if self.quantity_currency == QuantityCurrency.QUOTE:
                # Convert quote quantity to base quantity using midpoint
                final_qty = randomized_qty / midpoint
            else:
                final_qty = randomized_qty
                
            # Round quantity to appropriate precision
            # For now, use 8 decimal places - in production, get from exchange info
            final_qty = round(final_qty, 8)
            
            # Place order on the specified exchange
            timestamp_ms = int(time.time() * 1000)
            client_order_id = f"pass_quote_{side.upper()}_{timestamp_ms}"
            
            # Convert side: 'bid' -> 'buy', 'ask' -> 'sell' (like other strategies)
            order_side = 'buy' if side == 'bid' else 'sell'
            
            order_id = await self._place_order(
                exchange=exchange,
                side=order_side,
                amount=float(final_qty),
                price=float(price),
                client_order_id=client_order_id
            )
            
            if order_id:
                now = datetime.now(timezone.utc)
                if side == 'bid':
                    line.bid_orders[exchange] = order_id
                    line.bid_placed_at[exchange] = now
                    line.bid_placed_price = price
                else:
                    line.ask_orders[exchange] = order_id
                    line.ask_placed_at[exchange] = now
                    line.ask_placed_price = price
                self.orders_placed += 1
                self.logger.info(
                    f"Line {line.line_id}: Placed {side} order @ {price:.4f} "
                    f"qty={final_qty:.8f} on {exchange}"
                )
                
        except Exception as e:
            self.logger.error(f"Error placing {side} order for line {line.line_id} on {exchange}: {e}")
            
    async def _cancel_line_order(self, line: QuoteLine, side: str, reason: str, exchange: Optional[str] = None) -> None:
        """Cancel orders for a specific line, side, and (optionally) exchange only."""
        try:
            if side == 'bid':
                orders_to_cancel = line.bid_orders
                placed_at = line.bid_placed_at
            else:
                orders_to_cancel = line.ask_orders
                placed_at = line.ask_placed_at
            if not orders_to_cancel:
                return
            cancel_tasks = []
            if exchange:
                # Cancel only for the specified exchange
                order_id = orders_to_cancel.get(exchange)
                if order_id:
                    self.logger.info(
                        f"Line {line.line_id}: Cancelling {side.upper()} order on {exchange} due to {reason.upper()} (order_id: {order_id})"
                    )
                    cancel_tasks.append(self._cancel_order(order_id, exchange))
            else:
                # Cancel on all exchanges
                for ex, order_id in orders_to_cancel.items():
                    self.logger.info(
                        f"Line {line.line_id}: Cancelling {side.upper()} order on {ex} due to {reason.upper()} (order_id: {order_id})"
                    )
                    cancel_tasks.append(self._cancel_order(order_id, ex))
            await asyncio.gather(*cancel_tasks, return_exceptions=True)
            # Update line state
            if exchange:
                if exchange in orders_to_cancel:
                    del orders_to_cancel[exchange]
                if exchange in placed_at:
                    del placed_at[exchange]
                if side == 'bid' and not orders_to_cancel:
                    line.bid_placed_price = None
                if side == 'ask' and not orders_to_cancel:
                    line.ask_placed_price = None
            else:
                orders_to_cancel.clear()
                placed_at.clear()
                if side == 'bid':
                    line.bid_placed_price = None
                else:
                    line.ask_placed_price = None
            # Update stats
            if reason == 'timeout':
                self.orders_cancelled_timeout += 1
            elif reason == 'drift':
                self.orders_cancelled_drift += 1
            self.logger.info(
                f"Line {line.line_id}: Cancelled {side} order(s) on {exchange if exchange else 'all exchanges'} due to {reason}"
            )
        except Exception as e:
            self.logger.error(f"Error cancelling {side} orders for line {line.line_id} on {exchange if exchange else 'all'}: {e}")
            
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get enhanced performance statistics."""
        base_stats = super().get_performance_stats()
        
        # Add passive quoting specific stats
        passive_stats = {
            'orders_placed': self.orders_placed,
            'orders_cancelled_timeout': self.orders_cancelled_timeout,
            'orders_cancelled_drift': self.orders_cancelled_drift,
            'quote_lines': len(self.quote_lines),
            'active_lines': sum(
                1 for line in self.quote_lines 
                if line.bid_orders or line.ask_orders
            ),
            'available_exchanges': len(self.redis_orderbook_manager.get_available_exchanges()),
            'orderbook_status': self.redis_orderbook_manager.get_status()
        }
        
        base_stats.update(passive_stats)
        return base_stats
        
    def get_line_status(self) -> List[Dict[str, Any]]:
        """Get status of all quote lines."""
        line_status = []
        
        for line in self.quote_lines:
            status = {
                'line_id': line.line_id,
                'sides': line.sides.value,
                'spread_bps': line.spread_bps,
                'quantity': line.quantity,
                'timeout_seconds': line.timeout_seconds,
                'drift_bps': line.drift_bps,
                'bid_active': line.get_active_bid_count(),
                'ask_active': line.get_active_ask_count(),
                'bid_exchanges': list(line.bid_orders.keys()),
                'ask_exchanges': list(line.ask_orders.keys()),
                'last_midpoints': {
                    exchange: float(midpoint) 
                    for exchange, midpoint in line.last_midpoint_per_exchange.items()
                }
            }
            
            if line.bid_placed_at:
                status['bid_age_seconds'] = (
                    datetime.now(timezone.utc) - line.bid_placed_at
                ).total_seconds()
                
            if line.ask_placed_at:
                status['ask_age_seconds'] = (
                    datetime.now(timezone.utc) - line.ask_placed_at
                ).total_seconds()
                
            line_status.append(status)
            
        return line_status
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert strategy state to dictionary with passive quoting details."""
        base_dict = super().to_dict()
        
        passive_dict = {
            'strategy_type': 'passive_quoting',
            'quantity_currency': self.quantity_currency.value,
            'line_status': self.get_line_status(),
            'redis_orderbook_status': self.redis_orderbook_manager.get_status()
        }
        
        base_dict.update(passive_dict)
        return base_dict 
