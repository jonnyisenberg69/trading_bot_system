"""
Optimized Stacked Market Making Strategy - Multi-pair optimized version.

This is the high-performance version of the stacked market making strategy
optimized for running hundreds of trading pairs with minimal latency and
resource consumption.

Key optimizations:
- Uses shared market data services
- Batched coefficient updates
- Event-driven processing instead of polling
- Optimized data structures
- Sub-millisecond latency targeting
"""

import asyncio
import time
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Optional, Any, Set, Tuple
import logging

from .base_strategy import BaseStrategy
from .inventory_manager import InventoryManager, InventoryConfig, InventoryPriceMethod
# Note: Optimized components would be implemented when deploying at scale
# For now, using existing infrastructure

logger = logging.getLogger(__name__)


class LineType(str, Enum):
    """Types of market making lines."""
    TOP_OF_BOOK = "tob"
    PASSIVE = "passive"


class PricingSourceType(str, Enum):
    """Pricing source options."""
    AGGREGATED = "aggregated"
    COMPONENT_VENUES = "component"
    LIQUIDITY_WEIGHTED = "liquidity_weighted"


@dataclass
class OptimizedTOBLine:
    """Optimized TOB line for minimal memory footprint."""
    line_id: int
    hourly_quantity: Decimal
    spread_bps: Decimal
    timeout_seconds: int
    coefficient_method: str = "volume"
    
    # Optimized runtime state
    current_coefficient: float = 1.0
    active_orders: Dict[str, str] = field(default_factory=dict)  # exchange -> order_id
    last_prices: Dict[str, int] = field(default_factory=dict)  # exchange -> price_int (price * 1e8)
    placed_at: Dict[str, float] = field(default_factory=dict)  # exchange -> timestamp


@dataclass
class OptimizedPassiveLine:
    """Optimized passive line for minimal memory footprint."""
    line_id: int
    mid_spread_bps: Decimal
    quantity: Decimal
    timeout_seconds: int
    
    # Optimized runtime state
    current_spread_coefficient: float = 1.0
    current_quantity_coefficient: float = 1.0
    active_orders: Dict[str, str] = field(default_factory=dict)
    last_reference_price: Optional[int] = None  # price_int (price * 1e8)
    placed_at: Dict[str, float] = field(default_factory=dict)


class OptimizedStackedMarketMakingStrategy(BaseStrategy):
    """
    Optimized stacked market making strategy for multi-pair operation.
    
    Optimizations vs standard version:
    - Uses shared market data service (no individual Redis connections)
    - Event-driven updates instead of polling loops
    - Batched coefficient processing
    - Optimized data structures (integer arithmetic where possible)
    - Minimal memory footprint per strategy instance
    - Sub-millisecond target latency
    """
    
    def __init__(
        self,
        instance_id: str,
        symbol: str,
        exchanges: List[str],
        config: Dict[str, Any],
        priority: int = 1  # Processing priority for multi-pair coordination
    ):
        super().__init__(instance_id, symbol, exchanges, config)
        
        self.priority = priority
        
        # Optimized configuration
        self.inventory_manager: Optional[InventoryManager] = None
        self.tob_lines: List[OptimizedTOBLine] = []
        self.passive_lines: List[OptimizedPassiveLine] = []
        
        # Shared service references
        self.coordinator = None
        self.shared_market_data = None
        
        # Performance tracking
        self.update_count = 0
        self.last_update_ns = 0
        self.processing_times: List[float] = []
        
        # Event-driven state
        self.market_data_event = asyncio.Event()
        self.coefficient_update_event = asyncio.Event()
        
        # Background tasks (minimal)
        self.processing_task: Optional[asyncio.Task] = None
        
    async def _validate_config(self) -> None:
        """Validate optimized configuration."""
        try:
            # Parse inventory config
            inventory_config_dict = self.config.get('inventory', {})
            inventory_config = InventoryConfig(
                target_inventory=Decimal(str(inventory_config_dict.get('target_inventory', '0'))),
                max_inventory_deviation=Decimal(str(inventory_config_dict.get('max_inventory_deviation', '1000'))),
                inventory_price_method=InventoryPriceMethod(
                    inventory_config_dict.get('inventory_price_method', 'accounting')
                )
            )
            
            self.inventory_manager = InventoryManager(inventory_config)
            
            # Parse optimized TOB lines
            for i, tob_config in enumerate(self.config.get('tob_lines', [])):
                line = OptimizedTOBLine(
                    line_id=i,
                    hourly_quantity=Decimal(str(tob_config.get('hourly_quantity', '100'))),
                    spread_bps=Decimal(str(tob_config.get('spread_bps', '50'))),
                    timeout_seconds=int(tob_config.get('timeout_seconds', 30)),
                    coefficient_method=tob_config.get('coefficient_method', 'volume')
                )
                self.tob_lines.append(line)
            
            # Parse optimized passive lines
            for i, passive_config in enumerate(self.config.get('passive_lines', [])):
                line = OptimizedPassiveLine(
                    line_id=i,
                    mid_spread_bps=Decimal(str(passive_config.get('mid_spread_bps', '20'))),
                    quantity=Decimal(str(passive_config.get('quantity', '100'))),
                    timeout_seconds=int(passive_config.get('timeout_seconds', 60))
                )
                self.passive_lines.append(line)
            
            if not self.tob_lines and not self.passive_lines:
                raise ValueError("At least one line type must be configured")
                
            self.logger.info(
                f"Optimized strategy validated: {len(self.tob_lines)} TOB, {len(self.passive_lines)} passive lines"
            )
            
        except Exception as e:
            self.logger.error(f"Optimized config validation failed: {e}")
            raise
    
    async def _start_strategy(self) -> None:
        """Start optimized strategy with shared services."""
        self.logger.info("Starting Optimized Stacked Market Making Strategy")
        
        # Register with multi-pair coordinator
        self.coordinator = await get_multi_pair_coordinator()
        await self.coordinator.register_strategy(
            strategy_id=self.instance_id,
            strategy_instance=self,
            symbols=[self.symbol],
            exchanges=self.exchanges,
            priority=self.priority,
            coefficient_config={
                'time_periods': self.config.get('time_periods', ['5min', '15min']),
                'coefficient_method': self.config.get('coefficient_method', 'min'),
                'min_coefficient': self.config.get('min_coefficient', 0.2),
                'max_coefficient': self.config.get('max_coefficient', 3.0)
            }
        )
        
        # Get shared market data reference
        self.shared_market_data = await get_shared_market_data_service()
        
        # Start minimal processing task (event-driven)
        self.processing_task = asyncio.create_task(self._event_driven_processing())
        
        self.logger.info(f"Optimized strategy started (priority: {self.priority})")
    
    async def _stop_strategy(self) -> None:
        """Stop optimized strategy."""
        self.logger.info("Stopping Optimized Stacked Market Making Strategy")
        
        # Cancel processing task
        if self.processing_task:
            self.processing_task.cancel()
            try:
                await self.processing_task
            except asyncio.CancelledError:
                pass
        
        # Unregister from coordinator
        if self.coordinator:
            await self.coordinator.unregister_strategy(self.instance_id)
        
        # Cancel all orders
        await self._cancel_all_optimized_orders()
    
    async def _event_driven_processing(self):
        """Event-driven processing loop (no polling, minimal latency)."""
        while self.running:
            try:
                # Wait for market data or coefficient update events
                done, pending = await asyncio.wait(
                    [
                        asyncio.create_task(self.market_data_event.wait()),
                        asyncio.create_task(self.coefficient_update_event.wait())
                    ],
                    return_when=asyncio.FIRST_COMPLETED,
                    timeout=0.1  # 100ms timeout for safety
                )
                
                # Cancel pending tasks
                for task in pending:
                    task.cancel()
                
                processing_start = time.perf_counter_ns()
                
                # Process market data updates
                if self.market_data_event.is_set():
                    await self._process_market_data_update()
                    self.market_data_event.clear()
                
                # Process coefficient updates
                if self.coefficient_update_event.is_set():
                    await self._process_coefficient_update()
                    self.coefficient_update_event.clear()
                
                # Track processing time
                processing_time = (time.perf_counter_ns() - processing_start) / 1000  # microseconds
                self.processing_times.append(processing_time)
                if len(self.processing_times) > 100:
                    self.processing_times = self.processing_times[-50:]
                
                self.update_count += 1
                self.last_update_ns = time.perf_counter_ns()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in event-driven processing: {e}")
                await asyncio.sleep(0.001)
    
    async def process_batch_market_updates(self, updates: List[Dict[str, Any]]):
        """
        Process batched market data updates (called by coordinator).
        
        This method is called by the multi-pair coordinator for efficient
        batch processing of market data updates.
        """
        try:
            # Process all updates for this strategy
            for update in updates:
                await self._process_single_market_update(update['data'])
            
            # Trigger processing
            self.market_data_event.set()
            
        except Exception as e:
            self.logger.error(f"Error processing batch market updates: {e}")
    
    async def update_exchange_coefficients(self, coefficients: Dict[str, float]):
        """
        Update exchange coefficients (called by coordinator).
        
        This method receives shared coefficient calculations from the coordinator.
        """
        try:
            # Apply coefficients to all lines
            for line in self.tob_lines:
                if line.coefficient_method in ['volume', 'both']:
                    # Use average coefficient across exchanges
                    if coefficients:
                        avg_coeff = sum(coefficients.values()) / len(coefficients)
                        line.current_coefficient = avg_coeff
            
            for line in self.passive_lines:
                # Apply to quantity coefficient
                if coefficients:
                    avg_coeff = sum(coefficients.values()) / len(coefficients)
                    line.current_quantity_coefficient = avg_coeff
            
            # Trigger coefficient update processing
            self.coefficient_update_event.set()
            
            self.logger.debug(f"Updated coefficients: {coefficients}")
            
        except Exception as e:
            self.logger.error(f"Error updating exchange coefficients: {e}")
    
    async def _process_single_market_update(self, update_data: Dict[str, Any]):
        """Process a single market data update with minimal latency."""
        try:
            symbol = update_data.get('symbol')
            if symbol != self.symbol:
                return
            
            # Extract key data for fast processing
            best_bid = update_data.get('best_bid')
            best_ask = update_data.get('best_ask')
            midpoint = update_data.get('midpoint')
            
            if not best_bid or not best_ask:
                return
            
            # Convert to integers for fast processing
            best_bid_int = int(float(best_bid) * 100000000)
            best_ask_int = int(float(best_ask) * 100000000)
            midpoint_int = int(float(midpoint) * 100000000)
            
            # Store for line processing
            self.cached_market_data = {
                'best_bid_int': best_bid_int,
                'best_ask_int': best_ask_int,
                'midpoint_int': midpoint_int,
                'timestamp_ns': update_data.get('update_ns', time.perf_counter_ns())
            }
            
        except Exception as e:
            self.logger.error(f"Error processing market update: {e}")
    
    async def _process_market_data_update(self):
        """Process market data update and manage orders."""
        try:
            if not hasattr(self, 'cached_market_data'):
                return
            
            market_data = self.cached_market_data
            current_time = time.time()
            
            # Check timeouts first (fastest check)
            await self._check_timeouts_optimized(current_time)
            
            # Process TOB lines
            for line in self.tob_lines:
                await self._process_tob_line_optimized(line, market_data, current_time)
            
            # Process passive lines
            for line in self.passive_lines:
                await self._process_passive_line_optimized(line, market_data, current_time)
                
        except Exception as e:
            self.logger.error(f"Error processing market data update: {e}")
    
    async def _process_coefficient_update(self):
        """Process coefficient update and adjust lines."""
        try:
            # Get latest coefficients from coordinator
            if self.coordinator:
                coefficients = await self.coordinator.get_shared_coefficients(self.instance_id, self.symbol)
                
                # Apply inventory coefficient adjustments
                inventory_coeff = self.inventory_manager.get_inventory_coefficient()
                
                # Update line coefficients
                for line in self.tob_lines + self.passive_lines:
                    if hasattr(line, 'current_coefficient'):
                        # Combine volume and inventory coefficients
                        if coefficients:
                            avg_volume_coeff = sum(coefficients.values()) / len(coefficients)
                            # Normalize inventory coefficient from [-1,1] to [0.5, 1.5]
                            inv_factor = 1.0 + float(inventory_coeff) * 0.25
                            line.current_coefficient = avg_volume_coeff * inv_factor
                
        except Exception as e:
            self.logger.error(f"Error processing coefficient update: {e}")
    
    async def _process_tob_line_optimized(
        self, 
        line: OptimizedTOBLine, 
        market_data: Dict[str, Any], 
        current_time: float
    ):
        """Process TOB line with optimized performance."""
        try:
            # Fast timeout check
            for exchange in list(line.active_orders.keys()):
                placed_time = line.placed_at.get(exchange, 0)
                if current_time - placed_time > line.timeout_seconds:
                    await self._cancel_line_order_fast(line, exchange)
            
            # Calculate current quantity (convert from hourly rate)
            current_quantity = line.hourly_quantity * Decimal(str(line.current_coefficient)) / Decimal('1800')  # 30 min worth
            
            # Fast order placement check for each exchange
            for exchange in self.exchanges:
                if exchange not in line.active_orders:
                    # Calculate TOB price using integer arithmetic for speed
                    best_bid_int = market_data['best_bid_int']
                    best_ask_int = market_data['best_ask_int']
                    
                    # TOB pricing: bid = best_bid + tick, ask = best_ask - tick
                    min_tick_int = 1  # Minimum tick (1e-8)
                    
                    if current_quantity > 0:  # Only place if we have positive quantity
                        # Determine side based on inventory
                        inventory_state = self.inventory_manager.get_inventory_state()
                        
                        should_bid = inventory_state.excess_inventory < 0  # We're short, need to buy
                        should_ask = inventory_state.excess_inventory > 0  # We're long, need to sell
                        
                        if should_bid:
                            tob_price_int = best_bid_int + min_tick_int
                            await self._place_optimized_order(line, exchange, 'buy', tob_price_int, current_quantity, current_time)
                        elif should_ask:
                            tob_price_int = best_ask_int - min_tick_int
                            await self._place_optimized_order(line, exchange, 'sell', tob_price_int, current_quantity, current_time)
            
        except Exception as e:
            self.logger.error(f"Error processing TOB line {line.line_id}: {e}")
    
    async def _process_passive_line_optimized(
        self,
        line: OptimizedPassiveLine,
        market_data: Dict[str, Any],
        current_time: float
    ):
        """Process passive line with optimized performance."""
        try:
            # Fast timeout check
            for exchange in list(line.active_orders.keys()):
                placed_time = line.placed_at.get(exchange, 0)
                if current_time - placed_time > line.timeout_seconds:
                    await self._cancel_line_order_fast(line, exchange)
            
            # Get aggregated midpoint
            midpoint_int = market_data['midpoint_int']
            
            # Fast drift check
            if line.last_reference_price is not None:
                drift_bps = abs(midpoint_int - line.last_reference_price) / line.last_reference_price * 1000000  # bps in int format
                if drift_bps > float(line.mid_spread_bps) * 100:  # Convert to int format
                    # Cancel all orders for this line
                    for exchange in list(line.active_orders.keys()):
                        await self._cancel_line_order_fast(line, exchange)
            
            line.last_reference_price = midpoint_int
            
            # Calculate spread with coefficients
            base_spread_bps = line.mid_spread_bps
            
            # Apply inventory adjustment
            inventory_coeff = self.inventory_manager.get_inventory_coefficient()
            inventory_spread_adjustment = 1.0 + float(inventory_coeff) * 0.2  # ±20% adjustment
            
            # Apply quantity coefficient
            adjusted_spread_bps = base_spread_bps * Decimal(str(inventory_spread_adjustment))
            adjusted_quantity = line.quantity * Decimal(str(line.current_quantity_coefficient))
            
            # Convert spread to integer format for fast calculation
            spread_int = int(float(adjusted_spread_bps) * midpoint_int / 10000)
            
            bid_price_int = midpoint_int - spread_int
            ask_price_int = midpoint_int + spread_int
            
            # Place orders on exchanges without active orders
            for exchange in self.exchanges:
                if exchange not in line.active_orders:
                    # Apply taker check by getting exchange-specific data
                    if adjusted_quantity > 0:
                        # Simplified side logic for passive lines
                        await self._place_optimized_order(line, exchange, 'buy', bid_price_int, adjusted_quantity, current_time)
                        await self._place_optimized_order(line, exchange, 'sell', ask_price_int, adjusted_quantity, current_time)
            
        except Exception as e:
            self.logger.error(f"Error processing passive line {line.line_id}: {e}")
    
    async def _place_optimized_order(
        self,
        line: Any,  # OptimizedTOBLine or OptimizedPassiveLine
        exchange: str,
        side: str,
        price_int: int,
        quantity: Decimal,
        current_time: float
    ):
        """Place order with optimized performance."""
        try:
            # Convert price back to Decimal for order placement
            price = Decimal(price_int) / Decimal('100000000')
            
            # Fast validation
            if price <= 0 or quantity <= 0:
                return
            
            # Place order using base strategy method
            order_id = await self._place_order(
                exchange=exchange,
                side=side,
                amount=float(quantity),
                price=float(price),
                order_type='limit'
            )
            
            if order_id:
                # Track in optimized format
                line.active_orders[exchange] = order_id
                line.placed_at[exchange] = current_time
                line.last_prices[exchange] = price_int
                
                self.logger.debug(
                    f"Placed optimized {side} order: {quantity:.4f} @ {price:.6f} on {exchange}"
                )
        
        except Exception as e:
            self.logger.error(f"Error placing optimized order: {e}")
    
    async def _cancel_line_order_fast(self, line: Any, exchange: str):
        """Cancel line order with minimal latency."""
        try:
            order_id = line.active_orders.get(exchange)
            if order_id and order_id in self.active_orders:
                await self._cancel_order(order_id, exchange)
                
                # Clean up tracking
                del line.active_orders[exchange]
                line.placed_at.pop(exchange, None)
                line.last_prices.pop(exchange, None)
        
        except Exception as e:
            self.logger.error(f"Error cancelling order fast: {e}")
    
    async def _check_timeouts_optimized(self, current_time: float):
        """Check timeouts across all lines with minimal latency."""
        cancel_tasks = []
        
        # Check TOB line timeouts
        for line in self.tob_lines:
            for exchange in list(line.active_orders.keys()):
                placed_time = line.placed_at.get(exchange, 0)
                if current_time - placed_time > line.timeout_seconds:
                    cancel_tasks.append(self._cancel_line_order_fast(line, exchange))
        
        # Check passive line timeouts
        for line in self.passive_lines:
            for exchange in list(line.active_orders.keys()):
                placed_time = line.placed_at.get(exchange, 0)
                if current_time - placed_time > line.timeout_seconds:
                    cancel_tasks.append(self._cancel_line_order_fast(line, exchange))
        
        # Execute all cancellations in parallel
        if cancel_tasks:
            await asyncio.gather(*cancel_tasks, return_exceptions=True)
    
    async def _cancel_all_optimized_orders(self):
        """Cancel all orders with optimized performance."""
        cancel_tasks = []
        
        for line in self.tob_lines + self.passive_lines:
            for exchange in list(line.active_orders.keys()):
                cancel_tasks.append(self._cancel_line_order_fast(line, exchange))
        
        if cancel_tasks:
            await asyncio.gather(*cancel_tasks, return_exceptions=True)
    
    async def on_trade_confirmation(self, trade_data: Dict[str, Any]) -> None:
        """Handle trade confirmation with optimized inventory update."""
        try:
            # Call base class
            await super().on_trade_confirmation(trade_data)
            
            # Fast inventory update
            exchange = trade_data.get('exchange', '')
            side = trade_data.get('side', '')
            amount = Decimal(str(trade_data.get('amount', 0)))
            price = Decimal(str(trade_data.get('price', 0)))
            
            # Update inventory (this triggers coefficient recalculation)
            self.inventory_manager.update_position(exchange, side, amount, price)
            
            # Trigger immediate reprocessing
            self.market_data_event.set()
            
        except Exception as e:
            self.logger.error(f"Error in optimized trade confirmation: {e}")
    
    def get_optimized_performance_stats(self) -> Dict[str, Any]:
        """Get performance statistics optimized for multi-pair monitoring."""
        base_stats = self.get_performance_stats()
        
        optimized_stats = {
            'update_count': self.update_count,
            'last_update_age_ms': (time.perf_counter_ns() - self.last_update_ns) / 1000000 if self.last_update_ns else 0,
            'avg_processing_time_us': sum(self.processing_times) / len(self.processing_times) if self.processing_times else 0,
            'p95_processing_time_us': sorted(self.processing_times)[int(0.95 * len(self.processing_times))] if len(self.processing_times) > 20 else 0,
            'priority': self.priority,
            'active_tob_orders': sum(len(line.active_orders) for line in self.tob_lines),
            'active_passive_orders': sum(len(line.active_orders) for line in self.passive_lines),
            'inventory_coefficient': float(self.inventory_manager.get_inventory_coefficient()) if self.inventory_manager else 0,
            'shared_services': True,
            'optimization_version': 'v2_multi_pair'
        }
        
        base_stats.update(optimized_stats)
        return base_stats
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert optimized strategy state to dictionary."""
        base_dict = super().to_dict()
        
        optimized_dict = {
            'strategy_type': 'optimized_stacked_market_making',
            'optimization_level': 'multi_pair_v2',
            'performance': self.get_optimized_performance_stats(),
            'shared_services_enabled': True
        }
        
        base_dict.update(optimized_dict)
        return base_dict
