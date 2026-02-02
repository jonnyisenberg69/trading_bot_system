"""
Order manager for handling orders across multiple exchanges.

Responsible for:
- Order creation, tracking, and status monitoring
- Managing order lifecycle (placement, updates, cancellation)
- Order history and reporting
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any, Union, Callable
from decimal import Decimal
from datetime import datetime
import uuid
import json
from collections import defaultdict
import structlog

from .order import Order, OrderStatus, OrderSide, OrderType
from exchanges.connectors import list_supported_exchanges
from database.repositories.order_repository import OrderRepository
from .tracking import PositionManager


logger = structlog.get_logger(__name__)


class OrderManager:
    """
    Order manager for handling orders across multiple exchanges.
    
    Responsible for tracking all orders, handling status updates,
    and providing a unified interface for order operations.
    """
    
    def __init__(
        self, 
        exchange_connectors: Dict[str, Any], 
        order_repository: OrderRepository = None,
        position_manager: Optional[PositionManager] = None
    ):
        """
        Initialize order manager.
        
        Args:
            exchange_connectors: Dictionary of exchange connectors
            order_repository: Repository for order persistence
            position_manager: Manager for tracking positions
        """
        self.exchange_connectors = exchange_connectors
        self.order_repository = order_repository
        self.position_manager = position_manager
        
        # Maps to store orders by different keys
        self.orders_by_id = {}  # client_order_id -> Order
        self.orders_by_exchange_id = {}  # (exchange, exchange_order_id) -> Order
        self.orders_by_symbol = defaultdict(list)  # symbol -> [Order]
        self.orders_by_exchange = defaultdict(list)  # exchange -> [Order]
        self.orders_by_status = defaultdict(list)  # status -> [Order]
        
        # Callbacks
        self.on_order_update_callbacks = []
        self.on_order_fill_callbacks = []
        
        # Status monitoring
        self.monitor_task = None
        self.monitor_interval = 10  # seconds
        
        self.logger = logger.bind(component="OrderManager")
        self.logger.info("Order manager initialized")
        
    async def start(self) -> None:
        """Start order monitoring and other background tasks."""
        self.logger.info("Starting order manager")
        
        # Load existing orders from repository
        if self.order_repository:
            await self._load_open_orders_from_repository()
            
        # Start status monitoring
        self.monitor_task = asyncio.create_task(self._monitor_orders())
        
        # Load open orders from exchanges
        await self._load_open_orders_from_exchanges()
    
    async def stop(self) -> None:
        """Stop order monitoring and other background tasks."""
        self.logger.info("Stopping order manager")
        
        if self.monitor_task:
            self.monitor_task.cancel()
            try:
                await self.monitor_task
            except asyncio.CancelledError:
                pass
            
        # Persist all orders
        if self.order_repository:
            for order in self.orders_by_id.values():
                await self.order_repository.save_order(order)
    
    async def create_order(
        self,
        symbol: str,
        side: Union[str, OrderSide],
        amount: Union[str, Decimal],
        order_type: Union[str, OrderType] = OrderType.LIMIT,
        price: Optional[Union[str, Decimal]] = None,
        exchange: Optional[str] = None,
        time_in_force: Optional[str] = None,
        reduce_only: bool = False,
        post_only: bool = False,
        leverage: Optional[int] = None,
        margin_type: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        client_order_id: Optional[str] = None,
        smart_route: bool = False
    ) -> Order:
        """
        Create a new order.
        
        Args:
            symbol: Trading symbol
            side: Buy or sell
            amount: Order amount
            order_type: Type of order
            price: Order price (required for limit orders)
            exchange: Specific exchange to use
            time_in_force: Time in force setting
            reduce_only: Whether order is reduce-only
            post_only: Whether order is post-only
            leverage: Leverage for margin/futures orders
            margin_type: Margin type for futures orders
            params: Additional exchange-specific parameters
            client_order_id: Custom order ID
            smart_route: Whether to use smart routing across exchanges
            
        Returns:
            Created order
        """
        # Convert amount to Decimal
        if isinstance(amount, str):
            amount = Decimal(amount)
            
        # Convert price to Decimal if provided
        if price is not None and isinstance(price, str):
            price = Decimal(price)
        
        # Create order object
        order = Order(
            symbol=symbol,
            side=side,
            amount=amount,
            order_type=order_type,
            price=price,
            exchange=exchange,
            client_order_id=client_order_id,
            time_in_force=time_in_force,
            reduce_only=reduce_only,
            post_only=post_only,
            leverage=leverage,
            margin_type=margin_type,
            params=params or {}
        )
        
        # Register order in maps
        self._register_order(order)
        
        # Smart routing across exchanges
        if smart_route and not exchange:
            await self._smart_route_order(order)
        else:
            # Place order on specified exchange
            if not exchange:
                # Use default exchange if none specified
                exchange = next(iter(self.exchange_connectors.keys()))
                order.exchange = exchange
                
            await self._place_order_on_exchange(order, exchange)
        
        # Save order to repository
        if self.order_repository:
            await self.order_repository.save_order(order)
            
        return order
    
    async def cancel_order(self, order_id: str) -> bool:
        """
        Cancel an order.
        
        Args:
            order_id: Client order ID
            
        Returns:
            True if cancellation was successful
        """
        if order_id not in self.orders_by_id:
            self.logger.warning(f"Order {order_id} not found for cancellation")
            return False
            
        order = self.orders_by_id[order_id]
        
        # If order is already cancelled or filled, no need to cancel
        if order.status in [OrderStatus.CANCELLED, OrderStatus.FILLED, OrderStatus.REJECTED]:
            self.logger.info(f"Order {order_id} already in terminal state: {order.status}")
            return True
            
        # If order is smart routed, cancel all child orders
        if order.is_smart_routed:
            success = True
            for child_id in order.child_orders:
                if child_id in self.orders_by_id:
                    child_success = await self.cancel_order(child_id)
                    success = success and child_success
            
            if success:
                order.cancel("Parent order cancelled via child orders")
                await self._notify_order_update(order)
                
                if self.order_repository:
                    await self.order_repository.save_order(order)
                    
            return success
        
        # Regular order cancellation
        try:
            exchange = order.exchange
            if not exchange or exchange not in self.exchange_connectors:
                self.logger.error(f"Cannot cancel order {order_id}: unknown exchange {exchange}")
                return False
                
            connector = self.exchange_connectors[exchange]
            
            # If order hasn't been placed on exchange yet
            if not order.exchange_order_id:
                order.cancel("Order cancelled before placement on exchange")
                await self._notify_order_update(order)
                
                if self.order_repository:
                    await self.order_repository.save_order(order)
                    
                return True
                
            # Cancel on exchange
            result = await connector.cancel_order(order.exchange_order_id, order.symbol)
            
            # Update order status
            if result and 'status' in result:
                order.update(result)
            else:
                # Force cancel if exchange didn't confirm
                order.cancel("Cancel request sent to exchange")
                
            await self._notify_order_update(order)
            
            # Save to repository
            if self.order_repository:
                await self.order_repository.save_order(order)
                
            return True
            
        except Exception as e:
            self.logger.error(f"Error cancelling order {order_id}: {e}")
            return False
    
    async def cancel_all_orders(self, symbol: Optional[str] = None, exchange: Optional[str] = None) -> int:
        """
        Cancel all active orders, optionally filtered by symbol and/or exchange.
        
        Args:
            symbol: Filter by symbol
            exchange: Filter by exchange
            
        Returns:
            Number of orders cancelled
        """
        orders_to_cancel = []
        
        # Get active orders
        for order in self.orders_by_id.values():
            if order.status in [OrderStatus.PENDING, OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED]:
                # Apply filters
                if symbol and order.symbol != symbol:
                    continue
                if exchange and order.exchange != exchange:
                    continue
                    
                orders_to_cancel.append(order.id)
        
        # Cancel orders
        cancelled_count = 0
        for order_id in orders_to_cancel:
            success = await self.cancel_order(order_id)
            if success:
                cancelled_count += 1
                
        return cancelled_count
    
    async def get_order(self, order_id: str) -> Optional[Order]:
        """
        Get order by client order ID.
        
        Args:
            order_id: Client order ID
            
        Returns:
            Order if found, None otherwise
        """
        if order_id in self.orders_by_id:
            order = self.orders_by_id[order_id]
            
            # Refresh order status from exchange if not in terminal state
            if order.status not in [OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED, OrderStatus.EXPIRED]:
                await self._refresh_order_status(order)
                
            return order
            
        # Try to load from repository
        if self.order_repository:
            order = await self.order_repository.get_order_by_id(order_id)
            if order:
                self._register_order(order)
                return order
                
        return None
    
    async def get_orders_by_symbol(self, symbol: str, include_filled: bool = False) -> List[Order]:
        """
        Get orders for a specific symbol.
        
        Args:
            symbol: Trading symbol
            include_filled: Whether to include filled orders
            
        Returns:
            List of orders
        """
        if symbol in self.orders_by_symbol:
            orders = self.orders_by_symbol[symbol]
            
            if not include_filled:
                return [o for o in orders if o.status != OrderStatus.FILLED]
                
            return orders
            
        return []
    
    async def get_open_orders(self, symbol: Optional[str] = None, exchange: Optional[str] = None) -> List[Order]:
        """
        Get all open orders, optionally filtered by symbol and/or exchange.
        
        Args:
            symbol: Filter by symbol
            exchange: Filter by exchange
            
        Returns:
            List of open orders
        """
        open_orders = []
        
        for order in self.orders_by_id.values():
            if order.status in [OrderStatus.PENDING, OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED]:
                # Apply filters
                if symbol and order.symbol != symbol:
                    continue
                if exchange and order.exchange != exchange:
                    continue
                    
                open_orders.append(order)
                
        return open_orders
    
    def register_order_update_callback(self, callback: Callable[[Order], None]) -> None:
        """
        Register callback for order updates.
        
        Args:
            callback: Function to call on order update
        """
        self.on_order_update_callbacks.append(callback)
        
    def register_order_fill_callback(self, callback: Callable[[Order, Dict], None]) -> None:
        """
        Register callback for order fills.
        
        Args:
            callback: Function to call on order fill
        """
        self.on_order_fill_callbacks.append(callback)
    
    async def _place_order_on_exchange(self, order: Order, exchange: str) -> bool:
        """
        Place order on specific exchange.
        
        Args:
            order: Order to place
            exchange: Exchange to use
            
        Returns:
            True if order was placed successfully
        """
        if exchange not in self.exchange_connectors:
            self.logger.error(f"Unknown exchange: {exchange}")
            order.reject(f"Unknown exchange: {exchange}")
            await self._notify_order_update(order)
            return False
            
        connector = self.exchange_connectors[exchange]
        
        try:
            # Update exchange in order
            order.exchange = exchange
            
            # Prepare parameters
            params = order.params.copy() if order.params else {}
            
            if order.time_in_force:
                params['timeInForce'] = order.time_in_force.value.upper()
                
            if order.reduce_only:
                params['reduceOnly'] = True
                
            if order.post_only:
                params['postOnly'] = True
                
            if order.client_order_id:
                params['clientOrderId'] = order.id
                
            # Place order
            result = await connector.place_order(
                symbol=order.symbol,
                side=order.side.value,
                amount=order.amount,
                price=order.price,
                order_type=order.order_type.value,
                params=params
            )
            
            # Update order with exchange data
            if result:
                order.update(result)
                await self._notify_order_update(order)
                return True
            else:
                order.reject("No response from exchange")
                await self._notify_order_update(order)
                return False
                
        except Exception as e:
            self.logger.error(f"Error placing order on {exchange}: {e}")
            order.reject(f"Error: {str(e)}")
            await self._notify_order_update(order)
            return False
    
    async def _smart_route_order(self, order: Order) -> bool:
        """
        Smart route order across multiple exchanges.
        
        Args:
            order: Order to route
            
        Returns:
            True if order was routed successfully
        """
        self.logger.info(f"Smart routing order {order.id}")
        
        # Mark order as smart routed
        order.is_smart_routed = True
        
        # TODO: Implement smart routing logic
        # For now, just split equally between exchanges
        
        # Get available exchanges for the symbol
        available_exchanges = []
        for exchange, connector in self.exchange_connectors.items():
            # Check if symbol is available on exchange
            try:
                if await connector.has_symbol(order.symbol):
                    available_exchanges.append(exchange)
            except:
                continue
        
        if not available_exchanges:
            order.reject(f"No exchange available for symbol {order.symbol}")
            await self._notify_order_update(order)
            return False
            
        # For now, just use the first available exchange
        exchange = available_exchanges[0]
        
        # Create a child order on selected exchange
        child_order = Order(
            symbol=order.symbol,
            side=order.side,
            amount=order.amount,
            order_type=order.order_type,
            price=order.price,
            stop_price=order.stop_price,
            exchange=exchange,
            time_in_force=order.time_in_force,
            reduce_only=order.reduce_only,
            post_only=order.post_only,
            leverage=order.leverage,
            margin_type=order.margin_type,
            params=order.params
        )
        
        # Set parent/child relationship
        child_order.set_as_child(order.id)
        order.add_child_order(child_order.id)
        
        # Register child order
        self._register_order(child_order)
        
        # Place child order
        success = await self._place_order_on_exchange(child_order, exchange)
        
        # Update parent order status based on child
        if success:
            order.status = OrderStatus.OPEN
            await self._notify_order_update(order)
            return True
        else:
            order.reject("Failed to place child orders")
            await self._notify_order_update(order)
            return False
    
    async def _refresh_order_status(self, order: Order) -> None:
        """
        Refresh order status from exchange.
        
        Args:
            order: Order to refresh
        """
        if not order.exchange or not order.exchange_order_id:
            return
            
        if order.exchange not in self.exchange_connectors:
            return
            
        try:
            connector = self.exchange_connectors[order.exchange]
            result = await connector.get_order_status(order.exchange_order_id, order.symbol)
            
            if result:
                # Update order with exchange data
                previous_status = order.status
                order.update(result)
                
                # Notify if status changed
                if order.status != previous_status:
                    await self._notify_order_update(order)
                    
                    # Save to repository
                    if self.order_repository:
                        await self.order_repository.save_order(order)
                        
        except Exception as e:
            self.logger.error(f"Error refreshing order {order.id} status: {e}")
    
    async def _monitor_orders(self) -> None:
        """Monitor order statuses periodically."""
        while True:
            try:
                # Get all non-terminal orders
                active_orders = [
                    order for order in self.orders_by_id.values()
                    if order.status not in [OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED, OrderStatus.EXPIRED]
                ]
                
                # Refresh status for each order
                for order in active_orders:
                    await self._refresh_order_status(order)
                    
                # Prevent excessive CPU usage
                await asyncio.sleep(0.01)
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in order monitoring: {e}")
                
            await asyncio.sleep(self.monitor_interval)
    
    async def _load_open_orders_from_repository(self) -> None:
        """Load open orders from repository."""
        if not self.order_repository:
            return
            
        try:
            # Load open orders
            orders = await self.order_repository.get_open_orders()
            
            for order in orders:
                self._register_order(order)
                
            self.logger.info(f"Loaded {len(orders)} open orders from repository")
        except Exception as e:
            self.logger.error(f"Error loading orders from repository: {e}")
    
    async def _load_open_orders_from_exchanges(self) -> None:
        """Load open orders from all exchanges."""
        for exchange, connector in self.exchange_connectors.items():
            try:
                exchange_orders = await connector.get_open_orders()
                
                for exchange_order in exchange_orders:
                    # Check if we already know this order
                    exchange_id = exchange_order.get('id')
                    symbol = exchange_order.get('symbol')
                    
                    if not exchange_id or not symbol:
                        continue
                        
                    key = (exchange, exchange_id)
                    
                    if key in self.orders_by_exchange_id:
                        # Update existing order
                        order = self.orders_by_exchange_id[key]
                        order.update(exchange_order)
                    else:
                        # Create new order from exchange data
                        order = Order(
                            symbol=symbol,
                            side=exchange_order.get('side', 'buy'),
                            amount=Decimal(str(exchange_order.get('amount', 0))),
                            price=Decimal(str(exchange_order.get('price', 0))) if exchange_order.get('price') else None,
                            order_type=exchange_order.get('type', 'limit'),
                            exchange=exchange,
                            exchange_order_id=exchange_id
                        )
                        
                        # Update with full exchange data
                        order.update(exchange_order)
                        
                        # Register order
                        self._register_order(order)
                        
                        # Save to repository
                        if self.order_repository:
                            await self.order_repository.save_order(order)
                            
                self.logger.info(f"Loaded {len(exchange_orders)} open orders from {exchange}")
                
            except Exception as e:
                self.logger.error(f"Error loading orders from {exchange}: {e}")
    
    def _register_order(self, order: Order) -> None:
        """
        Register order in tracking maps.
        
        Args:
            order: Order to register
        """
        # Store in primary map
        self.orders_by_id[order.id] = order
        
        # Store by exchange ID if available
        if order.exchange and order.exchange_order_id:
            key = (order.exchange, order.exchange_order_id)
            self.orders_by_exchange_id[key] = order
            
        # Store in filtered maps
        self.orders_by_symbol[order.symbol].append(order)
        self.orders_by_exchange[order.exchange].append(order)
        self.orders_by_status[order.status.value].append(order)
    
    async def _notify_order_update(self, order: Order) -> None:
        """
        Notify callbacks about order update.
        
        Args:
            order: Updated order
        """
        for callback in self.on_order_update_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(order)
                else:
                    callback(order)
            except Exception as e:
                self.logger.error(f"Error in order update callback: {e}")
                
    async def _notify_order_fill(self, order: Order, fill_data: Dict[str, Any]) -> None:
        """
        Notify callbacks about order fill.
        
        Args:
            order: Filled order
            fill_data: Fill data
        """
        # Update position tracking if position manager is available
        if self.position_manager:
            try:
                await self.position_manager.update_from_order(order, fill_data)
            except Exception as e:
                self.logger.error(f"Error updating position for order {order.id}: {e}")
        
        for callback in self.on_order_fill_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(order, fill_data)
                else:
                    callback(order, fill_data)
            except Exception as e:
                self.logger.error(f"Error in order fill callback: {e}")
