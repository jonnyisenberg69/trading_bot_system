"""
Order model for trading system.

Provides a unified interface for orders across different exchanges.
"""

import uuid
from enum import Enum
from decimal import Decimal
from typing import Dict, Optional, Any, List
from datetime import datetime, timezone


class OrderSide(str, Enum):
    """Order side enum."""
    BUY = 'buy'
    SELL = 'sell'


class OrderType(str, Enum):
    """Order type enum."""
    LIMIT = 'limit'
    MARKET = 'market'
    STOP_LIMIT = 'stop_limit'
    STOP_MARKET = 'stop_market'
    TRAILING_STOP = 'trailing_stop'
    POST_ONLY = 'post_only'
    FILL_OR_KILL = 'fill_or_kill'
    IMMEDIATE_OR_CANCEL = 'ioc'


class OrderStatus(str, Enum):
    """Order status enum."""
    PENDING = 'pending'
    OPEN = 'open'
    FILLED = 'filled'
    PARTIALLY_FILLED = 'partially_filled'
    CANCELLED = 'cancelled'
    REJECTED = 'rejected'
    EXPIRED = 'expired'


class OrderTimeInForce(str, Enum):
    """Time in force enum."""
    GTC = 'gtc'  # Good Till Cancelled
    IOC = 'ioc'  # Immediate Or Cancel
    FOK = 'fok'  # Fill Or Kill
    GTD = 'gtd'  # Good Till Date


class Order:
    """
    Unified order model for all exchanges.
    
    Tracks all order information including:
    - Order specifications (symbol, side, amount, etc.)
    - Exchange identifiers
    - Order state and history
    - Execution details
    """
    
    def __init__(
        self,
        symbol: str,
        side: OrderSide,
        amount: Decimal,
        order_type: OrderType = OrderType.LIMIT,
        price: Optional[Decimal] = None,
        stop_price: Optional[Decimal] = None,
        exchange: Optional[str] = None,
        exchange_order_id: Optional[str] = None,
        client_order_id: Optional[str] = None,
        time_in_force: OrderTimeInForce = OrderTimeInForce.GTC,
        reduce_only: bool = False,
        post_only: bool = False,
        leverage: Optional[int] = None,
        margin_type: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize a new order.
        
        Args:
            symbol: Trading symbol (e.g., 'BTC/USDT')
            side: Order side (buy/sell)
            amount: Order amount in base currency
            order_type: Type of order (limit, market, etc.)
            price: Order price (required for limit orders)
            stop_price: Stop price (for stop orders)
            exchange: Exchange to place the order on
            exchange_order_id: Order ID assigned by exchange
            client_order_id: Custom order ID for tracking
            time_in_force: Time in force setting
            reduce_only: Whether order is reduce-only
            post_only: Whether order is post-only
            leverage: Leverage for margin/futures orders
            margin_type: Margin type for futures orders
            params: Additional exchange-specific parameters
        """
        # Order identification
        self.id = client_order_id or str(uuid.uuid4())
        self.exchange_order_id = exchange_order_id
        self.exchange = exchange
        
        # Order specification
        self.symbol = symbol
        self.side = side if isinstance(side, OrderSide) else OrderSide(side)
        self.order_type = order_type if isinstance(order_type, OrderType) else OrderType(order_type)
        self.amount = amount
        self.price = price
        self.stop_price = stop_price
        self.time_in_force = time_in_force if isinstance(time_in_force, OrderTimeInForce) else OrderTimeInForce(time_in_force)
        self.reduce_only = reduce_only
        self.post_only = post_only
        self.leverage = leverage
        self.margin_type = margin_type
        self.params = params or {}
        
        # Order state
        self.status = OrderStatus.PENDING
        self.filled_amount = Decimal('0')
        self.remaining_amount = amount
        self.avg_fill_price = None
        self.cost = Decimal('0')
        self.fee = Decimal('0')
        self.fee_currency = None
        
        # Timestamps
        self.created_at = datetime.now(timezone.utc)
        self.updated_at = self.created_at
        self.filled_at = None
        self.cancelled_at = None
        
        # Order history
        self.status_history = [{
            'status': self.status.value,
            'timestamp': self.created_at,
            'message': 'Order created'
        }]
        
        # Execution details
        self.fills = []
        
        # Routing details for multi-exchange orders
        self.child_orders = []
        self.parent_order_id = None
        self.is_smart_routed = False
        
    def update(self, exchange_order: Dict[str, Any]) -> None:
        """
        Update order with exchange data.
        
        Args:
            exchange_order: Order data from exchange
        """
        if 'id' in exchange_order and not self.exchange_order_id:
            self.exchange_order_id = exchange_order['id']
            
        if 'status' in exchange_order:
            new_status = OrderStatus(exchange_order['status'])
            if new_status != self.status:
                self._update_status(new_status, f"Status changed from {self.status.value} to {new_status.value}")
                
        if 'filled' in exchange_order:
            self.filled_amount = Decimal(str(exchange_order['filled']))
            self.remaining_amount = self.amount - self.filled_amount
            
        if 'price' in exchange_order and exchange_order['price']:
            self.avg_fill_price = Decimal(str(exchange_order['price']))
            
        if 'cost' in exchange_order and exchange_order['cost']:
            self.cost = Decimal(str(exchange_order['cost']))
            
        if 'fee' in exchange_order and exchange_order['fee']:
            if 'cost' in exchange_order['fee'] and exchange_order['fee']['cost']:
                self.fee = Decimal(str(exchange_order['fee']['cost']))
            if 'currency' in exchange_order['fee']:
                self.fee_currency = exchange_order['fee']['currency']
                
        # Update timestamp
        self.updated_at = datetime.now(timezone.utc)
        
        # Check if order is filled
        if self.status == OrderStatus.FILLED and not self.filled_at:
            self.filled_at = self.updated_at
            
    def cancel(self, reason: str = "User requested cancellation") -> None:
        """
        Mark order as cancelled.
        
        Args:
            reason: Cancellation reason
        """
        self._update_status(OrderStatus.CANCELLED, reason)
        self.cancelled_at = datetime.now(timezone.utc)
        
    def reject(self, reason: str) -> None:
        """
        Mark order as rejected.
        
        Args:
            reason: Rejection reason
        """
        self._update_status(OrderStatus.REJECTED, reason)
        
    def add_fill(self, fill_data: Dict[str, Any]) -> None:
        """
        Add fill to order.
        
        Args:
            fill_data: Fill data from exchange
        """
        fill = {
            'timestamp': datetime.now(timezone.utc),
            'amount': Decimal(str(fill_data.get('amount', 0))),
            'price': Decimal(str(fill_data.get('price', 0))),
            'cost': Decimal(str(fill_data.get('cost', 0))),
            'fee': Decimal(str(fill_data.get('fee', {}).get('cost', 0))),
            'fee_currency': fill_data.get('fee', {}).get('currency')
        }
        
        self.fills.append(fill)
        
        # Update filled amount
        self.filled_amount += fill['amount']
        self.remaining_amount = self.amount - self.filled_amount
        
        # Calculate average fill price
        total_cost = sum(f['price'] * f['amount'] for f in self.fills)
        self.avg_fill_price = total_cost / self.filled_amount if self.filled_amount > 0 else None
        
        # Update order status
        if self.remaining_amount > 0:
            self._update_status(OrderStatus.PARTIALLY_FILLED, f"Partially filled: {self.filled_amount}/{self.amount}")
        else:
            self._update_status(OrderStatus.FILLED, f"Completely filled at average price {self.avg_fill_price}")
            self.filled_at = datetime.now(timezone.utc)
            
    def add_child_order(self, child_order_id: str) -> None:
        """
        Add child order to this order (for smart routing).
        
        Args:
            child_order_id: ID of child order
        """
        self.is_smart_routed = True
        self.child_orders.append(child_order_id)
        
    def set_as_child(self, parent_order_id: str) -> None:
        """
        Mark this order as a child of a parent order.
        
        Args:
            parent_order_id: ID of parent order
        """
        self.parent_order_id = parent_order_id
        
    def _update_status(self, status: OrderStatus, message: str = "") -> None:
        """
        Update order status and add to history.
        
        Args:
            status: New status
            message: Status update message
        """
        self.status = status
        self.updated_at = datetime.now(timezone.utc)
        self.status_history.append({
            'status': status.value,
            'timestamp': self.updated_at,
            'message': message
        })
        
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert order to dictionary for serialization.
        
        Returns:
            Dictionary representation of order
        """
        return {
            'id': self.id,
            'exchange_order_id': self.exchange_order_id,
            'exchange': self.exchange,
            'symbol': self.symbol,
            'side': self.side.value,
            'order_type': self.order_type.value,
            'amount': float(self.amount),
            'price': float(self.price) if self.price else None,
            'stop_price': float(self.stop_price) if self.stop_price else None,
            'time_in_force': self.time_in_force.value,
            'reduce_only': self.reduce_only,
            'post_only': self.post_only,
            'leverage': self.leverage,
            'margin_type': self.margin_type,
            'status': self.status.value,
            'filled_amount': float(self.filled_amount),
            'remaining_amount': float(self.remaining_amount),
            'avg_fill_price': float(self.avg_fill_price) if self.avg_fill_price else None,
            'cost': float(self.cost) if self.cost else 0,
            'fee': float(self.fee) if self.fee else 0,
            'fee_currency': self.fee_currency,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat(),
            'filled_at': self.filled_at.isoformat() if self.filled_at else None,
            'cancelled_at': self.cancelled_at.isoformat() if self.cancelled_at else None,
            'status_history': self.status_history,
            'is_smart_routed': self.is_smart_routed,
            'child_orders': self.child_orders,
            'parent_order_id': self.parent_order_id
        }
        
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Order':
        """
        Create order from dictionary.
        
        Args:
            data: Dictionary data
            
        Returns:
            Order instance
        """
        order = cls(
            symbol=data['symbol'],
            side=OrderSide(data['side']),
            amount=Decimal(str(data['amount'])),
            order_type=OrderType(data['order_type']),
            price=Decimal(str(data['price'])) if data.get('price') else None,
            stop_price=Decimal(str(data['stop_price'])) if data.get('stop_price') else None,
            exchange=data.get('exchange'),
            exchange_order_id=data.get('exchange_order_id'),
            client_order_id=data.get('id'),
            time_in_force=OrderTimeInForce(data.get('time_in_force', 'gtc')),
            reduce_only=data.get('reduce_only', False),
            post_only=data.get('post_only', False),
            leverage=data.get('leverage'),
            margin_type=data.get('margin_type')
        )
        
        # Restore state
        order.status = OrderStatus(data['status'])
        order.filled_amount = Decimal(str(data['filled_amount']))
        order.remaining_amount = Decimal(str(data['remaining_amount']))
        order.avg_fill_price = Decimal(str(data['avg_fill_price'])) if data.get('avg_fill_price') else None
        order.cost = Decimal(str(data['cost'])) if data.get('cost') else Decimal('0')
        order.fee = Decimal(str(data['fee'])) if data.get('fee') else Decimal('0')
        order.fee_currency = data.get('fee_currency')
        
        # Restore timestamps
        order.created_at = datetime.fromisoformat(data['created_at'])
        order.updated_at = datetime.fromisoformat(data['updated_at'])
        order.filled_at = datetime.fromisoformat(data['filled_at']) if data.get('filled_at') else None
        order.cancelled_at = datetime.fromisoformat(data['cancelled_at']) if data.get('cancelled_at') else None
        
        # Restore history
        order.status_history = data.get('status_history', [])
        
        # Restore routing info
        order.is_smart_routed = data.get('is_smart_routed', False)
        order.child_orders = data.get('child_orders', [])
        order.parent_order_id = data.get('parent_order_id')
        
        return order
