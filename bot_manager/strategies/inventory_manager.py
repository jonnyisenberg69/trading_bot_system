"""
Inventory Management System - Provides inventory tracking and coefficient calculation for the new strategy.

This module implements the inventory management logic described in NEW_BOT.txt:
- Inventory coefficient calculation: (excess inventory - target inventory) / target inventory
- Range: -1 (max short) to +1 (max long), 0 at target
- Inventory price tracking (manual or accounting method)
- Target inventory and max deviation management
"""

from dataclasses import dataclass, field
from decimal import Decimal
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Tuple
import logging
from enum import Enum

logger = logging.getLogger(__name__)


class InventoryPriceMethod(str, Enum):
    """Inventory price calculation methods."""
    MANUAL = "manual"  # User-provided manual inventory price
    ACCOUNTING = "accounting"  # Weighted average accounting method
    MARK_TO_MARKET = "mark_to_market"  # Use current market price


@dataclass
class InventoryConfig:
    """Configuration for inventory management."""
    target_inventory: Decimal  # Target inventory amount (signed)
    max_inventory_deviation: Decimal  # Maximum allowed deviation from target
    inventory_price_method: InventoryPriceMethod = InventoryPriceMethod.ACCOUNTING
    manual_inventory_price: Optional[Decimal] = None  # Used when method is MANUAL
    start_time: Optional[datetime] = None  # Start time for inventory calculations
    
    def __post_init__(self):
        """Validate inventory configuration."""
        if self.inventory_price_method == InventoryPriceMethod.MANUAL and self.manual_inventory_price is None:
            raise ValueError("Manual inventory price must be provided when using MANUAL price method")


@dataclass
class InventoryPosition:
    """Represents a position component for inventory calculation."""
    exchange: str
    base_amount: Decimal  # Base currency amount (signed: positive = long, negative = short)
    quote_amount: Decimal  # Quote currency amount (signed)
    avg_price: Optional[Decimal]  # Average price for this position
    last_updated: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass 
class InventoryState:
    """Current inventory state and calculated metrics."""
    current_inventory: Decimal  # Current total inventory (signed)
    target_inventory: Decimal  # Target inventory amount
    excess_inventory: Decimal  # Excess above target (signed)
    inventory_coefficient: Decimal  # Calculated coefficient (-1 to +1)
    inventory_price: Optional[Decimal]  # Current inventory price
    max_deviation: Decimal  # Maximum allowed deviation
    positions: Dict[str, InventoryPosition] = field(default_factory=dict)  # exchange -> position
    last_updated: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    def is_at_max_long(self) -> bool:
        """Check if inventory is at maximum long position."""
        return self.excess_inventory >= self.max_deviation
    
    def is_at_max_short(self) -> bool:
        """Check if inventory is at maximum short position."""
        return self.excess_inventory <= -self.max_deviation
    
    def get_capacity_long(self) -> Decimal:
        """Get remaining capacity for long positions."""
        return max(Decimal('0'), self.max_deviation - self.excess_inventory)
    
    def get_capacity_short(self) -> Decimal:
        """Get remaining capacity for short positions."""
        return max(Decimal('0'), self.excess_inventory + self.max_deviation)


class InventoryManager:
    """
    Inventory management system for the new strategy.
    
    Provides:
    - Real-time inventory tracking across exchanges
    - Inventory coefficient calculation
    - Position aggregation and risk metrics
    - Inventory price calculation using various methods
    """
    
    def __init__(self, config: InventoryConfig):
        self.config = config
        self.logger = logger
        
        # Position tracking
        self.positions: Dict[str, InventoryPosition] = {}  # exchange -> position
        self.inventory_state: Optional[InventoryState] = None
        
        # Historical tracking for accounting method
        self.trade_history: List[Dict[str, Any]] = []
        
        # Performance metrics
        self.coefficient_history: List[Tuple[datetime, Decimal]] = []
        self.last_update = datetime.now(timezone.utc)
        
    def update_position(
        self, 
        exchange: str, 
        side: str, 
        amount: Decimal, 
        price: Decimal,
        fee_cost: Optional[Decimal] = None,
        fee_currency: Optional[str] = None
    ) -> None:
        """
        Update position for a specific exchange based on a trade.
        
        Args:
            exchange: Exchange name
            side: 'buy' or 'sell'
            amount: Trade amount in base currency
            price: Trade price
            fee_cost: Optional fee cost
            fee_currency: Optional fee currency
        """
        if exchange not in self.positions:
            self.positions[exchange] = InventoryPosition(
                exchange=exchange,
                base_amount=Decimal('0'),
                quote_amount=Decimal('0'),
                avg_price=None
            )
        
        position = self.positions[exchange]
        old_base = position.base_amount
        old_quote = position.quote_amount
        
        # Update base and quote amounts
        if side == 'buy':
            position.base_amount += amount
            position.quote_amount -= amount * price
        else:  # sell
            position.base_amount -= amount
            position.quote_amount += amount * price
        
        # Handle fees
        if fee_cost and fee_currency:
            if fee_currency.upper() == self._get_base_currency():
                position.base_amount -= fee_cost
            elif fee_currency.upper() == self._get_quote_currency():
                position.quote_amount -= fee_cost
        
        # Update average price using weighted average
        if position.base_amount != 0:
            # Calculate average price from quote/base ratio
            position.avg_price = -position.quote_amount / position.base_amount
        else:
            position.avg_price = None
        
        position.last_updated = datetime.now(timezone.utc)
        
        # Add to trade history for accounting method
        self.trade_history.append({
            'timestamp': datetime.now(timezone.utc),
            'exchange': exchange,
            'side': side,
            'amount': amount,
            'price': price,
            'fee_cost': fee_cost,
            'fee_currency': fee_currency
        })
        
        # Limit trade history to last 10000 trades for performance
        if len(self.trade_history) > 10000:
            self.trade_history = self.trade_history[-10000:]
        
        self.logger.info(
            f"Updated {exchange} position: base={position.base_amount:.8f}, "
            f"quote={position.quote_amount:.8f}, avg_price={position.avg_price:.4f if position.avg_price else 'N/A'}"
        )
        
        # Recalculate inventory state
        self._calculate_inventory_state()
    
    def get_inventory_state(self) -> InventoryState:
        """Get current inventory state with calculated coefficient."""
        if self.inventory_state is None:
            self._calculate_inventory_state()
        return self.inventory_state
    
    def get_inventory_coefficient(self) -> Decimal:
        """Get current inventory coefficient (-1 to +1)."""
        state = self.get_inventory_state()
        return state.inventory_coefficient
    
    def get_spread_adjustment(self, base_spread_bps: Decimal, max_spread_bps: Decimal) -> Decimal:
        """
        Calculate spread adjustment based on inventory coefficient.
        
        Formula from NEW_BOT.txt:
        Coefficient * (max_spread - min_spread) + min_spread
        
        Args:
            base_spread_bps: Base/minimum spread in basis points
            max_spread_bps: Maximum spread in basis points
            
        Returns:
            Adjusted spread in basis points
        """
        coefficient = self.get_inventory_coefficient()
        spread_range = max_spread_bps - base_spread_bps
        adjusted_spread = coefficient * spread_range + base_spread_bps
        
        # Ensure spread doesn't go negative
        return max(adjusted_spread, Decimal('0'))
    
    def should_quote_side(self, side: str, inventory_price: Optional[Decimal], reference_price: Decimal, spread_bps: Decimal) -> bool:
        """
        Determine if we should quote on a specific side based on inventory logic.
        
        From NEW_BOT.txt:
        - If outbound price > inventory price + spread: offer
        - If outbound price < inventory price - spread: bid
        
        Args:
            side: 'bid' or 'offer'
            inventory_price: Current inventory price
            reference_price: Current reference/outbound price
            spread_bps: Spread in basis points
            
        Returns:
            True if we should quote this side, False otherwise
        """
        if inventory_price is None:
            # If no inventory price available, allow quoting
            return True
        
        spread_decimal = spread_bps / Decimal('10000')
        upper_bound = inventory_price * (Decimal('1') + spread_decimal)
        lower_bound = inventory_price * (Decimal('1') - spread_decimal)
        
        if side == 'offer' and reference_price > upper_bound:
            return True
        elif side == 'bid' and reference_price < lower_bound:
            return True
        
        return False
    
    def _calculate_inventory_state(self) -> None:
        """Calculate current inventory state and coefficient."""
        # Aggregate positions across all exchanges
        total_base = Decimal('0')
        total_quote = Decimal('0')
        
        for position in self.positions.values():
            total_base += position.base_amount
            total_quote += position.quote_amount
        
        # Calculate excess inventory (signed)
        target_inv = self.config.target_inventory if self.config.target_inventory is not None else Decimal('0')
        excess_inventory = total_base - target_inv
        
        # Calculate inventory coefficient
        # Formula: (excess inventory - target inventory) / target inventory
        # But since excess = current - target, this simplifies to:
        # excess / target_inventory
        # NEGATIVE TARGET SUPPORT: Use abs(target) to handle negative targets correctly
        # This ensures coefficient is positive when we're above target (need to sell)
        # and negative when we're below target (need to buy), regardless of target sign
        if target_inv != 0:
            coefficient = excess_inventory / abs(target_inv)
        else:
            coefficient = Decimal('0')
        
        # Clamp coefficient to [-1, +1] range
        coefficient = max(Decimal('-1'), min(Decimal('1'), coefficient))
        
        # Calculate inventory price based on method
        inventory_price = self._calculate_inventory_price()
        
        # Create inventory state
        self.inventory_state = InventoryState(
            current_inventory=total_base,
            target_inventory=target_inv,
            excess_inventory=excess_inventory,
            inventory_coefficient=coefficient,
            inventory_price=inventory_price,
            max_deviation=self.config.max_inventory_deviation,
            positions={exchange: position for exchange, position in self.positions.items()}
        )
        
        # Track coefficient history
        self.coefficient_history.append((datetime.now(timezone.utc), coefficient))
        # Keep last 1000 coefficient values
        if len(self.coefficient_history) > 1000:
            self.coefficient_history = self.coefficient_history[-1000:]
        
        self.last_update = datetime.now(timezone.utc)
        
        # Safe logging with None checks
        inventory_price_str = f"{float(inventory_price):.4f}" if inventory_price is not None else "N/A"
        
        self.logger.info(
            f"Inventory state updated: current={total_base:.8f}, target={float(target_inv):.8f}, "
            f"excess={excess_inventory:.8f}, coefficient={coefficient:.4f}, price={inventory_price_str}"
        )
    
    def _calculate_inventory_price(self) -> Optional[Decimal]:
        """Calculate inventory price based on configured method."""
        if self.config.inventory_price_method == InventoryPriceMethod.MANUAL:
            return self.config.manual_inventory_price
        
        elif self.config.inventory_price_method == InventoryPriceMethod.ACCOUNTING:
            # Weighted average price of current positions
            total_value = Decimal('0')
            total_quantity = Decimal('0')
            
            for position in self.positions.values():
                if position.base_amount != 0 and position.avg_price:
                    # Use absolute value for weighting but preserve direction
                    abs_amount = abs(position.base_amount)
                    total_value += abs_amount * position.avg_price
                    total_quantity += abs_amount
            
            if total_quantity > 0:
                return total_value / total_quantity
            else:
                return None
        
        elif self.config.inventory_price_method == InventoryPriceMethod.MARK_TO_MARKET:
            # This would use current market price - implementation depends on market data access
            # For now, return None and let calling code provide market price
            return None
        
        return None
    
    def _get_base_currency(self) -> str:
        """Get base currency from strategy symbol (simplified)."""
        # This would normally be injected or configured
        return "BERA"  # Default for now
    
    def _get_quote_currency(self) -> str:
        """Get quote currency from strategy symbol (simplified)."""
        return "USDT"  # Default for now
    
    def set_inventory_price(self, price: Decimal) -> None:
        """Manually set inventory price (for MARK_TO_MARKET method)."""
        if self.inventory_state:
            self.inventory_state.inventory_price = price
            self.logger.info(f"Inventory price manually set to {price:.4f}")
    
    def get_position_summary(self) -> Dict[str, Any]:
        """Get summary of all positions."""
        state = self.get_inventory_state()
        
        return {
            'current_inventory': float(state.current_inventory),
            'target_inventory': float(state.target_inventory),
            'excess_inventory': float(state.excess_inventory),
            'inventory_coefficient': float(state.inventory_coefficient),
            'inventory_price': float(state.inventory_price) if state.inventory_price else None,
            'max_deviation': float(state.max_deviation),
            'is_at_max_long': state.is_at_max_long(),
            'is_at_max_short': state.is_at_max_short(),
            'capacity_long': float(state.get_capacity_long()),
            'capacity_short': float(state.get_capacity_short()),
            'positions': {
                exchange: {
                    'base_amount': float(pos.base_amount),
                    'quote_amount': float(pos.quote_amount),
                    'avg_price': float(pos.avg_price) if pos.avg_price else None,
                    'last_updated': pos.last_updated.isoformat()
                }
                for exchange, pos in state.positions.items()
            },
            'last_updated': state.last_updated.isoformat()
        }
