"""
Order book data structure implementation.

Represents an exchange order book with bid and ask price levels, providing
methods for updating levels, calculating spreads, and querying best prices.
"""

from decimal import Decimal
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional, Any


class OrderBook:
    """
    Order book data structure maintaining bid and ask price levels.
    
    Features:
    - Maintains sorted bids and asks price levels
    - Updates price levels efficiently
    - Provides methods for best bid/ask, spread, mid price
    - Tracks sequence numbers for orderbook updates
    """
    
    def __init__(self, symbol: str, exchange: str):
        """
        Initialize order book for a symbol on an exchange.
        
        Args:
            symbol: Trading symbol (e.g., 'BTC/USDT')
            exchange: Exchange name (e.g., 'binance')
        """
        self.symbol = symbol
        self.exchange = exchange
        self.bids: Dict[Decimal, Decimal] = {}  # price -> amount
        self.asks: Dict[Decimal, Decimal] = {}  # price -> amount
        self.last_update = None
        self.sequence = 0
        self.snapshot_id = None  # For tracking snapshot/delta sequence
    
    def update_bid(self, price: Decimal, amount: Decimal) -> None:
        """
        Update a bid price level.
        
        Args:
            price: Bid price
            amount: Bid amount (0 to remove level)
        """
        if amount == 0:
            self.bids.pop(price, None)
        else:
            self.bids[price] = amount
        self.last_update = datetime.now(timezone.utc)
        self.sequence += 1
    
    def update_ask(self, price: Decimal, amount: Decimal) -> None:
        """
        Update an ask price level.
        
        Args:
            price: Ask price
            amount: Ask amount (0 to remove level)
        """
        if amount == 0:
            self.asks.pop(price, None)
        else:
            self.asks[price] = amount
        self.last_update = datetime.now(timezone.utc)
        self.sequence += 1
    
    def get_best_bid(self) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        """
        Get best (highest) bid price and amount.
        
        Returns:
            Tuple of (price, amount) or (None, None) if no bids
        """
        if not self.bids:
            return None, None
        best_price = max(self.bids.keys())
        return best_price, self.bids[best_price]
    
    def get_best_ask(self) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        """
        Get best (lowest) ask price and amount.
        
        Returns:
            Tuple of (price, amount) or (None, None) if no asks
        """
        if not self.asks:
            return None, None
        best_price = min(self.asks.keys())
        return best_price, self.asks[best_price]
    
    def get_spread(self) -> Optional[Decimal]:
        """
        Get bid-ask spread.
        
        Returns:
            Spread (ask - bid) or None if not available
        """
        best_bid, _ = self.get_best_bid()
        best_ask, _ = self.get_best_ask()
        
        if best_bid is None or best_ask is None:
            return None
        
        return best_ask - best_bid
    
    def get_mid_price(self) -> Optional[Decimal]:
        """
        Get mid price (average of best bid and ask).
        
        Returns:
            Mid price or None if not available
        """
        best_bid, _ = self.get_best_bid()
        best_ask, _ = self.get_best_ask()
        
        if best_bid is None or best_ask is None:
            return None
        
        return (best_bid + best_ask) / 2
    
    def get_depth(self, levels: int = 10000) -> Dict[str, List[Tuple[Decimal, Decimal]]]:
        """
        Get order book depth with sorted price levels.
        
        Args:
            levels: Number of price levels to return on each side
            
        Returns:
            Dictionary with sorted bids and asks
        """
        sorted_bids = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)
        sorted_asks = sorted(self.asks.items(), key=lambda x: x[0])
        
        return {
            'bids': sorted_bids[:levels],
            'asks': sorted_asks[:levels]
        }
    
    def clear(self) -> None:
        """Clear all price levels."""
        self.bids.clear()
        self.asks.clear()
        self.last_update = datetime.now(timezone.utc)
        self.sequence += 1
    
    def is_valid(self) -> bool:
        """
        Check if order book is valid (best bid < best ask).
        
        Returns:
            True if valid, False if crossed
        """
        best_bid, _ = self.get_best_bid()
        best_ask, _ = self.get_best_ask()
        
        if best_bid is None or best_ask is None:
            return True
        
        return best_bid < best_ask
    
    def update_from_snapshot(self, data: Dict[str, Any]) -> None:
        """
        Update order book from a snapshot.
        
        Args:
            data: Snapshot data with bids and asks
        """
        # Clear existing data
        self.clear()
        
        # Save snapshot ID if available
        if 'lastUpdateId' in data:
            self.snapshot_id = data['lastUpdateId']
        
        # Process bids
        if 'bids' in data:
            for price_str, amount_str in data['bids']:
                price = Decimal(str(price_str))
                amount = Decimal(str(amount_str))
                if amount > 0:
                    self.bids[price] = amount
        
        # Process asks
        if 'asks' in data:
            for price_str, amount_str in data['asks']:
                price = Decimal(str(price_str))
                amount = Decimal(str(amount_str))
                if amount > 0:
                    self.asks[price] = amount
        
        self.last_update = datetime.now(timezone.utc)
    
    def apply_delta(self, data: Dict[str, Any]) -> bool:
        """
        Apply a delta update to the order book.
        
        Args:
            data: Delta update data with bids and asks changes
            
        Returns:
            True if update successfully applied, False otherwise
        """
        if self.snapshot_id is None and 'U' in data and 'u' in data:
            # Can't apply delta without snapshot
            return False
        
        # Check sequence continuity if applicable
        if 'U' in data and 'u' in data and self.snapshot_id is not None:
            first_update_id = data['U']
            last_update_id = data['u']
            
            if first_update_id <= self.snapshot_id + 1 <= last_update_id:
                # Update is consistent with our snapshot
                self.snapshot_id = last_update_id
            else:
                # Update is not consistent, need new snapshot
                return False
        
        # Apply bid updates
        if 'b' in data:
            for price_str, amount_str in data['b']:
                price = Decimal(str(price_str))
                amount = Decimal(str(amount_str))
                self.update_bid(price, amount)
        
        # Apply ask updates
        if 'a' in data:
            for price_str, amount_str in data['a']:
                price = Decimal(str(price_str))
                amount = Decimal(str(amount_str))
                self.update_ask(price, amount)
        
        self.last_update = datetime.now(timezone.utc)
        return True
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert order book to dictionary.
        
        Returns:
            Dictionary representation of the order book
        """
        return {
            'symbol': self.symbol,
            'exchange': self.exchange,
            # Output all bids and asks, fully sorted
            'bids': [[str(p), str(a)] for p, a in sorted(self.bids.items(), key=lambda x: x[0], reverse=True)],
            'asks': [[str(p), str(a)] for p, a in sorted(self.asks.items(), key=lambda x: x[0])],
            'sequence': self.sequence,
            'snapshot_id': self.snapshot_id,
            'timestamp': int(datetime.now(timezone.utc).timestamp() * 1000)
        }
    
    def __str__(self) -> str:
        """String representation of the order book."""
        best_bid, bid_amount = self.get_best_bid() or (None, None)
        best_ask, ask_amount = self.get_best_ask() or (None, None)
        spread = self.get_spread()
        mid = self.get_mid_price()
        
        return (
            f"OrderBook({self.symbol} @ {self.exchange}) - "
            f"Best bid: {best_bid}({bid_amount}), "
            f"Best ask: {best_ask}({ask_amount}), "
            f"Spread: {spread}, Mid: {mid}, "
            f"Depth: {len(self.bids)}/{len(self.asks)}, "
            f"Seq: {self.sequence}"
        )
