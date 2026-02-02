"""
Orderbook aggregator for combining multiple exchange orderbooks.

Provides functionality to combine orderbooks across different exchanges and market types
(spot, perpetual) to create a unified view of the market.
"""

from decimal import Decimal
from typing import Dict, List, Optional, Set, Tuple, Any
import logging
from datetime import datetime, timezone
import statistics

from .orderbook import OrderBook

logger = logging.getLogger(__name__)


class OrderbookAggregator:
    """
    Aggregates orderbooks across exchanges and market types.
    
    Features:
    - Combines orderbooks from multiple exchanges
    - Provides different views: spot only, perpetual only, or all combined
    - Calculates best prices, spreads, and liquidity metrics
    """
    
    def __init__(self):
        """Initialize orderbook aggregator."""
        # References to orderbooks managed by OrderbookManager
        self.exchange_orderbooks: Dict[str, Dict[str, OrderBook]] = {}
        
        # Exchange metadata
        self.exchange_market_types: Dict[str, str] = {}  # exchange -> market_type
        
        # Combined orderbooks by symbol
        self.combined_orderbooks: Dict[str, Dict[str, OrderBook]] = {
            'spot': {},      # symbol -> OrderBook (spot exchanges only)
            'futures': {},   # symbol -> OrderBook (futures exchanges only)
            'all': {}        # symbol -> OrderBook (all exchanges)
        }
        
        # Timestamps of last update
        self.last_update_time: Dict[str, Dict[str, float]] = {
            'spot': {},
            'futures': {},
            'all': {}
        }
        
        self.logger = logging.getLogger(f"{__name__}.OrderbookAggregator")
    
    def register_exchange_orderbooks(self, exchange_orderbooks: Dict[str, Dict[str, OrderBook]], 
                                   exchange_market_types: Dict[str, str]):
        """
        Register exchange orderbooks for aggregation.
        
        Args:
            exchange_orderbooks: Dictionary of exchange -> symbol -> OrderBook
            exchange_market_types: Dictionary of exchange -> market_type
        """
        self.exchange_orderbooks = exchange_orderbooks
        self.exchange_market_types = exchange_market_types
        self.logger.info(f"Registered orderbooks for {len(exchange_orderbooks)} exchanges")
    
    def update_combined_orderbooks(self, symbol: str):
        """
        Update combined orderbooks for a symbol.
        
        Args:
            symbol: Trading symbol
        """
        # Get all exchanges with this symbol
        spot_orderbooks = []
        futures_orderbooks = []
        all_orderbooks = []
        
        for exchange, orderbooks in self.exchange_orderbooks.items():
            if symbol in orderbooks:
                orderbook = orderbooks[symbol]
                all_orderbooks.append(orderbook)
                
                # Categorize by market type
                market_type = self.exchange_market_types.get(exchange, 'spot')
                if market_type in ['futures', 'perpetual', 'future', 'perp']:
                    futures_orderbooks.append(orderbook)
                else:
                    spot_orderbooks.append(orderbook)
        
        # Update spot combined orderbook
        if spot_orderbooks:
            if symbol not in self.combined_orderbooks['spot']:
                self.combined_orderbooks['spot'][symbol] = OrderBook(symbol, 'combined_spot')
            
            self._combine_orderbooks(spot_orderbooks, self.combined_orderbooks['spot'][symbol])
            self.last_update_time['spot'][symbol] = datetime.now(timezone.utc).timestamp()
        
        # Update futures combined orderbook
        if futures_orderbooks:
            if symbol not in self.combined_orderbooks['futures']:
                self.combined_orderbooks['futures'][symbol] = OrderBook(symbol, 'combined_futures')
            
            self._combine_orderbooks(futures_orderbooks, self.combined_orderbooks['futures'][symbol])
            self.last_update_time['futures'][symbol] = datetime.now(timezone.utc).timestamp()
        
        # Update all combined orderbook
        if all_orderbooks:
            if symbol not in self.combined_orderbooks['all']:
                self.combined_orderbooks['all'][symbol] = OrderBook(symbol, 'combined_all')
            
            self._combine_orderbooks(all_orderbooks, self.combined_orderbooks['all'][symbol])
            self.last_update_time['all'][symbol] = datetime.now(timezone.utc).timestamp()
    
    def _combine_orderbooks(self, source_orderbooks: List[OrderBook], target_orderbook: OrderBook):
        """
        Combine multiple orderbooks into a single orderbook.
        
        Args:
            source_orderbooks: List of source orderbooks
            target_orderbook: Target orderbook to update
        """
        # Clear target orderbook
        target_orderbook.clear()
        
        # Combine bids
        all_bids = {}
        for orderbook in source_orderbooks:
            for price, amount in orderbook.bids.items():
                if price in all_bids:
                    all_bids[price] += amount
                else:
                    all_bids[price] = amount
        
        # Combine asks
        all_asks = {}
        for orderbook in source_orderbooks:
            for price, amount in orderbook.asks.items():
                if price in all_asks:
                    all_asks[price] += amount
                else:
                    all_asks[price] = amount
        
        # Update target orderbook
        for price, amount in all_bids.items():
            target_orderbook.update_bid(price, amount)
        
        for price, amount in all_asks.items():
            target_orderbook.update_ask(price, amount)
        
        # Update sequence number
        target_orderbook.sequence = max([ob.sequence for ob in source_orderbooks]) if source_orderbooks else 0
    
    def get_combined_orderbook(self, symbol: str, market_type: Optional[str] = None) -> Optional[OrderBook]:
        """
        Get combined orderbook for a symbol.
        
        Args:
            symbol: Trading symbol
            market_type: Market type ('spot', 'futures', or None for all)
            
        Returns:
            Combined OrderBook or None if not available
        """
        if market_type is None:
            market_type = 'all'
        
        market_type = market_type.lower()
        if market_type not in self.combined_orderbooks:
            return None
        
        # Update combined orderbook if it exists
        if symbol in self.combined_orderbooks[market_type]:
            self.update_combined_orderbooks(symbol)
            return self.combined_orderbooks[market_type][symbol]
        
        return None
    
    def get_best_price_exchange(self, symbol: str, side: str, market_type: Optional[str] = None) -> Tuple[Optional[str], Optional[Decimal], Optional[Decimal]]:
        """
        Find exchange with the best price for a symbol.
        
        Args:
            symbol: Trading symbol
            side: 'bid' or 'ask'
            market_type: Market type ('spot', 'futures', or None for all)
            
        Returns:
            Tuple of (exchange, price, amount) or (None, None, None) if not found
        """
        best_exchange = None
        best_price = None
        best_amount = None
        
        # Filter exchanges by market type
        exchanges = self.exchange_orderbooks.keys()
        if market_type:
            exchanges = [ex for ex in exchanges if self.exchange_market_types.get(ex, 'spot') == market_type]
        
        for exchange in exchanges:
            if symbol in self.exchange_orderbooks.get(exchange, {}):
                orderbook = self.exchange_orderbooks[exchange][symbol]
                
                if side.lower() == 'bid':
                    price, amount = orderbook.get_best_bid()
                    if price is not None and (best_price is None or price > best_price):
                        best_price = price
                        best_amount = amount
                        best_exchange = exchange
                else:  # ask
                    price, amount = orderbook.get_best_ask()
                    if price is not None and (best_price is None or price < best_price):
                        best_price = price
                        best_amount = amount
                        best_exchange = exchange
        
        return best_exchange, best_price, best_amount
    
    def get_aggregated_price(self, symbol: str, side: str, market_type: Optional[str] = None) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        """
        Get volume-weighted average price across exchanges.
        
        Args:
            symbol: Trading symbol
            side: 'bid' or 'ask'
            market_type: Market type ('spot', 'futures', or None for all)
            
        Returns:
            Tuple of (price, amount) or (None, None) if not available
        """
        prices = []
        amounts = []
        
        # Filter exchanges by market type
        exchanges = self.exchange_orderbooks.keys()
        if market_type:
            exchanges = [ex for ex in exchanges if self.exchange_market_types.get(ex, 'spot') == market_type]
        
        for exchange in exchanges:
            if symbol in self.exchange_orderbooks.get(exchange, {}):
                orderbook = self.exchange_orderbooks[exchange][symbol]
                
                if side.lower() == 'bid':
                    price, amount = orderbook.get_best_bid()
                    if price is not None and amount is not None:
                        prices.append(float(price))
                        amounts.append(float(amount))
                else:  # ask
                    price, amount = orderbook.get_best_ask()
                    if price is not None and amount is not None:
                        prices.append(float(price))
                        amounts.append(float(amount))
        
        if not prices:
            return None, None
        
        # Calculate weighted average price
        if sum(amounts) > 0:
            weighted_price = sum(p * a for p, a in zip(prices, amounts)) / sum(amounts)
            total_amount = sum(amounts)
            return Decimal(str(weighted_price)), Decimal(str(total_amount))
        
        # Fallback to regular average if no amounts
        return Decimal(str(statistics.mean(prices))), Decimal('0')
    
    def get_market_depth(self, symbol: str, depth_pct: float = 0.01, market_type: Optional[str] = None) -> Dict[str, Any]:
        """
        Get market depth for a symbol.
        
        Args:
            symbol: Trading symbol
            depth_pct: Depth percentage from mid price (e.g., 0.01 for 1%)
            market_type: Market type ('spot', 'futures', or None for all)
            
        Returns:
            Dictionary with market depth information
        """
        orderbook = self.get_combined_orderbook(symbol, market_type)
        if not orderbook:
            return {}
        
        # Get mid price
        mid_price = orderbook.get_mid_price()
        if not mid_price:
            return {}
        
        # Calculate depth range
        lower_bound = mid_price * (1 - Decimal(str(depth_pct)))
        upper_bound = mid_price * (1 + Decimal(str(depth_pct)))
        
        # Calculate total volume within range
        bid_volume = Decimal('0')
        ask_volume = Decimal('0')
        
        for price, amount in orderbook.bids.items():
            if price >= lower_bound:
                bid_volume += amount
        
        for price, amount in orderbook.asks.items():
            if price <= upper_bound:
                ask_volume += amount
        
        return {
            'symbol': symbol,
            'mid_price': float(mid_price),
            'lower_bound': float(lower_bound),
            'upper_bound': float(upper_bound),
            'bid_volume': float(bid_volume),
            'ask_volume': float(ask_volume),
            'total_volume': float(bid_volume + ask_volume),
            'imbalance': float((bid_volume - ask_volume) / (bid_volume + ask_volume)) if (bid_volume + ask_volume) > 0 else 0
        }
    
    def get_market_summary(self, symbol: str) -> Dict[str, Any]:
        """
        Get market summary for a symbol across different market types.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            Dictionary with market summary information
        """
        # Get combined orderbooks
        spot_orderbook = self.get_combined_orderbook(symbol, 'spot')
        futures_orderbook = self.get_combined_orderbook(symbol, 'futures')
        all_orderbook = self.get_combined_orderbook(symbol, 'all')
        
        result = {
            'symbol': symbol,
            'timestamp': datetime.now(timezone.utc).timestamp(),
            'datetime': datetime.now(timezone.utc).isoformat(),
            'spot': self._get_orderbook_summary(spot_orderbook) if spot_orderbook else None,
            'futures': self._get_orderbook_summary(futures_orderbook) if futures_orderbook else None,
            'all': self._get_orderbook_summary(all_orderbook) if all_orderbook else None,
            'exchanges': {}
        }
        
        # Add exchange-specific data
        for exchange, orderbooks in self.exchange_orderbooks.items():
            if symbol in orderbooks:
                orderbook = orderbooks[symbol]
                result['exchanges'][exchange] = self._get_orderbook_summary(orderbook)
        
        return result
    
    def _get_orderbook_summary(self, orderbook: OrderBook) -> Dict[str, Any]:
        """
        Get summary information for an orderbook.
        
        Args:
            orderbook: OrderBook instance
            
        Returns:
            Dictionary with orderbook summary
        """
        best_bid, bid_amount = orderbook.get_best_bid() or (None, None)
        best_ask, ask_amount = orderbook.get_best_ask() or (None, None)
        spread = orderbook.get_spread()
        mid_price = orderbook.get_mid_price()
        
        return {
            'best_bid': float(best_bid) if best_bid else None,
            'best_ask': float(best_ask) if best_ask else None,
            'bid_amount': float(bid_amount) if bid_amount else None,
            'ask_amount': float(ask_amount) if ask_amount else None,
            'spread': float(spread) if spread else None,
            'mid_price': float(mid_price) if mid_price else None,
            'spread_pct': float(spread / mid_price * 100) if spread and mid_price else None,
            'bid_levels': len(orderbook.bids),
            'ask_levels': len(orderbook.asks),
            'sequence': orderbook.sequence
        }
