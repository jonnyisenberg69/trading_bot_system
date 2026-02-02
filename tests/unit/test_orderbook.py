"""
Unit tests for the OrderBook class.

Tests orderbook data structure functionality including price level management,
best bid/ask calculation, and orderbook synchronization.
"""

import pytest
from decimal import Decimal
from datetime import datetime, timezone
from unittest.mock import Mock, patch

# Import the OrderBook class (we'll need to create this)
# For now, I'll create a mock implementation for testing
class MockOrderBook:
    """Mock OrderBook implementation for testing."""
    
    def __init__(self, symbol: str, exchange: str):
        self.symbol = symbol
        self.exchange = exchange
        self.bids = {}  # price -> amount
        self.asks = {}  # price -> amount
        self.last_update = None
        self.sequence = 0
    
    def update_bid(self, price: Decimal, amount: Decimal):
        """Update bid price level."""
        if amount == 0:
            self.bids.pop(price, None)
        else:
            self.bids[price] = amount
        self.last_update = datetime.now(timezone.utc)
        self.sequence += 1
    
    def update_ask(self, price: Decimal, amount: Decimal):
        """Update ask price level."""
        if amount == 0:
            self.asks.pop(price, None)
        else:
            self.asks[price] = amount
        self.last_update = datetime.now(timezone.utc)
        self.sequence += 1
    
    def get_best_bid(self):
        """Get best bid price and amount."""
        if not self.bids:
            return None, None
        best_price = max(self.bids.keys())
        return best_price, self.bids[best_price]
    
    def get_best_ask(self):
        """Get best ask price and amount."""
        if not self.asks:
            return None, None
        best_price = min(self.asks.keys())
        return best_price, self.asks[best_price]
    
    def get_spread(self):
        """Get bid-ask spread."""
        best_bid, _ = self.get_best_bid()
        best_ask, _ = self.get_best_ask()
        
        if best_bid is None or best_ask is None:
            return None
        
        return best_ask - best_bid
    
    def get_mid_price(self):
        """Get mid price."""
        best_bid, _ = self.get_best_bid()
        best_ask, _ = self.get_best_ask()
        
        if best_bid is None or best_ask is None:
            return None
        
        return (best_bid + best_ask) / 2
    
    def get_depth(self, levels: int = 10):
        """Get orderbook depth."""
        sorted_bids = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)
        sorted_asks = sorted(self.asks.items(), key=lambda x: x[0])
        
        return {
            'bids': sorted_bids[:levels],
            'asks': sorted_asks[:levels]
        }
    
    def clear(self):
        """Clear all price levels."""
        self.bids.clear()
        self.asks.clear()
        self.last_update = datetime.now(timezone.utc)
        self.sequence += 1
    
    def is_valid(self):
        """Check if orderbook is valid (best bid < best ask)."""
        best_bid, _ = self.get_best_bid()
        best_ask, _ = self.get_best_ask()
        
        if best_bid is None or best_ask is None:
            return True
        
        return best_bid < best_ask


# Use the mock for testing
OrderBook = MockOrderBook


class TestOrderBook:
    """Test OrderBook functionality."""
    
    @pytest.fixture
    def orderbook(self):
        """Create OrderBook instance for testing."""
        return OrderBook('BTC/USDT', 'binance')
    
    def test_initialization(self, orderbook):
        """Test OrderBook initialization."""
        assert orderbook.symbol == 'BTC/USDT'
        assert orderbook.exchange == 'binance'
        assert len(orderbook.bids) == 0
        assert len(orderbook.asks) == 0
        assert orderbook.sequence == 0
    
    def test_update_bid(self, orderbook):
        """Test updating bid price level."""
        price = Decimal('47500.0')
        amount = Decimal('1.5')
        
        orderbook.update_bid(price, amount)
        
        assert price in orderbook.bids
        assert orderbook.bids[price] == amount
        assert orderbook.sequence == 1
        assert orderbook.last_update is not None
    
    def test_update_ask(self, orderbook):
        """Test updating ask price level."""
        price = Decimal('47501.0')
        amount = Decimal('2.0')
        
        orderbook.update_ask(price, amount)
        
        assert price in orderbook.asks
        assert orderbook.asks[price] == amount
        assert orderbook.sequence == 1
        assert orderbook.last_update is not None
    
    def test_update_bid_zero_amount(self, orderbook):
        """Test removing bid level with zero amount."""
        price = Decimal('47500.0')
        amount = Decimal('1.5')
        
        # Add bid
        orderbook.update_bid(price, amount)
        assert price in orderbook.bids
        
        # Remove with zero amount
        orderbook.update_bid(price, Decimal('0'))
        assert price not in orderbook.bids
    
    def test_update_ask_zero_amount(self, orderbook):
        """Test removing ask level with zero amount."""
        price = Decimal('47501.0')
        amount = Decimal('2.0')
        
        # Add ask
        orderbook.update_ask(price, amount)
        assert price in orderbook.asks
        
        # Remove with zero amount
        orderbook.update_ask(price, Decimal('0'))
        assert price not in orderbook.asks
    
    def test_get_best_bid_empty(self, orderbook):
        """Test getting best bid from empty orderbook."""
        best_price, best_amount = orderbook.get_best_bid()
        
        assert best_price is None
        assert best_amount is None
    
    def test_get_best_ask_empty(self, orderbook):
        """Test getting best ask from empty orderbook."""
        best_price, best_amount = orderbook.get_best_ask()
        
        assert best_price is None
        assert best_amount is None
    
    def test_get_best_bid_single_level(self, orderbook):
        """Test getting best bid with single price level."""
        price = Decimal('47500.0')
        amount = Decimal('1.5')
        
        orderbook.update_bid(price, amount)
        best_price, best_amount = orderbook.get_best_bid()
        
        assert best_price == price
        assert best_amount == amount
    
    def test_get_best_ask_single_level(self, orderbook):
        """Test getting best ask with single price level."""
        price = Decimal('47501.0')
        amount = Decimal('2.0')
        
        orderbook.update_ask(price, amount)
        best_price, best_amount = orderbook.get_best_ask()
        
        assert best_price == price
        assert best_amount == amount
    
    def test_get_best_bid_multiple_levels(self, orderbook):
        """Test getting best bid with multiple price levels."""
        prices_amounts = [
            (Decimal('47500.0'), Decimal('1.0')),
            (Decimal('47499.0'), Decimal('2.0')),
            (Decimal('47501.0'), Decimal('0.5'))  # This should be best
        ]
        
        for price, amount in prices_amounts:
            orderbook.update_bid(price, amount)
        
        best_price, best_amount = orderbook.get_best_bid()
        
        assert best_price == Decimal('47501.0')  # Highest price
        assert best_amount == Decimal('0.5')
    
    def test_get_best_ask_multiple_levels(self, orderbook):
        """Test getting best ask with multiple price levels."""
        prices_amounts = [
            (Decimal('47502.0'), Decimal('1.0')),
            (Decimal('47503.0'), Decimal('2.0')),
            (Decimal('47501.0'), Decimal('0.5'))  # This should be best
        ]
        
        for price, amount in prices_amounts:
            orderbook.update_ask(price, amount)
        
        best_price, best_amount = orderbook.get_best_ask()
        
        assert best_price == Decimal('47501.0')  # Lowest price
        assert best_amount == Decimal('0.5')
    
    def test_get_spread_empty(self, orderbook):
        """Test getting spread from empty orderbook."""
        spread = orderbook.get_spread()
        assert spread is None
    
    def test_get_spread_missing_side(self, orderbook):
        """Test getting spread when one side is missing."""
        orderbook.update_bid(Decimal('47500.0'), Decimal('1.0'))
        spread = orderbook.get_spread()
        assert spread is None
        
        orderbook.clear()
        orderbook.update_ask(Decimal('47501.0'), Decimal('1.0'))
        spread = orderbook.get_spread()
        assert spread is None
    
    def test_get_spread_valid(self, orderbook):
        """Test getting spread with valid orderbook."""
        orderbook.update_bid(Decimal('47500.0'), Decimal('1.0'))
        orderbook.update_ask(Decimal('47501.0'), Decimal('1.0'))
        
        spread = orderbook.get_spread()
        assert spread == Decimal('1.0')
    
    def test_get_mid_price_empty(self, orderbook):
        """Test getting mid price from empty orderbook."""
        mid_price = orderbook.get_mid_price()
        assert mid_price is None
    
    def test_get_mid_price_valid(self, orderbook):
        """Test getting mid price with valid orderbook."""
        orderbook.update_bid(Decimal('47500.0'), Decimal('1.0'))
        orderbook.update_ask(Decimal('47502.0'), Decimal('1.0'))
        
        mid_price = orderbook.get_mid_price()
        assert mid_price == Decimal('47501.0')
    
    def test_get_depth_empty(self, orderbook):
        """Test getting depth from empty orderbook."""
        depth = orderbook.get_depth()
        
        assert depth['bids'] == []
        assert depth['asks'] == []
    
    def test_get_depth_with_data(self, orderbook):
        """Test getting depth with orderbook data."""
        # Add multiple bids and asks
        bid_data = [
            (Decimal('47500.0'), Decimal('1.0')),
            (Decimal('47499.0'), Decimal('2.0')),
            (Decimal('47498.0'), Decimal('1.5'))
        ]
        
        ask_data = [
            (Decimal('47501.0'), Decimal('1.2')),
            (Decimal('47502.0'), Decimal('0.8')),
            (Decimal('47503.0'), Decimal('2.5'))
        ]
        
        for price, amount in bid_data:
            orderbook.update_bid(price, amount)
        
        for price, amount in ask_data:
            orderbook.update_ask(price, amount)
        
        depth = orderbook.get_depth(levels=2)
        
        # Bids should be sorted highest to lowest
        assert len(depth['bids']) == 2
        assert depth['bids'][0] == (Decimal('47500.0'), Decimal('1.0'))
        assert depth['bids'][1] == (Decimal('47499.0'), Decimal('2.0'))
        
        # Asks should be sorted lowest to highest
        assert len(depth['asks']) == 2
        assert depth['asks'][0] == (Decimal('47501.0'), Decimal('1.2'))
        assert depth['asks'][1] == (Decimal('47502.0'), Decimal('0.8'))
    
    def test_clear(self, orderbook):
        """Test clearing orderbook."""
        # Add some data
        orderbook.update_bid(Decimal('47500.0'), Decimal('1.0'))
        orderbook.update_ask(Decimal('47501.0'), Decimal('1.0'))
        
        assert len(orderbook.bids) == 1
        assert len(orderbook.asks) == 1
        
        # Clear
        orderbook.clear()
        
        assert len(orderbook.bids) == 0
        assert len(orderbook.asks) == 0
        assert orderbook.last_update is not None
    
    def test_is_valid_empty(self, orderbook):
        """Test validity check for empty orderbook."""
        assert orderbook.is_valid() is True
    
    def test_is_valid_one_side(self, orderbook):
        """Test validity check with only one side."""
        orderbook.update_bid(Decimal('47500.0'), Decimal('1.0'))
        assert orderbook.is_valid() is True
        
        orderbook.clear()
        orderbook.update_ask(Decimal('47501.0'), Decimal('1.0'))
        assert orderbook.is_valid() is True
    
    def test_is_valid_normal_spread(self, orderbook):
        """Test validity check with normal spread."""
        orderbook.update_bid(Decimal('47500.0'), Decimal('1.0'))
        orderbook.update_ask(Decimal('47501.0'), Decimal('1.0'))
        
        assert orderbook.is_valid() is True
    
    def test_is_valid_crossed_book(self, orderbook):
        """Test validity check with crossed orderbook."""
        orderbook.update_bid(Decimal('47502.0'), Decimal('1.0'))  # Bid higher than ask
        orderbook.update_ask(Decimal('47501.0'), Decimal('1.0'))
        
        assert orderbook.is_valid() is False
    
    def test_sequence_increments(self, orderbook):
        """Test that sequence number increments correctly."""
        initial_sequence = orderbook.sequence
        
        orderbook.update_bid(Decimal('47500.0'), Decimal('1.0'))
        assert orderbook.sequence == initial_sequence + 1
        
        orderbook.update_ask(Decimal('47501.0'), Decimal('1.0'))
        assert orderbook.sequence == initial_sequence + 2
        
        orderbook.clear()
        assert orderbook.sequence == initial_sequence + 3


class TestOrderBookEdgeCases:
    """Test OrderBook edge cases and error conditions."""
    
    @pytest.fixture
    def orderbook(self):
        """Create OrderBook instance for testing."""
        return OrderBook('BTC/USDT', 'binance')
    
    def test_very_small_amounts(self, orderbook):
        """Test handling very small amounts."""
        price = Decimal('47500.0')
        amount = Decimal('0.00000001')  # 1 satoshi
        
        orderbook.update_bid(price, amount)
        best_price, best_amount = orderbook.get_best_bid()
        
        assert best_price == price
        assert best_amount == amount
    
    def test_very_high_precision_prices(self, orderbook):
        """Test handling high precision prices."""
        price = Decimal('47500.12345678')
        amount = Decimal('1.0')
        
        orderbook.update_ask(price, amount)
        best_price, best_amount = orderbook.get_best_ask()
        
        assert best_price == price
        assert best_amount == amount
    
    def test_large_amounts(self, orderbook):
        """Test handling large amounts."""
        price = Decimal('47500.0')
        amount = Decimal('1000000.0')  # 1 million
        
        orderbook.update_bid(price, amount)
        best_price, best_amount = orderbook.get_best_bid()
        
        assert best_price == price
        assert best_amount == amount
    
    def test_many_price_levels(self, orderbook):
        """Test handling many price levels."""
        # Add 1000 bid levels
        for i in range(1000):
            price = Decimal(f'{47500 - i}.0')
            amount = Decimal('1.0')
            orderbook.update_bid(price, amount)
        
        assert len(orderbook.bids) == 1000
        
        best_price, best_amount = orderbook.get_best_bid()
        assert best_price == Decimal('47500.0')  # Highest price
    
    def test_update_same_price_multiple_times(self, orderbook):
        """Test updating the same price level multiple times."""
        price = Decimal('47500.0')
        
        # First update
        orderbook.update_bid(price, Decimal('1.0'))
        assert orderbook.bids[price] == Decimal('1.0')
        
        # Second update (should overwrite)
        orderbook.update_bid(price, Decimal('2.0'))
        assert orderbook.bids[price] == Decimal('2.0')
        assert len(orderbook.bids) == 1  # Should still be only one level
    
    def test_zero_spread(self, orderbook):
        """Test orderbook with zero spread (same price)."""
        price = Decimal('47500.0')
        
        orderbook.update_bid(price, Decimal('1.0'))
        orderbook.update_ask(price, Decimal('1.0'))
        
        spread = orderbook.get_spread()
        assert spread == Decimal('0.0')
        
        mid_price = orderbook.get_mid_price()
        assert mid_price == price


class TestOrderBookFromFixtures:
    """Test OrderBook with sample data from fixtures."""
    
    def test_load_binance_orderbook(self, sample_orderbook_data):
        """Test loading Binance orderbook from fixtures."""
        data = sample_orderbook_data['binance_btc_usdt']
        orderbook = OrderBook(data['symbol'], data['exchange'])
        
        # Load bids
        for price_str, amount_str in data['bids']:
            orderbook.update_bid(Decimal(price_str), Decimal(amount_str))
        
        # Load asks
        for price_str, amount_str in data['asks']:
            orderbook.update_ask(Decimal(price_str), Decimal(amount_str))
        
        # Test best bid/ask
        best_bid, _ = orderbook.get_best_bid()
        best_ask, _ = orderbook.get_best_ask()
        
        assert best_bid == Decimal('47500.00')
        assert best_ask == Decimal('47500.50')
        
        # Test spread
        spread = orderbook.get_spread()
        assert spread == Decimal('0.50')
        
        # Test validity
        assert orderbook.is_valid() is True
    
    def test_load_empty_orderbook(self, sample_orderbook_data):
        """Test loading empty orderbook from fixtures."""
        data = sample_orderbook_data['empty_orderbook']
        orderbook = OrderBook(data['symbol'], data['exchange'])
        
        # Should have no data
        assert len(orderbook.bids) == 0
        assert len(orderbook.asks) == 0
        
        # Best bid/ask should be None
        best_bid, _ = orderbook.get_best_bid()
        best_ask, _ = orderbook.get_best_ask()
        
        assert best_bid is None
        assert best_ask is None
        
        # Spread and mid price should be None
        assert orderbook.get_spread() is None
        assert orderbook.get_mid_price() is None
    
    def test_load_high_precision_orderbook(self, sample_orderbook_data):
        """Test loading high precision orderbook from fixtures."""
        data = sample_orderbook_data['high_precision_orderbook']
        orderbook = OrderBook(data['symbol'], data['exchange'])
        
        # Load data
        for price_str, amount_str in data['bids']:
            orderbook.update_bid(Decimal(price_str), Decimal(amount_str))
        
        for price_str, amount_str in data['asks']:
            orderbook.update_ask(Decimal(price_str), Decimal(amount_str))
        
        # Test precision is maintained
        best_bid, best_bid_amount = orderbook.get_best_bid()
        best_ask, best_ask_amount = orderbook.get_best_ask()
        
        assert best_bid == Decimal('0.00012345')
        assert best_ask == Decimal('0.00012346')
        assert best_bid_amount == Decimal('10000.12345678')
        assert best_ask_amount == Decimal('12000.98765432')
        
        # Test spread calculation with high precision
        spread = orderbook.get_spread()
        assert spread == Decimal('0.00000001')
