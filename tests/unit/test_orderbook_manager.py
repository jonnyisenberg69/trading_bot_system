"""
Unit tests for the OrderbookManager class.

Tests orderbook manager initialization, exchange connection, symbol subscription,
and orderbook data handling.
"""

import pytest
import asyncio
import json
from decimal import Decimal
from unittest.mock import Mock, patch, AsyncMock, MagicMock

from market_data.orderbook_manager import OrderbookManager
from market_data.orderbook import OrderBook
from exchanges.websocket import WebSocketManager, WSState, WSMessageType
from exchanges.base_connector import BaseExchangeConnector


class MockWebSocketManager:
    """Mock WebSocketManager for testing."""
    
    def __init__(self):
        self.connections = {}
        self.subscriptions = {}
        self.handlers = {}
        self._running = False
    
    async def start(self):
        """Mock start method."""
        self._running = True
    
    async def stop(self):
        """Mock stop method."""
        self._running = False
    
    async def connect_exchange(self, exchange, endpoint, conn_type):
        """Mock connect_exchange method."""
        conn_id = f"{exchange.name}_{conn_type}_test"
        self.connections[conn_id] = {
            'exchange': exchange,
            'endpoint': endpoint,
            'conn_type': conn_type,
            'state': WSState.CONNECTED
        }
        self.subscriptions[conn_id] = {}
        return conn_id
    
    async def subscribe(self, conn_id, channel, symbol):
        """Mock subscribe method."""
        if conn_id not in self.subscriptions:
            self.subscriptions[conn_id] = {}
        
        if channel not in self.subscriptions[conn_id]:
            self.subscriptions[conn_id][channel] = set()
        
        self.subscriptions[conn_id][channel].add(symbol)
        return True
    
    async def unsubscribe(self, conn_id, channel, symbol):
        """Mock unsubscribe method."""
        if (conn_id in self.subscriptions and 
            channel in self.subscriptions[conn_id] and 
            symbol in self.subscriptions[conn_id][channel]):
            self.subscriptions[conn_id][channel].remove(symbol)
            return True
        return False
    
    def register_handler(self, message_type, handler):
        """Mock register_handler method."""
        if message_type not in self.handlers:
            self.handlers[message_type] = []
        
        self.handlers[message_type].append(handler)
    
    def get_connection_status(self, conn_id=None):
        """Mock get_connection_status method."""
        if conn_id:
            if conn_id not in self.connections:
                return {'error': f"Connection {conn_id} not found"}
            
            conn_info = self.connections[conn_id]
            return {
                'conn_id': conn_id,
                'exchange': conn_info['exchange'].name,
                'conn_type': conn_info['conn_type'],
                'state': conn_info['state']
            }
        else:
            return {
                conn_id: {
                    'exchange': info['exchange'].name,
                    'conn_type': info['conn_type'],
                    'state': info['state']
                }
                for conn_id, info in self.connections.items()
            }


class MockExchangeConnector:
    """Mock exchange connector for testing."""
    
    def __init__(self, name="binance"):
        self.name = name
        self.api_key = "test_api_key"
        self.secret = "test_secret"


@pytest.fixture
def mock_ws_manager():
    """Create a mock WebSocketManager."""
    return MockWebSocketManager()


@pytest.fixture
def mock_exchange():
    """Create a mock exchange connector."""
    return MockExchangeConnector()


@pytest.fixture
def orderbook_manager(mock_ws_manager):
    """Create an OrderbookManager instance with a mock WebSocketManager."""
    manager = OrderbookManager(mock_ws_manager)
    return manager


@pytest.mark.asyncio
async def test_init(orderbook_manager):
    """Test OrderbookManager initialization."""
    assert orderbook_manager.orderbooks == {}
    assert orderbook_manager.connections == {}
    assert orderbook_manager.subscribed_symbols == {}
    assert orderbook_manager._running is False
    assert orderbook_manager._initialized is False


@pytest.mark.asyncio
async def test_start_stop(orderbook_manager):
    """Test starting and stopping the orderbook manager."""
    # Start manager
    await orderbook_manager.start()
    assert orderbook_manager._running is True
    assert orderbook_manager.ws_manager._running is True
    
    # Stop manager
    await orderbook_manager.stop()
    assert orderbook_manager._running is False
    assert orderbook_manager.ws_manager._running is False


@pytest.mark.asyncio
async def test_add_exchange(orderbook_manager, mock_exchange):
    """Test adding an exchange."""
    # Start manager
    await orderbook_manager.start()
    
    # Add exchange
    await orderbook_manager.add_exchange(mock_exchange)
    
    # Verify exchange is added
    exchange_name = mock_exchange.name.lower()
    assert exchange_name in orderbook_manager.orderbooks
    assert exchange_name in orderbook_manager.subscribed_symbols
    assert exchange_name in orderbook_manager.connections
    
    # Stop manager
    await orderbook_manager.stop()


@pytest.mark.asyncio
async def test_subscribe_symbol(orderbook_manager, mock_exchange):
    """Test subscribing to a symbol."""
    # Start manager
    await orderbook_manager.start()
    
    # Add exchange
    await orderbook_manager.add_exchange(mock_exchange)
    
    # Subscribe to symbol
    symbol = "BTC/USDT"
    result = await orderbook_manager.subscribe_symbol(mock_exchange.name, symbol)
    
    # Verify subscription
    exchange_name = mock_exchange.name.lower()
    assert result is True
    assert symbol in orderbook_manager.subscribed_symbols[exchange_name]
    assert symbol in orderbook_manager.orderbooks[exchange_name]
    
    # Verify orderbook is created
    orderbook = orderbook_manager.orderbooks[exchange_name][symbol]
    assert isinstance(orderbook, OrderBook)
    assert orderbook.symbol == symbol
    assert orderbook.exchange == exchange_name
    
    # Stop manager
    await orderbook_manager.stop()


@pytest.mark.asyncio
async def test_unsubscribe_symbol(orderbook_manager, mock_exchange):
    """Test unsubscribing from a symbol."""
    # Start manager
    await orderbook_manager.start()
    
    # Add exchange
    await orderbook_manager.add_exchange(mock_exchange)
    
    # Subscribe to symbol
    symbol = "BTC/USDT"
    await orderbook_manager.subscribe_symbol(mock_exchange.name, symbol)
    
    # Unsubscribe from symbol
    result = await orderbook_manager.unsubscribe_symbol(mock_exchange.name, symbol)
    
    # Verify unsubscription
    exchange_name = mock_exchange.name.lower()
    assert result is True
    assert symbol not in orderbook_manager.subscribed_symbols[exchange_name]
    
    # Orderbook should still exist
    assert symbol in orderbook_manager.orderbooks[exchange_name]
    
    # Stop manager
    await orderbook_manager.stop()


@pytest.mark.asyncio
async def test_get_orderbook(orderbook_manager, mock_exchange):
    """Test getting an orderbook."""
    # Start manager
    await orderbook_manager.start()
    
    # Add exchange
    await orderbook_manager.add_exchange(mock_exchange)
    
    # Subscribe to symbol
    symbol = "BTC/USDT"
    await orderbook_manager.subscribe_symbol(mock_exchange.name, symbol)
    
    # Get orderbook
    orderbook = orderbook_manager.get_orderbook(mock_exchange.name, symbol)
    
    # Verify orderbook
    assert orderbook is not None
    assert isinstance(orderbook, OrderBook)
    assert orderbook.symbol == symbol
    assert orderbook.exchange == mock_exchange.name.lower()
    
    # Get non-existent orderbook
    non_existent = orderbook_manager.get_orderbook(mock_exchange.name, "ETH/USDT")
    assert non_existent is None
    
    # Stop manager
    await orderbook_manager.stop()


@pytest.mark.asyncio
async def test_get_best_bid_ask(orderbook_manager, mock_exchange):
    """Test getting best bid and ask."""
    # Start manager
    await orderbook_manager.start()
    
    # Add exchange
    await orderbook_manager.add_exchange(mock_exchange)
    
    # Subscribe to symbol
    symbol = "BTC/USDT"
    await orderbook_manager.subscribe_symbol(mock_exchange.name, symbol)
    
    # Get orderbook
    exchange_name = mock_exchange.name.lower()
    orderbook = orderbook_manager.orderbooks[exchange_name][symbol]
    
    # Add some prices
    orderbook.update_bid(Decimal('50000.0'), Decimal('1.0'))
    orderbook.update_bid(Decimal('49900.0'), Decimal('2.0'))
    orderbook.update_ask(Decimal('50100.0'), Decimal('1.5'))
    orderbook.update_ask(Decimal('50200.0'), Decimal('2.5'))
    
    # Get best bid/ask
    best_bid, best_ask = orderbook_manager.get_best_bid_ask(exchange_name, symbol)
    
    # Verify best prices
    assert best_bid == Decimal('50000.0')
    assert best_ask == Decimal('50100.0')
    
    # Get best bid/ask for non-existent orderbook
    best_bid, best_ask = orderbook_manager.get_best_bid_ask(exchange_name, "ETH/USDT")
    assert best_bid is None
    assert best_ask is None
    
    # Stop manager
    await orderbook_manager.stop()


@pytest.mark.asyncio
async def test_find_best_bid_ask_across_exchanges(orderbook_manager):
    """Test finding best bid and ask across exchanges."""
    # Create multiple exchanges
    binance = MockExchangeConnector("binance")
    bybit = MockExchangeConnector("bybit")
    
    # Start manager
    await orderbook_manager.start()
    
    # Add exchanges
    await orderbook_manager.add_exchange(binance)
    await orderbook_manager.add_exchange(bybit)
    
    # Subscribe to symbol on both exchanges
    symbol = "BTC/USDT"
    await orderbook_manager.subscribe_symbol(binance.name, symbol)
    await orderbook_manager.subscribe_symbol(bybit.name, symbol)
    
    # Get orderbooks
    binance_book = orderbook_manager.orderbooks[binance.name.lower()][symbol]
    bybit_book = orderbook_manager.orderbooks[bybit.name.lower()][symbol]
    
    # Add different prices to each exchange
    # Binance has better bid, Bybit has better ask
    binance_book.update_bid(Decimal('50000.0'), Decimal('1.0'))
    binance_book.update_ask(Decimal('50200.0'), Decimal('1.5'))
    
    bybit_book.update_bid(Decimal('49900.0'), Decimal('2.0'))
    bybit_book.update_ask(Decimal('50100.0'), Decimal('2.5'))
    
    # Find best bid (should be Binance)
    best_bid_exchange, best_bid_price, best_bid_amount = orderbook_manager.find_best_bid(symbol)
    
    # Verify best bid
    assert best_bid_exchange == binance.name.lower()
    assert best_bid_price == Decimal('50000.0')
    assert best_bid_amount == Decimal('1.0')
    
    # Find best ask (should be Bybit)
    best_ask_exchange, best_ask_price, best_ask_amount = orderbook_manager.find_best_ask(symbol)
    
    # Verify best ask
    assert best_ask_exchange == bybit.name.lower()
    assert best_ask_price == Decimal('50100.0')
    assert best_ask_amount == Decimal('2.5')
    
    # Calculate cross-exchange spread
    spread = orderbook_manager.calculate_cross_exchange_spread(symbol)
    
    # Verify spread
    assert spread == Decimal('100.0')  # 50100 - 50000
    
    # Stop manager
    await orderbook_manager.stop()


@pytest.mark.asyncio
async def test_handle_orderbook_message(orderbook_manager, mock_exchange):
    """Test handling orderbook messages."""
    # Start manager
    await orderbook_manager.start()
    
    # Add exchange
    await orderbook_manager.add_exchange(mock_exchange)
    
    # Subscribe to symbol
    symbol = "BTC/USDT"
    await orderbook_manager.subscribe_symbol(mock_exchange.name, symbol)
    
    # Get orderbook
    exchange_name = mock_exchange.name.lower()
    orderbook = orderbook_manager.orderbooks[exchange_name][symbol]
    
    # Create mock snapshot message
    snapshot_message = {
        'exchange': exchange_name,
        'type': WSMessageType.ORDERBOOK,
        'symbol': symbol,
        'is_snapshot': True,
        'raw': {
            'lastUpdateId': 12345,
            'bids': [['50000.0', '1.0'], ['49900.0', '2.0']],
            'asks': [['50100.0', '1.5'], ['50200.0', '2.5']]
        }
    }
    
    # Process snapshot message
    await orderbook_manager._handle_orderbook_message(snapshot_message)
    
    # Verify orderbook state after snapshot
    assert orderbook.snapshot_id == 12345
    assert len(orderbook.bids) == 2
    assert len(orderbook.asks) == 2
    assert Decimal('50000.0') in orderbook.bids
    assert Decimal('50100.0') in orderbook.asks
    
    # Create mock delta message
    delta_message = {
        'exchange': exchange_name,
        'type': WSMessageType.ORDERBOOK,
        'symbol': symbol,
        'is_snapshot': False,
        'raw': {
            'U': 12346,
            'u': 12347,
            'b': [['50100.0', '3.0'], ['49900.0', '0']],  # Update bid, remove bid
            'a': [['50100.0', '0'], ['50300.0', '1.0']]   # Remove ask, add ask
        }
    }
    
    # Process delta message
    await orderbook_manager._handle_orderbook_message(delta_message)
    
    # Verify orderbook state after delta
    assert orderbook.snapshot_id == 12347
    assert len(orderbook.bids) == 2  # One removed, one added
    assert len(orderbook.asks) == 2  # One removed, one added
    assert Decimal('50100.0') in orderbook.bids
    assert Decimal('49900.0') not in orderbook.bids
    assert Decimal('50100.0') not in orderbook.asks
    assert Decimal('50300.0') in orderbook.asks
    
    # Stop manager
    await orderbook_manager.stop()


@pytest.mark.asyncio
async def test_get_arbitrage_opportunities(orderbook_manager):
    """Test getting arbitrage opportunities."""
    # Create multiple exchanges
    binance = MockExchangeConnector("binance")
    bybit = MockExchangeConnector("bybit")
    
    # Start manager
    await orderbook_manager.start()
    
    # Add exchanges
    await orderbook_manager.add_exchange(binance)
    await orderbook_manager.add_exchange(bybit)
    
    # Subscribe to symbol on both exchanges
    symbol = "BTC/USDT"
    await orderbook_manager.subscribe_symbol(binance.name, symbol)
    await orderbook_manager.subscribe_symbol(bybit.name, symbol)
    
    # Get orderbooks
    binance_book = orderbook_manager.orderbooks[binance.name.lower()][symbol]
    bybit_book = orderbook_manager.orderbooks[bybit.name.lower()][symbol]
    
    # Create an arbitrage opportunity (buy on Bybit, sell on Binance)
    binance_book.update_bid(Decimal('50500.0'), Decimal('1.0'))  # Higher bid
    binance_book.update_ask(Decimal('50600.0'), Decimal('1.5'))
    
    bybit_book.update_bid(Decimal('50000.0'), Decimal('2.0'))
    bybit_book.update_ask(Decimal('50100.0'), Decimal('2.5'))  # Lower ask
    
    # Get arbitrage opportunities with 0.1% min profit
    opportunities = orderbook_manager.get_arbitrage_opportunities(symbol, min_profit_pct=0.1)
    
    # Verify opportunities
    # The implementation finds 2 opportunities:
    # 1. Buy on Bybit, sell on Binance
    # 2. The reverse direction is also checked
    assert len(opportunities) == 2
    
    # Find the opportunity to buy on Bybit and sell on Binance
    bybit_binance_opp = next(
        (opp for opp in opportunities 
         if opp['buy_exchange'] == bybit.name.lower() and 
         opp['sell_exchange'] == binance.name.lower()),
        None
    )
    
    # Verify the opportunity
    assert bybit_binance_opp is not None
    assert bybit_binance_opp['symbol'] == symbol
    assert bybit_binance_opp['buy_price'] == Decimal('50100.0')
    assert bybit_binance_opp['sell_price'] == Decimal('50500.0')
    assert bybit_binance_opp['profit_pct'] > 0.7  # ~0.8% profit
    
    # Stop manager
    await orderbook_manager.stop()


@pytest.mark.asyncio
async def test_get_status(orderbook_manager, mock_exchange):
    """Test getting status."""
    # Start manager
    await orderbook_manager.start()
    
    # Add exchange
    await orderbook_manager.add_exchange(mock_exchange)
    
    # Subscribe to symbols
    await orderbook_manager.subscribe_symbol(mock_exchange.name, "BTC/USDT")
    await orderbook_manager.subscribe_symbol(mock_exchange.name, "ETH/USDT")
    
    # Get status
    status = orderbook_manager.get_status()
    
    # Verify status
    assert status['running'] is True
    assert mock_exchange.name.lower() in status['exchanges']
    assert status['total_symbols'] == 2
    assert len(status['symbols_by_exchange'][mock_exchange.name.lower()]) == 2
    assert mock_exchange.name.lower() in status['connections']
    
    # Stop manager
    await orderbook_manager.stop() 