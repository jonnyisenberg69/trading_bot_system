"""
Unit tests for the WebSocketManager class.

Tests WebSocket connection management, subscription handling, and message processing.
"""

import pytest
import asyncio
import json
import time
from unittest.mock import Mock, patch, AsyncMock, MagicMock
from datetime import datetime

from exchanges.websocket import WebSocketManager, WSState, WSMessageType
from exchanges.base_connector import BaseExchangeConnector


class MockWebSocketConnection:
    """Mock WebSocket connection for testing."""
    
    def __init__(self, messages=None):
        self.messages = messages or []
        self.message_index = 0
        self.closed = False
        self.sent_messages = []
    
    async def send(self, message):
        """Mock send method."""
        self.sent_messages.append(message)
    
    async def recv(self):
        """Mock receive method."""
        if self.message_index < len(self.messages):
            message = self.messages[self.message_index]
            self.message_index += 1
            return message
        else:
            # Wait forever
            await asyncio.Future()
    
    async def close(self, code=1000, reason=""):
        """Mock close method."""
        self.closed = True
    
    async def ping(self):
        """Mock ping method."""
        pong_waiter = asyncio.Future()
        pong_waiter.set_result(None)
        return pong_waiter


class MockExchangeConnector:
    """Mock exchange connector for testing."""
    
    def __init__(self, name="binance"):
        self.name = name
        self.api_key = "test_api_key"
        self.secret = "test_secret"


@pytest.fixture
def mock_websocket():
    """Create a mock WebSocket connection."""
    with patch('websockets.connect') as mock_connect:
        mock_ws = MockWebSocketConnection()
        mock_connect.return_value.__aenter__.return_value = mock_ws
        yield mock_ws


@pytest.fixture
def mock_exchange():
    """Create a mock exchange connector."""
    return MockExchangeConnector()


@pytest.fixture
def ws_manager():
    """Create a WebSocketManager instance."""
    manager = WebSocketManager()
    return manager


@pytest.mark.asyncio
async def test_init(ws_manager):
    """Test WebSocketManager initialization."""
    assert ws_manager.connections == {}
    assert ws_manager.connection_tasks == {}
    assert ws_manager.subscriptions == {}
    assert ws_manager.message_handlers == {}
    assert ws_manager._running is False


@pytest.mark.asyncio
async def test_start_stop(ws_manager):
    """Test starting and stopping the WebSocket manager."""
    # Start manager
    await ws_manager.start()
    assert ws_manager._running is True
    
    # Stop manager
    await ws_manager.stop()
    assert ws_manager._running is False


@pytest.mark.asyncio
async def test_connect_exchange(ws_manager, mock_exchange, mock_websocket):
    """Test connecting to an exchange."""
    # Start manager
    await ws_manager.start()
    
    # Connect to exchange
    conn_id = await ws_manager.connect_exchange(
        mock_exchange, 
        "wss://test.exchange.com/ws", 
        "public"
    )
    
    # Verify connection is stored
    assert conn_id in ws_manager.connections
    assert ws_manager.connections[conn_id]['exchange'] == mock_exchange
    assert ws_manager.connections[conn_id]['endpoint'] == "wss://test.exchange.com/ws"
    assert ws_manager.connections[conn_id]['conn_type'] == "public"
    assert conn_id in ws_manager.subscriptions
    
    # Stop manager
    await ws_manager.stop()


@pytest.mark.asyncio
async def test_close_connection(ws_manager, mock_exchange, mock_websocket):
    """Test closing a connection."""
    # Start manager
    await ws_manager.start()
    
    # Connect to exchange
    conn_id = await ws_manager.connect_exchange(
        mock_exchange, 
        "wss://test.exchange.com/ws", 
        "public"
    )
    
    # Close connection
    result = await ws_manager.close_connection(conn_id)
    
    # Verify connection is closed
    assert result is True
    assert conn_id not in ws_manager.connections
    assert conn_id not in ws_manager.connection_tasks
    assert conn_id not in ws_manager.subscriptions
    
    # Stop manager
    await ws_manager.stop()


@pytest.mark.asyncio
async def test_subscribe_unsubscribe(ws_manager, mock_exchange, mock_websocket):
    """Test subscribing and unsubscribing to channels."""
    # Start manager
    await ws_manager.start()
    
    # Connect to exchange
    conn_id = await ws_manager.connect_exchange(
        mock_exchange, 
        "wss://test.exchange.com/ws", 
        "public"
    )
    
    # Set connection state to connected
    ws_manager.connections[conn_id]['state'] = WSState.CONNECTED
    ws_manager.connections[conn_id]['ws'] = mock_websocket
    
    # Subscribe to channel
    symbol = "BTC/USDT"
    channel = "depth"
    result = await ws_manager.subscribe(conn_id, channel, symbol)
    
    # Verify subscription
    assert result is True
    assert channel in ws_manager.subscriptions[conn_id]
    assert symbol in ws_manager.subscriptions[conn_id][channel]
    assert len(mock_websocket.sent_messages) == 1
    
    # Parse sent message
    sent_msg = json.loads(mock_websocket.sent_messages[0])
    assert sent_msg["method"] == "SUBSCRIBE"
    assert "btcusdt@depth" in sent_msg["params"]
    
    # Unsubscribe from channel
    result = await ws_manager.unsubscribe(conn_id, channel, symbol)
    
    # Verify unsubscription
    assert result is True
    assert symbol not in ws_manager.subscriptions[conn_id][channel]
    assert len(mock_websocket.sent_messages) == 2
    
    # Parse sent message
    sent_msg = json.loads(mock_websocket.sent_messages[1])
    assert sent_msg["method"] == "UNSUBSCRIBE"
    assert "btcusdt@depth" in sent_msg["params"]
    
    # Stop manager
    await ws_manager.stop()


@pytest.mark.asyncio
async def test_register_handler(ws_manager):
    """Test registering message handlers."""
    # Create handler
    async def test_handler(message):
        pass
    
    # Register handler
    ws_manager.register_handler(WSMessageType.ORDERBOOK, test_handler)
    
    # Verify handler is registered
    assert WSMessageType.ORDERBOOK in ws_manager.message_handlers
    assert test_handler in ws_manager.message_handlers[WSMessageType.ORDERBOOK]


@pytest.mark.asyncio
async def test_get_connection_status(ws_manager, mock_exchange):
    """Test getting connection status."""
    # Start manager
    await ws_manager.start()
    
    # Connect to exchange
    conn_id = await ws_manager.connect_exchange(
        mock_exchange, 
        "wss://test.exchange.com/ws", 
        "public"
    )
    
    # Get status for single connection
    status = ws_manager.get_connection_status(conn_id)
    
    # Verify status
    assert status['conn_id'] == conn_id
    assert status['exchange'] == mock_exchange.name
    assert status['conn_type'] == "public"
    assert status['state'] == WSState.DISCONNECTED
    
    # Get status for all connections
    all_status = ws_manager.get_connection_status()
    
    # Verify status
    assert conn_id in all_status
    assert all_status[conn_id]['exchange'] == mock_exchange.name
    assert all_status[conn_id]['conn_type'] == "public"
    assert all_status[conn_id]['state'] == WSState.DISCONNECTED
    
    # Stop manager
    await ws_manager.stop()


@pytest.mark.asyncio
async def test_process_message(ws_manager):
    """Test processing WebSocket messages."""
    # Start manager
    await ws_manager.start()
    
    # Create mock handler
    mock_handler = AsyncMock()
    
    # Register handler
    ws_manager.register_handler(WSMessageType.ORDERBOOK, mock_handler)
    
    # Create mock connection
    mock_exchange = MockExchangeConnector()
    conn_id = await ws_manager.connect_exchange(
        mock_exchange, 
        "wss://test.exchange.com/ws", 
        "public"
    )
    
    # Create mock message
    message = json.dumps({
        "e": "depthUpdate",
        "E": 1234567890123,
        "s": "BTCUSDT",
        "U": 1,
        "u": 10,
        "b": [["50000.0", "1.5"]],
        "a": [["50001.0", "2.0"]]
    })
    
    # Process message
    await ws_manager._process_message(conn_id, message)
    
    # Verify handler was called
    assert mock_handler.called
    
    # Check normalized message
    called_with = mock_handler.call_args[0][0]
    assert called_with['exchange'] == 'binance'
    assert called_with['type'] == WSMessageType.ORDERBOOK
    assert called_with['symbol'] == 'BTC/USDT'
    
    # Stop manager
    await ws_manager.stop()


@pytest.mark.asyncio
async def test_connection_handler_reconnection(ws_manager, mock_exchange):
    """Test connection handler with reconnection."""
    # Mock the reconnection manager
    ws_manager.reconnection_manager.mark_failure = Mock()
    ws_manager.reconnection_manager.should_reconnect = Mock(return_value=True)
    ws_manager.reconnection_manager.get_delay = Mock(return_value=0.01)  # Very short delay
    
    # Create a mock WebSocket with failure
    with patch('websockets.connect') as mock_connect:
        # First connection attempt fails
        mock_connect.side_effect = [ConnectionError("Test error"), AsyncMock()]
        
        # Start manager
        await ws_manager.start()
        
        # Connect to exchange
        conn_id = await ws_manager.connect_exchange(
            mock_exchange, 
            "wss://test.exchange.com/ws", 
            "public"
        )
        
        # Wait for reconnection attempt
        await asyncio.sleep(0.1)
        
        # Verify reconnection was attempted
        assert ws_manager.reconnection_manager.mark_failure.called
        assert ws_manager.reconnection_manager.should_reconnect.called
        assert ws_manager.reconnection_manager.get_delay.called
        
        # Stop manager
        await ws_manager.stop()


@pytest.mark.asyncio
async def test_determine_message_type():
    """Test determining message type."""
    # Create manager
    manager = WebSocketManager()
    
    # Binance orderbook update
    message = {
        "e": "depthUpdate",
        "E": 1234567890123,
        "s": "BTCUSDT"
    }
    assert manager._determine_message_type("binance", message) == WSMessageType.ORDERBOOK
    
    # Binance trade
    message = {
        "e": "trade",
        "E": 1234567890123,
        "s": "BTCUSDT"
    }
    assert manager._determine_message_type("binance", message) == WSMessageType.TRADE
    
    # Bybit orderbook
    message = {
        "topic": "orderbook.100.BTCUSDT"
    }
    assert manager._determine_message_type("bybit", message) == WSMessageType.ORDERBOOK
    
    # Generic orderbook
    message = {
        "orderbook": {
            "bids": [],
            "asks": []
        }
    }
    assert manager._determine_message_type("unknown", message) == WSMessageType.ORDERBOOK
    
    # Non-dict message
    message = "not a dict"
    assert manager._determine_message_type("binance", message) is None


@pytest.mark.asyncio
async def test_normalize_message():
    """Test normalizing messages."""
    # Create manager
    manager = WebSocketManager()
    
    # Binance orderbook update
    message = {
        "e": "depthUpdate",
        "E": 1234567890123,
        "s": "BTCUSDT",
        "b": [["50000.0", "1.5"]],
        "a": [["50001.0", "2.0"]]
    }
    normalized = manager._normalize_message("binance", WSMessageType.ORDERBOOK, message)
    
    # Verify normalized message
    assert normalized['exchange'] == 'binance'
    assert normalized['type'] == WSMessageType.ORDERBOOK
    assert normalized['symbol'] == 'BTC/USDT'
    assert 'is_snapshot' in normalized
    assert len(normalized['bids']) == 1
    assert len(normalized['asks']) == 1
    assert normalized['bids'][0] == [50000.0, 1.5]
    assert normalized['asks'][0] == [50001.0, 2.0]
    
    # Binance trade
    message = {
        "e": "trade",
        "E": 1234567890123,
        "s": "BTCUSDT",
        "t": 12345,
        "p": "50000.0",
        "q": "1.5",
        "m": False,
        "T": 1234567890123
    }
    normalized = manager._normalize_message("binance", WSMessageType.TRADE, message)
    
    # Verify normalized message
    assert normalized['exchange'] == 'binance'
    assert normalized['type'] == WSMessageType.TRADE
    assert normalized['symbol'] == 'BTC/USDT'
    assert normalized['id'] == '12345'
    assert normalized['price'] == 50000.0
    assert normalized['amount'] == 1.5
    assert normalized['side'] == 'buy'  # m=False means buyer is maker 