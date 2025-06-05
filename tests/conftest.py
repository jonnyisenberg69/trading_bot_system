"""
Pytest configuration and shared fixtures for trading bot tests.

This module provides common fixtures and configuration for testing
the trading bot system components.
"""

import asyncio
import json
import pytest
import pytest_asyncio
from pathlib import Path
from typing import Dict, Any
from decimal import Decimal
from unittest.mock import Mock, AsyncMock
import redis.asyncio as redis

# Import test modules
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from exchanges.base_connector import BaseExchangeConnector
from exchanges.rate_limiter import RateLimiter, RateLimit
from exchanges.connectors.binance import BinanceConnector
from exchanges.connectors.bybit import BybitConnector
from market_data.orderbook import OrderBook


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def sample_orderbook_data():
    """Load sample orderbook data from fixtures."""
    fixtures_path = Path(__file__).parent / "fixtures" / "sample_orderbook.json"
    with open(fixtures_path, 'r') as f:
        return json.load(f)


@pytest.fixture
def mock_redis():
    """Mock Redis client for testing."""
    mock_redis = AsyncMock(spec=redis.Redis)
    
    # Mock Redis operations
    mock_redis.get = AsyncMock(return_value=None)
    mock_redis.set = AsyncMock(return_value=True)
    mock_redis.delete = AsyncMock(return_value=1)
    mock_redis.eval = AsyncMock(return_value=1)  # Allow rate limiting
    mock_redis.keys = AsyncMock(return_value=[])
    
    return mock_redis


@pytest.fixture
def rate_limiter(mock_redis):
    """Create rate limiter instance with mocked Redis."""
    return RateLimiter(mock_redis)


@pytest.fixture
def binance_config():
    """Binance connector configuration for testing."""
    return {
        'name': 'binance',
        'api_key': 'test_api_key',
        'secret': 'test_secret',
        'sandbox': True,
        'market_type': 'spot'
    }


@pytest.fixture
def bybit_config():
    """Bybit connector configuration for testing."""
    return {
        'name': 'bybit',
        'api_key': 'test_api_key',
        'secret': 'test_secret',
        'sandbox': True,
        'market_type': 'spot'
    }


@pytest.fixture
def hyperliquid_config():
    """Hyperliquid connector configuration for testing."""
    return {
        'name': 'hyperliquid',
        'wallet_address': '0x1234567890abcdef1234567890abcdef12345678',
        'private_key': 'test_private_key',
        'sandbox': True
    }


@pytest.fixture
def mexc_config():
    """MEXC connector configuration for testing."""
    return {
        'name': 'mexc',
        'api_key': 'test_api_key',
        'secret': 'test_secret',
        'sandbox': True,
        'market_type': 'spot'
    }


@pytest.fixture
def gateio_config():
    """Gate.io connector configuration for testing."""
    return {
        'name': 'gateio',
        'api_key': 'test_api_key',
        'secret': 'test_secret',
        'sandbox': True,
        'market_type': 'spot'
    }


@pytest.fixture
def bitget_config():
    """Bitget connector configuration for testing."""
    return {
        'name': 'bitget',
        'api_key': 'test_api_key',
        'secret': 'test_secret',
        'sandbox': True,
        'market_type': 'spot'
    }


@pytest.fixture
def mock_exchange_connector():
    """Mock exchange connector for testing."""
    mock_connector = Mock(spec=BaseExchangeConnector)
    
    # Mock async methods
    mock_connector.connect = AsyncMock(return_value=True)
    mock_connector.disconnect = AsyncMock(return_value=True)
    mock_connector.get_balance = AsyncMock(return_value={'USDT': Decimal('1000.0')})
    mock_connector.get_orderbook = AsyncMock(return_value={
        'symbol': 'BTC/USDT',
        'bids': [[Decimal('47500'), Decimal('1.0')]],
        'asks': [[Decimal('47501'), Decimal('1.0')]]
    })
    mock_connector.place_order = AsyncMock(return_value={
        'id': 'test_order_123',
        'status': 'open',
        'symbol': 'BTC/USDT',
        'side': 'buy',
        'amount': Decimal('0.1'),
        'price': Decimal('47500')
    })
    mock_connector.cancel_order = AsyncMock(return_value={
        'id': 'test_order_123',
        'status': 'cancelled'
    })
    mock_connector.get_order_status = AsyncMock(return_value={
        'id': 'test_order_123',
        'status': 'filled'
    })
    mock_connector.get_open_orders = AsyncMock(return_value=[])
    mock_connector.get_trade_history = AsyncMock(return_value=[])
    mock_connector.get_positions = AsyncMock(return_value=[])
    mock_connector.health_check = AsyncMock(return_value={
        'status': 'healthy',
        'connected': True
    })
    
    # Mock properties
    mock_connector.name = 'mock_exchange'
    mock_connector.connected = True
    
    return mock_connector


@pytest.fixture
def mock_ccxt_exchange():
    """Mock ccxt exchange for testing."""
    mock_exchange = AsyncMock()
    
    # Mock ccxt methods
    mock_exchange.load_markets = AsyncMock()
    mock_exchange.fetch_time = AsyncMock(return_value=1640995200000)
    mock_exchange.fetch_balance = AsyncMock(return_value={
        'USDT': {'free': 1000.0, 'used': 0.0, 'total': 1000.0},
        'BTC': {'free': 0.1, 'used': 0.0, 'total': 0.1}
    })
    mock_exchange.fetch_order_book = AsyncMock(return_value={
        'symbol': 'BTC/USDT',
        'timestamp': 1640995200000,
        'datetime': '2022-01-01T00:00:00.000Z',
        'bids': [[47500.0, 1.0], [47499.0, 0.5]],
        'asks': [[47501.0, 1.0], [47502.0, 0.5]]
    })
    mock_exchange.create_limit_order = AsyncMock(return_value={
        'id': 'test_order_123',
        'symbol': 'BTC/USDT',
        'side': 'buy',
        'amount': 0.1,
        'price': 47500.0,
        'status': 'open',
        'filled': 0.0,
        'remaining': 0.1,
        'timestamp': 1640995200000
    })
    mock_exchange.create_market_order = AsyncMock(return_value={
        'id': 'test_order_124',
        'symbol': 'BTC/USDT',
        'side': 'buy',
        'amount': 0.1,
        'price': None,
        'status': 'filled',
        'filled': 0.1,
        'remaining': 0.0,
        'timestamp': 1640995200000
    })
    mock_exchange.cancel_order = AsyncMock(return_value={
        'id': 'test_order_123',
        'status': 'cancelled'
    })
    mock_exchange.fetch_order = AsyncMock(return_value={
        'id': 'test_order_123',
        'status': 'filled',
        'filled': 0.1,
        'remaining': 0.0
    })
    mock_exchange.fetch_open_orders = AsyncMock(return_value=[])
    mock_exchange.fetch_my_trades = AsyncMock(return_value=[])
    mock_exchange.close = AsyncMock()
    
    # Mock properties
    mock_exchange.markets = {
        'BTC/USDT': {
            'id': 'BTCUSDT',
            'symbol': 'BTC/USDT',
            'base': 'BTC',
            'quote': 'USDT',
            'active': True
        }
    }
    
    return mock_exchange


@pytest.fixture
def sample_trade_data():
    """Sample trade data for testing."""
    return {
        'id': 'trade_123456',
        'order_id': 'order_123',
        'symbol': 'BTC/USDT',
        'side': 'buy',
        'amount': Decimal('0.1'),
        'price': Decimal('47500.50'),
        'cost': Decimal('4750.05'),
        'fee': {
            'currency': 'USDT',
            'cost': Decimal('4.75005')
        },
        'timestamp': 1640995200000,
        'datetime': '2022-01-01T00:00:00.000Z',
        'exchange': 'binance'
    }


@pytest.fixture
def sample_order_data():
    """Sample order data for testing."""
    return {
        'id': 'order_123',
        'symbol': 'BTC/USDT',
        'side': 'buy',
        'amount': Decimal('0.1'),
        'price': Decimal('47500.0'),
        'filled': Decimal('0.0'),
        'remaining': Decimal('0.1'),
        'status': 'open',
        'type': 'limit',
        'timestamp': 1640995200000,
        'datetime': '2022-01-01T00:00:00.000Z',
        'exchange': 'binance'
    }


@pytest.fixture
def websocket_test_messages():
    """Sample WebSocket messages for testing."""
    return {
        'binance_orderbook_update': {
            'stream': 'btcusdt@depth',
            'data': {
                'e': 'depthUpdate',
                's': 'BTCUSDT',
                'U': 157,
                'u': 160,
                'b': [['47500.00', '1.00000000']],
                'a': [['47501.00', '0.50000000']]
            }
        },
        'bybit_orderbook_snapshot': {
            'topic': 'orderbook.1.BTCUSDT',
            'type': 'snapshot',
            'data': {
                'symbol': 'BTCUSDT',
                'bids': [['47500', '1.0'], ['47499', '0.5']],
                'asks': [['47501', '1.0'], ['47502', '0.5']]
            }
        },
        'heartbeat': {
            'ping': 1640995200000
        },
        'error_message': {
            'error': {
                'code': -1121,
                'msg': 'Invalid symbol'
            }
        }
    }


@pytest_asyncio.fixture
async def orderbook_btc_usdt(sample_orderbook_data):
    """Create OrderBook instance for BTC/USDT."""
    orderbook = OrderBook('BTC/USDT', 'binance')
    
    # Initialize with sample data
    data = sample_orderbook_data['binance_btc_usdt']
    for price, amount in data['bids']:
        orderbook.update_bid(Decimal(price), Decimal(amount))
    
    for price, amount in data['asks']:
        orderbook.update_ask(Decimal(price), Decimal(amount))
    
    return orderbook


@pytest.fixture
def rate_limit_configs():
    """Rate limit configurations for testing."""
    return {
        'binance': {
            'default': RateLimit(1200, 60),  # 1200 weight per minute
            'order': RateLimit(10, 1),
            'query': RateLimit(20, 1)
        },
        'bybit': {
            'default': RateLimit(600, 5),   # 600 requests per 5 seconds
            'order': RateLimit(100, 5),
            'query': RateLimit(600, 5)
        }
    }


# Pytest configuration
def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests"
    )
    config.addinivalue_line(
        "markers", "unit: marks tests as unit tests"
    )
    config.addinivalue_line(
        "markers", "exchange: marks tests that require exchange API access"
    )


def pytest_addoption(parser):
    """Add custom command line options."""
    parser.addoption(
        "--test-symbol",
        action="store",
        default="BTC/USDT",
        help="Symbol to use for exchange tests (e.g., 'BTC/USDT', 'ETH/USDT')"
    )
    parser.addoption(
        "--alt-symbol",
        action="store",
        default="ETH/USDT",
        help="Alternative symbol to use for exchange tests"
    )


@pytest.fixture
def test_symbol_from_cli(request):
    """Get test symbol from command line option."""
    return request.config.getoption("--test-symbol")


@pytest.fixture
def alt_symbol_from_cli(request):
    """Get alternative test symbol from command line option."""
    return request.config.getoption("--alt-symbol")


@pytest.fixture
def test_symbols(test_symbol_from_cli, alt_symbol_from_cli):
    """Provide test symbols for different exchanges."""
    default_symbol = test_symbol_from_cli
    alt_symbol = alt_symbol_from_cli
    perp_symbol = default_symbol.replace('/USDT', '/USDT:USDT') if '/USDT' in default_symbol else default_symbol
    
    return {
        'binance': default_symbol,
        'binance_perp': perp_symbol,
        'bybit': default_symbol,
        'bybit_perp': perp_symbol,
        'hyperliquid': default_symbol,
        'mexc': default_symbol,
        'gateio': default_symbol,
        'bitget': default_symbol,
        'default': default_symbol,
        'alt': alt_symbol
    }


def pytest_collection_modifyitems(config, items):
    """Automatically mark tests based on their location."""
    for item in items:
        # Mark integration tests
        if "integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)
        
        # Mark unit tests
        if "unit" in str(item.fspath):
            item.add_marker(pytest.mark.unit)
        
        # Mark slow tests (those that involve actual network calls)
        if any(marker in item.name.lower() for marker in ['network', 'api', 'real']):
            item.add_marker(pytest.mark.slow)


# Custom pytest plugins for async testing
pytest_plugins = ["pytest_asyncio"]
