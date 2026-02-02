"""
Integration tests for exchange connectors.

Tests the exchange connector implementations with mocked ccxt exchanges
to verify correct API integration and data normalization.
"""

import pytest
import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock, patch, Mock
from datetime import datetime

from exchanges.connectors.binance import BinanceConnector
from exchanges.connectors.bybit import BybitConnector
from exchanges.connectors.hyperliquid import HyperliquidConnector
from exchanges.connectors.mexc import MexcConnector
from exchanges.connectors.gateio import GateIOConnector
from exchanges.connectors.bitget import BitgetConnector
from exchanges.base_connector import OrderType, OrderStatus


class TestBinanceConnector:
    """Test Binance connector integration."""
    
    @pytest.fixture
    def connector(self, binance_config, mock_ccxt_exchange):
        """Create Binance connector with mocked ccxt exchange."""
        connector = BinanceConnector(binance_config)
        
        # Replace the ccxt exchange with mock
        connector.exchange = mock_ccxt_exchange
        
        return connector
    
    @pytest.mark.asyncio
    async def test_connect(self, connector, mock_ccxt_exchange):
        """Test Binance connection."""
        result = await connector.connect()
        
        assert result is True
        assert connector.connected is True
        mock_ccxt_exchange.load_markets.assert_called_once()
        mock_ccxt_exchange.fetch_time.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_disconnect(self, connector, mock_ccxt_exchange):
        """Test Binance disconnection."""
        connector.connected = True
        
        result = await connector.disconnect()
        
        assert result is True
        assert connector.connected is False
        mock_ccxt_exchange.close.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_balance(self, connector, mock_ccxt_exchange):
        """Test getting balance."""
        with patch.object(connector, '_check_rate_limit', return_value=True):
            balance = await connector.get_balance()
        
        assert 'USDT' in balance
        assert balance['USDT'] == Decimal('1000.0')
        assert 'BTC' in balance
        assert balance['BTC'] == Decimal('0.1')
        
        mock_ccxt_exchange.fetch_balance.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_balance_specific_currency(self, connector, mock_ccxt_exchange):
        """Test getting balance for specific currency."""
        with patch.object(connector, '_check_rate_limit', return_value=True):
            balance = await connector.get_balance('USDT')
        
        assert balance == {'USDT': Decimal('1000.0')}
    
    @pytest.mark.asyncio
    async def test_get_orderbook(self, connector, mock_ccxt_exchange):
        """Test getting orderbook."""
        with patch.object(connector, '_check_rate_limit', return_value=True):
            orderbook = await connector.get_orderbook('BTC/USDT')
        
        assert orderbook['symbol'] == 'BTC/USDT'
        assert orderbook['exchange'] == 'binance'
        assert len(orderbook['bids']) == 2
        assert len(orderbook['asks']) == 2
        
        # Check data types
        assert isinstance(orderbook['bids'][0][0], Decimal)
        assert isinstance(orderbook['bids'][0][1], Decimal)
        
        mock_ccxt_exchange.fetch_order_book.assert_called_once_with('BTCUSDT', 100)
    
    @pytest.mark.asyncio
    async def test_place_limit_order(self, connector, mock_ccxt_exchange):
        """Test placing limit order."""
        with patch.object(connector, '_check_rate_limit', return_value=True):
            order = await connector.place_order(
                'BTC/USDT',
                'buy',
                Decimal('0.1'),
                Decimal('47500.0'),
                OrderType.LIMIT
            )
        
        assert order['id'] == 'test_order_123'
        assert order['symbol'] == 'BTC/USDT'
        assert order['side'] == 'buy'
        assert order['amount'] == Decimal('0.1')
        assert order['price'] == Decimal('47500.0')
        assert order['exchange'] == 'binance'
        
        mock_ccxt_exchange.create_limit_order.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_place_market_order(self, connector, mock_ccxt_exchange):
        """Test placing market order."""
        with patch.object(connector, '_check_rate_limit', return_value=True):
            order = await connector.place_order(
                'BTC/USDT',
                'buy',
                Decimal('0.1'),
                None,
                OrderType.MARKET
            )
        
        assert order['id'] == 'test_order_124'
        assert order['type'] == 'market'
        
        mock_ccxt_exchange.create_market_order.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_cancel_order(self, connector, mock_ccxt_exchange):
        """Test cancelling order."""
        with patch.object(connector, '_check_rate_limit', return_value=True):
            result = await connector.cancel_order('test_order_123', 'BTC/USDT')
        
        assert result['id'] == 'test_order_123'
        assert result['status'] == OrderStatus.CANCELLED
        
        mock_ccxt_exchange.cancel_order.assert_called_once_with('test_order_123', 'BTCUSDT')
    
    @pytest.mark.asyncio
    async def test_get_order_status(self, connector, mock_ccxt_exchange):
        """Test getting order status."""
        with patch.object(connector, '_check_rate_limit', return_value=True):
            status = await connector.get_order_status('test_order_123', 'BTC/USDT')
        
        assert status['id'] == 'test_order_123'
        assert status['status'] == OrderStatus.FILLED
        
        mock_ccxt_exchange.fetch_order.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_rate_limit_exceeded(self, connector, mock_ccxt_exchange):
        """Test behavior when rate limit is exceeded."""
        with patch.object(connector, '_check_rate_limit', return_value=False):
            with pytest.raises(Exception, match="Rate limit exceeded"):
                await connector.get_balance()
    
    def test_normalize_symbol(self, connector):
        """Test symbol normalization."""
        assert connector.normalize_symbol('BTC/USDT') == 'BTCUSDT'
        assert connector.normalize_symbol('ETH/BTC') == 'ETHBTC'
    
    @pytest.mark.asyncio
    async def test_health_check(self, connector, mock_ccxt_exchange):
        """Test health check."""
        connector.connected = True
        
        health = await connector.health_check()
        
        assert health['status'] == 'healthy'
        assert health['connected'] is True
        assert 'latency_ms' in health


class TestBybitConnector:
    """Test Bybit connector integration."""
    
    @pytest.fixture
    def connector(self, bybit_config, mock_ccxt_exchange):
        """Create Bybit connector with mocked ccxt exchange."""
        connector = BybitConnector(bybit_config)
        connector.exchange = mock_ccxt_exchange
        return connector
    
    @pytest.mark.asyncio
    async def test_connect_spot(self, connector, mock_ccxt_exchange):
        """Test Bybit spot connection."""
        result = await connector.connect()
        
        assert result is True
        assert connector.connected is True
        mock_ccxt_exchange.load_markets.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_linear_futures_config(self, bybit_config, mock_ccxt_exchange):
        """Test Bybit linear futures configuration."""
        bybit_config['market_type'] = 'linear'
        connector = BybitConnector(bybit_config)
        
        assert connector.market_type == 'linear'
    
    @pytest.mark.asyncio
    async def test_get_positions_spot(self, connector):
        """Test getting positions for spot market (should return empty)."""
        positions = await connector.get_positions()
        assert positions == []
    
    @pytest.mark.asyncio
    async def test_get_positions_futures(self, bybit_config, mock_ccxt_exchange):
        """Test getting positions for futures market."""
        bybit_config['market_type'] = 'linear'
        connector = BybitConnector(bybit_config)
        connector.exchange = mock_ccxt_exchange
        
        # Mock positions
        mock_ccxt_exchange.fetch_positions.return_value = [{
            'symbol': 'BTC/USDT',
            'contracts': 0.1,
            'entryPrice': 47500.0,
            'markPrice': 47550.0,
            'unrealizedPnl': 5.0,
            'percentage': 0.1,
            'timestamp': 1640995200000
        }]
        
        with patch.object(connector, '_check_rate_limit', return_value=True):
            positions = await connector.get_positions()
        
        assert len(positions) == 1
        assert positions[0]['symbol'] == 'BTC/USDT'
        assert positions[0]['side'] == 'long'
        assert positions[0]['size'] == Decimal('0.1')
    
    def test_normalize_symbol_spot(self, connector):
        """Test symbol normalization for spot."""
        assert connector.normalize_symbol('BTC/USDT') == 'BTC/USDT'
    
    def test_normalize_symbol_linear(self, bybit_config):
        """Test symbol normalization for linear futures."""
        bybit_config['market_type'] = 'linear'
        connector = BybitConnector(bybit_config)
        
        assert connector.normalize_symbol('BTC/USD:USD') == 'BTCUSDT'
        assert connector.normalize_symbol('BTC/USDT') == 'BTCUSDT'


class TestHyperliquidConnector:
    """Test Hyperliquid connector integration."""
    
    @pytest.fixture
    def connector(self, hyperliquid_config, mock_ccxt_exchange):
        """Create Hyperliquid connector with mocked ccxt exchange."""
        connector = HyperliquidConnector(hyperliquid_config)
        connector.exchange = mock_ccxt_exchange
        return connector
    
    @pytest.mark.asyncio
    async def test_connect(self, connector, mock_ccxt_exchange):
        """Test Hyperliquid connection."""
        result = await connector.connect()
        
        assert result is True
        assert connector.connected is True
        assert connector.market_type == 'future'
    
    def test_normalize_symbol(self, connector):
        """Test Hyperliquid symbol normalization."""
        assert connector.normalize_symbol('BTC/USD:USD') == 'BTC-USD'
        assert connector.normalize_symbol('BTC/USDT') == 'BTC-USDT'
    
    def test_denormalize_symbol(self, connector):
        """Test Hyperliquid symbol denormalization."""
        assert connector.denormalize_symbol('BTC-USD') == 'BTC/USD:USD'
    
    @pytest.mark.asyncio
    async def test_place_order_with_reduce_only(self, connector, mock_ccxt_exchange):
        """Test placing order with reduce-only parameter."""
        with patch.object(connector, '_check_rate_limit', return_value=True):
            await connector.place_order(
                'BTC/USD:USD',
                'sell',
                Decimal('0.1'),
                Decimal('47500.0'),
                OrderType.LIMIT,
                {'reduce_only': True}
            )
        
        # Check that reduce_only was converted to reduceOnly
        call_args = mock_ccxt_exchange.create_limit_order.call_args
        params = call_args[1]['params'] if len(call_args) > 1 else call_args[0][5]
        assert 'reduceOnly' in params
        assert params['reduceOnly'] is True


class TestMexcConnector:
    """Test MEXC connector integration."""
    
    @pytest.fixture
    def connector(self, mock_ccxt_exchange):
        """Create MEXC connector with mocked ccxt exchange."""
        config = {
            'name': 'mexc',
            'api_key': 'test_key',
            'secret': 'test_secret'
        }
        connector = MexcConnector(config)
        connector.exchange = mock_ccxt_exchange
        return connector
    
    @pytest.mark.asyncio
    async def test_connect(self, connector, mock_ccxt_exchange):
        """Test MEXC connection."""
        result = await connector.connect()
        
        assert result is True
        assert connector.market_type == 'spot'
    
    @pytest.mark.asyncio
    async def test_get_positions(self, connector):
        """Test getting positions (should return empty for spot)."""
        positions = await connector.get_positions()
        assert positions == []
    
    def test_normalize_symbol(self, connector):
        """Test MEXC symbol normalization."""
        assert connector.normalize_symbol('BTC/USDT') == 'BTC/USDT'


class TestGateIOConnector:
    """Test Gate.io connector integration."""
    
    @pytest.fixture
    def connector(self, mock_ccxt_exchange):
        """Create Gate.io connector with mocked ccxt exchange."""
        config = {
            'name': 'gateio',
            'api_key': 'test_key',
            'secret': 'test_secret',
            'sandbox': True
        }
        connector = GateIOConnector(config)
        connector.exchange = mock_ccxt_exchange
        return connector
    
    def test_normalize_symbol(self, connector):
        """Test Gate.io symbol normalization."""
        assert connector.normalize_symbol('BTC/USDT') == 'BTC_USDT'
        assert connector.normalize_symbol('ETH/BTC') == 'ETH_BTC'
    
    def test_denormalize_symbol(self, connector):
        """Test Gate.io symbol denormalization."""
        assert connector.denormalize_symbol('BTC_USDT') == 'BTC/USDT'
        assert connector.denormalize_symbol('ETH_BTC') == 'ETH/BTC'
    
    @pytest.mark.asyncio
    async def test_place_order_with_time_in_force(self, connector, mock_ccxt_exchange):
        """Test placing order with time in force parameter."""
        with patch.object(connector, '_check_rate_limit', return_value=True):
            await connector.place_order(
                'BTC/USDT',
                'buy',
                Decimal('0.1'),
                Decimal('47500.0'),
                OrderType.LIMIT,
                {'time_in_force': 'IOC'}
            )
        
        # Check that time_in_force was converted to timeInForce
        call_args = mock_ccxt_exchange.create_limit_order.call_args
        params = call_args[1]['params'] if len(call_args) > 1 else call_args[0][5]
        assert 'timeInForce' in params
        assert params['timeInForce'] == 'IOC'


class TestBitgetConnector:
    """Test Bitget connector integration."""
    
    @pytest.fixture
    def connector(self, mock_ccxt_exchange):
        """Create Bitget connector with mocked ccxt exchange."""
        config = {
            'name': 'bitget',
            'api_key': 'test_key',
            'secret': 'test_secret',
            'passphrase': 'test_passphrase',
            'sandbox': True
        }
        connector = BitgetConnector(config)
        connector.exchange = mock_ccxt_exchange
        return connector
    
    @pytest.mark.asyncio
    async def test_connect(self, connector, mock_ccxt_exchange):
        """Test Bitget connection."""
        result = await connector.connect()
        
        assert result is True
        assert connector.market_type == 'spot'
    
    def test_passphrase_required(self):
        """Test that Bitget requires passphrase in config."""
        config = {
            'name': 'bitget',
            'api_key': 'test_key',
            'secret': 'test_secret',
            'passphrase': 'test_passphrase'
        }
        
        # Should not raise exception
        connector = BitgetConnector(config)
        assert connector.passphrase == 'test_passphrase'


class TestExchangeConnectorErrorHandling:
    """Test error handling across all connectors."""
    
    @pytest.mark.asyncio
    async def test_connection_failure(self, binance_config):
        """Test handling connection failures."""
        connector = BinanceConnector(binance_config)
        
        # Mock ccxt to raise exception
        with patch('ccxt.pro.binance') as mock_ccxt:
            mock_exchange = AsyncMock()
            mock_exchange.load_markets.side_effect = Exception("Connection failed")
            mock_ccxt.return_value = mock_exchange
            
            connector.exchange = mock_exchange
            
            result = await connector.connect()
            assert result is False
            assert connector.connected is False
    
    @pytest.mark.asyncio
    async def test_api_error_handling(self, binance_config, mock_ccxt_exchange):
        """Test handling API errors."""
        connector = BinanceConnector(binance_config)
        connector.exchange = mock_ccxt_exchange
        
        # Mock API to raise exception
        mock_ccxt_exchange.fetch_balance.side_effect = Exception("API Error")
        
        with patch.object(connector, '_check_rate_limit', return_value=True):
            balance = await connector.get_balance()
        
        # Should return empty dict on error
        assert balance == {}
    
    @pytest.mark.asyncio
    async def test_invalid_order_parameters(self, binance_config, mock_ccxt_exchange):
        """Test handling invalid order parameters."""
        connector = BinanceConnector(binance_config)
        connector.exchange = mock_ccxt_exchange
        
        with patch.object(connector, '_check_rate_limit', return_value=True):
            # Should raise exception for limit order without price
            with pytest.raises(ValueError, match="Price required for limit orders"):
                await connector.place_order(
                    'BTC/USDT',
                    'buy',
                    Decimal('0.1'),
                    None,  # No price for limit order
                    OrderType.LIMIT
                )
    
    @pytest.mark.asyncio
    async def test_unsupported_order_type(self, binance_config, mock_ccxt_exchange):
        """Test handling unsupported order types."""
        connector = BinanceConnector(binance_config)
        connector.exchange = mock_ccxt_exchange
        
        with patch.object(connector, '_check_rate_limit', return_value=True):
            with pytest.raises(ValueError, match="Unsupported order type"):
                await connector.place_order(
                    'BTC/USDT',
                    'buy',
                    Decimal('0.1'),
                    Decimal('47500.0'),
                    'unsupported_type'
                )


class TestConcurrentOperations:
    """Test concurrent operations across connectors."""
    
    @pytest.mark.asyncio
    async def test_concurrent_balance_requests(self, binance_config, mock_ccxt_exchange):
        """Test concurrent balance requests."""
        connector = BinanceConnector(binance_config)
        connector.exchange = mock_ccxt_exchange
        
        with patch.object(connector, '_check_rate_limit', return_value=True):
            # Create multiple concurrent requests
            tasks = [connector.get_balance() for _ in range(5)]
            results = await asyncio.gather(*tasks)
        
        # All should succeed
        assert len(results) == 5
        for balance in results:
            assert 'USDT' in balance
    
    @pytest.mark.asyncio
    async def test_concurrent_orderbook_requests(self, binance_config, mock_ccxt_exchange):
        """Test concurrent orderbook requests."""
        connector = BinanceConnector(binance_config)
        connector.exchange = mock_ccxt_exchange
        
        symbols = ['BTC/USDT', 'ETH/USDT', 'LTC/USDT']
        
        with patch.object(connector, '_check_rate_limit', return_value=True):
            tasks = [connector.get_orderbook(symbol) for symbol in symbols]
            results = await asyncio.gather(*tasks)
        
        assert len(results) == 3
        for orderbook in results:
            assert 'bids' in orderbook
            assert 'asks' in orderbook


class TestDataNormalization:
    """Test data normalization across different exchanges."""
    
    def test_order_status_normalization(self):
        """Test order status normalization."""
        connector = BinanceConnector({'name': 'test'})
        
        assert connector._normalize_status('open') == OrderStatus.OPEN
        assert connector._normalize_status('closed') == OrderStatus.FILLED
        assert connector._normalize_status('canceled') == OrderStatus.CANCELLED
        assert connector._normalize_status('cancelled') == OrderStatus.CANCELLED
        assert connector._normalize_status('rejected') == OrderStatus.REJECTED
    
    def test_decimal_precision_preservation(self, binance_config, mock_ccxt_exchange):
        """Test that decimal precision is preserved in normalization."""
        connector = BinanceConnector(binance_config)
        
        # Mock ccxt order response with float values
        ccxt_order = {
            'id': 'test',
            'symbol': 'BTC/USDT',
            'side': 'buy',
            'amount': 0.12345678,
            'price': 47500.123456,
            'filled': 0.1,
            'remaining': 0.02345678,
            'status': 'open'
        }
        
        normalized = connector._normalize_order(ccxt_order)
        
        # Check that Decimal precision is maintained
        assert isinstance(normalized['amount'], Decimal)
        assert isinstance(normalized['price'], Decimal)
        assert normalized['amount'] == Decimal('0.12345678')
        assert normalized['price'] == Decimal('47500.123456')
