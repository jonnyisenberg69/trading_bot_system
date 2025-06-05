"""
Unit tests for position tracking system.
"""

import pytest
import asyncio
import tempfile
import os
from decimal import Decimal
from datetime import datetime

from order_management.tracking import Position, PositionManager
from order_management.order import Order, OrderSide, OrderStatus, OrderType


@pytest.fixture
def position():
    """Create a test position."""
    return Position(exchange="binance_spot", symbol="BTC/USDT")


@pytest.fixture
def position_manager():
    """Create a test position manager with temporary data directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        manager = PositionManager(data_dir=tmpdir)
        yield manager


def test_position_init(position):
    """Test position initialization."""
    assert position.exchange == "binance_spot"
    assert position.symbol == "BTC/USDT"
    assert position.is_perpetual is False
    assert position.p1 == Decimal('0')
    assert position.p2 == Decimal('0')
    assert position.p1_fee == Decimal('0')
    assert position.p2_fee == Decimal('0')
    assert position.size == Decimal('0')
    assert position.value == Decimal('0')
    assert position.avg_price is None
    assert position.is_open is False
    assert position.side is None


def test_position_update_from_trade(position):
    """Test updating position from trade data."""
    # Buy 1 BTC at 50,000 USDT
    trade1 = {
        'id': '1',
        'side': 'buy',
        'amount': 1.0,
        'price': 50000.0,
        'cost': 50000.0,
        'fee': {
            'cost': 0.001,  # 0.001 BTC fee
            'currency': 'BTC'
        }
    }
    
    position.update_from_trade(trade1)
    
    # Check position after buy
    assert position.p1 == Decimal('1')
    assert position.p2 == Decimal('-50000')
    assert position.p1_fee == Decimal('0.001')
    assert position.p2_fee == Decimal('0')
    assert position.size == Decimal('1')
    assert position.value == Decimal('50000')
    assert position.avg_price == Decimal('50000')
    assert position.is_open is True
    assert position.side == 'long'
    
    # Sell 0.5 BTC at 52,000 USDT
    trade2 = {
        'id': '2',
        'side': 'sell',
        'amount': 0.5,
        'price': 52000.0,
        'cost': 26000.0,
        'fee': {
            'cost': 13.0,  # 13 USDT fee
            'currency': 'USDT'
        }
    }
    
    position.update_from_trade(trade2)
    
    # Check position after partial sell
    assert position.p1 == Decimal('0.5')
    assert position.p2 == Decimal('-24000')  # -50000 + 26000
    assert position.p1_fee == Decimal('0.001')
    assert position.p2_fee == Decimal('13')
    assert position.size == Decimal('0.5')
    assert position.value == Decimal('24000')
    assert position.avg_price == Decimal('48026')  # (-24000 - 13) / 0.5
    assert position.is_open is True
    assert position.side == 'long'
    
    # Sell remaining 0.5 BTC at 48,000 USDT
    trade3 = {
        'id': '3',
        'side': 'sell',
        'amount': 0.5,
        'price': 48000.0,
        'cost': 24000.0,
        'fee': {
            'cost': 12.0,  # 12 USDT fee
            'currency': 'USDT'
        }
    }
    
    position.update_from_trade(trade3)
    
    # Check position after full sell (should be closed)
    assert position.p1 == Decimal('0')
    assert position.p2 == Decimal('0')  # -50000 + 26000 + 24000
    assert position.p1_fee == Decimal('0.001')
    assert position.p2_fee == Decimal('25')  # 13 + 12
    assert position.size == Decimal('0')
    assert position.value == Decimal('0')
    assert position.avg_price is None  # No position
    assert position.is_open is False
    assert position.side is None


def test_position_serialization(position):
    """Test position serialization and deserialization."""
    # Create position with some trades
    position.update_from_trade({
        'id': '1',
        'side': 'buy',
        'amount': 1.0,
        'price': 50000.0,
        'cost': 50000.0,
        'fee': {
            'cost': 0.001,
            'currency': 'BTC'
        }
    })
    
    # Serialize
    data = position.to_dict()
    
    # Verify serialized data
    assert data['exchange'] == "binance_spot"
    assert data['symbol'] == "BTC/USDT"
    assert data['p1'] == 1.0
    assert data['p2'] == -50000.0
    assert data['p1_fee'] == 0.001
    assert data['p2_fee'] == 0.0
    assert data['avg_price'] == 50000.0
    
    # Deserialize
    new_position = Position.from_dict(data)
    
    # Verify deserialized position
    assert new_position.exchange == "binance_spot"
    assert new_position.symbol == "BTC/USDT"
    assert new_position.p1 == Decimal('1')
    assert new_position.p2 == Decimal('-50000')
    assert new_position.p1_fee == Decimal('0.001')
    assert new_position.p2_fee == Decimal('0')
    assert new_position.avg_price == Decimal('50000')


@pytest.mark.asyncio
async def test_position_manager_update_from_order(position_manager):
    """Test updating position manager from an order fill."""
    # Create a test order
    order = Order(
        id="order1",
        client_order_id="client_order1",
        symbol="ETH/USDT",
        side=OrderSide.BUY,
        amount=Decimal('10'),
        price=Decimal('3000'),
        order_type=OrderType.LIMIT,
        exchange="binance_spot",
        status=OrderStatus.FILLED
    )
    
    # Fill data
    fill_data = {
        'id': 'fill1',
        'amount': 10.0,
        'price': 3000.0,
        'cost': 30000.0,
        'fee': {
            'cost': 15.0,
            'currency': 'USDT'
        }
    }
    
    # Update position
    await position_manager.update_from_order(order, fill_data)
    
    # Get position
    position = position_manager.get_position("binance_spot", "ETH/USDT")
    
    # Check position
    assert position is not None
    assert position.exchange == "binance_spot"
    assert position.symbol == "ETH/USDT"
    assert position.p1 == Decimal('10')
    assert position.p2 == Decimal('-30000')
    assert position.p2_fee == Decimal('15')
    assert position.avg_price == Decimal('3001.5')  # (-30000 - 15) / 10
    assert position.is_open is True
    assert position.side == 'long'


@pytest.mark.asyncio
async def test_position_manager_multiple_exchanges(position_manager):
    """Test tracking positions across multiple exchanges."""
    # Update with trades on different exchanges
    
    # Binance Spot: Buy 1 BTC
    await position_manager.update_from_trade("binance_spot", {
        'symbol': 'BTC/USDT',
        'id': '1',
        'side': 'buy',
        'amount': 1.0,
        'price': 50000.0,
        'cost': 50000.0,
        'fee': {
            'cost': 0.001,
            'currency': 'BTC'
        }
    })
    
    # Bybit Perp: Sell 0.5 BTC (short)
    await position_manager.update_from_trade("bybit_perp", {
        'symbol': 'BTC/USDT',
        'id': '2',
        'side': 'sell',
        'amount': 0.5,
        'price': 51000.0,
        'cost': 25500.0,
        'fee': {
            'cost': 12.75,
            'currency': 'USDT'
        }
    })
    
    # Check individual positions
    binance_pos = position_manager.get_position("binance_spot", "BTC/USDT")
    bybit_pos = position_manager.get_position("bybit_perp", "BTC/USDT")
    
    assert binance_pos.p1 == Decimal('1')
    assert binance_pos.p2 == Decimal('-50000')
    assert binance_pos.side == 'long'
    
    assert bybit_pos.p1 == Decimal('-0.5')
    assert bybit_pos.p2 == Decimal('25500')
    assert bybit_pos.side == 'short'
    
    # Check net position
    net_position = position_manager.get_net_position("BTC/USDT")
    
    assert net_position['p1'] == Decimal('0.5')  # 1 - 0.5
    assert net_position['p2'] == Decimal('-24500')  # -50000 + 25500
    assert net_position['side'] == 'long'
    assert net_position['avg_price'] == Decimal('49000')  # -(-24500) / 0.5


@pytest.mark.asyncio
async def test_position_manager_persistence(position_manager):
    """Test position persistence to disk."""
    # Add a position
    await position_manager.update_from_trade("binance_spot", {
        'symbol': 'BTC/USDT',
        'id': '1',
        'side': 'buy',
        'amount': 1.0,
        'price': 50000.0,
        'cost': 50000.0,
        'fee': {
            'cost': 0.001,
            'currency': 'BTC'
        }
    })
    
    # Save positions
    await position_manager.save_positions()
    
    # Create new manager with same data directory
    new_manager = PositionManager(data_dir=position_manager.data_dir)
    
    # Load positions
    await new_manager.load_positions()
    
    # Check position was loaded correctly
    position = new_manager.get_position("binance_spot", "BTC/USDT")
    
    assert position is not None
    assert position.p1 == Decimal('1')
    assert position.p2 == Decimal('-50000')
    assert position.p1_fee == Decimal('0.001')
    assert position.avg_price == Decimal('50000')


@pytest.mark.asyncio
async def test_position_reset(position_manager):
    """Test resetting positions."""
    # Add positions
    await position_manager.update_from_trade("binance_spot", {
        'symbol': 'BTC/USDT',
        'id': '1',
        'side': 'buy',
        'amount': 1.0,
        'price': 50000.0,
        'cost': 50000.0
    })
    
    await position_manager.update_from_trade("bybit_spot", {
        'symbol': 'ETH/USDT',
        'id': '2',
        'side': 'buy',
        'amount': 10.0,
        'price': 3000.0,
        'cost': 30000.0
    })
    
    # Reset specific position
    result = await position_manager.reset_position("binance_spot", "BTC/USDT")
    assert result is True
    
    # Check position was reset
    assert position_manager.get_position("binance_spot", "BTC/USDT") is None
    assert position_manager.get_position("bybit_spot", "ETH/USDT") is not None
    
    # Reset all positions
    await position_manager.reset_all_positions()
    
    # Check all positions were reset
    assert position_manager.get_position("bybit_spot", "ETH/USDT") is None
    assert len(position_manager.get_all_positions()) == 0
    assert len(position_manager.symbols) == 0


@pytest.mark.asyncio
async def test_position_manager_summary(position_manager):
    """Test position summary generation."""
    # Add positions
    await position_manager.update_from_trade("binance_spot", {
        'symbol': 'BTC/USDT',
        'id': '1',
        'side': 'buy',
        'amount': 1.0,
        'price': 50000.0,
        'cost': 50000.0
    })
    
    await position_manager.update_from_trade("bybit_perp", {
        'symbol': 'ETH/USDT',
        'id': '2',
        'side': 'sell',
        'amount': 10.0,
        'price': 3000.0,
        'cost': 30000.0
    })
    
    # Get summary
    summary = position_manager.summarize_positions()
    
    # Check summary
    assert summary['total_positions'] == 2
    assert summary['open_positions'] == 2
    assert len(summary['symbols']) == 2
    assert len(summary['exchanges']) == 2
    assert summary['positions_by_exchange']['binance_spot'] == 1
    assert summary['positions_by_exchange']['bybit_perp'] == 1
    assert summary['long_value'] == 50000.0
    assert summary['short_value'] == 30000.0
    assert summary['net_exposure'] == 20000.0  # 50000 - 30000 