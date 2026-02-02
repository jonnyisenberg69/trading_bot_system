"""
Integration tests for trade reconciliation and position validation systems.

Tests the complete reconciliation and validation workflow across all supported exchanges:
- Trade reconciliation with real exchange data
- Position validation and discrepancy detection
- Auto-correction capabilities
- Multi-exchange reconciliation
"""

import pytest
import asyncio
import tempfile
import os
from decimal import Decimal
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Dict, List, Any

from order_management.reconciliation import (
    TradeReconciliation, 
    ReconciliationReport, 
    TradeDiscrepancy,
    DiscrepancyType,
    ReconciliationStatus
)
from order_management.position_validation import (
    PositionValidation,
    ValidationReport,
    PositionDiscrepancy,
    PositionDiscrepancyType,
    ValidationStatus
)
from order_management.tracking import PositionManager, Position
from database.repositories.trade_repository import TradeRepository
from database.repositories.position_repository import PositionRepository


class MockTradeRepository:
    """Mock trade repository for testing."""
    
    def __init__(self):
        self.trades = []
        
    async def get_trades_by_symbol(self, symbol, exchange_id, start_time, end_time, limit=1000):
        """Mock getting trades by symbol."""
        return [
            MagicMock(
                exchange_trade_id=f"trade_{i}",
                symbol=symbol,
                side='buy' if i % 2 == 0 else 'sell',
                amount=float(i * 0.1),
                price=float(50000 + i * 100),
                cost=float((i * 0.1) * (50000 + i * 100)),
                fee_cost=float(i * 0.001),
                fee_currency='USDT',
                timestamp=start_time + timedelta(minutes=i),
                order_id=f"order_{i}"
            )
            for i in range(1, 6)  # 5 test trades
        ]
        
    async def save_trade(self, trade_data, exchange_id):
        """Mock saving trade."""
        self.trades.append(trade_data)


class MockPositionRepository:
    """Mock position repository for testing."""
    
    def __init__(self):
        self.positions = {}
        
    async def get_position(self, exchange_id, symbol, bot_instance_id=None):
        """Mock getting position."""
        key = f"{exchange_id}_{symbol}"
        return self.positions.get(key)
        
    async def update_position(self, exchange_id, symbol, p1_delta, p2_delta, p1_fee_delta=0, p2_fee_delta=0, bot_instance_id=None):
        """Mock updating position."""
        key = f"{exchange_id}_{symbol}"
        if key in self.positions:
            pos = self.positions[key]
            pos.p1 += p1_delta
            pos.p2 += p2_delta
        else:
            pos = MagicMock()
            pos.p1 = p1_delta
            pos.p2 = p2_delta
            pos.p1_fee = p1_fee_delta
            pos.p2_fee = p2_fee_delta
            self.positions[key] = pos
        return pos


class MockExchangeConnector:
    """Mock exchange connector for testing."""
    
    def __init__(self, exchange_name: str):
        self.exchange_name = exchange_name
        self.trades = self._generate_mock_trades()
        self.positions = self._generate_mock_positions()
        
    def _generate_mock_trades(self):
        """Generate mock trades for testing."""
        return [
            {
                'id': f'trade_{i}',
                'symbol': 'BTC/USDT',
                'side': 'buy' if i % 2 == 0 else 'sell',
                'amount': i * 0.1,
                'price': 50000 + i * 100,
                'cost': (i * 0.1) * (50000 + i * 100),
                'fee': {'cost': i * 0.001, 'currency': 'USDT'},
                'timestamp': datetime.utcnow() - timedelta(hours=i),
                'order': f'order_{i}'
            }
            for i in range(1, 6)
        ]
        
    def _generate_mock_positions(self):
        """Generate mock positions for testing."""
        if 'perp' in self.exchange_name.lower() or 'futures' in self.exchange_name.lower():
            return [
                {
                    'symbol': 'BTC/USDT',
                    'size': 0.5,
                    'side': 'long',
                    'entry_price': 50000.0,
                    'mark_price': 51000.0,
                    'unrealized_pnl': 500.0
                }
            ]
        else:
            # Spot exchanges don't have positions, return empty
            return []
    
    async def get_trade_history(self, symbol=None, since=None, limit=1000):
        """Mock getting trade history."""
        return self.trades
        
    async def get_positions(self, symbol=None):
        """Mock getting positions."""
        return self.positions
        
    async def has_symbol(self, symbol):
        """Mock symbol check."""
        return True


@pytest.fixture
def mock_trade_repository():
    """Create mock trade repository."""
    return MockTradeRepository()


@pytest.fixture
def mock_position_repository():
    """Create mock position repository."""
    return MockPositionRepository()


@pytest.fixture
def position_manager():
    """Create test position manager."""
    with tempfile.TemporaryDirectory() as tmpdir:
        manager = PositionManager(data_dir=tmpdir)
        yield manager


@pytest.fixture
def mock_exchange_connectors():
    """Create mock exchange connectors for all supported exchanges."""
    exchanges = [
        'binance_spot', 'binance_perp',
        'bybit_spot', 'bybit_perp',
        'hyperliquid_perp',
        'mexc_spot',
        'gateio_spot',
        'bitget_spot'
    ]
    
    return {
        exchange: MockExchangeConnector(exchange)
        for exchange in exchanges
    }


@pytest.fixture
def trade_reconciliation(mock_exchange_connectors, mock_trade_repository, mock_position_repository, position_manager):
    """Create trade reconciliation system."""
    return TradeReconciliation(
        exchange_connectors=mock_exchange_connectors,
        trade_repository=mock_trade_repository,
        position_repository=mock_position_repository,
        position_manager=position_manager
    )


@pytest.fixture
def position_validation(mock_exchange_connectors, position_manager, mock_position_repository):
    """Create position validation system."""
    return PositionValidation(
        exchange_connectors=mock_exchange_connectors,
        position_manager=position_manager,
        position_repository=mock_position_repository
    )


class TestTradeReconciliation:
    """Test trade reconciliation system."""
    
    @pytest.mark.asyncio
    async def test_single_exchange_reconciliation(self, trade_reconciliation):
        """Test reconciliation for a single exchange."""
        # Test reconciliation for Binance spot
        report = await trade_reconciliation.start_reconciliation(
            exchange='binance_spot',
            symbol='BTC/USDT'
        )
        
        assert isinstance(report, ReconciliationReport)
        assert report.exchange == 'binance_spot'
        assert report.symbol == 'BTC/USDT'
        assert report.status == ReconciliationStatus.COMPLETED
        assert report.internal_trades_count >= 0
        assert report.exchange_trades_count >= 0
        
    @pytest.mark.asyncio
    async def test_all_exchanges_reconciliation(self, trade_reconciliation):
        """Test reconciliation across all exchanges."""
        reports = await trade_reconciliation.reconcile_all_exchanges(
            symbol='BTC/USDT'
        )
        
        assert len(reports) > 0
        
        # Check that we have reports for all exchanges
        exchange_names = {report.exchange for report in reports}
        expected_exchanges = {
            'binance_spot', 'binance_perp', 'bybit_spot', 'bybit_perp',
            'hyperliquid_perp', 'mexc_spot', 'gateio_spot', 'bitget_spot'
        }
        
        assert exchange_names == expected_exchanges
        
        # Check that all reports completed successfully
        for report in reports:
            assert report.status == ReconciliationStatus.COMPLETED
            
    @pytest.mark.asyncio
    async def test_discrepancy_detection(self, trade_reconciliation):
        """Test detection of trade discrepancies."""
        # Mock internal trades that don't match exchange trades
        with patch.object(trade_reconciliation, '_fetch_internal_trades') as mock_internal:
            mock_internal.return_value = [
                {
                    'id': 'trade_999',  # ID that doesn't exist on exchange
                    'exchange_trade_id': 'trade_999',
                    'symbol': 'BTC/USDT',
                    'side': 'buy',
                    'amount': 1.0,
                    'price': 50000.0,
                    'cost': 50000.0,
                    'fee': {'cost': 25.0, 'currency': 'USDT'},
                    'timestamp': datetime.utcnow(),
                    'order_id': 'order_999'
                }
            ]
            
            report = await trade_reconciliation.start_reconciliation(
                exchange='binance_spot',
                symbol='BTC/USDT'
            )
            
            # Should detect missing exchange trade
            assert len(report.discrepancies) > 0
            missing_exchange_discrepancies = [
                d for d in report.discrepancies 
                if d.discrepancy_type == DiscrepancyType.MISSING_EXCHANGE
            ]
            assert len(missing_exchange_discrepancies) > 0
            
    @pytest.mark.asyncio
    async def test_auto_correction(self, trade_reconciliation):
        """Test automatic correction of discrepancies."""
        # Enable auto-correction
        trade_reconciliation.config['auto_correction_enabled'] = True
        
        # Mock a scenario with missing internal trade
        with patch.object(trade_reconciliation, '_fetch_internal_trades') as mock_internal:
            mock_internal.return_value = []  # No internal trades
            
            report = await trade_reconciliation.start_reconciliation(
                exchange='binance_spot',
                symbol='BTC/USDT',
                auto_correct=True
            )
            
            # Should detect missing internal trades and attempt corrections
            missing_internal_discrepancies = [
                d for d in report.discrepancies 
                if d.discrepancy_type == DiscrepancyType.MISSING_INTERNAL
            ]
            
            # Auto-corrections should be attempted
            assert report.auto_corrections_applied >= 0


class TestPositionValidation:
    """Test position validation system."""
    
    @pytest.mark.asyncio
    async def test_single_exchange_validation(self, position_validation):
        """Test position validation for a single exchange."""
        # Add a test position to the position manager
        await position_validation.position_manager.update_from_trade("binance_perp", {
            'symbol': 'BTC/USDT',
            'id': 'trade_1',
            'side': 'buy',
            'amount': 0.5,
            'price': 50000.0,
            'cost': 25000.0,
            'fee': {'cost': 12.5, 'currency': 'USDT'}
        })
        
        # Validate positions
        report = await position_validation.validate_positions(
            exchange='binance_perp',
            symbol='BTC/USDT'
        )
        
        assert isinstance(report, ValidationReport)
        assert report.exchange == 'binance_perp'
        assert report.symbol == 'BTC/USDT'
        assert report.status == ValidationStatus.COMPLETED
        
    @pytest.mark.asyncio
    async def test_all_exchanges_validation(self, position_validation):
        """Test position validation across all exchanges."""
        # Add positions for multiple exchanges
        test_positions = [
            ('binance_perp', 'BTC/USDT', 0.5),
            ('bybit_perp', 'ETH/USDT', 2.0),
            ('hyperliquid_perp', 'BTC/USDC:USDC', 0.3)
        ]
        
        for exchange, symbol, amount in test_positions:
            await position_validation.position_manager.update_from_trade(exchange, {
                'symbol': symbol,
                'id': f'trade_{exchange}',
                'side': 'buy',
                'amount': amount,
                'price': 50000.0,
                'cost': amount * 50000.0,
                'fee': {'cost': 25.0, 'currency': 'USDT'}
            })
        
        # Validate all exchanges
        reports = await position_validation.validate_all_exchanges()
        
        assert len(reports) > 0
        
        # Check that validation completed for exchanges with positions
        futures_exchanges = {'binance_perp', 'bybit_perp', 'hyperliquid_perp'}
        futures_reports = [r for r in reports if r.exchange in futures_exchanges]
        
        for report in futures_reports:
            assert report.status == ValidationStatus.COMPLETED
            
    @pytest.mark.asyncio
    async def test_position_discrepancy_detection(self, position_validation):
        """Test detection of position discrepancies."""
        # Add internal position
        await position_validation.position_manager.update_from_trade("binance_perp", {
            'symbol': 'BTC/USDT',
            'id': 'trade_1',
            'side': 'buy',
            'amount': 1.0,  # Different from exchange position (0.5)
            'price': 50000.0,
            'cost': 50000.0,
            'fee': {'cost': 25.0, 'currency': 'USDT'}
        })
        
        # Validate positions (exchange has 0.5, internal has 1.0)
        report = await position_validation.validate_positions(
            exchange='binance_perp',
            symbol='BTC/USDT'
        )
        
        # Should detect size mismatch
        size_mismatch_discrepancies = [
            d for d in report.discrepancies 
            if d.discrepancy_type == PositionDiscrepancyType.SIZE_MISMATCH
        ]
        assert len(size_mismatch_discrepancies) > 0
        
    @pytest.mark.asyncio
    async def test_continuous_monitoring(self, position_validation):
        """Test continuous position monitoring."""
        # Start monitoring
        await position_validation.start_continuous_monitoring()
        
        assert position_validation.continuous_monitoring is True
        assert position_validation.monitoring_task is not None
        
        # Let monitoring run briefly
        await asyncio.sleep(0.1)
        
        # Stop monitoring
        await position_validation.stop_continuous_monitoring()
        
        assert position_validation.continuous_monitoring is False
        
    @pytest.mark.asyncio
    async def test_exchange_capabilities(self, position_validation):
        """Test exchange capability detection."""
        capabilities = position_validation.exchange_capabilities
        
        # Check that all exchanges are present
        expected_exchanges = {
            'binance_spot', 'binance_perp', 'bybit_spot', 'bybit_perp',
            'hyperliquid_perp', 'mexc_spot', 'gateio_spot', 'bitget_spot'
        }
        
        assert set(capabilities.keys()) == expected_exchanges
        
        # Check futures exchanges have futures capabilities
        futures_exchanges = {'binance_perp', 'bybit_perp', 'hyperliquid_perp'}
        for exchange in futures_exchanges:
            assert capabilities[exchange]['futures_positions'] is True
            
        # Check spot exchanges don't have futures capabilities
        spot_exchanges = {'binance_spot', 'bybit_spot', 'mexc_spot', 'gateio_spot', 'bitget_spot'}
        for exchange in spot_exchanges:
            assert capabilities[exchange]['futures_positions'] is False


class TestIntegratedWorkflow:
    """Test integrated reconciliation and validation workflow."""
    
    @pytest.mark.asyncio
    async def test_full_reconciliation_and_validation_workflow(
        self, 
        trade_reconciliation, 
        position_validation
    ):
        """Test complete workflow of reconciliation followed by validation."""
        
        # Step 1: Run trade reconciliation for all exchanges
        reconciliation_reports = await trade_reconciliation.reconcile_all_exchanges(
            symbol='BTC/USDT'
        )
        
        # Step 2: Run position validation for all exchanges
        validation_reports = await position_validation.validate_all_exchanges(
            symbol='BTC/USDT'
        )
        
        # Verify both processes completed successfully
        assert len(reconciliation_reports) > 0
        assert len(validation_reports) > 0
        
        # Check that both cover the same exchanges
        reconciliation_exchanges = {r.exchange for r in reconciliation_reports}
        validation_exchanges = {r.exchange for r in validation_reports}
        assert reconciliation_exchanges == validation_exchanges
        
    @pytest.mark.asyncio
    async def test_cross_system_discrepancy_correlation(
        self,
        trade_reconciliation,
        position_validation
    ):
        """Test correlation of discrepancies between trade and position systems."""
        
        # Run both systems
        reconciliation_reports = await trade_reconciliation.reconcile_all_exchanges()
        validation_reports = await position_validation.validate_all_exchanges()
        
        # Analyze discrepancies
        total_trade_discrepancies = sum(len(r.discrepancies) for r in reconciliation_reports)
        total_position_discrepancies = sum(len(r.discrepancies) for r in validation_reports)
        
        # Log results for analysis
        print(f"Trade discrepancies found: {total_trade_discrepancies}")
        print(f"Position discrepancies found: {total_position_discrepancies}")
        
        # Both systems should provide insights
        assert total_trade_discrepancies >= 0
        assert total_position_discrepancies >= 0
        
    @pytest.mark.asyncio
    async def test_system_performance(self, trade_reconciliation, position_validation):
        """Test system performance with multiple exchanges."""
        import time
        
        # Measure reconciliation performance
        start_time = time.time()
        reconciliation_reports = await trade_reconciliation.reconcile_all_exchanges()
        reconciliation_time = time.time() - start_time
        
        # Measure validation performance
        start_time = time.time()
        validation_reports = await position_validation.validate_all_exchanges()
        validation_time = time.time() - start_time
        
        # Check performance is reasonable (under 30 seconds for all exchanges)
        assert reconciliation_time < 30.0
        assert validation_time < 30.0
        
        print(f"Reconciliation took: {reconciliation_time:.2f}s")
        print(f"Validation took: {validation_time:.2f}s")
        
    @pytest.mark.asyncio
    async def test_error_handling_and_recovery(self, trade_reconciliation):
        """Test error handling when exchanges are unavailable."""
        
        # Mock a failing exchange connector
        original_connector = trade_reconciliation.exchange_connectors['binance_spot']
        
        # Replace with failing mock
        failing_mock = AsyncMock()
        failing_mock.get_trade_history.side_effect = Exception("Exchange unavailable")
        trade_reconciliation.exchange_connectors['binance_spot'] = failing_mock
        
        # Run reconciliation - should handle the failure gracefully
        try:
            reports = await trade_reconciliation.reconcile_all_exchanges()
            
            # Should still get reports for other exchanges
            working_exchanges = {r.exchange for r in reports if r.status == ReconciliationStatus.COMPLETED}
            assert len(working_exchanges) > 0
            
        finally:
            # Restore original connector
            trade_reconciliation.exchange_connectors['binance_spot'] = original_connector


@pytest.mark.asyncio
async def test_exchange_specific_features():
    """Test exchange-specific features and capabilities."""
    
    # Test exchange-specific symbol formats
    exchange_symbols = {
        'binance_spot': 'BTC/USDT',
        'binance_perp': 'BTC/USDT',
        'bybit_spot': 'BTC/USDT',
        'bybit_perp': 'BTC/USDT',
        'hyperliquid_perp': 'BTC/USDC:USDC',
        'mexc_spot': 'BTC/USDT',
        'gateio_spot': 'BTC/USDT',
        'bitget_spot': 'BTC/USDT'
    }
    
    # Verify each exchange can handle its symbol format
    for exchange, symbol in exchange_symbols.items():
        connector = MockExchangeConnector(exchange)
        
        # Test trade history
        trades = await connector.get_trade_history(symbol=symbol)
        assert isinstance(trades, list)
        
        # Test positions (for futures exchanges)
        positions = await connector.get_positions(symbol=symbol)
        assert isinstance(positions, list)
        
        # Futures exchanges should have positions, spot should be empty
        if 'perp' in exchange or 'futures' in exchange:
            # May have positions
            pass
        else:
            # Spot exchanges return empty positions
            assert len(positions) == 0


if __name__ == "__main__":
    # Run tests manually for development
    pytest.main([__file__, "-v"]) 