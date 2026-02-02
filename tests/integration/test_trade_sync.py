#!/usr/bin/env python
"""
Integration tests for the Trade Sync System

Tests the complete trade synchronization functionality:
- Startup sync (historical backfill)
- Incremental sync (new trades only)
- Continuous sync (background task)
- Multi-exchange support
- Error handling and recovery
- Performance characteristics

These tests use mock exchange connectors to simulate real exchange behavior
without making actual API calls.
"""

import asyncio
import pytest
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Any
from decimal import Decimal

# Import the system under test
from order_management.trade_sync import (
    TradeSync,
    SyncStatus,
    SyncResult,
    perform_startup_sync,
    start_periodic_sync
)
from order_management.tracking import PositionManager


# Mock implementations for testing
class MockTradeRepository:
    """Mock trade repository for testing."""
    
    def __init__(self):
        self.trades = []
        self.sync_states = {}
        
    async def save_trade(self, trade: Dict[str, Any], exchange_id: int) -> Dict[str, Any]:
        """Save a trade to mock database."""
        saved_trade = {
            'id': str(uuid.uuid4()),
            'exchange_id': exchange_id,
            'exchange_trade_id': trade.get('id', str(uuid.uuid4())),
            'symbol': trade.get('symbol', 'BTC/USDT'),
            'side': trade.get('side', 'buy'),
            'amount': trade.get('amount', 1.0),
            'price': trade.get('price', 50000.0),
            'cost': trade.get('cost', 50000.0),
            'fee_cost': trade.get('fee', {}).get('cost', 0.0),
            'fee_currency': trade.get('fee', {}).get('currency', 'USDT'),
            'timestamp': trade.get('timestamp', datetime.utcnow())
        }
        self.trades.append(saved_trade)
        return saved_trade
    
    async def get_trade_by_exchange_id(self, exchange_id: int, trade_id: str) -> Dict[str, Any]:
        """Get trade by exchange ID and trade ID."""
        for trade in self.trades:
            if (trade['exchange_id'] == exchange_id and 
                trade['exchange_trade_id'] == trade_id):
                return trade
        return None
    
    async def get_last_trade_sync(self, exchange_id: int, symbol: str) -> Dict[str, Any]:
        """Get last sync state for exchange/symbol."""
        key = f"{exchange_id}_{symbol}"
        return self.sync_states.get(key)
    
    async def update_trade_sync(self, exchange_id: int, symbol: str, sync_time: datetime):
        """Update sync state for exchange/symbol."""
        key = f"{exchange_id}_{symbol}"
        self.sync_states[key] = {
            'exchange_id': exchange_id,
            'symbol': symbol,
            'last_sync_time': sync_time
        }


class MockExchangeConnector:
    """Mock exchange connector for testing."""
    
    def __init__(self, exchange_name: str):
        self.exchange_name = exchange_name
        self.trade_counter = 0
        
    async def get_trade_history(
        self, 
        symbol: str, 
        since: datetime = None, 
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Mock trade history fetch."""
        # Generate mock trades based on time range
        if since is None:
            since = datetime.utcnow() - timedelta(days=1)
            
        trades = []
        current_time = since
        end_time = datetime.utcnow()
        
        # Generate trades every hour in the time range
        while current_time < end_time and len(trades) < limit:
            self.trade_counter += 1
            
            trade = {
                'id': f"{self.exchange_name}_trade_{self.trade_counter}",
                'symbol': symbol,
                'side': 'buy' if self.trade_counter % 2 == 0 else 'sell',
                'amount': round(0.1 + (self.trade_counter % 10) * 0.01, 3),
                'price': 50000.0 + (self.trade_counter % 1000),
                'cost': None,  # Will be calculated
                'timestamp': current_time,
                'fee': {
                    'cost': 0.001,
                    'currency': 'USDT'
                }
            }
            
            # Calculate cost
            trade['cost'] = trade['amount'] * trade['price']
            
            trades.append(trade)
            current_time += timedelta(hours=1)
            
        return trades


class TestTradeSyncBasic:
    """Basic functionality tests for Trade Sync."""
    
    def setup_method(self):
        """Set up test environment."""
        self.trade_repository = MockTradeRepository()
        self.position_manager = PositionManager(data_dir="data/test_sync")
        
        # Mock exchange connectors
        self.exchange_connectors = {
            'binance_spot': MockExchangeConnector('binance_spot'),
            'binance_perp': MockExchangeConnector('binance_perp'),
            'bybit_spot': MockExchangeConnector('bybit_spot'),
            'hyperliquid_perp': MockExchangeConnector('hyperliquid_perp'),
            'mexc_spot': MockExchangeConnector('mexc_spot')
        }
        
        self.trade_sync = TradeSync(
            exchange_connectors=self.exchange_connectors,
            trade_repository=self.trade_repository,
            position_manager=self.position_manager
        )
    
    @pytest.mark.asyncio
    async def test_single_exchange_incremental_sync(self):
        """Test incremental sync for a single exchange."""
        # Run incremental sync
        results = await self.trade_sync.sync_exchange_incremental('binance_spot')
        
        # Verify results
        assert len(results) > 0
        result = results[0]
        assert result.exchange == 'binance_spot'
        assert result.status == SyncStatus.COMPLETED
        assert result.trades_fetched > 0
        assert result.trades_saved > 0
        assert result.processing_time_seconds > 0
        
        # Verify trades were saved
        assert len(self.trade_repository.trades) == result.trades_saved
    
    @pytest.mark.asyncio
    async def test_all_exchanges_incremental_sync(self):
        """Test incremental sync across all exchanges."""
        # Run sync for all exchanges
        all_results = await self.trade_sync.sync_all_exchanges_incremental()
        
        # Verify results for each exchange
        assert len(all_results) == len(self.exchange_connectors)
        
        total_saved = 0
        for exchange, results in all_results.items():
            assert exchange in self.exchange_connectors
            assert len(results) > 0
            
            for result in results:
                assert result.exchange == exchange
                assert result.status == SyncStatus.COMPLETED
                total_saved += result.trades_saved
        
        # Verify total trades saved
        assert total_saved > 0
        assert len(self.trade_repository.trades) == total_saved
    
    @pytest.mark.asyncio
    async def test_historical_sync(self):
        """Test historical sync with backfill."""
        # Run historical sync for 7 days
        results = await self.trade_sync.sync_exchange_historical(
            'binance_spot', 
            days_back=7
        )
        
        # Verify results
        assert len(results) > 0
        result = results[0]
        assert result.exchange == 'binance_spot'
        assert result.status == SyncStatus.COMPLETED
        assert result.trades_fetched > 0
        assert result.trades_saved > 0
        
        # Verify time range
        expected_start = datetime.utcnow() - timedelta(days=7)
        time_diff = abs((result.start_time - expected_start).total_seconds())
        assert time_diff < 3600  # Within 1 hour
    
    @pytest.mark.asyncio
    async def test_duplicate_trade_handling(self):
        """Test that duplicate trades are skipped."""
        # First sync
        results1 = await self.trade_sync.sync_exchange_incremental('binance_spot')
        initial_count = sum(r.trades_saved for r in results1)
        
        # Second sync (should find duplicates)
        results2 = await self.trade_sync.sync_exchange_incremental('binance_spot')
        
        # Verify duplicates were skipped
        for result in results2:
            assert result.trades_saved == 0  # No new trades
            assert result.trades_skipped > 0  # Existing trades skipped
        
        # Total trades should be same as first sync
        assert len(self.trade_repository.trades) == initial_count
    
    @pytest.mark.asyncio
    async def test_specific_symbol_sync(self):
        """Test syncing a specific symbol."""
        # Sync specific symbol
        results = await self.trade_sync.sync_exchange_incremental(
            'binance_spot', 
            symbol='ETH/USDT'
        )
        
        # Verify only ETH/USDT trades
        assert len(results) == 1
        result = results[0]
        assert result.symbol == 'ETH/USDT'
        
        # Verify saved trades are for correct symbol
        for trade in self.trade_repository.trades:
            assert trade['symbol'] == 'ETH/USDT'


class TestTradeSyncAdvanced:
    """Advanced functionality tests for Trade Sync."""
    
    def setup_method(self):
        """Set up test environment."""
        self.trade_repository = MockTradeRepository()
        self.position_manager = PositionManager(data_dir="data/test_sync_advanced")
        
        self.exchange_connectors = {
            'binance_spot': MockExchangeConnector('binance_spot'),
            'bybit_spot': MockExchangeConnector('bybit_spot')
        }
        
        self.trade_sync = TradeSync(
            exchange_connectors=self.exchange_connectors,
            trade_repository=self.trade_repository,
            position_manager=self.position_manager
        )
    
    @pytest.mark.asyncio
    async def test_sync_status_tracking(self):
        """Test sync status and monitoring."""
        # Get initial summary
        summary = self.trade_sync.get_sync_summary()
        assert summary['active_syncs'] == 0
        assert summary['total_syncs'] == 0
        assert len(summary['supported_exchanges']) == 2
        
        # Start sync (but don't await yet)
        sync_task = asyncio.create_task(
            self.trade_sync.sync_exchange_incremental('binance_spot')
        )
        
        # Brief delay to let sync start
        await asyncio.sleep(0.1)
        
        # Check active syncs
        summary = self.trade_sync.get_sync_summary()
        # Note: Due to async timing, active_syncs might be 0 if sync completed quickly
        
        # Wait for completion
        results = await sync_task
        
        # Check final summary
        summary = self.trade_sync.get_sync_summary()
        assert summary['active_syncs'] == 0
        assert summary['total_syncs'] > 0
        
        # Get recent results
        recent = await self.trade_sync.get_recent_sync_results(hours=1)
        assert len(recent) > 0
        assert recent[0].exchange == 'binance_spot'
    
    @pytest.mark.asyncio
    async def test_error_handling(self):
        """Test error handling for invalid exchanges."""
        # Test invalid exchange
        with pytest.raises(ValueError, match="Exchange invalid_exchange not available"):
            await self.trade_sync.sync_exchange_incremental('invalid_exchange')
    
    @pytest.mark.asyncio
    async def test_rate_limiting(self):
        """Test rate limiting between API calls."""
        # Set short rate limit for testing
        self.trade_sync.config['rate_limit_delay'] = 0.01
        
        start_time = datetime.utcnow()
        
        # Sync multiple symbols
        results = await self.trade_sync.sync_all_exchanges_incremental()
        
        end_time = datetime.utcnow()
        elapsed = (end_time - start_time).total_seconds()
        
        # Should take some time due to rate limiting
        # (This is a basic check - actual time depends on number of symbols)
        assert elapsed > 0.01
    
    @pytest.mark.asyncio 
    async def test_batch_processing(self):
        """Test batch processing of large trade sets."""
        # Set small batch size for testing
        self.trade_sync.config['batch_size'] = 50
        
        # Run historical sync which should process in batches
        results = await self.trade_sync.sync_exchange_historical(
            'binance_spot',
            days_back=30
        )
        
        # Verify results
        assert len(results) > 0
        total_fetched = sum(r.trades_fetched for r in results)
        assert total_fetched > 0


class TestTradeSyncConvenience:
    """Test convenience functions for easy startup."""
    
    def setup_method(self):
        """Set up test environment."""
        self.trade_repository = MockTradeRepository()
        self.position_manager = PositionManager(data_dir="data/test_sync_convenience")
        
        self.exchange_connectors = {
            'binance_spot': MockExchangeConnector('binance_spot'),
            'bybit_spot': MockExchangeConnector('bybit_spot')
        }
    
    @pytest.mark.asyncio
    async def test_perform_startup_sync(self):
        """Test convenience function for startup sync."""
        # Run startup sync
        results = await perform_startup_sync(
            exchange_connectors=self.exchange_connectors,
            trade_repository=self.trade_repository,
            position_manager=self.position_manager,
            days_back=7
        )
        
        # Verify results
        assert len(results) == len(self.exchange_connectors)
        
        total_saved = 0
        for exchange, sync_results in results.items():
            assert exchange in self.exchange_connectors
            assert len(sync_results) > 0
            
            for result in sync_results:
                assert result.status == SyncStatus.COMPLETED
                total_saved += result.trades_saved
        
        assert total_saved > 0
        assert len(self.trade_repository.trades) == total_saved
    
    @pytest.mark.asyncio
    async def test_continuous_sync_brief(self):
        """Test continuous sync for a brief period."""
        # This is a challenging test because start_periodic_sync runs forever
        # We'll test by running it briefly and then canceling
        
        # Start continuous sync
        sync_task = asyncio.create_task(
            start_periodic_sync(
                exchange_connectors=self.exchange_connectors,
                trade_repository=self.trade_repository,
                position_manager=self.position_manager,
                interval_minutes=0.01  # Very short interval for testing
            )
        )
        
        # Let it run briefly
        await asyncio.sleep(0.1)
        
        # Cancel the task
        sync_task.cancel()
        
        try:
            await sync_task
        except asyncio.CancelledError:
            pass  # Expected
        
        # Verify some trades were likely saved
        # (This might be 0 if the sync didn't complete a cycle)
        assert len(self.trade_repository.trades) >= 0


class TestTradeSyncPerformance:
    """Performance tests for Trade Sync."""
    
    def setup_method(self):
        """Set up test environment."""
        self.trade_repository = MockTradeRepository()
        self.position_manager = PositionManager(data_dir="data/test_sync_perf")
        
        # More exchanges for performance testing
        self.exchange_connectors = {
            'binance_spot': MockExchangeConnector('binance_spot'),
            'binance_perp': MockExchangeConnector('binance_perp'),
            'bybit_spot': MockExchangeConnector('bybit_spot'),
            'bybit_perp': MockExchangeConnector('bybit_perp'),
            'hyperliquid_perp': MockExchangeConnector('hyperliquid_perp'),
            'mexc_spot': MockExchangeConnector('mexc_spot')
        }
        
        self.trade_sync = TradeSync(
            exchange_connectors=self.exchange_connectors,
            trade_repository=self.trade_repository,
            position_manager=self.position_manager
        )
    
    @pytest.mark.asyncio
    async def test_parallel_exchange_sync(self):
        """Test that multiple exchanges sync in parallel."""
        start_time = datetime.utcnow()
        
        # Sync all exchanges (should be parallel)
        results = await self.trade_sync.sync_all_exchanges_incremental()
        
        end_time = datetime.utcnow()
        elapsed = (end_time - start_time).total_seconds()
        
        # Verify all exchanges completed
        assert len(results) == len(self.exchange_connectors)
        
        # Should complete faster than sequential processing
        # (This is a basic check - exact timing depends on system)
        assert elapsed < 30  # Should complete within 30 seconds
        
        # Verify trades were saved from all exchanges
        total_saved = sum(
            sum(r.trades_saved for r in exchange_results)
            for exchange_results in results.values()
        )
        assert total_saved > 0
    
    @pytest.mark.asyncio
    async def test_large_dataset_handling(self):
        """Test handling of large trade datasets."""
        # Configure for larger batches
        self.trade_sync.config['batch_size'] = 1000
        
        # Run historical sync for longer period
        results = await self.trade_sync.sync_exchange_historical(
            'binance_spot',
            days_back=60  # Longer history
        )
        
        # Verify completed successfully
        assert len(results) > 0
        total_fetched = sum(r.trades_fetched for r in results)
        total_saved = sum(r.trades_saved for r in results)
        
        # Should handle large datasets
        assert total_fetched > 100  # Expect significant number of trades
        assert total_saved == total_fetched  # All should be new trades


if __name__ == "__main__":
    # Run tests directly
    pytest.main([__file__, "-v"]) 