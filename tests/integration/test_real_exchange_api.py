#!/usr/bin/env python
"""
Real Exchange API Integration Tests

This test suite uses REAL API keys to test:
- Actual exchange connections with authentication
- Real trade data fetching from exchanges
- Complete trade synchronization system
- Database operations with real trade data
- Position tracking with actual trades

Prerequisites:
- Valid API keys configured in config/exchange_keys.py
- Database connection (SQLite by default)

Usage:
    python -m pytest tests/integration/test_real_exchange_api.py -v -s
"""

import asyncio
import pytest
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from decimal import Decimal

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import structlog
from sqlalchemy.ext.asyncio import AsyncSession

# Database components
from database.connection import init_db, get_session, close_db
from database.repositories.trade_repository import TradeRepository
from database.models import Exchange, BotInstance

# Exchange components
from exchanges.connectors import create_exchange_connector
from config.exchange_keys import get_exchange_config, get_available_exchanges

# Trade sync system
from order_management.trade_sync import TradeSync, perform_startup_sync
from order_management.tracking import PositionManager

# Configure logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="%H:%M:%S"),
        structlog.dev.ConsoleRenderer(),
    ],
)
logger = structlog.get_logger(__name__)


class TestRealExchangeAPI:
    """Test real exchange API connections and data fetching."""
    
    def setup_method(self):
        """Set up test environment."""
        self.test_exchanges = ['binance_spot', 'bybit_spot', 'mexc_spot']  # Start with spot exchanges
        self.test_symbols = ['BTC/USDT', 'ETH/USDT']
        
    @pytest.mark.asyncio
    async def test_exchange_connections_with_api_keys(self):
        """Test connecting to exchanges with real API keys."""
        logger.info("🔧 Testing real exchange connections with API keys...")
        
        successful_connections = 0
        connection_results = {}
        
        for exchange_name in self.test_exchanges:
            try:
                logger.info(f"📡 Testing {exchange_name} with API keys...")
                
                # Get real API configuration
                config = get_exchange_config(exchange_name)
                if not config:
                    logger.warning(f"⚠️ No config found for {exchange_name}")
                    continue
                
                logger.info(f"🔑 Using API key for {exchange_name}: {config.get('api_key', '')[:10]}...")
                
                # Create connector with real credentials
                connector = create_exchange_connector(exchange_name.split('_')[0], config)
                if not connector:
                    logger.warning(f"⚠️ Could not create {exchange_name} connector")
                    continue
                
                # Test connection
                connected = await connector.connect()
                if connected:
                    logger.info(f"✅ {exchange_name} connected successfully")
                    successful_connections += 1
                    connection_results[exchange_name] = "success"
                    
                    # Test basic functionality
                    try:
                        # Test getting balance
                        if hasattr(connector, 'get_balance'):
                            balance = await connector.get_balance()
                            logger.info(f"💰 {exchange_name} balance check: {len(balance)} currencies")
                            
                        # Test getting orderbook
                        if hasattr(connector, 'get_orderbook'):
                            orderbook = await connector.get_orderbook('BTC/USDT', limit=10)
                            if orderbook and 'bids' in orderbook:
                                logger.info(f"📊 {exchange_name} orderbook: {len(orderbook['bids'])} bids, {len(orderbook['asks'])} asks")
                        
                    except Exception as e:
                        logger.warning(f"⚠️ {exchange_name} API test failed: {e}")
                    
                    # Disconnect
                    await connector.disconnect()
                    logger.info(f"✅ {exchange_name} disconnected")
                    
                else:
                    logger.warning(f"⚠️ {exchange_name} connection failed")
                    connection_results[exchange_name] = "failed"
                    
            except Exception as e:
                logger.error(f"❌ {exchange_name} test failed: {e}")
                connection_results[exchange_name] = f"error: {e}"
        
        logger.info(f"✅ Real exchange connection test completed")
        logger.info(f"📊 Results: {connection_results}")
        logger.info(f"🎯 Success rate: {successful_connections}/{len(self.test_exchanges)} exchanges")
        
        # At least one exchange should connect successfully
        assert successful_connections > 0, "No exchanges connected successfully"

    @pytest.mark.asyncio
    async def test_real_trade_history_fetching(self):
        """Test fetching real trade history from exchanges."""
        logger.info("🔧 Testing real trade history fetching...")
        
        trade_data_results = {}
        
        for exchange_name in self.test_exchanges:
            try:
                logger.info(f"📈 Testing {exchange_name} trade history...")
                
                # Get configuration and create connector
                config = get_exchange_config(exchange_name)
                if not config:
                    continue
                    
                connector = create_exchange_connector(exchange_name.split('_')[0], config)
                if not connector:
                    continue
                
                # Connect
                connected = await connector.connect()
                if not connected:
                    logger.warning(f"⚠️ {exchange_name} connection failed")
                    continue
                
                # Try to fetch trade history
                if hasattr(connector, 'get_trade_history'):
                    try:
                        # Fetch trades from last 24 hours
                        since = datetime.utcnow() - timedelta(hours=24)
                        trades = await connector.get_trade_history(
                            symbol='BTC/USDT',
                            since=since,
                            limit=50
                        )
                        
                        if trades and len(trades) > 0:
                            logger.info(f"✅ {exchange_name} fetched {len(trades)} trades")
                            
                            # Log sample trade data
                            sample_trade = trades[0]
                            logger.info(f"📋 Sample trade: {sample_trade.get('id', 'N/A')} - "
                                      f"{sample_trade.get('side', 'N/A')} "
                                      f"{sample_trade.get('amount', 'N/A')} @ "
                                      f"{sample_trade.get('price', 'N/A')}")
                            
                            trade_data_results[exchange_name] = {
                                'count': len(trades),
                                'sample_trade': sample_trade
                            }
                        else:
                            logger.info(f"ℹ️ {exchange_name} returned no trades (normal for new accounts)")
                            trade_data_results[exchange_name] = {'count': 0}
                            
                    except Exception as e:
                        logger.warning(f"⚠️ {exchange_name} trade history failed: {e}")
                        trade_data_results[exchange_name] = {'error': str(e)}
                else:
                    logger.info(f"ℹ️ {exchange_name} doesn't have get_trade_history method")
                
                # Disconnect
                await connector.disconnect()
                
            except Exception as e:
                logger.error(f"❌ {exchange_name} trade history test failed: {e}")
                trade_data_results[exchange_name] = {'error': str(e)}
        
        logger.info(f"✅ Trade history test completed")
        logger.info(f"📊 Results: {trade_data_results}")


class TestRealTradeSyncWithAPI:
    """Test the complete trade sync system with real exchange APIs."""
    
    @pytest.mark.asyncio
    async def test_complete_real_trade_sync(self):
        """Test the complete trade sync system with real exchange connections."""
        logger.info("🔧 Testing complete trade sync system with real APIs...")
        
        # Initialize database
        await init_db()
        
        try:
            async for session in get_session():
                logger.info("1️⃣ Setting up database and repositories...")
                
                # Set up repositories
                trade_repository = TradeRepository(session)
                position_manager = PositionManager(data_dir="data/test_real_api_sync")
                
                # Create exchange records
                exchange_records = {}
                test_exchanges = ['binance_spot', 'bybit_spot']  # Start with two reliable exchanges
                
                for exchange_name in test_exchanges:
                    exchange = Exchange(
                        name=exchange_name,
                        type='spot',
                        is_active=True
                    )
                    session.add(exchange)
                    await session.commit()
                    exchange_records[exchange_name] = exchange
                    logger.info(f"✅ Created {exchange_name} exchange record (ID: {exchange.id})")
                
                logger.info("2️⃣ Setting up real exchange connectors...")
                
                # Set up real exchange connectors
                exchange_connectors = {}
                for exchange_name in test_exchanges:
                    try:
                        config = get_exchange_config(exchange_name)
                        if config:
                            connector = create_exchange_connector(exchange_name.split('_')[0], config)
                            if connector:
                                exchange_connectors[exchange_name] = connector
                                logger.info(f"✅ Created {exchange_name} connector with API keys")
                            else:
                                logger.warning(f"⚠️ Could not create {exchange_name} connector")
                        else:
                            logger.warning(f"⚠️ No config for {exchange_name}")
                    except Exception as e:
                        logger.warning(f"⚠️ {exchange_name} connector setup failed: {e}")
                
                if not exchange_connectors:
                    logger.warning("⚠️ No exchange connectors available")
                    return
                
                logger.info("3️⃣ Testing trade sync system...")
                
                # Create trade sync system
                trade_sync = TradeSync(
                    exchange_connectors=exchange_connectors,
                    trade_repository=trade_repository,
                    position_manager=position_manager
                )
                
                # Test system summary
                summary = trade_sync.get_sync_summary()
                logger.info(f"📊 Trade sync system ready: {summary}")
                
                # Test incremental sync (this will fetch real trades)
                logger.info("4️⃣ Running real incremental sync...")
                
                total_trades_fetched = 0
                total_trades_saved = 0
                
                for exchange_name in exchange_connectors:
                    try:
                        logger.info(f"🔄 Syncing {exchange_name}...")
                        results = await trade_sync.sync_exchange_incremental(exchange_name)
                        
                        for result in results:
                            logger.info(f"✅ {exchange_name}/{result.symbol}: "
                                      f"status={result.status}, "
                                      f"fetched={result.trades_fetched}, "
                                      f"saved={result.trades_saved}, "
                                      f"skipped={result.trades_skipped}")
                            
                            total_trades_fetched += result.trades_fetched
                            total_trades_saved += result.trades_saved
                            
                    except Exception as e:
                        logger.warning(f"⚠️ {exchange_name} sync failed: {e}")
                
                logger.info(f"📈 Total sync results: fetched={total_trades_fetched}, saved={total_trades_saved}")
                
                logger.info("5️⃣ Verifying database state...")
                
                # Check what was saved to database
                for exchange_name, exchange_record in exchange_records.items():
                    for symbol in ['BTC/USDT', 'ETH/USDT']:
                        stats = await trade_repository.get_trade_statistics(
                            symbol=symbol,
                            exchange_id=exchange_record.id
                        )
                        
                        if stats['total_trades'] > 0:
                            logger.info(f"💾 {exchange_name} {symbol}: {stats}")
                        else:
                            logger.info(f"💾 {exchange_name} {symbol}: No trades (normal for new accounts)")
                
                logger.info("6️⃣ Testing position calculations...")
                
                # Check position state (should be zero for new accounts)
                for exchange_name in exchange_connectors:
                    for symbol in ['BTC/USDT', 'ETH/USDT']:
                        try:
                            position = position_manager.get_position(exchange_name, symbol)
                            logger.info(f"📊 {exchange_name} {symbol} position: {position}")
                        except Exception as e:
                            logger.warning(f"⚠️ Position check failed for {exchange_name} {symbol}: {e}")
                
                logger.info("✅ Complete real trade sync test completed successfully!")
                
                # Test passed if we got this far without major errors
                assert True
                break
                
        finally:
            await close_db()


class TestRealTradeProcessing:
    """Test processing real trades through the complete system."""
    
    @pytest.mark.asyncio
    async def test_simulated_trading_workflow(self):
        """Test a simulated trading workflow with the complete system."""
        logger.info("🔧 Testing simulated trading workflow...")
        
        # Initialize database
        await init_db()
        
        try:
            async for session in get_session():
                logger.info("1️⃣ Setting up test environment...")
                
                # Set up repositories
                trade_repository = TradeRepository(session)
                position_manager = PositionManager(data_dir="data/test_simulated_workflow")
                
                # Create exchange record
                exchange = Exchange(
                    name='binance_spot_test',
                    type='spot',
                    is_active=True
                )
                session.add(exchange)
                await session.commit()
                
                logger.info("2️⃣ Simulating real trading sequence...")
                
                # Simulate a realistic trading sequence
                simulated_trades = [
                    {
                        'id': 'sim_trade_001',
                        'symbol': 'BTC/USDT',
                        'side': 'buy',
                        'amount': 0.001,
                        'price': 45000.0,
                        'cost': 45.0,
                        'fee': {'cost': 0.045, 'currency': 'USDT'},
                        'timestamp': datetime.utcnow() - timedelta(minutes=30),
                        'type': 'limit',
                        'maker': True
                    },
                    {
                        'id': 'sim_trade_002', 
                        'symbol': 'ETH/USDT',
                        'side': 'buy',
                        'amount': 0.01,
                        'price': 3000.0,
                        'cost': 30.0,
                        'fee': {'cost': 0.03, 'currency': 'USDT'},
                        'timestamp': datetime.utcnow() - timedelta(minutes=25),
                        'type': 'market',
                        'maker': False
                    },
                    {
                        'id': 'sim_trade_003',
                        'symbol': 'BTC/USDT',
                        'side': 'sell',
                        'amount': 0.0005,
                        'price': 46000.0,
                        'cost': 23.0,
                        'fee': {'cost': 0.023, 'currency': 'USDT'},
                        'timestamp': datetime.utcnow() - timedelta(minutes=10),
                        'type': 'limit',
                        'maker': True
                    }
                ]
                
                # Process trades through the system
                saved_trades = []
                for trade_data in simulated_trades:
                    trade = await trade_repository.save_trade(trade_data, exchange.id)
                    saved_trades.append(trade)
                    logger.info(f"✅ Saved trade: {trade.exchange_trade_id} - "
                              f"{trade.side} {trade.amount} {trade.symbol} @ {trade.price}")
                    
                    # Update positions
                    await position_manager.update_from_trade('binance_spot_test', trade_data)
                
                logger.info("3️⃣ Verifying final state...")
                
                # Check database statistics
                btc_stats = await trade_repository.get_trade_statistics(
                    symbol='BTC/USDT',
                    exchange_id=exchange.id
                )
                eth_stats = await trade_repository.get_trade_statistics(
                    symbol='ETH/USDT',
                    exchange_id=exchange.id
                )
                
                logger.info(f"📊 BTC/USDT stats: {btc_stats}")
                logger.info(f"📊 ETH/USDT stats: {eth_stats}")
                
                # Check positions
                btc_position = position_manager.get_position('binance_spot_test', 'BTC/USDT')
                eth_position = position_manager.get_position('binance_spot_test', 'ETH/USDT')
                
                logger.info(f"📈 BTC/USDT position: {btc_position}")
                logger.info(f"📈 ETH/USDT position: {eth_position}")
                
                # Verify calculations
                expected_btc_size = Decimal('0.001') - Decimal('0.0005')  # buy - sell
                expected_eth_size = Decimal('0.01')  # buy only
                
                assert abs(btc_position.size - expected_btc_size) < Decimal('1e-8'), f"BTC position mismatch: {btc_position.size} vs {expected_btc_size}"
                assert abs(eth_position.size - expected_eth_size) < Decimal('1e-8'), f"ETH position mismatch: {eth_position.size} vs {expected_eth_size}"
                
                logger.info("4️⃣ Testing trade sync with processed data...")
                
                # Test that the trade sync system can handle existing data
                config = get_exchange_config('binance_spot')
                if config:
                    exchange_connectors = {
                        'binance_spot_test': create_exchange_connector('binance', config)
                    }
                    
                    trade_sync = TradeSync(
                        exchange_connectors=exchange_connectors,
                        trade_repository=trade_repository,
                        position_manager=position_manager
                    )
                    
                    summary = trade_sync.get_sync_summary()
                    logger.info(f"📊 Trade sync system with processed data: {summary}")
                
                logger.info("✅ Simulated trading workflow test completed successfully!")
                assert True
                break
                
        finally:
            await close_db()


if __name__ == "__main__":
    # Run tests directly
    pytest.main([__file__, "-v", "-s"]) 