#!/usr/bin/env python
"""
Example: Trading Bot Startup with Trade Sync

This example shows how to integrate the simple trade sync system 
into your trading bot startup process.

It demonstrates:
1. Startup sync (historical backfill) when the bot starts
2. Continuous sync running as a background task
3. Proper error handling and logging

Usage:
    python examples/startup_with_trade_sync.py
"""

import asyncio
import logging
import signal
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import structlog
from order_management.trade_sync import (
    perform_startup_sync, 
    start_periodic_sync
)
from order_management.tracking import PositionManager
from database.repositories.trade_repository import TradeRepository
from database.repositories.position_repository import PositionRepository


# Configure logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
        structlog.processors.JSONRenderer() if "--json" in sys.argv else structlog.dev.ConsoleRenderer(),
    ],
)
logger = structlog.get_logger(__name__)


class TradingBotWithSync:
    """
    Example trading bot that integrates trade synchronization.
    
    This shows the minimal integration needed to keep your trades
    synchronized with exchanges.
    """
    
    def __init__(self):
        self.logger = logger.bind(component="TradingBot")
        self.shutdown_event = asyncio.Event()
        self.background_tasks = []
        
        # Initialize your components
        self.position_manager = None
        self.trade_repository = None
        self.position_repository = None
        self.exchange_connectors = {}
        
    async def initialize(self):
        """Initialize trading bot components."""
        self.logger.info("Initializing trading bot...")
        
        # Initialize position manager
        self.position_manager = PositionManager(data_dir="data/positions")
        
        # Initialize repositories (use your actual database implementations)
        # For demo, we'll use mock ones
        from tests.integration.test_reconciliation_and_validation import (
            MockTradeRepository,
            MockPositionRepository,
            MockExchangeConnector
        )
        
        self.trade_repository = MockTradeRepository()
        self.position_repository = MockPositionRepository()
        
        # Initialize exchange connectors (use your actual exchange configs)
        self.exchange_connectors = {
            'binance_spot': MockExchangeConnector('binance_spot'),
            'binance_perp': MockExchangeConnector('binance_perp'),
            'bybit_spot': MockExchangeConnector('bybit_spot'),
            'bybit_perp': MockExchangeConnector('bybit_perp'),
            'hyperliquid_perp': MockExchangeConnector('hyperliquid_perp'),
            'mexc_spot': MockExchangeConnector('mexc_spot'),
            'gateio_spot': MockExchangeConnector('gateio_spot'),
            'bitget_spot': MockExchangeConnector('bitget_spot')
        }
        
        self.logger.info("✅ Trading bot initialized")
        
    async def startup_trade_sync(self, days_back: int = 30):
        """
        Perform startup trade synchronization.
        
        This should be called when your bot starts to ensure all recent
        trades are captured in your database.
        """
        self.logger.info(f"🔄 Starting trade synchronization (last {days_back} days)")
        
        try:
            # Perform startup sync across all exchanges
            results = await perform_startup_sync(
                exchange_connectors=self.exchange_connectors,
                trade_repository=self.trade_repository,
                position_manager=self.position_manager,
                days_back=days_back
            )
            
            # Log results
            total_saved = 0
            for exchange, sync_results in results.items():
                exchange_saved = sum(r.trades_saved for r in sync_results)
                total_saved += exchange_saved
                
                if exchange_saved > 0:
                    self.logger.info(f"Synced {exchange}: {exchange_saved} trades")
            
            if total_saved > 0:
                self.logger.info(f"✅ Startup sync completed - {total_saved} trades synchronized")
            else:
                self.logger.info("✅ Startup sync completed - all trades already up to date")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Startup sync failed: {e}")
            return False
    
    async def start_continuous_sync(self, interval_minutes: int = 5):
        """
        Start continuous trade synchronization as a background task.
        
        This will run continuously to keep trades synchronized.
        """
        self.logger.info(f"🔄 Starting continuous sync (every {interval_minutes} minutes)")
        
        # Start continuous sync as background task
        sync_task = asyncio.create_task(
            start_periodic_sync(
                exchange_connectors=self.exchange_connectors,
                trade_repository=self.trade_repository,
                position_manager=self.position_manager,
                interval_minutes=interval_minutes
            )
        )
        
        self.background_tasks.append(sync_task)
        self.logger.info("✅ Continuous sync started")
        
        return sync_task
    
    async def start_trading_strategies(self):
        """Start your actual trading strategies."""
        self.logger.info("🚀 Starting trading strategies...")
        
        # This is where you'd start your actual trading logic
        # For demo, we'll just simulate some activity
        
        while not self.shutdown_event.is_set():
            # Simulate trading activity
            self.logger.info("💹 Trading strategies running...")
            
            # Your trading logic would go here:
            # - Monitor markets
            # - Execute trades
            # - Manage positions
            # - etc.
            
            # Wait before next iteration
            try:
                await asyncio.wait_for(self.shutdown_event.wait(), timeout=30)
            except asyncio.TimeoutError:
                continue  # Continue trading loop
            else:
                break  # Shutdown event was set
        
        self.logger.info("🛑 Trading strategies stopped")
    
    async def start(self):
        """Start the complete trading bot."""
        self.logger.info("🚀 Starting trading bot with trade synchronization")
        
        try:
            # 1. Initialize components
            await self.initialize()
            
            # 2. Perform startup trade sync (critical for accurate positions)
            sync_success = await self.startup_trade_sync(days_back=30)
            if not sync_success:
                self.logger.error("❌ Startup sync failed - cannot start trading safely")
                return False
            
            # 3. Start continuous sync (background task)
            await self.start_continuous_sync(interval_minutes=5)
            
            # 4. Start trading strategies (your main bot logic)
            trading_task = asyncio.create_task(self.start_trading_strategies())
            self.background_tasks.append(trading_task)
            
            self.logger.info("✅ Trading bot started successfully")
            
            # Wait for shutdown signal
            await self.shutdown_event.wait()
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to start trading bot: {e}")
            return False
        
        finally:
            await self.shutdown()
    
    async def shutdown(self):
        """Gracefully shutdown the trading bot."""
        self.logger.info("🛑 Shutting down trading bot...")
        
        # Set shutdown event
        self.shutdown_event.set()
        
        # Cancel all background tasks
        for task in self.background_tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        
        # Close exchange connections
        for connector in self.exchange_connectors.values():
            try:
                if hasattr(connector, 'disconnect'):
                    await connector.disconnect()
            except Exception as e:
                self.logger.warning(f"Error disconnecting exchange: {e}")
        
        self.logger.info("✅ Trading bot shutdown complete")
    
    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""
        def signal_handler(signum, frame):
            self.logger.info(f"Received signal {signum}, initiating shutdown...")
            asyncio.create_task(self.shutdown())
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)


async def main():
    """Main function - entry point for your trading bot."""
    bot = TradingBotWithSync()
    
    # Setup signal handlers for graceful shutdown
    bot.setup_signal_handlers()
    
    try:
        # Start the bot
        success = await bot.start()
        
        if success:
            logger.info("🎉 Trading bot completed successfully")
        else:
            logger.error("❌ Trading bot failed to start")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("⚠️ Received keyboard interrupt")
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        sys.exit(1)
    finally:
        # Ensure clean shutdown
        await bot.shutdown()


if __name__ == "__main__":
    # Run the trading bot
    asyncio.run(main()) 