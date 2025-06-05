#!/usr/bin/env python
"""
Demo script for the Trade Sync System

This demonstrates the simple trade synchronization system that:
- Fetches trades from exchanges since last sync time
- Fills in any missing trades automatically
- Performs historical backfill on startup
- Runs continuous sync in background

Usage:
    python scripts/demo_trade_sync.py [--startup-days DAYS] [--sync-interval MINUTES]
"""

import asyncio
import argparse
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import structlog
from order_management.trade_sync import (
    TradeSync,
    perform_startup_sync,
    start_periodic_sync
)
from order_management.tracking import PositionManager

# Configure logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="%H:%M:%S"),
        structlog.dev.ConsoleRenderer(),
    ],
)
logger = structlog.get_logger(__name__)


class TradeSyncDemo:
    """Demo of the simple trade sync system."""
    
    def __init__(self):
        self.logger = logger.bind(component="Demo")
        
    async def setup_demo_environment(self):
        """Set up demo environment with mock components."""
        self.logger.info("🔧 Setting up demo environment...")
        
        # Import mock classes for demo
        from tests.integration.test_reconciliation_and_validation import (
            MockTradeRepository,
            MockExchangeConnector
        )
        
        # Initialize mock components
        self.trade_repository = MockTradeRepository()
        self.position_manager = PositionManager(data_dir="data/demo_sync")
        
        # Mock exchange connectors
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
        
        self.logger.info("✅ Demo environment ready")
        
    async def demo_startup_sync(self, days_back: int = 30):
        """Demonstrate startup sync (historical backfill)."""
        self.logger.info(f"🚀 DEMO: Startup Sync ({days_back} days back)")
        
        # This is what you'd call when your trading system starts up
        results = await perform_startup_sync(
            exchange_connectors=self.exchange_connectors,
            trade_repository=self.trade_repository,
            position_manager=self.position_manager,
            days_back=days_back
        )
        
        # Display results
        total_saved = 0
        for exchange, sync_results in results.items():
            exchange_saved = sum(r.trades_saved for r in sync_results)
            total_saved += exchange_saved
            
            if exchange_saved > 0:
                self.logger.info(f"  {exchange}: {exchange_saved} trades saved")
            else:
                self.logger.info(f"  {exchange}: No new trades")
        
        self.logger.info(f"🎉 Startup sync completed - {total_saved} total trades saved")
        return results
        
    async def demo_incremental_sync(self):
        """Demonstrate incremental sync (only new trades)."""
        self.logger.info("⏰ DEMO: Incremental Sync")
        
        # Create sync system
        trade_sync = TradeSync(
            exchange_connectors=self.exchange_connectors,
            trade_repository=self.trade_repository,
            position_manager=self.position_manager
        )
        
        # Run incremental sync across all exchanges
        results = await trade_sync.sync_all_exchanges_incremental()
        
        # Display results
        total_saved = 0
        for exchange, sync_results in results.items():
            exchange_saved = sum(r.trades_saved for r in sync_results)
            total_saved += exchange_saved
            
            if exchange_saved > 0:
                self.logger.info(f"  {exchange}: {exchange_saved} new trades")
        
        if total_saved == 0:
            self.logger.info("  No new trades found (already up to date)")
        else:
            self.logger.info(f"🔄 Incremental sync saved {total_saved} new trades")
        
        return results
        
    async def demo_single_exchange_sync(self):
        """Demonstrate syncing a single exchange."""
        self.logger.info("🎯 DEMO: Single Exchange Sync")
        
        trade_sync = TradeSync(
            exchange_connectors=self.exchange_connectors,
            trade_repository=self.trade_repository,
            position_manager=self.position_manager
        )
        
        # Sync just Binance spot
        results = await trade_sync.sync_exchange_incremental('binance_spot')
        
        for result in results:
            self.logger.info(f"  {result.symbol}: "
                           f"fetched={result.trades_fetched}, "
                           f"saved={result.trades_saved}, "
                           f"skipped={result.trades_skipped}")
        
        return results
        
    async def demo_continuous_sync(self, interval_minutes: int = 1):
        """Demonstrate continuous sync (runs in background)."""
        self.logger.info(f"🔄 DEMO: Continuous Sync (every {interval_minutes} minutes)")
        
        # This would normally run as a background task
        trade_sync = TradeSync(
            exchange_connectors=self.exchange_connectors,
            trade_repository=self.trade_repository,
            position_manager=self.position_manager
        )
        
        # Set short interval for demo
        trade_sync.config['sync_interval_minutes'] = interval_minutes
        
        self.logger.info("Starting continuous sync (will run 3 cycles for demo)...")
        
        # Run a few cycles for demo
        for cycle in range(3):
            self.logger.info(f"🔄 Sync cycle {cycle + 1}")
            
            results = await trade_sync.sync_all_exchanges_incremental()
            
            total_saved = sum(
                sum(r.trades_saved for r in exchange_results)
                for exchange_results in results.values()
            )
            
            if total_saved > 0:
                self.logger.info(f"  Cycle {cycle + 1}: {total_saved} trades saved")
            else:
                self.logger.info(f"  Cycle {cycle + 1}: No new trades")
            
            # Wait for next cycle (short delay for demo)
            if cycle < 2:  # Don't wait after last cycle
                await asyncio.sleep(5)  # 5 seconds for demo instead of minutes
        
        self.logger.info("🏁 Continuous sync demo completed")
        
    async def demo_sync_status_tracking(self):
        """Demonstrate sync status and monitoring."""
        self.logger.info("📊 DEMO: Sync Status Tracking")
        
        trade_sync = TradeSync(
            exchange_connectors=self.exchange_connectors,
            trade_repository=self.trade_repository,
            position_manager=self.position_manager
        )
        
        # Run sync to generate some results
        await trade_sync.sync_exchange_incremental('binance_spot')
        
        # Get system summary
        summary = trade_sync.get_sync_summary()
        self.logger.info("📋 System Summary:")
        self.logger.info(f"  Active syncs: {summary['active_syncs']}")
        self.logger.info(f"  Total syncs: {summary['total_syncs']}")
        self.logger.info(f"  Supported exchanges: {len(summary['supported_exchanges'])}")
        
        # Get recent results
        recent_results = await trade_sync.get_recent_sync_results(hours=1)
        self.logger.info(f"📈 Recent activity: {len(recent_results)} sync operations")
        
        for result in recent_results[-3:]:  # Show last 3
            self.logger.info(f"  {result.exchange}/{result.symbol}: "
                           f"{result.status} - {result.trades_saved} saved")
    
    async def run_full_demo(self, startup_days: int = 7, sync_interval: int = 1):
        """Run complete demo of trade sync system."""
        self.logger.info("🚀 Trade Sync System Demo")
        self.logger.info("=" * 50)
        
        await self.setup_demo_environment()
        
        try:
            # 1. Startup sync (historical backfill)
            await self.demo_startup_sync(startup_days)
            print()
            
            # 2. Incremental sync
            await self.demo_incremental_sync()
            print()
            
            # 3. Single exchange sync
            await self.demo_single_exchange_sync()
            print()
            
            # 4. Status tracking
            await self.demo_sync_status_tracking()
            print()
            
            # 5. Continuous sync (brief demo)
            await self.demo_continuous_sync(sync_interval)
            
            self.logger.info("=" * 50)
            self.logger.info("🎉 Trade Sync Demo Completed Successfully!")
            
        except Exception as e:
            self.logger.error(f"Demo failed: {e}")
            raise


def show_usage_examples():
    """Show usage examples for production."""
    print("""
📖 USAGE EXAMPLES FOR PRODUCTION:

1. STARTUP SYNC (run when your trading system starts):
   
   from order_management.trade_sync import perform_startup_sync
   
   # Backfill last 30 days of trades
   results = await perform_startup_sync(
       exchange_connectors=your_exchange_connectors,
       trade_repository=your_trade_repository,
       position_manager=your_position_manager,
       days_back=30
   )

2. CONTINUOUS SYNC (run as background task):
   
   from order_management.trade_sync import start_periodic_sync
   
   # Sync every 5 minutes continuously
   asyncio.create_task(start_periodic_sync(
       exchange_connectors=your_exchange_connectors,
       trade_repository=your_trade_repository,
       position_manager=your_position_manager,
       interval_minutes=5
   ))

3. MANUAL SYNC (run when needed):
   
   from order_management.trade_sync import TradeSync
   
   trade_sync = TradeSync(exchange_connectors, trade_repository, position_manager)
   
   # Sync single exchange
   results = await trade_sync.sync_exchange_incremental('binance_spot')
   
   # Sync all exchanges
   results = await trade_sync.sync_all_exchanges_incremental()

THAT'S IT! Simple, focused, and efficient. 🎯
""")


async def main():
    """Main demo function."""
    parser = argparse.ArgumentParser(description='Trade Sync System Demo')
    parser.add_argument('--startup-days', type=int, default=7,
                       help='Days to look back for startup sync (default: 7)')
    parser.add_argument('--sync-interval', type=int, default=1,
                       help='Minutes between sync cycles for demo (default: 1)')
    parser.add_argument('--show-examples', action='store_true',
                       help='Show usage examples instead of running demo')
    
    args = parser.parse_args()
    
    if args.show_examples:
        show_usage_examples()
        return
    
    demo = TradeSyncDemo()
    await demo.run_full_demo(args.startup_days, args.sync_interval)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Demo interrupted by user")
    except Exception as e:
        print(f"❌ Demo failed: {e}")
        import traceback
        traceback.print_exc() 