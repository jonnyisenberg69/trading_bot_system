#!/usr/bin/env python
"""
Demonstration script for Trade Reconciliation and Position Validation systems.

This script demonstrates how to use both systems together for comprehensive
monitoring and validation across all supported exchanges.

Usage:
    python scripts/demo_reconciliation_validation.py

Features demonstrated:
- Trade reconciliation across all exchanges
- Position validation with discrepancy detection
- Continuous monitoring setup
- Error handling and recovery
- Reporting and alerting
"""

import asyncio
import json
import logging
from pathlib import Path
from datetime import datetime, timedelta
from decimal import Decimal
import sys
import os

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import structlog
from order_management.reconciliation import (
    TradeReconciliation,
    DiscrepancyType,
    ReconciliationStatus
)
from order_management.position_validation import (
    PositionValidation,
    PositionDiscrepancyType,
    ValidationStatus
)
from order_management.tracking import PositionManager
from database.repositories.trade_repository import TradeRepository
from database.repositories.position_repository import PositionRepository
from exchanges.connectors import (
    BinanceConnector,
    BybitConnector,
    HyperliquidConnector,
    MexcConnector,
    GateIOConnector,
    BitgetConnector
)


# Configure logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
        structlog.processors.format_exc_info,
        structlog.dev.ConsoleRenderer(),
    ],
)
logger = structlog.get_logger(__name__)


class ReconciliationValidationDemo:
    """
    Demonstration of trade reconciliation and position validation systems.
    
    Shows how to:
    - Set up both systems with real exchange connectors
    - Run comprehensive reconciliation across all exchanges
    - Validate positions and detect discrepancies
    - Handle continuous monitoring
    - Generate reports and alerts
    """
    
    def __init__(self):
        """Initialize demo system."""
        self.logger = logger.bind(component="Demo")
        
        # Initialize components (would use real databases in production)
        self.position_manager = PositionManager(data_dir="data/demo_positions")
        self.trade_repository = None  # Would be initialized with real database
        self.position_repository = None  # Would be initialized with real database
        
        # Exchange configurations (would load from secure config in production)
        self.exchange_configs = self._load_exchange_configs()
        
        # Initialize exchange connectors
        self.exchange_connectors = {}
        self._initialize_exchange_connectors()
        
        # Initialize reconciliation and validation systems
        self.trade_reconciliation = None
        self.position_validation = None
        self._initialize_systems()
        
    def _load_exchange_configs(self) -> dict:
        """Load exchange configurations (demo only - use secure storage in production)."""
        config_file = Path("tests/fixtures/exchange_credentials.json")
        
        if config_file.exists():
            with open(config_file, 'r') as f:
                return json.load(f)
        else:
            self.logger.warning("No exchange credentials found - using demo mode")
            return {}
    
    def _initialize_exchange_connectors(self):
        """Initialize exchange connectors for all supported exchanges."""
        connector_map = {
            'binance_spot': BinanceConnector,
            'binance_perp': BinanceConnector,
            'bybit_spot': BybitConnector,
            'bybit_perp': BybitConnector,
            'hyperliquid_perp': HyperliquidConnector,
            'mexc_spot': MexcConnector,
            'gateio_spot': GateIOConnector,
            'bitget_spot': BitgetConnector,
        }
        
        for exchange_name, connector_class in connector_map.items():
            if exchange_name in self.exchange_configs:
                try:
                    config = self.exchange_configs[exchange_name]
                    connector = connector_class(config)
                    self.exchange_connectors[exchange_name] = connector
                    self.logger.info(f"Initialized {exchange_name} connector")
                except Exception as e:
                    self.logger.error(f"Failed to initialize {exchange_name}: {e}")
            else:
                self.logger.warning(f"No config found for {exchange_name}")
    
    def _initialize_systems(self):
        """Initialize reconciliation and validation systems."""
        if not self.exchange_connectors:
            self.logger.error("No exchange connectors available")
            return
            
        # For demo purposes, we'll use mock repositories
        # In production, these would be real database repositories
        from tests.integration.test_reconciliation_and_validation import (
            MockTradeRepository, 
            MockPositionRepository
        )
        
        self.trade_repository = MockTradeRepository()
        self.position_repository = MockPositionRepository()
        
        # Initialize reconciliation system
        self.trade_reconciliation = TradeReconciliation(
            exchange_connectors=self.exchange_connectors,
            trade_repository=self.trade_repository,
            position_repository=self.position_repository,
            position_manager=self.position_manager
        )
        
        # Initialize validation system
        self.position_validation = PositionValidation(
            exchange_connectors=self.exchange_connectors,
            position_manager=self.position_manager,
            position_repository=self.position_repository,
            trade_reconciliation=self.trade_reconciliation
        )
        
        self.logger.info("Initialized reconciliation and validation systems")
    
    async def demo_basic_reconciliation(self):
        """Demonstrate basic trade reconciliation."""
        self.logger.info("🔍 DEMO: Basic Trade Reconciliation")
        
        if not self.trade_reconciliation:
            self.logger.error("Trade reconciliation system not available")
            return
        
        # Test reconciliation for a single exchange
        if 'binance_spot' in self.exchange_connectors:
            self.logger.info("Testing reconciliation for Binance Spot...")
            
            try:
                report = await self.trade_reconciliation.start_reconciliation(
                    exchange='binance_spot',
                    symbol='BTC/USDT',
                    start_time=datetime.utcnow() - timedelta(hours=24)
                )
                
                self._print_reconciliation_report(report)
                
            except Exception as e:
                self.logger.error(f"Reconciliation failed: {e}")
        
        self.logger.info("✅ Basic reconciliation demo completed\n")
    
    async def demo_multi_exchange_reconciliation(self):
        """Demonstrate reconciliation across multiple exchanges."""
        self.logger.info("🔍 DEMO: Multi-Exchange Reconciliation")
        
        if not self.trade_reconciliation:
            self.logger.error("Trade reconciliation system not available")
            return
        
        try:
            # Run reconciliation across all exchanges
            reports = await self.trade_reconciliation.reconcile_all_exchanges(
                symbol='BTC/USDT'
            )
            
            self.logger.info(f"📊 Reconciled {len(reports)} exchanges")
            
            # Analyze results
            total_discrepancies = 0
            critical_issues = 0
            
            for report in reports:
                total_discrepancies += len(report.discrepancies)
                critical_issues += len([
                    d for d in report.discrepancies 
                    if d.severity == 'critical'
                ])
                
                self.logger.info(f"Exchange {report.exchange}: "
                               f"{len(report.discrepancies)} discrepancies, "
                               f"status: {report.status}")
            
            self.logger.info(f"📈 Total discrepancies: {total_discrepancies}")
            self.logger.info(f"🚨 Critical issues: {critical_issues}")
            
        except Exception as e:
            self.logger.error(f"Multi-exchange reconciliation failed: {e}")
        
        self.logger.info("✅ Multi-exchange reconciliation demo completed\n")
    
    async def demo_position_validation(self):
        """Demonstrate position validation."""
        self.logger.info("📊 DEMO: Position Validation")
        
        if not self.position_validation:
            self.logger.error("Position validation system not available")
            return
        
        # Add some test positions to demonstrate validation
        await self._setup_test_positions()
        
        try:
            # Validate all exchanges
            reports = await self.position_validation.validate_all_exchanges()
            
            self.logger.info(f"🔍 Validated positions on {len(reports)} exchanges")
            
            # Analyze validation results
            total_position_discrepancies = 0
            critical_position_issues = 0
            
            for report in reports:
                total_position_discrepancies += len(report.discrepancies)
                critical_position_issues += len([
                    d for d in report.discrepancies 
                    if d.severity == 'critical'
                ])
                
                self._print_validation_report(report)
            
            self.logger.info(f"📈 Total position discrepancies: {total_position_discrepancies}")
            self.logger.info(f"🚨 Critical position issues: {critical_position_issues}")
            
        except Exception as e:
            self.logger.error(f"Position validation failed: {e}")
        
        self.logger.info("✅ Position validation demo completed\n")
    
    async def demo_continuous_monitoring(self):
        """Demonstrate continuous monitoring setup."""
        self.logger.info("⏰ DEMO: Continuous Monitoring")
        
        if not self.position_validation:
            self.logger.error("Position validation system not available")
            return
        
        try:
            # Start continuous monitoring
            await self.position_validation.start_continuous_monitoring()
            self.logger.info("🟢 Continuous monitoring started")
            
            # Let it run for a brief period
            self.logger.info("Running monitoring for 5 seconds...")
            await asyncio.sleep(5)
            
            # Stop monitoring
            await self.position_validation.stop_continuous_monitoring()
            self.logger.info("🔴 Continuous monitoring stopped")
            
        except Exception as e:
            self.logger.error(f"Continuous monitoring demo failed: {e}")
        
        self.logger.info("✅ Continuous monitoring demo completed\n")
    
    async def demo_error_handling(self):
        """Demonstrate error handling and recovery."""
        self.logger.info("⚠️ DEMO: Error Handling and Recovery")
        
        if not self.trade_reconciliation:
            self.logger.error("Trade reconciliation system not available")
            return
        
        # Simulate exchange failure
        original_connectors = self.exchange_connectors.copy()
        
        try:
            # Remove some connectors to simulate failures
            failed_exchanges = []
            for exchange in list(self.exchange_connectors.keys())[:2]:
                del self.exchange_connectors[exchange]
                failed_exchanges.append(exchange)
            
            self.logger.info(f"Simulating failures for: {failed_exchanges}")
            
            # Update reconciliation system
            self.trade_reconciliation.exchange_connectors = self.exchange_connectors
            
            # Run reconciliation with some exchanges "failed"
            reports = await self.trade_reconciliation.reconcile_all_exchanges()
            
            working_exchanges = [r.exchange for r in reports]
            self.logger.info(f"✅ Working exchanges: {working_exchanges}")
            self.logger.info(f"❌ Failed exchanges: {failed_exchanges}")
            
            # Restore original connectors
            self.exchange_connectors = original_connectors
            self.trade_reconciliation.exchange_connectors = self.exchange_connectors
            
        except Exception as e:
            self.logger.error(f"Error handling demo failed: {e}")
        
        self.logger.info("✅ Error handling demo completed\n")
    
    async def demo_reporting_and_alerts(self):
        """Demonstrate reporting and alerting capabilities."""
        self.logger.info("📋 DEMO: Reporting and Alerts")
        
        if not self.trade_reconciliation or not self.position_validation:
            self.logger.error("Systems not available")
            return
        
        try:
            # Get system summaries
            reconciliation_summary = self.trade_reconciliation.get_reconciliation_summary()
            validation_summary = self.position_validation.get_validation_summary()
            
            self.logger.info("📊 System Status Report:")
            self.logger.info(f"  Reconciliation: {reconciliation_summary['total_reconciliations']} total runs")
            self.logger.info(f"  Validation: {validation_summary['total_validations']} total runs")
            self.logger.info(f"  Monitoring: {'Active' if validation_summary['continuous_monitoring'] else 'Inactive'}")
            
            # Get recent discrepancies
            recent_trade_discrepancies = await self.trade_reconciliation.get_recent_discrepancies(hours=24)
            recent_position_discrepancies = await self.position_validation.get_recent_discrepancies(hours=24)
            
            self.logger.info(f"📈 Recent Activity (24h):")
            self.logger.info(f"  Trade discrepancies: {len(recent_trade_discrepancies)}")
            self.logger.info(f"  Position discrepancies: {len(recent_position_discrepancies)}")
            
        except Exception as e:
            self.logger.error(f"Reporting demo failed: {e}")
        
        self.logger.info("✅ Reporting demo completed\n")
    
    async def _setup_test_positions(self):
        """Set up test positions for validation demo."""
        test_positions = [
            ('binance_perp', 'BTC/USDT', 0.5, 50000.0),
            ('bybit_perp', 'ETH/USDT', 2.0, 3000.0),
            ('hyperliquid_perp', 'BTC/USDC:USDC', 0.3, 50000.0)
        ]
        
        for exchange, symbol, amount, price in test_positions:
            if exchange in self.exchange_connectors:
                await self.position_manager.update_from_trade(exchange, {
                    'symbol': symbol,
                    'id': f'demo_trade_{exchange}',
                    'side': 'buy',
                    'amount': amount,
                    'price': price,
                    'cost': amount * price,
                    'fee': {'cost': amount * price * 0.001, 'currency': 'USDT'}
                })
                
                self.logger.info(f"Added test position: {amount} {symbol} on {exchange}")
    
    def _print_reconciliation_report(self, report):
        """Print reconciliation report in a readable format."""
        self.logger.info(f"📋 Reconciliation Report for {report.exchange}")
        self.logger.info(f"  Symbol: {report.symbol}")
        self.logger.info(f"  Status: {report.status}")
        self.logger.info(f"  Internal trades: {report.internal_trades_count}")
        self.logger.info(f"  Exchange trades: {report.exchange_trades_count}")
        self.logger.info(f"  Matched trades: {report.matched_trades_count}")
        self.logger.info(f"  Discrepancies: {len(report.discrepancies)}")
        self.logger.info(f"  Auto-corrections: {report.auto_corrections_applied}")
        self.logger.info(f"  Processing time: {report.processing_time_seconds:.2f}s")
        
        if report.discrepancies:
            self.logger.info("  Discrepancy types:")
            for disc_type, count in report.discrepancies_by_type.items():
                self.logger.info(f"    {disc_type}: {count}")
    
    def _print_validation_report(self, report):
        """Print validation report in a readable format."""
        self.logger.info(f"📊 Validation Report for {report.exchange}")
        self.logger.info(f"  Symbol: {report.symbol}")
        self.logger.info(f"  Status: {report.status}")
        self.logger.info(f"  Internal positions: {report.internal_positions_count}")
        self.logger.info(f"  Exchange positions: {report.exchange_positions_count}")
        self.logger.info(f"  Matched positions: {report.matched_positions_count}")
        self.logger.info(f"  Discrepancies: {len(report.discrepancies)}")
        self.logger.info(f"  Auto-corrections: {report.auto_corrections_applied}")
        self.logger.info(f"  Processing time: {report.processing_time_seconds:.2f}s")
        
        if report.discrepancies:
            self.logger.info("  Position discrepancy types:")
            for disc_type, count in report.discrepancies_by_type.items():
                self.logger.info(f"    {disc_type}: {count}")
    
    async def run_full_demo(self):
        """Run the complete demonstration."""
        self.logger.info("🚀 Starting Trade Reconciliation & Position Validation Demo")
        self.logger.info("=" * 70)
        
        try:
            # Basic reconciliation
            await self.demo_basic_reconciliation()
            
            # Multi-exchange reconciliation
            await self.demo_multi_exchange_reconciliation()
            
            # Position validation
            await self.demo_position_validation()
            
            # Continuous monitoring
            await self.demo_continuous_monitoring()
            
            # Error handling
            await self.demo_error_handling()
            
            # Reporting and alerts
            await self.demo_reporting_and_alerts()
            
            self.logger.info("🎉 Demo completed successfully!")
            
        except Exception as e:
            self.logger.error(f"Demo failed: {e}")
            raise
        
        finally:
            # Cleanup
            if self.position_validation and self.position_validation.continuous_monitoring:
                await self.position_validation.stop_continuous_monitoring()
            
            # Close exchange connections
            for connector in self.exchange_connectors.values():
                try:
                    await connector.disconnect()
                except:
                    pass


async def main():
    """Main demo function."""
    demo = ReconciliationValidationDemo()
    await demo.run_full_demo()


if __name__ == "__main__":
    # Run the demo
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Demo interrupted by user")
    except Exception as e:
        print(f"❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("👋 Demo finished") 