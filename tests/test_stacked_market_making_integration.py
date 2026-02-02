"""
Integration tests for the Stacked Market Making Strategy.

Tests the complete integration of all components:
- Inventory management system
- Volume coefficient engine  
- Enhanced aggregated orderbook manager
- Multi-reference pricing engine
- Dual line strategy logic
"""

import asyncio
import unittest
from unittest.mock import MagicMock, patch
import sys
from pathlib import Path
from decimal import Decimal
from datetime import datetime, timezone

# Add trading_bot_system to path
sys.path.append(str(Path(__file__).parent.parent))

from bot_manager.strategies.stacked_market_making import StackedMarketMakingStrategy
from bot_manager.strategies.inventory_manager import InventoryManager, InventoryConfig, InventoryPriceMethod
from market_data_collection.moving_averages import MovingAverageCalculator, MovingAverageConfig
from market_data_collection.exchange_coefficients import SimpleExchangeCoefficientCalculator
from market_data.enhanced_aggregated_orderbook_manager import EnhancedAggregatedOrderbookManager
from config.stacked_market_making_config import ConfigTemplates, validate_config


class TestStackedMarketMakingIntegration(unittest.TestCase):
    """Integration tests for stacked market making strategy."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.test_config = ConfigTemplates.conservative_bera_usdt()
        self.strategy = None
    
    def tearDown(self):
        """Clean up after tests."""
        if self.strategy:
            # Stop strategy if running
            try:
                asyncio.run(self.strategy.stop())
            except:
                pass
    
    def test_config_validation(self):
        """Test configuration validation."""
        # Test valid config
        errors = validate_config(self.test_config)
        self.assertEqual([], errors, "Valid config should have no validation errors")
        
        # Test invalid config - missing required field
        invalid_config = dict(self.test_config)
        del invalid_config['base_coin']
        errors = validate_config(invalid_config)
        self.assertTrue(len(errors) > 0, "Invalid config should have validation errors")
    
    def test_inventory_manager_initialization(self):
        """Test inventory manager initialization and basic operations."""
        # Create inventory config
        inventory_config = InventoryConfig(
            target_inventory=Decimal('1000'),
            max_inventory_deviation=Decimal('500'),
            inventory_price_method=InventoryPriceMethod.ACCOUNTING
        )
        
        # Initialize inventory manager
        inventory_manager = InventoryManager(inventory_config)
        
        # Test initial state
        state = inventory_manager.get_inventory_state()
        self.assertEqual(state.target_inventory, Decimal('1000'))
        self.assertEqual(state.current_inventory, Decimal('0'))
        self.assertEqual(state.inventory_coefficient, Decimal('0'))  # At target initially
        
        # Test position update
        inventory_manager.update_position(
            exchange='binance',
            side='buy',
            amount=Decimal('100'),
            price=Decimal('2.0')
        )
        
        updated_state = inventory_manager.get_inventory_state()
        self.assertEqual(updated_state.current_inventory, Decimal('100'))
        self.assertEqual(updated_state.excess_inventory, Decimal('-900'))  # 100 - 1000 = -900
        # Coefficient = -900 / 1000 = -0.9 (but clamped to [-1, 1])
        self.assertEqual(updated_state.inventory_coefficient, Decimal('-0.9'))
    
    async def test_existing_coefficient_system_integration(self):
        """Test integration with existing proven coefficient system."""
        # Test MA calculator creation
        ma_configs = [
            MovingAverageConfig(period=30, ma_type='sma', volume_type='quote'),
            MovingAverageConfig(period=90, ma_type='sma', volume_type='base'),
            MovingAverageConfig(period=30, ma_type='ewma', volume_type='quote')
        ]
        
        ma_calculator = MovingAverageCalculator(ma_configs)
        
        # Test coefficient calculator creation
        coeff_calculator = SimpleExchangeCoefficientCalculator(
            ma_calculator=ma_calculator,
            calculation_method='min',
            min_coefficient=0.2,
            max_coefficient=3.0
        )
        
        self.assertIsNotNone(ma_calculator, "MA calculator should be created")
        self.assertIsNotNone(coeff_calculator, "Coefficient calculator should be created")
        
        # Test status methods
        ma_status = ma_calculator.get_status()
        coeff_status = coeff_calculator.get_status()
        
        self.assertIn('total_ma_configs', ma_status)
        self.assertIn('calculation_method', coeff_status)
    
    @patch('bot_manager.strategies.base_strategy.BaseStrategy._initialize_exchange_connections')
    @patch('bot_manager.strategies.base_strategy.BaseStrategy.ensure_aggregator_running') 
    async def test_strategy_initialization(self, mock_aggregator, mock_connections):
        """Test strategy initialization without real exchange connections."""
        # Mock the external dependencies
        mock_aggregator.return_value = None
        mock_connections.return_value = None
        
        # Create strategy with test config
        self.strategy = StackedMarketMakingStrategy(
            instance_id="test_stacked_001",
            symbol="BERA/USDT",
            exchanges=["binance", "bybit"], 
            config=self.test_config
        )
        
        # Mock exchange connectors
        self.strategy.exchange_connectors = {
            'binance': MagicMock(),
            'bybit': MagicMock()
        }
        
        # Test initialization
        await self.strategy.initialize()
        
        # Check that components are initialized
        self.assertIsNotNone(self.strategy.strategy_config, "Strategy config should be parsed")
        self.assertIsNotNone(self.strategy.inventory_manager, "Inventory manager should be initialized")
        
        # Check configuration parsing
        config = self.strategy.strategy_config
        self.assertEqual(config.base_coin, 'BERA')
        self.assertEqual(config.quote_currencies, ['USDT'])
        self.assertTrue(len(config.tob_lines) > 0 or len(config.passive_lines) > 0)
    
    def test_all_config_templates(self):
        """Test that all configuration templates are valid."""
        templates = {
            'conservative': ConfigTemplates.conservative_bera_usdt(),
            'aggressive': ConfigTemplates.aggressive_multi_currency(),
            'high_frequency': ConfigTemplates.high_frequency_single_venue()
        }
        
        for name, config in templates.items():
            with self.subTest(template=name):
                errors = validate_config(config)
                self.assertEqual([], errors, f"{name} template should be valid: {errors}")
                
                # Test key requirements
                self.assertIn('base_coin', config)
                self.assertIn('inventory', config)
                self.assertTrue(
                    config.get('tob_lines') or config.get('passive_lines'),
                    f"{name} template should have at least one line type"
                )
    
    async def test_coefficient_calculations(self):
        """Test coefficient calculations integration."""
        # Create inventory manager with test config
        inventory_config = InventoryConfig(
            target_inventory=Decimal('1000'),
            max_inventory_deviation=Decimal('500')
        )
        inventory_manager = InventoryManager(inventory_config)
        
        # Test coefficient at different inventory levels
        test_cases = [
            # (current_position, expected_coefficient_range)
            (Decimal('1000'), (Decimal('-0.1'), Decimal('0.1'))),    # At target
            (Decimal('1500'), (Decimal('0.4'), Decimal('0.6'))),     # Long position  
            (Decimal('500'), (Decimal('-0.6'), Decimal('-0.4'))),    # Short position
            (Decimal('1600'), (Decimal('0.9'), Decimal('1.0'))),     # Near max long
        ]
        
        for position, (min_coeff, max_coeff) in test_cases:
            with self.subTest(position=position):
                # Simulate position
                inventory_manager.positions['test_exchange'] = MagicMock()
                inventory_manager.positions['test_exchange'].base_amount = position
                inventory_manager._calculate_inventory_state()
                
                coefficient = inventory_manager.get_inventory_coefficient()
                self.assertGreaterEqual(coefficient, min_coeff)
                self.assertLessEqual(coefficient, max_coeff)
    
    async def test_pricing_integration(self):
        """Test pricing integration with market data services.""" 
        # Mock market data service
        mock_market_data = MagicMock()
        mock_market_data.orderbooks = {
            ('binance', 'BERA/USDT'): {
                'bids': [[2.0, 100], [1.99, 200]],
                'asks': [[2.01, 150], [2.02, 250]]
            },
            ('bybit', 'BERA/USDT'): {
                'bids': [[1.999, 120], [1.989, 180]],
                'asks': [[2.011, 160], [2.021, 220]]
            }
        }
        
        # Test enhanced pricing engine
        from trading_bot_system.market_data.multi_reference_pricing_engine import MultiReferencePricingEngine
        
        pricing_engine = MultiReferencePricingEngine(mock_market_data)
        await pricing_engine.start()
        
        try:
            # Test multi-exchange WAPQ
            wapq_price = await pricing_engine._get_multi_exchange_wapq(
                'BERA/USDT', 'mid', Decimal('100')
            )
            
            self.assertIsNotNone(wapq_price, "WAPQ price should be calculated")
            self.assertGreater(wapq_price, Decimal('1.5'))
            self.assertLess(wapq_price, Decimal('3.0'))
            
        finally:
            await pricing_engine.stop()


class TestMarketDataCompatibility(unittest.TestCase):
    """Test compatibility with existing market data services."""
    
    def test_multi_symbol_service_compatibility(self):
        """Test that enhanced service works with existing multi-symbol service."""
        from trading_bot_system.market_data.multi_symbol_market_data_service import (
            MultiSymbolMarketDataService, SymbolConfig
        )
        
        # Create symbol configs for the strategy requirements
        symbol_configs = [
            SymbolConfig(
                base_symbol='BERA/USDT',
                exchanges=[
                    {'name': 'binance', 'type': 'spot'},
                    {'name': 'bybit', 'type': 'spot'},
                    {'name': 'gateio', 'type': 'spot'}
                ]
            ),
            SymbolConfig(
                base_symbol='BTC/USDT',
                exchanges=[
                    {'name': 'binance', 'type': 'spot'},
                    {'name': 'bybit', 'type': 'spot'}
                ]
            )
        ]
        
        # Initialize service
        service = MultiSymbolMarketDataService(
            symbol_configs=symbol_configs
        )
        
        # Check that service initializes with required symbols
        self.assertEqual(len(service.symbol_configs), 2)
        self.assertIn('BERA/USDT', service.symbol_configs)
        self.assertIn('BTC/USDT', service.symbol_configs)
    
    def test_pricing_engine_compatibility(self):
        """Test that enhanced pricing engine maintains compatibility."""
        from trading_bot_system.market_data.multi_reference_pricing_engine import (
            MultiReferencePricingEngine, PricingContext, PricingMethod
        )
        
        # Mock market data service
        mock_service = MagicMock()
        
        # Initialize pricing engine
        pricing_engine = MultiReferencePricingEngine(mock_service)
        
        # Test pricing context creation
        context = PricingContext(
            symbol='BERA/USDT',
            side='mid',
            quantity=Decimal('100'),
            pricing_method=PricingMethod.MULTI_REFERENCE_WEIGHTED
        )
        
        self.assertEqual(context.symbol, 'BERA/USDT')
        self.assertEqual(context.pricing_method, PricingMethod.MULTI_REFERENCE_WEIGHTED)


async def run_integration_tests():
    """Run all integration tests."""
    print("🧪 Running Stacked Market Making Integration Tests")
    
    # Create test suite
    suite = unittest.TestSuite()
    
    # Add test classes
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestStackedMarketMakingIntegration))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestMarketDataCompatibility))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    if result.wasSuccessful():
        print("\n✅ All integration tests passed!")
    else:
        print(f"\n❌ {len(result.failures)} test failures, {len(result.errors)} test errors")
        
        for test, traceback in result.failures:
            print(f"FAILURE: {test}")
            print(traceback)
        
        for test, traceback in result.errors:
            print(f"ERROR: {test}")
            print(traceback)
    
    return result.wasSuccessful()


if __name__ == "__main__":
    success = asyncio.run(run_integration_tests())
    sys.exit(0 if success else 1)
