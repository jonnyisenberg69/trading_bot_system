"""
Configuration templates and validation for Stacked Market Making Strategy.

Provides:
- Configuration templates for easy setup
- Validation utilities
- Example configurations
- Configuration builders for different scenarios
"""

from dataclasses import dataclass, asdict
from decimal import Decimal
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
import json

from bot_manager.strategies.inventory_manager import InventoryConfig, InventoryPriceMethod
from bot_manager.strategies.stacked_market_making import (
    StackedMarketMakingConfig, TOBLineConfig, PassiveLineConfig, PricingSourceType
)
# Using existing proven coefficient system


class StackedMarketMakingConfigBuilder:
    """Builder for creating Stacked Market Making configurations."""
    
    def __init__(self):
        self.reset()
    
    def reset(self):
        """Reset builder to default state."""
        self._config = {
            'base_coin': 'BERA',
            'quote_currencies': ['USDT'],
            'exchanges': ['binance', 'bybit', 'gateio'],
            'inventory': {},
            'tob_lines': [],
            'passive_lines': [],
            'moving_average_periods': [5, 15, 60],
            'leverage': '1.0',
            'taker_check': True,
            'smart_pricing_source': 'aggregated',
            'hedging_enabled': True,
            # Using existing proven coefficient system
        }
        return self
    
    def set_basic_info(
        self, 
        base_coin: str, 
        quote_currencies: List[str], 
        exchanges: List[str]
    ):
        """Set basic strategy information."""
        self._config['base_coin'] = base_coin
        self._config['quote_currencies'] = quote_currencies
        self._config['exchanges'] = exchanges
        return self
    
    def set_inventory_config(
        self,
        target_inventory: Decimal,
        max_deviation: Decimal,
        price_method: InventoryPriceMethod = InventoryPriceMethod.ACCOUNTING,
        manual_price: Optional[Decimal] = None,
        start_time: Optional[datetime] = None
    ):
        """Set inventory management configuration."""
        self._config['inventory'] = {
            'target_inventory': str(target_inventory),
            'max_inventory_deviation': str(max_deviation), 
            'inventory_price_method': price_method.value,
            'manual_inventory_price': str(manual_price) if manual_price else None,
            'start_time': start_time.isoformat() if start_time else None
        }
        return self
    
    def add_tob_line(
        self,
        hourly_quantity: Decimal,
        spread_bps: Decimal = Decimal('50'),
        sides: str = "both",
        timeout_seconds: int = 30,
        drift_bps: Decimal = Decimal('30'),
        min_price: Optional[Decimal] = None,
        max_price: Optional[Decimal] = None,
        coefficient_method: str = "inventory"
    ):
        """Add a Top of Book line configuration."""
        tob_line = {
            'hourly_quantity': str(hourly_quantity),
            'quantity_currency': 'base',
            'sides': sides,
            'spread_from_inventory': True,
            'spread_bps': str(spread_bps),
            'drift_bps': str(drift_bps),
            'timeout_seconds': timeout_seconds,
            'coefficient_method': coefficient_method,
            'max_coefficient': '2.0',
            'min_coefficient': '0.1'
        }
        
        if min_price is not None:
            tob_line['min_price'] = str(min_price)
        if max_price is not None:
            tob_line['max_price'] = str(max_price)
        
        self._config['tob_lines'].append(tob_line)
        return self
    
    def add_passive_line(
        self,
        mid_spread_bps: Decimal,
        quantity: Decimal,
        sides: str = "both",
        timeout_seconds: int = 60,
        drift_bps: Decimal = Decimal('20'),
        min_spread_bps: Decimal = Decimal('10'),
        max_spread_bps: Decimal = Decimal('200'),
        randomization_factor: Decimal = Decimal('0.05')
    ):
        """Add a Passive line configuration."""
        passive_line = {
            'mid_spread_bps': str(mid_spread_bps),
            'quantity': str(quantity),
            'sides': sides,
            'spread_coefficient_method': 'inventory',
            'quantity_coefficient_method': 'volume',
            'min_spread_bps': str(min_spread_bps),
            'max_spread_bps': str(max_spread_bps),
            'drift_bps': str(drift_bps),
            'timeout_seconds': timeout_seconds,
            'randomization_factor': str(randomization_factor)
        }
        
        self._config['passive_lines'].append(passive_line)
        return self
    
    def set_coefficient_config(
        self,
        time_periods: List[str] = None,
        coefficient_method: str = 'min',
        min_coefficient: float = 0.2,
        max_coefficient: float = 3.0
    ):
        """Set volume coefficient configuration using existing proven system."""
        if time_periods is None:
            time_periods = ['5min', '15min', '30min']
            
        self._config['time_periods'] = time_periods
        self._config['coefficient_method'] = coefficient_method
        self._config['min_coefficient'] = str(min_coefficient)
        self._config['max_coefficient'] = str(max_coefficient)
        return self
    
    def set_advanced_options(
        self,
        leverage: Decimal = Decimal('1.0'),
        taker_check: bool = True,
        smart_pricing_source: PricingSourceType = PricingSourceType.AGGREGATED,
        hedging_enabled: bool = True
    ):
        """Set advanced strategy options."""
        self._config['leverage'] = str(leverage)
        self._config['taker_check'] = taker_check
        self._config['smart_pricing_source'] = smart_pricing_source.value
        self._config['hedging_enabled'] = hedging_enabled
        return self
    
    def build(self) -> Dict[str, Any]:
        """Build final configuration dictionary."""
        return dict(self._config)
    
    def build_json(self, indent: int = 2) -> str:
        """Build configuration as JSON string."""
        return json.dumps(self._config, indent=indent)


class ConfigTemplates:
    """Pre-defined configuration templates for common scenarios."""
    
    @staticmethod
    def conservative_bera_usdt() -> Dict[str, Any]:
        """Conservative BERA/USDT configuration with moderate inventory targets."""
        builder = StackedMarketMakingConfigBuilder()
        
        return (builder
            .set_basic_info('BERA', ['USDT'], ['binance', 'bybit', 'gateio'])
            .set_inventory_config(
                target_inventory=Decimal('1000'),  # 1000 BERA target
                max_deviation=Decimal('500'),       # ±500 BERA max deviation
                price_method=InventoryPriceMethod.ACCOUNTING
            )
            .add_tob_line(
                hourly_quantity=Decimal('50'),      # 50 BERA/hour
                spread_bps=Decimal('30'),           # 30 bps spread
                timeout_seconds=30,
                coefficient_method='inventory'
            )
            .add_passive_line(
                mid_spread_bps=Decimal('15'),       # 15 bps base spread
                quantity=Decimal('25'),             # 25 BERA per order
                timeout_seconds=120,                # 2 min timeout
                randomization_factor=Decimal('0.03')  # 3% randomization
            )
            .set_coefficient_config(
                time_periods=['5min', '15min'],
                coefficient_method='min'
            )
            .build()
        )
    
    @staticmethod  
    def aggressive_multi_currency() -> Dict[str, Any]:
        """Aggressive multi-currency configuration with multiple quote currencies."""
        builder = StackedMarketMakingConfigBuilder()
        
        return (builder
            .set_basic_info('BERA', ['USDT', 'BTC', 'ETH'], ['binance', 'bybit', 'gateio', 'mexc'])
            .set_inventory_config(
                target_inventory=Decimal('2000'),   # 2000 BERA target
                max_deviation=Decimal('1000'),      # ±1000 BERA max deviation
                price_method=InventoryPriceMethod.MARK_TO_MARKET
            )
            .add_tob_line(
                hourly_quantity=Decimal('100'),     # 100 BERA/hour
                spread_bps=Decimal('20'),           # 20 bps spread
                timeout_seconds=20,                 # Faster timeout
                coefficient_method='both'           # Use both inventory and volume
            )
            .add_tob_line(
                hourly_quantity=Decimal('200'),     # Second TOB line
                spread_bps=Decimal('40'),           # Wider spread
                timeout_seconds=45
            )
            .add_passive_line(
                mid_spread_bps=Decimal('10'),       # Tight 10 bps spread
                quantity=Decimal('50'),
                timeout_seconds=90
            )
            .add_passive_line(
                mid_spread_bps=Decimal('25'),       # Second passive line
                quantity=Decimal('100'),
                timeout_seconds=180,
                max_spread_bps=Decimal('300')       # Allow wider spreads
            )
            .set_coefficient_config(
                time_periods=['5min', '10min', '15min'],  # More granular periods
                coefficient_method='mid',  # Use mid for aggressive strategy
                max_coefficient=4.0  # Higher max for aggressive
            )
            .set_advanced_options(
                leverage=Decimal('1.5'),            # Use leverage
                smart_pricing_source=PricingSourceType.LIQUIDITY_WEIGHTED,
                hedging_enabled=True
            )
            .build()
        )
    
    @staticmethod
    def high_frequency_single_venue() -> Dict[str, Any]:
        """High-frequency configuration focusing on single venue optimization.""" 
        builder = StackedMarketMakingConfigBuilder()
        
        return (builder
            .set_basic_info('BERA', ['USDT'], ['binance'])  # Single exchange focus
            .set_inventory_config(
                target_inventory=Decimal('500'),
                max_deviation=Decimal('250'),
                price_method=InventoryPriceMethod.ACCOUNTING
            )
            .add_tob_line(
                hourly_quantity=Decimal('200'),     # High frequency
                spread_bps=Decimal('15'),           # Tight spreads
                timeout_seconds=10,                 # Very fast timeout
                drift_bps=Decimal('15'),           # Tight drift tolerance
                coefficient_method='volume'
            )
            .add_passive_line(
                mid_spread_bps=Decimal('8'),        # Very tight spreads
                quantity=Decimal('75'),
                timeout_seconds=30,                 # Fast passive timeout  
                randomization_factor=Decimal('0.02')  # Low randomization
            )
            .set_coefficient_config(
                time_periods=['1min', '5min'],      # Very short periods for HF
                coefficient_method='max',            # Use max for high frequency
                min_coefficient=0.5,                # Higher min for stability
                max_coefficient=5.0                 # Very high max for HF
            )
            .set_advanced_options(
                smart_pricing_source=PricingSourceType.SINGLE_VENUE,
                hedging_enabled=False               # No hedging for single venue
            )
            .build()
        )


def validate_config(config: Dict[str, Any]) -> List[str]:
    """
    Validate stacked market making configuration.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        List of validation error messages (empty if valid)
    """
    errors = []
    
    # Required fields
    required_fields = ['base_coin', 'inventory']
    for field in required_fields:
        if field not in config:
            errors.append(f"Missing required field: {field}")
    
    # Validate inventory config
    if 'inventory' in config:
        inventory = config['inventory']
        if 'target_inventory' not in inventory:
            errors.append("inventory.target_inventory is required")
        if 'max_inventory_deviation' not in inventory:
            errors.append("inventory.max_inventory_deviation is required")
        
        # Validate inventory price method
        price_method = inventory.get('inventory_price_method', 'accounting')
        if price_method == 'manual' and not inventory.get('manual_inventory_price'):
            errors.append("manual_inventory_price required when using manual price method")
    
    # Validate that at least one line type is configured
    if not config.get('tob_lines') and not config.get('passive_lines'):
        errors.append("At least one TOB line or passive line must be configured")
    
    # Validate TOB lines
    for i, tob_line in enumerate(config.get('tob_lines', [])):
        required_tob_fields = ['hourly_quantity']
        for field in required_tob_fields:
            if field not in tob_line:
                errors.append(f"TOB line {i}: Missing required field '{field}'")
        
        # Validate sides
        if tob_line.get('sides', 'both') not in ['both', 'bid', 'offer']:
            errors.append(f"TOB line {i}: Invalid sides value")
    
    # Validate passive lines
    for i, passive_line in enumerate(config.get('passive_lines', [])):
        required_passive_fields = ['mid_spread_bps', 'quantity']
        for field in required_passive_fields:
            if field not in passive_line:
                errors.append(f"Passive line {i}: Missing required field '{field}'")
        
        # Validate sides
        if passive_line.get('sides', 'both') not in ['both', 'bid', 'offer']:
            errors.append(f"Passive line {i}: Invalid sides value")
        
        # Validate spread bounds
        min_spread = float(passive_line.get('min_spread_bps', 10))
        max_spread = float(passive_line.get('max_spread_bps', 200))
        mid_spread = float(passive_line.get('mid_spread_bps', 20))
        
        if min_spread >= max_spread:
            errors.append(f"Passive line {i}: min_spread_bps must be < max_spread_bps")
        if not (min_spread <= mid_spread <= max_spread):
            errors.append(f"Passive line {i}: mid_spread_bps must be between min and max")
    
    # Validate exchanges
    supported_exchanges = ['binance', 'bybit', 'hyperliquid', 'mexc', 'gateio', 'bitget', 'bitfinex']
    for exchange in config.get('exchanges', []):
        if exchange not in supported_exchanges:
            errors.append(f"Unsupported exchange: {exchange}")
    
    return errors


def create_example_configs() -> Dict[str, Dict[str, Any]]:
    """Create example configurations for different use cases."""
    
    examples = {
        'conservative_bera_usdt': ConfigTemplates.conservative_bera_usdt(),
        'aggressive_multi_currency': ConfigTemplates.aggressive_multi_currency(),
        'high_frequency_single_venue': ConfigTemplates.high_frequency_single_venue()
    }
    
    return examples


def save_config_template(config: Dict[str, Any], filename: str) -> None:
    """Save configuration template to file."""
    with open(filename, 'w') as f:
        json.dump(config, f, indent=2)


def load_config_template(filename: str) -> Dict[str, Any]:
    """Load configuration template from file."""
    with open(filename, 'r') as f:
        return json.load(f)


# Example usage and templates
if __name__ == "__main__":
    # Create example configurations
    examples = create_example_configs()
    
    print("Available configuration templates:")
    for name, config in examples.items():
        print(f"\n{name.upper()}:")
        print(f"  Base coin: {config['base_coin']}")
        print(f"  Quote currencies: {config['quote_currencies']}")
        print(f"  Exchanges: {config['exchanges']}")
        print(f"  TOB lines: {len(config['tob_lines'])}")
        print(f"  Passive lines: {len(config['passive_lines'])}")
        
        # Validate each example
        validation_errors = validate_config(config)
        if validation_errors:
            print(f"  ❌ VALIDATION ERRORS: {validation_errors}")
        else:
            print(f"  ✅ Configuration valid")
    
    # Save example configs
    for name, config in examples.items():
        save_config_template(config, f"{name}_template.json")
        print(f"\nSaved {name} template to {name}_template.json")
