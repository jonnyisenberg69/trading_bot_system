"""
Example script for running the Stacked Market Making Strategy.

This example demonstrates how to set up and run the new strategy with
the various configuration templates and monitoring capabilities.
"""

import asyncio
import logging
import sys
from pathlib import Path

# Add the trading_bot_system to Python path
sys.path.append(str(Path(__file__).parent.parent))

from bot_manager.strategies.stacked_market_making import StackedMarketMakingStrategy
from config.stacked_market_making_config import (
    ConfigTemplates, StackedMarketMakingConfigBuilder, validate_config
)
from bot_manager.strategies.inventory_manager import InventoryPriceMethod
from decimal import Decimal

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def run_conservative_example():
    """Run conservative BERA/USDT example configuration."""
    logger.info("🚀 Starting Conservative Stacked Market Making Example")
    
    # Use pre-built template
    config = ConfigTemplates.conservative_bera_usdt()
    
    # Validate configuration
    validation_errors = validate_config(config)
    if validation_errors:
        logger.error(f"❌ Configuration validation failed: {validation_errors}")
        return
    
    logger.info("✅ Configuration validation passed")
    
    # Create strategy instance
    strategy = StackedMarketMakingStrategy(
        instance_id="stacked_conservative_001",
        symbol="BERA/USDT",
        exchanges=["binance", "bybit", "gateio"], 
        config=config
    )
    
    try:
        # Initialize and start strategy
        await strategy.initialize()
        await strategy.start()
        
        logger.info("🟢 Strategy started successfully")
        
        # Monitor for 60 seconds
        for i in range(12):  # 12 * 5 = 60 seconds
            await asyncio.sleep(5)
            
            # Log strategy state
            state = strategy.get_strategy_state()
            inventory_state = state.get('inventory_state', {})
            
            logger.info(
                f"📊 Status Update {i+1}/12: "
                f"inventory_coeff={inventory_state.get('inventory_coefficient', 'N/A'):.3f}, "
                f"current_inventory={inventory_state.get('current_inventory', 'N/A')}, "
                f"active_orders={state.get('active_orders_count', 0)}"
            )
        
        logger.info("🔄 Example run completed, stopping strategy")
        
    except KeyboardInterrupt:
        logger.info("⏹️  Received interrupt signal")
    except Exception as e:
        logger.error(f"❌ Error running strategy: {e}")
    finally:
        # Clean shutdown
        await strategy.stop()
        logger.info("🛑 Strategy stopped")


async def run_custom_example():
    """Run custom configuration example.""" 
    logger.info("🚀 Starting Custom Stacked Market Making Example")
    
    # Build custom configuration
    config = (StackedMarketMakingConfigBuilder()
        .set_basic_info('BERA', ['USDT'], ['binance', 'bybit'])
        .set_inventory_config(
            target_inventory=Decimal('800'),
            max_deviation=Decimal('400'),
            price_method=InventoryPriceMethod.ACCOUNTING
        )
        .add_tob_line(
            hourly_quantity=Decimal('60'),
            spread_bps=Decimal('25'), 
            timeout_seconds=25,
            coefficient_method='inventory'
        )
        .add_passive_line(
            mid_spread_bps=Decimal('12'),
            quantity=Decimal('30'),
            timeout_seconds=90,
            randomization_factor=Decimal('0.025')
        )
        # Using existing proven coefficient system
        .build()
    )
    
    # Validate
    validation_errors = validate_config(config)
    if validation_errors:
        logger.error(f"❌ Custom configuration validation failed: {validation_errors}")
        return
    
    logger.info("✅ Custom configuration validation passed")
    
    # Print configuration summary
    logger.info(f"📋 Configuration Summary:")
    logger.info(f"   TOB Lines: {len(config['tob_lines'])}")
    logger.info(f"   Passive Lines: {len(config['passive_lines'])}")
    logger.info(f"   Target Inventory: {config['inventory']['target_inventory']}")
    logger.info(f"   Max Deviation: {config['inventory']['max_inventory_deviation']}")
    
    # Create and run strategy
    strategy = StackedMarketMakingStrategy(
        instance_id="stacked_custom_001",
        symbol="BERA/USDT",
        exchanges=["binance", "bybit"],
        config=config
    )
    
    try:
        await strategy.initialize()
        await strategy.start()
        
        logger.info("🟢 Custom strategy started")
        
        # Run for shorter duration
        for i in range(6):  # 30 seconds
            await asyncio.sleep(5)
            
            state = strategy.get_strategy_state()
            logger.info(f"📊 Custom Status {i+1}/6: {state.get('performance', {})}")
        
    except Exception as e:
        logger.error(f"❌ Error running custom strategy: {e}")
    finally:
        await strategy.stop()
        logger.info("🛑 Custom strategy stopped")


async def demonstrate_config_templates():
    """Demonstrate all available configuration templates."""
    logger.info("📚 Demonstrating Configuration Templates")
    
    templates = {
        'Conservative': ConfigTemplates.conservative_bera_usdt(),
        'Aggressive Multi-Currency': ConfigTemplates.aggressive_multi_currency(),
        'High Frequency Single Venue': ConfigTemplates.high_frequency_single_venue()
    }
    
    for name, config in templates.items():
        logger.info(f"\n--- {name.upper()} TEMPLATE ---")
        
        # Validate each template
        validation_errors = validate_config(config)
        if validation_errors:
            logger.error(f"❌ {name} template has errors: {validation_errors}")
            continue
        
        logger.info(f"✅ {name} template is valid")
        
        # Show key parameters
        inventory = config['inventory']
        logger.info(f"   Target Inventory: {inventory['target_inventory']}")
        logger.info(f"   Max Deviation: {inventory['max_inventory_deviation']}")
        logger.info(f"   TOB Lines: {len(config['tob_lines'])}")
        logger.info(f"   Passive Lines: {len(config['passive_lines'])}")
        logger.info(f"   Exchanges: {config['exchanges']}")
        
        # Show first TOB line details if present
        if config['tob_lines']:
            tob = config['tob_lines'][0]
            logger.info(f"   TOB Spread: {tob['spread_bps']} bps")
            logger.info(f"   TOB Hourly Qty: {tob['hourly_quantity']}")
        
        # Show first passive line details if present
        if config['passive_lines']:
            passive = config['passive_lines'][0]
            logger.info(f"   Passive Spread: {passive['mid_spread_bps']} bps")
            logger.info(f"   Passive Quantity: {passive['quantity']}")


async def main():
    """Main function to run examples."""
    logger.info("🎯 Stacked Market Making Strategy Examples")
    
    # Demonstrate configuration templates
    await demonstrate_config_templates()
    
    print("\n" + "="*60)
    print("Select example to run:")
    print("1. Conservative BERA/USDT")
    print("2. Custom Configuration") 
    print("3. Just show templates (no trading)")
    print("q. Quit")
    print("="*60)
    
    try:
        choice = input("Enter choice (1-3, q): ").strip().lower()
        
        if choice == '1':
            await run_conservative_example()
        elif choice == '2':
            await run_custom_example()
        elif choice == '3':
            logger.info("Templates demonstrated above ☝️")
        elif choice == 'q':
            logger.info("👋 Goodbye!")
        else:
            logger.warning("Invalid choice, exiting")
            
    except KeyboardInterrupt:
        logger.info("\n⏹️  Interrupted by user")
    except Exception as e:
        logger.error(f"❌ Error in main: {e}")


if __name__ == "__main__":
    # Run the example
    asyncio.run(main())
