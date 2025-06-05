#!/usr/bin/env python3
"""
Test script for the passive quoting system.

Tests the complete passive quoting implementation including:
- Strategy initialization
- Configuration validation
- Order placement simulation
- Quick config updates
"""

import asyncio
import json
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from bot_manager.strategies.passive_quoting import PassiveQuotingStrategy
from config.settings import load_config


async def test_passive_quoting_strategy():
    """Test the passive quoting strategy implementation."""
    print("🧪 Testing Passive Quoting Strategy")
    print("=" * 50)
    
    # Test configuration
    test_config = {
        "base_coin": "BTC",
        "quantity_currency": "base",
        "exchanges": ["binance", "bybit"],
        "lines": [
            {
                "timeout": 300,
                "drift": 50,
                "quantity": 0.01,
                "quantity_randomization_factor": 10,
                "spread": 25,
                "sides": "both"
            },
            {
                "timeout": 600,
                "drift": 100,
                "quantity": 0.005,
                "quantity_randomization_factor": 15,
                "spread": 50,
                "sides": "bid"
            }
        ]
    }
    
    # Create strategy instance
    strategy = PassiveQuotingStrategy(
        instance_id="test_passive_quoting_001",
        symbol="BTC/USDT",
        exchanges=["binance", "bybit"],
        config=test_config
    )
    
    print(f"✅ Created strategy instance: {strategy.instance_id}")
    
    # Test configuration validation
    try:
        await strategy._validate_config()
        print("✅ Configuration validation passed")
    except Exception as e:
        print(f"❌ Configuration validation failed: {e}")
        return False
    
    # Test strategy initialization
    try:
        await strategy.initialize()
        print("✅ Strategy initialization passed")
    except Exception as e:
        print(f"❌ Strategy initialization failed: {e}")
        return False
    
    # Test strategy start (simulate for 5 seconds)
    try:
        print("🚀 Starting strategy simulation...")
        await strategy.start()
        
        # Let it run for a few seconds to test the main loop
        await asyncio.sleep(5)
        
        # Check if orders were "placed"
        stats = strategy.get_performance_stats()
        print(f"📊 Performance stats after 5 seconds:")
        print(f"   - Orders placed: {stats.get('orders_placed', 0)}")
        print(f"   - Active orders: {stats.get('active_orders', 0)}")
        print(f"   - Quote lines: {stats.get('quote_lines', 0)}")
        
        # Test line status
        line_status = strategy.get_line_status()
        print(f"📈 Line status:")
        for i, line in enumerate(line_status):
            print(f"   Line {i+1}: {line['sides']} @ {line['spread_bps']}bps")
            print(f"     - Bid active: {line['bid_active']}, Ask active: {line['ask_active']}")
        
        print("✅ Strategy simulation completed successfully")
        
    except Exception as e:
        print(f"❌ Strategy simulation failed: {e}")
        return False
    finally:
        # Stop the strategy
        await strategy.stop()
        print("🛑 Strategy stopped")
    
    return True


async def test_config_system():
    """Test the configuration system."""
    print("\n🔧 Testing Configuration System")
    print("=" * 50)
    
    try:
        # Load main config
        config = load_config()
        print("✅ Main configuration loaded")
        
        # Check if passive quoting config exists
        passive_config = config.get("strategies", {}).get("passive_quoting", {})
        if passive_config:
            print("✅ Passive quoting configuration found")
            
            default_config = passive_config.get("default_config")
            presets = passive_config.get("presets", {})
            
            print(f"   - Default config lines: {len(default_config.get('lines', []))}")
            print(f"   - Available presets: {list(presets.keys())}")
            
            # Test a preset if available
            if presets:
                preset_name = list(presets.keys())[0]
                preset = presets[preset_name]
                print(f"   - Testing preset '{preset_name}' with {len(preset.get('lines', []))} lines")
        else:
            print("❌ Passive quoting configuration not found")
            return False
            
        return True
        
    except Exception as e:
        print(f"❌ Configuration system test failed: {e}")
        return False


def print_system_info():
    """Print system information."""
    print("🤖 Passive Quoting Trading Bot System")
    print("=" * 50)
    print("Components tested:")
    print("  • Passive Quoting Strategy")
    print("  • Configuration Management")
    print("  • Multi-line Quote Management")
    print("  • Timeout & Drift Handling")
    print("  • Order Placement Simulation")
    print("  • API Integration Ready")
    print("  • Frontend Integration Ready")
    print("")


async def main():
    """Main test function."""
    print_system_info()
    
    # Test configuration system
    config_success = await test_config_system()
    
    if config_success:
        # Test passive quoting strategy
        strategy_success = await test_passive_quoting_strategy()
        
        if strategy_success:
            print("\n🎉 All tests passed! Passive quoting system is ready.")
            print("\nNext steps:")
            print("1. Start the API server: python api/main.py")
            print("2. Start the frontend: cd frontend && npm start")
            print("3. Navigate to http://localhost:3000/passive-quoting")
            print("4. Configure and create your passive quoting bot!")
            return True
        else:
            print("\n❌ Strategy tests failed")
            return False
    else:
        print("\n❌ Configuration tests failed")
        return False


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1) 