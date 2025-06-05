#!/usr/bin/env python3
"""
Test Position Separation Fixes

This script tests that the position manager now correctly:
1. Separates spot and perp positions 
2. Creates zero positions for all exchanges
3. Displays all connected exchanges
"""

import asyncio
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from order_management.tracking import PositionManager
import structlog

logger = structlog.get_logger(__name__)

async def test_position_separation():
    """Test that spot and perp positions are kept separate."""
    print("🔍 Testing position separation for spot vs perp...")
    
    try:
        pm = PositionManager()
        await pm.start()
        
        # Mock exchange connectors
        mock_connectors = {
            'binance_spot': {'name': 'binance_spot'},
            'binance_perp': {'name': 'binance_perp'},
            'bybit_spot': {'name': 'bybit_spot'},
            'bybit_perp': {'name': 'bybit_perp'},
            'mexc_spot': {'name': 'mexc_spot'},
            'gateio_spot': {'name': 'gateio_spot'},
            'bitget_spot': {'name': 'bitget_spot'},
            'hyperliquid_perp': {'name': 'hyperliquid_perp'}
        }
        
        # Test 1: Create positions for spot symbol
        print("\n📋 Creating positions for BERA/USDT (spot symbol)")
        pm.ensure_exchange_positions(mock_connectors, "BERA/USDT")
        
        spot_positions = pm.get_all_positions("BERA/USDT")
        print(f"   Spot positions created: {len(spot_positions)}")
        for pos in spot_positions:
            print(f"   - {pos.exchange}: {pos.symbol} (perp: {pos.is_perpetual})")
        
        # Test 2: Create positions for perp symbol
        print("\n📋 Creating positions for BERA/USDT-PERP (perp symbol)")
        pm.ensure_exchange_positions(mock_connectors, "BERA/USDT-PERP")
        
        perp_positions = pm.get_all_positions("BERA/USDT-PERP")
        print(f"   Perp positions created: {len(perp_positions)}")
        for pos in perp_positions:
            print(f"   - {pos.exchange}: {pos.symbol} (perp: {pos.is_perpetual})")
        
        # Test 3: Simulate trades and verify separation
        print("\n📋 Simulating trades to test position separation...")
        
        # Add trade to bybit_spot
        spot_trade = {
            'id': 'spot_trade_1',
            'symbol': 'BERA/USDT',
            'side': 'buy',
            'amount': 10.0,
            'price': 2.50,
            'cost': 25.0,
            'timestamp': '2025-06-04T19:30:00Z',
            'fee': {'cost': 0.01, 'currency': 'USDT'}
        }
        await pm.update_from_trade('bybit_spot', spot_trade)
        
        # Add trade to bybit_perp
        perp_trade = {
            'id': 'perp_trade_1', 
            'symbol': 'BERA/USDT-PERP',
            'side': 'sell',
            'amount': 5.0,
            'price': 2.60,
            'cost': 13.0,
            'timestamp': '2025-06-04T19:31:00Z',
            'fee': {'cost': 0.005, 'currency': 'USDT'}
        }
        await pm.update_from_trade('bybit_perp', perp_trade)
        
        # Verify positions are separate
        bybit_spot_pos = pm.get_position('bybit_spot', 'BERA/USDT')
        bybit_perp_pos = pm.get_position('bybit_perp', 'BERA/USDT-PERP')
        
        print(f"\n✅ Position Separation Results:")
        print(f"   Bybit Spot (BERA/USDT): size={bybit_spot_pos.size if bybit_spot_pos else 'None'}")
        print(f"   Bybit Perp (BERA/USDT-PERP): size={bybit_perp_pos.size if bybit_perp_pos else 'None'}")
        
        if (bybit_spot_pos and bybit_perp_pos and 
            bybit_spot_pos.size != bybit_perp_pos.size):
            print("   ✅ SUCCESS: Spot and perp positions are correctly separated!")
            return True
        else:
            print("   ❌ FAILED: Positions are still being mixed")
            return False
            
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

async def test_all_exchange_display():
    """Test that all exchanges appear in position lists."""
    print("\n🔍 Testing that all exchanges appear with zero positions...")
    
    try:
        pm = PositionManager()
        await pm.start()
        
        # Mock all exchange connectors
        mock_connectors = {
            'binance_spot': {'name': 'binance_spot'},
            'binance_perp': {'name': 'binance_perp'},
            'bybit_spot': {'name': 'bybit_spot'},
            'bybit_perp': {'name': 'bybit_perp'},
            'mexc_spot': {'name': 'mexc_spot'},
            'gateio_spot': {'name': 'gateio_spot'},
            'bitget_spot': {'name': 'bitget_spot'},
            'hyperliquid_perp': {'name': 'hyperliquid_perp'}
        }
        
        # Ensure positions for both symbol types
        pm.ensure_exchange_positions(mock_connectors, "BERA/USDT")      # Spot
        pm.ensure_exchange_positions(mock_connectors, "BERA/USDT-PERP") # Perp
        
        # Get all positions
        all_positions = pm.get_all_positions()
        
        print(f"\n📊 All positions created: {len(all_positions)}")
        
        # Group by exchange
        exchanges_found = set()
        for pos in all_positions:
            exchanges_found.add(pos.exchange)
            print(f"   - {pos.exchange}: {pos.symbol} (size: {pos.size})")
        
        expected_exchanges = set(mock_connectors.keys())
        missing_exchanges = expected_exchanges - exchanges_found
        
        if not missing_exchanges:
            print(f"\n✅ SUCCESS: All {len(expected_exchanges)} exchanges have positions!")
            return True
        else:
            print(f"\n❌ MISSING exchanges: {missing_exchanges}")
            return False
            
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

async def main():
    """Run all tests."""
    print("🚀 Testing Position Separation Fixes")
    print("=" * 60)
    
    test1_passed = await test_position_separation()
    test2_passed = await test_all_exchange_display()
    
    print("\n" + "=" * 60)
    print("📊 TEST RESULTS")
    print("=" * 60)
    
    if test1_passed:
        print("✅ Position Separation: PASSED")
    else:
        print("❌ Position Separation: FAILED")
        
    if test2_passed:
        print("✅ All Exchange Display: PASSED")
    else:
        print("❌ All Exchange Display: FAILED")
        
    if test1_passed and test2_passed:
        print("\n🎉 ALL TESTS PASSED! The fixes should work correctly.")
        print("\nNext steps:")
        print("1. Run a trade sync to see the real results")
        print("2. Check the dashboard - you should see:")
        print("   - Separate bybit_spot and bybit_perp positions")
        print("   - hyperliquid_perp and bitget_spot with 0.0 values")
        print("   - No more duplicate positions")
    else:
        print("\n⚠️  Some tests failed. Please review the output above.")
    
    print("=" * 60)

if __name__ == "__main__":
    asyncio.run(main()) 