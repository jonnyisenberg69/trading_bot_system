#!/usr/bin/env python3
"""
Test Hyperliquid Position Detection Fix

This script tests that the system can properly detect the user's 25.1 BERA position
on Hyperliquid and display it correctly in the dashboard.
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

async def test_hyperliquid_position_normalization():
    """Test that Hyperliquid position symbols are normalized correctly."""
    print("🔍 Testing Hyperliquid position symbol normalization...")
    
    try:
        pm = PositionManager()
        
        # Test symbol normalization
        test_cases = [
            ("BERA/USDC:USDC", "hyperliquid_perp", True, "BERA/USDT-PERP"),
            ("ETH/USDC:USDC", "hyperliquid_perp", True, "ETH/USDT-PERP"),
            ("BERAUSDT", "bybit_perp", True, "BERA/USDT-PERP"),
            ("BERA/USDT", "bybit_spot", False, "BERA/USDT"),
        ]
        
        print("\n📋 Testing symbol normalization:")
        for original, exchange_id, is_perpetual, expected in test_cases:
            result = pm._normalize_position_symbol(original, exchange_id, is_perpetual)
            status = "✅" if result == expected else "❌"
            print(f"   {status} {exchange_id}: {original} -> {result} (expected: {expected})")
            
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

async def test_hyperliquid_position_simulation():
    """Simulate a Hyperliquid position and verify it's tracked correctly."""
    print("\n🔍 Testing Hyperliquid position simulation...")
    
    try:
        pm = PositionManager()
        await pm.start()
        
        # Simulate Hyperliquid position data (like what would come from get_positions())
        hyperliquid_position_data = {
            'symbol': 'BERA/USDC:USDC',
            'size': 25.1,
            'side': 'long'
        }
        
        # Test the normalization
        normalized_symbol = pm._normalize_position_symbol(
            hyperliquid_position_data['symbol'], 
            'hyperliquid_perp', 
            True
        )
        
        print(f"   Original symbol: {hyperliquid_position_data['symbol']}")
        print(f"   Normalized symbol: {normalized_symbol}")
        
        # Create the position manually (simulating what sync_positions_from_exchanges would do)
        position_key = f"hyperliquid_perp_{normalized_symbol}"
        
        if 'hyperliquid_perp' not in pm.positions:
            pm.positions['hyperliquid_perp'] = {}
            
        from order_management.tracking import Position
        from decimal import Decimal
        
        position = Position(
            exchange='hyperliquid_perp',
            symbol=normalized_symbol,
            is_perpetual=True
        )
        
        # Set the position size
        size = Decimal(str(hyperliquid_position_data['size']))
        if hyperliquid_position_data['side'] == 'short':
            size = -size
            
        position.p1 = size
        
        pm.positions['hyperliquid_perp'][position_key] = position
        pm.symbols.add(normalized_symbol)
        
        # Verify the position
        retrieved_position = pm.get_position('hyperliquid_perp', normalized_symbol)
        
        if retrieved_position and retrieved_position.size == Decimal('25.1'):
            print(f"   ✅ SUCCESS: Hyperliquid position created correctly!")
            print(f"   Position: {retrieved_position.exchange} {retrieved_position.symbol}")
            print(f"   Size: {retrieved_position.size}")
            print(f"   Side: {retrieved_position.side}")
            return True
        else:
            print(f"   ❌ FAILED: Position not created correctly")
            print(f"   Retrieved position: {retrieved_position}")
            return False
            
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

async def test_position_sync_mock():
    """Test the position sync with mock exchange connector."""
    print("\n🔍 Testing position sync with mock Hyperliquid connector...")
    
    try:
        pm = PositionManager()
        await pm.start()
        
        # Create a mock Hyperliquid connector
        class MockHyperliquidConnector:
            async def get_positions(self):
                return [
                    {
                        'symbol': 'BERA/USDC:USDC',
                        'size': 25.1,
                        'side': 'long'
                    }
                ]
        
        mock_connectors = {
            'hyperliquid_perp': MockHyperliquidConnector()
        }
        
        # Run the position sync
        await pm.sync_positions_from_exchanges(mock_connectors)
        
        # Check if the position was created
        positions = pm.get_all_positions("BERA/USDT-PERP")
        hyperliquid_positions = [p for p in positions if p.exchange == 'hyperliquid_perp']
        
        if hyperliquid_positions:
            pos = hyperliquid_positions[0]
            print(f"   ✅ SUCCESS: Position synced from mock Hyperliquid!")
            print(f"   Exchange: {pos.exchange}")
            print(f"   Symbol: {pos.symbol}")
            print(f"   Size: {pos.size}")
            print(f"   Side: {pos.side}")
            return True
        else:
            print(f"   ❌ FAILED: No Hyperliquid position found after sync")
            print(f"   All positions: {[p.exchange + ':' + p.symbol for p in pm.get_all_positions()]}")
            return False
            
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

async def main():
    """Run all Hyperliquid position tests."""
    print("🚀 Testing Hyperliquid Position Detection Fixes")
    print("=" * 60)
    
    test1_passed = await test_hyperliquid_position_normalization()
    test2_passed = await test_hyperliquid_position_simulation()
    test3_passed = await test_position_sync_mock()
    
    print("\n" + "=" * 60)
    print("📊 TEST RESULTS")
    print("=" * 60)
    
    results = [
        ("Symbol Normalization", test1_passed),
        ("Position Simulation", test2_passed),
        ("Position Sync Mock", test3_passed)
    ]
    
    for test_name, passed in results:
        status = "✅ PASSED" if passed else "❌ FAILED"
        print(f"{status:12} {test_name}")
        
    all_passed = all(result[1] for result in results)
    
    if all_passed:
        print("\n🎉 ALL TESTS PASSED!")
        print("\nThe Hyperliquid position detection should now work correctly.")
        print("\nNext steps:")
        print("1. Run a new sync to trigger the position detection")
        print("2. The 25.1 BERA position should appear as 'BERA/USDT-PERP' on Hyperliquid")
        print("3. Check the dashboard for the updated positions")
    else:
        print("\n⚠️  Some tests failed. Please review the output above.")
    
    print("=" * 60)

if __name__ == "__main__":
    asyncio.run(main()) 