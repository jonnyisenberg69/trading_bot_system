#!/usr/bin/env python
"""
Test script for position endpoints.
"""

import asyncio
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

async def test_position_endpoints():
    """Test position endpoints directly."""
    try:
        from api.routes.position_routes import get_position_summary, get_net_positions, export_positions
        from database.repositories import TradeRepository
        from database import get_session
        from order_management.tracking import PositionManager
        
        print("Testing position endpoints...")
        
        # Get database session and repositories
        async for session in get_session():
            trade_repository = TradeRepository(session)
            break
        
        # Create position manager
        position_manager = PositionManager(data_dir="data/positions")
        position_manager.set_trade_repository(trade_repository)
        
        print("Position manager initialized")
        
        # Test position summary endpoint
        print("\n1. Testing position summary...")
        try:
            summary = await get_position_summary(position_manager=position_manager)
            print(f"Position summary: {summary}")
        except Exception as e:
            print(f"Position summary error: {e}")
            import traceback
            traceback.print_exc()
        
        # Test net positions endpoint with None symbol (fix the Query issue)
        print("\n2. Testing net positions...")
        try:
            net_positions = await get_net_positions(symbol=None, position_manager=position_manager)
            print(f"Net positions: {net_positions}")
        except Exception as e:
            print(f"Net positions error: {e}")
            import traceback
            traceback.print_exc()
        
        # Test net positions endpoint with specific symbol
        print("\n2b. Testing net positions for BERA/USDT...")
        try:
            net_positions_bera = await get_net_positions(symbol="BERA/USDT", position_manager=position_manager)
            print(f"Net positions for BERA/USDT: {net_positions_bera}")
        except Exception as e:
            print(f"Net positions for BERA/USDT error: {e}")
            import traceback
            traceback.print_exc()
        
        # Test export positions endpoint
        print("\n3. Testing export positions...")
        try:
            export_data = await export_positions(position_manager=position_manager)
            print(f"Export data keys: {list(export_data.keys())}")
            print(f"Summary: {export_data.get('summary', {})}")
            print(f"Net positions count: {len(export_data.get('net_positions', {}))}")
            print(f"Detailed positions count: {len(export_data.get('detailed_positions', []))}")
        except Exception as e:
            print(f"Export positions error: {e}")
            import traceback
            traceback.print_exc()
            
    except Exception as e:
        print(f'Error: {e}')
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_position_endpoints()) 