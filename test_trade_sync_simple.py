#!/usr/bin/env python
"""
Simple test script for trade sync system.
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add project root to path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

async def test_sync():
    """Test trade sync for PASSIVE_QUOTING account."""
    try:
        # We need to simulate the dependencies that the sync_trades function needs
        # Let's create a minimal test that bypasses the FastAPI dependencies
        
        from order_management.enhanced_trade_sync import EnhancedTradeSync
        from database.repositories import TradeRepository
        from database import get_session
        from order_management.tracking import PositionManager
        
        print("Testing enhanced trade sync directly...")
        
        # Get database session and repositories
        async for session in get_session():
            trade_repository = TradeRepository(session)
            break
        
        # Create position manager
        position_manager = PositionManager(data_dir="data/positions")
        position_manager.set_trade_repository(trade_repository)
        
        # Create a minimal exchange connector dict (empty for now since we don't have real connectors)
        sync_connectors = {}
        
        # Initialize enhanced trade sync
        enhanced_trade_sync = EnhancedTradeSync(
            exchange_connectors=sync_connectors,
            trade_repository=trade_repository,
            position_manager=position_manager
        )
        
        print("Enhanced trade sync initialized successfully!")
        print("Note: No actual trades will be synced without exchange connectors.")
        
        # Check if trades were stored
        from sqlalchemy import select, func
        from database.models import Trade
        
        async for session in get_session():
            result = await session.execute(select(func.count(Trade.id)))
            total_trades = result.scalar_one()
            print(f'Total trades in database: {total_trades}')
            break
            
    except Exception as e:
        print(f'Error: {e}')
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_sync()) 