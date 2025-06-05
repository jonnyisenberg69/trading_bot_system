#!/usr/bin/env python
"""
Script to wipe all trade data from PostgreSQL database.

This script will:
1. Delete all trades from the database
2. Reset position data files
3. Provide confirmation before executing
"""

import sys
import asyncio
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from database import get_session
from database.repositories import TradeRepository
from order_management.tracking import PositionManager
from sqlalchemy import text
import structlog

logger = structlog.get_logger(__name__)


async def wipe_all_trade_data():
    """Wipe all trade data from database and reset positions."""
    
    print("🚨 WARNING: This will delete ALL trade data from the database!")
    print("This action cannot be undone.")
    print()
    
    # Get confirmation
    confirm = input("Type 'DELETE ALL TRADES' to confirm: ")
    if confirm != "DELETE ALL TRADES":
        print("❌ Operation cancelled.")
        return
    
    print("\n🗑️  Starting trade data cleanup...")
    
    try:
        # Get database session
        async for session in get_session():
            trade_repository = TradeRepository(session)
            
            # Count current trades
            print("📊 Counting existing trades...")
            
            # Delete all trades
            print("🔥 Deleting all trades from database...")
            
            # Execute the deletion using proper text() function
            result = await session.execute(text("DELETE FROM trades"))
            deleted_count = result.rowcount
            await session.commit()
            
            print(f"✅ Deleted {deleted_count} trades from database")
            break
    
    except Exception as e:
        logger.error(f"Error deleting trades from database: {e}")
        print(f"❌ Error: {e}")
        return
    
    # Reset position data files
    try:
        print("🔄 Resetting position data files...")
        position_manager = PositionManager(data_dir="data/positions")
        await position_manager.reset_all_positions()
        print("✅ Position data files reset")
        
    except Exception as e:
        logger.error(f"Error resetting position files: {e}")
        print(f"❌ Error resetting positions: {e}")
    
    print("\n🎉 Trade data cleanup completed!")
    print("📋 Summary:")
    print(f"   - Deleted {deleted_count} trades from PostgreSQL")
    print("   - Reset all position data files")
    print("   - System is ready for fresh trade sync")


async def main():
    """Main function."""
    await wipe_all_trade_data()


if __name__ == "__main__":
    asyncio.run(main()) 