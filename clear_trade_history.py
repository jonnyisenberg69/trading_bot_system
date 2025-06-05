#!/usr/bin/env python3
"""
Clear Trade History

This script safely clears all trades from the database for a fresh start.

Author: AI Assistant
Date: 2025-06-04
"""

import asyncio
import logging
import structlog
import sys
import os
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

# Setup logging
logging.basicConfig(level=logging.INFO)
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
        structlog.processors.JSONRenderer()
    ]
)
logger = structlog.get_logger(__name__)


async def clear_all_trades():
    """Clear all trades from the database."""
    logger.info("🧹 Starting to clear all trades from database...")
    
    try:
        # Import required modules
        from database import init_db, get_session
        from config.settings import load_config
        from database.models import Trade
        from sqlalchemy import select, func, delete
        
        # Load config and initialize database
        config = load_config()
        db_config = config.get('database', {})
        db_url = db_config.get('url')
        await init_db(db_url)
        
        async for session in get_session():
            try:
                # Get current trade count
                result = await session.execute(select(func.count(Trade.id)))
                initial_count = result.scalar()
                
                logger.info(f"📊 Current trade count: {initial_count}")
                
                if initial_count == 0:
                    logger.info("✅ Database is already clean - no trades to remove")
                    return
                
                # Ask for confirmation (in production, you might want to require a flag)
                logger.info(f"⚠️ About to delete {initial_count} trades from database")
                
                # Delete all trades
                delete_stmt = delete(Trade)
                result = await session.execute(delete_stmt)
                deleted_count = result.rowcount
                
                # Commit the deletion
                await session.commit()
                
                # Verify deletion
                result = await session.execute(select(func.count(Trade.id)))
                final_count = result.scalar()
                
                logger.info(f"✅ Successfully deleted {deleted_count} trades")
                logger.info(f"📊 Final trade count: {final_count}")
                
                if final_count == 0:
                    logger.info("🎉 Database cleared successfully!")
                else:
                    logger.warning(f"⚠️ {final_count} trades still remain in database")
                
                return deleted_count
                
            except Exception as e:
                logger.error(f"❌ Error during trade deletion: {e}")
                await session.rollback()
                raise
            finally:
                await session.close()
                break
        
    except Exception as e:
        logger.error(f"❌ Failed to clear trades: {e}")
        raise


async def main():
    """Main execution function."""
    logger.info("🚀 Starting trade history clearing...")
    
    try:
        deleted_count = await clear_all_trades()
        if deleted_count is not None:
            logger.info(f"🎉 SUCCESS: Cleared {deleted_count} trades from database")
        else:
            logger.info("✅ Database was already clean")
    except Exception as e:
        logger.error(f"❌ FAILURE: {e}")


if __name__ == "__main__":
    asyncio.run(main()) 