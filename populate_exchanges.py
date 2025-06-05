#!/usr/bin/env python3
"""
Script to populate the exchanges table with current exchange connections.

This ensures that the database has the exchange records needed for foreign key constraints.
"""

import asyncio
import sys
import os

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import init_db, get_session
from database.models import Exchange
from api.services.exchange_manager import ExchangeManager
from config.settings import load_config
import structlog
from sqlalchemy import text

logger = structlog.get_logger(__name__)


async def populate_exchanges():
    """Populate the exchanges table with current exchange connections."""
    try:
        # Initialize database
        await init_db()
        logger.info("Database initialized")
        
        # Load configuration
        config = load_config()
        logger.info("Configuration loaded")
        
        # Initialize exchange manager
        exchange_manager = ExchangeManager(config)
        await exchange_manager.start()
        logger.info("Exchange manager started")
        
        # Get all connections
        connections = exchange_manager.get_all_connections()
        logger.info(f"Found {len(connections)} exchange connections")
        
        # Populate database
        async for session in get_session():
            try:
                for connection in connections:
                    # Check if exchange already exists
                    existing = await session.execute(
                        text("SELECT id FROM exchanges WHERE name = :name"),
                        {"name": connection.connection_id}
                    )
                    if existing.scalar_one_or_none():
                        logger.info(f"Exchange {connection.connection_id} already exists")
                        continue
                    
                    # Create new exchange record
                    exchange = Exchange(
                        name=connection.connection_id,
                        type=connection.exchange_type,
                        api_key_id=connection.connection_id,  # Use connection_id as reference
                        is_active=connection.has_credentials
                    )
                    
                    session.add(exchange)
                    logger.info(f"Added exchange: {connection.connection_id}")
                
                await session.commit()
                logger.info("Successfully populated exchanges table")
                
                # Verify the results
                result = await session.execute(text("SELECT id, name, type FROM exchanges"))
                exchanges = result.fetchall()
                logger.info(f"Exchanges in database:")
                for exchange in exchanges:
                    logger.info(f"  ID: {exchange[0]}, Name: {exchange[1]}, Type: {exchange[2]}")
                
                break
                
            except Exception as e:
                await session.rollback()
                logger.error(f"Error populating exchanges: {e}")
                raise
            finally:
                await session.close()
        
        # Stop exchange manager
        await exchange_manager.stop()
        logger.info("Exchange manager stopped")
        
    except Exception as e:
        logger.error(f"Failed to populate exchanges: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(populate_exchanges()) 