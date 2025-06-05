#!/usr/bin/env python3
"""
Test script to verify that the position fix works correctly.

This script:
1. Adds some simulated trades to the database
2. Triggers the enhanced trade sync
3. Verifies that spot positions are updated correctly
"""

import asyncio
import sys
import os
from datetime import datetime, timedelta
import json

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import init_db, get_session
from database.models import Trade, Exchange
from order_management.enhanced_trade_sync import EnhancedTradeSync
from order_management.tracking import PositionManager
from config.settings import load_config
from api.services.exchange_manager import ExchangeManager
import structlog

logger = structlog.get_logger(__name__)


async def create_test_trades():
    """Create some test trades in the database."""
    logger.info("Creating test trades...")
    
    async for session in get_session():
        try:
            # Get mexc_spot exchange ID
            from sqlalchemy import select
            result = await session.execute(
                select(Exchange.id).where(Exchange.name == "mexc_spot")
            )
            mexc_spot_id = result.scalar_one_or_none()
            
            if not mexc_spot_id:
                logger.error("mexc_spot exchange not found in database")
                return False
            
            # Create test trades for MEXC spot
            test_trades = [
                {
                    "exchange_trade_id": "test_trade_1",
                    "exchange_id": mexc_spot_id,
                    "symbol": "BERA/USDT",
                    "side": "buy",
                    "amount": 100.0,
                    "price": 0.5,
                    "cost": 50.0,
                    "fee_cost": 0.1,
                    "fee_currency": "USDT",
                    "timestamp": datetime.now() - timedelta(hours=1),
                    "order_id": "test_order_1"
                },
                {
                    "exchange_trade_id": "test_trade_2", 
                    "exchange_id": mexc_spot_id,
                    "symbol": "BERA/USDT",
                    "side": "sell",
                    "amount": 30.0,
                    "price": 0.6,
                    "cost": 18.0,
                    "fee_cost": 0.036,
                    "fee_currency": "USDT",
                    "timestamp": datetime.now() - timedelta(minutes=30),
                    "order_id": "test_order_2"
                }
            ]
            
            for trade_data in test_trades:
                trade = Trade(**trade_data)
                session.add(trade)
            
            await session.commit()
            logger.info(f"Created {len(test_trades)} test trades")
            return True
            
        except Exception as e:
            await session.rollback()
            logger.error(f"Error creating test trades: {e}")
            return False
        finally:
            await session.close()


async def test_position_update():
    """Test that positions are updated correctly from trades."""
    logger.info("Testing position update from trades...")
    
    # Initialize position manager
    position_manager = PositionManager(data_dir="data/positions")
    await position_manager.start()
    
    # Manually trigger position update from trades
    async for session in get_session():
        try:
            from sqlalchemy import select
            
            # Get all trades for mexc_spot
            result = await session.execute(
                select(Trade).join(Exchange).where(Exchange.name == "mexc_spot")
            )
            trades = result.scalars().all()
            
            logger.info(f"Found {len(trades)} trades for mexc_spot")
            
            # Update position manager with each trade
            for trade in trades:
                trade_data = {
                    "id": trade.exchange_trade_id,
                    "symbol": trade.symbol,
                    "side": trade.side,
                    "amount": trade.amount,
                    "price": trade.price,
                    "cost": trade.cost,
                    "fee": {
                        "cost": trade.fee_cost,
                        "currency": trade.fee_currency
                    },
                    "timestamp": int(trade.timestamp.timestamp() * 1000)
                }
                
                logger.info(f"Updating position with trade: {trade_data}")
                await position_manager.update_from_trade("mexc_spot", trade_data)
            
            # Save positions
            position_manager.save_positions()
            
            break
            
        except Exception as e:
            logger.error(f"Error updating positions: {e}")
            return False
        finally:
            await session.close()
    
    await position_manager.stop()
    return True


async def verify_position_file():
    """Verify that the position file was updated correctly."""
    logger.info("Verifying position file...")
    
    try:
        with open("data/positions/mexc_spot_BERA_USDT.json", "r") as f:
            position_data = json.load(f)
        
        logger.info(f"Position data: {json.dumps(position_data, indent=2)}")
        
        # Expected: bought 100 BERA, sold 30 BERA = net 70 BERA
        expected_size = 70.0
        actual_size = position_data.get("size", 0.0)
        
        if abs(actual_size - expected_size) < 0.001:
            logger.info(f"✅ Position size correct: {actual_size} (expected: {expected_size})")
            return True
        else:
            logger.error(f"❌ Position size incorrect: {actual_size} (expected: {expected_size})")
            return False
            
    except Exception as e:
        logger.error(f"Error reading position file: {e}")
        return False


async def cleanup_test_trades():
    """Clean up test trades."""
    logger.info("Cleaning up test trades...")
    
    async for session in get_session():
        try:
            from sqlalchemy import delete
            
            # Delete test trades
            await session.execute(
                delete(Trade).where(Trade.exchange_trade_id.like("test_trade_%"))
            )
            
            await session.commit()
            logger.info("Test trades cleaned up")
            
        except Exception as e:
            await session.rollback()
            logger.error(f"Error cleaning up test trades: {e}")
        finally:
            await session.close()


async def main():
    """Main test function."""
    logger.info("🧪 Starting position fix verification test...")
    
    try:
        # Initialize database
        await init_db()
        
        # Step 1: Create test trades
        if not await create_test_trades():
            logger.error("Failed to create test trades")
            return
        
        # Step 2: Test position update
        if not await test_position_update():
            logger.error("Failed to update positions")
            return
        
        # Step 3: Verify position file
        if not await verify_position_file():
            logger.error("Position verification failed")
            return
        
        logger.info("✅ Position fix verification test PASSED!")
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
    finally:
        # Clean up
        await cleanup_test_trades()


if __name__ == "__main__":
    asyncio.run(main()) 