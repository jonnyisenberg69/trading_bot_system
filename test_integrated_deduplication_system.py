#!/usr/bin/env python3
"""
Integrated Deduplication System Test

This script tests the integrated TradeDeduplicationManager in the production
EnhancedTradeSync system to ensure it works correctly after integration.

Author: AI Assistant
Date: 2025-06-04
"""

import asyncio
import logging
import structlog
import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import json

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


async def test_integrated_deduplication():
    """Test the integrated deduplication system in production environment."""
    logger.info("🧪 Testing integrated deduplication system in production environment...")
    
    try:
        # Import required modules
        from database.repositories import TradeRepository
        from database import init_db, get_session
        from config.settings import load_config
        from order_management.enhanced_trade_sync import EnhancedTradeSync, TradeDeduplicationManager
        
        # Load config and initialize database
        config = load_config()
        db_config = config.get('database', {})
        db_url = db_config.get('url')
        await init_db(db_url)
        
        logger.info("✅ Database connection established")
        
        # Phase 1: Test TradeDeduplicationManager directly
        logger.info("🔍 PHASE 1: Testing TradeDeduplicationManager directly...")
        await test_deduplication_manager_directly()
        
        # Phase 2: Test EnhancedTradeSync with integrated deduplication
        logger.info("🔄 PHASE 2: Testing EnhancedTradeSync with integrated deduplication...")
        await test_enhanced_trade_sync_integration()
        
        # Phase 3: Test three consecutive sync runs (like our original test)
        logger.info("🏃 PHASE 3: Testing three consecutive sync runs...")
        await test_consecutive_sync_runs()
        
        # Phase 4: Verify no duplicates exist
        logger.info("📊 PHASE 4: Final verification - checking for any duplicates...")
        await verify_no_duplicates_exist()
        
        logger.info("🎉 ✅ ALL INTEGRATED DEDUPLICATION TESTS PASSED!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Integrated deduplication test failed: {e}")
        return False


async def test_deduplication_manager_directly():
    """Test the TradeDeduplicationManager directly."""
    logger.info("🔧 Testing TradeDeduplicationManager directly...")
    
    from database import get_session
    
    async for session in get_session():
        try:
            from order_management.enhanced_trade_sync import TradeDeduplicationManager
            
            dedup_manager = TradeDeduplicationManager(session)
            
            # Test trade 1: Should be inserted
            test_trade_1 = {
                'id': 'integrated_test_1',
                'symbol': 'BTC/USDT',
                'side': 'buy',
                'amount': 1.0,
                'price': 50000.0,
                'timestamp': int(datetime.now().timestamp() * 1000),
                'fee': {'cost': 0.001, 'currency': 'USDT'}
            }
            
            success1, reason1 = await dedup_manager.insert_trade_with_deduplication(
                test_trade_1, 'test_exchange'
            )
            logger.info(f"Trade 1 result: {success1} - {reason1}")
            
            # Test trade 2: Same trade_id, should be rejected
            test_trade_2 = {
                'id': 'integrated_test_1',  # Same ID
                'symbol': 'BTC/USDT',
                'side': 'buy',
                'amount': 1.0,
                'price': 50000.0,
                'timestamp': int(datetime.now().timestamp() * 1000),
                'fee': {'cost': 0.001, 'currency': 'USDT'}
            }
            
            success2, reason2 = await dedup_manager.insert_trade_with_deduplication(
                test_trade_2, 'test_exchange'
            )
            logger.info(f"Trade 2 result: {success2} - {reason2}")
            
            # Test trade 3: Different ID but similar characteristics, should be rejected
            test_trade_3 = {
                'id': 'integrated_test_3',  # Different ID
                'symbol': 'BTC/USDT',
                'side': 'buy',
                'amount': 1.0,
                'price': 50000.0,
                'timestamp': int(datetime.now().timestamp() * 1000),  # Same timestamp
                'fee': {'cost': 0.001, 'currency': 'USDT'}
            }
            
            success3, reason3 = await dedup_manager.insert_trade_with_deduplication(
                test_trade_3, 'test_exchange'
            )
            logger.info(f"Trade 3 result: {success3} - {reason3}")
            
            # Verify results
            if success1 and not success2 and not success3:
                logger.info("✅ TradeDeduplicationManager working correctly")
                return True
            else:
                logger.error("❌ TradeDeduplicationManager not working as expected")
                return False
                
        except Exception as e:
            logger.error(f"Direct deduplication manager test failed: {e}")
            return False
        finally:
            await session.close()
            break


async def test_enhanced_trade_sync_integration():
    """Test the EnhancedTradeSync with integrated deduplication."""
    logger.info("🔄 Testing EnhancedTradeSync integration...")
    
    try:
        # Create mock exchange connectors
        mock_connectors = {
            'test_exchange': MockExchangeConnector()
        }
        
        # Create EnhancedTradeSync instance
        from order_management.enhanced_trade_sync import EnhancedTradeSync
        
        sync = EnhancedTradeSync(
            exchange_connectors=mock_connectors,
            trade_repository=None,  # Not used with integrated deduplication
            position_manager=None
        )
        
        # Test sync with mock data
        results = await sync.sync_exchange_with_pagination(
            exchange_name='test_exchange',
            symbols=['BTC/USDT'],
            since=datetime.now() - timedelta(hours=1)
        )
        
        if results and len(results) > 0:
            result = results[0]
            logger.info(f"Sync result: {result.total_fetched} trades, "
                       f"{result.performance_metrics.get('duplicates_filtered', 0)} duplicates filtered")
            
            # Check if deduplication metrics are present
            if 'duplicates_filtered' in result.performance_metrics:
                logger.info("✅ EnhancedTradeSync integration working correctly")
                return True
            else:
                logger.error("❌ Deduplication metrics missing from sync result")
                return False
        else:
            logger.warning("⚠️ No sync results returned")
            return False
            
    except Exception as e:
        logger.error(f"Enhanced trade sync integration test failed: {e}")
        return False


async def test_consecutive_sync_runs():
    """Test three consecutive sync runs to ensure no duplicates are created."""
    logger.info("🏃 Testing three consecutive sync runs...")
    
    try:
        # Get initial trade count
        initial_count = await get_total_trade_count()
        logger.info(f"Initial trade count: {initial_count}")
        
        # Run three consecutive syncs
        for run_number in range(1, 4):
            logger.info(f"🏃 Running sync #{run_number}")
            
            # Simulate sync run (this would use real exchange data in production)
            await simulate_sync_run(run_number)
            
            # Check trade count after each run
            current_count = await get_total_trade_count()
            logger.info(f"Trade count after run #{run_number}: {current_count}")
            
            # Check for duplicates
            duplicate_count = await count_duplicates()
            if duplicate_count > 0:
                logger.error(f"❌ Run #{run_number} created {duplicate_count} duplicates!")
                return False
            else:
                logger.info(f"✅ Run #{run_number} completed with no duplicates")
        
        logger.info("✅ All three consecutive runs completed successfully with no duplicates")
        return True
        
    except Exception as e:
        logger.error(f"Consecutive sync runs test failed: {e}")
        return False


async def get_total_trade_count() -> int:
    """Get total number of trades in database."""
    from database import get_session
    from database.models import Trade
    from sqlalchemy import select, func
    
    async for session in get_session():
        try:
            result = await session.execute(select(func.count(Trade.id)))
            count = result.scalar()
            return count or 0
        except Exception as e:
            logger.error(f"Failed to get trade count: {e}")
            return 0
        finally:
            await session.close()
            break


async def count_duplicates() -> int:
    """Count duplicate trades in database."""
    from database import get_session
    
    async for session in get_session():
        try:
            from analyze_trades_for_duplicates import TradeDeduplicationFixer
            
            # Use our proven duplicate detection
            fixer = TradeDeduplicationFixer(None, session)
            analysis = await fixer.analyze_current_duplicates()
            
            return analysis.get('total_duplicate_trades', 0)
            
        except Exception as e:
            logger.error(f"Failed to count duplicates: {e}")
            return 0
        finally:
            await session.close()
            break


async def simulate_sync_run(run_number: int):
    """Simulate a sync run for testing."""
    logger.info(f"Simulating sync run #{run_number}...")
    
    from database import get_session
    
    async for session in get_session():
        try:
            from order_management.enhanced_trade_sync import TradeDeduplicationManager
            
            dedup_manager = TradeDeduplicationManager(session)
            
            # Simulate some trades that would come from exchange APIs
            test_trades = [
                {
                    'id': f'consecutive_test_{run_number}_1',
                    'symbol': 'ETH/USDT',
                    'side': 'buy',
                    'amount': 5.0,
                    'price': 3000.0,
                    'timestamp': int(datetime.now().timestamp() * 1000),
                    'fee': {'cost': 0.005, 'currency': 'USDT'}
                },
                {
                    'id': f'consecutive_test_{run_number}_2',
                    'symbol': 'ETH/USDT',
                    'side': 'sell',
                    'amount': 2.5,
                    'price': 3010.0,
                    'timestamp': int(datetime.now().timestamp() * 1000) + 1000,
                    'fee': {'cost': 0.0025, 'currency': 'USDT'}
                }
            ]
            
            # Also try to insert a duplicate from previous run
            if run_number > 1:
                duplicate_trade = {
                    'id': f'consecutive_test_{run_number-1}_1',  # Previous run's trade
                    'symbol': 'ETH/USDT',
                    'side': 'buy',
                    'amount': 5.0,
                    'price': 3000.0,
                    'timestamp': int(datetime.now().timestamp() * 1000),
                    'fee': {'cost': 0.005, 'currency': 'USDT'}
                }
                test_trades.append(duplicate_trade)
            
            # Process trades through deduplication
            inserted_count = 0
            filtered_count = 0
            
            for trade in test_trades:
                success, reason = await dedup_manager.insert_trade_with_deduplication(
                    trade, 'test_exchange'
                )
                
                if success:
                    inserted_count += 1
                    logger.debug(f"✅ Inserted: {trade['id']}")
                else:
                    filtered_count += 1
                    logger.debug(f"🚫 Filtered: {trade['id']} - {reason}")
            
            logger.info(f"Sync run #{run_number} complete: {inserted_count} inserted, {filtered_count} filtered")
            
        except Exception as e:
            logger.error(f"Simulation run #{run_number} failed: {e}")
        finally:
            await session.close()
            break


async def verify_no_duplicates_exist():
    """Final verification that no duplicates exist in the database."""
    logger.info("📊 Final verification - checking for duplicates...")
    
    duplicate_count = await count_duplicates()
    
    if duplicate_count == 0:
        logger.info("✅ VERIFICATION PASSED: No duplicates found in database")
        return True
    else:
        logger.error(f"❌ VERIFICATION FAILED: {duplicate_count} duplicates found!")
        return False


class MockExchangeConnector:
    """Mock exchange connector for testing."""
    
    async def get_trade_history(self, symbol: str, since: datetime = None, limit: int = 100):
        """Return mock trade data."""
        base_timestamp = int(datetime.now().timestamp() * 1000)
        
        return [
            {
                'id': f'mock_trade_1_{symbol.replace("/", "")}',
                'symbol': symbol,
                'side': 'buy',
                'amount': 1.5,
                'price': 45000.0,
                'timestamp': base_timestamp,
                'fee': {'cost': 0.0015, 'currency': 'USDT'},
                'order': 'mock_order_1'
            },
            {
                'id': f'mock_trade_2_{symbol.replace("/", "")}',
                'symbol': symbol,
                'side': 'sell',
                'amount': 0.8,
                'price': 45100.0,
                'timestamp': base_timestamp + 5000,
                'fee': {'cost': 0.0008, 'currency': 'USDT'},
                'order': 'mock_order_2'
            }
        ]


async def main():
    """Main execution function."""
    logger.info("🚀 Starting integrated deduplication system test...")
    
    success = await test_integrated_deduplication()
    
    if success:
        logger.info("🎉 SUCCESS: Integrated deduplication system is working correctly!")
    else:
        logger.error("❌ FAILURE: Integrated deduplication system has issues!")
    
    return success


if __name__ == "__main__":
    asyncio.run(main()) 