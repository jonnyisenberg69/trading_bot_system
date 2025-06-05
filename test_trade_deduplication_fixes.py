#!/usr/bin/env python3
"""
Comprehensive Trade Deduplication Fix and Testing

This script will:
1. Analyze current duplicates 
2. Implement proper deduplication logic
3. Test with three consecutive runs to ensure no new duplicates
4. Verify the fix is working correctly

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
from typing import Dict, List, Optional, Any, Tuple, Set
import json
from collections import defaultdict, Counter
from sqlalchemy import select, and_, or_

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


class TradeDeduplicationFixer:
    """Comprehensive trade deduplication system."""
    
    def __init__(self, trade_repository, session):
        self.trade_repository = trade_repository
        self.session = session
        self.processed_trade_ids = set()
        self.processed_composite_keys = set()
    
    async def analyze_current_duplicates(self) -> Dict[str, Any]:
        """Analyze current duplicate situation."""
        logger.info("🔍 Analyzing current duplicate situation...")
        
        trades = await self.get_all_trades()
        
        analysis = {
            "total_trades": len(trades),
            "cross_exchange_duplicates": self.find_cross_exchange_duplicates(trades),
            "identical_duplicates": self.find_identical_duplicates(trades),
            "same_tradeid_duplicates": self.find_same_tradeid_duplicates(trades),
            "timestamp": datetime.now().isoformat()
        }
        
        # Calculate total duplicate count
        total_duplicates = 0
        for dup_type, groups in analysis.items():
            if isinstance(groups, list):
                for group in groups:
                    total_duplicates += len(group) - 1  # All but one are duplicates
        
        analysis["total_duplicate_trades"] = total_duplicates
        analysis["duplicate_percentage"] = (total_duplicates / len(trades) * 100) if trades else 0
        
        logger.info(f"📊 Analysis complete: {len(trades)} trades, {total_duplicates} duplicates ({analysis['duplicate_percentage']:.2f}%)")
        
        return analysis
    
    async def get_all_trades(self) -> List[Dict[str, Any]]:
        """Get all trades from database."""
        from database.models import Trade
        
        stmt = select(Trade).order_by(Trade.timestamp.desc())
        result = await self.session.execute(stmt)
        trades = result.scalars().all()
        
        trade_list = []
        for trade in trades:
            trade_dict = {
                'id': trade.id,
                'trade_id': trade.exchange_trade_id,
                'exchange_id': trade.exchange_id,
                'symbol': trade.symbol,
                'side': trade.side,
                'amount': float(trade.amount),
                'price': float(trade.price),
                'cost': float(trade.cost) if trade.cost else 0,
                'fee_cost': float(trade.fee_cost) if trade.fee_cost else 0,
                'fee_currency': trade.fee_currency,
                'timestamp': int(trade.timestamp.timestamp() * 1000) if trade.timestamp else None,
                'datetime': trade.timestamp.isoformat() if trade.timestamp else None,
                'order_id': trade.order_id,
                'created_at': trade.created_at.isoformat() if trade.created_at else None
            }
            trade_list.append(trade_dict)
        
        return trade_list
    
    def find_cross_exchange_duplicates(self, trades: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
        """Find trades with same trade_id across different exchanges."""
        trade_groups = defaultdict(list)
        
        for trade in trades:
            trade_groups[trade['trade_id']].append(trade)
        
        duplicates = []
        for trade_id, group in trade_groups.items():
            if len(group) > 1:
                exchanges = set(t['exchange_id'] for t in group)
                if len(exchanges) > 1:  # Same trade_id on different exchanges
                    duplicates.append(group)
        
        return duplicates
    
    def find_identical_duplicates(self, trades: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
        """Find trades that are identical in all aspects."""
        composite_groups = defaultdict(list)
        
        for trade in trades:
            # Create composite key for identical trade detection
            key = (
                trade['symbol'],
                trade['side'],
                round(trade['amount'], 8),
                round(trade['price'], 8),
                trade['timestamp'] // 1000 if trade['timestamp'] else 0,
                trade['exchange_id']  # Include exchange to avoid false positives
            )
            composite_groups[key].append(trade)
        
        return [group for group in composite_groups.values() if len(group) > 1]
    
    def find_same_tradeid_duplicates(self, trades: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
        """Find trades with same trade_id on same exchange."""
        trade_groups = defaultdict(list)
        
        for trade in trades:
            key = (trade['trade_id'], trade['exchange_id'])
            trade_groups[key].append(trade)
        
        return [group for group in trade_groups.values() if len(group) > 1]
    
    async def remove_duplicates(self, dry_run: bool = False) -> Dict[str, Any]:
        """Remove duplicate trades from database."""
        logger.info(f"🧹 {'DRY RUN: ' if dry_run else ''}Starting duplicate removal...")
        
        from database.models import Trade
        
        analysis = await self.analyze_current_duplicates()
        
        if analysis["total_duplicate_trades"] == 0:
            logger.info("✅ No duplicates found - database is clean!")
            return {"removed": 0, "kept": analysis["total_trades"]}
        
        removed_count = 0
        kept_count = 0
        
        # Get all duplicate groups
        all_duplicate_groups = []
        for dup_type in ["cross_exchange_duplicates", "identical_duplicates", "same_tradeid_duplicates"]:
            if dup_type in analysis:
                all_duplicate_groups.extend(analysis[dup_type])
        
        # Track IDs to remove
        ids_to_remove = set()
        
        for group in all_duplicate_groups:
            if len(group) <= 1:
                continue
            
            # Sort by database ID to keep the first one (oldest entry)
            sorted_group = sorted(group, key=lambda x: x['id'])
            
            # Keep the first one, mark others for removal
            keep_trade = sorted_group[0]
            remove_trades = sorted_group[1:]
            
            logger.info(f"📝 Group with {len(group)} duplicates:")
            logger.info(f"   KEEPING: ID:{keep_trade['id']} Trade:{keep_trade['trade_id']} Ex:{keep_trade['exchange_id']}")
            
            for remove_trade in remove_trades:
                logger.info(f"   REMOVING: ID:{remove_trade['id']} Trade:{remove_trade['trade_id']} Ex:{remove_trade['exchange_id']}")
                ids_to_remove.add(remove_trade['id'])
            
            kept_count += 1
            removed_count += len(remove_trades)
        
        if not dry_run and ids_to_remove:
            # Actually remove the duplicates
            stmt = select(Trade).where(Trade.id.in_(ids_to_remove))
            result = await self.session.execute(stmt)
            trades_to_delete = result.scalars().all()
            
            for trade in trades_to_delete:
                await self.session.delete(trade)
            
            await self.session.commit()
            logger.info(f"✅ Removed {len(trades_to_delete)} duplicate trades from database")
        
        return {"removed": removed_count, "kept": analysis["total_trades"] - removed_count}
    
    async def is_duplicate_trade(self, trade_data: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Check if a trade is a duplicate before inserting.
        
        Returns: (is_duplicate, reason)
        """
        # Check 1: Same trade_id already exists
        if trade_data.get('exchange_trade_id') in self.processed_trade_ids:
            return True, "Same trade_id already processed in this session"
        
        # Check 2: Check database for existing trade_id
        from database.models import Trade
        
        existing_trade = await self.session.execute(
            select(Trade).where(Trade.exchange_trade_id == trade_data.get('exchange_trade_id'))
        )
        if existing_trade.scalar_one_or_none():
            return True, "Trade_id already exists in database"
        
        # Check 3: Composite key check (same trade characteristics)
        composite_key = (
            trade_data.get('symbol'),
            trade_data.get('side'),
            round(float(trade_data.get('amount', 0)), 8),
            round(float(trade_data.get('price', 0)), 8),
            trade_data.get('timestamp', 0) // 1000,  # Round to second
            trade_data.get('exchange_id')
        )
        
        if composite_key in self.processed_composite_keys:
            return True, "Same trade characteristics already processed"
        
        # Check database for similar trades (within 1 second)
        timestamp = trade_data.get('timestamp', 0)
        timestamp_start = timestamp - 1000  # 1 second before
        timestamp_end = timestamp + 1000    # 1 second after
        
        similar_trades = await self.session.execute(
            select(Trade).where(
                and_(
                    Trade.symbol == trade_data.get('symbol'),
                    Trade.side == trade_data.get('side'),
                    Trade.amount == trade_data.get('amount'),
                    Trade.price == trade_data.get('price'),
                    Trade.exchange_id == trade_data.get('exchange_id'),
                    Trade.timestamp.between(
                        datetime.fromtimestamp(timestamp_start / 1000),
                        datetime.fromtimestamp(timestamp_end / 1000)
                    )
                )
            )
        )
        
        if similar_trades.scalar_one_or_none():
            return True, "Similar trade already exists in database (within 1 second)"
        
        # Not a duplicate - mark as processed
        self.processed_trade_ids.add(trade_data.get('exchange_trade_id'))
        self.processed_composite_keys.add(composite_key)
        
        return False, "Not a duplicate"
    
    async def insert_trade_with_deduplication(self, trade_data: Dict[str, Any]) -> Tuple[bool, str]:
        """Insert trade only if it's not a duplicate."""
        is_duplicate, reason = await self.is_duplicate_trade(trade_data)
        
        if is_duplicate:
            logger.debug(f"🚫 Skipping duplicate trade: {reason}")
            return False, reason
        
        # Insert the trade
        from database.models import Trade
        
        trade = Trade(
            exchange_trade_id=trade_data.get('exchange_trade_id'),
            exchange_id=trade_data.get('exchange_id'),
            symbol=trade_data.get('symbol'),
            side=trade_data.get('side'),
            amount=trade_data.get('amount'),
            price=trade_data.get('price'),
            cost=trade_data.get('cost'),
            fee_cost=trade_data.get('fee_cost'),
            fee_currency=trade_data.get('fee_currency'),
            timestamp=datetime.fromtimestamp(trade_data.get('timestamp', 0) / 1000),
            order_id=trade_data.get('order_id')
        )
        
        self.session.add(trade)
        await self.session.commit()
        
        logger.debug(f"✅ Inserted new trade: {trade_data.get('exchange_trade_id')}")
        return True, "Trade inserted successfully"


async def test_deduplication_system():
    """Test the deduplication system with three consecutive runs."""
    logger.info("🧪 Starting comprehensive deduplication test...")
    
    try:
        # Import required modules
        from database.repositories import TradeRepository
        from database import init_db, get_session
        from config.settings import load_config
        
        # Load config and initialize database
        config = load_config()
        db_config = config.get('database', {})
        db_url = db_config.get('url')
        await init_db(db_url)
        
        # Get database session
        async for session in get_session():
            trade_repository = TradeRepository(session)
            dedup_fixer = TradeDeduplicationFixer(trade_repository, session)
            break
        
        logger.info("✅ Database connection established")
        
        # Phase 1: Analyze current state
        logger.info("🔍 PHASE 1: Analyzing current duplicate situation...")
        initial_analysis = await dedup_fixer.analyze_current_duplicates()
        
        logger.info("📊 INITIAL STATE:")
        logger.info(f"   Total trades: {initial_analysis['total_trades']}")
        logger.info(f"   Duplicate trades: {initial_analysis['total_duplicate_trades']}")
        logger.info(f"   Duplicate percentage: {initial_analysis['duplicate_percentage']:.2f}%")
        
        # Phase 2: Remove existing duplicates
        logger.info("🧹 PHASE 2: Removing existing duplicates...")
        
        # First do a dry run
        dry_run_result = await dedup_fixer.remove_duplicates(dry_run=True)
        logger.info(f"📋 DRY RUN: Would remove {dry_run_result['removed']} duplicates, keep {dry_run_result['kept']} trades")
        
        # Actually remove duplicates
        removal_result = await dedup_fixer.remove_duplicates(dry_run=False)
        logger.info(f"✅ REMOVED: {removal_result['removed']} duplicates, kept {removal_result['kept']} trades")
        
        # Phase 3: Three consecutive test runs
        logger.info("🔄 PHASE 3: Testing with three consecutive runs...")
        
        test_results = []
        
        for run_number in range(1, 4):
            logger.info(f"🏃 TEST RUN #{run_number}")
            
            # Simulate trade sync (this would normally fetch from exchanges)
            await simulate_trade_sync(dedup_fixer, session, run_number)
            
            # Analyze state after this run
            post_run_analysis = await dedup_fixer.analyze_current_duplicates()
            
            test_results.append({
                "run_number": run_number,
                "total_trades": post_run_analysis['total_trades'],
                "duplicate_trades": post_run_analysis['total_duplicate_trades'],
                "duplicate_percentage": post_run_analysis['duplicate_percentage']
            })
            
            logger.info(f"📊 RUN #{run_number} RESULT:")
            logger.info(f"   Total trades: {post_run_analysis['total_trades']}")
            logger.info(f"   Duplicate trades: {post_run_analysis['total_duplicate_trades']}")
            logger.info(f"   Duplicate percentage: {post_run_analysis['duplicate_percentage']:.2f}%")
            
            if post_run_analysis['total_duplicate_trades'] > 0:
                logger.error(f"❌ RUN #{run_number} FAILED: {post_run_analysis['total_duplicate_trades']} duplicates found!")
                # Show details of duplicates
                for dup_type in ["cross_exchange_duplicates", "identical_duplicates", "same_tradeid_duplicates"]:
                    if dup_type in post_run_analysis and post_run_analysis[dup_type]:
                        logger.error(f"   {dup_type}: {len(post_run_analysis[dup_type])} groups")
            else:
                logger.info(f"✅ RUN #{run_number} PASSED: No duplicates detected!")
        
        # Phase 4: Final analysis
        logger.info("📋 PHASE 4: Final analysis...")
        
        final_analysis = await dedup_fixer.analyze_current_duplicates()
        
        # Generate test report
        test_report = {
            "test_timestamp": datetime.now().isoformat(),
            "initial_state": initial_analysis,
            "removal_result": removal_result,
            "test_runs": test_results,
            "final_state": final_analysis,
            "test_passed": final_analysis['total_duplicate_trades'] == 0,
            "improvement": {
                "trades_before": initial_analysis['total_trades'],
                "trades_after": final_analysis['total_trades'],
                "duplicates_before": initial_analysis['total_duplicate_trades'],
                "duplicates_after": final_analysis['total_duplicate_trades'],
                "percentage_improvement": initial_analysis['duplicate_percentage'] - final_analysis['duplicate_percentage']
            }
        }
        
        # Save report
        report_file = f"deduplication_test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump(test_report, f, indent=2)
        
        logger.info(f"📄 Test report saved to {report_file}")
        
        # Final verdict
        if test_report['test_passed']:
            logger.info("🎉 ✅ DEDUPLICATION TEST PASSED!")
            logger.info("   All three consecutive runs produced no duplicates")
            logger.info(f"   Improvement: {test_report['improvement']['duplicates_before']} → {test_report['improvement']['duplicates_after']} duplicates")
        else:
            logger.error("❌ DEDUPLICATION TEST FAILED!")
            logger.error(f"   Still have {final_analysis['total_duplicate_trades']} duplicates after fixes")
            logger.error("   Need to investigate further")
        
        return test_report
            
    except Exception as e:
        logger.error(f"❌ Error during deduplication test: {e}")
        raise


async def simulate_trade_sync(dedup_fixer: TradeDeduplicationFixer, session, run_number: int):
    """Simulate a trade sync operation to test deduplication."""
    logger.info(f"🔄 Simulating trade sync for run #{run_number}...")
    
    # Simulate some test trades (these would normally come from exchange APIs)
    test_trades = [
        {
            'exchange_trade_id': f'test_trade_{run_number}_1',
            'exchange_id': 3,
            'symbol': 'BERA/USDT',
            'side': 'buy',
            'amount': 10.0,
            'price': 2.45,
            'cost': 24.5,
            'fee_cost': 0.025,
            'fee_currency': 'USDT',
            'timestamp': int(datetime.now().timestamp() * 1000),
            'order_id': f'order_{run_number}_1'
        },
        {
            'exchange_trade_id': f'test_trade_{run_number}_2',
            'exchange_id': 4,
            'symbol': 'BERA/USDT',
            'side': 'sell',
            'amount': 5.0,
            'price': 2.46,
            'cost': 12.3,
            'fee_cost': 0.0123,
            'fee_currency': 'USDT',
            'timestamp': int(datetime.now().timestamp() * 1000) + 1000,
            'order_id': f'order_{run_number}_2'
        }
    ]
    
    # Try to insert each trade
    for trade_data in test_trades:
        success, reason = await dedup_fixer.insert_trade_with_deduplication(trade_data)
        if success:
            logger.info(f"✅ Inserted trade: {trade_data['exchange_trade_id']}")
        else:
            logger.info(f"🚫 Skipped trade: {trade_data['exchange_trade_id']} - {reason}")
    
    # Also try to insert a duplicate from a previous run (should be rejected)
    if run_number > 1:
        duplicate_trade = {
            'exchange_trade_id': f'test_trade_{run_number-1}_1',  # Same as previous run
            'exchange_id': 3,
            'symbol': 'BERA/USDT',
            'side': 'buy',
            'amount': 10.0,
            'price': 2.45,
            'cost': 24.5,
            'fee_cost': 0.025,
            'fee_currency': 'USDT',
            'timestamp': int(datetime.now().timestamp() * 1000),
            'order_id': f'order_{run_number-1}_1'
        }
        
        success, reason = await dedup_fixer.insert_trade_with_deduplication(duplicate_trade)
        if not success:
            logger.info(f"✅ Correctly rejected duplicate: {duplicate_trade['exchange_trade_id']} - {reason}")
        else:
            logger.error(f"❌ FAILED to detect duplicate: {duplicate_trade['exchange_trade_id']}")


async def main():
    """Main execution function."""
    logger.info("🚀 Starting comprehensive trade deduplication fix and test...")
    
    test_report = await test_deduplication_system()
    
    if test_report['test_passed']:
        logger.info("🎉 SUCCESS: Deduplication system is working correctly!")
    else:
        logger.error("❌ FAILURE: Deduplication system needs more work!")
    
    return test_report


if __name__ == "__main__":
    asyncio.run(main()) 