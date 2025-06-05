#!/usr/bin/env python3
"""
Cross-Exchange Duplicate Detection

This script specifically looks for the same trade appearing across multiple exchanges,
which the original analysis missed.

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
from collections import defaultdict, Counter
from sqlalchemy import select

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


async def main():
    """Main execution function."""
    logger.info("🕵️ Starting cross-exchange duplicate detection...")
    
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
        
        # Get database session and repositories
        async for session in get_session():
            trade_repository = TradeRepository(session)
            break
        
        logger.info("✅ Database connection established")
        
        # Get all trades
        all_trades = await get_all_trades(trade_repository)
        logger.info(f"📊 Found {len(all_trades)} total trades in database")
        
        # Find cross-exchange duplicates
        cross_exchange_duplicates = find_cross_exchange_duplicates(all_trades)
        
        # Find identical trade data duplicates (ignoring exchange)
        identical_trade_duplicates = find_identical_trade_duplicates(all_trades)
        
        # Show results
        logger.info(f"🚨 Found {len(cross_exchange_duplicates)} cross-exchange duplicate groups")
        logger.info(f"🚨 Found {len(identical_trade_duplicates)} identical trade duplicate groups")
        
        if cross_exchange_duplicates:
            logger.info("📋 CROSS-EXCHANGE DUPLICATES (same trade_id, different exchanges):")
            for i, group in enumerate(cross_exchange_duplicates):
                logger.info(f"  Group {i+1}: {len(group)} trades with same trade_id")
                for trade in group:
                    logger.info(f"    DB_ID:{trade['id']} Trade:{trade['trade_id']} "
                               f"Ex:{trade['exchange_id']} {trade['symbol']} "
                               f"{trade['side']} {trade['amount']} @ {trade['price']} "
                               f"Time:{trade['datetime']}")
        
        if identical_trade_duplicates:
            logger.info("📋 IDENTICAL TRADE DUPLICATES (same data, possibly different trade_id):")
            for i, group in enumerate(identical_trade_duplicates):
                logger.info(f"  Group {i+1}: {len(group)} identical trades")
                for trade in group:
                    logger.info(f"    DB_ID:{trade['id']} Trade:{trade['trade_id']} "
                               f"Ex:{trade['exchange_id']} {trade['symbol']} "
                               f"{trade['side']} {trade['amount']} @ {trade['price']} "
                               f"Time:{trade['datetime']}")
        
        # Generate detailed report
        await generate_cross_exchange_report(all_trades, cross_exchange_duplicates, identical_trade_duplicates)
        
        logger.info("🎉 Cross-exchange duplicate analysis completed!")
        
    except Exception as e:
        logger.error(f"❌ Error during analysis: {e}")
        raise


async def get_all_trades(trade_repository: Any) -> List[Dict[str, Any]]:
    """Get all trades from the database."""
    logger.info("📥 Fetching all trades from database...")
    
    try:
        from database.models import Trade
        
        # Get all trades
        stmt = select(Trade).order_by(Trade.timestamp.desc())
        result = await trade_repository.session.execute(stmt)
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
        
    except Exception as e:
        logger.error(f"Error fetching trades: {e}")
        return []


def find_cross_exchange_duplicates(trades: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
    """Find trades with the same trade_id appearing across different exchanges."""
    trade_groups = defaultdict(list)
    
    # Group trades by trade_id (ignoring exchange)
    for trade in trades:
        trade_id = trade['trade_id']
        trade_groups[trade_id].append(trade)
    
    # Find groups with trades from multiple exchanges
    cross_exchange_duplicates = []
    for trade_id, group in trade_groups.items():
        if len(group) > 1:
            # Check if trades are from different exchanges
            exchange_ids = set(trade['exchange_id'] for trade in group)
            if len(exchange_ids) > 1:
                cross_exchange_duplicates.append(group)
    
    return cross_exchange_duplicates


def find_identical_trade_duplicates(trades: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
    """Find trades that are identical in all key aspects (ignoring exchange and trade_id)."""
    trade_groups = defaultdict(list)
    
    # Group trades by key characteristics (ignoring exchange_id and trade_id)
    for trade in trades:
        # Create a key based on all important trade attributes
        key = (
            trade['symbol'],
            trade['side'],
            round(trade['amount'], 8),  # Round to avoid floating point issues
            round(trade['price'], 8),
            trade['timestamp'] // 1000 if trade['timestamp'] else 0  # Round to nearest second
        )
        trade_groups[key].append(trade)
    
    # Find groups with multiple trades
    identical_duplicates = []
    for key, group in trade_groups.items():
        if len(group) > 1:
            # Check if these are actually different trades (different DB IDs)
            db_ids = set(trade['id'] for trade in group)
            if len(db_ids) > 1:
                identical_duplicates.append(group)
    
    return identical_duplicates


async def generate_cross_exchange_report(trades: List[Dict[str, Any]], 
                                       cross_exchange_dupes: List[List[Dict[str, Any]]],
                                       identical_dupes: List[List[Dict[str, Any]]]):
    """Generate detailed cross-exchange duplicate report."""
    logger.info("📄 Generating cross-exchange duplicate report...")
    
    # Calculate impact
    total_duplicate_trades = 0
    for group in cross_exchange_dupes:
        total_duplicate_trades += len(group)
    for group in identical_dupes:
        total_duplicate_trades += len(group)
    
    # Create report
    report = {
        "timestamp": datetime.now().isoformat(),
        "total_trades_analyzed": len(trades),
        "cross_exchange_duplicates": {
            "groups_found": len(cross_exchange_dupes),
            "total_duplicate_trades": sum(len(group) for group in cross_exchange_dupes),
            "details": []
        },
        "identical_trade_duplicates": {
            "groups_found": len(identical_dupes),
            "total_duplicate_trades": sum(len(group) for group in identical_dupes),
            "details": []
        },
        "impact_analysis": {
            "total_potentially_duplicate_trades": total_duplicate_trades,
            "percentage_of_total": (total_duplicate_trades / len(trades)) * 100 if trades else 0,
            "recommended_actions": [
                "Investigate why same trade_id appears on multiple exchanges",
                "Review trade sync logic for cross-exchange contamination",
                "Implement stricter deduplication checking trade_id globally",
                "Consider which exchange should be the authoritative source for each trade"
            ]
        }
    }
    
    # Add detailed information for cross-exchange duplicates
    for group in cross_exchange_dupes:
        group_info = {
            "trade_id": group[0]['trade_id'],
            "duplicate_count": len(group),
            "exchanges_involved": list(set(trade['exchange_id'] for trade in group)),
            "trades": []
        }
        
        for trade in group:
            trade_info = {
                "database_id": trade['id'],
                "exchange_id": trade['exchange_id'],
                "symbol": trade['symbol'],
                "side": trade['side'],
                "amount": trade['amount'],
                "price": trade['price'],
                "timestamp": trade['timestamp'],
                "datetime": trade['datetime']
            }
            group_info["trades"].append(trade_info)
        
        report["cross_exchange_duplicates"]["details"].append(group_info)
    
    # Add detailed information for identical duplicates
    for group in identical_dupes:
        group_info = {
            "duplicate_count": len(group),
            "key_characteristics": {
                "symbol": group[0]['symbol'],
                "side": group[0]['side'],
                "amount": group[0]['amount'],
                "price": group[0]['price'],
                "timestamp": group[0]['timestamp']
            },
            "trades": []
        }
        
        for trade in group:
            trade_info = {
                "database_id": trade['id'],
                "trade_id": trade['trade_id'],
                "exchange_id": trade['exchange_id'],
                "datetime": trade['datetime']
            }
            group_info["trades"].append(trade_info)
        
        report["identical_trade_duplicates"]["details"].append(group_info)
    
    # Save report
    report_file = f"cross_exchange_duplicate_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)
    
    logger.info(f"📄 Cross-exchange duplicate report saved to {report_file}")
    
    # Print summary
    logger.info("📊 CROSS-EXCHANGE DUPLICATE SUMMARY:")
    logger.info(f"  Cross-exchange duplicates: {len(cross_exchange_dupes)} groups, {sum(len(g) for g in cross_exchange_dupes)} trades")
    logger.info(f"  Identical trade duplicates: {len(identical_dupes)} groups, {sum(len(g) for g in identical_dupes)} trades")
    logger.info(f"  Total duplicate trades: {total_duplicate_trades}")
    logger.info(f"  Percentage of database: {(total_duplicate_trades / len(trades)) * 100:.2f}%")
    
    if cross_exchange_dupes or identical_dupes:
        logger.warning("🚨 DEDUPLICATION SYSTEM IS FAILING!")
        logger.warning("   Same trades are being stored multiple times")
        logger.warning("   This could lead to incorrect position calculations")
        logger.warning("   Immediate action required to fix the sync logic")
    
    return report


if __name__ == "__main__":
    asyncio.run(main()) 