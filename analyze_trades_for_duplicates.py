#!/usr/bin/env python3
"""
Comprehensive Trade Duplicate Analysis

This script will examine all trades in the database and detect potential duplicates
using multiple criteria to help debug the deduplication system.

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
import pandas as pd
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
    logger.info("🔍 Starting comprehensive trade duplicate analysis...")
    
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
        
        # 1. Get all trades from database
        all_trades = await get_all_trades(trade_repository)
        logger.info(f"📊 Found {len(all_trades)} total trades in database")
        
        # 2. Analyze trade distribution
        await analyze_trade_distribution(all_trades)
        
        # 3. Detect duplicates using multiple criteria
        await detect_duplicates_comprehensive(all_trades)
        
        # 4. Analyze recent trades in detail
        await analyze_recent_trades(all_trades)
        
        # 5. Check for specific BERA/USDT duplicates
        await analyze_bera_trades(all_trades)
        
        # 6. Generate duplicate analysis report
        await generate_duplicate_report(all_trades)
        
        logger.info("🎉 Trade duplicate analysis completed!")
        
    except Exception as e:
        logger.error(f"❌ Error during analysis: {e}")
        raise


async def get_all_trades(trade_repository: Any) -> List[Dict[str, Any]]:
    """Get all trades from the database."""
    logger.info("📥 Fetching all trades from database...")
    
    try:
        # Use direct SQLAlchemy query since there's no get_all method
        from database.models import Trade
        
        # Get all trades with a limit to prevent overwhelming memory
        stmt = select(Trade).order_by(Trade.timestamp.desc()).limit(10000)
        result = await trade_repository.session.execute(stmt)
        trades = result.scalars().all()
        
        trade_list = []
        for trade in trades:
            trade_dict = {
                'id': trade.id,
                'trade_id': trade.exchange_trade_id,  # Fixed: use exchange_trade_id
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


async def analyze_trade_distribution(trades: List[Dict[str, Any]]):
    """Analyze the distribution of trades across exchanges and symbols."""
    logger.info("📈 Analyzing trade distribution...")
    
    # Count by exchange
    exchange_counts = Counter(trade['exchange_id'] for trade in trades)
    
    # Count by symbol
    symbol_counts = Counter(trade['symbol'] for trade in trades)
    
    # Count by date
    date_counts = Counter()
    for trade in trades:
        if trade['datetime']:
            date = trade['datetime'][:10]  # Extract date part
            date_counts[date] += 1
    
    logger.info("📊 TRADE DISTRIBUTION:")
    logger.info(f"  By Exchange: {dict(exchange_counts.most_common())}")
    logger.info(f"  By Symbol: {dict(symbol_counts.most_common())}")
    logger.info(f"  By Date (recent): {dict(date_counts.most_common(7))}")
    
    # Check for unusual patterns
    total_trades = len(trades)
    for exchange_id, count in exchange_counts.items():
        percentage = (count / total_trades) * 100
        if percentage > 50:
            logger.warning(f"⚠️  Exchange {exchange_id} has {percentage:.1f}% of all trades - potential duplication source")


async def detect_duplicates_comprehensive(trades: List[Dict[str, Any]]):
    """Detect duplicates using multiple criteria."""
    logger.info("🔍 Detecting duplicates using comprehensive criteria...")
    
    duplicates_found = {}
    
    # 1. Exact duplicates (same trade_id + exchange_id)
    exact_duplicates = find_exact_duplicates(trades)
    if exact_duplicates:
        duplicates_found['exact'] = exact_duplicates
        logger.warning(f"🚨 Found {len(exact_duplicates)} exact duplicate groups")
    
    # 2. Near-time duplicates (same exchange, symbol, amount, price within 1 second)
    time_duplicates = find_near_time_duplicates(trades)
    if time_duplicates:
        duplicates_found['near_time'] = time_duplicates
        logger.warning(f"🚨 Found {len(time_duplicates)} near-time duplicate groups")
    
    # 3. Same order duplicates (same order_id but different trade_id)
    order_duplicates = find_order_duplicates(trades)
    if order_duplicates:
        duplicates_found['same_order'] = order_duplicates
        logger.warning(f"🚨 Found {len(order_duplicates)} same-order duplicate groups")
    
    # 4. Composite duplicates (matching multiple fields)
    composite_duplicates = find_composite_duplicates(trades)
    if composite_duplicates:
        duplicates_found['composite'] = composite_duplicates
        logger.warning(f"🚨 Found {len(composite_duplicates)} composite duplicate groups")
    
    if not duplicates_found:
        logger.info("✅ No obvious duplicates found using standard criteria")
    else:
        # Show details of first few duplicates
        for duplicate_type, groups in duplicates_found.items():
            logger.info(f"📋 {duplicate_type.upper()} DUPLICATES:")
            for i, group in enumerate(groups[:3]):  # Show first 3 groups
                logger.info(f"  Group {i+1}: {len(group)} trades")
                for trade in group[:2]:  # Show first 2 trades in group
                    logger.info(f"    ID:{trade['id']} Trade:{trade['trade_id']} "
                               f"Ex:{trade['exchange_id']} {trade['symbol']} "
                               f"{trade['side']} {trade['amount']} @ {trade['price']} "
                               f"Time:{trade['datetime']}")
    
    return duplicates_found


def find_exact_duplicates(trades: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
    """Find trades with exact same trade_id and exchange_id."""
    trade_groups = defaultdict(list)
    
    for trade in trades:
        key = (trade['trade_id'], trade['exchange_id'])
        trade_groups[key].append(trade)
    
    return [group for group in trade_groups.values() if len(group) > 1]


def find_near_time_duplicates(trades: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
    """Find trades that are very similar and close in time."""
    duplicate_groups = []
    
    # Sort trades by timestamp
    sorted_trades = sorted(trades, key=lambda x: x['timestamp'] if x['timestamp'] else 0)
    
    for i, trade1 in enumerate(sorted_trades):
        similar_trades = [trade1]
        
        # Look for similar trades within 1 second
        for j in range(i + 1, len(sorted_trades)):
            trade2 = sorted_trades[j]
            
            # If timestamp difference > 1 second, stop looking
            if abs((trade2['timestamp'] or 0) - (trade1['timestamp'] or 0)) > 1000:
                break
            
            # Check if trades are similar
            if (trade1['exchange_id'] == trade2['exchange_id'] and
                trade1['symbol'] == trade2['symbol'] and
                trade1['side'] == trade2['side'] and
                abs(trade1['amount'] - trade2['amount']) < 0.0001 and
                abs(trade1['price'] - trade2['price']) < 0.0001):
                similar_trades.append(trade2)
        
        if len(similar_trades) > 1:
            duplicate_groups.append(similar_trades)
    
    # Remove overlapping groups
    unique_groups = []
    used_ids = set()
    
    for group in duplicate_groups:
        group_ids = {trade['id'] for trade in group}
        if not group_ids.intersection(used_ids):
            unique_groups.append(group)
            used_ids.update(group_ids)
    
    return unique_groups


def find_order_duplicates(trades: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
    """Find trades with same order_id but different trade_id."""
    order_groups = defaultdict(list)
    
    for trade in trades:
        if trade['order_id']:
            key = (trade['order_id'], trade['exchange_id'])
            order_groups[key].append(trade)
    
    duplicate_groups = []
    for group in order_groups.values():
        if len(group) > 1:
            # Check if they have different trade_ids
            trade_ids = {trade['trade_id'] for trade in group}
            if len(trade_ids) > 1:
                duplicate_groups.append(group)
    
    return duplicate_groups


def find_composite_duplicates(trades: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
    """Find duplicates using composite key matching."""
    composite_groups = defaultdict(list)
    
    for trade in trades:
        # Create composite key
        key = (
            trade['exchange_id'],
            trade['symbol'],
            trade['side'],
            round(trade['amount'], 8),  # Round to avoid floating point issues
            round(trade['price'], 8),
            trade['timestamp'] // 1000 if trade['timestamp'] else 0  # Round to nearest second
        )
        composite_groups[key].append(trade)
    
    return [group for group in composite_groups.values() if len(group) > 1]


async def analyze_recent_trades(trades: List[Dict[str, Any]]):
    """Analyze recent trades in detail."""
    logger.info("🕐 Analyzing recent trades...")
    
    # Get trades from last 24 hours
    now = datetime.now()
    yesterday = now - timedelta(days=1)
    yesterday_timestamp = int(yesterday.timestamp() * 1000)
    
    recent_trades = [
        trade for trade in trades 
        if trade['timestamp'] and trade['timestamp'] > yesterday_timestamp
    ]
    
    logger.info(f"📅 Found {len(recent_trades)} trades in last 24 hours")
    
    if recent_trades:
        # Show recent trades by time
        recent_sorted = sorted(recent_trades, key=lambda x: x['timestamp'], reverse=True)
        
        logger.info("🕒 MOST RECENT TRADES:")
        for i, trade in enumerate(recent_sorted[:10]):
            timestamp_dt = datetime.fromtimestamp(trade['timestamp'] / 1000)
            logger.info(f"  {i+1}. ID:{trade['id']} Trade:{trade['trade_id']} "
                       f"Ex:{trade['exchange_id']} {trade['symbol']} "
                       f"{trade['side']} {trade['amount']} @ {trade['price']} "
                       f"Time:{timestamp_dt.strftime('%H:%M:%S')}")


async def analyze_bera_trades(trades: List[Dict[str, Any]]):
    """Analyze BERA-related trades specifically."""
    logger.info("🐻 Analyzing BERA trades specifically...")
    
    bera_trades = [
        trade for trade in trades 
        if 'BERA' in trade['symbol'].upper()
    ]
    
    logger.info(f"🎯 Found {len(bera_trades)} BERA-related trades")
    
    if bera_trades:
        # Group by symbol variant
        symbol_groups = defaultdict(list)
        for trade in bera_trades:
            symbol_groups[trade['symbol']].append(trade)
        
        logger.info("📊 BERA TRADES BY SYMBOL:")
        for symbol, symbol_trades in symbol_groups.items():
            logger.info(f"  {symbol}: {len(symbol_trades)} trades")
            
            # Check for duplicates within this symbol
            if len(symbol_trades) > 1:
                # Sort by timestamp
                symbol_trades_sorted = sorted(symbol_trades, key=lambda x: x['timestamp'] or 0)
                
                logger.info(f"    Recent {symbol} trades:")
                for trade in symbol_trades_sorted[-5:]:  # Last 5 trades
                    timestamp_dt = datetime.fromtimestamp(trade['timestamp'] / 1000) if trade['timestamp'] else None
                    logger.info(f"      ID:{trade['id']} Trade:{trade['trade_id']} "
                               f"Ex:{trade['exchange_id']} {trade['side']} "
                               f"{trade['amount']} @ {trade['price']} "
                               f"Time:{timestamp_dt.strftime('%m-%d %H:%M:%S') if timestamp_dt else 'None'}")


async def generate_duplicate_report(trades: List[Dict[str, Any]]):
    """Generate comprehensive duplicate analysis report."""
    logger.info("📄 Generating duplicate analysis report...")
    
    # Run all duplicate detection methods
    duplicates = await detect_duplicates_comprehensive(trades)
    
    # Create detailed report
    report = {
        "timestamp": datetime.now().isoformat(),
        "total_trades_analyzed": len(trades),
        "analysis_summary": {
            "exact_duplicates": len(duplicates.get('exact', [])),
            "near_time_duplicates": len(duplicates.get('near_time', [])),
            "same_order_duplicates": len(duplicates.get('same_order', [])),
            "composite_duplicates": len(duplicates.get('composite', []))
        },
        "detailed_duplicates": {}
    }
    
    # Add detailed duplicate information
    for duplicate_type, groups in duplicates.items():
        report["detailed_duplicates"][duplicate_type] = []
        
        for group in groups:
            group_info = {
                "trade_count": len(group),
                "trades": []
            }
            
            for trade in group:
                trade_info = {
                    "database_id": trade['id'],
                    "trade_id": trade['trade_id'],
                    "exchange_id": trade['exchange_id'],
                    "symbol": trade['symbol'],
                    "side": trade['side'],
                    "amount": trade['amount'],
                    "price": trade['price'],
                    "timestamp": trade['timestamp'],
                    "datetime": trade['datetime'],
                    "order_id": trade['order_id']
                }
                group_info["trades"].append(trade_info)
            
            report["detailed_duplicates"][duplicate_type].append(group_info)
    
    # Save report
    report_file = f"trade_duplicate_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)
    
    logger.info(f"📄 Duplicate analysis report saved to {report_file}")
    
    # Print summary
    logger.info("📊 DUPLICATE ANALYSIS SUMMARY:")
    total_duplicate_trades = 0
    for duplicate_type, count in report["analysis_summary"].items():
        logger.info(f"  {duplicate_type}: {count} groups")
        if duplicate_type in duplicates:
            total_trades_in_groups = sum(len(group) for group in duplicates[duplicate_type])
            total_duplicate_trades += total_trades_in_groups
            logger.info(f"    Total trades involved: {total_trades_in_groups}")
    
    logger.info(f"  📊 Total trades that may be duplicates: {total_duplicate_trades}")
    if len(trades) > 0:
        logger.info(f"  📊 Duplicate percentage: {(total_duplicate_trades / len(trades)) * 100:.2f}%")
    else:
        logger.info("  📊 Duplicate percentage: N/A (no trades found)")
    
    return report


if __name__ == "__main__":
    asyncio.run(main()) 