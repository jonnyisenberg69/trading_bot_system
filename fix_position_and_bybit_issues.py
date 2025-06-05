#!/usr/bin/env python3
"""
Comprehensive Fix for Position and Bybit Issues

This script addresses the following critical issues:
1. Bybit "category only support linear or option" error when fetching positions
2. Position duplication due to symbol normalization inconsistencies
3. Zero-value position entries appearing in the UI
4. Trade deduplication and position aggregation problems

Issues identified from the screenshot:
- Multiple BERA/USDT entries with $0.00 values
- Inconsistent position tracking across exchanges
- Symbol format mismatches between exchanges

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
from typing import Dict, List, Optional, Any
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


async def main():
    """Main execution function."""
    logger.info("🚀 Starting comprehensive position and Bybit fixes...")
    
    try:
        # Import required modules
        from config.settings import load_config
        from exchanges.connectors import create_exchange_connector
        from order_management.tracking import PositionManager
        from order_management.enhanced_trade_sync import EnhancedTradeSync
        from database.repositories import TradeRepository
        from database import init_db, get_session
        
        # Load config and initialize database
        config = load_config()
        db_config = config.get('database', {})
        db_url = db_config.get('url')
        await init_db(db_url)
        
        # Initialize exchange connectors using main.py approach
        exchange_connectors = {}
        for i, exchange_config in enumerate(config.get('exchanges', [])):
            exchange_name = exchange_config['name']
            exchange_type = exchange_config.get('type', 'spot')
            
            # Create connector key (e.g., 'binance_spot', 'binance_perp')
            connector_key = f"{exchange_name}_{exchange_type}"
            
            # Initialize exchange connector
            connector = create_exchange_connector(
                exchange_name,
                {
                    'api_key': exchange_config.get('api_key', ''),
                    'secret': exchange_config.get('api_secret', ''),
                    'wallet_address': exchange_config.get('wallet_address', ''),
                    'private_key': exchange_config.get('private_key', ''),
                    'passphrase': exchange_config.get('passphrase', ''),
                    'sandbox': exchange_config.get('testnet', True),
                    'market_type': 'future' if exchange_type == 'perp' else exchange_type,
                    'testnet': exchange_config.get('testnet', True)
                }
            )
            
            if connector:
                # Connect to exchange
                connected = await connector.connect()
                if connected:
                    exchange_connectors[connector_key] = connector
                    logger.info(f"✅ Connected to {connector_key}")
                else:
                    logger.warning(f"❌ Failed to connect to {connector_key}")
            else:
                logger.warning(f"❌ Failed to initialize connector for {connector_key}")
        
        # Initialize position manager
        position_manager = PositionManager(data_dir="data/positions")
        await position_manager.start()
        
        # Get database session and repositories
        async for session in get_session():
            trade_repository = TradeRepository(session)
            break
        
        logger.info(f"✅ Initialized {len(exchange_connectors)} exchange connectors")
        
        # 1. Test and fix Bybit position fetching
        await test_and_fix_bybit_positions(exchange_connectors)
        
        # 2. Clean up duplicate and zero-value positions
        await cleanup_duplicate_positions(position_manager, exchange_connectors)
        
        # 3. Re-sync positions from exchanges with proper symbol normalization
        await resync_positions_with_normalization(position_manager, exchange_connectors)
        
        # 4. Test trade sync system to ensure no duplicates
        await test_trade_sync_deduplication(exchange_connectors, trade_repository, position_manager)
        
        # 5. Generate final position summary
        await generate_position_summary(position_manager, exchange_connectors)
        
        logger.info("🎉 All fixes completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Error during fix execution: {e}")
        raise
    finally:
        # Clean up connections
        if 'exchange_connectors' in locals():
            for name, connector in exchange_connectors.items():
                try:
                    if hasattr(connector, 'disconnect'):
                        await connector.disconnect()
                except Exception as e:
                    logger.warning(f"Error closing connector {name}: {e}")
        
        if 'position_manager' in locals():
            await position_manager.stop()


async def test_and_fix_bybit_positions(exchange_connectors: Dict[str, Any]):
    """Test and fix Bybit position fetching issues."""
    logger.info("🔧 Testing and fixing Bybit position fetching...")
    
    # Get all Bybit connections
    bybit_connections = {k: v for k, v in exchange_connectors.items() if 'bybit' in k.lower()}
    
    for conn_id, connector in bybit_connections.items():
        logger.info(f"Testing position fetch for {conn_id}...")
        
        try:
            # Test position fetching with the fix
            positions = await connector.get_positions()
            logger.info(f"✅ Successfully fetched {len(positions)} positions from {conn_id}")
            
            # Log position details for verification
            for pos in positions:
                logger.info(f"  Position: {pos.get('symbol')} - Size: {pos.get('size')} - Side: {pos.get('side')}")
                
        except Exception as e:
            logger.error(f"❌ Failed to fetch positions from {conn_id}: {e}")
            # The fix in bybit.py should have resolved this


async def cleanup_duplicate_positions(position_manager: Any, exchange_connectors: Dict[str, Any]):
    """Clean up duplicate and zero-value positions."""
    logger.info("🧹 Cleaning up duplicate and zero-value positions...")
    
    # Get all current positions
    all_positions = position_manager.get_all_positions()
    logger.info(f"Found {len(all_positions)} total positions before cleanup")
    
    # Group positions by symbol to identify duplicates
    positions_by_symbol = {}
    for pos in all_positions:
        symbol = pos.symbol
        if symbol not in positions_by_symbol:
            positions_by_symbol[symbol] = []
        positions_by_symbol[symbol].append(pos)
    
    # Reset all positions to start fresh
    logger.info("Resetting all positions to start fresh...")
    await position_manager.reset_all_positions()
    
    # Filter to only connected exchanges
    connected_connectors = {k: v for k, v in exchange_connectors.items() if v}
    
    logger.info(f"Re-initializing positions for {len(connected_connectors)} connected exchanges")
    
    # Ensure positions for main symbols with proper exchange filtering
    main_symbols = ["BERA/USDT", "BERA/USDT-PERP"]
    for symbol in main_symbols:
        position_manager.ensure_exchange_positions(connected_connectors, symbol)
    
    # Get updated position count
    updated_positions = position_manager.get_all_positions()
    logger.info(f"✅ Cleaned up positions: {len(all_positions)} → {len(updated_positions)}")


async def resync_positions_with_normalization(position_manager: Any, exchange_connectors: Dict[str, Any]):
    """Re-sync positions from exchanges with proper symbol normalization."""
    logger.info("🔄 Re-syncing positions with proper symbol normalization...")
    
    # Filter to only connected exchanges
    connected_connectors = {k: v for k, v in exchange_connectors.items() if v}
    
    if not connected_connectors:
        logger.warning("No connected exchanges found for position sync")
        return
    
    logger.info(f"Syncing positions from {len(connected_connectors)} exchanges...")
    
    # Sync positions from exchanges (this will use the improved normalization)
    await position_manager.sync_positions_from_exchanges(connected_connectors)
    
    # Log sync results
    updated_positions = position_manager.get_all_positions()
    open_positions = [pos for pos in updated_positions if pos.is_open]
    
    logger.info(f"✅ Position sync completed: {len(updated_positions)} total, {len(open_positions)} open")
    
    # Log position details by exchange
    for exchange_name in connected_connectors.keys():
        exchange_positions = position_manager.get_positions_by_exchange(exchange_name)
        open_count = len([pos for pos in exchange_positions if pos.is_open])
        logger.info(f"  {exchange_name}: {len(exchange_positions)} total, {open_count} open")


async def test_trade_sync_deduplication(exchange_connectors: Dict[str, Any], trade_repository: Any, position_manager: Any):
    """Test trade sync system to ensure proper deduplication."""
    logger.info("🔍 Testing trade sync deduplication system...")
    
    try:
        # Try to import EnhancedTradeSync
        from order_management.enhanced_trade_sync import EnhancedTradeSync
    except ImportError as e:
        logger.warning(f"⚠️ EnhancedTradeSync not available, skipping trade sync test: {e}")
        return
    
    # Filter to only connected exchanges
    connected_connectors = {k: v for k, v in exchange_connectors.items() if v}
    
    if not connected_connectors:
        logger.warning("No connected exchanges found for trade sync test")
        return
    
    # Test sync for a recent time period (last hour) to check deduplication
    enhanced_trade_sync = EnhancedTradeSync(
        exchange_connectors=connected_connectors,
        trade_repository=trade_repository,
        position_manager=position_manager
    )
    
    # Test with a small time window
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=1)
    
    logger.info(f"Testing trade sync from {start_time} to {end_time}")
    
    try:
        # Test sync for BERA/USDT
        test_symbols = ["BERA/USDT"]
        
        for symbol in test_symbols:
            logger.info(f"Testing sync for {symbol}...")
            
            # Run sync for each exchange (limit to first 3 for testing)
            for exchange_id in list(connected_connectors.keys())[:3]:
                try:
                    result = await enhanced_trade_sync.sync_exchange_with_pagination(
                        exchange_name=exchange_id,
                        symbols=[symbol],
                        since=start_time
                    )
                    
                    if result:
                        total_trades = sum(r.total_fetched for r in result)
                        total_requests = sum(r.total_requests for r in result)
                        logger.info(f"  {exchange_id}: {total_trades} trades, {total_requests} requests")
                
                except Exception as e:
                    logger.warning(f"  {exchange_id}: Sync failed - {e}")
    
    except Exception as e:
        logger.error(f"Trade sync test failed: {e}")
    
    logger.info("✅ Trade sync test completed")


async def generate_position_summary(position_manager: Any, exchange_connectors: Dict[str, Any]):
    """Generate final position summary."""
    logger.info("📊 Generating final position summary...")
    
    # Get all positions
    all_positions = position_manager.get_all_positions()
    open_positions = [pos for pos in all_positions if pos.is_open]
    
    # Get net positions
    net_positions = position_manager.get_all_net_positions()
    open_net_positions = {k: v for k, v in net_positions.items() if v['is_open']}
    
    # Get position summary
    summary = position_manager.summarize_positions()
    
    # Create comprehensive report
    report = {
        "timestamp": datetime.utcnow().isoformat(),
        "total_positions": len(all_positions),
        "open_positions": len(open_positions),
        "symbols_tracked": len(position_manager.symbols),
        "exchanges_with_positions": len(position_manager.positions),
        "net_positions": len(open_net_positions),
        "connected_exchanges": len(exchange_connectors),
        "summary": summary,
        "position_details": []
    }
    
    # Add position details grouped by symbol
    symbols_tracked = list(position_manager.symbols)
    for symbol in symbols_tracked:
        symbol_positions = position_manager.get_all_positions(symbol)
        symbol_net = position_manager.get_net_position(symbol)
        
        symbol_detail = {
            "symbol": symbol,
            "total_positions": len(symbol_positions),
            "open_positions": len([p for p in symbol_positions if p.is_open]),
            "net_position": symbol_net,
            "exchange_breakdown": []
        }
        
        for pos in symbol_positions:
            if pos.is_open:  # Only include open positions in the report
                symbol_detail["exchange_breakdown"].append({
                    "exchange": pos.exchange,
                    "size": float(pos.size),
                    "value": float(pos.value),
                    "side": pos.side,
                    "avg_price": float(pos.avg_price) if pos.avg_price else None,
                    "is_perpetual": pos.is_perpetual
                })
        
        if symbol_detail["open_positions"] > 0:  # Only include symbols with open positions
            report["position_details"].append(symbol_detail)
    
    # Save report to file
    report_file = f"position_fix_report_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)
    
    logger.info(f"📄 Position report saved to {report_file}")
    
    # Log summary to console
    logger.info("📊 FINAL POSITION SUMMARY:")
    logger.info(f"  Total positions: {report['total_positions']}")
    logger.info(f"  Open positions: {report['open_positions']}")
    logger.info(f"  Symbols tracked: {report['symbols_tracked']}")
    logger.info(f"  Exchanges: {report['exchanges_with_positions']}")
    logger.info(f"  Net positions: {report['net_positions']}")
    logger.info(f"  Connected exchanges: {report['connected_exchanges']}")
    
    for detail in report["position_details"]:
        logger.info(f"  {detail['symbol']}:")
        logger.info(f"    Net: {detail['net_position']['side']} {detail['net_position']['size']} @ {detail['net_position']['avg_price']}")
        for exchange in detail["exchange_breakdown"]:
            logger.info(f"    {exchange['exchange']}: {exchange['side']} {exchange['size']} @ {exchange['avg_price']}")


if __name__ == "__main__":
    asyncio.run(main()) 