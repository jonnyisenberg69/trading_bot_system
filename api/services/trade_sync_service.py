"""
Trade Synchronization Service.

This service centralizes the logic for fetching trades from exchanges
and updating the database. It is designed to be called by other
services, such as the WebSocket manager or a dedicated API endpoint.
"""

import time
from typing import Dict, Any, List
from datetime import datetime, timezone
import structlog
import asyncio

from api.services.bot_manager import BotManager
from api.services.exchange_manager import ExchangeManager
from api.services.risk_table_processor import RiskTableProcessor
from api.services.risk_data_service import RiskDataService
from order_management.tracking import PositionManager
from utils.trade_sync_logger import setup_trade_sync_logger, log_sync_summary
from utils.symbol_utils import convert_symbol_for_exchange
from order_management.enhanced_trade_sync import EnhancedTradeSync
from database.repositories import TradeRepository
from database import get_session, init_db

logger = structlog.get_logger(__name__)


async def trigger_trade_sync(
    account_name: str,
    start_time: str,
    end_time: str,
    bot_manager: BotManager,
    exchange_manager: ExchangeManager,
    position_manager: PositionManager
):
    """
    Triggers trade synchronization for a given account and time range.

    This function is a reusable component that encapsulates the logic
    originally found in the /sync-trades API endpoint.
    """
    trade_sync_logger = setup_trade_sync_logger()
    
    try:
        # Get all bot instances to determine symbols
        bot_instances = bot_manager.get_all_instances()
        symbols = []
        for bot in bot_instances:
            if bot.symbol and bot.symbol not in symbols:
                symbols.append(bot.symbol)
            if hasattr(bot, 'config') and isinstance(bot.config, dict):
                exchange_symbols = bot.config.get('exchange_symbols', {})
                for _, symbol in exchange_symbols.items():
                    if symbol and symbol not in symbols:
                        symbols.append(symbol)
        
        if not symbols:
            symbols = ["BERA/USDT"]

        # Extract base coins for filtering
        base_coins = list(set([s.split('/')[0] for s in symbols if '/' in s]))
        
        # Get connected exchanges
        all_connections = exchange_manager.get_all_connections()
        active_connections = [
            conn for conn in all_connections 
            if conn.has_credentials and conn.connector and conn.status.value == "connected"
        ]

        if not active_connections:
            logger.warning(f"No active connections found for account {account_name}")
            return

        # Get database session and trade repository
        async for session in get_session():
            trade_repository = TradeRepository(session)
            break
        
        position_manager.set_trade_repository(trade_repository)
        
        sync_connectors = {conn.connection_id: conn.connector for conn in active_connections}
        
        enhanced_trade_sync = EnhancedTradeSync(
            exchange_connectors=sync_connectors,
            trade_repository=trade_repository,
            position_manager=position_manager
        )
        
        all_exchanges = [conn.connection_id for conn in active_connections]
        start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
        end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
        
        logger.info(f"Starting trade sync for account: {account_name}", start=start_dt, end=end_dt, symbols=symbols)
        
        sync_start_time = time.time()
        successful_syncs, failed_syncs, total_trades_synced = 0, 0, 0
        
        # Collect exchange IDs for filtering
        exchange_ids = []
        async for session in get_session():
            from database.models import Exchange
            from sqlalchemy import select
            
            for conn in active_connections:
                result = await session.execute(
                    select(Exchange.id).where(Exchange.name == conn.connection_id)
                )
                exchange_id = result.scalar_one_or_none()
                if exchange_id:
                    exchange_ids.append(exchange_id)
            break
        
        for exchange_id in all_exchanges:
            try:
                exchange_symbols = [convert_symbol_for_exchange(s, exchange_id) for s in symbols]
                
                pagination_results = await enhanced_trade_sync.sync_exchange_with_pagination(
                    exchange_name=exchange_id,
                    symbols=exchange_symbols,
                    since=start_dt,
                    until=end_dt,
                    base_coins=base_coins
                )
                
                total_trades = sum(r.total_fetched for r in pagination_results)
                successful_syncs += 1
                total_trades_synced += total_trades
                logger.info(f"✅ {exchange_id}: {total_trades} trades synced")
                
            except Exception as e:
                logger.error(f"Failed to sync {exchange_id}: {e}", exc_info=True)
                failed_syncs += 1

        total_sync_time_ms = int((time.time() - sync_start_time) * 1000)
        log_sync_summary(
            trade_sync_logger,
            total_exchanges=len(all_exchanges),
            successful=successful_syncs,
            failed=failed_syncs,
            total_trades=total_trades_synced,
            total_time_ms=total_sync_time_ms
        )
        
        # Process synced trades into risk monitoring tables
        logger.info("Processing trades into risk monitoring tables...")
        try:
            # Get session maker for RiskTableProcessor
            session_maker = await init_db()
            
            # Initialize risk table processor
            risk_processor = RiskTableProcessor(session_maker)
            
            # Process trades with the same time range
            # Use a dedicated session for processing
            async with session_maker() as processing_session:
                try:
                    # First, clean up any existing small positions
                    await _cleanup_small_positions(session_maker, exchange_ids)
                    
                    # Process trades into risk tables
                    processing_result = await risk_processor.process_trades_to_risk_tables(
                        start_time=start_dt.replace(tzinfo=None),  # Ensure timezone-naive
                        end_time=end_dt.replace(tzinfo=None),      # Ensure timezone-naive
                        exchange_ids=exchange_ids
                    )
                    
                    logger.info(
                        "Risk table processing complete",
                        trades_processed=processing_result['trades_processed'],
                        positions_updated=processing_result['positions_updated'],
                        pnl_events_created=processing_result['pnl_events_created'],
                        final_positions=processing_result['final_positions']
                    )
                    
                    # Consolidate positions after processing
                    await _consolidate_positions(session_maker, start_dt, end_dt, exchange_ids)
                    
                except Exception as e:
                    logger.error(f"Failed to process trades in session: {e}", exc_info=True)
            
        except Exception as e:
            logger.error(f"Failed to process trades into risk tables: {e}", exc_info=True)
            # Don't raise - at least trades are synced
        
        try:
            all_normalized_symbols = set()
            for symbol in symbols:
                base = symbol.split('/')[0] if '/' in symbol else symbol
                all_normalized_symbols.add(f"{base}/USDT")  # Spot
                all_normalized_symbols.add(f"{base}/USDT-PERP")  # Perp
            
            for symbol in all_normalized_symbols:
                position_manager.ensure_exchange_positions(sync_connectors, symbol)
            
            await position_manager.sync_positions_from_exchanges(sync_connectors)
            
        except Exception as e:
            logger.warning(f"Failed to ensure/sync exchange positions: {e}")
        
        logger.info(f"Sync completed for {account_name}")
        
    except Exception as e:
        logger.error(f"Error in trade sync service: {e}", exc_info=True)


async def _cleanup_small_positions(session_maker, exchange_ids: List[int] = None):
    """Clean up very small positions that are likely dust."""
    try:
        from database.models import InventoryPriceHistory
        from sqlalchemy import delete, and_, func, select
        
        async with session_maker() as session:
            # Find the latest timestamp for each exchange_id and symbol
            subquery = (
                select(
                    InventoryPriceHistory.exchange_id,
                    InventoryPriceHistory.symbol,
                    func.max(InventoryPriceHistory.timestamp).label("max_timestamp")
                )
                .group_by(
                    InventoryPriceHistory.exchange_id,
                    InventoryPriceHistory.symbol
                )
            )
            
            if exchange_ids:
                subquery = subquery.filter(InventoryPriceHistory.exchange_id.in_(exchange_ids))
            
            subquery = subquery.subquery()
            
            # Find entries with very small sizes using SQLAlchemy 2.0 syntax
            query = (
                select(InventoryPriceHistory)
                .join(
                    subquery,
                    and_(
                        InventoryPriceHistory.exchange_id == subquery.c.exchange_id,
                        InventoryPriceHistory.symbol == subquery.c.symbol,
                        InventoryPriceHistory.timestamp == subquery.c.max_timestamp
                    )
                )
                .where(func.abs(InventoryPriceHistory.size) <= 1e-8)
            )
            
            # Delete these entries
            result = await session.execute(query)
            small_positions = result.scalars().all()
            count = 0
            for position in small_positions:
                await session.delete(position)
                count += 1
            
            await session.commit()
            logger.info(f"Cleaned up {count} small positions")
            
    except Exception as e:
        logger.error(f"Error cleaning up small positions: {e}")


async def _consolidate_positions(session_maker, start_time, end_time, exchange_ids: List[int] = None):
    """Consolidate position data to avoid duplication."""
    try:
        # Initialize risk data service
        risk_service = RiskDataService(session_maker)
        
        # Consolidate positions
        result = await risk_service.consolidate_positions(
            start_time=start_time,
            end_time=end_time,
            exchange_ids=exchange_ids
        )
        
        logger.info(
            "Position consolidation complete",
            total_entries=result['total_entries'],
            entries_to_keep=result['entries_to_keep'],
            entries_to_remove=result['entries_to_remove']
        )
        
        # Clean position data (remove old entries)
        clean_result = await risk_service.clean_position_data()
        logger.info(f"Cleaned {clean_result['inventory_entries_cleaned']} old inventory entries")
        
    except Exception as e:
        logger.error(f"Error consolidating positions: {e}") 
