#!/usr/bin/env python
"""
Main entry point for trading bot system.

Initializes all components and starts the system.
"""

import asyncio
import os
import signal
import sys
import logging
from typing import Dict, Any, List
import structlog
from pathlib import Path

# Configure logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
        structlog.processors.JSONRenderer()
    ]
)
logger = structlog.get_logger()

# Add project root to path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

# Import components
from config.settings import load_config
from exchanges.connectors import create_exchange_connector
from exchanges.trade_sync import TradeSynchronizer
from order_management import OrderManager, ExecutionEngine, PositionManager
from database import init_db, close_db, get_session
from database.repositories import OrderRepository, TradeRepository, PositionRepository
from database.models import Exchange
from sqlalchemy import select


async def initialize_system(config: Dict[str, Any]):
    """
    Initialize all system components.
    
    Args:
        config: System configuration
        
    Returns:
        Dictionary with initialized components
    """
    # Create directories
    os.makedirs("data/positions", exist_ok=True)
    os.makedirs("data/db", exist_ok=True)
    
    # Initialize database
    db_config = config.get('database', {})
    db_url = db_config.get('url')
    await init_db(db_url)
    
    # Get session factory
    session_factory = get_session
    
    # Initialize exchange connectors
    exchange_connectors = {}
    exchange_id_map = {}  # Map exchange names to database IDs
    
    # Initialize repositories with first session
    async for session in session_factory():
        order_repository = OrderRepository(session)
        trade_repository = TradeRepository(session)
        position_repository = PositionRepository(session)
        
        # Get exchange IDs from database
        exchanges_result = await session.execute(select(Exchange.id, Exchange.name))
        db_exchanges = {name: id for id, name in exchanges_result.fetchall()}
        
        break
    
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
            exchange_connectors[connector_key] = connector
            # Look up actual exchange ID from database
            if connector_key in db_exchanges:
                exchange_id_map[connector_key] = db_exchanges[connector_key]
                logger.info(f"Initialized exchange connector: {connector_key} (DB ID: {db_exchanges[connector_key]})")
            else:
                logger.warning(f"Exchange {connector_key} not found in database - skipping trade sync for this exchange")
        else:
            logger.warning(f"Failed to initialize exchange connector: {connector_key}")
    
    # Initialize trade synchronization system (DISABLED - using EnhancedTradeSync via API)
    # trade_sync = TradeSynchronizer(
    #     exchange_connectors=exchange_connectors,
    #     trade_repository=trade_repository,
    #     position_repository=position_repository,
    #     exchange_id_map=exchange_id_map
    # )
    trade_sync = None  # Disabled - using EnhancedTradeSync via API routes
    
    # Initialize position tracking system
    position_manager = PositionManager(data_dir="data/positions")
    await position_manager.start()
    
    # Sync positions from exchanges that support it (like Hyperliquid)
    await position_manager.sync_positions_from_exchanges(exchange_connectors)
    
    # Initialize order management system
    order_manager = OrderManager(
        exchange_connectors=exchange_connectors,
        order_repository=order_repository,
        position_manager=position_manager
    )
    await order_manager.start()
    
    # Initialize execution engine
    execution_engine = ExecutionEngine(
        order_manager=order_manager,
        exchange_connectors=exchange_connectors
    )
    await execution_engine.start()
    
    # Start trade synchronization for all symbols of interest (DISABLED)
    # symbols_by_exchange = {}
    # for connector_key in exchange_connectors:
    #     # For now, just track BTC/USDT and ETH/USDT on all exchanges
    #     symbols_by_exchange[connector_key] = ["BTC/USDT", "ETH/USDT"]
    #     
    # await trade_sync.start(symbols_by_exchange)
    
    # Note: Trade synchronization is now handled via EnhancedTradeSync through API routes
    
    return {
        'exchange_connectors': exchange_connectors,
        'order_repository': order_repository,
        'trade_repository': trade_repository,
        'position_repository': position_repository,
        'position_manager': position_manager,
        'order_manager': order_manager,
        'execution_engine': execution_engine,
        'trade_sync': trade_sync
    }


async def shutdown_system(components: Dict[str, Any]):
    """
    Shutdown all system components.
    
    Args:
        components: Dictionary with system components
    """
    # Shutdown trade synchronization
    if 'trade_sync' in components and components['trade_sync'] is not None:
        await components['trade_sync'].stop()
    
    # Shutdown execution engine
    if 'execution_engine' in components:
        await components['execution_engine'].stop()
        
    # Shutdown order manager
    if 'order_manager' in components:
        await components['order_manager'].stop()
        
    # Shutdown position manager
    if 'position_manager' in components:
        await components['position_manager'].stop()
        
    # Close exchange connectors
    if 'exchange_connectors' in components:
        for name, connector in components['exchange_connectors'].items():
            try:
                if hasattr(connector, 'disconnect'):
                    await connector.disconnect()
                elif hasattr(connector, 'close'):
                    await connector.close()
            except Exception as e:
                logger.warning(f"Error closing connector {name}: {e}")
    
    # Close database
    await close_db()
            
    logger.info("System shutdown complete")


async def main():
    """Main entry point."""
    logger.info("Starting trading bot system")
    
    # Load configuration
    config = load_config()
    
    # Initialize system
    components = await initialize_system(config)
    
    # Setup signal handlers for graceful shutdown
    loop = asyncio.get_running_loop()
    
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown_system(components)))
    
    try:
        # Keep running until interrupted
        while True:
            await asyncio.sleep(1)
            
    except asyncio.CancelledError:
        # Shutdown requested
        logger.info("Shutdown requested")
        
    finally:
        # Ensure clean shutdown
        await shutdown_system(components)
        
    logger.info("Trading bot system stopped")


if __name__ == "__main__":
    asyncio.run(main())
