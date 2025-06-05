#!/usr/bin/env python
"""
Example script demonstrating trade tracking and position monitoring.

This script shows how to:
- Initialize trade synchronization
- Track trades on multiple exchanges
- Monitor positions from trades
- Display trade and position summaries
"""

import asyncio
import os
import sys
import json
from decimal import Decimal
import structlog
import argparse
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
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import load_config
from exchanges.connectors import create_exchange_connector
from exchanges.trade_sync import TradeSynchronizer
from database import init_db, get_session
from database.repositories.trade_repository import TradeRepository
from database.repositories.position_repository import PositionRepository


async def main(exchange_name: str, symbol: str, api_key: str = "", api_secret: str = "", testnet: bool = True):
    """
    Run trade tracking example.
    
    Args:
        exchange_name: Exchange name (e.g., binance, bybit)
        symbol: Symbol to track (e.g., BTC/USDT)
        api_key: API key
        api_secret: API secret
        testnet: Use testnet
    """
    logger.info("Starting trade tracking example", 
                exchange=exchange_name, 
                symbol=symbol, 
                testnet=testnet)
    
    # Create directories
    os.makedirs("data/db", exist_ok=True)
    
    # Initialize database
    await init_db("sqlite+aiosqlite:///data/db/tracking_example.db")
    
    # Setup exchange connector
    connector_key = f"{exchange_name}_spot"  # Assuming spot for simplicity
    connector = create_exchange_connector(
        exchange_name,
        api_key=api_key,
        api_secret=api_secret,
        testnet=testnet,
        market_type="spot"
    )
    
    if not connector:
        logger.error(f"Failed to create connector for {exchange_name}")
        return
        
    await connector.connect()
    
    # Create repositories
    async for session in get_session():
        trade_repository = TradeRepository(session)
        position_repository = PositionRepository(session)
        break
    
    # Create exchange ID map (simple case with just one exchange)
    exchange_id_map = {connector_key: 1}
    
    # Initialize trade synchronizer
    trade_sync = TradeSynchronizer(
        exchange_connectors={connector_key: connector},
        trade_repository=trade_repository,
        position_repository=position_repository,
        exchange_id_map=exchange_id_map
    )
    
    # Start tracking trades for symbol
    await trade_sync.start({connector_key: [symbol]})
    
    # Display initial message
    print(f"\nTracking trades for {symbol} on {exchange_name}...")
    print("Press Ctrl+C to stop")
    
    try:
        # Main monitoring loop
        while True:
            # Get recent trades
            async for session in get_session():
                trade_repo = TradeRepository(session)
                position_repo = PositionRepository(session)
                
                # Get trades
                trades = await trade_repo.get_trades_by_symbol(
                    symbol=symbol,
                    exchange_id=1,
                    limit=10
                )
                
                # Get position
                position = await position_repo.get_position(
                    exchange_id=1,
                    symbol=symbol
                )
                
                # Display current state
                print("\n" + "="*50)
                print(f"Current position for {symbol} on {exchange_name}:")
                if position:
                    print(f"  Size: {position.size}")
                    print(f"  Side: {position.side}")
                    print(f"  Avg Price: {position.avg_price}")
                    print(f"  P1 (base): {position.p1}")
                    print(f"  P2 (quote): {position.p2}")
                    print(f"  P1 fee: {position.p1_fee}")
                    print(f"  P2 fee: {position.p2_fee}")
                else:
                    print("  No position found")
                    
                print("\nRecent trades:")
                if trades:
                    for trade in trades:
                        print(f"  {trade.timestamp}: {trade.side} {trade.amount} @ {trade.price} (ID: {trade.exchange_trade_id})")
                else:
                    print("  No trades found")
                
                break
                
            # Wait before next update
            await asyncio.sleep(10)
            
    except KeyboardInterrupt:
        print("\nStopping trade tracking...")
    finally:
        # Shutdown trade synchronizer
        await trade_sync.stop()
        
        # Close exchange connector
        await connector.close()
        
        logger.info("Trade tracking example stopped")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Trade tracking example")
    parser.add_argument("--exchange", "-e", required=True, help="Exchange name (e.g., binance, bybit)")
    parser.add_argument("--symbol", "-s", required=True, help="Symbol to track (e.g., BTC/USDT)")
    parser.add_argument("--api-key", "-k", default="", help="API key")
    parser.add_argument("--api-secret", "-a", default="", help="API secret")
    parser.add_argument("--testnet", "-t", action="store_true", help="Use testnet")
    
    args = parser.parse_args()
    
    asyncio.run(main(
        exchange_name=args.exchange,
        symbol=args.symbol,
        api_key=args.api_key,
        api_secret=args.api_secret,
        testnet=args.testnet
    )) 