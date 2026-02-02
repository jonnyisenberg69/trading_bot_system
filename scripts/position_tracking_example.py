#!/usr/bin/env python
"""
Example script demonstrating position tracking across multiple exchanges.

This script shows how to:
- Initialize position tracking
- Track trades on multiple exchanges
- Calculate net positions
- Display position summaries
"""

import asyncio
import os
import json
from decimal import Decimal
import structlog

from order_management.tracking import Position, PositionManager
from order_management.order import Order, OrderSide, OrderType, OrderStatus


# Configure logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
        structlog.processors.JSONRenderer()
    ]
)
logger = structlog.get_logger()


async def main():
    """Run position tracking example."""
    logger.info("Starting position tracking example")
    
    # Create position manager with a temporary data directory
    data_dir = os.path.join(os.getcwd(), "data", "positions_example")
    os.makedirs(data_dir, exist_ok=True)
    
    position_manager = PositionManager(data_dir=data_dir)
    await position_manager.start()
    
    # Reset any existing positions to start fresh
    await position_manager.reset_all_positions()
    
    # Example 1: Update positions from trade data
    logger.info("Example 1: Updating positions from trade data")
    
    # Binance Spot: Buy 1 BTC
    await position_manager.update_from_trade("binance_spot", {
        'symbol': 'BTC/USDT',
        'id': 'trade1',
        'side': 'buy',
        'amount': 1.0,
        'price': 50000.0,
        'cost': 50000.0,
        'fee': {
            'cost': 0.001,
            'currency': 'BTC'
        }
    })
    
    # Bybit Perp: Sell 0.5 BTC (short)
    await position_manager.update_from_trade("bybit_perp", {
        'symbol': 'BTC/USDT',
        'id': 'trade2',
        'side': 'sell',
        'amount': 0.5,
        'price': 51000.0,
        'cost': 25500.0,
        'fee': {
            'cost': 12.75,
            'currency': 'USDT'
        }
    })
    
    # Example 2: Update positions from order fills
    logger.info("Example 2: Updating positions from order fills")
    
    # Create order object for Binance Futures
    order = Order(
        id="order1",
        client_order_id="client_order1",
        symbol="ETH/USDT",
        side=OrderSide.BUY,
        amount=Decimal('10'),
        price=Decimal('3000'),
        order_type=OrderType.LIMIT,
        exchange="binance_futures",
        status=OrderStatus.FILLED
    )
    
    # Fill data
    fill_data = {
        'id': 'fill1',
        'amount': 10.0,
        'price': 3000.0,
        'cost': 30000.0,
        'fee': {
            'cost': 15.0,
            'currency': 'USDT'
        }
    }
    
    # Update position from order fill
    await position_manager.update_from_order(order, fill_data)
    
    # Display individual positions
    logger.info("Individual positions:")
    positions = position_manager.get_all_positions()
    for pos in positions:
        logger.info(f"Position: {pos.exchange} {pos.symbol}", 
                    size=float(pos.size),
                    avg_price=float(pos.avg_price) if pos.avg_price else None,
                    p1=float(pos.p1),
                    p2=float(pos.p2),
                    p1_fee=float(pos.p1_fee),
                    p2_fee=float(pos.p2_fee),
                    side=pos.side)
    
    # Display net positions
    logger.info("Net positions:")
    net_positions = position_manager.get_all_net_positions()
    for symbol, pos in net_positions.items():
        logger.info(f"Net position for {symbol}",
                    size=float(pos['size']),
                    avg_price=float(pos['avg_price']) if pos['avg_price'] else None,
                    p1=float(pos['p1']),
                    p2=float(pos['p2']),
                    p1_fee=float(pos['p1_fee']),
                    p2_fee=float(pos['p2_fee']),
                    side=pos['side'])
    
    # Display position summary
    summary = position_manager.summarize_positions()
    logger.info("Position summary", 
                total_positions=summary['total_positions'],
                open_positions=summary['open_positions'],
                symbols=summary['symbols'],
                exchanges=summary['exchanges'],
                long_value=summary['long_value'],
                short_value=summary['short_value'],
                net_exposure=summary['net_exposure'])
    
    # Save all positions
    await position_manager.save_positions()
    logger.info("Positions saved to disk")
    
    # Example of loading positions
    logger.info("Loading positions from disk")
    new_manager = PositionManager(data_dir=data_dir)
    await new_manager.load_positions()
    
    loaded_positions = new_manager.get_all_positions()
    logger.info(f"Loaded {len(loaded_positions)} positions from disk")
    
    # Clean up
    await position_manager.stop()
    logger.info("Position tracking example completed")


if __name__ == "__main__":
    asyncio.run(main()) 