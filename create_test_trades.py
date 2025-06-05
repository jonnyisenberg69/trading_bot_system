#!/usr/bin/env python
"""
Create test trades to verify the position system works.
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

async def create_test_trades():
    """Create some test trades to verify the position system."""
    try:
        from order_management.enhanced_trade_sync import TradeDeduplicationManager
        from database import get_session
        
        print("Creating test trades...")
        
        # Sample trades for different exchanges
        test_trades = [
            {
                'id': 'binance_test_1',
                'symbol': 'BERA/USDT',
                'side': 'buy',
                'amount': 100.0,
                'price': 0.5,
                'cost': 50.0,
                'timestamp': int(datetime.now().timestamp() * 1000),
                'fee': {'cost': 0.1, 'currency': 'USDT'},
                'exchange': 'binance_spot'
            },
            {
                'id': 'bybit_test_1',
                'symbol': 'BERA/USDT',
                'side': 'sell',
                'amount': 50.0,
                'price': 0.6,
                'cost': 30.0,
                'timestamp': int(datetime.now().timestamp() * 1000),
                'fee': {'cost': 0.05, 'currency': 'USDT'},
                'exchange': 'bybit_spot'
            },
            {
                'id': 'mexc_test_1',
                'symbol': 'BERA/USDT',
                'side': 'buy',
                'amount': 200.0,
                'price': 0.45,
                'cost': 90.0,
                'timestamp': int(datetime.now().timestamp() * 1000),
                'fee': {'cost': 0.2, 'currency': 'USDT'},
                'exchange': 'mexc_spot'
            }
        ]
        
        async for session in get_session():
            dedup_manager = TradeDeduplicationManager(session)
            
            for trade in test_trades:
                exchange_name = trade.pop('exchange')  # Remove exchange from trade data
                success, reason = await dedup_manager.insert_trade_with_deduplication(
                    trade_data=trade,
                    exchange_name=exchange_name
                )
                
                if success:
                    print(f"✅ Created test trade: {trade['id']} on {exchange_name}")
                else:
                    print(f"❌ Failed to create trade: {reason}")
            
            break
        
        # Verify trades were created
        from database.models import Trade, Exchange
        from sqlalchemy import select, func
        
        async for session in get_session():
            result = await session.execute(select(func.count(Trade.id)))
            total_trades = result.scalar_one()
            print(f"\nTotal trades in database: {total_trades}")
            
            # Show recent trades
            result = await session.execute(
                select(Trade, Exchange.name).join(Exchange).order_by(Trade.timestamp.desc()).limit(5)
            )
            recent_trades = result.all()
            
            print("\nRecent trades:")
            for trade, exchange_name in recent_trades:
                print(f"  {exchange_name}: {trade.exchange_trade_id} - {trade.symbol} - {trade.side} {trade.amount}@{trade.price}")
            
            break
            
    except Exception as e:
        print(f'Error: {e}')
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(create_test_trades()) 