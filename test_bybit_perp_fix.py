#!/usr/bin/env python
"""
Test script to verify bybit_perp trade insertion fix.
"""

import asyncio
from database import get_session
from order_management.enhanced_trade_sync import TradeDeduplicationManager
from datetime import datetime

async def test_bybit_perp_fix():
    """Test that bybit_perp trades can now be inserted."""
    
    # Sample bybit_perp trade data (similar to what would come from the connector)
    bybit_perp_trade = {
        'id': 'bybit_perp_test_123',
        'symbol': 'BERAUSDT',  # Bybit perp uses this format
        'side': 'buy',
        'amount': 10.0,
        'price': 2.45,
        'cost': 24.5,
        'timestamp': int(datetime.now().timestamp() * 1000),
        'fee': {'cost': 0.02, 'currency': 'USDT'},
        'order': 'order_123'
    }
    
    print("Testing bybit_perp trade insertion...")
    print(f"Trade data: {bybit_perp_trade}")
    
    async for session in get_session():
        try:
            dedup_manager = TradeDeduplicationManager(session)
            
            # Test insertion
            success, reason = await dedup_manager.insert_trade_with_deduplication(
                trade_data=bybit_perp_trade,
                exchange_name='bybit_perp'
            )
            
            print(f"Insertion result: success={success}, reason={reason}")
            
            if success:
                print("✅ SUCCESS: bybit_perp trade was inserted!")
                
                # Verify it's in the database
                from database.models import Trade, Exchange
                from sqlalchemy import select
                
                result = await session.execute(
                    select(Trade, Exchange.name).join(Exchange)
                    .where(Trade.exchange_trade_id == bybit_perp_trade['id'])
                )
                trade_record = result.first()
                
                if trade_record:
                    trade, exchange_name = trade_record
                    print(f"✅ VERIFIED: Trade found in database:")
                    print(f"   Exchange: {exchange_name}")
                    print(f"   Symbol: {trade.symbol}")
                    print(f"   Side: {trade.side}")
                    print(f"   Amount: {trade.amount}")
                    print(f"   Price: {trade.price}")
                else:
                    print("❌ ERROR: Trade not found in database after insertion")
            else:
                print(f"❌ FAILED: {reason}")
                
        except Exception as e:
            print(f"❌ ERROR: {e}")
            import traceback
            traceback.print_exc()
        finally:
            await session.close()
        break

if __name__ == "__main__":
    asyncio.run(test_bybit_perp_fix()) 