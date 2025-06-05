#!/usr/bin/env python
"""
Debug script to test the deduplication logic.
"""

import asyncio
from database import get_session
from database.repositories import TradeRepository
from order_management.enhanced_trade_sync import TradeDeduplicationManager

async def test_deduplication():
    # Sample trade data
    sample_trade = {
        'id': 'test_trade_456',  # Different ID to avoid cache issues
        'symbol': 'BERA/USDT',
        'side': 'buy',
        'amount': 100.0,
        'price': 0.5,
        'cost': 50.0,
        'timestamp': 1733400000000,  # Some timestamp
        'fee': {
            'cost': 0.1,
            'currency': 'USDT'
        }
    }
    
    async for session in get_session():
        try:
            # Test the deduplication manager
            dedup_manager = TradeDeduplicationManager(session)
            
            print("Testing deduplication with sample trade...")
            print(f"Trade: {sample_trade}")
            
            # Test insertion directly (this should work now)
            success, reason = await dedup_manager.insert_trade_with_deduplication(sample_trade, 'binance_spot')
            print(f"Insert success: {success}, Reason: {reason}")
            
            # Test insertion again (this should be duplicate)
            success2, reason2 = await dedup_manager.insert_trade_with_deduplication(sample_trade, 'binance_spot')
            print(f"Insert again success: {success2}, Reason: {reason2}")
            
        except Exception as e:
            print(f"Error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            await session.close()
        break

if __name__ == "__main__":
    asyncio.run(test_deduplication()) 