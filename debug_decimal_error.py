"""
Debug script to find the exact line causing the Decimal subscriptable error.
"""

import asyncio
import logging
import traceback
import sys
from decimal import Decimal

from bot_manager.strategies.stacked_market_making import StackedMarketMakingStrategy

# Setup logging to catch exact error locations
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DebugExchangeConnector:
    """Minimal connector to avoid side effects."""
    def __init__(self, exchange_name: str, symbol: str):
        self.exchange_name = exchange_name
        self.symbol = symbol

async def debug_decimal_error():
    """Find the exact line causing the decimal error."""
    try:
        config = {
            'base_coin': 'BERA',
            'quote_currencies': ['USDT'],
            'exchanges': ['binance'],
            'inventory': {
                'target_inventory': '1000.0',
                'max_inventory_deviation': '500.0',
                'inventory_price_method': 'manual',
                'manual_inventory_price': '0.45'
            },
            'tob_lines': [{
                'line_id': 0,
                'hourly_quantity': '100.0',
                'spread_bps': '20.0',
                'timeout_seconds': 30,
                'coefficient_method': 'inventory'
            }],
            'passive_lines': [],
            'time_periods': ['5min'],
            'coefficient_method': 'min',
            'min_coefficient': '1.0',
            'max_coefficient': '1.0'
        }
        
        logger.info("🔧 Creating minimal strategy...")
        strategy = StackedMarketMakingStrategy(
            instance_id="debug_decimal",
            symbol="BERA/USDT",
            exchanges=['binance'],
            config=config
        )
        
        strategy.exchange_connectors = {
            'binance': DebugExchangeConnector('binance', 'BERA/USDT')
        }
        
        logger.info("🔧 Initializing...")
        await strategy.initialize()
        
        logger.info("🔧 Creating mock market data...")
        # Mock just enough to avoid None errors
        class MockOrderbook:
            def get_best_bid_ask(self, exchange, symbol):
                return Decimal('0.4523'), Decimal('0.4527')
        
        class MockManager:
            def __init__(self):
                self.redis_manager = MockOrderbook()
        
        strategy.enhanced_orderbook_manager = MockManager()
        
        logger.info("🔧 Testing active line initialization...")
        strategy._initialize_active_lines()
        
        logger.info(f"📈 TOB lines: {len(strategy.active_tob_lines)}")
        
        # Now try to process one line manually to catch the exact error
        if strategy.active_tob_lines:
            line_id = list(strategy.active_tob_lines.keys())[0]
            line = strategy.active_tob_lines[line_id]
            
            logger.info(f"🎯 Processing TOB line {line_id} manually...")
            
            try:
                await strategy._process_tob_line(line)
                logger.info("✅ TOB line processed successfully!")
            except Exception as e:
                logger.error(f"❌ Error in _process_tob_line: {e}")
                traceback.print_exc()
                
                # Try to isolate further
                try:
                    logger.info("🔧 Testing individual TOB functions...")
                    await strategy._place_missing_tob_orders(line, 'binance')
                except Exception as e2:
                    logger.error(f"❌ Error in _place_missing_tob_orders: {e2}")
                    traceback.print_exc()
                    
                    # Print debug info
                    logger.info(f"🔍 Debug line object: {type(line)}")
                    logger.info(f"🔍 Debug line.config: {type(line.config)}")
                    logger.info(f"🔍 Debug line attributes: {dir(line)}")
        
    except Exception as e:
        logger.error(f"❌ Debug failed: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(debug_decimal_error())
