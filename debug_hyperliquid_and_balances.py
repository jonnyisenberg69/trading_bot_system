#!/usr/bin/env python3
"""
Debug Hyperliquid Trades and Balances

This script specifically tests Hyperliquid to see what responses we're getting.

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


async def debug_hyperliquid():
    """Debug Hyperliquid connector specifically."""
    logger.info("🔍 Starting Hyperliquid debug session...")
    
    try:
        # Import required modules
        from config.settings import load_config
        from exchanges.connectors import create_exchange_connector
        
        # Load config
        config = load_config()
        
        # Create Hyperliquid connector
        exchanges_list = config.get('exchanges', [])
        
        # Find Hyperliquid config in the list
        hl_config = None
        for exchange in exchanges_list:
            if exchange.get('name') == 'hyperliquid' and exchange.get('type') == 'perp':
                hl_config = exchange
                break
        
        if not hl_config:
            logger.error("❌ No Hyperliquid config found!")
            logger.info(f"Available exchanges: {[ex.get('name') + '_' + ex.get('type') for ex in exchanges_list]}")
            return
        
        logger.info(f"Hyperliquid config keys: {list(hl_config.keys())}")
        
        connector = create_exchange_connector('hyperliquid', hl_config)
        
        # Test connection
        logger.info("🔗 Testing Hyperliquid connection...")
        connected = await connector.connect()
        
        if not connected:
            logger.error("❌ Failed to connect to Hyperliquid!")
            return
        
        logger.info("✅ Successfully connected to Hyperliquid")
        
        # Test 1: Get balance
        logger.info("💰 Testing Hyperliquid balance...")
        try:
            balance = await connector.get_balance()
            logger.info(f"📊 Balance result: {balance}")
        except Exception as e:
            logger.error(f"❌ Balance error: {e}")
        
        # Test 2: Get positions
        logger.info("📈 Testing Hyperliquid positions...")
        try:
            positions = await connector.get_positions()
            logger.info(f"📊 Found {len(positions)} positions")
            for pos in positions:
                logger.info(f"   Position: {pos.get('symbol')} - {pos.get('side')} {pos.get('size')} @ {pos.get('entry_price')}")
        except Exception as e:
            logger.error(f"❌ Positions error: {e}")
        
        # Test 3: Test symbol normalization
        logger.info("🔤 Testing symbol normalization...")
        test_symbols = ['BERA/USDT', 'BTC/USDT', 'ETH/USDT']
        for symbol in test_symbols:
            normalized = connector.normalize_symbol(symbol)
            denormalized = connector.denormalize_symbol(normalized)
            logger.info(f"   {symbol} -> {normalized} -> {denormalized}")
        
        # Test 4: Get trade history for different symbols
        logger.info("📝 Testing Hyperliquid trade history...")
        
        test_symbols = ['BERA/USDT', 'BTC/USDT', 'ETH/USDT']
        since_time = datetime.now() - timedelta(days=30)  # Last 30 days
        
        for symbol in test_symbols:
            logger.info(f"🔍 Checking trades for {symbol}...")
            try:
                trades = await connector.get_trade_history(
                    symbol=symbol,
                    since=since_time,
                    limit=50
                )
                logger.info(f"📊 {symbol}: Found {len(trades)} trades")
                
                if trades:
                    # Show first few trades
                    for i, trade in enumerate(trades[:3]):
                        logger.info(f"   Trade {i+1}: {trade.get('id')} - {trade.get('side')} {trade.get('amount')} @ {trade.get('price')} at {trade.get('datetime')}")
                
            except Exception as e:
                logger.error(f"❌ Trade history error for {symbol}: {e}")
        
        # Test 5: Try to get ALL trades (no symbol filter)
        logger.info("📝 Testing Hyperliquid ALL trades (no symbol filter)...")
        try:
            all_trades = await connector.get_trade_history(
                symbol=None,  # All symbols
                since=since_time,
                limit=100
            )
            logger.info(f"📊 ALL TRADES: Found {len(all_trades)} trades")
            
            if all_trades:
                # Group by symbol
                by_symbol = {}
                for trade in all_trades:
                    symbol = trade.get('symbol', 'UNKNOWN')
                    if symbol not in by_symbol:
                        by_symbol[symbol] = []
                    by_symbol[symbol].append(trade)
                
                logger.info("📊 Trades by symbol:")
                for symbol, symbol_trades in by_symbol.items():
                    logger.info(f"   {symbol}: {len(symbol_trades)} trades")
                    # Show latest trade
                    if symbol_trades:
                        latest = max(symbol_trades, key=lambda x: x.get('timestamp', 0))
                        logger.info(f"      Latest: {latest.get('side')} {latest.get('amount')} @ {latest.get('price')} at {latest.get('datetime')}")
            
        except Exception as e:
            logger.error(f"❌ All trades error: {e}")
        
        # Test 6: Check raw ccxt exchange
        logger.info("🔧 Testing raw ccxt exchange...")
        try:
            if hasattr(connector, 'exchange'):
                markets = await connector.exchange.load_markets()
                logger.info(f"📊 CCXT markets loaded: {len(markets)} markets")
                
                # Show some market symbols
                market_symbols = list(markets.keys())[:10]
                logger.info(f"   Sample markets: {market_symbols}")
                
                # Try raw fetch_my_trades
                logger.info("🔧 Testing raw fetch_my_trades...")
                raw_trades = await connector.exchange.fetch_my_trades(limit=10)
                logger.info(f"📊 Raw CCXT trades: {len(raw_trades)} trades")
                
                if raw_trades:
                    for trade in raw_trades[:3]:
                        logger.info(f"   Raw trade: {trade.get('symbol')} - {trade.get('side')} {trade.get('amount')} @ {trade.get('price')}")
                
        except Exception as e:
            logger.error(f"❌ Raw CCXT error: {e}")
        
        # Cleanup
        await connector.disconnect()
        logger.info("🎉 Hyperliquid debug session completed")
        
    except Exception as e:
        logger.error(f"❌ Debug session failed: {e}")
        raise


async def main():
    """Main execution function."""
    logger.info("🚀 Starting Hyperliquid debug...")
    
    await debug_hyperliquid()


if __name__ == "__main__":
    asyncio.run(main()) 