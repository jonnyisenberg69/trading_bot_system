"""
Simple Autodiscovery Trade Collector

Use the same approach as comprehensive_bera_pricing_tracker.py:
Just use the known expected pairs and test which ones have live data.
"""

import asyncio
import logging
import redis
import json
from typing import Dict, List, Set, Optional
from datetime import datetime, timezone, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


async def autodiscover_and_collect_all_bera():
    """Use same autodiscovery approach as pricing system to collect ALL BERA pairs."""
    
    print("\n🔍 AUTODISCOVERY TRADE COLLECTOR")
    print("Using same approach as comprehensive_bera_pricing_tracker.py")
    print("="*80)
    
    # Step 1: Use the same expected pairs as pricing system
    expected_exchange_pairs = {
        'binance': ['BERA/USDT', 'BERA/BTC', 'BERA/USDC', 'BERA/BNB', 'BERA/TRY', 'BERA/FDUSD'],
        'bybit': ['BERA/USDT', 'BERA/USDC'],  
        'mexc': ['BERA/USDT', 'BERA/USDC'],
        'bitget': ['BERA/USDT'],
        'gateio': ['BERA/USDT'],
        'hyperliquid': ['BERA/USDC:USDC']  # Perp format
    }
    
    # Step 2: Test which pairs have live pricing data (same as pricing system)
    redis_client = redis.Redis(decode_responses=True)
    
    print("\n📊 TESTING WHICH EXPECTED PAIRS HAVE LIVE DATA:")
    print("-" * 60)
    
    available_pairs = {}
    
    for exchange, expected_pairs in expected_exchange_pairs.items():
        print(f"\n🔍 {exchange.upper()}:")
        
        available_pairs[exchange] = []
        
        for symbol in expected_pairs:
            # Test different key formats (same as pricing system)
            test_keys = [
                f"combined_orderbook:spot:{symbol}",
                f"combined_orderbook:perp:{symbol}",
                f"combined_orderbook:all:{symbol}",
                f"orderbook:{exchange}:{symbol}",
                f"orderbook:{exchange}_spot:{symbol}",
            ]
            
            has_data = False
            for test_key in test_keys:
                if redis_client.exists(test_key):
                    has_data = True
                    print(f"  ✅ {symbol}: Found data at {test_key}")
                    break
            
            if has_data:
                available_pairs[exchange].append(symbol)
            else:
                print(f"  ❌ {symbol}: No data found")
    
    # Step 3: Start TradeCollector for all available pairs
    print(f"\n🚀 STARTING TRADE COLLECTION FOR ALL AVAILABLE PAIRS")
    print("-" * 60)
    
    try:
        from market_data_collection.collector import TradeCollector, ExchangeConfig
        from market_data_collection.database import TradeDataManager
        
        # Initialize database
        db_manager = TradeDataManager()
        await db_manager.initialize()
        
        # Create custom TradeCollector
        collector = TradeCollector(db_manager=db_manager)
        
        # Override exchange configs with ALL available pairs
        collector.exchange_configs = {}
        
        total_configured = 0
        
        for exchange, symbols in available_pairs.items():
            if symbols:
                symbols_dict = {}
                
                # Categorize symbols by type
                for symbol in symbols:
                    if ':' in symbol and symbol != 'BERA/USDC:USDC':
                        # Generic perpetual format
                        symbols_dict['perp'] = symbol
                    else:
                        # Spot format or Hyperliquid perp
                        if symbol == 'BERA/USDC:USDC':
                            symbols_dict['perp'] = symbol  # Hyperliquid perp
                        else:
                            symbols_dict['spot'] = symbol  # Regular spot
                
                if symbols_dict:
                    collector.exchange_configs[exchange] = ExchangeConfig(
                        exchange_id=exchange,
                        symbols=symbols_dict
                    )
                    
                    total_configured += len(symbols)
                    print(f"✅ {exchange}: {symbols_dict}")
        
        print(f"📊 Configured {total_configured} BERA pairs across {len(collector.exchange_configs)} exchanges")
        
        # Start collection
        await collector.start()
        print("✅ AUTODISCOVERED TRADE COLLECTION STARTED")
        
        # Collect for 3 minutes
        print("⏱️  Collecting for 3 minutes...")
        await asyncio.sleep(180)
        
        # Calculate coefficients
        print("\n🧮 CALCULATING COEFFICIENTS FOR ALL AVAILABLE PAIRS")
        print("="*60)
        
        from market_data_collection.moving_averages import MovingAverageCalculator, MovingAverageConfig
        from market_data_collection.exchange_coefficients import SimpleExchangeCoefficientCalculator
        
        # Setup MA calculator
        ma_configs = [
            MovingAverageConfig(period=2, ma_type='ewma', volume_type='base'),
            MovingAverageConfig(period=4, ma_type='ewma', volume_type='base'),
            MovingAverageConfig(period=3, ma_type='ewma', volume_type='quote'),
            MovingAverageConfig(period=6, ma_type='ewma', volume_type='quote'),
        ]
        
        ma_calculator = MovingAverageCalculator(ma_configs=ma_configs)
        
        coeff_calculator = SimpleExchangeCoefficientCalculator(
            ma_calculator=ma_calculator,
            calculation_method='min',
            min_coefficient=0.5,
            max_coefficient=2.0
        )
        
        # Build MA data and calculate coefficients
        current_time = datetime.now(timezone.utc)
        
        for exchange, symbols in available_pairs.items():
            for symbol in symbols:
                # Build MA data with multiple intervals
                for update_num in range(1, 6):
                    window_start = current_time - timedelta(minutes=6-update_num)
                    window_end = current_time - timedelta(minutes=5-update_num)
                    
                    trades = await db_manager.get_trades_by_timerange(window_start, window_end)
                    
                    # Filter for this specific exchange/symbol
                    relevant_trades = []
                    for trade in trades:
                        if trade.exchange == exchange and trade.symbol == symbol:
                            relevant_trades.append({
                                'side': trade.side,
                                'amount': float(trade.amount),
                                'price': float(trade.price),
                                'cost': float(trade.amount) * float(trade.price),
                                'timestamp': trade.timestamp
                            })
                    
                    if relevant_trades:
                        volume_data = ma_calculator.calculate_volume_data(relevant_trades[:8])
                        ma_calculator.update_moving_averages(symbol, exchange, volume_data, window_end)
        
        # Display results in requested format
        print("\n" + "="*80)
        print("🎯 AUTODISCOVERED BERA COEFFICIENTS")  
        print("="*80)
        
        total_pairs = 0
        working_coefficients = 0
        
        for exchange, symbols in available_pairs.items():
            if symbols:
                print(f"\nCoefficients {exchange.title()}:")
                
                for symbol in symbols:
                    total_pairs += 1
                    try:
                        coefficient = coeff_calculator.calculate_coefficient(symbol, exchange)
                        
                        if coefficient is not None:
                            print(f"- {symbol}: {coefficient:.4f}")
                            working_coefficients += 1
                        else:
                            print(f"- {symbol}: Building...")
                    
                    except Exception as e:
                        print(f"- {symbol}: Error - {e}")
        
        print(f"\n📊 AUTODISCOVERY SUMMARY:")
        print(f"✅ Total available pairs: {total_pairs}")
        print(f"✅ Working coefficients: {working_coefficients}")
        print(f"⏳ Building/No data: {total_pairs - working_coefficients}")
        
        coverage = (working_coefficients / total_pairs * 100) if total_pairs > 0 else 0
        print(f"📈 Coverage: {coverage:.1f}%")
        
        # Stop collection
        await collector.stop()
        await db_manager.close()
        
        print("✅ Autodiscovered trade collection completed")
        
    except Exception as e:
        print(f"❌ Failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    print("🔍 SIMPLE AUTODISCOVERY TRADE COLLECTOR")
    print("Same logic as comprehensive_bera_pricing_tracker.py")
    asyncio.run(autodiscover_and_collect_all_bera())
