"""
Restart Comprehensive BERA Trade Collection

Kill existing processes and start trade collection for ALL available BERA pairs 
on ALL exchanges to ensure complete coefficient coverage.
"""

import asyncio
import logging
import subprocess
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


async def restart_comprehensive_bera_collection():
    """Restart trade collection for all BERA pairs."""
    
    print("\n" + "="*80)
    print("🔄 RESTARTING COMPREHENSIVE BERA TRADE COLLECTION")
    print("Ensuring ALL available BERA pairs are collected")
    print("="*80)
    
    # Step 1: Check what BERA pairs are actually available
    print("\n📊 STEP 1: DISCOVERING ALL AVAILABLE BERA PAIRS")
    print("-" * 50)
    
    try:
        from honest_bera_exchange_status import check_all_bera_exchanges
        
        # Get comprehensive BERA pair status
        bera_status = await check_all_bera_exchanges()
        
        print("✅ BERA pair discovery completed")
        
        # Extract available pairs by exchange
        available_pairs = {}
        for exchange_data in bera_status.values():
            exchange = exchange_data['exchange']
            if exchange not in available_pairs:
                available_pairs[exchange] = []
            
            for symbol, data in exchange_data.get('symbols', {}).items():
                if 'BERA' in symbol and data.get('available', False):
                    # Convert to database format if needed
                    db_symbol = symbol
                    if exchange in ['binance', 'bybit'] and symbol == 'BERA/USDT':
                        db_symbol = 'BERA/USDT:USDT'  # Perp format
                    elif exchange == 'hyperliquid' and 'USDC' in symbol:
                        db_symbol = 'BERA/USDC:USDC'  # Hyperliquid perp format
                    
                    available_pairs[exchange].append(db_symbol)
        
        print("📊 DISCOVERED AVAILABLE BERA PAIRS:")
        total_pairs = 0
        for exchange, pairs in available_pairs.items():
            print(f"   📈 {exchange}: {pairs}")
            total_pairs += len(pairs)
        
        print(f"📊 Total available BERA pairs: {total_pairs}")
        
        if total_pairs == 0:
            print("❌ No BERA pairs discovered - checking exchange connectors...")
            return
        
    except Exception as e:
        print(f"❌ Discovery failed: {e}")
        # Fallback to known pairs
        available_pairs = {
            'binance': ['BERA/USDT', 'BERA/USDT:USDT', 'BERA/FDUSD', 'BERA/BNB', 'BERA/TRY'],
            'bybit': ['BERA/USDT', 'BERA/USDT:USDT'],
            'bitget': ['BERA/USDT'],
            'gateio': ['BERA/USDT'],
            'mexc': ['BERA/USDT'],
            'hyperliquid': ['BERA/USDC:USDC'],
        }
        print("Using fallback BERA pair configuration")
    
    # Step 2: Kill existing processes
    print("\n🔄 STEP 2: KILLING EXISTING PROCESSES")
    print("-" * 40)
    
    try:
        subprocess.run(['pkill', '-f', 'TradeCollector'], check=False)
        subprocess.run(['pkill', '-f', 'trade_collector'], check=False)
        subprocess.run(['pkill', '-f', 'market_data_service'], check=False)
        print("✅ Killed existing trade collection processes")
        time.sleep(3)  # Wait for cleanup
    except Exception as e:
        print(f"⚠️  Kill process warning: {e}")
    
    # Step 3: Start comprehensive TradeCollector
    print("\n🚀 STEP 3: STARTING COMPREHENSIVE TRADE COLLECTION")
    print("-" * 50)
    
    try:
        from market_data_collection.collector import TradeCollector, ExchangeConfig
        from market_data_collection.database import TradeDataManager
        
        # Initialize database
        db_manager = TradeDataManager()
        await db_manager.initialize()
        print("✅ Database manager initialized")
        
        # Create custom TradeCollector with ALL BERA pairs
        collector = TradeCollector(db_manager=db_manager)
        
        # Override exchange configs to include ALL available BERA pairs
        collector.exchange_configs = {}
        
        for exchange, pairs in available_pairs.items():
            if pairs:  # Only configure exchanges with available pairs
                symbols_dict = {}
                
                for pair in pairs:
                    if ':' in pair:
                        symbols_dict['perp'] = pair  # Perpetual format
                    else:
                        symbols_dict['spot'] = pair  # Spot format
                
                collector.exchange_configs[exchange] = ExchangeConfig(
                    exchange_id=exchange,
                    symbols=symbols_dict
                )
                
                print(f"✅ Configured {exchange}: {symbols_dict}")
        
        # Start comprehensive collection
        print(f"\n🔄 Starting collection for {len(collector.exchange_configs)} exchanges...")
        await collector.start()
        
        print("✅ COMPREHENSIVE BERA TRADE COLLECTION STARTED")
        print(f"📊 Collecting on {sum(len(pairs) for pairs in available_pairs.values())} BERA pairs")
        print("⏱️  Running for 5 minutes to build comprehensive MA data...")
        
        # Let it collect for 5 minutes to build robust data
        for minute in range(1, 6):
            await asyncio.sleep(60)
            
            # Check collection status
            status = collector.get_status()
            active_tasks = status.get('active_tasks', 0)
            buffer_size = status.get('buffer_size', 0)
            
            print(f"📊 Minute {minute}: {active_tasks} tasks active, {buffer_size} trades buffered")
            
            # Quick coefficient check
            if minute >= 3:  # After 3 minutes, check if coefficients are building
                from market_data_collection.moving_averages import MovingAverageCalculator, MovingAverageConfig
                from market_data_collection.exchange_coefficients import SimpleExchangeCoefficientCalculator
                
                # Quick test with simple periods
                test_configs = [
                    MovingAverageConfig(period=2, ma_type='ewma', volume_type='base'),
                    MovingAverageConfig(period=4, ma_type='ewma', volume_type='base'),
                ]
                
                test_calculator = MovingAverageCalculator(ma_configs=test_configs)
                test_coeff_calc = SimpleExchangeCoefficientCalculator(
                    ma_calculator=test_calculator,
                    calculation_method='min',
                    min_coefficient=0.5,
                    max_coefficient=2.0
                )
                
                # Test binance BERA/USDT
                current_time = datetime.now(timezone.utc)
                recent_start = current_time - timedelta(minutes=3)
                
                trades = await db_manager.get_trades_by_timerange(recent_start, current_time)
                binance_trades = [t for t in trades if t.exchange == 'binance' and 'BERA' in t.symbol][:20]
                
                if binance_trades:
                    trade_dicts = [{'side': t.side, 'amount': float(t.amount), 'price': float(t.price), 
                                  'cost': float(t.amount) * float(t.price), 'timestamp': t.timestamp} 
                                 for t in binance_trades]
                    
                    volume_data = test_calculator.calculate_volume_data(trade_dicts)
                    test_calculator.update_moving_averages('BERA/USDT', 'binance', volume_data, current_time)
                    
                    coeff = test_coeff_calc.calculate_coefficient('BERA/USDT', 'binance')
                    if coeff:
                        print(f"   📈 Sample coefficient building: binance BERA/USDT = {coeff:.4f}")
        
        print("\n🎯 COMPREHENSIVE COLLECTION COMPLETE")
        print("Stopping collector...")
        
        await collector.stop()
        await db_manager.close()
        
        print("✅ Comprehensive BERA trade collection completed")
        print("🧮 Ready to calculate coefficients for ALL BERA pairs")
        
    except Exception as e:
        print(f"❌ Failed to start comprehensive collection: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    print("🔄 RESTARTING COMPREHENSIVE BERA TRADE COLLECTION")
    
    # Import datetime here for the test
    from datetime import datetime, timezone, timedelta
    
    asyncio.run(restart_comprehensive_bera_collection())
