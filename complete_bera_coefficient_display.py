"""
Complete BERA Coefficient Display

Show coefficients for ALL BERA pairs across ALL exchanges exactly as requested:

Coefficients Binance:
- BERA/USDT: X
- BERA/USDC: Y
- BERA/BNB: Z

etc.

Uses the same autodiscovery approach as the pricing system but for trade collection.
"""

import asyncio
import logging
import redis
import json
from typing import Dict, List, Set, Optional
from datetime import datetime, timezone, timedelta

logging.basicConfig(level=logging.WARNING)  # Quiet logs for clean output


async def get_comprehensive_bera_coefficients():
    """Get coefficients for ALL BERA pairs using proper autodiscovery and collection."""
    
    print("\n" + "="*80)
    print("🎯 COMPREHENSIVE BERA COEFFICIENT DISPLAY")
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    try:
        # Step 1: Autodiscover ALL BERA pairs using Redis data
        print("\n🔍 STEP 1: AUTODISCOVERING ALL BERA PAIRS")
        print("-" * 50)
        
        redis_client = redis.Redis(decode_responses=True)
        
        # Get all combined orderbook keys with BERA
        bera_keys = redis_client.keys('combined_orderbook:*BERA*')
        
        # Parse to get exchange/symbol mappings
        discovered_pairs = {}
        
        for key in bera_keys:
            try:
                # Format: combined_orderbook:market_type:symbol
                parts = key.split(':')
                if len(parts) >= 3:
                    market_type = parts[1]
                    symbol = ':'.join(parts[2:])  # Handle symbols with colons
                    
                    print(f"   📊 Found: {market_type} {symbol}")
                    
                    # Get the actual data to see which exchanges have it
                    data = redis_client.get(key)
                    if data:
                        orderbook_data = json.loads(data)
                        exchanges = orderbook_data.get('exchanges', [])
                        
                        for exchange_data in exchanges:
                            exchange_name = exchange_data.get('exchange', '')
                            
                            # Map to standard exchange names
                            if 'binance' in exchange_name.lower():
                                exchange = 'binance'
                            elif 'bybit' in exchange_name.lower():
                                exchange = 'bybit'
                            elif 'bitget' in exchange_name.lower():
                                exchange = 'bitget'
                            elif 'gate' in exchange_name.lower():
                                exchange = 'gateio'
                            elif 'mexc' in exchange_name.lower():
                                exchange = 'mexc'
                            elif 'hyperliquid' in exchange_name.lower():
                                exchange = 'hyperliquid'
                            else:
                                continue
                            
                            if exchange not in discovered_pairs:
                                discovered_pairs[exchange] = set()
                            discovered_pairs[exchange].add(symbol)
                            
            except Exception as e:
                print(f"   ⚠️  Error parsing {key}: {e}")
        
        # Convert sets to lists
        discovered_pairs = {k: list(v) for k, v in discovered_pairs.items()}
        
        total_pairs = sum(len(pairs) for pairs in discovered_pairs.values())
        print(f"\n✅ AUTODISCOVERED {total_pairs} BERA PAIRS:")
        for exchange, pairs in discovered_pairs.items():
            print(f"   📈 {exchange}: {pairs}")
        
        # Step 2: Start comprehensive trade collection
        print(f"\n🔄 STEP 2: STARTING COMPREHENSIVE TRADE COLLECTION")
        print("-" * 50)
        
        from market_data_collection.collector import TradeCollector
        from market_data_collection.database import TradeDataManager
        
        db_manager = TradeDataManager()
        collector = TradeCollector(db_manager=db_manager)
        
        # Configure collector for ALL discovered pairs
        collector.exchange_configs = {}
        
        configured_total = 0
        for exchange, symbols in discovered_pairs.items():
            if symbols:
                symbols_dict = {}
                
                # Categorize by market type
                for symbol in symbols:
                    if ':' in symbol and 'USDC:USDC' in symbol:
                        # Hyperliquid perp format
                        symbols_dict['perp'] = symbol
                    elif ':' in symbol:
                        # Other perp format
                        symbols_dict['perp'] = symbol
                    else:
                        # Spot format
                        symbols_dict['spot'] = symbol
                
                from market_data_collection.collector import ExchangeConfig
                collector.exchange_configs[exchange] = ExchangeConfig(
                    exchange_id=exchange,
                    symbols=symbols_dict
                )
                
                configured_total += len(symbols)
                print(f"✅ {exchange}: {symbols_dict}")
        
        print(f"\n📊 Configured {configured_total} BERA pairs across {len(collector.exchange_configs)} exchanges")
        
        # Step 3: Collect trades for 2 minutes 
        print(f"\n⏱️  STEP 3: COLLECTING TRADES FOR 2 MINUTES")
        print("-" * 50)
        
        start_time = datetime.now()
        
        # Start collection task
        collection_task = asyncio.create_task(collector.start())
        
        # Let it collect for 2 minutes
        await asyncio.sleep(120)
        
        # Stop collection
        collection_task.cancel()
        try:
            await collection_task
        except asyncio.CancelledError:
            pass
        
        end_time = datetime.now()
        
        print(f"✅ Trade collection completed ({(end_time - start_time).total_seconds():.1f}s)")
        
        # Step 4: Feed MA calculator with fresh trade data and calculate coefficients
        print(f"\n🧮 STEP 4: CALCULATING COEFFICIENTS FOR ALL PAIRS")
        print("-" * 50)
        
        from market_data_collection.moving_averages import MovingAverageCalculator, MovingAverageConfig
        from market_data_collection.exchange_coefficients import SimpleExchangeCoefficientCalculator
        
        # Setup MA calculator with shorter periods for faster results
        ma_configs = [
            MovingAverageConfig('sma2_base', volume_type='base_volume', ma_type='sma', period_points=2),
            MovingAverageConfig('sma4_base', volume_type='base_volume', ma_type='sma', period_points=4),
            MovingAverageConfig('ewma2_base', volume_type='base_volume', ma_type='ewma', period_points=2),
            MovingAverageConfig('ewma4_base', volume_type='base_volume', ma_type='ewma', period_points=4),
        ]
        
        ma_calculator = MovingAverageCalculator(ma_configs)
        coefficient_calculator = SimpleExchangeCoefficientCalculator(
            ma_calculator=ma_calculator,
            time_periods=['2min', '4min'],  # Short periods for quick testing
            coefficient_method='min',
            min_coefficient=0.2,
            max_coefficient=3.0
        )
        
        # Step 5: Update MAs with fresh trade data for each exchange/symbol
        print(f"\n📊 STEP 5: UPDATING MOVING AVERAGES WITH FRESH DATA")
        print("-" * 50)
        
        current_time = datetime.now(timezone.utc)
        
        total_updates = 0
        for exchange in discovered_pairs.keys():
            for symbol in discovered_pairs[exchange]:
                try:
                    # Get fresh trades for this exchange/symbol
                    start_window = current_time - timedelta(minutes=10)
                    end_window = current_time
                    
                    trades = await db_manager.get_trades_by_timerange(start_window, end_window)
                    
                    # Filter for this specific exchange and symbol
                    filtered_trades = []
                    for trade in trades:
                        if (hasattr(trade, 'exchange') and trade.exchange == exchange and 
                            hasattr(trade, 'symbol') and trade.symbol == symbol):
                            filtered_trades.append(trade)
                    
                    if filtered_trades:
                        # Convert to volume data format
                        total_base_volume = sum(float(t.amount) for t in filtered_trades)
                        total_quote_volume = sum(float(t.amount) * float(t.price) for t in filtered_trades)
                        
                        from market_data_collection.moving_averages import VolumeData
                        volume_data = VolumeData(
                            base_volume=total_base_volume,
                            quote_volume=total_quote_volume,
                            timestamp=current_time
                        )
                        
                        # Update MA calculator
                        ma_calculator.update_moving_averages(symbol, exchange, volume_data)
                        total_updates += 1
                        
                        print(f"   ✅ {exchange} {symbol}: {len(filtered_trades)} trades, {total_base_volume:.2f} base vol")
                    else:
                        print(f"   ⚠️  {exchange} {symbol}: No fresh trades found")
                        
                except Exception as e:
                    print(f"   ❌ {exchange} {symbol}: Error updating - {e}")
        
        print(f"\n📊 Updated MAs for {total_updates} exchange/symbol combinations")
        
        # Step 6: Display coefficients in requested format
        print(f"\n" + "="*80)
        print("🎯 BERA COEFFICIENTS - ALL EXCHANGES")
        print("="*80)
        
        working_coefficients = 0
        total_pairs = 0
        
        for exchange in sorted(discovered_pairs.keys()):
            print(f"\nCoefficients {exchange.title()}:")
            
            exchange_working = 0
            for symbol in sorted(discovered_pairs[exchange]):
                total_pairs += 1
                try:
                    coefficient = coefficient_calculator.calculate_coefficient(symbol, exchange)
                    
                    if coefficient and coefficient != 1.0:
                        print(f"- {symbol}: {coefficient:.4f}")
                        working_coefficients += 1
                        exchange_working += 1
                    else:
                        print(f"- {symbol}: No data")
                        
                except Exception as e:
                    print(f"- {symbol}: Error ({str(e)[:50]})")
            
            if exchange_working == 0:
                print(f"  ⚠️  No working coefficients for {exchange}")
        
        print(f"\n" + "="*80)
        print(f"📊 SUMMARY: {working_coefficients}/{total_pairs} BERA pairs with working coefficients")
        print(f"📈 Coverage: {(working_coefficients/total_pairs)*100:.1f}%")
        print("="*80)
        
        if working_coefficients < total_pairs * 0.8:  # Less than 80% coverage
            print(f"\n⚠️  COVERAGE TOO LOW: {working_coefficients}/{total_pairs} pairs working")
            print("🔧 Need to improve trade collection or wait for more data accumulation")
        
    except Exception as e:
        print(f"❌ Error in comprehensive coefficient test: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(get_comprehensive_bera_coefficients())
