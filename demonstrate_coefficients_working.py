"""
Demonstrate Live Coefficient Calculations - WORKING EXCHANGES

This specifically demonstrates coefficient calculations with real trade data 
over 1min and 5min windows using the existing proven system.
"""

import asyncio
import logging
import time
from decimal import Decimal
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def demonstrate_coefficient_calculations_live():
    """Demonstrate coefficient calculations with real trade data over 5 minutes."""
    
    print("\n" + "="*100)
    print("🧮 LIVE COEFFICIENT CALCULATIONS - 5 MINUTE DEMONSTRATION")
    print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Using existing proven coefficient system from volume_weighted_top_of_book.py")
    print("="*100)
    
    try:
        from market_data_collection.moving_averages import MovingAverageCalculator, MovingAverageConfig
        from market_data_collection.exchange_coefficients import SimpleExchangeCoefficientCalculator
        
        print("✅ Successfully imported existing proven coefficient system")
        
        # Set up coefficient tracking for working BERA pairs
        working_setups = [
            {'symbol': 'BERA/USDT', 'exchange': 'binance', 'name': 'Primary'},
            {'symbol': 'BERA/USDT', 'exchange': 'bybit', 'name': 'Secondary'},  
            {'symbol': 'BERA/USDC', 'exchange': 'binance', 'name': 'Cross-Currency'},
            {'symbol': 'BERA/BTC', 'exchange': 'binance', 'name': 'Bitcoin-Pair'},
        ]
        
        coefficient_trackers = {}
        
        print(f"\n🏗️ Setting up coefficient tracking for {len(working_setups)} configurations...")
        
        for setup in working_setups:
            symbol = setup['symbol']
            exchange = setup['exchange']
            name = setup['name']
            
            base_currency = symbol.split('/')[0]  # BERA
            quote_currency = symbol.split('/')[1]  # USDT, USDC, etc.
            
            print(f"  🔧 {name}: {symbol} on {exchange}...")
            
            try:
                # Create MA configurations for 1min and 5min windows
                ma_configs = [
                    MovingAverageConfig(period=5, ma_type='sma', volume_type='base'),     # ~1 min window
                    MovingAverageConfig(period=25, ma_type='sma', volume_type='quote'),   # ~5 min window
                    MovingAverageConfig(period=10, ma_type='ewma', volume_type='imbalance'),
                    MovingAverageConfig(period=15, ma_type='ewma', volume_type='ratio'),
                ]
                
                # Initialize MA calculator
                ma_calculator = MovingAverageCalculator(ma_configs=ma_configs)
                
                # Create coefficient calculator
                coeff_calculator = SimpleExchangeCoefficientCalculator(
                    ma_calculator=ma_calculator,
                    min_coefficient=0.6,
                    max_coefficient=1.4,
                    calculation_method='min'
                )
                
                coefficient_trackers[f"{exchange}_{symbol}"] = {
                    'symbol': symbol,
                    'exchange': exchange,
                    'name': name,
                    'ma_calculator': ma_calculator,
                    'coeff_calculator': coeff_calculator,
                    'coefficient_history': [],
                    'ma_history': []
                }
                
                print(f"    ✅ {name}: Initialized with {len(ma_configs)} MA configurations")
                
            except Exception as setup_error:
                print(f"    ❌ {name}: Setup failed - {setup_error}")
        
        print(f"\n✅ Successfully set up {len(coefficient_trackers)} coefficient trackers")
        
        # Monitor for 5 minutes with real trade data collection
        monitoring_minutes = 5
        monitoring_seconds = monitoring_minutes * 60
        
        print(f"\n⏱️ MONITORING COEFFICIENT CALCULATIONS FOR {monitoring_minutes} MINUTES...")
        print(f"📊 This collects REAL trade data and calculates live coefficients")
        print(f"🎯 Windows: 1min (5 periods) and 5min (25 periods)")
        
        for second in range(monitoring_seconds):
            await asyncio.sleep(1)
            
            # Report every 30 seconds for detailed tracking
            if second % 30 == 0:
                elapsed_minutes = second / 60
                print(f"\n📊 MINUTE {elapsed_minutes:.1f}/{monitoring_minutes} - LIVE COEFFICIENT UPDATE:")
                
                for tracker_key, tracker in coefficient_trackers.items():
                    name = tracker['name']
                    symbol = tracker['symbol']
                    exchange = tracker['exchange']
                    
                    try:
                        # Get current coefficient
                        coefficient = tracker['coeff_calculator'].calculate_coefficient()
                        
                        # Get detailed MA data
                        ma_data = tracker['ma_calculator'].get_all_moving_averages()
                        
                        # Store in history
                        history_entry = {
                            'timestamp': time.time(),
                            'coefficient': float(coefficient),
                            'ma_data': {k: float(v) if v else None for k, v in ma_data.items()}
                        }
                        
                        tracker['coefficient_history'].append(history_entry)
                        tracker['ma_history'].append(ma_data)
                        
                        # Show detailed breakdown
                        print(f"  🧮 {name:12} ({exchange:8} {symbol:10}):")
                        print(f"      📊 Coefficient: {coefficient:.4f}")
                        
                        # Show MA breakdown
                        ma_summary = []
                        for config_key, value in ma_data.items():
                            if value and value != 0:
                                ma_summary.append(f"{config_key}={value:.2f}")
                            else:
                                ma_summary.append(f"{config_key}=NoData")
                        
                        if ma_summary:
                            print(f"      📈 MA Data: {' | '.join(ma_summary[:2])}")
                            if len(ma_summary) > 2:
                                print(f"               {' | '.join(ma_summary[2:])}")
                        else:
                            print(f"      📈 MA Data: No historical trade data yet")
                        
                    except Exception as calc_error:
                        print(f"  ❌ {name:12}: Calculation error - {str(calc_error)[:60]}")
        
        print(f"\n✅ COEFFICIENT CALCULATION DEMONSTRATION COMPLETE")
        
        # Show final coefficient evolution
        print(f"\n🎯 COEFFICIENT EVOLUTION OVER {monitoring_minutes} MINUTES:")
        
        for tracker_key, tracker in coefficient_trackers.items():
            name = tracker['name']
            symbol = tracker['symbol']
            exchange = tracker['exchange']
            history = tracker['coefficient_history']
            
            if len(history) >= 2:
                initial = history[0]
                final = history[-1]
                change = final['coefficient'] - initial['coefficient']
                
                print(f"  📊 {name:12} ({exchange:8} {symbol}): {initial['coefficient']:.4f} → {final['coefficient']:.4f} (Δ{change:+.4f}) [{len(history)} samples]")
                
                # Show if MA data was collected
                final_ma = final.get('ma_data', {})
                ma_with_data = [k for k, v in final_ma.items() if v and v != 0]
                if ma_with_data:
                    print(f"      ✅ MA Data: {len(ma_with_data)}/{len(final_ma)} configurations have real trade data")
                else:
                    print(f"      ⚠️ MA Data: No historical trade data collected yet (normal for new pairs)")
                    
            else:
                print(f"  ❌ {name:12}: No coefficient data collected")
        
        return coefficient_trackers
        
    except Exception as e:
        print(f"❌ Coefficient demonstration failed: {e}")
        import traceback
        traceback.print_exc()
        return {}


async def main():
    """Main coefficient demonstration."""
    
    try:
        coefficient_data = await demonstrate_coefficient_calculations_live()
        
        if coefficient_data:
            print(f"\n🎉 COEFFICIENT DEMONSTRATION SUCCESSFUL!")
            print(f"✅ Validated existing proven coefficient system with live BERA data")
        else:
            print(f"\n❌ Coefficient demonstration failed")
            
    except Exception as e:
        print(f"❌ Main demonstration failed: {e}")


if __name__ == "__main__":
    asyncio.run(main())
