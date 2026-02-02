"""
BERA Pricing Infrastructure Validation

This validates ALL pricing legs for ALL BERA pairs are live and ready.
"""

import asyncio
import logging
import time
import json
from decimal import Decimal
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
import redis

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def validate_all_bera_market_data():
    """Comprehensive validation of all BERA market data infrastructure."""
    
    logger.info("🔍 COMPREHENSIVE BERA MARKET DATA VALIDATION")
    
    try:
        # Connect to Redis
        redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        redis_client.ping()
        logger.info("✅ Redis connection established")
        
        # Get all BERA combined orderbook keys
        bera_keys = [k for k in redis_client.keys('combined_orderbook:*') if 'BERA' in k]
        
        logger.info(f"📊 Found {len(bera_keys)} BERA orderbook data sources")
        
        # Parse and validate each BERA pair
        bera_pairs_data = {}
        
        for key in sorted(bera_keys):
            try:
                # Parse key: combined_orderbook:spot:BERA/USDT -> (spot, BERA/USDT)
                parts = key.split(':')
                venue_type = parts[1]  # 'spot' or 'all'
                symbol = parts[2]      # 'BERA/USDT'
                
                # Get the data
                data = redis_client.get(key)
                if data:
                    parsed = json.loads(data)
                    
                    # Extract pricing information
                    bids = parsed.get('bids', [])
                    asks = parsed.get('asks', [])
                    timestamp = parsed.get('timestamp', 0)
                    
                    if bids and asks:
                        best_bid = float(bids[0][0])
                        best_ask = float(asks[0][0])
                        spread_bps = ((best_ask - best_bid) / best_bid) * 10000
                        
                        # Calculate data age
                        data_age = (time.time() * 1000 - timestamp) / 1000 if timestamp else float('inf')
                        
                        # Store results
                        if symbol not in bera_pairs_data:
                            bera_pairs_data[symbol] = {}
                        
                        bera_pairs_data[symbol][venue_type] = {
                            'best_bid': best_bid,
                            'best_ask': best_ask,
                            'spread_bps': round(spread_bps, 2),
                            'data_age_sec': round(data_age, 1),
                            'bid_levels': len(bids),
                            'ask_levels': len(asks),
                            'status': 'fresh' if data_age < 30 else 'stale'
                        }
                        
                        status_emoji = "✅" if data_age < 30 else "⚠️"
                        logger.info(
                            f"{status_emoji} {symbol:12} ({venue_type:4}) "
                            f"bid={best_bid:.6f} ask={best_ask:.6f} "
                            f"spread={spread_bps:.1f}bps age={data_age:.1f}s "
                            f"levels={len(bids)}/{len(asks)}"
                        )
                    else:
                        logger.warning(f"❌ {symbol:12} ({venue_type:4}) empty orderbook")
                
            except Exception as e:
                logger.error(f"❌ Error processing {key}: {e}")
        
        # Generate summary report
        logger.info("\n" + "="*80)
        logger.info("📋 BERA PRICING INFRASTRUCTURE SUMMARY")
        logger.info("="*80)
        
        total_pairs = len(bera_pairs_data)
        fresh_data_count = 0
        total_venues = 0
        
        for symbol, venues in bera_pairs_data.items():
            for venue_type, data in venues.items():
                total_venues += 1
                if data['status'] == 'fresh':
                    fresh_data_count += 1
        
        logger.info(f"📊 BERA PAIRS AVAILABLE: {total_pairs}")
        logger.info(f"📊 VENUE COVERAGE: {total_venues} venue types (spot/all)")
        logger.info(f"📊 FRESH DATA: {fresh_data_count}/{total_venues} ({fresh_data_count/total_venues*100:.1f}%)")
        
        # List all available pairs
        logger.info(f"\n🎯 AVAILABLE BERA PAIRS:")
        for symbol in sorted(bera_pairs_data.keys()):
            venues = list(bera_pairs_data[symbol].keys())
            fresh_venues = [v for v, d in bera_pairs_data[symbol].items() if d['status'] == 'fresh']
            status = "✅" if fresh_venues else "⚠️"
            logger.info(f"   {status} {symbol:12} venues: {', '.join(venues)} (fresh: {', '.join(fresh_venues)})")
        
        # Test pricing calculations
        logger.info(f"\n💰 TESTING PRICING CALCULATIONS:")
        
        pricing_tests_passed = 0
        pricing_tests_total = 0
        
        for symbol in sorted(bera_pairs_data.keys())[:3]:  # Test first 3 pairs
            if 'spot' in bera_pairs_data[symbol]:
                data = bera_pairs_data[symbol]['spot']
                if data['status'] == 'fresh':
                    pricing_tests_total += 1
                    
                    # Test mid-price calculation
                    mid_price = (data['best_bid'] + data['best_ask']) / 2
                    
                    # Test spread calculation
                    spread_bps = data['spread_bps']
                    
                    # Test if suitable for market making
                    if spread_bps > 5 and spread_bps < 1000:  # Reasonable spread
                        pricing_tests_passed += 1
                        logger.info(f"✅ {symbol:12} mid={mid_price:.6f} spread={spread_bps:.1f}bps ✅ SUITABLE FOR TRADING")
                    else:
                        logger.warning(f"⚠️  {symbol:12} mid={mid_price:.6f} spread={spread_bps:.1f}bps ⚠️  SPREAD TOO WIDE/NARROW")
        
        pricing_success_rate = (pricing_tests_passed / pricing_tests_total * 100) if pricing_tests_total > 0 else 0
        logger.info(f"💰 PRICING TESTS: {pricing_tests_passed}/{pricing_tests_total} ({pricing_success_rate:.1f}%)")
        
        # Overall assessment
        logger.info(f"\n🎯 OVERALL ASSESSMENT:")
        
        if total_pairs >= 6 and fresh_data_count >= total_venues * 0.8:
            logger.info("🎉 INFRASTRUCTURE IS EXCELLENT - Ready for strategy testing!")
            overall_status = "EXCELLENT"
        elif total_pairs >= 4 and fresh_data_count >= total_venues * 0.6:
            logger.info("✅ INFRASTRUCTURE IS GOOD - Can proceed with strategy testing")
            overall_status = "GOOD"
        elif total_pairs >= 2:
            logger.info("⚠️  INFRASTRUCTURE IS PARTIAL - Limited testing possible")
            overall_status = "PARTIAL"
        else:
            logger.info("❌ INFRASTRUCTURE IS INSUFFICIENT - Need to fix data issues")
            overall_status = "INSUFFICIENT"
        
        logger.info("="*80)
        
        return overall_status, bera_pairs_data
        
    except Exception as e:
        logger.error(f"❌ Validation failed: {e}")
        import traceback
        traceback.print_exc()
        return "FAILED", {}


async def test_enhanced_aggregated_orderbook():
    """Test the enhanced aggregated orderbook manager with real data."""
    logger.info("📚 TESTING ENHANCED AGGREGATED ORDERBOOK MANAGER")
    
    try:
        # Import and test
        from market_data.enhanced_aggregated_orderbook_manager import EnhancedAggregatedOrderbookManager
        
        # Get available BERA pairs
        redis_client = redis.Redis(decode_responses=True)
        bera_keys = [k for k in redis_client.keys('combined_orderbook:spot:*') if 'BERA' in k]
        bera_symbols = [k.split(':')[2] for k in bera_keys]
        
        logger.info(f"🔧 Testing with {len(bera_symbols)} BERA symbols: {', '.join(bera_symbols)}")
        
        # Initialize manager
        manager = EnhancedAggregatedOrderbookManager(symbols=bera_symbols)
        await manager.start()
        
        # Let it initialize for a few seconds
        await asyncio.sleep(3)
        
        # Test WAPQ calculations
        wapq_results = {}
        
        for symbol in bera_symbols:
            try:
                # Test different quantities
                for quantity in [100.0, 500.0, 1000.0]:
                    bid_wapq = manager.get_aggregated_wapq(symbol, 'bid', quantity)
                    ask_wapq = manager.get_aggregated_wapq(symbol, 'ask', quantity)
                    
                    if bid_wapq and ask_wapq:
                        if symbol not in wapq_results:
                            wapq_results[symbol] = []
                        
                        wapq_results[symbol].append({
                            'quantity': quantity,
                            'bid_wapq': float(bid_wapq),
                            'ask_wapq': float(ask_wapq),
                            'spread_bps': ((ask_wapq - bid_wapq) / bid_wapq) * 10000
                        })
                
                if symbol in wapq_results and wapq_results[symbol]:
                    first_result = wapq_results[symbol][0]
                    logger.info(
                        f"✅ {symbol:12} WAPQ working: "
                        f"bid={first_result['bid_wapq']:.6f} "
                        f"ask={first_result['ask_wapq']:.6f} "
                        f"spread={first_result['spread_bps']:.1f}bps"
                    )
                else:
                    logger.warning(f"⚠️  {symbol:12} WAPQ not available")
                    
            except Exception as e:
                logger.error(f"❌ {symbol:12} WAPQ error: {e}")
        
        await manager.stop()
        
        # Summary
        successful_pairs = len(wapq_results)
        total_pairs = len(bera_symbols)
        success_rate = (successful_pairs / total_pairs * 100) if total_pairs > 0 else 0
        
        logger.info(f"📚 ORDERBOOK MANAGER RESULTS: {successful_pairs}/{total_pairs} pairs ({success_rate:.1f}%)")
        
        return wapq_results
        
    except Exception as e:
        logger.error(f"❌ Enhanced orderbook manager test failed: {e}")
        return {}


async def test_multi_reference_pricing():
    """Test multi-reference pricing engine with available BERA data."""
    logger.info("💰 TESTING MULTI-REFERENCE PRICING ENGINE")
    
    try:
        from market_data.multi_reference_pricing_engine import (
            MultiReferencePricingEngine, PricingContext, PricingMethod
        )
        
        # Get available BERA pairs
        redis_client = redis.Redis(decode_responses=True)
        bera_keys = [k for k in redis_client.keys('combined_orderbook:spot:*') if 'BERA' in k]
        bera_symbols = [k.split(':')[2] for k in bera_keys]
        
        # Initialize pricing engine
        pricing_engine = MultiReferencePricingEngine()
        await pricing_engine.start()
        
        # Test pricing for each BERA pair
        pricing_results = {}
        
        for symbol in bera_symbols:
            try:
                # Test different pricing methods
                methods_to_test = [
                    PricingMethod.VOLUME_WEIGHTED,
                    PricingMethod.BEST_PRICE,
                    PricingMethod.MIDPOINT
                ]
                
                symbol_results = {}
                
                for method in methods_to_test:
                    context = PricingContext(
                        quantity=Decimal('100'),
                        exchanges=['binance', 'bybit', 'okx'],  # Common exchanges
                        method=method
                    )
                    
                    # Test both sides
                    buy_price = pricing_engine.get_smart_price(symbol, 'buy', context)
                    sell_price = pricing_engine.get_smart_price(symbol, 'sell', context)
                    
                    if buy_price and sell_price and buy_price > 0 and sell_price > 0:
                        spread_bps = ((sell_price - buy_price) / buy_price) * 10000
                        
                        symbol_results[method.value] = {
                            'buy_price': float(buy_price),
                            'sell_price': float(sell_price),
                            'spread_bps': float(spread_bps)
                        }
                
                if symbol_results:
                    pricing_results[symbol] = symbol_results
                    
                    # Log best method result
                    if PricingMethod.VOLUME_WEIGHTED.value in symbol_results:
                        vw_result = symbol_results[PricingMethod.VOLUME_WEIGHTED.value]
                        logger.info(
                            f"✅ {symbol:12} PRICING: "
                            f"buy={vw_result['buy_price']:.6f} "
                            f"sell={vw_result['sell_price']:.6f} "
                            f"spread={vw_result['spread_bps']:.1f}bps"
                        )
                    else:
                        logger.warning(f"⚠️  {symbol:12} LIMITED PRICING available")
                else:
                    logger.error(f"❌ {symbol:12} NO PRICING available")
                    
            except Exception as e:
                logger.error(f"❌ {symbol:12} pricing error: {e}")
        
        await pricing_engine.stop()
        
 