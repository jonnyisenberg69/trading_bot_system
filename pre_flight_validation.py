#!/usr/bin/env python
"""
Pre-Flight Validation

Comprehensive validation of all exchanges before running comprehensive test.
Checks minimum order sizes, balances, symbol availability, and trading requirements.
"""

import asyncio
import sys
from pathlib import Path
from decimal import Decimal
import json

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from exchanges.connectors import create_exchange_connector
from config.exchange_keys import get_exchange_config

async def validate_exchange_requirements():
    """Validate all exchange requirements for comprehensive testing."""
    
    print("🔍 PRE-FLIGHT VALIDATION")
    print("=" * 80)
    
    # Test configuration with BERA/USDT focus
    validation_config = {
        'binance_spot': {'symbol': 'BERA/USDT', 'test_amount': Decimal('1')},
        'binance_perp': {'symbol': 'BTC/USDT:USDT', 'test_amount': Decimal('0.001')},
        'bybit_spot': {'symbol': 'BERA/USDT', 'test_amount': Decimal('1')},
        'bybit_perp': {'symbol': 'BTC/USDT:USDT', 'test_amount': Decimal('0.001')},
        'mexc_spot': {'symbol': 'BERA/USDT', 'test_amount': Decimal('3')},
        'gateio_spot': {'symbol': 'BERA/USDT', 'test_amount': Decimal('2')},
        'bitget_spot': {'symbol': 'BERA/USDT', 'test_amount': Decimal('1')},
        'hyperliquid_perp': {'symbol': 'BTC/USDC:USDC', 'test_amount': Decimal('0.001')}
    }
    
    validation_results = {}
    recommendations = {}
    
    for exchange_name, config in validation_config.items():
        print(f"\n📋 VALIDATING {exchange_name.upper()}")
        print("-" * 50)
        
        try:
            # Get configuration and create connector
            exchange_config = get_exchange_config(exchange_name)
            connector = create_exchange_connector(exchange_name.split('_')[0], exchange_config)
            
            connected = await connector.connect()
            if not connected:
                print(f"❌ Connection failed")
                validation_results[exchange_name] = {
                    'status': 'connection_failed',
                    'issues': ['Cannot connect to exchange']
                }
                continue
            
            symbol = config['symbol']
            test_amount = config['test_amount']
            issues = []
            suggestions = []
            
            print(f"   🧪 Testing symbol: {symbol}")
            print(f"   📊 Test amount: {test_amount}")
            
            # 1. Check symbol availability
            markets = connector.exchange.markets
            if symbol not in markets:
                issues.append(f"Symbol {symbol} not found")
                print(f"   ❌ Symbol not available")
                
                # Find alternatives
                if 'BERA' in symbol:
                    alternatives = [s for s in markets.keys() if 'BERA' in s and markets[s].get('active', False)][:3]
                    if alternatives:
                        suggestions.append(f"Alternative BERA symbols: {', '.join(alternatives)}")
                        print(f"   💡 Alternatives: {', '.join(alternatives)}")
            else:
                market = markets[symbol]
                is_active = market.get('active', False)
                is_correct_type = market.get('type', '') == ('spot' if '_spot' in exchange_name else 'swap')
                
                if not is_active:
                    issues.append(f"Symbol {symbol} is inactive")
                    print(f"   ❌ Symbol inactive")
                elif not is_correct_type:
                    issues.append(f"Symbol {symbol} wrong type (expected: {'spot' if '_spot' in exchange_name else 'perp'})")
                    print(f"   ❌ Wrong market type")
                else:
                    print(f"   ✅ Symbol available and active")
                    
                    # Check market limits
                    limits = market.get('limits', {})
                    amount_limits = limits.get('amount', {})
                    cost_limits = limits.get('cost', {})
                    
                    min_amount = amount_limits.get('min')
                    min_cost = cost_limits.get('min')
                    
                    print(f"   📏 Market limits:")
                    print(f"       Min amount: {min_amount}")
                    print(f"       Min cost: {min_cost}")
                    
                    # Get current price for calculations
                    try:
                        orderbook = await connector.get_orderbook(symbol, limit=1)
                        if orderbook and orderbook.get('bids') and orderbook.get('asks'):
                            market_price = (orderbook['bids'][0][0] + orderbook['asks'][0][0]) / 2
                            test_notional = float(test_amount) * float(market_price)
                            
                            print(f"   💰 Current price: ${market_price:,.2f}")
                            print(f"   💰 Test notional: ${test_notional:.2f}")
                            
                            # Check minimum amount
                            if min_amount and float(test_amount) < float(min_amount):
                                issues.append(f"Test amount {test_amount} below minimum {min_amount}")
                                suggested_amount = float(min_amount) * 1.1  # 10% buffer
                                suggestions.append(f"Use minimum amount: {suggested_amount}")
                                print(f"   ❌ Amount too small (min: {min_amount})")
                            
                            # Check minimum cost/notional
                            if min_cost and test_notional < float(min_cost):
                                issues.append(f"Test notional ${test_notional:.2f} below minimum ${min_cost}")
                                suggested_amount = float(min_cost) / float(market_price) * 1.1  # 10% buffer
                                suggestions.append(f"Use minimum notional amount: {suggested_amount:.4f}")
                                print(f"   ❌ Notional too small (min: ${min_cost})")
                            
                            if not issues:
                                print(f"   ✅ Order size requirements met")
                        else:
                            issues.append("Cannot get market price")
                            print(f"   ❌ Cannot get orderbook")
                    except Exception as e:
                        issues.append(f"Price check failed: {e}")
                        print(f"   ❌ Price check error: {e}")
            
            # 2. Check balance
            try:
                balance = await connector.get_balance()
                quote_currency = symbol.split('/')[1].split(':')[0] if ':' in symbol else symbol.split('/')[1]
                quote_balance = balance.get(quote_currency, Decimal('0'))
                
                print(f"   💳 {quote_currency} balance: {quote_balance}")
                
                if float(quote_balance) < 5:  # Minimum $5 for testing
                    issues.append(f"Insufficient {quote_currency} balance: {quote_balance}")
                    suggestions.append(f"Add more {quote_currency} balance (current: {quote_balance})")
                    print(f"   ⚠️  Low balance")
                else:
                    print(f"   ✅ Sufficient balance")
                    
            except Exception as e:
                issues.append(f"Balance check failed: {e}")
                print(f"   ❌ Balance check error: {e}")
            
            # 3. Exchange-specific checks
            if exchange_name == 'binance_spot':
                # Binance has strict notional filters
                if symbol in markets:
                    # Most Binance spot pairs have $10+ minimum notional
                    suggestions.append("Use at least $12 notional for Binance spot")
                    print(f"   💡 Binance recommendation: $12+ notional")
            
            elif exchange_name == 'gateio_spot':
                # Gate.io has $3 minimum for most pairs
                suggestions.append("Use at least $4 notional for Gate.io")
                print(f"   💡 Gate.io recommendation: $4+ notional")
            
            elif 'mexc' in exchange_name:
                # MEXC works well but check for quote vs base amount logic
                suggestions.append("MEXC uses quote amount for market buys")
                print(f"   💡 MEXC uses quote amount logic")
            
            await connector.disconnect()
            
            # Store results
            validation_results[exchange_name] = {
                'status': 'success' if not issues else 'issues_found',
                'issues': issues,
                'suggestions': suggestions,
                'symbol': symbol,
                'test_amount': float(test_amount)
            }
            
            if not issues:
                print(f"   🎉 {exchange_name}: READY FOR TESTING")
            else:
                print(f"   ⚠️  {exchange_name}: {len(issues)} issues found")
                
        except Exception as e:
            print(f"   ❌ Validation error: {e}")
            validation_results[exchange_name] = {
                'status': 'error',
                'error': str(e)
            }
    
    # Generate summary and recommendations
    print(f"\n" + "="*80)
    print(f"📊 VALIDATION SUMMARY")
    print(f"="*80)
    
    ready_count = 0
    total_issues = 0
    
    for exchange, result in validation_results.items():
        status = result.get('status', 'unknown')
        issues = result.get('issues', [])
        
        if status == 'success':
            emoji = "✅"
            ready_count += 1
        elif status == 'issues_found':
            emoji = "⚠️"
            total_issues += len(issues)
        else:
            emoji = "❌"
            total_issues += 1
        
        print(f"{emoji} {exchange}: {status}")
        
        if issues:
            for issue in issues[:2]:  # Show first 2 issues
                print(f"     - {issue}")
    
    print(f"\n📈 READINESS SCORE: {ready_count}/{len(validation_config)} exchanges ready")
    print(f"🚨 TOTAL ISSUES: {total_issues}")
    
    # Generate recommended configuration
    print(f"\n🛠️  RECOMMENDED CONFIGURATION:")
    print("-" * 40)
    
    recommended_config = {}
    
    for exchange, result in validation_results.items():
        if result.get('status') in ['success', 'issues_found']:
            suggestions = result.get('suggestions', [])
            symbol = result.get('symbol', 'Unknown')
            
            # Calculate recommended amount based on suggestions
            recommended_amount = result.get('test_amount', 1)
            
            for suggestion in suggestions:
                if 'minimum amount:' in suggestion:
                    try:
                        amount = float(suggestion.split('minimum amount: ')[1])
                        recommended_amount = max(recommended_amount, amount)
                    except:
                        pass
                elif 'minimum notional amount:' in suggestion:
                    try:
                        amount = float(suggestion.split('minimum notional amount: ')[1])
                        recommended_amount = max(recommended_amount, amount)
                    except:
                        pass
                elif '$12+' in suggestion:
                    recommended_amount = max(recommended_amount, 5)  # Conservative for BERA
                elif '$4+' in suggestion:
                    recommended_amount = max(recommended_amount, 2)  # Conservative for BERA
            
            recommended_config[exchange] = {
                'symbol': symbol,
                'amount': round(recommended_amount, 4)
            }
            
            print(f"   {exchange}: {symbol}, amount={recommended_amount:.4f}")
    
    # Save results
    results_file = "data/pre_flight_validation.json"
    with open(results_file, 'w') as f:
        json.dump({
            'validation_results': validation_results,
            'recommended_config': recommended_config,
            'summary': {
                'ready_exchanges': ready_count,
                'total_exchanges': len(validation_config),
                'total_issues': total_issues
            }
        }, f, indent=2, default=str)
    
    print(f"\n💾 Results saved to: {results_file}")
    
    if ready_count >= len(validation_config) * 0.6:  # 60% success rate
        print(f"\n🎉 READY TO PROCEED WITH COMPREHENSIVE TEST")
        return True
    else:
        print(f"\n⚠️  FIX ISSUES BEFORE RUNNING COMPREHENSIVE TEST")
        return False

if __name__ == "__main__":
    asyncio.run(validate_exchange_requirements()) 