#!/usr/bin/env python
"""
Minimum Notional Requirements Verification

Verifies that configured order amounts meet exchange minimum requirements.
"""

def verify_notional_requirements():
    """Verify that our configured amounts meet minimum notional requirements."""
    print("💰 MINIMUM NOTIONAL REQUIREMENTS VERIFICATION")
    print("="*60)
    
    # Exchange minimum notional requirements (USD)
    min_notionals = {
        'binance_spot': 5.0,     # $5 minimum
        'binance_perp': 5.0,     # $5 minimum  
        'bybit_spot': 1.0,       # $1 minimum
        'bybit_perp': 1.0,       # $1 minimum
        'mexc_spot': 1.0,        # $1 minimum
        'gateio_spot': 1.0,      # $1 minimum
        'bitget_spot': 1.0,      # $1 minimum
        'hyperliquid_perp': 10.0 # $10 minimum
    }
    
    # Our configured amounts
    configured_amounts = {
        'binance_spot': 2.5,     # BERA amount
        'binance_perp': 2.5,     # BERA amount
        'bybit_spot': 2.0,       # BERA amount
        'bybit_perp': 2.0,       # BERA amount
        'mexc_spot': 2.0,        # BERA amount
        'gateio_spot': 2.0,      # BERA amount
        'bitget_spot': 2.0,      # BERA amount
        'hyperliquid_perp': 4.0  # BERA amount
    }
    
    # Current BERA price (approximate)
    bera_price = 2.60  # USD per BERA
    
    print(f"💱 Using BERA price: ${bera_price:.2f}")
    print(f"📊 Verifying {len(configured_amounts)} exchanges...\n")
    
    all_pass = True
    
    for exchange, bera_amount in configured_amounts.items():
        min_required = min_notionals[exchange]
        notional_value = bera_amount * bera_price
        
        status = "✅ PASS" if notional_value >= min_required else "❌ FAIL"
        margin = notional_value - min_required
        
        print(f"{exchange:15} | {bera_amount:4.1f} BERA × ${bera_price:.2f} = ${notional_value:5.2f} | Min: ${min_required:4.1f} | {status} ({margin:+5.2f})")
        
        if notional_value < min_required:
            all_pass = False
    
    print(f"\n🎯 SUMMARY:")
    if all_pass:
        print(f"   ✅ ALL EXCHANGES PASS - Minimum notional requirements met!")
        print(f"   🚀 Ready for trading tests!")
    else:
        print(f"   ❌ Some exchanges fail minimum requirements")
        print(f"   📝 Increase amounts for failing exchanges")
    
    return all_pass

if __name__ == "__main__":
    verify_notional_requirements() 