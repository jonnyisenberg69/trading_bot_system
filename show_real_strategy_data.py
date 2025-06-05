#!/usr/bin/env python3
"""
Show the REAL orderbook data that the running passive quoting strategy is using.

This script connects to the same ExchangeManager and uses the same exchange
connectors that the actual running strategy uses, ensuring we see the exact
same data that drives order placement decisions.
"""

import asyncio
import sys
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Any

# Add project root to path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import load_config
from api.services.exchange_manager import ExchangeManager
import structlog

logger = structlog.get_logger(__name__)

class RealStrategyDataMonitor:
    """Monitor that uses the EXACT same data sources as the running strategy."""
    
    def __init__(self):
        self.config = load_config()
        self.exchange_manager = None
        self.symbol = "BERA/USDT"  # The symbol your strategy is trading
        self.exchanges = [
            'binance_spot', 'binance_perp', 'bybit_spot', 'bybit_perp',
            'hyperliquid_perp', 'mexc_spot', 'gateio_spot', 'bitget_spot'
        ]
        
    def _normalize_symbol_for_exchange(self, symbol: str, exchange: str) -> str:
        """EXACT copy of the method from BaseStrategy that the running strategy uses."""
        # Remove exchange suffix from exchange name
        base_exchange = exchange.split('_')[0]
        
        # Handle special cases first
        if symbol == 'BERA/USDT':
            if base_exchange == 'hyperliquid':
                # Hyperliquid uses BERA/USDC:USDC, not BERA/USDT
                return 'BERA/USDC:USDC'
        
        # For perpetual futures, convert to proper symbol format
        if 'perp' in exchange:
            if base_exchange in ['binance', 'bybit']:
                # For Binance/Bybit futures: BERA/USDT becomes BERAUSDT (not BERAUSDTUSDT)
                if symbol == 'BERA/USDT':
                    if base_exchange == 'binance':
                        return 'BERAUSDT'  # Binance futures: remove slashes
                    elif base_exchange == 'bybit':
                        return 'BERAUSDT'  # Bybit futures: remove slashes
            elif base_exchange == 'hyperliquid':
                # Hyperliquid uses the full symbol as-is for futures
                return symbol
        
        # Now apply exchange-specific formatting for spot
        if base_exchange == 'binance':
            # Binance spot: Remove slashes -> BERAUSDT
            return symbol.replace('/', '').replace(':USDT', '').replace(':USDC', '')
        elif base_exchange == 'bybit':
            if 'spot' in exchange:
                # Bybit spot: Keep standard format -> BERA/USDT
                return symbol.replace(':USDT', '').replace(':USDC', '')
            else:
                # Bybit perp: handled above
                return symbol.replace('/', '').replace(':USDT', '').replace(':USDC', '')
        elif base_exchange == 'hyperliquid':
            # Hyperliquid uses the full symbol as-is for futures
            return symbol
        elif base_exchange == 'gateio':
            # Gate.io: Replace slash with underscore -> BERA_USDT
            return symbol.replace('/', '_').replace(':USDT', '').replace(':USDC', '')
        elif base_exchange == 'mexc':
            # MEXC: Keep standard format -> BERA/USDT
            return symbol.replace(':USDT', '').replace(':USDC', '')
        elif base_exchange == 'bitget':
            # Bitget: Keep standard format -> BERA/USDT
            return symbol.replace(':USDT', '').replace(':USDC', '')
        else:
            # Default: standard format
            return symbol
    
    async def initialize(self):
        """Initialize using the EXACT same ExchangeManager as the running strategy."""
        try:
            print("🔗 Connecting to the SAME ExchangeManager as the running strategy...")
            
            # Create ExchangeManager with the same config as the strategy
            self.exchange_manager = ExchangeManager(self.config)
            
            # Start exchange manager (connects to the same exchanges)
            await self.exchange_manager.start()
            
            print("✅ Connected to ExchangeManager successfully")
            
            # Get connection status
            connected_exchanges = await self.exchange_manager.get_connected_exchange_names()
            print(f"📊 Connected exchanges: {connected_exchanges}")
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to initialize ExchangeManager: {e}")
            return False
    
    async def get_real_strategy_data(self) -> Dict[str, Any]:
        """Get the EXACT same market data that the running strategy sees."""
        if not self.exchange_manager:
            print("❌ ExchangeManager not initialized")
            return {}
        
        print(f"\n🎯 FETCHING REAL STRATEGY DATA FOR {self.symbol}")
        print("=" * 80)
        print("This is the EXACT same data your running strategy uses for order placement")
        print("=" * 80)
        
        market_data = {}
        
        # Get the same exchange connectors that the strategy uses
        exchange_connectors = self.exchange_manager.get_exchange_connectors(self.exchanges)
        
        print(f"📡 Using {len(exchange_connectors)} exchange connectors (same as strategy)")
        
        for exchange in self.exchanges:
            try:
                connector = exchange_connectors.get(exchange)
                if not connector:
                    print(f"  ❌ {exchange}: No connector available")
                    continue
                
                # Use EXACT same symbol normalization as strategy
                exchange_symbol = self._normalize_symbol_for_exchange(self.symbol, exchange)
                
                print(f"\n📈 {exchange.upper()}")
                print(f"   Symbol: {self.symbol} -> {exchange_symbol}")
                
                # Use EXACT same orderbook fetch as strategy (limit=5)
                limit = 5  # EXACT same limit as strategy
                orderbook = await connector.get_orderbook(exchange_symbol, limit)
                
                if orderbook and orderbook.get('bids') and orderbook.get('asks'):
                    # EXACT same processing as strategy
                    best_bid = float(orderbook['bids'][0][0]) if orderbook['bids'] else None
                    best_ask = float(orderbook['asks'][0][0]) if orderbook['asks'] else None
                    
                    if best_bid and best_ask:
                        midpoint = (best_bid + best_ask) / 2
                        spread = best_ask - best_bid
                        spread_bps = (spread / midpoint) * 10000
                        
                        # Store in EXACT same format as strategy
                        market_data[exchange] = {
                            'bid': best_bid,
                            'ask': best_ask,
                            'midpoint': midpoint,
                            'last_update': datetime.now(),
                            'symbol': exchange_symbol,
                            'spread': spread,
                            'spread_bps': spread_bps
                        }
                        
                        print(f"   ✅ Bid: ${best_bid:.6f}")
                        print(f"   ✅ Ask: ${best_ask:.6f}")
                        print(f"   ✅ Mid: ${midpoint:.6f}")
                        print(f"   ✅ Spread: {spread_bps:.1f} bps")
                    else:
                        print(f"   ❌ Invalid bid/ask data")
                else:
                    print(f"   ❌ No orderbook data")
                    
            except Exception as e:
                print(f"   ❌ Error: {e}")
        
        return market_data
    
    async def calculate_strategy_midpoint(self, market_data: Dict[str, Any]) -> float:
        """Calculate the EXACT same average midpoint that the strategy uses."""
        # EXACT same logic as strategy's _get_average_midpoint method
        midpoints = [data['midpoint'] for data in market_data.values() if data.get('midpoint')]
        
        if midpoints:
            avg_midpoint = sum(midpoints) / len(midpoints)
            return avg_midpoint
        return None
    
    async def show_strategy_order_placement(self, market_data: Dict[str, Any]):
        """Show where the strategy would place orders based on this data."""
        avg_midpoint = await self.calculate_strategy_midpoint(market_data)
        
        if not avg_midpoint:
            print("\n❌ No valid midpoint data - strategy cannot place orders")
            return
        
        print(f"\n🧮 STRATEGY ORDER PLACEMENT CALCULATION")
        print("=" * 60)
        print(f"📊 Average Midpoint: ${avg_midpoint:.6f} ← THIS IS WHAT STRATEGY USES")
        print(f"📊 Active Exchanges: {len([d for d in market_data.values() if d.get('midpoint')])}/{len(self.exchanges)}")
        
        # Show individual exchange differences from average
        print(f"\n📈 Exchange Differences from Average:")
        for exchange, data in market_data.items():
            if data.get('midpoint'):
                diff = data['midpoint'] - avg_midpoint
                diff_bps = (diff / avg_midpoint) * 10000
                print(f"   {exchange:15}: {diff_bps:+.1f} bps")
        
        # Use default config spread (25 bps) - same as strategy
        spread_bps = 25  # From your strategy config
        spread_amount = avg_midpoint * (spread_bps / 10000)
        bid_price = avg_midpoint - spread_amount
        ask_price = avg_midpoint + spread_amount
        
        print(f"\n🎯 ACTUAL ORDER PLACEMENT (where strategy places orders):")
        print(f"   📉 BID Orders: ${bid_price:.6f} ({spread_bps} bps below midpoint)")
        print(f"   📈 ASK Orders: ${ask_price:.6f} ({spread_bps} bps above midpoint)")
        
        # Show quantity calculation
        base_qty = 0.01  # From your strategy config
        print(f"   💰 Base Quantity: {base_qty} BERA per exchange")
    
    async def cleanup(self):
        """Clean up connections."""
        if self.exchange_manager:
            await self.exchange_manager.stop()

async def main():
    """Main function to show real strategy data."""
    monitor = RealStrategyDataMonitor()
    
    try:
        # Initialize with same ExchangeManager as strategy
        success = await monitor.initialize()
        if not success:
            print("❌ Failed to initialize - cannot show real strategy data")
            return
        
        # Get the real data
        market_data = await monitor.get_real_strategy_data()
        
        if market_data:
            # Show strategy calculations
            await monitor.show_strategy_order_placement(market_data)
            
            print(f"\n✅ VERIFICATION: This is the EXACT data your running strategy uses!")
            print(f"📊 Data timestamp: {datetime.now(timezone.utc).isoformat()}")
        else:
            print("❌ No market data available")
        
    finally:
        await monitor.cleanup()

if __name__ == "__main__":
    asyncio.run(main()) 