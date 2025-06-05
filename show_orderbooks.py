#!/usr/bin/env python3
"""
Comprehensive orderbook monitoring and logging script.

Logs detailed orderbook data to the logs directory including:
- Individual exchange orderbooks
- Combined spot/perp/all orderbooks
- Bid/ask depth calculations
- Real-time market metrics
"""

import asyncio
import sys
import json
import os
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Any, Tuple
from decimal import Decimal

# Add project root to path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import load_config
from exchanges.connectors import create_exchange_connector
import structlog

# Setup logging
logger = structlog.get_logger(__name__)

# Create logs directory structure
LOGS_DIR = Path("logs/orderbook_monitoring")
LOGS_DIR.mkdir(parents=True, exist_ok=True)

class OrderbookMonitor:
    """Comprehensive orderbook monitoring and logging system."""
    
    def __init__(self):
        self.config = load_config()
        self.connectors = {}
        self.symbol = "BERA/USDT"
        self.log_file = LOGS_DIR / f"orderbook_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
        
    def _normalize_symbol_for_exchange(self, symbol: str, exchange: str) -> str:
        """Convert symbol to exchange-specific format."""
        base_exchange = exchange.split('_')[0]
        
        if symbol == 'BERA/USDT':
            if base_exchange == 'hyperliquid':
                return 'BERA/USDC:USDC'
        
        if 'perp' in exchange:
            if base_exchange in ['binance', 'bybit']:
                return 'BERAUSDT'
            elif base_exchange == 'hyperliquid':
                return symbol
        
        # Spot formatting
        if base_exchange == 'binance':
            return 'BERAUSDT'
        elif base_exchange == 'bybit':
            return 'BERA/USDT' if 'spot' in exchange else 'BERAUSDT'
        elif base_exchange == 'gateio':
            return 'BERA_USDT'
        elif base_exchange in ['mexc', 'bitget']:
            return 'BERA/USDT'
        else:
            return symbol
    
    def _calculate_depth(self, orders: List[List]) -> Dict[str, Any]:
        """Calculate bid/ask depth at multiple USD levels."""
        depth_levels = [1000, 5000, 10000, 25000, 50000]  # Multiple depth levels to check
        results = {}
        
        total_volume = 0
        total_value = 0
        
        # Calculate total available depth
        for price_str, amount_str in orders:
            price = float(price_str)
            amount = float(amount_str)
            value = price * amount
            total_volume += amount
            total_value += value
        
        # Calculate depth at each level
        for level in depth_levels:
            level_volume = 0
            level_value = 0
            level_orders = 0
            
            for price_str, amount_str in orders:
                price = float(price_str)
                amount = float(amount_str)
                value = price * amount
                
                if level_value + value <= level:
                    level_volume += amount
                    level_value += value
                    level_orders += 1
                else:
                    # Partial fill to reach the level
                    remaining_value = level - level_value
                    if remaining_value > 0:
                        partial_amount = remaining_value / price
                        level_volume += partial_amount
                        level_value = level
                        level_orders += 1
                    break
            
            results[f'depth_{level}'] = {
                'volume': level_volume,
                'value_usd': level_value,
                'orders_count': level_orders,
                'available': level_value >= level  # True if we have at least this much depth
            }
        
        # Add total available depth
        results['total_available'] = {
            'volume': total_volume,
            'value_usd': total_value,
            'orders_count': len(orders)
        }
        
        # Return the most commonly used depth (5k) as the main metric for backward compatibility
        main_depth = results.get('depth_5000', results['total_available'])
        return {
            'volume': main_depth['volume'],
            'value_usd': main_depth['value_usd'],
            'levels': main_depth['orders_count'],
            'all_depths': results
        }
    
    def _combine_orderbooks(self, orderbooks: Dict[str, Dict], market_type: str = None) -> Dict[str, Any]:
        """Combine multiple orderbooks into a single aggregated view."""
        combined_bids = []
        combined_asks = []
        
        # Filter by market type if specified
        filtered_books = {}
        for exchange, data in orderbooks.items():
            if market_type:
                if market_type == 'spot' and 'spot' in exchange:
                    filtered_books[exchange] = data
                elif market_type == 'perp' and 'perp' in exchange:
                    filtered_books[exchange] = data
            else:
                filtered_books[exchange] = data
        
        # Aggregate all bids and asks
        for exchange, data in filtered_books.items():
            if data and data.get('orderbook'):
                ob = data['orderbook']
                if ob.get('bids'):
                    for price, amount in ob['bids']:
                        combined_bids.append([float(price), float(amount), exchange])
                if ob.get('asks'):
                    for price, amount in ob['asks']:
                        combined_asks.append([float(price), float(amount), exchange])
        
        # Sort bids (highest first) and asks (lowest first)
        combined_bids.sort(key=lambda x: x[0], reverse=True)
        combined_asks.sort(key=lambda x: x[0])
        
        if not combined_bids or not combined_asks:
            return None
        
        # Calculate metrics
        best_bid = combined_bids[0][0]
        best_ask = combined_asks[0][0]
        midpoint = (best_bid + best_ask) / 2
        spread = best_ask - best_bid
        spread_bps = (spread / midpoint) * 10000
        
        # Calculate depths
        bid_depth = self._calculate_depth([[str(p), str(a)] for p, a, _ in combined_bids])
        ask_depth = self._calculate_depth([[str(p), str(a)] for p, a, _ in combined_asks])
        
        return {
            'best_bid': best_bid,
            'best_ask': best_ask,
            'midpoint': midpoint,
            'spread': spread,
            'spread_bps': spread_bps,
            'bid_depth_usd': bid_depth['value_usd'],
            'ask_depth_usd': ask_depth['value_usd'],
            'bid_volume': bid_depth['volume'],
            'ask_volume': ask_depth['volume'],
            'exchanges_count': len(filtered_books),
            'top_bids': combined_bids[:5],
            'top_asks': combined_asks[:5]
        }
    
    async def connect_exchanges(self):
        """Connect to all configured exchanges."""
        print("📡 Connecting to exchanges...")
        
        for exchange_config in self.config.get('exchanges', []):
            exchange_name = exchange_config['name']
            exchange_type = exchange_config['type']
            full_name = f"{exchange_name}_{exchange_type}"
            
            try:
                print(f"  Connecting to {full_name}...")
                
                connector = create_exchange_connector(
                    exchange_name, 
                    {
                        'api_key': exchange_config.get('api_key', ''),
                        'secret': exchange_config.get('api_secret', ''),
                        'wallet_address': exchange_config.get('wallet_address', ''),
                        'private_key': exchange_config.get('private_key', ''),
                        'passphrase': exchange_config.get('passphrase', ''),
                        'sandbox': exchange_config.get('testnet', True),
                        'market_type': 'future' if exchange_config.get('type') == 'perp' else exchange_config.get('type', 'spot'),
                        'testnet': exchange_config.get('testnet', True)
                    }
                )
                
                if connector:
                    connected = await connector.connect()
                    if connected:
                        self.connectors[full_name] = connector
                        print(f"  ✅ {full_name} connected")
                    else:
                        print(f"  ❌ {full_name} failed to connect")
                else:
                    print(f"  ❌ {full_name} failed to create connector")
                    
            except Exception as e:
                print(f"  ❌ {full_name} error: {e}")
        
        print(f"📊 Connected to {len(self.connectors)} exchanges")
        return len(self.connectors) > 0
    
    async def fetch_orderbook_data(self) -> Dict[str, Any]:
        """Fetch orderbook data from all connected exchanges."""
        orderbooks = {}
        timestamp = datetime.now(timezone.utc)
        
        for exchange, connector in self.connectors.items():
            try:
                # Get exchange-specific symbol
                exchange_symbol = self._normalize_symbol_for_exchange(self.symbol, exchange)
                
                # Fetch orderbook with more depth for better analysis
                orderbook = await connector.get_orderbook(exchange_symbol, 20)
                
                if orderbook and orderbook.get('bids') and orderbook.get('asks'):
                    bids = orderbook['bids']
                    asks = orderbook['asks']
                    
                    # Calculate basic metrics
                    best_bid = float(bids[0][0])
                    best_ask = float(asks[0][0])
                    midpoint = (best_bid + best_ask) / 2
                    spread = best_ask - best_bid
                    spread_bps = (spread / midpoint) * 10000
                    
                    # Calculate depths
                    bid_depth = self._calculate_depth(bids)
                    ask_depth = self._calculate_depth(asks)
                    
                    orderbooks[exchange] = {
                        'timestamp': timestamp.isoformat(),
                        'symbol': exchange_symbol,
                        'best_bid': best_bid,
                        'best_ask': best_ask,
                        'midpoint': midpoint,
                        'spread': spread,
                        'spread_bps': spread_bps,
                        'bid_depth_usd': bid_depth['value_usd'],
                        'ask_depth_usd': ask_depth['value_usd'],
                        'bid_volume': bid_depth['volume'],
                        'ask_volume': ask_depth['volume'],
                        'bid_all_depths': bid_depth['all_depths'],
                        'ask_all_depths': ask_depth['all_depths'],
                        'orderbook': orderbook
                    }
                    
            except Exception as e:
                print(f"❌ Error fetching {exchange} orderbook: {e}")
        
        return orderbooks
    
    def log_data(self, data: Dict[str, Any]):
        """Log data to file and display summary."""
        # Write to log file
        with open(self.log_file, 'a') as f:
            f.write(json.dumps(data, default=str) + '\n')
        
        # Display summary
        timestamp = data['timestamp']
        individual = data['individual_exchanges']
        combined_all = data['combined_all']
        combined_spot = data['combined_spot']
        combined_perp = data['combined_perp']
        
        print(f"\n🕐 {timestamp}")
        print("=" * 80)
        
        # Individual exchanges summary with detailed depth info
        print(f"📊 INDIVIDUAL EXCHANGES ({len(individual)} active):")
        for exchange, metrics in individual.items():
            if metrics:
                # Get depth details if available
                bid_depth_str = f"${metrics['bid_depth_usd']:6.0f}"
                ask_depth_str = f"${metrics['ask_depth_usd']:6.0f}"
                
                # Add indicators for limited depth
                if 'all_depths' in metrics:
                    bid_depths = metrics.get('bid_all_depths', {})
                    ask_depths = metrics.get('ask_all_depths', {})
                    
                    # Check if we have less than 5k depth available
                    bid_total = bid_depths.get('total_available', {}).get('value_usd', 0)
                    ask_total = ask_depths.get('total_available', {}).get('value_usd', 0)
                    
                    if bid_total < 5000:
                        bid_depth_str = f"${bid_total:6.0f}*"  # * indicates limited depth
                    if ask_total < 5000:
                        ask_depth_str = f"${ask_total:6.0f}*"
                
                print(f"  {exchange:15} | ${metrics['best_bid']:8.4f} / ${metrics['best_ask']:8.4f} | "
                      f"Mid: ${metrics['midpoint']:8.4f} | Spread: {metrics['spread_bps']:5.1f}bps | "
                      f"Depth: {bid_depth_str}/{ask_depth_str}")
        
        # Combined orderbooks
        print(f"\n🔄 COMBINED ORDERBOOKS:")
        if combined_all:
            print(f"  ALL EXCHANGES   | ${combined_all['best_bid']:8.4f} / ${combined_all['best_ask']:8.4f} | "
                  f"Mid: ${combined_all['midpoint']:8.4f} | Spread: {combined_all['spread_bps']:5.1f}bps | "
                  f"Depth: ${combined_all['bid_depth_usd']:6.0f}/${combined_all['ask_depth_usd']:6.0f} | "
                  f"Exchanges: {combined_all['exchanges_count']}")
        
        if combined_spot:
            print(f"  SPOT ONLY       | ${combined_spot['best_bid']:8.4f} / ${combined_spot['best_ask']:8.4f} | "
                  f"Mid: ${combined_spot['midpoint']:8.4f} | Spread: {combined_spot['spread_bps']:5.1f}bps | "
                  f"Depth: ${combined_spot['bid_depth_usd']:6.0f}/${combined_spot['ask_depth_usd']:6.0f} | "
                  f"Exchanges: {combined_spot['exchanges_count']}")
        
        if combined_perp:
            print(f"  PERP ONLY       | ${combined_perp['best_bid']:8.4f} / ${combined_perp['best_ask']:8.4f} | "
                  f"Mid: ${combined_perp['midpoint']:8.4f} | Spread: {combined_perp['spread_bps']:5.1f}bps | "
                  f"Depth: ${combined_perp['bid_depth_usd']:6.0f}/${combined_perp['ask_depth_usd']:6.0f} | "
                  f"Exchanges: {combined_perp['exchanges_count']}")
        
        # Show detailed depth analysis every 10 iterations
        if data.get('iteration', 0) % 10 == 0:
            print(f"\n📊 DETAILED DEPTH ANALYSIS (every 10 iterations):")
            print("-" * 60)
            for exchange, metrics in individual.items():
                if metrics and 'all_depths' in metrics:
                    bid_depths = metrics.get('bid_all_depths', {})
                    ask_depths = metrics.get('ask_all_depths', {})
                    
                    print(f"\n  {exchange}:")
                    print(f"    BID DEPTH: $1k:{bid_depths.get('depth_1000', {}).get('value_usd', 0):6.0f} | "
                          f"$5k:{bid_depths.get('depth_5000', {}).get('value_usd', 0):6.0f} | "
                          f"$10k:{bid_depths.get('depth_10000', {}).get('value_usd', 0):6.0f} | "
                          f"Total:${bid_depths.get('total_available', {}).get('value_usd', 0):6.0f}")
                    print(f"    ASK DEPTH: $1k:{ask_depths.get('depth_1000', {}).get('value_usd', 0):6.0f} | "
                          f"$5k:{ask_depths.get('depth_5000', {}).get('value_usd', 0):6.0f} | "
                          f"$10k:{ask_depths.get('depth_10000', {}).get('value_usd', 0):6.0f} | "
                          f"Total:${ask_depths.get('total_available', {}).get('value_usd', 0):6.0f}")
    
    async def monitor_continuous(self, interval: int = 5):
        """Continuously monitor and log orderbook data."""
        print(f"🔍 STARTING CONTINUOUS ORDERBOOK MONITORING")
        print(f"📁 Logging to: {self.log_file}")
        print(f"⏱️  Update interval: {interval} seconds")
        print(f"💱 Symbol: {self.symbol}")
        print("=" * 80)
        
        iteration = 0
        while True:
            try:
                iteration += 1
                
                # Fetch orderbook data
                orderbooks = await self.fetch_orderbook_data()
                
                if not orderbooks:
                    print("❌ No orderbook data available")
                    await asyncio.sleep(interval)
                    continue
                
                # Create combined orderbooks
                combined_all = self._combine_orderbooks(orderbooks)
                combined_spot = self._combine_orderbooks(orderbooks, 'spot')
                combined_perp = self._combine_orderbooks(orderbooks, 'perp')
                
                # Prepare log data
                log_data = {
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'iteration': iteration,
                    'symbol': self.symbol,
                    'individual_exchanges': {k: {
                        'best_bid': v['best_bid'],
                        'best_ask': v['best_ask'],
                        'midpoint': v['midpoint'],
                        'spread': v['spread'],
                        'spread_bps': v['spread_bps'],
                        'bid_depth_usd': v['bid_depth_usd'],
                        'ask_depth_usd': v['ask_depth_usd'],
                        'bid_volume': v['bid_volume'],
                        'ask_volume': v['ask_volume'],
                        'bid_all_depths': v.get('bid_all_depths', {}),  # Include detailed depth
                        'ask_all_depths': v.get('ask_all_depths', {}),  # Include detailed depth
                    } for k, v in orderbooks.items()},
                    'combined_all': combined_all,
                    'combined_spot': combined_spot,
                    'combined_perp': combined_perp
                }
                
                # Log and display
                self.log_data(log_data)
                
                await asyncio.sleep(interval)
                
            except KeyboardInterrupt:
                print("\n🛑 Monitoring stopped by user")
                break
            except Exception as e:
                print(f"❌ Error in monitoring loop: {e}")
                await asyncio.sleep(interval)
    
    async def cleanup(self):
        """Close all connections."""
        for connector in self.connectors.values():
            try:
                await connector.close()
            except:
                pass


async def main():
    """Main monitoring function."""
    monitor = OrderbookMonitor()
    
    try:
        # Connect to exchanges
        connected = await monitor.connect_exchanges()
        if not connected:
            print("❌ No exchanges connected. Exiting.")
            return
        
        # Start continuous monitoring
        await monitor.monitor_continuous(interval=5)
        
    finally:
        await monitor.cleanup()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Orderbook monitoring stopped") 