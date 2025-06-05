#!/usr/bin/env python3
"""
Show the strategy's WebSocket orderbook data.

This script accesses the running strategy's WebSocket orderbook data 
to show the full orderbook depth that the system is using for trading decisions.
"""

import asyncio
import sys
import json
import time
import requests
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

import structlog

logger = structlog.get_logger(__name__)


class WebSocketOrderbookMonitor:
    """
    Monitor that shows the strategy's WebSocket orderbook data.
    
    This accesses the strategy's WebSocket orderbook data to show
    the full orderbook depth being used for trading decisions.
    """
    
    def __init__(self, api_base_url: str = "http://localhost:8000"):
        self.api_base_url = api_base_url
        self.session = requests.Session()
        self.session.timeout = 10
        
    def get_running_strategies(self) -> List[Dict[str, Any]]:
        """Get list of running strategies."""
        try:
            response = self.session.get(f"{self.api_base_url}/api/bots/")
            response.raise_for_status()
            data = response.json()
            
            running_strategies = []
            for bot_id, bot_info in data.get('bots', {}).items():
                if bot_info.get('status') == 'running':
                    running_strategies.append({
                        'instance_id': bot_id,
                        'symbol': bot_info.get('symbol'),
                        'strategy_type': bot_info.get('strategy_type'),
                        'exchanges': bot_info.get('exchanges', []),
                        'uptime': bot_info.get('uptime_seconds', 0)
                    })
            
            return running_strategies
            
        except Exception as e:
            logger.error(f"Error getting running strategies: {e}")
            return []
    
    def get_strategy_details(self, instance_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed strategy information including WebSocket orderbook data."""
        try:
            response = self.session.get(f"{self.api_base_url}/api/bots/{instance_id}")
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            logger.error(f"Error getting strategy details for {instance_id}: {e}")
            return None
    
    def display_strategy_orderbooks(self, strategy_details: Dict[str, Any]) -> None:
        """Display the strategy's WebSocket orderbook data."""
        instance_id = strategy_details.get('instance_id', 'unknown')
        symbol = strategy_details.get('symbol', 'unknown')
        
        print(f"\n🎯 STRATEGY: {instance_id}")
        print(f"   Symbol: {symbol}")
        print(f"   Status: {strategy_details.get('status')}")
        
        uptime = strategy_details.get('uptime_seconds')
        if uptime is not None:
            print(f"   Uptime: {uptime:.1f}s")
        else:
            print(f"   Uptime: N/A")
        
        print(f"   Exchanges: {len(strategy_details.get('exchanges', []))}")
        
        # Display WebSocket orderbook data
        websocket_orderbooks = strategy_details.get('websocket_orderbooks', {})
        if not websocket_orderbooks:
            print("\n❌ No WebSocket orderbook data available")
            return
        
        print(f"\n📊 WEBSOCKET ORDERBOOK DATA ({symbol})")
        print("=" * 80)
        
        for exchange, orderbook_data in websocket_orderbooks.items():
            self._display_exchange_orderbook(exchange, orderbook_data, symbol)
        
        # Display strategy line status if available
        line_status = strategy_details.get('line_status', [])
        if line_status:
            print(f"\n📈 QUOTE LINES STATUS")
            print("=" * 80)
            for line in line_status:
                self._display_line_status(line)
    
    def _display_exchange_orderbook(self, exchange: str, data: Dict[str, Any], symbol: str) -> None:
        """Display orderbook data for a specific exchange."""
        print(f"\n🏢 {exchange.upper()}")
        print("-" * 40)
        
        best_bid = data.get('best_bid')
        best_ask = data.get('best_ask')
        midpoint = data.get('midpoint')
        last_update = data.get('last_update')
        bids_count = data.get('bids_count', 0)
        asks_count = data.get('asks_count', 0)
        
        if best_bid is None or best_ask is None:
            print("   ❌ No orderbook data available")
            return
        
        # Calculate spread
        spread = best_ask - best_bid
        spread_bps = (spread / midpoint * 10000) if midpoint else 0
        
        print(f"   Best Bid:    ${best_bid:.6f}")
        print(f"   Best Ask:    ${best_ask:.6f}")
        print(f"   Midpoint:    ${midpoint:.6f}")
        print(f"   Spread:      ${spread:.6f} ({spread_bps:.2f} bps)")
        print(f"   Depth:       {bids_count} bids / {asks_count} asks")
        
        if last_update:
            try:
                update_time = datetime.fromisoformat(last_update.replace('Z', '+00:00'))
                age_seconds = (datetime.now(timezone.utc) - update_time).total_seconds()
                print(f"   Last Update: {age_seconds:.1f}s ago")
            except:
                print(f"   Last Update: {last_update}")
        else:
            print(f"   Last Update: Never")
    
    def _display_line_status(self, line: Dict[str, Any]) -> None:
        """Display status of a quote line."""
        line_id = line.get('line_id', 'unknown')
        sides = line.get('sides', 'unknown')
        spread_bps = line.get('spread_bps', 0)
        bid_active = line.get('bid_active', 0)
        ask_active = line.get('ask_active', 0)
        
        print(f"   Line {line_id}: {sides} @ {spread_bps:.1f}bps")
        print(f"     - Bid active: {bid_active}, Ask active: {ask_active}")
        
        bid_exchanges = line.get('bid_exchanges', [])
        ask_exchanges = line.get('ask_exchanges', [])
        if bid_exchanges:
            print(f"     - Bid exchanges: {', '.join(bid_exchanges)}")
        if ask_exchanges:
            print(f"     - Ask exchanges: {', '.join(ask_exchanges)}")
    
    def run_monitor(self) -> None:
        """Run the orderbook monitor."""
        print("🔍 WebSocket Orderbook Monitor")
        print("=" * 50)
        
        # Get running strategies
        strategies = self.get_running_strategies()
        
        if not strategies:
            print("❌ No running strategies found")
            return
        
        print(f"✅ Found {len(strategies)} running strategies")
        
        for strategy in strategies:
            instance_id = strategy['instance_id']
            
            # Get detailed strategy information
            details = self.get_strategy_details(instance_id)
            if details:
                self.display_strategy_orderbooks(details)
            else:
                print(f"\n❌ Could not get details for strategy {instance_id}")
        
        print(f"\n✅ Monitor completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


def main():
    """Main function."""
    monitor = WebSocketOrderbookMonitor()
    
    try:
        monitor.run_monitor()
    except KeyboardInterrupt:
        print("\n\n👋 Monitor stopped by user")
    except Exception as e:
        print(f"\n❌ Error running monitor: {e}")
        logger.error(f"Monitor error: {e}", exc_info=True)


if __name__ == "__main__":
    main() 