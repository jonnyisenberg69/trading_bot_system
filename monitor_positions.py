#!/usr/bin/env python3

"""
Position monitoring script for the trading bot system.

Uses the existing PositionManager infrastructure to provide real-time
position monitoring and analysis across all connected exchanges.
"""

import asyncio
import json
import sys
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any
import argparse

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from order_management.tracking import PositionManager, Position
from config.settings import load_config
import structlog

# Set up logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="ISO"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger(__name__)

class PositionMonitor:
    """Real-time position monitoring and analysis."""
    
    def __init__(self, data_dir: str = "data/positions"):
        """Initialize position monitor."""
        self.position_manager = PositionManager(data_dir=data_dir)
        self.logger = logger.bind(component="PositionMonitor")
        
    async def start(self):
        """Start position monitoring."""
        await self.position_manager.start()
        self.logger.info("Position monitor started")
        
    async def stop(self):
        """Stop position monitoring."""
        await self.position_manager.stop()
        self.logger.info("Position monitor stopped")
        
    def print_position_summary(self):
        """Print comprehensive position summary."""
        summary = self.position_manager.summarize_positions()
        
        print("\n" + "="*60)
        print("🎯 POSITION SUMMARY")
        print("="*60)
        print(f"📊 Total Positions: {summary['total_positions']}")
        print(f"🟢 Open Positions: {summary['open_positions']}")
        print(f"📈 Symbols Tracked: {len(summary['symbols'])}")
        print(f"🏦 Active Exchanges: {len(summary['exchanges'])}")
        print(f"💰 Long Value: ${summary['long_value']:,.2f}")
        print(f"💸 Short Value: ${summary['short_value']:,.2f}")
        print(f"⚖️  Net Exposure: ${summary['net_exposure']:,.2f}")
        
        if summary['symbols']:
            print(f"\n📋 Tracked Symbols: {', '.join(summary['symbols'])}")
            
        print(f"\n🏢 Exchanges: {', '.join(summary['exchanges'])}")
        
        # Positions by exchange
        print(f"\n📊 Positions by Exchange:")
        for exchange, count in summary['positions_by_exchange'].items():
            print(f"   {exchange}: {count} positions")
    
    def print_detailed_positions(self, symbol: str = None):
        """Print detailed position information."""
        positions = self.position_manager.get_all_positions(symbol)
        
        if not positions:
            print(f"\n❌ No positions found" + (f" for {symbol}" if symbol else ""))
            return
            
        print(f"\n" + "="*80)
        print(f"📋 DETAILED POSITIONS" + (f" - {symbol}" if symbol else ""))
        print("="*80)
        
        # Group by symbol
        by_symbol = {}
        for pos in positions:
            if pos.symbol not in by_symbol:
                by_symbol[pos.symbol] = []
            by_symbol[pos.symbol].append(pos)
            
        for sym, symbol_positions in by_symbol.items():
            print(f"\n🪙 {sym}")
            print("-" * 40)
            
            for pos in symbol_positions:
                status = "🟢 OPEN" if pos.is_open else "🔴 CLOSED"
                side_emoji = "📈" if pos.side == "long" else "📉" if pos.side == "short" else "⚪"
                
                print(f"  {side_emoji} {pos.exchange}: {status}")
                print(f"     Size: {pos.size:,.4f}")
                if pos.avg_price:
                    print(f"     Avg Price: ${pos.avg_price:,.4f}")
                print(f"     Value: ${pos.value:,.2f}")
                if pos.entry_time:
                    print(f"     Entry: {pos.entry_time.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"     Updated: {pos.last_update_time.strftime('%Y-%m-%d %H:%M:%S')}")
                
    def print_net_positions(self):
        """Print net positions across all exchanges."""
        net_positions = self.position_manager.get_all_net_positions()
        
        if not net_positions:
            print(f"\n❌ No net positions found")
            return
            
        print(f"\n" + "="*60)
        print("⚖️  NET POSITIONS (Across All Exchanges)")
        print("="*60)
        
        total_long_value = 0
        total_short_value = 0
        
        for symbol, net_pos in net_positions.items():
            if not net_pos['is_open']:
                continue
                
            side_emoji = "📈" if net_pos['side'] == "long" else "📉" if net_pos['side'] == "short" else "⚪"
            
            print(f"\n{side_emoji} {symbol} ({net_pos['side'].upper() if net_pos['side'] else 'FLAT'})")
            print(f"   Net Size: {net_pos['size']:,.4f}")
            if net_pos['avg_price']:
                print(f"   Avg Price: ${net_pos['avg_price']:,.4f}")
            print(f"   Net Value: ${net_pos['value']:,.2f}")
            
            if net_pos['side'] == 'long':
                total_long_value += float(net_pos['value'])
            elif net_pos['side'] == 'short':
                total_short_value += abs(float(net_pos['value']))
                
        print(f"\n💰 Total Long: ${total_long_value:,.2f}")
        print(f"💸 Total Short: ${total_short_value:,.2f}")
        print(f"⚖️  Net Exposure: ${total_long_value - total_short_value:,.2f}")
        
    def export_positions_json(self, filename: str = None):
        """Export positions to JSON file."""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"positions_export_{timestamp}.json"
            
        # Collect all position data
        export_data = {
            'timestamp': datetime.now().isoformat(),
            'summary': self.position_manager.summarize_positions(),
            'net_positions': self.position_manager.get_all_net_positions(),
            'detailed_positions': []
        }
        
        # Add detailed positions
        for pos in self.position_manager.get_all_positions():
            export_data['detailed_positions'].append(pos.to_dict())
            
        # Save to file
        with open(filename, 'w') as f:
            json.dump(export_data, f, indent=2, default=str)
            
        print(f"✅ Positions exported to: {filename}")
        
    async def watch_positions(self, interval: int = 30):
        """Watch positions in real-time."""
        print(f"👀 Watching positions (refresh every {interval}s). Press Ctrl+C to stop.")
        
        try:
            while True:
                # Clear screen
                os.system('clear' if os.name == 'posix' else 'cls')
                
                print(f"⏰ Last Update: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                
                # Reload positions from disk
                await self.position_manager.load_positions()
                
                # Display current positions
                self.print_position_summary()
                self.print_net_positions()
                
                # Wait for next update
                await asyncio.sleep(interval)
                
        except KeyboardInterrupt:
            print(f"\n👋 Position monitoring stopped")

async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Trading Bot Position Monitor")
    parser.add_argument("--data-dir", default="data/positions", help="Position data directory")
    parser.add_argument("--symbol", help="Filter by specific symbol")
    parser.add_argument("--export", help="Export to JSON file")
    parser.add_argument("--watch", type=int, metavar="SECONDS", help="Watch mode with refresh interval")
    parser.add_argument("--summary", action="store_true", help="Show summary only")
    parser.add_argument("--detailed", action="store_true", help="Show detailed positions")
    parser.add_argument("--net", action="store_true", help="Show net positions only")
    
    args = parser.parse_args()
    
    # Create monitor
    monitor = PositionMonitor(data_dir=args.data_dir)
    
    try:
        await monitor.start()
        
        if args.watch:
            await monitor.watch_positions(args.watch)
        elif args.export:
            monitor.export_positions_json(args.export)
        else:
            # Default: show comprehensive view
            if args.summary or (not args.detailed and not args.net):
                monitor.print_position_summary()
                
            if args.net or (not args.detailed and not args.summary):
                monitor.print_net_positions()
                
            if args.detailed or (not args.summary and not args.net):
                monitor.print_detailed_positions(args.symbol)
                
    finally:
        await monitor.stop()

if __name__ == "__main__":
    asyncio.run(main()) 