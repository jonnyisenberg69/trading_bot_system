"""
Position monitoring system for tracking trades across all bot instances.

Tracks:
- P1: Change in base coin (positive for buying base, negative for selling)
- P2: Change in quote coin (positive for selling, negative for buying)
- P1 fee: Absolute value of fee charged in base coin
- P2 fee: Absolute value of fee charged in quote coin
- Average price: -(p2+p2 fee)/(p1 + p1 fee)

Now uses PostgreSQL as single source of truth for trade data.
Positions are calculated on-demand from database trades.
"""

import asyncio
import json
import logging
from typing import Dict, List, Optional, Any, Union, Set
from decimal import Decimal
from datetime import datetime
import uuid
from pathlib import Path
import os
import structlog
from collections import defaultdict

from .order import Order, OrderStatus, OrderSide
from utils.trade_sync_logger import setup_trade_sync_logger, log_position_update

logger = structlog.get_logger(__name__)


class Position:
    """
    Position tracking for a specific symbol on a specific exchange.
    
    Tracks base and quote amounts, fees, and average price.
    """
    
    def __init__(self, exchange: str, symbol: str, is_perpetual: bool = False):
        """
        Initialize a position tracker.
        
        Args:
            exchange: Exchange name
            symbol: Trading symbol (e.g., BTC/USDT)
            is_perpetual: Whether this is a perpetual futures position
        """
        self.exchange = exchange
        self.symbol = symbol
        self.is_perpetual = is_perpetual
        
        # Position tracking
        self.p1 = Decimal('0')  # Base coin amount (positive for long, negative for short)
        self.p2 = Decimal('0')  # Quote coin amount (positive for selling base, negative for buying)
        self.p1_fee = Decimal('0')  # Fee in base coin (absolute value)
        self.p2_fee = Decimal('0')  # Fee in quote coin (absolute value)
        
        # Additional metadata
        self.last_update_time = datetime.now()
        self.trade_count = 0  # Count of trades that contributed to this position
        self.entry_time = None  # When position was first opened
        
    @property
    def size(self) -> Decimal:
        """Get position size in base currency."""
        return self.p1
    
    @property
    def value(self) -> Decimal:
        """Get position value in quote currency."""
        return -self.p2
    
    @property
    def avg_price(self) -> Optional[Decimal]:
        """
        Calculate average price for the position.
        
        Formula: -(p2+p2 fee)/(p1 + p1 fee) if position size is non-zero
        """
        denominator = self.p1
        
        if denominator == Decimal('0'):
            return None
            
        return -(self.p2 + self.p2_fee) / self.p1
    
    @property
    def total_fee_base(self) -> Decimal:
        """Get total fees paid in base currency."""
        return self.p1_fee
    
    @property
    def total_fee_quote(self) -> Decimal:
        """Get total fees paid in quote currency."""
        return self.p2_fee
    
    @property
    def is_open(self) -> bool:
        """Check if position is currently open."""
        return self.p1 != Decimal('0')
    
    @property
    def side(self) -> Optional[str]:
        """Get position side (long or short)."""
        if self.p1 > Decimal('0'):
            return 'long'
        elif self.p1 < Decimal('0'):
            return 'short'
        else:
            return None
    
    def update_from_trade(self, trade: Dict[str, Any]) -> None:
        """
        Update position based on a trade.
        
        Args:
            trade: Trade data dictionary with:
                - side: 'buy' or 'sell'
                - amount: Base currency amount
                - price: Execution price
                - cost: Quote currency amount (price * amount)
                - fee: Fee information (can be in base or quote currency)
        """
        try:
            side = trade.get('side', '').lower()
            amount = Decimal(str(trade.get('amount', 0)))
            price = Decimal(str(trade.get('price', 0)))
            cost = Decimal(str(trade.get('cost', price * amount)))
            
            # Extract fee information
            fee_cost = Decimal('0')
            fee_currency = ''
            
            if 'fee' in trade and trade['fee']:
                fee_cost = Decimal(str(trade['fee'].get('cost', 0)))
                fee_currency = trade['fee'].get('currency', '')
            
            # Calculate P1 (base currency) change
            if side == 'buy':
                self.p1 += amount
                self.p2 -= cost
            else:  # sell
                self.p1 -= amount
                self.p2 += cost
                
            # Update fees based on fee currency (only if symbol has '/')
            if '/' in self.symbol:
                base_currency, quote_currency = self.symbol.split('/')
                
                if fee_currency == base_currency:
                    self.p1_fee += fee_cost
                elif fee_currency == quote_currency:
                    self.p2_fee += fee_cost
                else:
                    # Fee in different currency, try to estimate in quote currency
                    self.p2_fee += fee_cost  # This is an approximation
            else:
                # If symbol doesn't contain '/', just add to quote fee
                self.p2_fee += fee_cost
                
            # Update metadata
            self.last_update_time = datetime.now()
            self.trade_count += 1
                
            # Set entry time if this is first trade
            if self.entry_time is None and amount > 0:
                self.entry_time = datetime.now()
                
        except Exception as e:
            # Keep error logging for debugging
            print(f"ERROR: Failed to update position from trade: {e}")
            print(f"Trade data: {trade}")
            print(f"Position: {self.exchange}_{self.symbol}")
            raise
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert position to dictionary for serialization.
        
        Returns:
            Dictionary representation of position
        """
        return {
            'exchange': self.exchange,
            'symbol': self.symbol,
            'is_perpetual': self.is_perpetual,
            'p1': float(self.p1),
            'p2': float(self.p2),
            'p1_fee': float(self.p1_fee),
            'p2_fee': float(self.p2_fee),
            'avg_price': float(self.avg_price) if self.avg_price else None,
            'size': float(self.size),
            'value': float(self.value),
            'side': self.side,
            'is_open': self.is_open,
            'last_update_time': self.last_update_time.isoformat(),
            'entry_time': self.entry_time.isoformat() if self.entry_time else None,
            'trade_count': self.trade_count
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Position':
        """
        Create position from dictionary.
        
        Args:
            data: Dictionary data
            
        Returns:
            Position instance
        """
        position = cls(
            exchange=data['exchange'],
            symbol=data['symbol'],
            is_perpetual=data.get('is_perpetual', False)
        )
        
        position.p1 = Decimal(str(data['p1']))
        position.p2 = Decimal(str(data['p2']))
        position.p1_fee = Decimal(str(data['p1_fee']))
        position.p2_fee = Decimal(str(data['p2_fee']))
        
        if 'last_update_time' in data:
            position.last_update_time = datetime.fromisoformat(data['last_update_time'])
            
        if 'entry_time' in data and data['entry_time']:
            position.entry_time = datetime.fromisoformat(data['entry_time'])
            
        if 'trade_count' in data:
            position.trade_count = data['trade_count']
            
        return position


class PositionManager:
    """
    Position manager for tracking positions across all exchanges and symbols.
    
    Now uses PostgreSQL as the single source of truth for trade data.
    Positions are calculated on-demand from database trades.
    """
    
    def __init__(self, data_dir: Optional[str] = None, trade_repository=None):
        """
        Initialize position manager.
        
        Args:
            data_dir: Directory for storing position metadata (default: ./data/positions)
            trade_repository: TradeRepository for database access
        """
        self.data_dir = data_dir or os.path.join(os.getcwd(), 'data', 'positions')
        os.makedirs(self.data_dir, exist_ok=True)
        
        # Trade repository for database access
        self.trade_repository = trade_repository
        
        # Position metadata cache (disk-based)
        self.position_metadata = {}  # For storing position metadata like exchange types
        
        # Lock for thread safety
        self.lock = asyncio.Lock()
        
        # Loggers
        self.logger = logger.bind(component="PositionManager")
        self.trade_sync_logger = setup_trade_sync_logger()
        self.logger.info("Position manager initialized (PostgreSQL-based)")
        
    def _sanitize_filename(self, filename: str) -> str:
        """
        Sanitize filename by replacing invalid characters.
        
        Args:
            filename: Original filename
            
        Returns:
            Sanitized filename safe for file system
        """
        # Replace invalid characters with underscores
        invalid_chars = ['/', '\\', ':', '*', '?', '"', '<', '>', '|']
        sanitized = filename
        for char in invalid_chars:
            sanitized = sanitized.replace(char, '_')
        return sanitized
        
    async def start(self) -> None:
        """Start position manager and load metadata."""
        self.logger.info("Starting position manager")
        
        # Load position metadata from disk
        await self.load_position_metadata()
        
    async def stop(self) -> None:
        """Stop position manager and save metadata."""
        self.logger.info("Stopping position manager")
        
        # Save position metadata to disk
        await self.save_position_metadata()
        
    async def calculate_position_from_trades(self, exchange: str, symbol: str) -> Position:
        """
        Calculate position for exchange/symbol from database trades.
        
        Args:
            exchange: Exchange name
            symbol: Trading symbol
            
        Returns:
            Calculated Position object
        """
        if not self.trade_repository:
            raise RuntimeError("Trade repository not available")
            
        # Determine if this is a perpetual futures position
        is_perpetual = False
        if '_' in exchange:
            _, market_type = exchange.split('_', 1)
            is_perpetual = market_type in ['perp', 'future', 'futures', 'perpetual']
        elif any(marker in exchange.lower() for marker in ['usdm', 'futures', 'perp', 'swap']):
            is_perpetual = True
            
        # Create position object
        position = Position(exchange=exchange, symbol=symbol, is_perpetual=is_perpetual)
        
        try:
            # Get all trades for this exchange/symbol from database
            trades = await self.trade_repository.get_trades_by_exchange_symbol(exchange, symbol)
            
            # Apply all trades to the position
            for trade_record in trades:
                # Convert database record to trade dict
                trade = {
                    'id': trade_record.id,
                    'side': trade_record.side,
                    'amount': float(trade_record.amount),
                    'price': float(trade_record.price),
                    'cost': float(trade_record.cost),
                    'fee': {
                        'cost': float(trade_record.fee_cost) if trade_record.fee_cost else 0,
                        'currency': trade_record.fee_currency
                    } if trade_record.fee_cost else None
                }
                
                position.update_from_trade(trade)
            
            # Log position calculation
            log_position_update(
                self.trade_sync_logger,
                exchange=exchange,
                symbol=symbol,
                position_size=float(position.size),
                position_value=float(position.value),
                avg_price=float(position.avg_price) if position.avg_price else None
            )
            
        except Exception as e:
            self.logger.error(f"Error calculating position from trades: {e}", 
                            exchange=exchange, symbol=symbol)
            
        return position
        
    def set_trade_repository(self, trade_repository):
        """Set the trade repository for database access."""
        self.trade_repository = trade_repository
            
    async def update_from_order(self, order: Order, fill_data: Dict[str, Any]) -> None:
        """
        Update positions based on order fill.
        Note: This now triggers a position recalculation from database.
        
        Args:
            order: Filled order
            fill_data: Fill data with execution details
        """
        # This method is kept for compatibility but now just triggers recalculation
        self.logger.info(f"Order fill processed for {order.exchange} {order.symbol}")
            
    async def update_from_trade(self, exchange: str, trade: Dict[str, Any]) -> None:
        """
        Process trade data. Position is recalculated from database.
        
        Args:
            exchange: Exchange name
            trade: Trade data
        """
        symbol = trade.get('symbol')
        if not symbol:
            self.logger.warning(f"Trade missing symbol: {trade}")
            return
            
        self.logger.info(f"Trade processed for {exchange} {symbol}")
        
    def get_position(self, exchange: str, symbol: str) -> Optional[Position]:
        """
        Get position for specific exchange and symbol.
        Calculates position from database trades.
        
        Args:
            exchange: Exchange name
            symbol: Trading symbol
            
        Returns:
            Position calculated from database trades
        """
        try:
            # This needs to be called from an async context
            # For now, return None and let async methods handle it
            return None
        except Exception as e:
            self.logger.error(f"Error getting position: {e}", exchange=exchange, symbol=symbol)
            return None
            
    async def get_position_async(self, exchange: str, symbol: str) -> Optional[Position]:
        """
        Async version of get_position that calculates from database.
        
        Args:
            exchange: Exchange name
            symbol: Trading symbol
            
        Returns:
            Position calculated from database trades
        """
        return await self.calculate_position_from_trades(exchange, symbol)
        
    async def get_all_positions(self, symbol: Optional[str] = None) -> List[Position]:
        """
        Get all positions, optionally filtered by symbol.
        Calculates positions from database trades.
        
        Args:
            symbol: Filter by symbol
            
        Returns:
            List of positions calculated from database
        """
        if not self.trade_repository:
            return []
            
        positions = []
        
        try:
            # Get all unique exchange/symbol combinations from database
            exchange_symbols = await self.trade_repository.get_unique_exchange_symbols()
            
            for exchange, db_symbol in exchange_symbols:
                if symbol and db_symbol != symbol:
                    continue
                    
                position = await self.calculate_position_from_trades(exchange, db_symbol)
                positions.append(position)
                
        except Exception as e:
            self.logger.error(f"Error getting all positions: {e}")
            
        return positions

    def ensure_exchange_positions(self, exchange_connectors: Dict[str, Any], symbol: str = "BERA/USDT") -> None:
        """
        Ensure all connected exchanges have position metadata entries.
        
        Args:
            exchange_connectors: Dictionary of connected exchange connectors
            symbol: Symbol to ensure positions for
        """
        for exchange_name, connector in exchange_connectors.items():
            if not connector:
                continue
                
            # Determine if this is a perpetual futures exchange
            is_perpetual = False
            if '_' in exchange_name:
                _, market_type = exchange_name.split('_', 1)
                is_perpetual = market_type in ['perp', 'future', 'futures', 'perpetual']
            elif any(marker in exchange_name.lower() for marker in ['usdm', 'futures', 'perp', 'swap']):
                is_perpetual = True
                
            # Only create positions for matching exchange types
            if is_perpetual and not symbol.endswith('-PERP'):
                continue
            elif not is_perpetual and symbol.endswith('-PERP'):
                continue
                
            # Store metadata
            position_key = f"{exchange_name}_{symbol}"
            self.position_metadata[position_key] = {
                'exchange': exchange_name,
                'symbol': symbol,
                'is_perpetual': is_perpetual,
                'created': datetime.now().isoformat()
            }
            
            self.logger.info(f"Ensured position metadata for {exchange_name} {symbol}")

    async def get_all_exchanges_with_positions(self, exchange_connectors: Dict[str, Any], symbol: str = "BERA/USDT") -> List[Position]:
        """
        Get positions for all connected exchanges, calculating from database.
        
        Args:
            exchange_connectors: Dictionary of connected exchange connectors
            symbol: Symbol to get positions for
            
        Returns:
            List of positions (including zero positions)
        """
        self.ensure_exchange_positions(exchange_connectors, symbol)
        
        positions = []
        for exchange_name in exchange_connectors.keys():
            # Skip based on symbol type vs exchange type
            is_perpetual = False
            if '_' in exchange_name:
                _, market_type = exchange_name.split('_', 1)
                is_perpetual = market_type in ['perp', 'future', 'futures', 'perpetual']
                
            if is_perpetual and not symbol.endswith('-PERP'):
                continue
            elif not is_perpetual and symbol.endswith('-PERP'):
                continue
                
            position = await self.calculate_position_from_trades(exchange_name, symbol)
            positions.append(position)
            
        return positions

    def get_positions_by_exchange(self, exchange: str) -> List[Position]:
        """
        Get all positions for a specific exchange.
        Note: This is a sync method that returns empty list. Use async version.
        """
        return []
        
    async def get_positions_by_exchange_async(self, exchange: str) -> List[Position]:
        """
        Get all positions for a specific exchange from database.
        
        Args:
            exchange: Exchange name
            
        Returns:
            List of positions for the exchange
        """
        if not self.trade_repository:
            return []
            
        positions = []
        
        try:
            # Get all symbols for this exchange from database
            symbols = await self.trade_repository.get_symbols_by_exchange(exchange)
            
            for symbol in symbols:
                position = await self.calculate_position_from_trades(exchange, symbol)
                positions.append(position)
                
        except Exception as e:
            self.logger.error(f"Error getting positions for exchange {exchange}: {e}")
            
        return positions
        
    async def get_net_position(self, symbol: str) -> Dict[str, Any]:
        """
        Get net position across all exchanges for a symbol.
        Calculates from database trades.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            Net position data
        """
        positions = await self.get_all_positions(symbol)
        
        # Calculate net position
        net_p1 = Decimal('0')
        net_p2 = Decimal('0')
        net_p1_fee = Decimal('0')
        net_p2_fee = Decimal('0')
        
        total_trades = 0
        exchanges = []
        
        for position in positions:
            if position.symbol == symbol:
                net_p1 += position.p1
                net_p2 += position.p2
                net_p1_fee += position.p1_fee
                net_p2_fee += position.p2_fee
                total_trades += position.trade_count
                exchanges.append(position.exchange)
        
        # Calculate net average price
        net_avg_price = None
        if net_p1 != Decimal('0'):
            net_avg_price = -(net_p2 + net_p2_fee) / net_p1
        
        return {
            'symbol': symbol,
            'size': float(net_p1),
            'value': float(-net_p2),
            'avg_price': float(net_avg_price) if net_avg_price else None,
            'side': 'long' if net_p1 > 0 else 'short' if net_p1 < 0 else None,
            'is_open': net_p1 != Decimal('0'),
            'total_fees_base': float(net_p1_fee),
            'total_fees_quote': float(net_p2_fee),
            'exchanges': exchanges,
            'trade_count': total_trades
        }
        
    async def get_all_net_positions(self) -> Dict[str, Dict[str, Any]]:
        """
        Get net positions for all symbols across all exchanges.
        Calculates from database trades.
        
        Returns:
            Dictionary of symbol -> net position data
        """
        if not self.trade_repository:
            return {}
            
        net_positions = {}
        
        try:
            # Get all unique symbols from database
            symbols = await self.trade_repository.get_unique_symbols()
            
            for symbol in symbols:
                net_position = await self.get_net_position(symbol)
                net_positions[symbol] = net_position
                
        except Exception as e:
            self.logger.error(f"Error getting all net positions: {e}")
            
        return net_positions
        
    async def reset_position(self, exchange: str, symbol: str) -> bool:
        """
        Reset (delete) a specific position by removing its trades from database.
        
        Args:
            exchange: Exchange name
            symbol: Trading symbol
            
        Returns:
            True if successful, False otherwise
        """
        if not self.trade_repository:
            return False
            
        try:
            await self.trade_repository.delete_trades_by_exchange_symbol(exchange, symbol)
            self.logger.info(f"Reset position for {exchange} {symbol}")
            return True
        except Exception as e:
            self.logger.error(f"Error resetting position: {e}", exchange=exchange, symbol=symbol)
            return False
            
    async def reset_all_positions(self) -> None:
        """
        Reset (delete) all positions by clearing all trades from database.
        """
        if not self.trade_repository:
            self.logger.warning("No trade repository available for reset")
            return
            
        try:
            await self.trade_repository.delete_all_trades()
            
            # Also clear position metadata files
            if os.path.exists(self.data_dir):
                for file in os.listdir(self.data_dir):
                    if file.endswith('.json'):
                        os.remove(os.path.join(self.data_dir, file))
                        
            self.position_metadata = {}
            self.logger.info("All positions reset")
            
        except Exception as e:
            self.logger.error(f"Error resetting all positions: {e}")

    async def load_position_metadata(self) -> None:
        """Load position metadata from disk."""
        metadata_file = os.path.join(self.data_dir, 'position_metadata.json')
        
        if os.path.exists(metadata_file):
            try:
                with open(metadata_file, 'r') as f:
                    self.position_metadata = json.load(f)
                self.logger.info("Position metadata loaded from disk")
            except Exception as e:
                self.logger.error(f"Error loading position metadata: {e}")
                
    async def save_position_metadata(self) -> None:
        """Save position metadata to disk."""
        metadata_file = os.path.join(self.data_dir, 'position_metadata.json')
        
        try:
            with open(metadata_file, 'w') as f:
                json.dump(self.position_metadata, f, indent=2)
            self.logger.info("Position metadata saved to disk")
        except Exception as e:
            self.logger.error(f"Error saving position metadata: {e}")
            
    async def summarize_positions(self) -> Dict[str, Any]:
        """
        Create position summary from database trades.
        
        Returns:
            Summary of all positions
        """
        positions = await self.get_all_positions()
        
        total_long_value = Decimal('0')
        total_short_value = Decimal('0')
        open_positions = 0
        positions_by_exchange = defaultdict(int)
        
        for position in positions:
            if position.is_open:
                open_positions += 1
                value = abs(position.value)
                
                if position.side == 'long':
                    total_long_value += value
                elif position.side == 'short':
                    total_short_value += value
                    
                positions_by_exchange[position.exchange] += 1
        
        net_exposure = total_long_value - total_short_value
        
        return {
            'total_positions': len(positions),
            'open_positions': open_positions,
            'long_value': float(total_long_value),
            'short_value': float(total_short_value),
            'net_exposure': float(net_exposure),
            'positions_by_exchange': dict(positions_by_exchange),
            'last_updated': datetime.now().isoformat()
        }
        
    async def sync_positions_from_exchanges(self, exchange_connectors: Dict[str, Any]) -> None:
        """
        Sync positions from exchanges that support position querying.
        Note: This is now handled by the trade sync system.
        
        Args:
            exchange_connectors: Dictionary of connected exchange connectors
        """
        self.logger.info("Position sync from exchanges handled by trade sync system")
        
    def _normalize_position_symbol(self, symbol: str, exchange_id: str, is_perpetual: bool) -> str:
        """
        Normalize position symbol for consistent tracking.
        
        Args:
            symbol: Original symbol
            exchange_id: Exchange identifier
            is_perpetual: Whether this is a perpetual futures position
            
        Returns:
            Normalized symbol
        """
        if is_perpetual:
            if not symbol.endswith('-PERP'):
                return f"{symbol}-PERP"
        else:
            if symbol.endswith('-PERP'):
                return symbol[:-5]  # Remove -PERP suffix
                
        return symbol
