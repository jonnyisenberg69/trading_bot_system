"""
Orderbook Manager for handling multiple exchange orderbooks.

Manages multiple orderbooks across different exchanges, handling WebSocket connections,
data normalization, and orderbook synchronization.
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any, Tuple, Set, Callable
from decimal import Decimal
from datetime import datetime

from .orderbook import OrderBook
from exchanges.websocket import WebSocketManager, WSMessageType
from exchanges.base_connector import BaseExchangeConnector

logger = logging.getLogger(__name__)


class OrderbookManager:
    """
    Manages orderbooks for multiple symbols across exchanges.
    
    Features:
    - Maintains orderbooks for multiple symbols/exchanges
    - Connects to exchange WebSockets for real-time updates
    - Handles orderbook snapshots and delta updates
    - Provides methods for accessing orderbook data
    """
    
    def __init__(self, websocket_manager: Optional[WebSocketManager] = None):
        """
        Initialize orderbook manager.
        
        Args:
            websocket_manager: WebSocket manager instance (created if None)
        """
        # WebSocket manager for handling connections
        self.ws_manager = websocket_manager or WebSocketManager()
        
        # Orderbooks by exchange and symbol
        self.orderbooks: Dict[str, Dict[str, OrderBook]] = {}
        
        # Connection tracking
        self.connections: Dict[str, str] = {}  # exchange -> connection_id
        
        # Subscription status
        self.subscribed_symbols: Dict[str, Set[str]] = {}  # exchange -> set(symbols)
        
        # Internal state
        self._running = False
        self._initialized = False
        
        self.logger = logging.getLogger(f"{__name__}.OrderbookManager")
    
    async def start(self):
        """Start the orderbook manager and WebSocket connections."""
        if self._running:
            self.logger.warning("Orderbook manager already running")
            return
        
        # Start WebSocket manager if not already running
        if not hasattr(self.ws_manager, '_running') or not self.ws_manager._running:
            await self.ws_manager.start()
        
        self._running = True
        
        # Register message handlers
        self.ws_manager.register_handler(WSMessageType.ORDERBOOK, self._handle_orderbook_message)
        
        self.logger.info("Orderbook manager started")
    
    async def stop(self):
        """Stop the orderbook manager and close connections."""
        if not self._running:
            self.logger.warning("Orderbook manager not running")
            return
        
        self._running = False
        
        # Close all connections if we own the WebSocket manager
        if not hasattr(self.ws_manager, '_running') or self.ws_manager._running:
            await self.ws_manager.stop()
        
        self.logger.info("Orderbook manager stopped")
    
    async def add_exchange(self, exchange: BaseExchangeConnector, symbols: List[str] = None):
        """
        Add an exchange and optionally subscribe to symbols.
        
        Args:
            exchange: Exchange connector instance
            symbols: List of symbols to subscribe to (optional)
        """
        exchange_name = exchange.name.lower()
        
        # Initialize data structures for this exchange
        if exchange_name not in self.orderbooks:
            self.orderbooks[exchange_name] = {}
        
        if exchange_name not in self.subscribed_symbols:
            self.subscribed_symbols[exchange_name] = set()
        
        # Get WebSocket endpoint for market data
        endpoint = self._get_ws_endpoint(exchange)
        
        if not endpoint:
            self.logger.error(f"No WebSocket endpoint found for {exchange_name}")
            return
        
        # Connect to WebSocket
        conn_id = await self.ws_manager.connect_exchange(
            exchange, endpoint, "orderbook"
        )
        
        # Store connection ID
        self.connections[exchange_name] = conn_id
        
        # Subscribe to symbols if provided
        if symbols:
            for symbol in symbols:
                await self.subscribe_symbol(exchange_name, symbol)
        
        self.logger.info(f"Added exchange {exchange_name} with connection {conn_id}")
    
    async def subscribe_symbol(self, exchange: str, symbol: str) -> bool:
        """
        Subscribe to orderbook updates for a symbol.
        
        Args:
            exchange: Exchange name
            symbol: Trading symbol (e.g., 'BTC/USDT')
            
        Returns:
            True if subscription successful, False otherwise
        """
        exchange = exchange.lower()
        
        if exchange not in self.connections:
            self.logger.error(f"Exchange {exchange} not connected")
            return False
        
        # Create orderbook if it doesn't exist
        if symbol not in self.orderbooks.get(exchange, {}):
            self.orderbooks[exchange][symbol] = OrderBook(symbol, exchange)
        
        # Subscribe to WebSocket channel
        conn_id = self.connections[exchange]
        result = await self.ws_manager.subscribe(conn_id, "depth", symbol)
        
        if result:
            # Track subscription
            self.subscribed_symbols[exchange].add(symbol)
            
            # Fetch initial snapshot (for some exchanges we might need to do this via REST API)
            # This is exchange-specific and would need to be implemented per exchange
            
            self.logger.info(f"Subscribed to {symbol} orderbook on {exchange}")
        
        return result
    
    async def unsubscribe_symbol(self, exchange: str, symbol: str) -> bool:
        """
        Unsubscribe from orderbook updates for a symbol.
        
        Args:
            exchange: Exchange name
            symbol: Trading symbol
            
        Returns:
            True if unsubscription successful, False otherwise
        """
        exchange = exchange.lower()
        
        if exchange not in self.connections:
            self.logger.error(f"Exchange {exchange} not connected")
            return False
        
        # Unsubscribe from WebSocket channel
        conn_id = self.connections[exchange]
        result = await self.ws_manager.unsubscribe(conn_id, "depth", symbol)
        
        if result:
            # Update tracking
            if exchange in self.subscribed_symbols and symbol in self.subscribed_symbols[exchange]:
                self.subscribed_symbols[exchange].remove(symbol)
            
            # Keep the orderbook object in case it's needed later
            
            self.logger.info(f"Unsubscribed from {symbol} orderbook on {exchange}")
        
        return result
    
    def get_orderbook(self, exchange: str, symbol: str) -> Optional[OrderBook]:
        """
        Get orderbook for a specific exchange and symbol.
        
        Args:
            exchange: Exchange name
            symbol: Trading symbol
            
        Returns:
            OrderBook instance or None if not found
        """
        exchange = exchange.lower()
        return self.orderbooks.get(exchange, {}).get(symbol)
    
    def get_best_bid_ask(self, exchange: str, symbol: str) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        """
        Get best bid and ask prices for a symbol.
        
        Args:
            exchange: Exchange name
            symbol: Trading symbol
            
        Returns:
            Tuple of (best_bid, best_ask) or (None, None) if not available
        """
        orderbook = self.get_orderbook(exchange, symbol)
        if not orderbook:
            return None, None
        
        best_bid, _ = orderbook.get_best_bid()
        best_ask, _ = orderbook.get_best_ask()
        
        return best_bid, best_ask
    
    def get_best_prices_across_exchanges(self, symbol: str) -> Dict[str, Tuple[Optional[Decimal], Optional[Decimal]]]:
        """
        Get best bid/ask prices for a symbol across all exchanges.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            Dictionary of exchange -> (best_bid, best_ask)
        """
        result = {}
        
        for exchange in self.orderbooks:
            if symbol in self.orderbooks[exchange]:
                orderbook = self.orderbooks[exchange][symbol]
                best_bid, _ = orderbook.get_best_bid()
                best_ask, _ = orderbook.get_best_ask()
                result[exchange] = (best_bid, best_ask)
        
        return result
    
    def find_best_bid(self, symbol: str) -> Tuple[Optional[str], Optional[Decimal], Optional[Decimal]]:
        """
        Find the highest bid price across all exchanges.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            Tuple of (exchange, price, amount) or (None, None, None) if not found
        """
        best_exchange = None
        best_price = None
        best_amount = None
        
        for exchange in self.orderbooks:
            if symbol in self.orderbooks[exchange]:
                orderbook = self.orderbooks[exchange][symbol]
                price, amount = orderbook.get_best_bid()
                
                if price is not None and (best_price is None or price > best_price):
                    best_price = price
                    best_amount = amount
                    best_exchange = exchange
        
        return best_exchange, best_price, best_amount
    
    def find_best_ask(self, symbol: str) -> Tuple[Optional[str], Optional[Decimal], Optional[Decimal]]:
        """
        Find the lowest ask price across all exchanges.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            Tuple of (exchange, price, amount) or (None, None, None) if not found
        """
        best_exchange = None
        best_price = None
        best_amount = None
        
        for exchange in self.orderbooks:
            if symbol in self.orderbooks[exchange]:
                orderbook = self.orderbooks[exchange][symbol]
                price, amount = orderbook.get_best_ask()
                
                if price is not None and (best_price is None or price < best_price):
                    best_price = price
                    best_amount = amount
                    best_exchange = exchange
        
        return best_exchange, best_price, best_amount
    
    def calculate_cross_exchange_spread(self, symbol: str) -> Optional[Decimal]:
        """
        Calculate the cross-exchange spread (best ask - best bid across exchanges).
        
        Args:
            symbol: Trading symbol
            
        Returns:
            Cross-exchange spread or None if not available
        """
        _, best_bid, _ = self.find_best_bid(symbol)
        _, best_ask, _ = self.find_best_ask(symbol)
        
        if best_bid is None or best_ask is None:
            return None
        
        return best_ask - best_bid
    
    def get_arbitrage_opportunities(self, symbol: str, min_profit_pct: float = 0.1) -> List[Dict[str, Any]]:
        """
        Find arbitrage opportunities across exchanges.
        
        Args:
            symbol: Trading symbol
            min_profit_pct: Minimum profit percentage threshold
            
        Returns:
            List of arbitrage opportunities
        """
        opportunities = []
        
        # Get all exchanges with this symbol
        exchanges = [
            exchange for exchange in self.orderbooks 
            if symbol in self.orderbooks[exchange]
        ]
        
        # Check all exchange pairs
        for i, buy_exchange in enumerate(exchanges):
            buy_orderbook = self.orderbooks[buy_exchange][symbol]
            _, buy_price = buy_orderbook.get_best_ask()
            
            if buy_price is None:
                continue
            
            for sell_exchange in exchanges[i+1:]:
                sell_orderbook = self.orderbooks[sell_exchange][symbol]
                sell_price, _ = sell_orderbook.get_best_bid()
                
                if sell_price is None:
                    continue
                
                # Calculate profit percentage
                profit_pct = ((sell_price / buy_price) - 1) * 100
                
                if profit_pct >= min_profit_pct:
                    opportunities.append({
                        'symbol': symbol,
                        'buy_exchange': buy_exchange,
                        'buy_price': buy_price,
                        'sell_exchange': sell_exchange,
                        'sell_price': sell_price,
                        'profit_pct': profit_pct
                    })
                
                # Check the reverse direction
                buy_price_rev, _ = buy_orderbook.get_best_bid()
                sell_price_rev, _ = sell_orderbook.get_best_ask()
                
                if buy_price_rev is not None and sell_price_rev is not None:
                    profit_pct_rev = ((buy_price_rev / sell_price_rev) - 1) * 100
                    
                    if profit_pct_rev >= min_profit_pct:
                        opportunities.append({
                            'symbol': symbol,
                            'buy_exchange': sell_exchange,
                            'buy_price': sell_price_rev,
                            'sell_exchange': buy_exchange,
                            'sell_price': buy_price_rev,
                            'profit_pct': profit_pct_rev
                        })
        
        return opportunities
    
    def get_all_symbols(self) -> Set[str]:
        """
        Get all symbols being tracked.
        
        Returns:
            Set of all symbols
        """
        symbols = set()
        
        for exchange in self.orderbooks:
            symbols.update(self.orderbooks[exchange].keys())
        
        return symbols
    
    def get_exchanges_for_symbol(self, symbol: str) -> List[str]:
        """
        Get all exchanges tracking a symbol.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            List of exchange names
        """
        return [
            exchange for exchange in self.orderbooks
            if symbol in self.orderbooks[exchange]
        ]
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get status of all orderbooks.
        
        Returns:
            Status dictionary
        """
        total_symbols = sum(len(books) for books in self.orderbooks.values())
        
        return {
            'running': self._running,
            'exchanges': list(self.orderbooks.keys()),
            'total_symbols': total_symbols,
            'symbols_by_exchange': {
                exchange: list(symbols.keys())
                for exchange, symbols in self.orderbooks.items()
            },
            'connections': {
                exchange: self.ws_manager.get_connection_status(conn_id)
                for exchange, conn_id in self.connections.items()
            }
        }
    
    async def _handle_orderbook_message(self, message: Dict[str, Any]):
        """
        Handle orderbook WebSocket message.
        
        Args:
            message: Normalized WebSocket message
        """
        # Extract message details
        exchange = message.get('exchange', '').lower()
        symbol = message.get('symbol')
        
        if not exchange or not symbol:
            return
        
        # Skip if we're not tracking this orderbook
        if exchange not in self.orderbooks or symbol not in self.orderbooks[exchange]:
            return
        
        orderbook = self.orderbooks[exchange][symbol]
        
        # Process message based on exchange
        message_type = message.get('type')
        if message_type != WSMessageType.ORDERBOOK:
            return
        
        raw_data = message.get('raw', {})
        
        # Check if this is a snapshot or update
        is_snapshot = message.get('is_snapshot', False)
        
        if is_snapshot:
            # Process snapshot
            orderbook.update_from_snapshot(raw_data)
            self.logger.debug(f"Applied orderbook snapshot for {symbol} on {exchange}")
        else:
            # Process delta update
            success = orderbook.apply_delta(raw_data)
            if not success:
                self.logger.warning(
                    f"Failed to apply orderbook update for {symbol} on {exchange}, "
                    f"need new snapshot"
                )
                # Here we would trigger a new snapshot fetch
    
    def _get_ws_endpoint(self, exchange: BaseExchangeConnector) -> Optional[str]:
        """Get WebSocket endpoint for an exchange."""
        exchange_name = exchange.name.lower()
        
        # Exchange-specific endpoints
        if exchange_name == 'binance':
            return 'wss://stream.binance.com:9443/ws'
        elif exchange_name == 'bybit':
            return 'wss://stream.bybit.com/v5/public/spot'
        elif exchange_name == 'hyperliquid':
            return 'wss://api.hyperliquid.xyz/ws'
        elif exchange_name == 'mexc':
            return 'wss://wbs.mexc.com/ws'
        elif exchange_name == 'gateio':
            return 'wss://api.gateio.ws/ws/v4/'
        elif exchange_name == 'bitget':
            return 'wss://ws.bitget.com/spot/v1/stream'
        
        # Try to get from exchange instance if it has a method
        if hasattr(exchange, 'get_ws_endpoint'):
            return exchange.get_ws_endpoint('market')
        
        return None
