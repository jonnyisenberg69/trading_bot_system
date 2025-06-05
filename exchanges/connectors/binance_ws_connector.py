"""
Binance exchange connector.

Provides WebSocket and REST API functionality for the Binance exchange.
"""

import os
import time
import asyncio
import json
from typing import Dict, List, Optional, Any, Tuple
from decimal import Decimal
import ccxt.async_support as ccxt
import structlog
import aiohttp
from datetime import datetime, timedelta

from exchanges.websocket_manager import WebSocketManager

logger = structlog.get_logger(__name__)


class BinanceWSConnector:
    """
    Binance WebSocket connector for market data.
    
    Provides:
    - Real CCXT Binance API integration
    - REST API access with rate limiting
    - Market data retrieval
    - Order book data
    """
    
    def __init__(
        self,
        api_key: str = None,
        api_secret: str = None,
        testnet: bool = False,
        request_timeout: int = 30000,
        rate_limit_factor: float = 0.8
    ):
        """
        Initialize Binance connector.
        
        Args:
            api_key: API key (optional)
            api_secret: API secret (optional)
            testnet: Use testnet (default: False)
            request_timeout: Request timeout in milliseconds (default: 30000)
            rate_limit_factor: Rate limit factor (0.0-1.0) to stay below limits
        """
        self.api_key = api_key or os.getenv("BINANCE_API_KEY", "")
        self.api_secret = api_secret or os.getenv("BINANCE_API_SECRET", "")
        self.testnet = testnet
        self.request_timeout = request_timeout
        self.rate_limit_factor = rate_limit_factor
        
        # Exchange objects
        self.exchange = None  # CCXT exchange object
        self.ws_managers = {}  # WebSocket managers
        
        # Cache
        self.markets_cache = {}
        self.symbols_cache = set()
        self.last_markets_update = 0
        self.markets_cache_ttl = 3600  # 1 hour
        
        # WebSocket status
        self.ws_subscriptions = set()
        
        # Logger
        self.logger = logger.bind(exchange="binance")
        
        # Initialize real CCXT exchange
        self._create_exchange()
        
    def _create_exchange(self):
        """Create real CCXT Binance exchange instance."""
        try:
            self.exchange = ccxt.binance({
                'apiKey': self.api_key,
                'secret': self.api_secret,
                'sandbox': self.testnet,
                'enableRateLimit': True,
                'rateLimit': int(100 * self.rate_limit_factor),  # 100ms default with factor
                'timeout': self.request_timeout,
                'options': {
                    'defaultType': 'spot',  # Default to spot trading
                    'adjustForTimeDifference': True,
                    'recvWindow': 10000,
                }
            })
            self.logger.info("Binance CCXT exchange initialized")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Binance exchange: {e}")
            raise
        
    async def initialize(self) -> None:
        """Initialize the connector."""
        try:
            # Load markets
            await self.load_markets()
            self.logger.info("Binance connector initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Binance connector: {e}")
            raise
        
    async def load_markets(self, force: bool = False) -> Dict[str, Any]:
        """
        Load markets from exchange.
        
        Args:
            force: Force reload markets
            
        Returns:
            Markets dictionary
        """
        now = time.time()
        
        # Check cache
        if not force and self.markets_cache and now - self.last_markets_update < self.markets_cache_ttl:
            return self.markets_cache
            
        try:
            # Load markets
            self.logger.info("Loading Binance markets...")
            markets = await self.exchange.load_markets(reload=True)
            
            # Update cache
            self.markets_cache = markets
            self.symbols_cache = set(markets.keys())
            self.last_markets_update = now
            
            # Count spot vs futures markets
            spot_count = sum(1 for m in markets.values() if m.get('spot', False))
            swap_count = sum(1 for m in markets.values() if m.get('swap', False))
            
            self.logger.info(f"Loaded {len(markets)} Binance markets "
                           f"(spot: {spot_count}, swap: {swap_count})")
            return markets
            
        except Exception as e:
            self.logger.error(f"Failed to load Binance markets: {e}")
            raise
        
    async def get_available_symbols(self, market_type: str = 'all') -> List[str]:
        """
        Get list of available trading symbols.
        
        Args:
            market_type: 'spot', 'swap', or 'all'
            
        Returns:
            List of symbols
        """
        # Ensure markets are loaded
        markets = await self.load_markets()
        
        symbols = []
        for symbol, market in markets.items():
            if market_type == 'all':
                symbols.append(symbol)
            elif market_type == 'spot' and market.get('spot', False):
                symbols.append(symbol)
            elif market_type == 'swap' and market.get('swap', False):
                symbols.append(symbol)
        
        return sorted(symbols)
        
    async def is_symbol_supported(self, symbol: str) -> bool:
        """
        Check if symbol is supported.
        
        Args:
            symbol: Symbol to check
            
        Returns:
            True if symbol is supported
        """
        # Load markets if needed
        if not self.symbols_cache:
            await self.load_markets()
            
        return symbol in self.symbols_cache
        
    async def get_ticker(self, symbol: str) -> Dict[str, Any]:
        """
        Get ticker for symbol.
        
        Args:
            symbol: Symbol to get ticker for
            
        Returns:
            Ticker dictionary
        """
        try:
            ticker = await self.exchange.fetch_ticker(symbol)
            
            return {
                'symbol': symbol,
                'exchange': 'binance',
                'last': Decimal(str(ticker.get('last', 0))),
                'bid': Decimal(str(ticker.get('bid', 0))),
                'ask': Decimal(str(ticker.get('ask', 0))),
                'high': Decimal(str(ticker.get('high', 0))),
                'low': Decimal(str(ticker.get('low', 0))),
                'volume': Decimal(str(ticker.get('baseVolume', 0))),
                'quoteVolume': Decimal(str(ticker.get('quoteVolume', 0))),
                'change': Decimal(str(ticker.get('change', 0))),
                'percentage': Decimal(str(ticker.get('percentage', 0))),
                'timestamp': ticker.get('timestamp'),
                'datetime': ticker.get('datetime'),
                'raw': ticker
            }
            
        except Exception as e:
            self.logger.error(f"Error getting {symbol} ticker: {e}")
            raise
            
    async def get_orderbook(self, symbol: str, limit: int = 20) -> Dict[str, Any]:
        """
        Get order book for symbol.
        
        Args:
            symbol: Symbol to get order book for
            limit: Order book depth
            
        Returns:
            Order book dictionary
        """
        try:
            orderbook = await self.exchange.fetch_order_book(symbol, limit)
            
            return {
                'symbol': symbol,
                'exchange': 'binance',
                'timestamp': orderbook.get('timestamp'),
                'datetime': orderbook.get('datetime'),
                'bids': [[Decimal(str(price)), Decimal(str(amount))] 
                        for price, amount in orderbook.get('bids', [])],
                'asks': [[Decimal(str(price)), Decimal(str(amount))] 
                        for price, amount in orderbook.get('asks', [])],
                'nonce': orderbook.get('nonce'),
                'raw': orderbook
            }
            
        except Exception as e:
            self.logger.error(f"Error getting {symbol} orderbook: {e}")
            raise
            
    async def get_trades(
        self,
        symbol: str,
        since: Optional[int] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get trades for symbol.
        
        Args:
            symbol: Symbol to get trades for
            since: Get trades since this timestamp
            limit: Maximum number of trades to get
            
        Returns:
            List of trades
        """
        try:
            trades = await self.exchange.fetch_trades(symbol, since, limit)
            
            result = []
            for trade in trades:
                result.append({
                    'id': trade.get('id'),
                    'symbol': symbol,
                    'exchange': 'binance',
                    'side': trade.get('side'),
                    'amount': Decimal(str(trade.get('amount', 0))),
                    'price': Decimal(str(trade.get('price', 0))),
                    'cost': Decimal(str(trade.get('cost', 0))),
                    'timestamp': trade.get('timestamp'),
                    'datetime': trade.get('datetime'),
                    'raw': trade
                })
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error getting {symbol} trades: {e}")
            raise
            
    async def get_my_trades(
        self,
        symbol: str,
        since: Optional[int] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get my trades for symbol.
        
        Args:
            symbol: Symbol to get trades for
            since: Get trades since this timestamp
            limit: Maximum number of trades to get
            
        Returns:
            List of trades
        """
        if not self.api_key or not self.api_secret:
            raise ValueError("API key and secret required for get_my_trades")
            
        try:
            trades = await self.exchange.fetch_my_trades(symbol, since, limit)
            
            result = []
            for trade in trades:
                result.append({
                    'id': trade.get('id'),
                    'order_id': trade.get('order'),
                    'symbol': symbol,
                    'exchange': 'binance',
                    'side': trade.get('side'),
                    'amount': Decimal(str(trade.get('amount', 0))),
                    'price': Decimal(str(trade.get('price', 0))),
                    'cost': Decimal(str(trade.get('cost', 0))),
                    'fee': trade.get('fee'),
                    'timestamp': trade.get('timestamp'),
                    'datetime': trade.get('datetime'),
                    'raw': trade
                })
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error getting my {symbol} trades: {e}")
            raise
            
    async def create_order(
        self,
        symbol: str,
        order_type: str,
        side: str,
        amount: float,
        price: Optional[float] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Create order.
        
        Args:
            symbol: Symbol to create order for
            order_type: Order type (market, limit, etc.)
            side: Order side (buy, sell)
            amount: Order amount
            price: Order price (required for limit orders)
            params: Additional parameters
            
        Returns:
            Order response
        """
        if not self.api_key or not self.api_secret:
            raise ValueError("API key and secret required for create_order")
            
        try:
            response = await self.exchange.create_order(
                symbol=symbol,
                type=order_type,
                side=side,
                amount=amount,
                price=price,
                params=params or {}
            )
            return response
        except Exception as e:
            self.logger.error(f"Error creating {symbol} {order_type} {side} order: {e}")
            raise
            
    async def cancel_order(
        self,
        order_id: str,
        symbol: str,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Cancel order.
        
        Args:
            order_id: Order ID to cancel
            symbol: Symbol for order
            params: Additional parameters
            
        Returns:
            Cancel response
        """
        if not self.api_key or not self.api_secret:
            raise ValueError("API key and secret required for cancel_order")
            
        try:
            response = await self.exchange.cancel_order(
                id=order_id,
                symbol=symbol,
                params=params or {}
            )
            return response
        except Exception as e:
            self.logger.error(f"Error canceling {symbol} order {order_id}: {e}")
            raise
            
    async def get_open_orders(
        self,
        symbol: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Get open orders.
        
        Args:
            symbol: Symbol to get open orders for (optional)
            params: Additional parameters
            
        Returns:
            List of open orders
        """
        if not self.api_key or not self.api_secret:
            raise ValueError("API key and secret required for get_open_orders")
            
        try:
            orders = await self.exchange.fetch_open_orders(
                symbol=symbol,
                params=params or {}
            )
            return orders
        except Exception as e:
            self.logger.error(f"Error getting open orders: {e}")
            raise
            
    async def get_balance(self) -> Dict[str, Any]:
        """
        Get account balance.
        
        Returns:
            Balance dictionary
        """
        if not self.api_key or not self.api_secret:
            raise ValueError("API key and secret required for get_balance")
            
        try:
            balance = await self.exchange.fetch_balance()
            return balance
        except Exception as e:
            self.logger.error(f"Error getting balance: {e}")
            raise
            
    async def close(self) -> None:
        """Close all connections."""
        try:
            # Close all WebSocket managers
            for key, ws_manager in self.ws_managers.items():
                try:
                    await ws_manager.disconnect("Connector closing")
                except Exception as e:
                    self.logger.error(f"Error closing WebSocket manager {key}: {e}")
                    
            # Close CCXT exchange
            if self.exchange:
                await self.exchange.close()
                
            self.logger.info("Binance connector closed")
            
        except Exception as e:
            self.logger.error(f"Error closing Binance connector: {e}")
            raise 