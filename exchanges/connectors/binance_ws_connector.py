"""
Binance exchange connector.

Provides WebSocket and REST API functionality for the Binance exchange.
"""

import os
import time
import asyncio
import json
import hmac
import hashlib
from typing import Dict, List, Optional, Any, Tuple
from decimal import Decimal
import ccxt.async_support as ccxt
import structlog
import aiohttp
from datetime import datetime, timedelta
import websockets
import traceback

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
    - Support for both spot and futures (linear/USDT) markets
    """
    
    def __init__(
        self,
        api_key: str = None,
        api_secret: str = None,
        testnet: bool = False,
        market_type: str = 'spot',
        request_timeout: int = 30000,
        rate_limit_factor: float = 0.8
    ):
        """
        Initialize Binance connector.
        
        Args:
            api_key: API key (optional)
            api_secret: API secret (optional)
            testnet: Use testnet (default: False)
            market_type: 'spot' or 'future' (default: 'spot')
            request_timeout: Request timeout in milliseconds (default: 30000)
            rate_limit_factor: Rate limit factor (0.0-1.0) to stay below limits
        """
        self.api_key = api_key or os.getenv("BINANCE_API_KEY", "")
        self.api_secret = api_secret or os.getenv("BINANCE_API_SECRET", "")
        self.testnet = testnet
        self.market_type = market_type
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
        
        # WebSocket connection
        self.websocket = None
        self.on_message = None  # Callback for WebSocket messages
        self.on_connected = None  # Callback for when WebSocket is connected and subscribed
        self.ws_task = None  # Task for WebSocket connection
        self.listen_key = None
        
        # Logger
        self.logger = logger.bind(exchange="binance", market_type=market_type)
        
        # Set canonical name for system consistency
        if self.market_type == 'future':
            self.name = 'binance_perp'
        else:
            self.name = 'binance_spot'
        
        # Initialize real CCXT exchange
        self._create_exchange()
        
    def _create_exchange(self):
        """Create real CCXT Binance exchange instance."""
        try:
            if self.market_type == 'future':
                self.exchange = ccxt.binanceusdm({
                    'apiKey': self.api_key,
                    'secret': self.api_secret,
                    'sandbox': self.testnet,
                    'enableRateLimit': True,
                    'rateLimit': int(100 * self.rate_limit_factor),
                    'timeout': self.request_timeout,
                    'options': {
                        'adjustForTimeDifference': True,
                        'recvWindow': 10000,
                    }
                })
            else:
                self.exchange = ccxt.binance({
                    'apiKey': self.api_key,
                    'secret': self.api_secret,
                    'sandbox': self.testnet,
                    'enableRateLimit': True,
                    'rateLimit': int(100 * self.rate_limit_factor),
                    'timeout': self.request_timeout,
                    'options': {
                        'defaultType': 'spot',
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
            
            # Set up direct WebSocket connection for monitoring
            self.ws_task = asyncio.create_task(self._setup_websocket_connection())
            
            self.logger.info("Binance connector initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Binance connector: {e}")
            self.logger.error(traceback.format_exc())
            raise
        
    async def _setup_websocket_connection(self):
        """Set up a direct WebSocket connection for monitoring trade messages."""
        try:
            # Get listen key
            self.listen_key = await self.get_listen_key()
            if not self.listen_key:
                self.logger.error("Failed to get listen key for direct WebSocket connection")
                return
                
            # Start listen key keepalive task
            keepalive_task = asyncio.create_task(self.keep_alive_listen_key(self.listen_key))
            
            # Determine WebSocket URL based on market type
            if self.market_type == 'future':
                ws_url = f"wss://fstream.binance.com/ws/{self.listen_key}"
            else:
                ws_url = f"wss://stream.binance.com/ws/{self.listen_key}"
                
            self.logger.info(f"Connecting to Binance WebSocket: {ws_url}")
            
            # Connect to WebSocket
            async with websockets.connect(ws_url) as self.websocket:
                self.logger.info("Connected to Binance WebSocket")
                
                # Wait a moment to ensure connection is stable
                await asyncio.sleep(1)
                
                # Notify that WebSocket is connected and ready
                if self.on_connected:
                    asyncio.create_task(self.on_connected())
                
                # Listen for messages
                while True:
                    try:
                        message = await self.websocket.recv()
                        await self._handle_websocket_message(message)
                    except websockets.exceptions.ConnectionClosed:
                        self.logger.warning("Binance WebSocket connection closed, reconnecting...")
                        break
                    except Exception as e:
                        self.logger.error(f"Error handling Binance WebSocket message: {e}")
                        
            # Cancel keepalive task
            keepalive_task.cancel()
            try:
                await keepalive_task
            except asyncio.CancelledError:
                pass
                
        except Exception as e:
            self.logger.error(f"Error in Binance WebSocket connection: {e}")
            self.logger.error(traceback.format_exc())
            
        # If connection was broken, try to reconnect after a delay
        await asyncio.sleep(5)
        
        # Reconnect
        if not self.ws_task.cancelled():
            self.ws_task = asyncio.create_task(self._setup_websocket_connection())
    
    async def _handle_websocket_message(self, message: str):
        """Handle WebSocket message."""
        try:
            data = json.loads(message)
            
            # Normalize the message to a common format
            normalized_msg = self._normalize_message(data)
            
            # Call the on_message callback if set
            if self.on_message and normalized_msg:
                await self.on_message(normalized_msg)
                
        except json.JSONDecodeError:
            self.logger.warning(f"Received invalid JSON message: {message}")
        except Exception as e:
            self.logger.error(f"Error processing WebSocket message: {e}")
            self.logger.error(traceback.format_exc())
    
    def _normalize_message(self, message: Dict) -> Optional[Dict]:
        """Normalize WebSocket message to a common format."""
        try:
            # Common message fields
            normalized = {
                'exchange': 'binance',
                'raw': message
            }
            
            # Event type
            event_type = message.get('e')
            
            # ORDER_TRADE_UPDATE event
            if event_type == 'ORDER_TRADE_UPDATE':
                order = message.get('o', {})
                execution_type = order.get('x')
                
                normalized.update({
                    'type': 'order_update',
                    'timestamp': message.get('T'),
                    'id': order.get('i'),
                    'client_order_id': order.get('c'),
                    'status': order.get('X'),
                    'filled': float(order.get('z', 0)),
                    'remaining': float(order.get('q', 0)) - float(order.get('z', 0)),
                    'price': float(order.get('p', 0)),
                    'amount': float(order.get('q', 0)),
                    'side': order.get('S', '').lower(),
                    'symbol': order.get('s'),
                    'fee': float(order.get('n', 0)),
                    'average_price': float(order.get('ap', 0))
                })
                
                # If execution type is TRADE and order is filled or partially filled, also create a trade message
                if execution_type == 'TRADE' and order.get('X') in ['FILLED', 'PARTIALLY_FILLED']:
                    trade_msg = normalized.copy()
                    trade_msg.update({
                        'type': 'trade',
                        'id': order.get('t'),
                        'order_id': order.get('i'),
                        'price': float(order.get('L', 0)),
                        'amount': float(order.get('l', 0)),
                        'side': order.get('S', '').lower(),
                        'symbol': order.get('s'),
                        'fee': float(order.get('n', 0)),
                        'fee_currency': order.get('N'),
                        'is_maker': order.get('m', False)
                    })
                    return trade_msg
                
                return normalized
                
            # ACCOUNT_UPDATE event
            elif event_type == 'ACCOUNT_UPDATE':
                account = message.get('a', {})
                positions = account.get('P', [])
                
                # We return the first position update we find
                for position in positions:
                    position_msg = normalized.copy()
                    position_msg.update({
                        'type': 'position',
                        'timestamp': message.get('T'),
                        'symbol': position.get('s'),
                        'size': float(position.get('pa', 0)),
                        'entry_price': float(position.get('ep', 0)),
                        'unrealized_pnl': float(position.get('up', 0)),
                        'margin_type': position.get('mt')
                    })
                    return position_msg
                    
            # Unknown message type
            return None
            
        except Exception as e:
            self.logger.error(f"Error normalizing message: {e}")
            self.logger.error(traceback.format_exc())
            return None
            
    async def get_listen_key(self):
        """Get a listen key from Binance API"""
        try:
            # Create a temporary session for this request
            async with aiohttp.ClientSession() as session:
                headers = {'X-MBX-APIKEY': self.api_key}
                
                # Use the correct endpoint based on market type
                if self.market_type == 'future':
                    url = 'https://fapi.binance.com/fapi/v1/listenKey'
                else:
                    url = 'https://api.binance.com/api/v3/userDataStream'
                    
                async with session.post(url, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        listen_key = data.get('listenKey')
                        if listen_key:
                            self.logger.info("Successfully obtained Binance listen key")
                            return listen_key
                        else:
                            self.logger.error("No listen key in response")
                            return None
                    else:
                        error_text = await response.text()
                        self.logger.error(f"Failed to get listen key. Status: {response.status}, Error: {error_text}")
                        return None
                        
        except Exception as e:
            self.logger.error(f"Error getting listen key: {e}")
            self.logger.error(traceback.format_exc())
            return None
    
    async def keep_alive_listen_key(self, listen_key):
        """Keep the listen key alive by sending periodic PUT requests"""
        try:
            while True:
                await asyncio.sleep(1800)  # Refresh every 30 minutes
                
                # Create a temporary session for this request
                async with aiohttp.ClientSession() as session:
                    headers = {'X-MBX-APIKEY': self.api_key}
                    
                    # Use the correct endpoint based on market type
                    if self.market_type == 'future':
                        url = f'https://fapi.binance.com/fapi/v1/listenKey?listenKey={listen_key}'
                    else:
                        url = f'https://api.binance.com/api/v3/userDataStream?listenKey={listen_key}'
                        
                    async with session.put(url, headers=headers) as response:
                        if response.status == 200:
                            self.logger.info("Successfully refreshed Binance listen key")
                        else:
                            error_text = await response.text()
                            self.logger.error(f"Failed to refresh listen key. Status: {response.status}, Error: {error_text}")
                            break
                            
        except asyncio.CancelledError:
            self.logger.info("Listen key keep-alive task cancelled")
        except Exception as e:
            self.logger.error(f"Error in listen key keep-alive: {e}")
            self.logger.error(traceback.format_exc())
    
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
        """Close the connector and release resources."""
        # Cancel WebSocket task
        if self.ws_task and not self.ws_task.done():
            self.ws_task.cancel()
            try:
                await self.ws_task
            except asyncio.CancelledError:
                pass
                
        # Close WebSocket connection
        if self.websocket:
            await self.websocket.close()
            self.websocket = None
            
        # Close CCXT exchange
        if self.exchange:
            await self.exchange.close()
            self.exchange = None
            
        self.logger.info("Binance connector closed")
        
    def get_ws_url(self, private: bool = False) -> str:
        """Get WebSocket URL based on market type and private flag."""
        if private:
            if self.market_type == 'future':
                return f'wss://fstream.binance.com/ws/{self.listen_key}'
            else:
                return f'wss://stream.binance.com:9443/ws/{self.listen_key}'
        else:
            if self.market_type == 'future':
                return 'wss://fstream.binance.com/ws'
            else:
                return 'wss://stream.binance.com:9443/ws'
                
    async def subscribe_private(self, ws_manager: WebSocketManager):
        """Subscribe to private user data channels."""
        # For Binance, private channels are accessed via listen key in URL
        # No explicit subscription is needed
        self.logger.info("Binance private subscription via listen key established")
        
    async def process_message(self, message: str) -> Dict:
        """Process WebSocket message."""
        try:
            data = json.loads(message)
            
            # Handle different message types
            if 'e' in data:
                event_type = data['e']
                
                # Handle private events
                if event_type == 'ORDER_TRADE_UPDATE':
                    return self._process_order_update(data)
                elif event_type == 'ACCOUNT_UPDATE':
                    return self._process_account_update(data)
                # Handle public events (add as needed)
                
            return data
        except Exception as e:
            self.logger.error(f"Error processing message: {e}")
            return {}
            
    def _process_order_update(self, data: Dict) -> Dict:
        """Process ORDER_TRADE_UPDATE event."""
        try:
            order = data.get('o', {})
            
            # Extract relevant fields
            result = {
                'type': 'order_update',
                'exchange': 'binance',
                'market_type': self.market_type,
                'order_id': str(order.get('i', '')),
                'client_order_id': str(order.get('c', '')),
                'symbol': order.get('s', ''),
                'side': order.get('S', '').lower(),
                'type': order.get('o', '').lower(),
                'status': order.get('X', ''),
                'price': float(order.get('p', 0)),
                'average_price': float(order.get('ap', 0)),
                'amount': float(order.get('q', 0)),
                'filled': float(order.get('z', 0)),
                'last_filled_qty': float(order.get('l', 0)),
                'last_filled_price': float(order.get('L', 0)),
                'fee': float(order.get('n', 0)),
                'timestamp': int(data.get('E', 0)),
                'raw': data
            }
            
            return result
        except Exception as e:
            self.logger.error(f"Error processing order update: {e}")
            return {'type': 'error', 'error': str(e), 'raw': data}
            
    def _process_account_update(self, data: Dict) -> Dict:
        """Process ACCOUNT_UPDATE event."""
        try:
            account = data.get('a', {})
            
            # Extract balance updates
            balances = []
            for b in account.get('B', []):
                balances.append({
                    'asset': b.get('a', ''),
                    'free': float(b.get('f', 0)),
                    'locked': float(b.get('l', 0))
                })
                
            # Extract position updates
            positions = []
            for p in account.get('P', []):
                positions.append({
                    'symbol': p.get('s', ''),
                    'position_amount': float(p.get('pa', 0)),
                    'entry_price': float(p.get('ep', 0)),
                    'unrealized_pnl': float(p.get('up', 0)),
                    'margin_type': p.get('mt', ''),
                    'isolated_wallet': float(p.get('iw', 0)),
                    'position_side': p.get('ps', '')
                })
                
            result = {
                'type': 'account_update',
                'exchange': 'binance',
                'market_type': self.market_type,
                'event': account.get('m', ''),  # Update reason
                'timestamp': int(data.get('E', 0)),
                'balances': balances,
                'positions': positions,
                'raw': data
            }
            
            return result
        except Exception as e:
            self.logger.error(f"Error processing account update: {e}")
            return {'type': 'error', 'error': str(e), 'raw': data}
            
    def _format_binance_symbol(self, symbol: str) -> str:
        """Format symbol for Binance WebSocket API (to lowercase without slash)."""
        # Convert standard format (BTC/USDT) to Binance format (btcusdt)
        if '/' in symbol:
            formatted = symbol.replace('/', '').lower()
        else:
            formatted = symbol.lower()
            
        return formatted 