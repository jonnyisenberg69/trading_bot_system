"""
Bybit exchange connector.

Provides WebSocket and REST API functionality for the Bybit exchange.
"""

import os
import time
import asyncio
import json
import hmac
import hashlib
from typing import Dict, List, Optional, Any, Tuple, Callable
from decimal import Decimal
import ccxt.async_support as ccxt
import structlog
import aiohttp
from datetime import datetime, timedelta
import traceback
import websockets

from exchanges.websocket_manager import WebSocketManager

logger = structlog.get_logger(__name__)


class BybitWSConnector:
    """
    Bybit WebSocket connector for market data.
    
    Provides:
    - Real CCXT Bybit API integration
    - REST API access with rate limiting
    - Market data retrieval
    - Order book data
    - Support for both spot and linear contracts via V5 API
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
        Initialize Bybit connector.
        
        Args:
            api_key: API key (optional)
            api_secret: API secret (optional)
            testnet: Use testnet (default: False)
            market_type: 'spot', 'linear', or 'inverse' (default: 'spot')
            request_timeout: Request timeout in milliseconds (default: 30000)
            rate_limit_factor: Rate limit factor (0.0-1.0) to stay below limits
        """
        self.api_key = api_key or os.getenv("BYBIT_API_KEY", "")
        self.api_secret = api_secret or os.getenv("BYBIT_API_SECRET", "")
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
        
        # Logger
        self.logger = logger.bind(exchange="bybit", market_type=market_type)
        # Debug log after logger is set
        self.logger.info(f"BybitWSConnector initialized with api_key={self.api_key}, api_secret={self.api_secret}")
        
        # Initialize real CCXT exchange
        self._create_exchange()
        
        # Set canonical name for system consistency
        if self.market_type in ('linear', 'future'):
            self.name = 'bybit_perp'
        else:
            self.name = 'bybit_spot'
        
        # WebSocket connection
        self.websocket = None
        self.on_message = None  # Callback for WebSocket messages
        self.on_connected = None  # Callback for when WebSocket is connected and subscribed
        self.ws_task = None  # Task for WebSocket connection
        
    def _create_exchange(self):
        """Create real CCXT Bybit exchange instance."""
        try:
            # Bybit unified V5 API
            exchange_options = {
                'apiKey': self.api_key,
                'secret': self.api_secret,
                'sandbox': self.testnet,
                'enableRateLimit': True,
                'rateLimit': int(120 * self.rate_limit_factor),
                'timeout': self.request_timeout,
                'options': {
                    'defaultType': self.market_type,
                }
            }
            
            self.exchange = ccxt.bybit(exchange_options)
            self.logger.info(f"Bybit CCXT exchange initialized with market type: {self.market_type}")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Bybit exchange: {e}")
            raise
        
    async def initialize(self) -> None:
        """Initialize the connector."""
        self.logger.info(f"BybitWSConnector initialized with api_key={self.api_key}, api_secret={self.api_secret}")
        
        # Create CCXT exchange object
        self.logger.info("Bybit CCXT exchange initialized with market type: {market_type}", market_type=self.market_type)
        self.exchange = ccxt.bybit({
            'apiKey': self.api_key,
            'secret': self.api_secret,
            'enableRateLimit': True,
            'timeout': self.request_timeout,
            'options': {
                'defaultType': self.market_type,
                'recvWindow': 5000,
                'rateLimit': 100 / self.rate_limit_factor  # Adjust rate limit
            }
        })
        
        # Load markets
        await self.load_markets()
        
        # Set up direct WebSocket connection for monitoring
        self.ws_task = asyncio.create_task(self._setup_websocket_connection())
        
        self.logger.info("Bybit connector initialized successfully")
        
    async def _setup_websocket_connection(self):
        """Set up a direct WebSocket connection for monitoring trade messages."""
        ws_url = "wss://stream.bybit.com/v5/private"
        try:
            # Connect to WebSocket
            self.logger.info(f"Connecting to Bybit WebSocket: {ws_url}")
            
            async with websockets.connect(ws_url) as self.websocket:
                # Generate authentication parameters (simple version that works in example script)
                expires = int((time.time() + 1) * 1000)
                signature = hmac.new(
                    self.api_secret.encode('utf-8'),
                    f"GET/realtime{expires}".encode('utf-8'),
                    hashlib.sha256
                ).hexdigest()
                
                # Create auth message
                auth_message = {
                    "op": "auth", 
                    "args": [self.api_key, expires, signature]
                }
                
                # Send authentication message
                self.logger.info(f"Sending authentication message to Bybit WebSocket")
                await self.websocket.send(json.dumps(auth_message))
                
                # Wait for authentication response
                auth_response = await self.websocket.recv()
                auth_data = json.loads(auth_response)
                self.logger.info(f"Received auth response: {auth_data}")
                
                if not auth_data.get('success', False):
                    self.logger.error(f"Bybit WebSocket authentication failed: {auth_data}")
                    return
                
                self.logger.info("Bybit WebSocket authentication successful")
                
                # Wait a moment for authentication to complete
                await asyncio.sleep(1)
                
                # Subscribe to channels based on market type
                if self.market_type in ('linear', 'future', 'swap'):
                    # Linear/Futures markets
                    subscription_channels = [
                        "execution.linear",   # Trade executions 
                        "order.linear",       # Order updates
                        "position.linear"     # Position updates
                    ]
                else:
                    # Spot markets
                    subscription_channels = [
                        "execution.spot",     # Trade executions
                        "order.spot",         # Order updates
                        "wallet"              # Wallet/balance updates
                    ]
                
                # Send subscription message
                subscription_message = {
                    "op": "subscribe",
                    "args": subscription_channels
                }
                await self.websocket.send(json.dumps(subscription_message))
                self.logger.info(f"Subscribed to Bybit WebSocket channels: {subscription_channels}")
                
                # Wait for subscription response
                sub_response = await self.websocket.recv()
                sub_data = json.loads(sub_response)
                self.logger.info(f"Received subscription response: {sub_data}")
                
                if not sub_data.get('success', False):
                    self.logger.error(f"Bybit WebSocket subscription failed: {sub_data}")
                    return
                    
                self.logger.info("Bybit WebSocket subscription successful")
                
                # Notify that WebSocket is connected and subscribed
                if self.on_connected:
                    asyncio.create_task(self.on_connected())
                
                # Start listening for messages
                while True:
                    try:
                        message = await self.websocket.recv()
                        await self._handle_websocket_message(message)
                    except websockets.exceptions.ConnectionClosed:
                        self.logger.warning("Bybit WebSocket connection closed, reconnecting...")
                        break
                    except Exception as e:
                        self.logger.error(f"Error handling Bybit WebSocket message: {e}")
                        
        except Exception as e:
            self.logger.error(f"Error in Bybit WebSocket connection: {e}")
            self.logger.error(traceback.format_exc())
        
        # If connection was broken, try to reconnect after a delay
        await asyncio.sleep(5)
        
        # Reconnect
        if not self.ws_task.cancelled():
            self.ws_task = asyncio.create_task(self._setup_websocket_connection())
    
    async def _handle_websocket_message(self, message: str):
        """Handle WebSocket message."""
        try:
            # Log raw message for debugging
            self.logger.info(f"Received raw message: {message}")
            
            data = json.loads(message)
            
            # Process various message types
            if 'topic' in data:
                topic = data.get('topic', '')
                self.logger.info(f"Received message for topic: {topic}")
                
                # Normalize the message to a common format
                normalized_msg = self._normalize_message(data)
                
                # Call the on_message callback if set
                if self.on_message and normalized_msg:
                    self.logger.info(f"Forwarding normalized message to callback: {normalized_msg}")
                    await self.on_message(normalized_msg)
                elif self.on_message:
                    # If we couldn't normalize the message, still send the raw data
                    self.logger.info(f"Forwarding raw message to callback (couldn't normalize)")
                    await self.on_message({
                        'exchange': 'bybit',
                        'type': 'raw',
                        'raw': data
                    })
            else:
                # Handle other message types (pings, etc.)
                self.logger.debug(f"Received non-topic message: {data}")
                if self.on_message:
                    # Forward even non-topic messages
                    await self.on_message({
                        'exchange': 'bybit',
                        'type': 'system',
                        'raw': data
                    })
                    
        except json.JSONDecodeError:
            self.logger.warning(f"Received invalid JSON message: {message}")
        except Exception as e:
            self.logger.error(f"Error processing WebSocket message: {e}")
            self.logger.error(traceback.format_exc())
    
    def _normalize_message(self, message: Dict) -> Optional[Dict]:
        """Normalize WebSocket message to a common format."""
        try:
            topic = message.get('topic', '')
            
            # Common message fields
            normalized = {
                'exchange': 'bybit',
                'timestamp': message.get('creationTime'),
                'raw': message
            }
            
            # Order updates
            if 'order.' in topic and 'data' in message:
                order_data = message['data'][0] if isinstance(message['data'], list) and message['data'] else message['data']
                normalized.update({
                    'type': 'order_update',
                    'id': order_data.get('orderId'),
                    'client_order_id': order_data.get('orderLinkId'),
                    'status': order_data.get('orderStatus'),
                    'filled': float(order_data.get('cumExecQty', 0)),
                    'remaining': float(order_data.get('leavesQty', 0)),
                    'price': float(order_data.get('price', 0)),
                    'amount': float(order_data.get('qty', 0)),
                    'side': order_data.get('side', '').lower(),
                    'symbol': order_data.get('symbol')
                })
                return normalized
                
            # Trade executions
            elif 'execution.' in topic and 'data' in message:
                for execution in message['data']:
                    # Create a new normalized message for each execution
                    exec_msg = normalized.copy()
                    exec_msg.update({
                        'type': 'trade',
                        'id': execution.get('execId'),
                        'order_id': execution.get('orderId'),
                        'price': float(execution.get('execPrice', 0)),
                        'amount': float(execution.get('execQty', 0)),
                        'side': execution.get('side', '').lower(),
                        'symbol': execution.get('symbol'),
                        'fee': float(execution.get('execFee', 0)),
                        'fee_currency': execution.get('execFeeAsset'),
                        'is_maker': execution.get('isMaker', False)
                    })
                    return exec_msg
                    
            # Position updates
            elif 'position.' in topic and 'data' in message:
                position_data = message['data'][0] if isinstance(message['data'], list) and message['data'] else message['data']
                normalized.update({
                    'type': 'position',
                    'symbol': position_data.get('symbol'),
                    'size': float(position_data.get('size', 0)),
                    'entry_price': float(position_data.get('entryPrice', 0)),
                    'leverage': float(position_data.get('leverage', 0)),
                    'side': position_data.get('side', '').lower()
                })
                return normalized
                
            # Unknown message type
            return None
            
        except Exception as e:
            self.logger.error(f"Error normalizing message: {e}")
            self.logger.error(traceback.format_exc())
            return None
    
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
            self.logger.info("Loading Bybit markets...")
            markets = await self.exchange.load_markets(reload=True)
            
            # Update cache
            self.markets_cache = markets
            self.symbols_cache = set(markets.keys())
            self.last_markets_update = now
            
            # Count spot vs futures markets
            spot_count = sum(1 for m in markets.values() if m.get('spot', False))
            swap_count = sum(1 for m in markets.values() if m.get('swap', False))
            
            self.logger.info(f"Loaded {len(markets)} Bybit markets "
                           f"(spot: {spot_count}, swap: {swap_count})")
            return markets
            
        except Exception as e:
            self.logger.error(f"Failed to load Bybit markets: {e}")
            raise
        
    def get_ws_url(self) -> str:
        """Get WebSocket URL for Bybit V5 API."""
        # Bybit uses the same WebSocket endpoint for all market types in V5 API
        if self.testnet:
            return "wss://stream-testnet.bybit.com/v5/private"
        else:
            return "wss://stream.bybit.com/v5/private"
            
    def generate_signature(self, timestamp: int) -> str:
        """Generate signature for Bybit V5 WebSocket authentication."""
        if not self.api_key or not self.api_secret:
            return "", ""
            
        # Bybit V5 API signature format
        message = f"{timestamp}{self.api_key}5000"
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            message.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        
        return signature
        
    async def subscribe_private(self, ws_manager: WebSocketManager) -> bool:
        """Subscribe to private Bybit V5 channels."""
        self.logger.info(f"[subscribe_private] api_key={self.api_key}, api_secret={self.api_secret}")
        if not self.api_key or not self.api_secret:
            self.logger.warning("No API credentials provided for Bybit private channels")
            return False
            
        try:
            # Find the WebSocket connection for this exchange
            connections = ws_manager.connections
            conn_id = None
            exchange_name = self.name
            
            for cid, conn_info in connections.items():
                if conn_info.get('exchange_name') == exchange_name:
                    conn_id = cid
                    break
            
            if not conn_id:
                self.logger.error(f"No WebSocket connection found for {exchange_name}")
                return False
                
            # Determine subscription channels based on market type
            if self.market_type in ('linear', 'future', 'swap'):
                # Linear/Futures markets
                subscription_channels = [
                    "execution.linear",   # Trade executions 
                    "order.linear",       # Order updates
                    "position.linear"     # Position updates
                ]
            else:
                # Spot markets
                subscription_channels = [
                    "execution.spot",     # Trade executions
                    "order.spot",         # Order updates
                    "wallet"              # Wallet/balance updates
                ]
            
            # Get the WebSocket connection
            ws = ws_manager._get_connection(conn_id)
            if not ws:
                self.logger.error(f"WebSocket connection {conn_id} not found")
                return False
                
            # Subscribe to all channels
            subscription_message = {
                "op": "subscribe",
                "args": subscription_channels
            }
            
            # Send subscription message
            await ws.send_str(json.dumps(subscription_message))
            self.logger.info(f"Sent subscription message to {exchange_name} WebSocket: {subscription_channels}")
            
            # Wait for subscription to take effect
            await asyncio.sleep(1)
            
            return True
        except Exception as e:
            self.logger.error(f"Error subscribing to Bybit private channels: {e}")
            self.logger.error(traceback.format_exc())
            return False
            
    async def process_message(self, message: str) -> Dict:
        """Process WebSocket message."""
        try:
            data = json.loads(message)
            
            # Handle different message types
            if 'topic' in data:
                topic = data['topic']
                
                # Parse execution updates
                if topic.startswith('execution.'):
                    return self._process_execution_update(data)
                
                # Parse position updates
                elif topic.startswith('position.'):
                    return self._process_position_update(data)
                
                # Parse order updates
                elif topic == 'order':
                    return self._process_order_update(data)
                
                # Parse wallet updates
                elif topic == 'wallet':
                    return self._process_wallet_update(data)
                    
            # Handle operation confirmations
            elif 'op' in data:
                if data['op'] == 'auth':
                    success = data.get('success', False)
                    if success:
                        self.logger.info("Bybit authentication successful")
                    else:
                        self.logger.error(f"Bybit authentication failed: {data}")
                        
                elif data['op'] == 'subscribe':
                    success = data.get('success', False)
                    if success:
                        self.logger.info(f"Bybit subscription successful: {data.get('args', [])}")
                    else:
                        self.logger.error(f"Bybit subscription failed: {data}")
                        
            return data
            
        except Exception as e:
            self.logger.error(f"Error processing Bybit message: {e}")
            return {}
            
    def _process_execution_update(self, data: Dict) -> Dict:
        """Process execution update from Bybit V5 API."""
        try:
            executions = data.get('data', [])
            if not executions:
                return {'type': 'empty_execution', 'raw': data}
                
            # Create a normalized trade execution format
            results = []
            for exec_data in executions:
                # Extract and normalize fields
                execution = {
                    'type': 'execution',
                    'exchange': 'bybit',
                    'market_type': self.market_type,
                    'order_id': exec_data.get('orderId', ''),
                    'order_link_id': exec_data.get('orderLinkId', ''),
                    'symbol': exec_data.get('symbol', ''),
                    'side': exec_data.get('side', '').lower(),
                    'price': float(exec_data.get('execPrice', 0)),
                    'size': float(exec_data.get('execQty', 0)),
                    'fee': float(exec_data.get('execFee', 0)),
                    'fee_currency': exec_data.get('execFeeId', ''),
                    'timestamp': exec_data.get('execTime', 0),
                    'is_maker': exec_data.get('isMaker', False),
                    'raw': exec_data
                }
                results.append(execution)
                
            return {
                'type': 'executions',
                'exchange': 'bybit',
                'market_type': self.market_type,
                'topic': data.get('topic', ''),
                'timestamp': data.get('ts', 0),
                'executions': results,
                'raw': data
            }
            
        except Exception as e:
            self.logger.error(f"Error processing Bybit execution update: {e}")
            return {'type': 'error', 'error': str(e), 'raw': data}
            
    def _process_position_update(self, data: Dict) -> Dict:
        """Process position update from Bybit V5 API."""
        try:
            positions = data.get('data', [])
            if not positions:
                return {'type': 'empty_position', 'raw': data}
                
            # Create a normalized position format
            results = []
            for pos_data in positions:
                # Extract and normalize fields
                position = {
                    'type': 'position',
                    'exchange': 'bybit',
                    'market_type': self.market_type,
                    'symbol': pos_data.get('symbol', ''),
                    'side': pos_data.get('side', ''),
                    'size': float(pos_data.get('size', 0)),
                    'entry_price': float(pos_data.get('entryPrice', 0)),
                    'leverage': float(pos_data.get('leverage', 0)),
                    'position_value': float(pos_data.get('positionValue', 0)),
                    'bust_price': float(pos_data.get('bustPrice', 0)),
                    'mark_price': float(pos_data.get('markPrice', 0)),
                    'unrealized_pnl': float(pos_data.get('unrealisedPnl', 0)),
                    'raw': pos_data
                }
                results.append(position)
                
            return {
                'type': 'positions',
                'exchange': 'bybit',
                'market_type': self.market_type,
                'topic': data.get('topic', ''),
                'timestamp': data.get('ts', 0),
                'positions': results,
                'raw': data
            }
            
        except Exception as e:
            self.logger.error(f"Error processing Bybit position update: {e}")
            return {'type': 'error', 'error': str(e), 'raw': data}
            
    def _process_order_update(self, data: Dict) -> Dict:
        """Process order update from Bybit V5 API."""
        try:
            orders = data.get('data', [])
            if not orders:
                return {'type': 'empty_order', 'raw': data}
                
            # Create a normalized order format
            results = []
            for order_data in orders:
                # Extract and normalize fields
                order = {
                    'type': 'order',
                    'exchange': 'bybit',
                    'market_type': self.market_type,
                    'order_id': order_data.get('orderId', ''),
                    'order_link_id': order_data.get('orderLinkId', ''),
                    'symbol': order_data.get('symbol', ''),
                    'side': order_data.get('side', '').lower(),
                    'order_type': order_data.get('orderType', '').lower(),
                    'price': float(order_data.get('price', 0)),
                    'qty': float(order_data.get('qty', 0)),
                    'filled_qty': float(order_data.get('cumExecQty', 0)),
                    'avg_price': float(order_data.get('avgPrice', 0)),
                    'status': order_data.get('orderStatus', ''),
                    'time_in_force': order_data.get('timeInForce', ''),
                    'create_time': order_data.get('createTime', 0),
                    'update_time': order_data.get('updateTime', 0),
                    'raw': order_data
                }
                results.append(order)
                
            return {
                'type': 'orders',
                'exchange': 'bybit',
                'market_type': self.market_type,
                'topic': data.get('topic', ''),
                'timestamp': data.get('ts', 0),
                'orders': results,
                'raw': data
            }
            
        except Exception as e:
            self.logger.error(f"Error processing Bybit order update: {e}")
            return {'type': 'error', 'error': str(e), 'raw': data}
            
    def _process_wallet_update(self, data: Dict) -> Dict:
        """Process wallet update from Bybit V5 API."""
        try:
            wallets = data.get('data', [])
            if not wallets:
                return {'type': 'empty_wallet', 'raw': data}
                
            # Create a normalized wallet format
            results = []
            for wallet_data in wallets:
                # Extract and normalize fields
                wallet = {
                    'type': 'wallet',
                    'exchange': 'bybit',
                    'coin': wallet_data.get('coin', ''),
                    'equity': float(wallet_data.get('equity', 0)),
                    'available_balance': float(wallet_data.get('availableBalance', 0)),
                    'used_margin': float(wallet_data.get('usedMargin', 0)),
                    'order_margin': float(wallet_data.get('orderMargin', 0)),
                    'position_margin': float(wallet_data.get('positionMargin', 0)),
                    'wallet_balance': float(wallet_data.get('walletBalance', 0)),
                    'realized_pnl': float(wallet_data.get('realisedPnl', 0)),
                    'unrealized_pnl': float(wallet_data.get('unrealisedPnl', 0)),
                    'raw': wallet_data
                }
                results.append(wallet)
                
            return {
                'type': 'wallets',
                'exchange': 'bybit',
                'market_type': self.market_type,
                'topic': data.get('topic', ''),
                'timestamp': data.get('ts', 0),
                'wallets': results,
                'raw': data
            }
            
        except Exception as e:
            self.logger.error(f"Error processing Bybit wallet update: {e}")
            return {'type': 'error', 'error': str(e), 'raw': data}
            
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
                'exchange': 'bybit',
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
                'exchange': 'bybit',
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
                    'exchange': 'bybit',
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
                    'exchange': 'bybit',
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
        """Close all connections and resources."""
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
                
            self.logger.info("Bybit connector closed")
            
        except Exception as e:
            self.logger.error(f"Error closing Bybit connector: {e}")
            
    def _format_bybit_symbol(self, symbol: str) -> str:
        """Format symbol for Bybit WebSocket API."""
        # Convert standard format (BTC/USDT) to Bybit format
        if '/' in symbol:
            if self.market_type == 'linear':
                base, quote = symbol.split('/')
                formatted = f"{base}{quote}"
            else:
                formatted = symbol.replace('/', '')
        else:
            formatted = symbol
            
        return formatted 