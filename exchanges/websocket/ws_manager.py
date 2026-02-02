"""
WebSocket manager for exchange connections.

Manages WebSocket connections to exchanges for real-time market data
with automatic reconnection and message handling.
"""

import asyncio
import json
import logging
import time
import uuid
import hmac
import hashlib
import base64
import traceback
from typing import Dict, List, Optional, Any, Set, Callable, Coroutine, Tuple
from dataclasses import dataclass
from enum import Enum

import websockets
from websockets.exceptions import ConnectionClosed
import aiohttp

from .reconnection import ReconnectionManager
from ..base_connector import BaseExchangeConnector

# Define WebSocket states and message types here to avoid circular imports
class WSState:
    """WebSocket connection states."""
    DISCONNECTED = 'disconnected'
    CONNECTING = 'connecting'
    CONNECTED = 'connected'
    RECONNECTING = 'reconnecting'
    ERROR = 'error'

class WSMessageType:
    """WebSocket message types."""
    SUBSCRIBE = 'subscribe'
    UNSUBSCRIBE = 'unsubscribe'
    ORDERBOOK = 'orderbook'
    TRADE = 'trade'
    ORDER_UPDATE = 'order_update'
    HEARTBEAT = 'heartbeat'
    ERROR = 'error'

logger = logging.getLogger(__name__)


@dataclass
class WebSocketConfig:
    """Configuration for WebSocket connections."""
    ping_interval: float = 30.0      # Seconds between ping messages
    pong_timeout: float = 10.0       # Seconds to wait for pong response
    close_timeout: float = 5.0       # Seconds to wait for close confirmation
    message_timeout: float = 30.0    # Seconds to wait for a response message
    reconnect_enabled: bool = True   # Enable automatic reconnection
    max_message_size: int = 10485760  # 10MB max message size
    
    # MEXC-specific improvements
    mexc_ping_interval: float = 20.0  # More frequent pings for MEXC
    mexc_connection_timeout: float = 10.0  # Connection timeout for MEXC


class WebSocketManager:
    """
    Manages WebSocket connections to exchanges.
    
    Features:
    - Multi-exchange support
    - Automatic reconnection with backoff
    - Subscription management
    - Message routing to handlers
    - Connection health monitoring
    """
    
    def __init__(self, config: Optional[Dict] = None):
        """
        Initialize WebSocket manager.
        
        Args:
            config: Configuration dictionary for WebSocket behavior
        """
        # Default configuration
        default_config = {
            'ping_interval': 30.0,
            'pong_timeout': 10.0,
            'close_timeout': 5.0,
            'message_timeout': 30.0,
            'reconnect_enabled': True,
            'max_message_size': 10485760  # 10MB
        }
        
        # Update with provided config
        if config:
            default_config.update(config)
        
        self.config = WebSocketConfig(**default_config)
        
        # Reconnection manager
        self.reconnection_manager = ReconnectionManager()
        
        # WebSocket connection state
        self.connections: Dict[str, Dict[str, Any]] = {}
        self.connection_tasks: Dict[str, asyncio.Task] = {}
        
        # CRITICAL FIX: Track connections by exchange+type to prevent duplicates
        self.connection_lookup: Dict[str, str] = {}  # key (exchange_type_url) -> conn_id
        
        # Subscription management
        self.subscriptions: Dict[str, Dict[str, Set[str]]] = {}  # conn_id -> channel -> set(symbols)
        
        # Message handlers
        self.message_handlers: Dict[str, List[Callable]] = {}
        
        # Internal state
        self._running = False
        self._stop_event = asyncio.Event()
        
        self.logger = logging.getLogger(f"{__name__}.WebSocketManager")
    
    async def start(self):
        """Start the WebSocket manager."""
        if self._running:
            self.logger.warning("WebSocket manager already running")
            return
        
        self._running = True
        self._stop_event.clear()
        
        self.logger.info("WebSocket manager started")
    
    async def stop(self):
        """Stop the WebSocket manager and close all connections."""
        if not self._running:
            self.logger.warning("WebSocket manager not running")
            return
        
        self._running = False
        self._stop_event.set()
        
        # Close all connections
        close_tasks = []
        for conn_id in list(self.connections.keys()):
            close_tasks.append(self.close_connection(conn_id))
        
        if close_tasks:
            self.logger.info(f"Closing {len(close_tasks)} WebSocket connections...")
            await asyncio.gather(*close_tasks, return_exceptions=True)
        
        self.logger.info("WebSocket manager stopped")
    
    async def connect_exchange(
        self, 
        exchange: 'BaseExchangeConnector',
        url: str,
        conn_type: str = 'public',
        use_reconnect: bool = True
    ) -> Optional[str]:
        """
        Connect to an exchange WebSocket endpoint.
        
        Args:
            exchange: Exchange connector instance
            url: WebSocket URL
            conn_type: Connection type ('public' or 'private')
            use_reconnect: Whether to use automatic reconnection
            
        Returns:
            Connection ID or None if connection failed
        """
        # Derive canonical exchange name using internal helper
        exchange_name = self._get_exchange_name(exchange)
        self.logger.info(f"Connecting to {exchange_name} WebSocket: {url}")
        
        # CRITICAL FIX: Check for existing connection before creating new one
        connection_key = f"{exchange_name}_{conn_type}_{url}"
        if connection_key in self.connection_lookup:
            existing_conn_id = self.connection_lookup[connection_key]
            if existing_conn_id in self.connections:
                conn_info = self.connections[existing_conn_id]
                # Check if connection is healthy
                if conn_info.get('state') in [WSState.CONNECTED, WSState.CONNECTING]:
                    self.logger.warning(f"⚠️ REUSING EXISTING CONNECTION for {exchange_name} {conn_type} (conn_id: {existing_conn_id})")
                    return existing_conn_id
                else:
                    self.logger.info(f"Existing connection {existing_conn_id} is in state {conn_info.get('state')}, creating new one")
                    # Remove old connection from lookup
                    del self.connection_lookup[connection_key]
        
        auth_data = None
        if conn_type == 'private':
            try:
                # Get exchange name from the connector (must have 'name' attribute)
                if not hasattr(exchange, 'name'):
                    self.logger.error(f"Exchange connector missing 'name' attribute: {exchange}")
                    return None
                
                # Prepare authentication data
                auth_data = await self._prepare_private_authentication(exchange)
                if not auth_data:
                    self.logger.error(f"Failed to prepare authentication for {exchange_name}")
                    return None
            except Exception as e:
                self.logger.error(f"Error preparing authentication for {exchange_name}: {e}")
                return None
        
        # Adjust URL if authentication returned a specific endpoint suffix (e.g., Binance listenKey)
        connect_url = url
        if conn_type == 'private' and auth_data and 'endpoint_suffix' in auth_data:
            # For exchanges like Binance that require the listenKey appended to the base URL
            connect_url = f"{url}{auth_data['endpoint_suffix']}"
        
        # Connect to WebSocket
        conn_id = await self.connect(connect_url, use_reconnect=use_reconnect)
        if not conn_id:
            self.logger.error(f"Failed to connect to {exchange_name} WebSocket")
            return None
        
        # CRITICAL FIX: Store connection in lookup table
        self.connection_lookup[connection_key] = conn_id
        self.logger.info(f"✅ Registered new connection {conn_id} for {connection_key}")
            
        # Store exchange info with the connection
        conn_info = self.connections[conn_id]
        conn_info['exchange'] = exchange
        conn_info['exchange_name'] = exchange_name
        conn_info['conn_type'] = conn_type
        conn_info['endpoint'] = url  # store for reconnection
        conn_info['auth_data'] = auth_data  # CRITICAL: Store auth data for reconnection!
        
        # Wait until the _connection_handler establishes WebSocket object
        start_time = time.time()
        max_wait = self.config.message_timeout  # default 30 seconds for handshake
        while time.time() - start_time < max_wait:
            if conn_info.get('state') == WSState.CONNECTED and conn_info.get('ws'):
                break
            await asyncio.sleep(0.05)

        if conn_info.get('state') != WSState.CONNECTED or conn_info.get('ws') is None:
            self.logger.error(
                f"WebSocket handshake for {exchange_name} not completed within {max_wait}s"
            )
            await self.close_connection(conn_id)
            return None
        
        # Authenticate if needed
        if conn_type == 'private' and auth_data:
            # Authenticate the connection
            success = await self._authenticate_exchange(conn_id, exchange_name, auth_data)
            if not success:
                self.logger.error(f"Failed to authenticate {exchange_name} WebSocket")
                await self.close_connection(conn_id)
                return None
        
        return conn_id
    
    async def close_connection(self, conn_id: str) -> bool:
        """
        Close a WebSocket connection.
        
        Args:
            conn_id: Connection ID to close
            
        Returns:
            True if closed successfully, False otherwise
        """
        if conn_id not in self.connections:
            self.logger.warning(f"Connection {conn_id} not found")
            return False
        
        conn_info = self.connections[conn_id]
        
        try:
            # Cancel connection task
            if conn_id in self.connection_tasks:
                task = self.connection_tasks[conn_id]
                if not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                del self.connection_tasks[conn_id]
            
            # Close WebSocket
            ws = conn_info.get('ws')
            if ws:
                try:
                    # Check if closed attribute exists and websocket is not closed
                    if hasattr(ws, 'closed') and not ws.closed:
                        await ws.close(code=1000, reason="Connection closed by client")
                    # For websocket implementations that don't have closed attribute
                    elif not hasattr(ws, 'closed'):
                        self.logger.debug(f"WebSocket for {conn_id} doesn't have 'closed' attribute, attempting close")
                        try:
                            await ws.close(code=1000, reason="Connection closed by client")
                        except Exception as e:
                            self.logger.debug(f"Close operation error (ignorable): {e}")
                except Exception as e:
                    self.logger.debug(f"Error during websocket close for {conn_id}: {e}")
            
            # Clean up
            if conn_id in self.subscriptions:
                del self.subscriptions[conn_id]
            
            # CRITICAL FIX: Remove from connection lookup
            keys_to_remove = []
            for key, cid in self.connection_lookup.items():
                if cid == conn_id:
                    keys_to_remove.append(key)
            for key in keys_to_remove:
                del self.connection_lookup[key]
                self.logger.debug(f"Removed connection lookup for {key}")
            
            del self.connections[conn_id]
            
            self.logger.info(f"Closed connection {conn_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error closing connection {conn_id}: {e}")
            return False
    
    async def subscribe(self, conn_id: str, channel: str, symbol: str) -> bool:
        """
        Subscribe to a channel for a symbol.
        
        Args:
            conn_id: Connection ID
            channel: Channel name (e.g., 'orderbook', 'trades')
            symbol: Trading symbol (e.g., 'BTC/USDT')
            
        Returns:
            True if subscription request sent, False otherwise
        """
        if conn_id not in self.connections:
            self.logger.error(f"Connection {conn_id} not found")
            return False
        
        conn_info = self.connections[conn_id]
        if conn_info['state'] != WSState.CONNECTED:
            self.logger.warning(
                f"Connection {conn_id} not connected (state: {conn_info['state']})"
            )
            # Queue subscription for when connection is established
            if channel not in self.subscriptions[conn_id]:
                self.subscriptions[conn_id][channel] = set()
            self.subscriptions[conn_id][channel].add(symbol)
            return False
        
        exchange = conn_info['exchange']
        
        try:
            # Get exchange-specific subscription message
            sub_message = self._get_subscription_message(
                exchange.name, channel, symbol, action='subscribe'
            )
            
            if not sub_message:
                self.logger.error(
                    f"Failed to generate subscription message for "
                    f"{exchange.name} {channel} {symbol}"
                )
                return False
            
            # Send subscription message
            ws = conn_info['ws']
            if isinstance(sub_message, dict):
                await ws.send_str(json.dumps(sub_message))
            else:
                await ws.send_str(sub_message)
            
            # Track subscription
            if channel not in self.subscriptions[conn_id]:
                self.subscriptions[conn_id][channel] = set()
            self.subscriptions[conn_id][channel].add(symbol)
            
            self.logger.info(
                f"Subscribed to {channel} for {symbol} on {exchange.name}"
            )
            return True
            
        except Exception as e:
            self.logger.error(
                f"Error subscribing to {channel} for {symbol} on {exchange.name}: {e}"
            )
            return False
    
    async def unsubscribe(self, conn_id: str, channel: str, symbol: str) -> bool:
        """
        Unsubscribe from a channel for a symbol.
        
        Args:
            conn_id: Connection ID
            channel: Channel name
            symbol: Trading symbol
            
        Returns:
            True if unsubscription request sent, False otherwise
        """
        if conn_id not in self.connections:
            self.logger.error(f"Connection {conn_id} not found")
            return False
        
        conn_info = self.connections[conn_id]
        if conn_info['state'] != WSState.CONNECTED:
            self.logger.warning(
                f"Connection {conn_id} not connected (state: {conn_info['state']})"
            )
            # Remove from tracked subscriptions
            if (channel in self.subscriptions[conn_id] and 
                symbol in self.subscriptions[conn_id][channel]):
                self.subscriptions[conn_id][channel].remove(symbol)
            return False
        
        exchange = conn_info['exchange']
        
        try:
            # Get exchange-specific unsubscription message
            unsub_message = self._get_subscription_message(
                exchange.name, channel, symbol, action='unsubscribe'
            )
            
            if not unsub_message:
                self.logger.error(
                    f"Failed to generate unsubscription message for "
                    f"{exchange.name} {channel} {symbol}"
                )
                return False
            
            # Send unsubscription message
            ws = conn_info['ws']
            if isinstance(unsub_message, dict):
                await ws.send_str(json.dumps(unsub_message))
            else:
                await ws.send_str(unsub_message)
            
            # Update subscription tracking
            if (channel in self.subscriptions[conn_id] and 
                symbol in self.subscriptions[conn_id][channel]):
                self.subscriptions[conn_id][channel].remove(symbol)
            
            self.logger.info(
                f"Unsubscribed from {channel} for {symbol} on {exchange.name}"
            )
            return True
            
        except Exception as e:
            self.logger.error(
                f"Error unsubscribing from {channel} for {symbol} on {exchange.name}: {e}"
            )
            return False
    
    def register_handler(self, message_type: str, handler: Callable) -> None:
        """
        Register a message handler for a specific message type.
        
        Args:
            message_type: Type of message to handle
            handler: Async function to call when message is received
        """
        if message_type not in self.message_handlers:
            self.message_handlers[message_type] = []
        
        self.message_handlers[message_type].append(handler)
        handler_count = len(self.message_handlers[message_type])
        self.logger.info(f"📝 Registered handler for {message_type} messages (total handlers: {handler_count})")
        
        # Log all registered message types for debugging
        all_types = list(self.message_handlers.keys())
        self.logger.debug(f"All registered message types: {all_types}")
    
    def get_connection_status(self, conn_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Get connection status.
        
        Args:
            conn_id: Connection ID, None for all connections
        
        Returns:
            Connection status dictionary
        """
        if conn_id:
            if conn_id not in self.connections:
                return {'error': f"Connection {conn_id} not found"}
            
            conn_info = self.connections[conn_id]
            return {
                'conn_id': conn_id,
                'exchange': conn_info['exchange'].name,
                'conn_type': conn_info['conn_type'],
                'state': conn_info['state'],
                'last_message': conn_info['last_message_time'],
                'subscriptions': self._get_subscription_summary(conn_id),
                'error': str(conn_info.get('error')) if conn_info.get('error') else None,
                'uptime': time.time() - conn_info['created_at']
            }
        else:
            # Return all connections
            result = {}
            for cid, info in self.connections.items():
                result[cid] = {
                    'exchange': info['exchange'].name,
                    'conn_type': info['conn_type'],
                    'state': info['state'],
                    'subscriptions': self._get_subscription_summary(cid),
                    'error': str(info.get('error')) if info.get('error') else None
                }
            return result
    
    async def _connection_handler(self, conn_id: str, url: str):
        """
        Handle WebSocket connection lifecycle.
        
        Args:
            conn_id: Connection ID
            url: WebSocket URL
        """
        reconnect_delay = 1  # Start with 1 second
        max_reconnect_delay = 60  # Maximum 60 seconds
        
        while True:
            try:
                # Get connection info
                conn_info = self.connections.get(conn_id)
                if not conn_info:
                    self.logger.error(f"Connection {conn_id} not found")
                    return
                
                # Check if connection should be closed
                if conn_info.get('state') == 'CLOSING':
                    self.logger.info(f"Connection {conn_id} is closing")
                    return
                
                # Update state
                conn_info['state'] = WSState.CONNECTING
                
                # Connect to WebSocket
                self.logger.info(f"Connecting to WebSocket: {url}")
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(url) as ws:
                        # Update connection info
                        conn_info['ws'] = ws
                        conn_info['state'] = WSState.CONNECTED
                        conn_info['last_message_time'] = time.time()
                        
                        # CRITICAL FIX: Re-authenticate if this is a private connection
                        conn_type = conn_info.get('conn_type', 'public')
                        exchange_name = self._get_exchange_name(conn_info['exchange'])
                        
                        if conn_type == 'private':
                            self.logger.critical(f"RE-AUTHENTICATING {exchange_name} private connection after reconnect")
                            auth_success = await self._authenticate_private_connection(conn_id, ws)
                            if not auth_success:
                                self.logger.error(f"Failed to re-authenticate {exchange_name} private connection")
                                break
                            
                            # Subscribe to private channels after authentication
                            self.logger.critical(f"RE-SUBSCRIBING {exchange_name} to private channels")
                            # Get symbols from connection info
                            symbols = conn_info.get('symbols', None)
                            sub_success = await self._subscribe_to_private_channels(conn_id, ws, symbols)
                            if not sub_success:
                                self.logger.warning(f"Failed to subscribe to some private channels for {exchange_name}")
                        
                        # Re-subscribe to all previously subscribed channels
                        if conn_id in self.subscriptions:
                            self.logger.info(f"Re-subscribing to {len(self.subscriptions[conn_id])} channels")
                            await self._resubscribe_all(conn_id)
                        
                        # Start ping task
                        ping_task = asyncio.create_task(self._ping_loop(conn_id))
                        
                        # CRITICAL: Start listenKey renewal for Binance private connections
                        listenkey_task = None
                        if conn_type == 'private' and 'binance' in exchange_name.lower():
                            listenkey_task = asyncio.create_task(self._binance_listenkey_renewal_loop(conn_id))
                        
                        # CRITICAL: Start listenKey renewal for MEXC private connections
                        mexc_listenkey_task = None
                        if conn_type == 'private' and 'mexc' in exchange_name.lower():
                            auth_data = conn_info.get('auth_data', {})
                            if auth_data.get('type') == 'listen_key':
                                mexc_listenkey_task = asyncio.create_task(self._mexc_listenkey_renewal_loop(conn_id))
                        
                        # Listen for messages
                        try:
                            async for msg in ws:
                                if msg.type == aiohttp.WSMsgType.TEXT:
                                    await self._handle_message(conn_id, msg.data)
                                elif msg.type == aiohttp.WSMsgType.ERROR:
                                    self.logger.error(f"WebSocket error: {ws.exception()}")
                                    break
                                elif msg.type == aiohttp.WSMsgType.CLOSED:
                                    self.logger.info(f"WebSocket closed")
                                    break
                                
                                # Update last message time
                                conn_info['last_message_time'] = time.time()
                        finally:
                            # Cancel ping task
                            ping_task.cancel()
                            if listenkey_task:
                                listenkey_task.cancel()
                            if mexc_listenkey_task:
                                mexc_listenkey_task.cancel()
                
                # Reset reconnect delay on successful connection
                reconnect_delay = 1
                
                # Check if reconnection is enabled
                if not conn_info.get('use_reconnect', True):
                    self.logger.info(f"Reconnection disabled for {conn_id}, not reconnecting")
                    return
                    
                # Update state
                conn_info['state'] = WSState.RECONNECTING
                self.logger.info(f"WebSocket disconnected, reconnecting in {reconnect_delay}s")
                
            except asyncio.CancelledError:
                self.logger.info(f"Connection task for {conn_id} cancelled")
                return
            except Exception as e:
                self.logger.error(f"WebSocket connection error: {e}")
                
                # Check if reconnection is enabled
                conn_info = self.connections.get(conn_id)
                if not conn_info or not conn_info.get('use_reconnect', True):
                    self.logger.info(f"Reconnection disabled for {conn_id}, not reconnecting")
                    return
                
                # Update state
                conn_info['state'] = WSState.RECONNECTING
                self.logger.info(f"Reconnecting in {reconnect_delay}s")
            
            # Wait before reconnecting
            await asyncio.sleep(reconnect_delay)
            
            # Increase reconnect delay with exponential backoff
            reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
    
    async def _ping_loop(self, conn_id: str) -> None:
        """Send periodic ping messages to keep connection alive."""
        if conn_id not in self.connections:
            return
            
        conn_info = self.connections[conn_id]
        exchange_name = self._get_exchange_name(conn_info['exchange'])
        
        # CRITICAL FIX: Use exchange-specific ping intervals
        if 'mexc' in exchange_name.lower():
            ping_interval = self.config.mexc_ping_interval  # 20s for MEXC
        elif 'bybit' in exchange_name.lower():
            ping_interval = 20.0  # Bybit requires 20s
        elif 'binance' in exchange_name.lower():
            ping_interval = 180.0  # Binance can go 3 minutes between pings
        elif 'gateio' in exchange_name.lower():
            ping_interval = 25.0  # Gate.io needs <30s
        elif 'bitget' in exchange_name.lower():
            ping_interval = 25.0  # Bitget also prefers frequent pings
        else:
            ping_interval = self.config.ping_interval  # 30s default
        
        self.logger.debug(f"Starting ping loop for {conn_id} ({exchange_name}) with interval: {ping_interval}s")
        
        try:
            while conn_info.get('state') == WSState.CONNECTED:
                await asyncio.sleep(ping_interval)
                
                # Check if connection is still active
                if conn_info.get('state') != WSState.CONNECTED:
                    break
                
                ws = conn_info.get('ws') or conn_info.get('websocket')
                if not ws or ws.closed:
                    break
                
                try:
                    # Send exchange-specific ping
                    base_exchange = exchange_name.split('_')[0] if '_' in exchange_name else exchange_name
                    
                    if 'mexc' in base_exchange:
                        # MEXC uses JSON ping
                        ping_msg = {"method": "PING"}
                        await ws.send_str(json.dumps(ping_msg))
                    elif 'bybit' in base_exchange:
                        # Bybit uses op ping
                        ping_msg = {"op": "ping"}
                        await ws.send_str(json.dumps(ping_msg))
                    elif 'gateio' in base_exchange:
                        # Gate.io uses channel ping
                        ping_msg = {
                            "time": int(time.time()),
                            "channel": "spot.ping"
                        }
                        await ws.send_str(json.dumps(ping_msg))
                    elif 'bitget' in base_exchange:
                        # Bitget uses simple ping string
                        await ws.send_str("ping")
                    elif 'hyperliquid' in base_exchange:
                        # Hyperliquid uses method ping
                        ping_msg = {"method": "ping"}
                        await ws.send_str(json.dumps(ping_msg))
                    else:
                        # Standard WebSocket ping (Binance and others)
                        await ws.ping()
                    
                    self.logger.debug(f"Sent ping to {conn_id}")
                    
                except Exception as e:
                    self.logger.warning(f"Failed to send ping to {conn_id}: {e}")
                    break
                    
        except asyncio.CancelledError:
            self.logger.debug(f"Ping loop cancelled for {conn_id}")
        except Exception as e:
            self.logger.error(f"Ping loop error for {conn_id}: {e}")
    
    async def _binance_listenkey_renewal_loop(self, conn_id: str) -> None:
        """Renew Binance listenKey every 30 minutes to keep connection alive."""
        if conn_id not in self.connections:
            return
            
        conn_info = self.connections[conn_id]
        auth_data = conn_info.get('auth_data', {})
        listen_key = auth_data.get('listenKey')
        
        if not listen_key:
            self.logger.warning(f"No listenKey found for Binance connection {conn_id}")
            return
        
        self.logger.info(f"Starting Binance listenKey renewal loop for {conn_id}")
        
        try:
            while conn_info.get('state') == WSState.CONNECTED:
                # Wait 30 minutes before renewal
                await asyncio.sleep(1800)  # 30 minutes
                
                # Check if connection is still active
                if conn_info.get('state') != WSState.CONNECTED:
                    break
                
                try:
                    # Get the exchange connector
                    exchange = conn_info.get('exchange')
                    if not exchange:
                        self.logger.error("No exchange connector found for listenKey renewal")
                        break
                    
                    # Renew the listenKey
                    ccxt_exchange = exchange.exchange
                    
                    # Send PUT request to renew listenKey
                    result = await ccxt_exchange.publicPutUserDataStream({
                        'listenKey': listen_key
                    })
                    
                    self.logger.info(f"Successfully renewed Binance listenKey for {conn_id}")
                    
                except Exception as e:
                    self.logger.error(f"Failed to renew Binance listenKey: {e}")
                    # Connection may need to be re-established
                    break
                    
        except asyncio.CancelledError:
            self.logger.info(f"Binance listenKey renewal loop cancelled for {conn_id}")
        except Exception as e:
            self.logger.error(f"Error in Binance listenKey renewal loop: {e}")

    async def _mexc_listenkey_renewal_loop(self, conn_id: str) -> None:
        """Renew MEXC listenKey every 30 minutes to keep connection alive."""
        if conn_id not in self.connections:
            return
            
        conn_info = self.connections[conn_id]
        auth_data = conn_info.get('auth_data', {})
        listen_key = auth_data.get('listenKey')
        api_key = auth_data.get('apiKey')
        secret = auth_data.get('secret')
        
        if not listen_key or not api_key or not secret:
            self.logger.warning(f"Missing credentials for MEXC listenKey renewal {conn_id}")
            return
        
        self.logger.info(f"Starting MEXC listenKey renewal loop for {conn_id}")
        
        try:
            while conn_info.get('state') == WSState.CONNECTED:
                # Wait 30 minutes before renewal (MEXC documentation states listenKey expires after 60 mins)
                await asyncio.sleep(1800)  # 30 minutes
                
                # Check if connection is still active
                if conn_info.get('state') != WSState.CONNECTED:
                    break
                
                try:
                    # Renew the listenKey via PUT request
                    url = "https://api.mexc.com/api/v3/userDataStream"
                    timestamp = str(int(time.time() * 1000))
                    query_string = f"listenKey={listen_key}&timestamp={timestamp}"
                    
                    # Sign the request
                    signature = hmac.new(
                        secret.encode('utf-8'),
                        query_string.encode('utf-8'),
                        hashlib.sha256
                    ).hexdigest()
                    
                    headers = {
                        "X-MEXC-APIKEY": api_key,
                        "Content-Type": "application/json"
                    }
                    
                    full_url = f"{url}?{query_string}&signature={signature}"
                    
                    async with aiohttp.ClientSession() as session:
                        async with session.put(full_url, headers=headers) as response:
                            if response.status == 200:
                                self.logger.info(f"Successfully renewed MEXC listenKey for {conn_id}")
                            else:
                                error_text = await response.text()
                                self.logger.error(f"Failed to renew MEXC listenKey: {error_text}")
                                # Connection may need to be re-established
                                break
                    
                except Exception as e:
                    self.logger.error(f"Failed to renew MEXC listenKey: {e}")
                    # Connection may need to be re-established
                    break
                    
        except asyncio.CancelledError:
            self.logger.info(f"MEXC listenKey renewal loop cancelled for {conn_id}")
        except Exception as e:
            self.logger.error(f"Error in MEXC listenKey renewal loop: {e}")
    
    async def _handle_reconnection(self, conn_id: str) -> None:
        """Handle connection reconnection with exponential backoff."""
        if conn_id not in self.connections:
            return
            
        conn_info = self.connections[conn_id]
        exchange = conn_info['exchange']
        endpoint = conn_info['endpoint']
        conn_type = conn_info['conn_type']  # Fixed: was 'type', should be 'conn_type'
        
        # Get reconnection delay from reconnection manager
        delay = self.reconnection_manager.get_delay(conn_id)
        
        if delay == float('inf'):
            self.logger.error(f"Max reconnection attempts reached for {conn_id}")
            return
        
        self.logger.info(f"Reconnecting to {conn_id} in {delay:.2f}s")
        await asyncio.sleep(delay)
        
        try:
            # Attempt reconnection
            new_conn_id = await self.connect_exchange(exchange, endpoint, conn_type)
            
            if new_conn_id:
                # Mark successful reconnection
                self.reconnection_manager.mark_success(conn_id)
                
                # Resubscribe to all channels
                await self._resubscribe_all(new_conn_id)
                
                self.logger.info(f"Successfully reconnected {conn_id} as {new_conn_id}")
            else:
                # Mark failed reconnection
                self.reconnection_manager.mark_failure(conn_id)
                
                # Schedule another reconnection attempt
                asyncio.create_task(self._handle_reconnection(conn_id))
                
        except Exception as e:
            self.logger.error(f"Reconnection failed for {conn_id}: {e}")
            self.reconnection_manager.mark_failure(conn_id, e)
            
            # Schedule another reconnection attempt
            asyncio.create_task(self._handle_reconnection(conn_id))
    
    async def _process_message(self, conn_id: str, message: str) -> None:
        """
        Process WebSocket message.
        
        Args:
            conn_id: Connection ID
            message: WebSocket message
        """
        if conn_id not in self.connections:
            self.logger.warning(f"Received message for unknown connection {conn_id}")
            return
        
        conn_info = self.connections[conn_id]
        exchange = conn_info['exchange']
        
        # Update last message time
        conn_info['last_message_time'] = time.time()
        
        # Get the exchange name string (conn_info['exchange'] contains the connector object)
        exchange_name = self._get_exchange_name(exchange) if hasattr(exchange, '__class__') else str(exchange)
        
        try:
            # Convert message from string to object if possible
            message_obj = None
            try:
                message_obj = json.loads(message)
            except:
                message_obj = message
            
            # Critical log for raw message debugging
            self.logger.critical(f"RAW_MESSAGE_RECEIVED from {exchange_name} [{conn_id}]: {message[:500]}")
            self.logger.critical(f"RAW_WEBSOCKET_MESSAGE: {message}")
            
            # CRITICAL FIX FOR BYBIT: Filter messages based on category
            # Bybit uses the same WebSocket URL for both spot and derivatives
            # We need to filter messages based on the category field
            if exchange_name.startswith('bybit_'):
                if isinstance(message_obj, dict) and 'data' in message_obj:
                    data_list = message_obj.get('data', [])
                    if isinstance(data_list, list) and len(data_list) > 0:
                        first_data = data_list[0]
                        if isinstance(first_data, dict) and 'category' in first_data:
                            msg_category = first_data.get('category', '').lower()
                            
                            # Check if message category matches connection type
                            if exchange_name == 'bybit_spot' and msg_category != 'spot':
                                self.logger.debug(f"Skipping {msg_category} message on bybit_spot connection")
                                return
                            elif exchange_name == 'bybit_perp' and msg_category == 'spot':
                                self.logger.debug(f"Skipping spot message on bybit_perp connection")
                                return
            
            # Determine message type
            self.logger.critical(f"DETERMINING_MESSAGE_TYPE for exchange: {exchange_name}, conn_id: {conn_id}")
            message_type = self._determine_message_type(exchange_name, message_obj)
            
            if not message_type:
                self.logger.critical(f"UNKNOWN_MESSAGE_TYPE from {exchange_name}: {message}")
                return
            
            self.logger.critical(f"DETERMINED_MESSAGE_TYPE: {message_type} for {exchange_name}")
            
            # Normalize message
            self.logger.critical(f"NORMALIZING_MESSAGE type={message_type} for {exchange_name}")
            normalized = self._normalize_message(exchange_name, message_type, message_obj)
            
            # Extract symbol for logging
            symbol = normalized.get('symbol', 'Unknown')
            self.logger.info(f"Processed {message_type} message for {symbol} from {exchange_name}")
            
            # Dispatch to handlers
            handlers = self.message_handlers.get(message_type, [])
            self.logger.critical(f"DISPATCHING_TO_HANDLERS: {message_type} has {len(handlers)} handlers")
            
            # Process message based on type
            if message_type == WSMessageType.TRADE:
                # Special handling for Bitget's multiple fills in one message
                if exchange_name.startswith('bitget_') and normalized.get('raw', {}).get('arg', {}).get('channel') == 'fill':
                    # Handle both data formats
                    data = normalized['raw'].get('data', [])
                    
                    # If data is a list, it's the fills directly (new format)
                    if isinstance(data, list):
                        fills = data
                    # If data is a dict, check for fills key (old format)
                    elif isinstance(data, dict):
                        fills = data.get('fills', [])
                    else:
                        fills = []
                    
                    if isinstance(fills, list) and len(fills) > 1:
                        self.logger.info(f"Processing {len(fills)} trades from Bitget fill channel")
                        # Process each fill as a separate trade
                        for fill in fills:
                            # Create a new message with single fill
                            single_fill_raw = dict(normalized['raw'])
                            single_fill_raw['data'] = [fill]  # Set data as a list with single fill
                            
                            # Normalize the single fill
                            single_normalized = self._normalize_message(exchange_name, message_type, single_fill_raw)
                            
                            # Dispatch to handlers
                            for handler in handlers:
                                await handler(conn_id, single_normalized)
                        return
                
                # Special handling for Bybit's multiple executions in one message
                elif exchange_name.startswith('bybit_') and normalized.get('raw', {}).get('topic') == 'execution':
                    data = normalized['raw'].get('data', [])
                    if isinstance(data, list) and len(data) > 1:
                        self.logger.info(f"Processing {len(data)} trades from Bybit execution channel")
                        # Process each execution as a separate trade
                        for trade in data:
                            # Create a new message with single trade
                            single_trade_raw = dict(normalized['raw'])
                            single_trade_raw['data'] = [trade]
                            
                            # Normalize the single trade
                            single_normalized = self._normalize_message(exchange_name, message_type, single_trade_raw)
                            
                            # Dispatch to handlers
                            for handler in handlers:
                                await handler(conn_id, single_normalized)
                        return
            
            # Standard message handling
            for handler in handlers:
                await handler(conn_id, normalized)
            
        except Exception as e:
            self.logger.error(f"Error processing message from {exchange_name}: {e}")
            self.logger.error(f"Message: {message}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
    
    async def _resubscribe_all(self, conn_id: str) -> None:
        """Resubscribe to all channels for a connection."""
        if conn_id not in self.subscriptions:
            return
        
        for channel, symbols in self.subscriptions[conn_id].items():
            for symbol in symbols:
                await self.subscribe(conn_id, channel, symbol)
    
    def _get_subscription_message(self, 
                                 exchange: str, 
                                 channel: str, 
                                 symbol: str, 
                                 action: str) -> Optional[Any]:
        """
        Generate exchange-specific subscription/unsubscription messages.
        Based on official exchange WebSocket API documentation.
        """
        try:
            if exchange == 'binance':
                # Binance WebSocket API
                # Public streams: wss://stream.binance.com:9443/ws/<streamName>
                # Private streams require listenKey
                formatted_symbol = self._format_binance_symbol(symbol)
                
                if channel == 'orderbook':
                    stream_name = f"{formatted_symbol}@depth"
                elif channel == 'trades':
                    stream_name = f"{formatted_symbol}@trade"
                elif channel == 'ticker':
                    stream_name = f"{formatted_symbol}@ticker"
                elif channel == 'kline':
                    stream_name = f"{formatted_symbol}@kline_1m"
                else:
                    # For private channels, no subscription message needed
                    # They're handled via listenKey endpoint
                    return None
                
                # Binance uses combined streams format
                if action == 'subscribe':
                    return {
                        "method": "SUBSCRIBE",
                        "params": [stream_name],
                        "id": int(time.time() * 1000)
                    }
                else:
                    return {
                        "method": "UNSUBSCRIBE", 
                        "params": [stream_name],
                        "id": int(time.time() * 1000)
                    }
            
            elif exchange == 'bybit':
                # Bybit V5 WebSocket API
                # Based on: https://bybit-exchange.github.io/docs/v5/websocket/connect
                formatted_symbol = self._format_bybit_symbol(symbol)
                
                if channel == 'orderbook':
                    topic = f"orderbook.50.{formatted_symbol}"
                elif channel == 'trades':
                    topic = f"publicTrade.{formatted_symbol}"
                elif channel == 'ticker':
                    topic = f"tickers.{formatted_symbol}"
                elif channel == 'kline':
                    topic = f"kline.1.{formatted_symbol}"
                # Private channels
                elif channel == 'order':
                    topic = "order"
                elif channel == 'execution':
                    topic = "execution"
                elif channel == 'position':
                    topic = "position"
                elif channel == 'wallet':
                    topic = "wallet"
                else:
                    self.logger.warning(f"Unknown Bybit channel: {channel}")
                    return None
                
                if action == 'subscribe':
                    return {
                        "op": "subscribe",
                        "args": [topic]
                    }
                else:
                    return {
                        "op": "unsubscribe",
                        "args": [topic]
                    }
            
            elif exchange == 'mexc':
                # MEXC WebSocket API
                # Based on: https://mexcdevelop.github.io/apidocs/spot_v3_en/#websocket-market-data
                formatted_symbol = self._format_mexc_symbol(symbol)
                
                # Handle private channels - MEXC has strict limits
                if 'private' in channel:
                    # CRITICAL FIX: MEXC V3 private channels don't require symbol
                    # private.deals and private.orders are account-wide channels
                    if channel in ['private.deals', 'private.orders', 'private.account']:
                        if action == 'subscribe':
                            return {
                                "method": "SUBSCRIPTION",
                                "params": [channel]  # No symbol needed for private channels
                            }
                        else:
                            return {
                                "method": "UNSUBSCRIPTION",
                                "params": [channel]
                            }
                    else:
                        # Other private channels might need different handling
                        if action == 'subscribe':
                            return {
                                "method": "SUBSCRIPTION",
                                "params": [channel]
                            }
                        else:
                            return {
                                "method": "UNSUBSCRIPTION",
                                "params": [channel]
                            }
                
                # Public channels
                if channel == 'orderbook':
                    channel_name = f"spot@public.limit.depth.v3.api@{formatted_symbol}@20"  # V3 format with depth
                elif channel == 'trades':
                    channel_name = f"spot@public.deals.v3.api@{formatted_symbol}"  # V3 format
                elif channel == 'ticker':
                    channel_name = f"spot@public.bookTicker.v3.api@{formatted_symbol}"  # V3 format
                elif channel == 'kline':
                    channel_name = f"spot@public.kline.v3.api@{formatted_symbol}@Min1"  # V3 format with interval
                else:
                    # Default format for other channels
                    channel_name = f"spot@public.{channel}.v3.api@{formatted_symbol}"
                
                if action == 'subscribe':
                    return {
                        "method": "SUBSCRIPTION",
                        "params": [channel_name]
                    }
                else:
                    return {
                        "method": "UNSUBSCRIPTION",
                        "params": [channel_name]
                    }
            
            elif exchange == 'gateio':
                # Gate.io WebSocket API
                # Based on: https://www.gate.io/docs/developers/apiv4/ws/en/
                formatted_symbol = self._format_gateio_symbol(symbol)
                
                if channel == 'orderbook':
                    channel_name = "spot.order_book"
                    payload = [formatted_symbol, "20", "100ms"]
                elif channel == 'trades':
                    channel_name = "spot.trades"
                    payload = [formatted_symbol]
                elif channel == 'ticker':
                    channel_name = "spot.tickers"
                    payload = [formatted_symbol]
                # Private channels
                elif channel in ['spot.orders', 'spot.usertrades', 'spot.balances']:
                    channel_name = channel
                    payload = ["USDT"]  # Currency for balance updates
                else:
                    self.logger.warning(f"Unknown Gate.io channel: {channel}")
                    return None
                
                if action == 'subscribe':
                    return {
                        "time": int(time.time()),
                        "channel": channel_name,
                        "event": "subscribe",
                        "payload": payload
                    }
                else:
                    return {
                        "time": int(time.time()),
                        "channel": channel_name,
                        "event": "unsubscribe",
                        "payload": payload
                    }
            
            elif exchange == 'bitget':
                # Bitget WebSocket API V2
                # Based on user's working script and Bitget V2 documentation
                formatted_symbol = self._format_bitget_symbol(symbol) if symbol else None
                
                # Bitget V2 WebSocket format for spot
                # Private channels
                if channel in ['order', 'orders']:
                    # V2 uses 'orders' channel for order updates and filled trades
                    arg = {
                        "instType": "SPOT",  # V2 uses uppercase 'SPOT'
                        "channel": "orders",
                        "instId": formatted_symbol or "default"  # Symbol or "default" for all
                    }
                elif channel in ['fill', 'fills', 'trade', 'trades', 'execution']:
                    # CRITICAL FIX: V2 has a separate 'fill' channel for trade executions
                    arg = {
                        "instType": "SPOT",
                        "channel": "fill",  # Use fill channel for actual trade data
                        "instId": formatted_symbol or "default"
                    }
                elif channel == 'account':
                    # Account balance updates
                    arg = {
                        "instType": "SPOT",
                        "channel": "account"
                    }
                # Public channels (if needed)
                elif channel == 'orderbook':
                    arg = {
                        "instType": "SPOT",
                        "channel": "books",
                        "instId": formatted_symbol
                    }
                elif channel == 'trades':
                    arg = {
                        "instType": "SPOT",
                        "channel": "trade",
                        "instId": formatted_symbol
                    }
                elif channel == 'ticker':
                    arg = {
                        "instType": "SPOT",
                        "channel": "ticker",
                        "instId": formatted_symbol
                    }
                else:
                    self.logger.warning(f"Unknown Bitget channel: {channel}")
                    return None
                
                if action == 'subscribe':
                    return {
                        "op": "subscribe",
                        "args": [arg]
                    }
                else:
                    return {
                        "op": "unsubscribe",
                        "args": [arg]
                    }
            
            elif exchange == 'hyperliquid':
                # Hyperliquid WebSocket API
                # Based on: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions
                if channel == 'orderbook':
                    subscription = {
                        "type": "l2Book",
                        "coin": symbol
                    }
                elif channel == 'trades':
                    subscription = {
                        "type": "trades",
                        "coin": symbol
                    }
                elif channel == 'ticker':
                    subscription = {
                        "type": "allMids"
                    }
                # Private channels require user address
                elif channel == 'orderUpdates':
                    subscription = {
                        "type": "orderUpdates",
                        "user": symbol  # symbol is actually user address for private channels
                    }
                elif channel == 'userFills':
                    subscription = {
                        "type": "userFills", 
                        "user": symbol
                    }
                elif channel == 'userEvents':
                    subscription = {
                        "type": "userEvents",
                        "user": symbol
                    }
                elif channel == 'userFundings':
                    subscription = {
                        "type": "userFundings",
                        "user": symbol
                    }
                else:
                    self.logger.warning(f"Unknown Hyperliquid channel: {channel}")
                    return None
                
                if action == 'subscribe':
                    return {
                        "method": "subscribe",
                        "subscription": subscription
                    }
                else:
                    return {
                        "method": "unsubscribe", 
                        "subscription": subscription
                    }
            
            else:
                self.logger.warning(f"Unsupported exchange for WebSocket: {exchange}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error generating subscription message for {exchange}: {e}")
            return None
    
    def _determine_message_type(self, exchange: str, message: Any) -> Optional[str]:
        """
        Determine message type based on exchange-specific message format.
        Based on official exchange WebSocket API documentation.
        """
        if not message:
            return None
        
        # CRITICAL FIX: Extract base exchange name for message type determination
        # The exchange parameter might be "binance_spot" but we need "binance"
        base_exchange = exchange.split('_')[0] if '_' in exchange else exchange
        base_exchange = base_exchange.lower()
        
        try:
            if base_exchange == 'binance':
                # Binance Spot and Futures WebSocket API
                # Based on: https://developers.binance.com/docs/binance-spot-api-docs/web-socket-api
                if isinstance(message, dict):
                    event_type = message.get('e')
                    # Check for executionReport (order updates)
                    if event_type == 'executionReport':
                        # This can be a trade or just an order status update
                        if message.get('x') == 'TRADE':
                            self.logger.debug("🎯 Binance TRADE execution detected (executionReport)")
                            return WSMessageType.TRADE
                        return WSMessageType.ORDER_UPDATE
                    # Check for ORDER_TRADE_UPDATE (futures)
                    elif event_type == 'ORDER_TRADE_UPDATE':
                        order_data = message.get('o', {})
                        if order_data.get('x') == 'TRADE':
                            self.logger.debug("🎯 Binance futures TRADE execution detected")
                            return WSMessageType.TRADE
                        return WSMessageType.ORDER_UPDATE
                    # Check for ACCOUNT_UPDATE (futures account updates)
                    elif event_type == 'ACCOUNT_UPDATE':
                        return WSMessageType.ORDER_UPDATE  # Account changes often include position updates
                    # Check for outboundAccountPosition (spot account updates)
                    elif event_type == 'outboundAccountPosition':
                        return WSMessageType.ORDER_UPDATE
                    # Check for depth updates (orderbook)
                    elif event_type == 'depthUpdate':
                        return WSMessageType.ORDERBOOK
                    # Check for public trade updates
                    elif event_type == 'trade':
                        return WSMessageType.TRADE
                    # Check for ticker updates
                    elif event_type == '24hrTicker':
                        return WSMessageType.HEARTBEAT  # Treat as heartbeat for now
                    # Check for stream field (data stream format)
                    elif 'stream' in message and 'data' in message:
                        data = message['data']
                        if isinstance(data, dict):
                            return self._determine_message_type(exchange, data)
                
            elif base_exchange == 'bybit':
                # Bybit V5 WebSocket API
                # Based on: https://bybit-exchange.github.io/docs/v5/websocket/wss-authentication
                if isinstance(message, dict):
                    # Check for topic field
                    topic = message.get('topic', '')
                    if topic:
                        # Private channels
                        if topic.startswith('execution'):
                            self.logger.debug("🎯 Bybit TRADE execution detected")
                            return WSMessageType.TRADE  # Trade executions are trades
                        elif topic.startswith('order'):
                            return WSMessageType.ORDER_UPDATE
                        elif topic.startswith('position'):
                            return WSMessageType.ORDER_UPDATE
                        elif topic.startswith('wallet'):
                            return WSMessageType.ORDER_UPDATE
                        # Public channels
                        elif 'orderbook' in topic or 'orderBookL2' in topic:
                            return WSMessageType.ORDERBOOK
                        elif 'publicTrade' in topic:
                            return WSMessageType.TRADE
                    # Check for op field (operation responses)
                    elif message.get('op') == 'ping':
                        return WSMessageType.HEARTBEAT
                    elif message.get('op') == 'auth':
                        return WSMessageType.HEARTBEAT  # Auth confirmation is not an order update
                
            elif base_exchange == 'mexc':
                # MEXC WebSocket API
                if isinstance(message, dict):
                    # CRITICAL FIX: Check for errors FIRST
                    if 'error' in message or ('code' in message and message.get('code') != 0):
                        return WSMessageType.ERROR
                    
                    # Check for channel field first
                    channel = message.get('channel', '') or message.get('c', '')
                    
                    # Private channels - check channel first to avoid misidentification
                    if 'private.deals' in channel:
                        self.logger.debug("🎯 MEXC TRADE execution detected (private.deals)")
                        return WSMessageType.TRADE
                    elif 'private.orders' in channel:
                        return WSMessageType.ORDER_UPDATE
                    elif 'private.account' in channel:
                        return WSMessageType.ORDER_UPDATE
                    
                    # Check for MEXC's actual trade message format (only if no channel)
                    # MEXC sends trades with structure: {'s': 'SYMBOL', 'd': {...trade data...}}
                    # But only treat as trade if it's not an order update
                    if 's' in message and 'd' in message and not channel:
                        # This is a trade message
                        self.logger.debug("🎯 MEXC TRADE execution detected (s/d format)")
                        return WSMessageType.TRADE
                    
                    # Public channels
                    if channel == 'push.trade':
                        return WSMessageType.TRADE
                    elif 'spot@public.deals' in channel:
                        return WSMessageType.TRADE
                    elif 'spot@public.limit.depth' in channel:
                        return WSMessageType.ORDERBOOK
                    # Check for pong response
                    elif message.get('msg') == 'PONG':
                        return WSMessageType.HEARTBEAT
                    # Handle error messages
                    elif 'code' in message or 'msg' in message:
                        # Check if it's actually trade data in msg field
                        if isinstance(message.get('msg'), dict):
                            msg_data = message.get('msg')
                            if 'trade' in str(msg_data).lower():
                                return WSMessageType.TRADE
                        return WSMessageType.ERROR
                        
            elif base_exchange == 'gateio':
                # Gate.io WebSocket API
                if isinstance(message, dict):
                    # CRITICAL FIX: Check for errors FIRST before any channel detection
                    if 'error' in message:
                        return WSMessageType.ERROR
                    
                    channel = message.get('channel', '')
                    event = message.get('event', '')
                    
                    # Private channels - exact match with subscription
                    if channel == 'spot.usertrades':
                        self.logger.debug("🎯 GATE.IO TRADE execution detected (spot.usertrades)")
                        return WSMessageType.TRADE
                    elif channel == 'spot.orders':
                        return WSMessageType.ORDER_UPDATE
                    # Public channels
                    elif channel == 'spot.trades':
                        return WSMessageType.TRADE
                    elif channel == 'spot.order_book':
                        return WSMessageType.ORDERBOOK
                    elif channel == 'spot.pong':
                        return WSMessageType.HEARTBEAT
                    # Error responses
                    elif 'error' in message:
                        # But check if it's a trade/order update with error field
                        if channel in ['spot.usertrades', 'spot.orders']:
                            if channel == 'spot.usertrades':
                                return WSMessageType.TRADE
                            else:
                                return WSMessageType.ORDER_UPDATE
                        return WSMessageType.ERROR
                        
            elif base_exchange == 'bitget':
                # Bitget WebSocket API
                if isinstance(message, dict):
                    # CRITICAL FIX: Check for errors FIRST
                    if message.get('event') == 'error' or 'error' in message:
                        return WSMessageType.ERROR
                    
                    # Check for action/event field
                    action = message.get('action', '')
                    event = message.get('event', '')
                    arg = message.get('arg', {})
                    
                    # Get channel from arg
                    channel = arg.get('channel', '') if isinstance(arg, dict) else ''
                    
                    # Private channels - exact match
                    if channel == 'fill':
                        self.logger.debug("🎯 BITGET TRADE execution detected (fill channel)")
                        return WSMessageType.TRADE
                    elif channel == 'orders':
                        # V2 orders channel only contains order updates, not trades
                        # CRITICAL FIX: Never treat orders channel messages as trades
                        # Even if the order is filled, it's still an ORDER_UPDATE
                        self.logger.debug("🎯 BITGET V2 ORDER_UPDATE detected (orders channel)")
                        return WSMessageType.ORDER_UPDATE
                    elif channel == 'order':
                        # Legacy V1 support
                        if 'data' in message:
                            data = message.get('data', [])
                            if isinstance(data, list) and len(data) > 0:
                                first_item = data[0] if isinstance(data[0], dict) else {}
                                # Check for trade-specific fields
                                if 'tradeId' in first_item or 'execType' in first_item:
                                    self.logger.debug("🎯 BITGET V1 TRADE execution detected (order channel with tradeId)")
                                    return WSMessageType.TRADE
                            self.logger.debug("🎯 BITGET V1 ORDER_UPDATE detected (order channel)")
                            return WSMessageType.ORDER_UPDATE
                        # Check event types
                        elif event == 'login':
                            return WSMessageType.HEARTBEAT
                        elif event == 'error':
                            # But check if it's a trade/order error
                            if channel in ['fill', 'orders']:
                                if channel == 'fill':
                                    return WSMessageType.TRADE
                                else:
                                    return WSMessageType.ORDER_UPDATE
                            return WSMessageType.ERROR
                        # Legacy format
                        elif action == 'snapshot' or action == 'update':
                            # Check data content
                            if 'data' in message:
                                data = message['data']
                                if isinstance(data, list) and len(data) > 0:
                                    first_item = data[0]
                                    if 'tradeId' in first_item:
                                        return WSMessageType.TRADE
                        
                        # Check for arg-based channel identification (newer Bitget format)
                        elif arg and isinstance(arg, dict):
                            arg_channel = arg.get('channel', '')
                            self.logger.debug(f"Bitget arg channel: {arg_channel}")
                            
                            if arg_channel == 'orders':
                                self.logger.debug("🎯 Bitget ORDER_UPDATE detected (arg orders)")
                                return WSMessageType.ORDER_UPDATE
                            elif arg_channel in ['fills', 'fill']:
                                # CRITICAL FIX: fills/fill channel contains TRADE data, not order updates
                                self.logger.debug("🎯 Bitget TRADE detected (arg fills/fill)")
                                return WSMessageType.TRADE
                            elif arg_channel == 'account':
                                self.logger.debug("🎯 Bitget ORDER_UPDATE detected (arg account)")
                                return WSMessageType.ORDER_UPDATE
                        
                        # Check for ping/pong
                        elif message.get('pong'):
                            return WSMessageType.HEARTBEAT
                        
                        # Log unrecognized Bitget message
                        self.logger.debug(f"❓ Unrecognized Bitget message format: {message}")
                        return WSMessageType.HEARTBEAT
            
            elif base_exchange == 'hyperliquid':
                # Hyperliquid WebSocket API
                if isinstance(message, dict):
                    # Check for channel field
                    channel = message.get('channel', '')
                    if channel:
                        # Private channels
                        if channel == 'userFills':
                            self.logger.debug("🎯 Hyperliquid TRADE execution detected")
                            return WSMessageType.TRADE  # User fills are trades
                        elif channel == 'orderUpdates':
                            return WSMessageType.ORDER_UPDATE
                        elif channel == 'userEvents':
                            return WSMessageType.ORDER_UPDATE
                        elif channel == 'userFundings':
                            return WSMessageType.ORDER_UPDATE
                        # Public channels
                        elif channel == 'l2Book':
                            return WSMessageType.ORDERBOOK
                        elif channel == 'trades':
                            return WSMessageType.TRADE
                        elif channel == 'allMids':
                            return WSMessageType.HEARTBEAT  # Treat as heartbeat
                    # Check for subscription responses
                    elif message.get('channel') == 'subscriptionResponse':
                        return WSMessageType.HEARTBEAT  # Subscription confirmations
                
                return None
        
        except Exception as e:
            self.logger.error(f"Error determining message type for {exchange}: {e}")
            return None # Return None on error

        return None # Default to None for unhandled messages
    
    def _normalize_message(self, 
                          exchange: str, 
                          message_type: str, 
                          message: Any) -> Dict[str, Any]:
        """
        Normalize WebSocket message to standard format.
        
        Args:
            exchange: Exchange name
            message_type: Message type
            message: Raw message data
            
        Returns:
            Normalized message dictionary
        """
        # Basic normalized structure
        normalized = {
            'exchange': exchange,
            'type': message_type,
            'timestamp': int(time.time() * 1000),
            'raw': message
        }
        
        # Extract symbol if available
        symbol = self._extract_symbol(exchange, message)
        if symbol:
            normalized['symbol'] = symbol
        
        # Type-specific normalization
        if message_type == WSMessageType.ORDERBOOK:
            normalized.update(self._normalize_orderbook(exchange, message))
        elif message_type == WSMessageType.TRADE:
            normalized.update(self._normalize_trade(exchange, message))
        elif message_type == WSMessageType.ORDER_UPDATE:
            normalized.update(self._normalize_order_update(exchange, message))
        
        return normalized
    
    def _normalize_orderbook(self, exchange: str, message: Any) -> Dict[str, Any]:
        """Normalize orderbook message."""
        result = {
            'is_snapshot': False,
            'bids': [],
            'asks': []
        }
        
        # Exchange-specific extraction
        if exchange.lower() == 'binance':
            # Binance orderbook update
            if 'e' in message and message['e'] == 'depthUpdate':
                result['is_snapshot'] = message.get('firstUpdateId', 0) == 0
                result['bids'] = [[float(p), float(q)] for p, q in message.get('b', [])]
                result['asks'] = [[float(p), float(q)] for p, q in message.get('a', [])]
                if 'E' in message:
                    result['timestamp'] = message['E']
        
        elif exchange.lower() == 'bybit':
            # Bybit orderbook update
            if 'topic' in message and ('depth' in message['topic'] or 'orderbook' in message['topic']):
                data = message.get('data', {})
                result['is_snapshot'] = message.get('type', '') == 'snapshot'
                if 'bids' in data:
                    result['bids'] = [[float(p), float(q)] for p, q in data.get('bids', [])]
                if 'asks' in data:
                    result['asks'] = [[float(p), float(q)] for p, q in data.get('asks', [])]
                result['timestamp'] = data.get('timestamp', int(time.time() * 1000))
                
        elif exchange.lower() == 'hyperliquid':
            # Hyperliquid orderbook update
            # Based on: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions
            if 'channel' in message and message['channel'] == 'l2Book':
                data = message.get('data', {})
                result['is_snapshot'] = True  # Hyperliquid always sends full snapshots
                
                # Hyperliquid format: {"levels": [["price", "size"], ...], "coin": "BTC"}
                levels = data.get('levels', [])
                if levels:
                    # Split levels into bids and asks based on price comparison to a reference
                    # In Hyperliquid, levels are sorted by price, bids are higher prices, asks are lower
                    all_levels = [[float(level[0]), float(level[1])] for level in levels if float(level[1]) > 0]
                    
                    if all_levels:
                        # Sort by price descending
                        all_levels.sort(key=lambda x: x[0], reverse=True)
                        
                        # Find the midpoint to separate bids and asks
                        mid_idx = len(all_levels) // 2
                        result['bids'] = all_levels[:mid_idx]  # Higher prices (bids)
                        result['asks'] = all_levels[mid_idx:]  # Lower prices (asks)
                        result['asks'].sort(key=lambda x: x[0])  # Sort asks ascending
                
                result['timestamp'] = data.get('time', int(time.time() * 1000))
            # Alternative format check
            elif 'data' in message and isinstance(message['data'], dict):
                data = message['data']
                if 'coin' in data and ('bids' in data or 'asks' in data):
                    result['is_snapshot'] = True
                    if 'bids' in data:
                        result['bids'] = [[float(item[0]), float(item[1])] for item in data.get('bids', [])]
                    if 'asks' in data:
                        result['asks'] = [[float(item[0]), float(item[1])] for item in data.get('asks', [])]
                    result['timestamp'] = data.get('time', int(time.time() * 1000))
        
        elif exchange.lower() == 'mexc':
            # MEXC orderbook update
            if 'data' in message and isinstance(message['data'], dict):
                data = message['data']
                result['is_snapshot'] = message.get('type', '') == 'snapshot'
                if 'bids' in data:
                    result['bids'] = [[float(p), float(q)] for p, q in data.get('bids', [])]
                if 'asks' in data:
                    result['asks'] = [[float(p), float(q)] for p, q in data.get('asks', [])]
                result['timestamp'] = data.get('timestamp', int(time.time() * 1000))
                
        elif exchange.lower() == 'gateio':
            # Gate.io orderbook update
            if 'method' in message and message['method'] == 'push' and 'params' in message:
                params = message['params']
                if len(params) >= 2 and 'order_book' in params[0]:
                    data = params[1]
                    result['is_snapshot'] = data.get('action', '') == 'update'
                    if 'bids' in data:
                        result['bids'] = [[float(p), float(q)] for p, q in data.get('bids', [])]
                    if 'asks' in data:
                        result['asks'] = [[float(p), float(q)] for p, q in data.get('asks', [])]
                    result['timestamp'] = data.get('t', int(time.time() * 1000))
                    
        elif exchange.lower() == 'bitget':
            # Bitget orderbook update
            if 'arg' in message and 'data' in message:
                arg = message['arg']
                data = message['data']
                if isinstance(data, list) and len(data) > 0:
                    data = data[0]  # Bitget sometimes wraps data in a list
                
                result['is_snapshot'] = arg.get('action', '') == 'snapshot'
                if 'bids' in data:
                    result['bids'] = [[float(p), float(q)] for p, q in data.get('bids', [])]
                if 'asks' in data:
                    result['asks'] = [[float(p), float(q)] for p, q in data.get('asks', [])]
                result['timestamp'] = data.get('ts', int(time.time() * 1000))
        
        return result
    
    def _normalize_trade(self, exchange: str, message: Any) -> Dict[str, Any]:
        """Normalize trade message."""
        result = {
            'id': None,
            'order_id': None,
            'price': None,
            'amount': None,
            'side': None,
            'symbol': None,
            'timestamp': None,
            'fee': None,
            'fee_currency': None,
            'is_maker': None
        }
        
        # CRITICAL FIX: Extract base exchange name
        base_exchange = exchange.split('_')[0] if '_' in exchange else exchange
        base_exchange = base_exchange.lower()
        
        # Exchange-specific extraction
        if base_exchange == 'binance':
            if 'e' in message:
                event_type = message['e']
                if event_type == 'trade':
                    # Public trade
                    result['id'] = str(message.get('t', ''))
                    result['price'] = float(message.get('p', 0))
                    result['amount'] = float(message.get('q', 0))
                    result['side'] = 'sell' if message.get('m', False) else 'buy'
                    result['symbol'] = message.get('s', '')
                    result['timestamp'] = message.get('T', 0)
                elif event_type == 'executionReport' and message.get('x') == 'TRADE':
                    # Private execution (user trade)
                    result['id'] = str(message.get('t', ''))  # Trade ID
                    result['order_id'] = str(message.get('i', ''))  # Order ID
                    result['price'] = float(message.get('L', 0))  # Last executed price
                    result['amount'] = float(message.get('l', 0))  # Last executed quantity
                    result['side'] = message.get('S', '').lower()
                    result['symbol'] = message.get('s', '')
                    result['timestamp'] = message.get('T', 0)
                    result['fee'] = float(message.get('n', 0))
                    result['fee_currency'] = message.get('N', '')
                    result['is_maker'] = message.get('m', False)
                elif event_type == 'ORDER_TRADE_UPDATE':
                    # Futures private execution in direct format
                    order = message.get('o', {})
                    if order.get('x') == 'TRADE':
                        result['id'] = str(order.get('t', ''))
                        result['order_id'] = str(order.get('i', ''))
                        result['price'] = float(order.get('L', 0))
                        result['amount'] = float(order.get('l', 0))
                        result['side'] = order.get('S', '').lower()
                        result['symbol'] = order.get('s', '')
                        result['timestamp'] = message.get('T', 0)
                        result['fee'] = float(order.get('n', 0))
                        result['fee_currency'] = order.get('N') if order.get('N') is not None else None
                        result['is_maker'] = order.get('m', False)
            # Handle nested stream format
            elif 'stream' in message and 'data' in message:
                data = message['data']
                if data.get('e') == 'ORDER_TRADE_UPDATE':
                    order = data.get('o', {})
                    if order.get('x') == 'TRADE':  # Execution type is TRADE
                        result['id'] = str(order.get('t', ''))
                        result['order_id'] = str(order.get('i', ''))
                        result['price'] = float(order.get('L', 0))
                        result['amount'] = float(order.get('l', 0))
                        result['side'] = order.get('S', '').lower()
                        result['symbol'] = order.get('s', '')
                        result['timestamp'] = data.get('T', 0)
                        result['fee'] = float(order.get('n', 0))
                        result['is_maker'] = order.get('m', False)
                        
        elif base_exchange == 'bybit':
            if 'topic' in message and 'data' in message:
                topic = message['topic']
                data = message['data']
                if isinstance(data, list) and data:
                    trade = data[0]
                    if 'publicTrade' in topic or ('trade' in topic and 'private' not in topic):
                        # Public trade
                        result['id'] = str(trade.get('i', ''))
                        result['price'] = float(trade.get('p', 0))
                        result['amount'] = float(trade.get('v', 0))
                        result['side'] = trade.get('S', '').lower()
                        result['symbol'] = trade.get('s', '')
                        result['timestamp'] = trade.get('T', 0)
                    elif 'execution' in topic:
                        # Private execution
                        result['id'] = str(trade.get('execId', ''))
                        result['order_id'] = str(trade.get('orderId', ''))
                        result['price'] = float(trade.get('execPrice', 0))
                        result['amount'] = float(trade.get('execQty', 0))
                        result['side'] = trade.get('side', '').lower()
                        result['symbol'] = trade.get('symbol', '')
                        result['timestamp'] = trade.get('execTime', 0)
                        result['fee'] = float(trade.get('execFee', 0))
                        result['is_maker'] = trade.get('isMaker', False)
                        
        elif base_exchange == 'mexc':
            # MEXC trade format handling
            # Check if this is a private.deals channel message
            channel = message.get('c', message.get('channel', ''))
            
            if 'private.deals' in channel:
                # Private deals channel format: {"c":"spot@private.deals.v3.api","d":{...},"s":"BERAUSDT","t":timestamp}
                data = message.get('d', {})
                symbol = message.get('s', '')
                
                # Parse trade data from 'd' object
                # S: 1 = Buy, 2 = Sell  
                side = 'buy' if data.get('S') == 1 else 'sell'
                
                # Use the trade ID from 't' field in data, not order ID
                trade_id = str(data.get('t', ''))  # This is the actual trade ID like "560388376742756352X1"
                
                # CRITICAL: Use trade ID as the unique identifier
                result['id'] = trade_id if trade_id else str(data.get('i', ''))
                result['order_id'] = str(data.get('i', ''))  # Order ID
                result['price'] = float(data.get('p', 0))  # Price
                result['amount'] = float(data.get('v', 0))  # Volume/Amount
                result['side'] = side
                result['symbol'] = self._format_mexc_symbol(symbol) if symbol else None  # Format symbol
                result['timestamp'] = int(data.get('T', 0))  # Trade time
                result['fee'] = float(data.get('n', 0))  # Fee amount
                result['fee_currency'] = data.get('N', '')  # Fee currency
                result['is_maker'] = data.get('m', 0) if 'm' in data else None
                
            # Legacy format support for s/d format
            elif 's' in message and 'd' in message:
                # This is MEXC's actual trade message format
                data = message['d']
                symbol = message['s']
                
                # Parse trade data from 'd' object
                # S: 1 = Buy, 2 = Sell
                side = 'buy' if data.get('S') == 1 else 'sell'
                
                result['id'] = str(data.get('i', ''))  # Order ID is used as trade ID
                result['order_id'] = str(data.get('i', ''))  # Order ID
                result['price'] = float(data.get('p', 0))  # Price
                result['amount'] = float(data.get('v', 0))  # Volume/Amount
                result['side'] = side
                result['symbol'] = self._format_mexc_symbol(symbol) if symbol else None  # Format symbol
                result['timestamp'] = int(data.get('T', 0))  # Trade time
                result['fee'] = float(data.get('n', 0))  # Fee amount
                # MEXC doesn't provide fee currency in this format
                result['is_maker'] = data.get('m', 0) if 'm' in data else None
                
            # Legacy format support for channel-based messages
            elif 'channel' in message and 'data' in message:
                channel = message['channel']
                data = message['data']
                if 'private.deals' in channel:
                    # V3 API format - data is typically an object, not array
                    if isinstance(data, dict):
                        trade = data
                    elif isinstance(data, list) and len(data) > 0:
                        trade = data[0]  # Process first trade if array
                    else:
                        trade = message
                    
                    # Private trade V3 format
                    result['id'] = str(trade.get('t', trade.get('id', '')))  # Trade ID
                    result['order_id'] = str(trade.get('o', trade.get('orderId', '')))  # Order ID
                    result['price'] = float(trade.get('p', 0))  # Price
                    result['amount'] = float(trade.get('q', 0))  # Quantity
                    result['side'] = 'buy' if trade.get('S') == 'BUY' else 'sell'  # Side
                    result['symbol'] = trade.get('s', '')  # Symbol
                    result['timestamp'] = int(trade.get('T', trade.get('dealTime', 0)))  # Trade time
                    result['fee'] = float(trade.get('n', 0))  # Commission amount
                    result['fee_currency'] = trade.get('N', '')  # Commission asset
                    result['is_maker'] = trade.get('m', False)  # Is maker
                else:
                    # Handle public trade format or other cases
                    result['id'] = str(message.get('t', message.get('id', '')))
                    result['price'] = float(message.get('p', 0))
                    result['amount'] = float(message.get('q', 0))
                    result['side'] = 'buy' if message.get('S') == 'BUY' else 'sell'
                    result['symbol'] = message.get('s', '')
                    result['timestamp'] = int(message.get('T', 0))
            else:
                # Fallback to flat message format
                result['id'] = str(message.get('t', message.get('id', '')))
                result['price'] = float(message.get('p', 0))
                result['amount'] = float(message.get('q', 0))
                result['side'] = 'buy' if message.get('S') == 'BUY' else 'sell'
                result['symbol'] = message.get('s', '')
                result['timestamp'] = int(message.get('T', 0))
        
        elif base_exchange == 'gateio':
            # Updated logic for Gate.io v4 WebSocket API
            if message.get('channel') == 'spot.usertrades':
                # User trades come in the result field
                trade_list = message.get('result', [])
                if isinstance(trade_list, list) and len(trade_list) > 0:
                    trade_data = trade_list[0]  # Process first trade
                elif isinstance(message.get('result'), dict):
                    trade_data = message.get('result')
                else:
                    trade_data = message

                result['id'] = str(trade_data.get('id', ''))
                result['order_id'] = str(trade_data.get('order_id', ''))
                result['price'] = float(trade_data.get('price', 0))
                result['amount'] = float(trade_data.get('amount', 0))
                result['side'] = trade_data.get('side', '').lower()
                result['symbol'] = trade_data.get('currency_pair', '')
                # Fix timestamp handling - create_time_ms is a string, create_time is an int
                create_time_ms = trade_data.get('create_time_ms')
                if create_time_ms:
                    # create_time_ms is a string like "1749402799970.254000"
                    result['timestamp'] = int(float(create_time_ms))
                else:
                    # Fall back to create_time (in seconds) and convert to milliseconds
                    create_time = trade_data.get('create_time', 0)
                    result['timestamp'] = int(create_time) * 1000 if create_time else None
                result['fee'] = float(trade_data.get('fee', 0))
                result['fee_currency'] = trade_data.get('fee_currency', '')
                result['is_maker'] = trade_data.get('role', '') == 'maker'
            # Keep old logic for public trades if needed, but prioritize user trades
            elif 'method' in message and message['method'] == 'push' and 'params' in message:
                params = message['params']
                if len(params) >= 2:
                    channel = params[0]
                    data = params[1]
                    if 'trades' in channel and 'user' not in channel:
                        # Public trade
                        if isinstance(data, list):
                            data = data[0] if data else {}
                        result['id'] = str(data.get('id', ''))
                        result['price'] = float(data.get('price', 0))
                        result['amount'] = float(data.get('amount', 0))
                        result['side'] = data.get('side', '').lower()
                        result['symbol'] = data.get('currency_pair', '')
                        result['timestamp'] = int(data.get('create_time_ms', 0))
                        
        elif base_exchange == 'bitget':
            # Bitget WebSocket format
            if 'arg' in message and 'data' in message:
                arg = message['arg']
                data = message['data']
                if 'channel' in arg:
                    channel = arg['channel']
                    if isinstance(data, list) and data:
                        trade = data[0]
                        if channel == 'trade':
                            # Public trade
                            result['id'] = str(trade.get('tradeId', ''))
                            result['price'] = float(trade.get('price', 0))
                            result['amount'] = float(trade.get('size', 0))
                            result['side'] = trade.get('side', '').lower()
                            result['symbol'] = trade.get('symbol', trade.get('instId', ''))
                            result['timestamp'] = int(trade.get('ts', 0))
                        elif channel in ['fills', 'fill']:
                            # V2 Private fill/trade - Updated to match actual Bitget V2 format
                            result['id'] = str(trade.get('tradeId', ''))
                            result['order_id'] = str(trade.get('orderId', ''))
                            # Extract client order ID from Bitget V2
                            result['client_order_id'] = trade.get('clientOid', '')
                            result['price'] = float(trade.get('priceAvg', trade.get('price', 0)))
                            result['amount'] = float(trade.get('size', 0))  # size is the filled amount
                            result['side'] = trade.get('side', '').lower()
                            result['symbol'] = self._format_bitget_symbol(trade.get('symbol', trade.get('instId', '')))
                            result['timestamp'] = int(trade.get('cTime', trade.get('uTime', 0)))
                            # Handle fee details - V2 format has feeDetail array
                            fee_details = trade.get('feeDetail', [])
                            if fee_details and isinstance(fee_details, list) and len(fee_details) > 0:
                                fee_detail = fee_details[0]  # Take first fee detail
                                result['fee'] = float(fee_detail.get('totalFee', 0))
                                result['fee_currency'] = fee_detail.get('feeCoin', '')
                            else:
                                result['fee'] = 0.0
                                result['fee_currency'] = ''
                            result['is_maker'] = trade.get('tradeScope', '') == 'maker'
                        elif channel == 'orders':
                            # CRITICAL FIX: V2 orders channel should NOT be processed as trades
                            # This channel only contains order updates, not trade executions
                            # Trade data should come from the 'fill' channel only
                            self.logger.warning(f"Ignoring orders channel message in trade normalization - this should be an ORDER_UPDATE")
                            # Return empty result to prevent duplicate trade insertion
                            return result
                        elif channel == 'order':
                            # Legacy V1 support
                            if 'tradeId' in trade or 'execType' in trade or 'fillId' in trade:
                                # This is a trade/fill
                                result['id'] = str(trade.get('tradeId', trade.get('fillId', '')))
                                result['order_id'] = str(trade.get('orderId', ''))
                                result['price'] = float(trade.get('fillPrice', trade.get('price', 0)))
                                result['amount'] = float(trade.get('fillQty', trade.get('size', trade.get('fillQuantity', 0))))
                                result['side'] = trade.get('side', '').lower()
                                result['symbol'] = self._format_bitget_symbol(trade.get('symbol', trade.get('instId', '')))
                                result['timestamp'] = int(trade.get('cTime', trade.get('fillTime', trade.get('ts', 0))))
                                # V1 fee format
                                result['fee'] = float(trade.get('fee', trade.get('fillFee', 0)))
                                result['fee_currency'] = trade.get('feeCcy', trade.get('fillFeeCcy', ''))
                                result['is_maker'] = trade.get('execType', '') == 'maker'
            
        elif base_exchange == 'hyperliquid':
            # Hyperliquid WebSocket trade handling
            if 'channel' in message and message['channel'] == 'userFills':
                # User fills are private trades
                data = message.get('data', {})
                if isinstance(data, dict) and 'fills' in data:
                    fills = data['fills']
                    if fills and isinstance(fills, list):
                        fill = fills[0]  # Process first fill
                        result['id'] = str(fill.get('tid', ''))
                        result['order_id'] = str(fill.get('oid', ''))
                        # Extract client order ID from Hyperliquid - can be 'cloid' or 'c'
                        result['client_order_id'] = fill.get('cloid') or fill.get('c', '')
                        result['price'] = float(fill.get('px', 0))
                        result['amount'] = float(fill.get('sz', 0))
                        # Convert Hyperliquid side format: 'B' -> 'buy', 'S' -> 'sell'
                        side = fill.get('side', '')
                        if side == 'B':
                            result['side'] = 'buy'
                        elif side == 'S':
                            result['side'] = 'sell'
                        else:
                            result['side'] = side.lower()
                        # CRITICAL FIX: Hyperliquid uses 'coin' not 'pair' or 's'
                        coin = fill.get('coin', '')
                        if coin and '/' not in coin:
                            result['symbol'] = f"{coin}/USDC"  # Hyperliquid uses USDC
                        else:
                            result['symbol'] = coin
                        result['timestamp'] = fill.get('time', 0)
                        result['fee'] = float(fill.get('fee', 0))
                        result['is_maker'] = fill.get('isMaker', False)
            elif 'channel' in message and message['channel'] == 'trades':
                # Public trades
                data = message.get('data', {})
                trades = data.get('trades', [])
                if trades and isinstance(trades, list):
                    trade = trades[0]  # Process first trade
                    result['id'] = str(trade.get('tid', ''))
                    result['price'] = float(trade.get('px', 0))
                    result['amount'] = float(trade.get('sz', 0))
                    # Convert Hyperliquid side format: 'B' -> 'buy', 'S' -> 'sell'
                    side = trade.get('side', '')
                    if side == 'B':
                        result['side'] = 'buy'
                    elif side == 'S':
                        result['side'] = 'sell'
                    else:
                        result['side'] = side.lower()
                    # CRITICAL FIX: Hyperliquid uses 'coin' not 'pair'
                    coin = trade.get('coin', '')
                    if coin and '/' not in coin:
                        result['symbol'] = f"{coin}/USD"  # Public trades use USD
                    else:
                        result['symbol'] = coin
                    result['timestamp'] = trade.get('time', 0)
            elif 'c' in message:  # Channel format
                channel = message['c']
                data = message.get('d', {})
                if 'trade' in channel and 'private' not in channel:
                    # Public trade
                    result['id'] = str(data.get('i', ''))
                    result['price'] = float(data.get('p', 0))
                    result['amount'] = float(data.get('q', 0))
                    result['side'] = 'sell' if data.get('m', False) else 'buy'
                    result['symbol'] = data.get('s', '')
                    result['timestamp'] = data.get('t', 0)
                elif 'deals' in channel and 'private' in channel:
                    # Private deal/trade
                    result['id'] = str(data.get('t', ''))
                    result['order_id'] = str(data.get('i', ''))
                    result['price'] = float(data.get('p', 0))
                    result['amount'] = float(data.get('q', 0))
                    result['side'] = data.get('S', '').lower()
                    result['symbol'] = data.get('s', '')
                    result['timestamp'] = data.get('T', 0)
                    result['fee'] = float(data.get('n', 0))
                    
        return result
    
    def _normalize_order_update(self, exchange: str, message: Any) -> Dict[str, Any]:
        """Normalize order update message."""
        result = {
            'id': None,
            'client_order_id': None,
            'status': None,
            'filled': None,
            'remaining': None,
            'price': None,
            'amount': None,
            'side': None,
            'symbol': None,
            'timestamp': None,
            'fee': None,
            'average_price': None
        }
        
        exchange = exchange.lower()
        
        # Exchange-specific extraction
        if exchange == 'binance':
            if 'e' in message:
                event_type = message['e']
                if event_type == 'executionReport':
                    result['id'] = str(message.get('i', ''))
                    result['client_order_id'] = message.get('c', '')
                    result['status'] = message.get('X', '')  # Order status
                    result['filled'] = float(message.get('z', 0))  # Cumulative filled qty
                    result['amount'] = float(message.get('q', 0))  # Original quantity
                    result['remaining'] = result['amount'] - result['filled']
                    result['price'] = float(message.get('p', 0))  # Order price
                    result['side'] = message.get('S', '').lower()  # Buy/Sell
                    result['symbol'] = message.get('s', '')
                    result['timestamp'] = message.get('T', 0)  # Transaction time
                    result['fee'] = float(message.get('n', 0))  # Commission
                    if result['filled'] > 0:
                        result['average_price'] = float(message.get('Z', 0)) / result['filled']  # Cumulative quote qty / filled
                elif event_type in ['ORDER_TRADE_UPDATE', 'ACCOUNT_UPDATE']:
                    # Futures format
                    order = message.get('o', {})
                    result['id'] = str(order.get('i', ''))
                    result['client_order_id'] = order.get('c', '')
                    result['status'] = order.get('X', '')
                    result['filled'] = float(order.get('z', 0))
                    result['amount'] = float(order.get('q', 0))
                    result['remaining'] = result['amount'] - result['filled']
                    result['price'] = float(order.get('p', 0))
                    result['side'] = order.get('S', '').lower()
                    result['symbol'] = order.get('s', '')
                    result['timestamp'] = message.get('T', 0)
            # Handle nested stream format
            elif 'stream' in message and 'data' in message:
                data = message['data']
                if data.get('e') in ['ORDER_TRADE_UPDATE', 'ACCOUNT_UPDATE']:
                    order = data.get('o', {})
                    result['id'] = str(order.get('i', ''))
                    result['client_order_id'] = order.get('c', '')
                    result['status'] = order.get('X', '')
                    result['filled'] = float(order.get('z', 0))
                    result['amount'] = float(order.get('q', 0))
                    result['remaining'] = result['amount'] - result['filled']
                    result['price'] = float(order.get('p', 0))
                    result['side'] = order.get('S', '').lower()
                    result['symbol'] = order.get('s', '')
                    result['timestamp'] = data.get('T', 0)
                    
        elif exchange == 'bybit':
            if 'topic' in message and 'data' in message:
                topic = message['topic']
                data = message['data']
                if isinstance(data, list) and data:
                    order = data[0]  # Take first order in array
                    if 'order' in topic:
                        result['id'] = str(order.get('orderId', ''))
                        result['client_order_id'] = order.get('orderLinkId', '')
                        result['status'] = order.get('orderStatus', '')
                        result['filled'] = float(order.get('cumExecQty', 0))
                        result['amount'] = float(order.get('qty', 0))
                        result['remaining'] = result['amount'] - result['filled']
                        result['price'] = float(order.get('price', 0))
                        result['side'] = order.get('side', '').lower()
                        result['symbol'] = order.get('symbol', '')
                        result['timestamp'] = order.get('updatedTime', 0)
                        result['fee'] = float(order.get('cumExecFee', 0))
                        if result['filled'] > 0:
                            result['average_price'] = float(order.get('avgPrice', 0))
                    elif 'execution' in topic:
                        result['id'] = str(order.get('orderId', ''))
                        result['client_order_id'] = order.get('orderLinkId', '')
                        result['status'] = 'partially_filled' if order.get('execType') == 'Trade' else 'unknown'
                        result['filled'] = float(order.get('execQty', 0))
                        result['price'] = float(order.get('execPrice', 0))
                        result['side'] = order.get('side', '').lower()
                        result['symbol'] = order.get('symbol', '')
                        result['timestamp'] = order.get('execTime', 0)
                        result['fee'] = float(order.get('execFee', 0))
                        
        elif exchange == 'mexc':
            if 'c' in message:  # Channel format
                channel = message['c']
                data = message.get('d', {})
                if 'orders' in channel:
                    result['id'] = str(data.get('i', ''))
                    result['client_order_id'] = data.get('c', '')
                    result['status'] = data.get('X', '')
                    result['filled'] = float(data.get('z', 0))
                    result['amount'] = float(data.get('q', 0))
                    result['remaining'] = result['amount'] - result['filled']
                    result['price'] = float(data.get('p', 0))
                    result['side'] = data.get('S', '').lower()
                    result['symbol'] = data.get('s', '')
                    result['timestamp'] = data.get('T', 0)
                elif 'deals' in channel:
                    result['id'] = str(data.get('i', ''))
                    result['status'] = 'filled'
                    result['filled'] = float(data.get('q', 0))
                    result['price'] = float(data.get('p', 0))
                    result['side'] = data.get('S', '').lower()
                    result['symbol'] = data.get('s', '')
                    result['timestamp'] = data.get('T', 0)
                    result['fee'] = float(data.get('n', 0))
                    
        elif exchange == 'gateio':
            if 'method' in message and message['method'] == 'push' and 'params' in message:
                params = message['params']
                if len(params) >= 2:
                    channel = params[0]
                    data = params[1]
                    if 'orders' in channel:
                        if isinstance(data, list):
                            data = data[0] if data else {}
                        result['id'] = str(data.get('id', ''))
                        result['client_order_id'] = data.get('text', '')
                        result['status'] = data.get('status', '')
                        result['filled'] = float(data.get('filled_total', 0))
                        result['amount'] = float(data.get('amount', 0))
                        result['remaining'] = result['amount'] - result['filled']
                        result['price'] = float(data.get('price', 0))
                        result['side'] = data.get('side', '').lower()
                        result['symbol'] = f"{data.get('currency_pair', '')}"
                        result['timestamp'] = int(data.get('update_time', 0) * 1000)
                        result['fee'] = float(data.get('fee', 0))
                        if result['filled'] > 0 and data.get('filled_total', 0) > 0:
                            result['average_price'] = float(data.get('avg_deal_price', 0))
                    elif 'usertrades' in channel:
                        if isinstance(data, list):
                            data = data[0] if data else {}
                        result['id'] = str(data.get('order_id', ''))
                        result['status'] = 'filled'
                        result['filled'] = float(data.get('amount', 0))
                        result['price'] = float(data.get('price', 0))
                        result['side'] = data.get('side', '').lower()
                        result['symbol'] = data.get('currency_pair', '')
                        result['timestamp'] = int(data.get('create_time', 0) * 1000)
                        result['fee'] = float(data.get('fee', 0))
                        
        elif exchange == 'bitget':
            if 'arg' in message and 'data' in message:
                arg = message['arg']
                data = message['data']
                if 'channel' in arg:
                    channel = arg['channel']
                    if isinstance(data, list) and data:
                        order = data[0]
                        if channel == 'trade':
                            # Public trade
                            result['id'] = str(trade.get('tradeId', ''))
                            result['price'] = float(trade.get('price', 0))
                            result['amount'] = float(trade.get('size', 0))
                            result['side'] = trade.get('side', '').lower()
                            result['symbol'] = trade.get('symbol', '')
                            result['timestamp'] = int(trade.get('ts', 0))
                        elif channel in ['fills', 'fill']:
                            # Private fill/trade
                            result['id'] = str(trade.get('tradeId', ''))
                            result['order_id'] = str(trade.get('orderId', ''))
                            result['price'] = float(trade.get('price', 0))
                            result['amount'] = float(trade.get('baseVolume', 0))
                            result['side'] = trade.get('side', '').lower()
                            result['symbol'] = trade.get('symbol', '')
                            result['timestamp'] = int(trade.get('ts', 0))
                            fee_detail = trade.get('feeDetail', {})
                            result['fee'] = float(fee_detail.get('totalFee', 0))
                            result['fee_currency'] = fee_detail.get('feeCcy', '')
                            
        elif exchange == 'hyperliquid':
            if 'data' in message:
                data = message['data']
                if 'fills' in data:
                    fills = data['fills']
                    if fills:
                        fill = fills[0]  # Take first fill
                        result['id'] = str(fill.get('tid', ''))
                        result['order_id'] = str(fill.get('oid', ''))
                        result['price'] = float(fill.get('px', 0))
                        result['amount'] = float(fill.get('sz', 0))
                        # Convert Hyperliquid side format: 'B' -> 'buy', 'S' -> 'sell'
                        side = fill.get('side', '')
                        if side == 'B':
                            result['side'] = 'buy'
                        elif side == 'S':
                            result['side'] = 'sell'
                        else:
                            result['side'] = side.lower()
                        # Format symbol properly - Hyperliquid uses coin name, we need to add /USDC
                        coin = fill.get('coin', '')
                        if coin and '/' not in coin:
                            result['symbol'] = f"{coin}/USDC"  # Hyperliquid uses USDC
                        else:
                            result['symbol'] = coin
                        result['timestamp'] = fill.get('time', 0)
                        result['fee'] = float(fill.get('fee', 0))
                        result['fee_currency'] = fill.get('feeToken', '')
                        result['is_maker'] = not fill.get('crossed', False)  # crossed = taker
                elif 'channel' in message and message['channel'] == 'userFills':
                    # Alternative format for user fills
                    data = message.get('data', {})
                    if 'fills' in data:
                        fills = data['fills']
                        if fills:
                            fill = fills[0]
                            result['id'] = str(fill.get('tid', ''))
                            result['order_id'] = str(fill.get('oid', ''))
                            # Extract client order ID from Hyperliquid - can be 'cloid' or 'c'
                            result['client_order_id'] = fill.get('cloid') or fill.get('c', '')
                            result['price'] = float(fill.get('px', 0))
                            result['amount'] = float(fill.get('sz', 0))
                            # Convert Hyperliquid side format: 'B' -> 'buy', 'S' -> 'sell'
                            side = fill.get('side', '')
                            if side == 'B':
                                result['side'] = 'buy'
                            elif side == 'S':
                                result['side'] = 'sell'
                            else:
                                result['side'] = side.lower()
                            # Format symbol properly
                            coin = fill.get('coin', '')
                            if coin and '/' not in coin:
                                result['symbol'] = f"{coin}/USDC"
                            else:
                                result['symbol'] = coin
                            result['timestamp'] = fill.get('time', 0)
                            result['fee'] = float(fill.get('fee', 0))
                            result['fee_currency'] = fill.get('feeToken', '')
                            result['is_maker'] = not fill.get('crossed', False)
            elif 'channel' in message:
                # Handle channel format for trades
                channel = message['channel']
                data = message.get('data', {})
                if channel == 'trades':
                    # Public trades
                    trades = data.get('trades', [])
                    if trades:
                        trade = trades[0]
                        result['id'] = str(trade.get('tid', ''))
                        result['price'] = float(trade.get('px', 0))
                        result['amount'] = float(trade.get('sz', 0))
                        # Convert Hyperliquid side format: 'B' -> 'buy', 'S' -> 'sell'
                        side = trade.get('side', '')
                        if side == 'B':
                            result['side'] = 'buy'
                        elif side == 'S':
                            result['side'] = 'sell'
                        else:
                            result['side'] = side.lower()
                        coin = trade.get('coin', '')
                        if coin and '/' not in coin:
                            result['symbol'] = f"{coin}/USDC"
                        else:
                            result['symbol'] = coin
                        result['timestamp'] = trade.get('time', 0)
        
        return result
    
    def _extract_symbol(self, exchange: str, message: Any) -> Optional[str]:
        """
        Extract symbol from WebSocket message.
        
        Args:
            exchange: Exchange name
            message: WebSocket message
            
        Returns:
            Symbol string or None
        """
        try:
            symbol = None  # Initialize symbol variable
            
            if exchange.lower() == 'binance':
                if isinstance(message, dict):
                    # Binance stream format: {"stream": "btcusdt@depth", "data": {...}}
                    if 'stream' in message:
                        stream = message['stream']
                        if '@' in stream:
                            symbol_part = stream.split('@')[0].upper()
                            # Convert BTCUSDT to BTC/USDT
                            if symbol_part.endswith('USDT'):
                                base = symbol_part[:-4]
                                symbol = f"{base}/USDT"
                            elif symbol_part.endswith('BTC'):
                                base = symbol_part[:-3]
                                symbol = f"{base}/BTC"
                            else:
                                symbol = symbol_part
                    # Direct data format
                    elif 's' in message:
                        symbol = message['s']
                        
            elif exchange.lower() == 'bybit':
                if isinstance(message, dict):
                    # Bybit format: {"topic": "orderbook.1.BTCUSDT", "data": {...}}
                    if 'topic' in message:
                        topic = message['topic']
                        if 'orderbook' in topic:
                            parts = topic.split('.')
                            if len(parts) >= 3:
                                symbol = parts[-1]  # Last part is symbol
                    elif 'data' in message and isinstance(message['data'], dict):
                        symbol = message['data'].get('s') or message['data'].get('symbol')
                        
            elif exchange.lower() == 'hyperliquid':
                if isinstance(message, dict):
                    # Hyperliquid format varies, try multiple approaches
                    if 'channel' in message and message['channel'] == 'l2Book':
                        data = message.get('data', {})
                        if isinstance(data, dict):
                            symbol = data.get('coin')
                            if symbol:
                                # Convert coin name to standard format
                                symbol = f"{symbol}/USD"  # Hyperliquid typically uses USD
                    elif 'data' in message:
                        data = message['data']
                        if isinstance(data, dict):
                            symbol = data.get('coin') or data.get('symbol')
                            if symbol and '/' not in symbol:
                                symbol = f"{symbol}/USD"
                    elif 'coin' in message:
                        symbol = message['coin']
                        if symbol and '/' not in symbol:
                            symbol = f"{symbol}/USD"
                            
            elif exchange.lower() == 'mexc':
                if isinstance(message, dict):
                    # MEXC trade format: {"s": "BTCUSDT", "d": {...}}
                    if 's' in message and 'd' in message:
                        symbol = message['s']
                        if symbol:
                            symbol = self._format_mexc_symbol(symbol)
                    # MEXC format: {"channel": "spot@public.bookTicker.v3.api@BTCUSDT", "data": {...}}
                    elif 'channel' in message:
                        channel = message['channel']
                        if '@' in channel:
                            parts = channel.split('@')
                            if len(parts) >= 3:
                                symbol_part = parts[-1]  # Last part after @
                                if symbol_part and symbol_part != 'BTCUSDT':  # Avoid hardcoded symbols
                                    symbol = self._format_mexc_symbol(symbol_part)
                    elif 'data' in message and isinstance(message['data'], dict):
                        symbol = message['data'].get('s') or message['data'].get('symbol')
                        if symbol:
                            symbol = self._format_mexc_symbol(symbol)
                        
            elif exchange.lower() == 'gateio':
                if isinstance(message, dict):
                    # Gate.io format: {"method": "spot.order_book_update", "params": ["BTC_USDT", ...]}
                    if 'method' in message and 'params' in message:
                        if message['method'] == 'spot.order_book_update' and message['params']:
                            symbol_part = message['params'][0]
                            symbol = self._format_gateio_symbol(symbol_part)
                    elif 'channel' in message:
                        channel = message['channel']
                        if 'spot.order_book' in channel:
                            # Extract symbol from channel name
                            parts = channel.split('.')
                            if len(parts) >= 3:
                                symbol = self._format_gateio_symbol(parts[-1])
                    # Handle additional Gate.io message formats
                    elif 'result' in message and isinstance(message['result'], dict):
                        # Handle subscription confirmation messages
                        if 'channel' in message['result']:
                            channel = message['result']['channel']
                            if 'spot.order_book' in channel:
                                parts = channel.split('.')
                                if len(parts) >= 3:
                                    symbol = self._format_gateio_symbol(parts[-1])
                                        # Handle raw data messages that might have symbol in different locations
                    elif 'raw' in message and isinstance(message['raw'], dict):
                        raw_data = message['raw']
                        if 'channel' in raw_data and 'spot.order_book' in raw_data['channel']:
                            parts = raw_data['channel'].split('.')
                            if len(parts) >= 3:
                                symbol = self._format_gateio_symbol(parts[-1])
                        elif 'result' in raw_data and isinstance(raw_data['result'], dict):
                            if 'channel' in raw_data['result'] and 'spot.order_book' in raw_data['result']['channel']:
                                parts = raw_data['result']['channel'].split('.')
                                if len(parts) >= 3:
                                    symbol = self._format_gateio_symbol(parts[-1])
                                 
            elif exchange.lower() == 'bitget':
                if isinstance(message, dict):
                    # Bitget format: {"action": "snapshot", "arg": {"instType": "sp", "channel": "books", "instId": "BTCUSDT"}}
                    if 'arg' in message and isinstance(message['arg'], dict):
                        symbol = message['arg'].get('instId')
                        if symbol:
                            symbol = self._format_bitget_symbol(symbol)
                    elif 'data' in message and isinstance(message['data'], list) and message['data']:
                        first_data = message['data'][0]
                        if isinstance(first_data, dict):
                            symbol = first_data.get('instId')
                            if symbol:
                                symbol = self._format_bitget_symbol(symbol)
            
            return symbol
            
        except Exception as e:
            self.logger.error(f"Error extracting symbol from {exchange} message: {e}")
            return None
    
    def _format_gateio_symbol(self, symbol: str) -> str:
        """Format symbol for Gate.io WebSocket API."""
        if '/' in symbol:
            return symbol.replace('/', '_')
        return symbol.upper()
    
    def _format_binance_symbol(self, symbol: str) -> str:
        """Format symbol for Binance WebSocket API (to lowercase without slash)."""
        if ':' in symbol:  # Futures symbol like BTC/USDT:USDT
            base_quote, settle = symbol.split(':')
            symbol = base_quote
        
        # Convert BTC/USDT to btcusdt
        if '/' in symbol:
            return symbol.replace('/', '').lower()
        return symbol.lower()
    
    def _format_bybit_symbol(self, symbol: str) -> str:
        """Format symbol for Bybit WebSocket API."""
        if ':' in symbol:  # Perpetual futures symbol like BTC/USDT:USDT
            base_quote, settle = symbol.split(':')
            return base_quote.replace('/', '').upper()
        
        # Convert BTC/USDT to BTCUSDT
        if '/' in symbol:
            return symbol.replace('/', '').upper()
        return symbol.upper()
    
    def _format_mexc_symbol(self, symbol: str) -> str:
        """Format symbol for MEXC WebSocket API.""" 
        # MEXC sends BERAUSDT but we need BERA/USDT
        # Try to split the symbol into base/quote format
        if '/' not in symbol:
            # Try common quote currencies
            for quote in ['USDT', 'USDC', 'BTC', 'ETH', 'BUSD']:
                if symbol.endswith(quote):
                    base = symbol[:-len(quote)]
                    if base:
                        return f"{base}/{quote}"
        
        # If already has slash, return as is
        return symbol
    
    def _format_bitget_symbol(self, symbol: str) -> str:
        """Format symbol for Bitget WebSocket API."""
        # Convert BTC/USDT to BTCUSDT
        if '/' in symbol:
            return symbol.replace('/', '').upper()
        return symbol.upper()
    
    def _try_split_symbol(self, symbol: str) -> str:
        """Try to split a symbol into base/quote format."""
        for quote in ['USDT', 'BUSD', 'USDC', 'USD', 'BTC', 'ETH', 'BNB']:
            if symbol.endswith(quote):
                base = symbol[:-len(quote)]
                if base:
                    return f"{base}/{quote}"
        return symbol
    
    def _get_subscription_summary(self, conn_id: str) -> Dict[str, List[str]]:
        """Get summary of subscriptions for a connection."""
        if conn_id not in self.subscriptions:
            return {}
        
        return {
            channel: list(symbols)
            for channel, symbols in self.subscriptions[conn_id].items()
        }

    async def _prepare_private_authentication(self, exchange: BaseExchangeConnector) -> Optional[Dict[str, Any]]:
        """
        Prepare authentication data for private WebSocket connection.
        
        Args:
            exchange: Exchange connector instance
            
        Returns:
            Authentication data dictionary or None if failed
        """
        # Get exchange name from connector type and attributes
        exchange_name = self._get_exchange_name(exchange)
        
        # CRITICAL FIX: Extract base exchange name for authentication
        # The full name might be "binance_spot" but auth methods are keyed by "binance"
        base_exchange_name = exchange_name.split('_')[0] if '_' in exchange_name else exchange_name
        
        self.logger.info(f"Preparing authentication for {exchange_name} (base: {base_exchange_name})")
        
        try:
            if base_exchange_name == 'binance':
                # Binance uses listenKey for private streams
                return await self._prepare_binance_auth(exchange)
            elif base_exchange_name == 'bybit':
                # Bybit uses API key authentication
                return await self._prepare_bybit_auth(exchange)
            elif base_exchange_name == 'mexc':
                # MEXC uses API signature authentication
                return await self._prepare_mexc_auth(exchange)
            elif base_exchange_name == 'gateio':
                # Gate.io uses API signature authentication
                return await self._prepare_gateio_auth(exchange)
            elif base_exchange_name == 'bitget':
                # Bitget uses API signature authentication
                return await self._prepare_bitget_auth(exchange)
            elif base_exchange_name == 'bitfinex':
                # Bitfinex uses HMAC-SHA384 auth over payload 'AUTH'+nonce
                return await self._prepare_bitfinex_auth(exchange)
            elif base_exchange_name == 'hyperliquid':
                # Hyperliquid uses wallet address
                return await self._prepare_hyperliquid_auth(exchange)
            else:
                self.logger.warning(f"No authentication method implemented for {base_exchange_name}")
                return None
                
        except Exception as e:
            self.logger.error(f"Failed to prepare authentication for {exchange_name}: {e}")
            return None
    
    def _get_exchange_name(self, exchange: BaseExchangeConnector) -> str:
        """
        Get the exchange name from the connector.
        
        Args:
            exchange: Exchange connector instance
            
        Returns:
            Exchange name string
        """
        # CRITICAL FIX: Preserve the full exchange name with suffix (e.g., binance_spot, bybit_perp)
        # This is needed for proper database lookups
        
        # First check if connector has explicit name attribute
        if hasattr(exchange, 'name') and exchange.name and exchange.name != 'unknown':
            name = exchange.name.lower()
            # If the name already has a suffix (spot, perp, etc.), return it as-is
            if '_' in name and any(suffix in name for suffix in ['spot', 'perp', 'future', 'futures']):
                self.logger.debug(f"Using full exchange name from connector: {name}")
                return name
        
        # Otherwise try to determine the exchange name from various sources
        # Try different methods to get the exchange name
        base_name = None
        
        if hasattr(exchange, 'name') and exchange.name and exchange.name != 'unknown':
            name = exchange.name.lower()
            # Map common variations
            if 'binance' in name:
                base_name = 'binance'
            elif 'bybit' in name:
                base_name = 'bybit'
            elif 'mexc' in name:
                base_name = 'mexc'
            elif 'gate' in name:
                base_name = 'gateio'
            elif 'bitget' in name:
                base_name = 'bitget'
            elif 'hyperliquid' in name:
                base_name = 'hyperliquid'
            else:
                base_name = name
        
        # Check the class name
        if not base_name:
            class_name = exchange.__class__.__name__.lower()
            if 'binance' in class_name:
                base_name = 'binance'
            elif 'bybit' in class_name:
                base_name = 'bybit'
            elif 'mexc' in class_name:
                base_name = 'mexc'
            elif 'gateio' in class_name:
                base_name = 'gateio'
            elif 'bitget' in class_name:
                base_name = 'bitget'
            elif 'hyperliquid' in class_name:
                base_name = 'hyperliquid'
        
        # Check the exchange instance type if available
        if not base_name and hasattr(exchange, 'exchange') and exchange.exchange:
            exchange_id = getattr(exchange.exchange, 'id', '').lower()
            if exchange_id:
                if 'binance' in exchange_id:
                    base_name = 'binance'
                elif 'bybit' in exchange_id:
                    base_name = 'bybit'
                elif 'mexc' in exchange_id:
                    base_name = 'mexc'
                elif 'gateio' in exchange_id or 'gate' in exchange_id:
                    base_name = 'gateio'
                elif 'bitget' in exchange_id:
                    base_name = 'bitget'
                elif 'hyperliquid' in exchange_id:
                    base_name = 'hyperliquid'
            
            # Also check the class name of the underlying exchange
            if not base_name:
                exchange_class = exchange.exchange.__class__.__name__.lower()
                if 'binance' in exchange_class:
                    base_name = 'binance'
                elif 'bybit' in exchange_class:
                    base_name = 'bybit'
                elif 'mexc' in exchange_class:
                    base_name = 'mexc'
                elif 'gateio' in exchange_class or 'gate' in exchange_class:
                    base_name = 'gateio'
                elif 'bitget' in exchange_class:
                    base_name = 'bitget'
                elif 'hyperliquid' in exchange_class:
                    base_name = 'hyperliquid'
        
        # If we have a base name, try to determine the market type and construct full name
        if base_name:
            # Check for market type to append appropriate suffix
            market_type = None
            
            # Check market_type attribute
            if hasattr(exchange, 'market_type'):
                market_type = exchange.market_type.lower()
            # Check underlying exchange type
            elif hasattr(exchange, 'exchange') and exchange.exchange:
                exchange_id = getattr(exchange.exchange, 'id', '').lower()
                if 'usdm' in exchange_id or 'coinm' in exchange_id:
                    market_type = 'perp'
            
            # Construct full name with suffix
            if market_type:
                if market_type in ['future', 'futures']:
                    full_name = f"{base_name}_perp"
                elif market_type in ['perpetual', 'perp']:
                    full_name = f"{base_name}_perp"
                else:
                    full_name = f"{base_name}_spot"
            else:
                # Default to spot if we can't determine the type
                full_name = f"{base_name}_spot"
            
            self.logger.debug(f"Constructed full exchange name: {full_name}")
            return full_name
        
        # If we still can't identify, log details for debugging
        self.logger.warning(f"Could not determine exchange name for {exchange.__class__.__name__}")
        self.logger.debug(f"Exchange details: class={exchange.__class__}, name={getattr(exchange, 'name', 'N/A')}")
        if hasattr(exchange, 'exchange') and exchange.exchange:
            self.logger.debug(f"Underlying exchange: {exchange.exchange.__class__}, id={getattr(exchange.exchange, 'id', 'N/A')}")
        
        return 'unknown'
    
    async def _prepare_binance_auth(self, exchange: BaseExchangeConnector) -> Optional[Dict[str, Any]]:
        """Prepare Binance listenKey authentication."""
        try:
            self.logger.info(f"Getting Binance listenKey for {exchange}")
            
            # Check if this is spot or futures based on exchange type
            exchange_type = exchange.exchange.__class__.__name__.lower()
            is_futures = 'usdm' in exchange_type or 'coinm' in exchange_type or hasattr(exchange, 'market_type') and exchange.market_type == 'future'
            
            self.logger.info(f"Exchange type: {exchange_type}, is_futures: {is_futures}")
            
            # Try different methods to get listenKey
            response = None
            if is_futures:
                # Try different futures listenKey methods (based on debug output)
                methods_to_try = [
                    'fapiPrivatePostListenKey',  # This is the correct one from debug output
                    'fapiPrivatePostListenkey', 
                    'fapiprivatePostListenkey',
                    'fapiprivate_post_listenkey'
                ]
                
                for method_name in methods_to_try:
                    if hasattr(exchange.exchange, method_name):
                        self.logger.info(f"Trying futures method: {method_name}")
                        method = getattr(exchange.exchange, method_name)
                        response = await method()
                        break
                else:
                    self.logger.error(f"No working futures listenKey method found for {exchange_type}")
                    return None
            else:
                # Try different spot listenKey methods (based on debug output)
                methods_to_try = [
                    'publicPostUserDataStream',  # This is for spot trading (from debug output)
                    'sapiPostUserDataStream',    # This might be for margin (causing the error)
                    'publicPostUserdatastream',
                    'sapiPostUserdatastream',
                    'sapi_post_userdatastream'
                ]
                
                for method_name in methods_to_try:
                    if hasattr(exchange.exchange, method_name):
                        self.logger.info(f"Trying spot method: {method_name}")
                        method = getattr(exchange.exchange, method_name)
                        response = await method()
                        break
                else:
                    self.logger.error(f"No working spot listenKey method found for {exchange_type}")
                    return None
            
            if response and 'listenKey' in response:
                listen_key = response['listenKey']
                self.logger.info(f"Got Binance listenKey: {listen_key[:8]}...")
                
                return {
                    'type': 'listen_key',
                    'listenKey': listen_key,
                    'endpoint_suffix': f'/{listen_key}'
                }
            else:
                self.logger.error(f"Invalid listenKey response: {response}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error getting Binance listenKey: {e}")
            # Log more details about the exchange
            self.logger.info(f"Exchange class: {exchange.exchange.__class__}")
            self.logger.info(f"Available methods: {[m for m in dir(exchange.exchange) if 'listen' in m.lower() or 'userdata' in m.lower()]}")
            return None
    
    async def _prepare_bybit_auth(self, exchange: BaseExchangeConnector) -> Optional[Dict[str, Any]]:
        """
        Prepare Bybit authentication data.
        
        Args:
            exchange: Exchange connector
            
        Returns:
            Authentication data dictionary or None
        """
        try:
            # Get API credentials - check for both api_key and apiKey formats
            api_key = getattr(exchange, 'api_key', None) or getattr(exchange, 'apiKey', None)
            
            # For secret, also check for api_secret (used by BybitWSConnector)
            secret = getattr(exchange, 'api_secret', None) or getattr(exchange, 'secret', None)
            
            # Log what we found for debugging
            self.logger.info(f"Found Bybit credentials: api_key={api_key is not None}, secret={secret is not None}")
            
            if not api_key or not secret:
                self.logger.error("Missing Bybit API credentials")
                return None
            
            # Generate timestamp and signature
            timestamp = int(time.time() * 1000)
            
            # For V5 API, the signature is calculated differently
            expires = timestamp + 10000  # Expires in 10 seconds
            
            # The string to sign should be: {api_key}{expires}
            signature_payload = f"{api_key}{expires}"
            
            # Generate HMAC-SHA256 signature
            signature = hmac.new(
                secret.encode('utf-8'),
                signature_payload.encode('utf-8'),
                hashlib.sha256
            ).hexdigest()
            
            self.logger.info("Generated Bybit authentication credentials")
            
            return {
                'api_key': api_key,
                'timestamp': str(expires),
                'sign': signature
            }
            
        except Exception as e:
            self.logger.error(f"Error preparing Bybit authentication: {e}")
            return None

    async def _prepare_mexc_auth(self, exchange: BaseExchangeConnector) -> Optional[Dict[str, Any]]:
        """Prepare MEXC API signature authentication and get listen key."""
        try:
            # Try to get API keys from different possible locations
            api_key = None
            secret = None
            
            # Check direct attributes first
            if hasattr(exchange, 'apiKey') and hasattr(exchange, 'secret'):
                api_key = exchange.apiKey
                secret = exchange.secret
            # Check exchange object attributes
            elif hasattr(exchange, 'exchange') and hasattr(exchange.exchange, 'apiKey'):
                api_key = exchange.exchange.apiKey
                secret = exchange.exchange.secret
                
            if not api_key or not secret:
                self.logger.error("MEXC connector missing API credentials")
                return None
                
            # MEXC uses listen key mechanism similar to Binance
            # Generate listen key for private WebSocket
            try:
                # Get underlying ccxt exchange instance
                ccxt_exchange = exchange.exchange
                
                # Create listen key via REST API
                timestamp = str(int(time.time() * 1000))
                params = f"timestamp={timestamp}"
                
                # Sign the request
                signature = hmac.new(
                    secret.encode('utf-8'),
                    params.encode('utf-8'),
                    hashlib.sha256
                ).hexdigest()
                
                # Make the request
                url = "https://api.mexc.com/api/v3/userDataStream"
                headers = {
                    "X-MEXC-APIKEY": api_key,
                    "Content-Type": "application/json"
                }
                
                full_url = f"{url}?{params}&signature={signature}"
                
                async with aiohttp.ClientSession() as session:
                    async with session.post(full_url, headers=headers) as response:
                        if response.status == 200:
                            data = await response.json()
                            listen_key = data.get("listenKey")
                            
                            if listen_key:
                                self.logger.info(f"Successfully obtained MEXC listen key")
                                return {
                                    'type': 'listen_key',
                                    'apiKey': api_key,
                                    'secret': secret,
                                    'listenKey': listen_key,
                                    'endpoint_suffix': f"?listenKey={listen_key}"
                                }
                            else:
                                self.logger.error("MEXC listen key response missing listenKey")
                                return None
                        else:
                            error_text = await response.text()
                            self.logger.error(f"Failed to obtain MEXC listenKey: {error_text}")
                            return None
                            
            except Exception as e:
                self.logger.error(f"Error getting MEXC listen key: {e}")
                # Fall back to regular API signature auth
                return {
                    'type': 'api_signature',
                    'apiKey': api_key,
                    'secret': secret
                }
                
        except Exception as e:
            self.logger.error(f"Error preparing MEXC auth: {e}")
            return None

    async def _prepare_gateio_auth(self, exchange: BaseExchangeConnector) -> Optional[Dict[str, Any]]:
        """Prepare Gate.io API signature authentication."""
        try:
            # Try to get API keys from different possible locations
            api_key = None
            secret = None
            
            # Check direct attributes first
            if hasattr(exchange, 'apiKey') and hasattr(exchange, 'secret'):
                api_key = exchange.apiKey
                secret = exchange.secret
            # Check exchange object attributes
            elif hasattr(exchange, 'exchange') and hasattr(exchange.exchange, 'apiKey'):
                api_key = exchange.exchange.apiKey
                secret = exchange.exchange.secret
                
            if not api_key or not secret:
                self.logger.error("Gate.io connector missing API credentials")
                return None
                
            return {
                'type': 'api_signature',
                'apiKey': api_key,
                'secret': secret
            }
        except Exception as e:
            self.logger.error(f"Error preparing Gate.io auth: {e}")
            return None

    async def _prepare_bitget_auth(self, exchange: BaseExchangeConnector) -> Optional[Dict[str, Any]]:
        """Prepare Bitget API signature authentication."""
        try:
            # Try to get API keys from different possible locations
            api_key = None
            secret = None
            passphrase = None
            
            # Check direct attributes first
            if hasattr(exchange, 'apiKey') and hasattr(exchange, 'secret'):
                api_key = exchange.apiKey
                secret = exchange.secret
                passphrase = getattr(exchange, 'password', None)
            # Check exchange object attributes
            elif hasattr(exchange, 'exchange') and hasattr(exchange.exchange, 'apiKey'):
                api_key = exchange.exchange.apiKey
                secret = exchange.exchange.secret
                passphrase = getattr(exchange.exchange, 'password', None)
                
            if not api_key or not secret:
                self.logger.error("Bitget connector missing API credentials")
                return None
                
            if not passphrase:
                self.logger.error("Bitget connector missing passphrase")
                return None
                
            return {
                'type': 'api_signature',
                'apiKey': api_key,
                'secret': secret,
                'passphrase': passphrase
            }
        except Exception as e:
            self.logger.error(f"Error preparing Bitget auth: {e}")
            return None

    async def _prepare_hyperliquid_auth(self, exchange: BaseExchangeConnector) -> Optional[Dict[str, Any]]:
        """Prepare Hyperliquid wallet authentication."""
        try:
            # Try to get wallet credentials from different possible locations
            wallet_address = None
            private_key = None
            
            # Check direct attributes first (both camelCase and snake_case)
            if hasattr(exchange, 'walletAddress') and hasattr(exchange, 'privateKey'):
                wallet_address = exchange.walletAddress
                private_key = exchange.privateKey
            elif hasattr(exchange, 'wallet_address') and hasattr(exchange, 'private_key'):
                wallet_address = exchange.wallet_address
                private_key = exchange.private_key
            # Check exchange object attributes
            elif hasattr(exchange, 'exchange'):
                wallet_address = getattr(exchange.exchange, 'walletAddress', None) or getattr(exchange.exchange, 'wallet_address', None)
                private_key = getattr(exchange.exchange, 'privateKey', None) or getattr(exchange.exchange, 'private_key', None)
                
            if not wallet_address or not private_key:
                self.logger.error("Hyperliquid connector missing wallet credentials")
                return None
                
            return {
                'type': 'wallet_signature',
                'walletAddress': wallet_address,
                'privateKey': private_key
            }
        except Exception as e:
            self.logger.error(f"Error preparing Hyperliquid auth: {e}")
            return None

    async def _prepare_bitfinex_auth(self, exchange: BaseExchangeConnector) -> Optional[Dict[str, Any]]:
        """Prepare Bitfinex authentication payload (apiKey + secret available)."""
        try:
            # Try to fetch credentials from connector attributes or config
            api_key = getattr(exchange, 'api_key', None)
            secret = getattr(exchange, 'secret', None)
            if not api_key and hasattr(exchange, 'config'):
                api_key = exchange.config.get('api_key')
            if not secret and hasattr(exchange, 'config'):
                secret = exchange.config.get('secret')
            # ccxt exchange instance may hold credentials as well
            if not api_key and hasattr(exchange, 'exchange'):
                api_key = getattr(exchange.exchange, 'apiKey', None)
            if not secret and hasattr(exchange, 'exchange'):
                secret = getattr(exchange.exchange, 'secret', None)
            if not api_key or not secret:
                self.logger.error("Bitfinex connector missing API credentials")
                return None
            return {'apiKey': api_key, 'secret': secret}
        except Exception as e:
            self.logger.error(f"Error preparing Bitfinex auth: {e}")
            return None

    async def _authenticate_private_connection(self, conn_id: str, ws) -> bool:
        """
        Authenticate private WebSocket connection.
        
        Args:
            conn_id: Connection ID
            ws: WebSocket connection
            
        Returns:
            True if authentication successful, False otherwise
        """
        conn_info = self.connections[conn_id]
        exchange = conn_info['exchange']
        auth_data = conn_info.get('auth_data')
        
        if not auth_data:
            self.logger.error(f"No authentication data for {exchange}")
            return False
        
        # Get exchange name using the same method as authentication preparation
        exchange_name = self._get_exchange_name(exchange)
        
        # CRITICAL FIX: Extract base exchange name for authentication
        base_exchange_name = exchange_name.split('_')[0] if '_' in exchange_name else exchange_name
        
        self.logger.info(f"Authenticating private connection for {exchange_name} (base: {base_exchange_name})")
        
        try:
            auth_success = False
            
            if base_exchange_name == 'binance':
                # Binance uses listenKey - no additional auth needed after connection
                auth_success = await self._authenticate_binance(conn_id, ws, auth_data)
            elif base_exchange_name == 'bybit':
                auth_success = await self._authenticate_bybit(conn_id, ws, auth_data)
            elif base_exchange_name == 'mexc':
                auth_success = await self._authenticate_mexc(conn_id, ws, auth_data)
            elif base_exchange_name == 'gateio':
                auth_success = await self._authenticate_gateio(conn_id, ws, auth_data)
            elif base_exchange_name == 'bitget':
                auth_success = await self._authenticate_bitget(conn_id, ws, auth_data)
            elif base_exchange_name == 'bitfinex':
                auth_success = await self._authenticate_bitfinex(conn_id, ws, auth_data)
            elif base_exchange_name == 'hyperliquid':
                auth_success = await self._authenticate_hyperliquid(conn_id, ws, auth_data)
            else:
                self.logger.warning(f"No authentication handler for {base_exchange_name}")
                return False
            
            # CRITICAL FIX: Subscribe to private channels after successful authentication
            if auth_success:
                self.logger.info(f"Authentication successful for {exchange_name}, subscribing to private channels...")
                # Wait a bit for authentication to settle
                await asyncio.sleep(0.5)
                # Subscribe to private channels
                # Get symbols from connection info
                symbols = conn_info.get('symbols', None)
                subscription_success = await self._subscribe_to_private_channels(conn_id, ws, symbols)
                if subscription_success:
                    self.logger.info(f"✅ Successfully subscribed to private channels for {exchange_name}")
                else:
                    self.logger.warning(f"⚠️ Failed to subscribe to private channels for {exchange_name}")
                return auth_success
            else:
                self.logger.error(f"Authentication failed for {exchange_name}")
                return False
                
        except Exception as e:
            self.logger.error(f"Authentication failed for {exchange_name}: {e}")
            return False
    
    async def _authenticate_binance(self, conn_id: str, ws, auth_data: Dict[str, Any]) -> bool:
        """Authenticate Binance private WebSocket connection."""
        try:
            # For Binance, authentication is done via listenKey in URL
            # No additional authentication message needed
            self.logger.info("Binance authentication completed via listenKey")
            return True
        except Exception as e:
            self.logger.error(f"Binance authentication error: {e}")
            return False

    async def _authenticate_bybit(self, conn_id: str, ws, auth_data: Dict[str, Any]) -> bool:
        """
        Authenticate Bybit WebSocket connection.
        
        Args:
            conn_id: Connection ID
            ws: WebSocket connection
            auth_data: Authentication data
            
        Returns:
            True if authenticated successfully, False otherwise
        """
        self.logger.info(f"Sending Bybit authentication message")
        
        try:
            # Get credentials from auth_data
            api_key = auth_data.get('api_key')
            
            # For V5 API, generate a fresh timestamp and signature
            expires = int(time.time() * 1000) + 5000  # 5 seconds expiry
            
            # According to Bybit V5 docs, message format is: "GET/realtime" + expires
            # Not api_key + expires as previously implemented
            message = f"GET/realtime{expires}"
            
            # Get the secret from auth_data or connector
            secret = None
            conn_info = self.connections.get(conn_id)
            if conn_info and conn_info.get('exchange'):
                exchange = conn_info.get('exchange')
                secret = getattr(exchange, 'api_secret', None) or getattr(exchange, 'secret', None)
            
            if not secret:
                self.logger.error("Cannot find API secret for Bybit authentication")
                return False
                
            # Generate HMAC-SHA256 signature
            signature = hmac.new(
                secret.encode('utf-8'),
                message.encode('utf-8'),
                hashlib.sha256
            ).hexdigest()
            
            # Bybit authentication requires sending a message with the auth parameters
            auth_message = {
                "op": "auth",
                "args": [
                    api_key,
                    expires,
                    signature
                ]
            }
            
            # Send authentication message
            await ws.send_str(json.dumps(auth_message))
            self.logger.info(f"Sent Bybit V5 auth message with expires={expires}")
            
            # Wait briefly for auth response (Bybit should respond with success message)
            await asyncio.sleep(1)
            
            # Return true for now and let the subscription handle any failures
            return True
            
        except Exception as e:
            self.logger.error(f"Bybit authentication error: {e}")
            return False

    async def _authenticate_mexc(self, conn_id: str, ws, auth_data: Dict[str, Any]) -> bool:
        """Authenticate MEXC private WebSocket connection."""
        try:
            # Check authentication type
            auth_type = auth_data.get('type', 'api_signature')
            
            if auth_type == 'listen_key':
                # Authentication already done via listenKey in URL
                self.logger.info("MEXC authentication completed via listenKey")
                return True
            else:
                # Fall back to API signature authentication
                api_key = auth_data['apiKey']
                secret = auth_data['secret']
                
                # Generate MEXC authentication with improved parameters
                timestamp = int(time.time() * 1000)
                query_string = f"api_key={api_key}&req_time={timestamp}"
                signature = hmac.new(
                    secret.encode('utf-8'),
                    query_string.encode('utf-8'),
                    hashlib.sha256
                ).hexdigest()
                
                # Send authentication message with improved format
                auth_message = {
                    "method": "SUBSCRIPTION",
                    "params": [],  # Empty params for auth only
                    "api_key": api_key,
                    "req_time": timestamp,
                    "signature": signature
                }
                
                # Use send_str() for aiohttp WebSocket
                await ws.send_str(json.dumps(auth_message))
                self.logger.info("Sent MEXC authentication message")
                
                # Wait a bit for authentication response
                await asyncio.sleep(0.5)
                return True
            
        except Exception as e:
            self.logger.error(f"MEXC authentication error: {e}")
            return False

    async def _authenticate_gateio(self, conn_id: str, ws, auth_data: Dict[str, Any]) -> bool:
        """Authenticate Gate.io private WebSocket connection."""
        try:
            # Gate.io authentication is done per-subscription, not as a separate message
            # Store auth data in connection info for use in subscriptions
            conn_info = self.connections.get(conn_id)
            if conn_info:
                conn_info['gateio_auth'] = {
                    'apiKey': auth_data['apiKey'],
                    'secret': auth_data['secret']
                }
            
            self.logger.info("Gate.io authentication data stored for subscription messages")
            return True
            
        except Exception as e:
            self.logger.error(f"Gate.io authentication error: {e}")
            return False

    async def _authenticate_bitget(self, conn_id: str, ws, auth_data: Dict[str, Any]) -> bool:
        """Authenticate Bitget private WebSocket connection."""
        try:
            api_key = auth_data['apiKey']
            secret = auth_data['secret']
            passphrase = auth_data['passphrase']
            
            # Generate Bitget authentication
            timestamp = str(int(time.time()))
            message = timestamp + "GET" + "/user/verify"
            signature = base64.b64encode(
                hmac.new(
                    secret.encode('utf-8'),
                    message.encode('utf-8'),
                    hashlib.sha256
                ).digest()
            ).decode('utf-8')
            
            auth_message = {
                "op": "login",
                "args": [{
                    "apiKey": api_key,
                    "passphrase": passphrase,
                    "timestamp": timestamp,
                    "sign": signature
                }]
            }
            
            # Use send_str() for aiohttp WebSocket
            await ws.send_str(json.dumps(auth_message))
            self.logger.info("Sent Bitget authentication message")
            return True
            
        except Exception as e:
            self.logger.error(f"Bitget authentication error: {e}")
            return False

    async def _authenticate_hyperliquid(self, conn_id: str, ws, auth_data: Dict[str, Any]) -> bool:
        """Authenticate Hyperliquid private WebSocket connection."""
        try:
            wallet_address = auth_data['walletAddress']
            
            # Subscribe to user events for the wallet
            subscribe_message = {
                "method": "subscribe",
                "subscription": {
                    "type": "user",
                    "user": wallet_address
                }
            }
            
            # Use send_str() for aiohttp WebSocket
            await ws.send_str(json.dumps(subscribe_message))
            self.logger.info(f"Sent Hyperliquid subscription for wallet: {wallet_address}")
            return True
            
        except Exception as e:
            self.logger.error(f"Hyperliquid authentication error: {e}")
            return False

    async def _authenticate_bitfinex(self, conn_id: str, ws, auth_data: Dict[str, Any]) -> bool:
        """Authenticate Bitfinex private WebSocket connection (v2)."""
        try:
            api_key = auth_data.get('apiKey')
            secret = auth_data.get('secret')
            if not api_key or not secret:
                self.logger.error("Missing Bitfinex API credentials in auth_data")
                return False
            # Bitfinex v2: payload = 'AUTH' + nonce
            nonce = int(time.time() * 1000)
            payload = f"AUTH{nonce}"
            signature = hmac.new(
                secret.encode('utf-8'),
                payload.encode('utf-8'),
                hashlib.sha384
            ).hexdigest()
            auth_message = {
                'event': 'auth',
                'apiKey': api_key,
                'authSig': signature,
                'authPayload': payload,
                'authNonce': nonce
            }
            await ws.send_str(json.dumps(auth_message))
            self.logger.info("Sent Bitfinex auth message")
            # Allow time for auth confirmation; subsequent messages on auth channel will arrive
            await asyncio.sleep(1)
            return True
        except Exception as e:
            self.logger.error(f"Bitfinex authentication error: {e}")
            return False

    async def _subscribe_to_private_channels(self, conn_id: str, ws, symbols: Optional[List[str]] = None) -> bool:
        """
        Subscribe to private channels for exchange.
        
        Args:
            conn_id: Connection ID
            ws: WebSocket connection
            
        Returns:
            True if subscribed successfully, False otherwise
        """
        conn_info = self.connections.get(conn_id)
        if not conn_info:
            return False
        
        exchange_name = self._get_exchange_name(conn_info['exchange'])
        # Extract base exchange name for consistent handling
        base_exchange = exchange_name.split('_')[0] if '_' in exchange_name else exchange_name
        
        try:
            # Exchange-specific subscription logic
            if base_exchange == 'binance':
                # Binance: Private messages handled via listenKey
                self.logger.info(f"Binance: Private messages handled via listenKey")
                return True
                
            elif base_exchange == 'bybit':
                # Bybit: Subscribe to private channels
                self.logger.info(f"Subscribing to Bybit private channels")
                
                # Fix: Bybit V5 uses single channel names, not array
                channels = ["execution", "order", "position", "wallet"]
                for channel in channels:
                    sub_message = {
                        "op": "subscribe",
                        "args": [channel]
                    }
                    await ws.send_str(json.dumps(sub_message))
                    self.logger.info(f"Subscribed to Bybit {channel} channel")
                    await asyncio.sleep(0.1)  # Small delay between subscriptions
                return True
                
            elif base_exchange == 'mexc':
                try:
                    # MEXC private channels - must include channel name in subscription
                    # Subscribe to private deals (trades)
                    await ws.send_str(json.dumps({
                        "method": "SUBSCRIPTION",
                        "params": ["spot@private.deals.v3.api"]
                    }))
                    self.logger.info("Subscribed to MEXC spot@private.deals")
                    await asyncio.sleep(0.5)
                    
                    # Subscribe to private orders
                    await ws.send_str(json.dumps({
                        "method": "SUBSCRIPTION", 
                        "params": ["spot@private.orders.v3.api"]
                    }))
                    self.logger.info("Subscribed to MEXC spot@private.orders")
                    await asyncio.sleep(0.5)
                    
                    # Skip account channel as it's being blocked
                    self.logger.info("Skipping MEXC account channel (blocked by exchange)")
                    return True
                    
                except Exception as e:
                    self.logger.error(f"MEXC subscription error: {e}")
                    return False
                
            elif base_exchange == 'gateio':
                # Gate.io: Subscribe to private channels with authentication in each message
                self.logger.info(f"Subscribing to Gate.io private channels")
                
                try:
                    # Get authentication data stored during auth phase
                    conn_info = self.connections.get(conn_id)
                    gateio_auth = conn_info.get('gateio_auth') if conn_info else None
                    
                    if not gateio_auth:
                        self.logger.error("No Gate.io authentication data found")
                        return False
                    
                    api_key = gateio_auth['apiKey']
                    secret = gateio_auth['secret']
                    
                    # Get symbols to subscribe to
                    pairs_to_subscribe = []
                    
                    # First try to use symbols parameter if provided
                    if symbols:
                        # Convert standard symbols to Gate.io format
                        for symbol in symbols:
                            if isinstance(symbol, str) and '/' in symbol:
                                gateio_pair = symbol.replace('/', '_')
                                pairs_to_subscribe.append(gateio_pair)
                                self.logger.info(f"Using symbol from parameter: {symbol} -> {gateio_pair}")
                    
                    # If no symbols parameter, use default
                    if not pairs_to_subscribe:
                        # Default to BERA_USDT for now
                        pairs_to_subscribe = ["BERA_USDT"]
                        self.logger.warning("No symbols provided, using default: BERA_USDT")
                    
                    self.logger.info(f"Gate.io pairs to subscribe: {pairs_to_subscribe}")
                    
                    # Subscribe to spot user trades with authentication
                    timestamp = int(time.time())
                    channel = "spot.usertrades"
                    event = "subscribe"
                    
                    # Generate signature for this subscription
                    signature_string = f'channel={channel}&event={event}&time={timestamp}'
                    signature = hmac.new(
                        secret.encode('utf-8'),
                        signature_string.encode('utf-8'),
                        hashlib.sha512
                    ).hexdigest()
                    
                    sub_message = {
                        "time": timestamp,
                        "channel": channel,
                        "event": event,
                        "payload": pairs_to_subscribe,
                        "auth": {
                            "method": "api_key",
                            "KEY": api_key,
                            "SIGN": signature
                        }
                    }
                    await ws.send_str(json.dumps(sub_message))
                    self.logger.info(f"Subscribed to Gate.io spot.usertrades for {pairs_to_subscribe} with auth")
                    
                    # Small delay between subscriptions
                    await asyncio.sleep(0.2)
                    
                    # Subscribe to spot orders with authentication
                    timestamp = int(time.time())
                    channel = "spot.orders"
                    event = "subscribe"
                    
                    # Generate signature for this subscription
                    signature_string = f'channel={channel}&event={event}&time={timestamp}'
                    signature = hmac.new(
                        secret.encode('utf-8'),
                        signature_string.encode('utf-8'),
                        hashlib.sha512
                    ).hexdigest()
                    
                    sub_message = {
                        "time": timestamp,
                        "channel": channel,
                        "event": event,
                        "payload": pairs_to_subscribe,
                        "auth": {
                            "method": "api_key",
                            "KEY": api_key,
                            "SIGN": signature
                        }
                    }
                    await ws.send_str(json.dumps(sub_message))
                    self.logger.info(f"Subscribed to Gate.io spot.orders for {pairs_to_subscribe} with auth")
                    
                    return True
                    
                except Exception as e:
                    self.logger.error(f"Error subscribing to Gate.io private channels: {e}")
                    return False
            
            elif base_exchange == 'bitget':
                # Bitget V2: Subscribe to fill channel for clean trade data
                self.logger.info(f"Subscribing to Bitget V2 Spot channels")
                
                try:
                    # Wait after login
                    await asyncio.sleep(1.0)
                    
                    # CRITICAL: Subscribe to fill channel for individual trade executions
                    # The fill channel provides clean trade data without cumulative volume issues
                    fill_sub_message = {
                        "op": "subscribe",
                        "args": [
                            {
                                "instType": "SPOT",
                                "channel": "fill",
                                "instId": "default"  # Monitor all symbols
                            }
                        ]
                    }
                    await ws.send_str(json.dumps(fill_sub_message))
                    self.logger.info(f"Subscribed to Bitget V2 fill channel for trade executions")
                    
                    # Also subscribe to orders channel for order status updates (but NOT for trades)
                    await asyncio.sleep(0.5)
                    orders_sub_message = {
                        "op": "subscribe",
                        "args": [
                            {
                                "instType": "SPOT",
                                "channel": "orders",
                                "instId": "default"  # Monitor all symbols
                            }
                        ]
                    }
                    await ws.send_str(json.dumps(orders_sub_message))
                    self.logger.info(f"Subscribed to Bitget V2 orders channel for order status updates")
                    
                    # Log symbols if provided
                    if symbols:
                        formatted_symbols = []
                        for symbol in symbols:
                            if isinstance(symbol, str) and '/' in symbol:
                                bitget_symbol = symbol.replace('/', '')
                                formatted_symbols.append(bitget_symbol)
                        if formatted_symbols:
                            self.logger.info(f"Note: Monitoring symbols {formatted_symbols} via 'default' subscription")
                    
                    self.logger.info(f"Bitget subscription complete - using fill channel for trades, orders channel for status")
                    
                    return True
                    
                except Exception as e:
                    self.logger.error(f"Failed to subscribe to Bitget private channels: {e}")
                    return False
            
            elif base_exchange == 'hyperliquid':
                # Hyperliquid: Subscribe to private channels
                self.logger.info(f"Subscribing to Hyperliquid private channels")
                
                # Get wallet address from auth data
                auth_data = conn_info.get('auth_data', {})
                wallet_address = auth_data.get('walletAddress')
                
                if wallet_address:
                    # Subscribe to user fills
                    sub_message = {
                        "method": "subscribe",
                        "subscription": {
                            "type": "userFills",
                            "user": wallet_address
                        }
                    }
                    await ws.send_str(json.dumps(sub_message))
                    self.logger.info(f"Subscribed to Hyperliquid userFills")
                    
                    # Also subscribe to order updates
                    await asyncio.sleep(0.1)
                    sub_message = {
                        "method": "subscribe",
                        "subscription": {
                            "type": "orderUpdates",
                            "user": wallet_address
                        }
                    }
                    await ws.send_str(json.dumps(sub_message))
                    self.logger.info(f"Subscribed to Hyperliquid orderUpdates")
                else:
                    self.logger.error("No wallet address found for Hyperliquid subscription")
                    return False
                return True
            
            elif base_exchange == 'bitfinex':
                # Bitfinex: private notifications are delivered on the authenticated channel; no extra subs needed
                self.logger.info("Bitfinex: private events are on auth channel; no additional subscription required")
                return True
            
            else:
                self.logger.warning(f"No private channel subscription handler for {base_exchange}")
                return False
            
        except Exception as e:
            self.logger.error(f"Error subscribing to private channels for {exchange_name}: {e}")
            return False

    async def _authenticate_exchange(self, conn_id: str, exchange_name: str, auth_data: Dict[str, Any]) -> bool:
        """
        Authenticate exchange WebSocket connection.
        
        Args:
            conn_id: Connection ID
            exchange_name: Exchange name (e.g., 'binance_perp')
            auth_data: Authentication data
            
        Returns:
            True if authenticated successfully, False otherwise
        """
        try:
            # Get WebSocket connection
            ws = self._get_connection(conn_id)
            if not ws:
                self.logger.error(f"Cannot authenticate {exchange_name} - WebSocket connection not found")
                return False
                
            # Determine exchange type from name
            lower_name = exchange_name.lower()
            if 'binance' in lower_name:
                return await self._authenticate_binance(conn_id, ws, auth_data)
            elif 'bybit' in lower_name:
                return await self._authenticate_bybit(conn_id, ws, auth_data)
            elif 'mexc' in lower_name:
                return await self._authenticate_mexc(conn_id, ws, auth_data)
            elif 'gate' in lower_name:
                return await self._authenticate_gateio(conn_id, ws, auth_data)
            elif 'bitget' in lower_name:
                return await self._authenticate_bitget(conn_id, ws, auth_data)
            elif 'hyperliquid' in lower_name:
                return await self._authenticate_hyperliquid(conn_id, ws, auth_data)
            elif 'bitfinex' in lower_name:
                return await self._authenticate_bitfinex(conn_id, ws, auth_data)
            else:
                self.logger.error(f"Unsupported exchange for authentication: {exchange_name}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error authenticating {exchange_name} WebSocket: {e}")
            return False

    def _get_connection(self, conn_id: str) -> Optional[aiohttp.ClientWebSocketResponse]:
        """
        Get WebSocket connection object for a connection ID.
        
        Args:
            conn_id: Connection ID
            
        Returns:
            WebSocket connection object or None if not found
        """
        conn_info = self.connections.get(conn_id)
        if not conn_info:
            self.logger.error(f"Connection {conn_id} not found")
            return None
            
        ws = conn_info.get('ws')
        if not ws:
            self.logger.error(f"WebSocket object not found for connection {conn_id}")
            return None
            
        return ws

    async def connect(self, url: str, use_reconnect: bool = True) -> Optional[str]:
        """
        Connect to a WebSocket endpoint.
        
        Args:
            url: WebSocket URL
            use_reconnect: Whether to use automatic reconnection
            
        Returns:
            Connection ID or None if connection failed
        """
        # Generate unique connection ID
        conn_id = str(uuid.uuid4())
        
        # Store basic connection info - the caller (connect_exchange) will update with exchange details
        self.connections[conn_id] = {
            'url': url,
            'state': WSState.CONNECTING,
            'use_reconnect': use_reconnect,
            'ws': None,
            'last_message_time': time.time(),
            'created_at': time.time(),
            'reconnect_count': 0,
            'subscriptions': {},
            'error': None
        }
        
        # Add to subscriptions tracking
        self.subscriptions[conn_id] = {}
        
        # Store connection task
        self.connection_tasks[conn_id] = asyncio.create_task(
            self._connection_handler(conn_id, url)
        )
        
        # Wait for connection to be established
        await asyncio.sleep(0.1)
        
        return conn_id

    async def _handle_message(self, conn_id: str, message: str):
        """
        Handle incoming WebSocket message.
        
        Args:
            conn_id: Connection ID
            message: WebSocket message
        """
        try:
            # Get exchange info for better logging
            exchange_name = "unknown"
            if conn_id in self.connections:
                conn_info = self.connections[conn_id]
                if 'exchange' in conn_info:
                    exchange_name = self._get_exchange_name(conn_info['exchange'])
            
            # Log raw message for debugging
            # CRITICAL: Changed to critical level to debug missing messages
            self.logger.critical(f"RAW_MESSAGE_RECEIVED from {exchange_name} [{conn_id}]: {message[:200]}")
            
            # Process the message using existing _process_message method if available
            await self._process_message(conn_id, message)
                
        except Exception as e:
            self.logger.error(f"Error handling WebSocket message: {e}")
            self.logger.error(traceback.format_exc())

# Import ReconnectionManager class if not moved to reconnection.py
try:
    from .reconnection import ReconnectionManager
except ImportError:
    # ReconnectionManager is defined in this file
    pass
