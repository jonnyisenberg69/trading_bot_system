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
from typing import Dict, List, Optional, Any, Set, Callable, Coroutine, Tuple
from dataclasses import dataclass
from enum import Enum

import websockets
from websockets.exceptions import ConnectionClosed

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
    
    async def connect_exchange(self, 
                              exchange: BaseExchangeConnector,
                              endpoint: str, 
                              conn_type: str = 'public') -> str:
        """
        Connect to an exchange WebSocket endpoint.
        
        Args:
            exchange: Exchange connector instance
            endpoint: WebSocket endpoint URL
            conn_type: Connection type ('public', 'private', etc.)
            
        Returns:
            Connection ID string
        """
        # Generate unique connection ID
        conn_id = f"{exchange.name}_{conn_type}_{uuid.uuid4().hex[:8]}"
        
        # For private connections, prepare authentication
        auth_data = None
        if conn_type == 'private':
            auth_data = await self._prepare_private_authentication(exchange)
            if not auth_data:
                self.logger.error(f"Failed to prepare authentication for {exchange.name}")
                return None
        
        # Store connection info
        self.connections[conn_id] = {
            'exchange': exchange,
            'endpoint': endpoint,
            'conn_type': conn_type,
            'auth_data': auth_data,
            'state': WSState.DISCONNECTED,
            'last_message_time': None,
            'ws': None,
            'error': None,
            'created_at': time.time()
        }
        
        # Initialize subscription tracking
        self.subscriptions[conn_id] = {}
        
        # Start connection task
        self.connection_tasks[conn_id] = asyncio.create_task(
            self._connection_handler(conn_id)
        )
        
        self.logger.info(f"Initialized connection {conn_id} to {exchange.name} ({endpoint})")
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
        Register a message handler function.
        
        Args:
            message_type: Type of message to handle
            handler: Callback function to process messages
        """
        if message_type not in self.message_handlers:
            self.message_handlers[message_type] = []
        
        self.message_handlers[message_type].append(handler)
        self.logger.debug(f"Registered handler for {message_type} messages")
    
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
                'error': str(conn_info['error']) if conn_info['error'] else None,
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
                    'error': str(info['error']) if info['error'] else None
                }
            return result
    
    async def _connection_handler(self, conn_id: str) -> None:
        """Handle WebSocket connection lifecycle."""
        if conn_id not in self.connections:
            self.logger.error(f"Connection {conn_id} not found")
            return
        
        conn_info = self.connections[conn_id]
        exchange = conn_info['exchange']
        endpoint = conn_info['endpoint']
        auth_data = conn_info.get('auth_data')
        exchange_name = self._get_exchange_name(exchange)
        
        # Use exchange-specific ping intervals
        if exchange_name == 'mexc':
            ping_interval = self.config.mexc_ping_interval
            connection_timeout = self.config.mexc_connection_timeout
        else:
            ping_interval = self.config.ping_interval
            connection_timeout = 30.0  # Default timeout
        
        self.logger.info(f"Starting connection handler for {conn_id} (exchange: {exchange_name}, ping: {ping_interval}s)")
        
        # Update connection state
        conn_info['state'] = WSState.CONNECTING
        
        try:
            # Import aiohttp here to avoid circular imports
            import aiohttp
            
            # Create WebSocket connection
            session = aiohttp.ClientSession()
            
            # Prepare connection parameters
            headers = {}
            
            # Add authentication headers if needed
            if auth_data and 'headers' in auth_data:
                headers.update(auth_data['headers'])
            
            # For Binance, append listenKey to endpoint
            if exchange_name == 'binance' and auth_data and 'listenKey' in auth_data:
                if '?' in endpoint:
                    endpoint = f"{endpoint}&stream={auth_data['listenKey']}"
                else:
                    endpoint = f"{endpoint}/{auth_data['listenKey']}"
            
            self.logger.info(f"Connecting to WebSocket: {endpoint}")
            
            # Establish WebSocket connection with timeout
            ws = await asyncio.wait_for(
                session.ws_connect(
                    endpoint,
                    headers=headers,
                    heartbeat=ping_interval,
                    timeout=aiohttp.ClientTimeout(total=connection_timeout)
                ),
                timeout=connection_timeout
            )
            
            # Store WebSocket and session in connection info
            conn_info['ws'] = ws
            conn_info['websocket'] = ws  # For compatibility
            conn_info['session'] = session
            conn_info['state'] = WSState.CONNECTED
            
            self.logger.info(f"WebSocket connected for {conn_id}")
            
            # Authenticate private connection if needed
            if conn_info['conn_type'] == 'private' and auth_data:
                auth_success = await self._authenticate_private_connection(conn_id, ws)
                if not auth_success:
                    self.logger.error(f"Authentication failed for {conn_id}")
                    conn_info['state'] = WSState.ERROR
                    conn_info['error'] = "Authentication failed"
                    return
            
            # Start ping task with exchange-specific interval
            ping_task = asyncio.create_task(self._ping_loop(conn_id, ping_interval))
            
            # Message handling loop
            async for message in ws:
                if message.type == aiohttp.WSMsgType.TEXT:
                    try:
                        await self._process_message(conn_id, message.data)
                        # Update last message time for connection health tracking
                        conn_info['last_message_time'] = time.time()
                    except Exception as e:
                        self.logger.error(f"Error processing message for {conn_id}: {e}")
                        
                elif message.type == aiohttp.WSMsgType.ERROR:
                    self.logger.error(f"WebSocket error for {conn_id}: {ws.exception()}")
                    break
                    
                elif message.type == aiohttp.WSMsgType.CLOSE:
                    self.logger.info(f"WebSocket closed for {conn_id}")
                    break
                    
        except asyncio.TimeoutError:
            self.logger.error(f"WebSocket connection timeout for {conn_id}")
            conn_info['state'] = WSState.ERROR
            conn_info['error'] = "Connection timeout"
        except Exception as e:
            self.logger.error(f"Connection handler error for {conn_id}: {e}")
            conn_info['state'] = WSState.ERROR
            conn_info['error'] = str(e)
        finally:
            # Cancel ping task
            if 'ping_task' in locals():
                ping_task.cancel()
                try:
                    await ping_task
                except asyncio.CancelledError:
                    pass
            
            # Clean up WebSocket and session
            if 'ws' in locals() and ws:
                try:
                    if not ws.closed:
                        await ws.close()
                except Exception as e:
                    self.logger.debug(f"Error closing WebSocket: {e}")
            
            if 'session' in locals() and session:
                try:
                    await session.close()
                except Exception as e:
                    self.logger.debug(f"Error closing session: {e}")
            
            # Update connection state
            conn_info['state'] = WSState.DISCONNECTED
            
            # Handle reconnection if enabled
            if self.config.reconnect_enabled and conn_info.get('should_reconnect', True):
                self.logger.info(f"Scheduling reconnection for {conn_id}")
                asyncio.create_task(self._handle_reconnection(conn_id))
            else:
                self.logger.info(f"Connection {conn_id} will not be reconnected")
    
    async def _ping_loop(self, conn_id: str, ping_interval: float) -> None:
        """Send periodic ping messages to keep connection alive."""
        if conn_id not in self.connections:
            return
            
        conn_info = self.connections[conn_id]
        exchange_name = self._get_exchange_name(conn_info['exchange'])
        
        self.logger.debug(f"Starting ping loop for {conn_id} (interval: {ping_interval}s)")
        
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
                    if exchange_name == 'mexc':
                        # MEXC uses JSON ping
                        ping_msg = {"method": "PING"}
                        await ws.send_str(json.dumps(ping_msg))
                    elif exchange_name in ['bybit', 'gateio']:
                        # These exchanges use JSON ping
                        ping_msg = {"op": "ping"}
                        await ws.send_str(json.dumps(ping_msg))
                    else:
                        # Standard WebSocket ping
                        await ws.ping()
                    
                    self.logger.debug(f"Sent ping to {conn_id}")
                    
                except Exception as e:
                    self.logger.warning(f"Failed to send ping to {conn_id}: {e}")
                    break
                    
        except asyncio.CancelledError:
            self.logger.debug(f"Ping loop cancelled for {conn_id}")
        except Exception as e:
            self.logger.error(f"Ping loop error for {conn_id}: {e}")
    
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
        """Process a WebSocket message."""
        if conn_id not in self.connections:
            return
        
        conn_info = self.connections[conn_id]
        exchange = conn_info['exchange']
        
        try:
            # Parse message
            if isinstance(message, str):
                try:
                    data = json.loads(message)
                except json.JSONDecodeError:
                    # Not JSON, might be a binary message
                    data = message
            else:
                # Already parsed or binary
                data = message
            
            # Extract message type and route to handlers
            message_type = self._determine_message_type(exchange.name, data)
            
            if message_type:
                # Create standardized message
                normalized_message = self._normalize_message(
                    exchange.name, message_type, data
                )
                
                # Route to handlers
                if message_type in self.message_handlers:
                    for handler in self.message_handlers[message_type]:
                        try:
                            asyncio.create_task(handler(normalized_message))
                        except Exception as e:
                            self.logger.error(f"Error in message handler: {e}")
            
        except Exception as e:
            self.logger.error(f"Error processing WebSocket message: {e}")
    
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
                
                # Handle private channels
                if 'private' in channel:
                    if action == 'subscribe':
                        return {
                            "method": "SUBSCRIPTION",
                            "params": [channel]  # Use channel as-is for private channels
                        }
                    else:
                        return {
                            "method": "UNSUBSCRIPTION",
                            "params": [channel]
                        }
                
                # Public channels
                if channel == 'orderbook':
                    channel_name = f"spot.depth.{formatted_symbol}"
                elif channel == 'trades':
                    channel_name = f"spot.deals.{formatted_symbol}"
                elif channel == 'ticker':
                    channel_name = f"spot.ticker.{formatted_symbol}"
                elif channel == 'kline':
                    channel_name = f"spot.kline.{formatted_symbol}"
                else:
                    channel_name = f"spot.{channel}.{formatted_symbol}"
                
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
                # Bitget WebSocket API
                # Based on: https://bitgetlimited.github.io/apidoc/en/spot/#websocketapi
                formatted_symbol = self._format_bitget_symbol(symbol)
                
                if channel == 'orderbook':
                    channel_name = "depth"
                    instId = formatted_symbol
                elif channel == 'trades':
                    channel_name = "trade"
                    instId = formatted_symbol
                elif channel == 'ticker':
                    channel_name = "ticker"
                    instId = formatted_symbol
                # Private channels
                elif channel in ['orders', 'fills', 'account', 'positions']:
                    channel_name = channel
                    instId = formatted_symbol if channel in ['orders', 'fills'] else "default"
                else:
                    self.logger.warning(f"Unknown Bitget channel: {channel}")
                    return None
                
                if action == 'subscribe':
                    return {
                        "op": "subscribe",
                        "args": [{
                            "instType": "SP",  # Spot trading
                            "channel": channel_name,
                            "instId": instId
                        }]
                    }
                else:
                    return {
                        "op": "unsubscribe",
                        "args": [{
                            "instType": "SP",
                            "channel": channel_name, 
                            "instId": instId
                        }]
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
        
        try:
            if exchange == 'binance':
                # Binance Spot and Futures WebSocket API
                # Based on: https://developers.binance.com/docs/binance-spot-api-docs/web-socket-api
                if isinstance(message, dict):
                    # Check for executionReport (order updates)
                    if message.get('e') == 'executionReport':
                        return WSMessageType.ORDER_UPDATE
                    # Check for ORDER_TRADE_UPDATE (futures)
                    elif message.get('e') == 'ORDER_TRADE_UPDATE':
                        return WSMessageType.ORDER_UPDATE
                    # Check for ACCOUNT_UPDATE (futures account updates)
                    elif message.get('e') == 'ACCOUNT_UPDATE':
                        return WSMessageType.ORDER_UPDATE  # Account changes often include position updates
                    # Check for outboundAccountPosition (spot account updates)
                    elif message.get('e') == 'outboundAccountPosition':
                        return WSMessageType.ORDER_UPDATE
                    # Check for depth updates (orderbook)
                    elif message.get('e') == 'depthUpdate':
                        return WSMessageType.ORDERBOOK
                    # Check for trade updates
                    elif message.get('e') == 'trade':
                        return WSMessageType.TRADE
                    # Check for ticker updates
                    elif message.get('e') == '24hrTicker':
                        return WSMessageType.HEARTBEAT  # Treat as heartbeat for now
                    # Check for stream field (data stream format)
                    elif 'stream' in message and 'data' in message:
                        data = message['data']
                        if isinstance(data, dict):
                            return self._determine_message_type(exchange, data)
                
            elif exchange == 'bybit':
                # Bybit V5 WebSocket API
                # Based on: https://bybit-exchange.github.io/docs/v5/websocket/wss-authentication
                if isinstance(message, dict):
                    # Check for topic field
                    topic = message.get('topic', '')
                    if topic:
                        # Private channels
                        if topic.startswith('order'):
                            return WSMessageType.ORDER_UPDATE
                        elif topic.startswith('execution'):
                            return WSMessageType.ORDER_UPDATE  # Trade executions are order updates
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
                        return WSMessageType.ORDER_UPDATE  # Auth confirmation
                
            elif exchange == 'mexc':
                # MEXC WebSocket API
                # Based on: https://mexcdevelop.github.io/apidocs/spot_v3_en/#websocket-market-data
                if isinstance(message, dict):
                    # Check for channel field
                    channel = message.get('channel', '')
                    if channel:
                        # Private channels
                        if 'private.orders' in channel:
                            return WSMessageType.ORDER_UPDATE
                        elif 'private.deals' in channel:
                            return WSMessageType.ORDER_UPDATE  # Private trades are order related
                        elif 'private.account' in channel:
                            return WSMessageType.ORDER_UPDATE
                        # Public channels
                        elif 'depth' in channel:
                            return WSMessageType.ORDERBOOK
                        elif 'deals' in channel:
                            return WSMessageType.TRADE
                    # Check for ping/pong
                    elif 'ping' in message:
                        return WSMessageType.HEARTBEAT
                
            elif exchange == 'gateio':
                # Gate.io WebSocket API
                # Based on: https://www.gate.io/docs/developers/apiv4/ws/en/
                if isinstance(message, dict):
                    # Check for channel field
                    channel = message.get('channel', '')
                    if channel:
                        # Private channels
                        if channel == 'spot.orders':
                            return WSMessageType.ORDER_UPDATE
                        elif channel == 'spot.usertrades':
                            return WSMessageType.ORDER_UPDATE  # User trades are order related
                        elif channel == 'spot.balances':
                            return WSMessageType.ORDER_UPDATE
                        # Public channels
                        elif channel == 'spot.order_book':
                            return WSMessageType.ORDERBOOK
                        elif channel == 'spot.trades':
                            return WSMessageType.TRADE
                    # Check for error field
                    elif 'error' in message:
                        return WSMessageType.ERROR
                
            elif exchange == 'bitget':
                # Bitget WebSocket API
                # Based on: https://bitgetlimited.github.io/apidoc/en/spot/#websocketapi
                if isinstance(message, dict):
                    # Check for channel or event field
                    channel = message.get('channel', message.get('event', ''))
                    if channel:
                        # Private channels
                        if channel == 'orders':
                            return WSMessageType.ORDER_UPDATE
                        elif channel == 'fills':
                            return WSMessageType.ORDER_UPDATE  # Fills are order related
                        elif channel == 'account':
                            return WSMessageType.ORDER_UPDATE
                        elif channel == 'positions':
                            return WSMessageType.ORDER_UPDATE
                        # Public channels
                        elif 'depth' in channel:
                            return WSMessageType.ORDERBOOK
                        elif 'trade' in channel:
                            return WSMessageType.TRADE
                    # Check for ping/pong
                    elif message.get('pong'):
                        return WSMessageType.HEARTBEAT
                
            elif exchange == 'hyperliquid':
                # Hyperliquid WebSocket API
                # Based on: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions
                if isinstance(message, dict):
                    # Check for channel field
                    channel = message.get('channel', '')
                    if channel:
                        # Private channels
                        if channel == 'orderUpdates':
                            return WSMessageType.ORDER_UPDATE
                        elif channel == 'userFills':
                            return WSMessageType.ORDER_UPDATE  # User fills are order related
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
                
                return WSMessageType.HEARTBEAT
        
        except Exception as e:
            self.logger.error(f"Error determining message type for {exchange}: {e}")
            return WSMessageType.HEARTBEAT
    
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
        
        exchange = exchange.lower()
        
        # Exchange-specific extraction
        if exchange == 'binance':
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
                        
        elif exchange == 'bybit':
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
                        
        elif exchange == 'mexc':
            if 'c' in message:  # Channel format
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
                    
        elif exchange == 'gateio':
            if 'method' in message and message['method'] == 'push' and 'params' in message:
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
                        result['timestamp'] = int(data.get('create_time', 0) * 1000)
                    elif 'usertrades' in channel:
                        # Private user trade
                        if isinstance(data, list):
                            data = data[0] if data else {}
                        result['id'] = str(data.get('id', ''))
                        result['order_id'] = str(data.get('order_id', ''))
                        result['price'] = float(data.get('price', 0))
                        result['amount'] = float(data.get('amount', 0))
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
                        trade = data[0]
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
                        result['side'] = fill.get('side', '').lower()
                        result['symbol'] = fill.get('coin', '')
                        result['timestamp'] = fill.get('time', 0)
                        result['fee'] = float(fill.get('fee', 0))
                        result['is_maker'] = not fill.get('liquidation', False)  # Assume non-liquidation fills can be maker
                elif 'trades' in data:
                    # Public trades
                    trades = data['trades']
                    if trades:
                        trade = trades[0]
                        result['id'] = str(trade.get('tid', ''))
                        result['price'] = float(trade.get('px', 0))
                        result['amount'] = float(trade.get('sz', 0))
                        result['side'] = trade.get('side', '').lower()
                        result['symbol'] = trade.get('coin', '')
                        result['timestamp'] = trade.get('time', 0)
        
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
                        if channel in ['orders', 'order']:
                            result['id'] = str(order.get('orderId', ''))
                            result['client_order_id'] = order.get('clientOid', '')
                            result['status'] = order.get('status', '')
                            result['filled'] = float(order.get('baseVolume', 0))
                            result['amount'] = float(order.get('size', 0))
                            result['remaining'] = result['amount'] - result['filled']
                            result['price'] = float(order.get('price', 0))
                            result['side'] = order.get('side', '').lower()
                            result['symbol'] = order.get('symbol', '')
                            result['timestamp'] = int(order.get('updateTime', 0))
                            result['fee'] = float(order.get('fee', 0))
                            if result['filled'] > 0:
                                result['average_price'] = float(order.get('priceAvg', 0))
                        elif channel in ['fills', 'fill']:
                            result['id'] = str(order.get('orderId', ''))
                            result['client_order_id'] = order.get('clientOid', '')
                            result['status'] = 'filled'
                            result['filled'] = float(order.get('baseVolume', 0))
                            result['price'] = float(order.get('price', 0))
                            result['side'] = order.get('side', '').lower()
                            result['symbol'] = order.get('symbol', '')
                            result['timestamp'] = int(order.get('ts', 0))
                            result['fee'] = float(order.get('feeDetail', {}).get('totalFee', 0))
                            
        elif exchange == 'hyperliquid':
            if 'data' in message:
                data = message['data']
                if 'orders' in data:
                    orders = data['orders']
                    if orders:
                        order = orders[0]  # Take first order
                        result['id'] = str(order.get('oid', ''))
                        result['client_order_id'] = order.get('cloid', '')
                        result['status'] = order.get('status', '')
                        result['filled'] = float(order.get('sz', 0)) - float(order.get('szOpen', 0))
                        result['amount'] = float(order.get('sz', 0))
                        result['remaining'] = float(order.get('szOpen', 0))
                        result['price'] = float(order.get('limitPx', 0))
                        result['side'] = order.get('side', '').lower()
                        result['symbol'] = order.get('coin', '')
                        result['timestamp'] = order.get('timestamp', 0)
                elif 'fills' in data:
                    fills = data['fills']
                    if fills:
                        fill = fills[0]  # Take first fill
                        result['id'] = str(fill.get('oid', ''))
                        result['status'] = 'filled'
                        result['filled'] = float(fill.get('sz', 0))
                        result['price'] = float(fill.get('px', 0))
                        result['side'] = fill.get('side', '').lower()
                        result['symbol'] = fill.get('coin', '')
                        result['timestamp'] = fill.get('time', 0)
                        result['fee'] = float(fill.get('fee', 0))
        
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
                    # MEXC format: {"channel": "spot@public.bookTicker.v3.api@BTCUSDT", "data": {...}}
                    if 'channel' in message:
                        channel = message['channel']
                        if '@' in channel:
                            parts = channel.split('@')
                            if len(parts) >= 3:
                                symbol_part = parts[-1]  # Last part after @
                                if symbol_part and symbol_part != 'BTCUSDT':  # Avoid hardcoded symbols
                                    symbol = self._format_mexc_symbol(symbol_part)
                    elif 'data' in message and isinstance(message['data'], dict):
                        symbol = message['data'].get('s') or message['data'].get('symbol')
                        
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
        # Convert BTC/USDT to BTCUSDT
        if '/' in symbol:
            return symbol.replace('/', '').upper()
        return symbol.upper()
    
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
        
        self.logger.info(f"Preparing authentication for {exchange_name}")
        
        try:
            if exchange_name == 'binance':
                # Binance uses listenKey for private streams
                return await self._prepare_binance_auth(exchange)
            elif exchange_name == 'bybit':
                # Bybit uses API key authentication
                return await self._prepare_bybit_auth(exchange)
            elif exchange_name == 'mexc':
                # MEXC uses API signature authentication
                return await self._prepare_mexc_auth(exchange)
            elif exchange_name == 'gateio':
                # Gate.io uses API signature authentication
                return await self._prepare_gateio_auth(exchange)
            elif exchange_name == 'bitget':
                # Bitget uses API signature authentication
                return await self._prepare_bitget_auth(exchange)
            elif exchange_name == 'hyperliquid':
                # Hyperliquid uses wallet address
                return await self._prepare_hyperliquid_auth(exchange)
            else:
                self.logger.warning(f"No authentication method implemented for {exchange_name}")
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
        # Try different methods to get the exchange name
        if hasattr(exchange, 'name') and exchange.name and exchange.name != 'unknown':
            name = exchange.name.lower()
            # Map common variations
            if 'binance' in name:
                return 'binance'
            elif 'bybit' in name:
                return 'bybit'
            elif 'mexc' in name:
                return 'mexc'
            elif 'gate' in name:
                return 'gateio'
            elif 'bitget' in name:
                return 'bitget'
            elif 'hyperliquid' in name:
                return 'hyperliquid'
            else:
                return name
        
        # Check the class name
        class_name = exchange.__class__.__name__.lower()
        if 'binance' in class_name:
            return 'binance'
        elif 'bybit' in class_name:
            return 'bybit'
        elif 'mexc' in class_name:
            return 'mexc'
        elif 'gateio' in class_name:
            return 'gateio'
        elif 'bitget' in class_name:
            return 'bitget'
        elif 'hyperliquid' in class_name:
            return 'hyperliquid'
        
        # Check the exchange instance type if available
        if hasattr(exchange, 'exchange') and exchange.exchange:
            exchange_id = getattr(exchange.exchange, 'id', '').lower()
            if exchange_id:
                if 'binance' in exchange_id:
                    return 'binance'
                elif 'bybit' in exchange_id:
                    return 'bybit'
                elif 'mexc' in exchange_id:
                    return 'mexc'
                elif 'gateio' in exchange_id or 'gate' in exchange_id:
                    return 'gateio'
                elif 'bitget' in exchange_id:
                    return 'bitget'
                elif 'hyperliquid' in exchange_id:
                    return 'hyperliquid'
            
            # Also check the class name of the underlying exchange
            exchange_class = exchange.exchange.__class__.__name__.lower()
            if 'binance' in exchange_class:
                return 'binance'
            elif 'bybit' in exchange_class:
                return 'bybit'
            elif 'mexc' in exchange_class:
                return 'mexc'
            elif 'gateio' in exchange_class or 'gate' in exchange_class:
                return 'gateio'
            elif 'bitget' in exchange_class:
                return 'bitget'
            elif 'hyperliquid' in exchange_class:
                return 'hyperliquid'
        
        # Enhanced check: look at config or market_type
        if hasattr(exchange, 'config') and exchange.config:
            # Check if there's any identifying information in config
            config_str = str(exchange.config).lower()
            if 'binance' in config_str:
                return 'binance'
            elif 'bybit' in config_str:
                return 'bybit'
            elif 'mexc' in config_str:
                return 'mexc'
            elif 'gate' in config_str:
                return 'gateio'
            elif 'bitget' in config_str:
                return 'bitget'
            elif 'hyperliquid' in config_str:
                return 'hyperliquid'
        
        # Last resort: check for specific attributes that might indicate exchange type
        if hasattr(exchange, 'market_type'):
            # This might help us identify, but we still need the exchange name
            pass
        
        # If we still can't identify, log details for debugging
        self.logger.warning(f"Could not determine exchange name for {class_name}")
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
        """Prepare Bybit API signature authentication."""
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
                self.logger.error("Bybit connector missing API credentials")
                return None
                
            return {
                'type': 'api_signature',
                'apiKey': api_key,
                'secret': secret
            }
        except Exception as e:
            self.logger.error(f"Error preparing Bybit auth: {e}")
            return None

    async def _prepare_mexc_auth(self, exchange: BaseExchangeConnector) -> Optional[Dict[str, Any]]:
        """Prepare MEXC API signature authentication."""
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
            
            # Check direct attributes first
            if hasattr(exchange, 'walletAddress') and hasattr(exchange, 'privateKey'):
                wallet_address = exchange.walletAddress
                private_key = exchange.privateKey
            # Check exchange object attributes
            elif hasattr(exchange, 'exchange'):
                wallet_address = getattr(exchange.exchange, 'walletAddress', None)
                private_key = getattr(exchange.exchange, 'privateKey', None)
                
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
        
        self.logger.info(f"Authenticating private connection for {exchange_name}")
        
        try:
            if exchange_name == 'binance':
                # Binance uses listenKey - no additional auth needed after connection
                return await self._authenticate_binance(conn_id, ws, auth_data)
            elif exchange_name == 'bybit':
                return await self._authenticate_bybit(conn_id, ws, auth_data)
            elif exchange_name == 'mexc':
                return await self._authenticate_mexc(conn_id, ws, auth_data)
            elif exchange_name == 'gateio':
                return await self._authenticate_gateio(conn_id, ws, auth_data)
            elif exchange_name == 'bitget':
                return await self._authenticate_bitget(conn_id, ws, auth_data)
            elif exchange_name == 'hyperliquid':
                return await self._authenticate_hyperliquid(conn_id, ws, auth_data)
            else:
                self.logger.warning(f"No authentication handler for {exchange_name}")
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
        """Authenticate Bybit private WebSocket connection."""
        try:
            api_key = auth_data['apiKey']
            secret = auth_data['secret']
            
            # Generate Bybit authentication
            timestamp = int(time.time() * 1000)
            signature_payload = f"GET/realtime{timestamp}"
            signature = hmac.new(
                secret.encode('utf-8'),
                signature_payload.encode('utf-8'),
                hashlib.sha256
            ).hexdigest()
            
            auth_message = {
                "op": "auth",
                "args": [api_key, timestamp, signature]
            }
            
            # Use send_str() for aiohttp WebSocket
            await ws.send_str(json.dumps(auth_message))
            self.logger.info("Sent Bybit authentication message")
            return True
            
        except Exception as e:
            self.logger.error(f"Bybit authentication error: {e}")
            return False

    async def _authenticate_mexc(self, conn_id: str, ws, auth_data: Dict[str, Any]) -> bool:
        """Authenticate MEXC private WebSocket connection."""
        try:
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
            api_key = auth_data['apiKey']
            secret = auth_data['secret']
            
            # Generate Gate.io authentication
            timestamp = str(int(time.time()))
            method = "GET"
            request_path = "/ws/v4/"
            body = ""
            
            # Create signature string
            signature_string = f"{method}\n{request_path}\n\n{body}\n{timestamp}"
            signature = hmac.new(
                secret.encode('utf-8'),
                signature_string.encode('utf-8'),
                hashlib.sha512
            ).hexdigest()
            
            auth_message = {
                "method": "server.sign",
                "params": [api_key, signature, timestamp],
                "id": 1234
            }
            
            # Use send_str() for aiohttp WebSocket
            await ws.send_str(json.dumps(auth_message))
            self.logger.info("Sent Gate.io authentication message")
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

    async def _subscribe_to_private_channels(self, conn_id: str, ws) -> bool:
        """
        Subscribe to private channels after authentication.
        
        Args:
            conn_id: Connection ID
            ws: WebSocket connection
            
        Returns:
            True if subscriptions successful, False otherwise
        """
        conn_info = self.connections[conn_id]
        exchange = conn_info['exchange']
        exchange_name = self._get_exchange_name(exchange)
        
        self.logger.info(f"Subscribing to private channels for {exchange_name}")
        
        try:
            if exchange_name == 'binance':
                # Binance: listenKey already handles all private messages
                # No additional subscriptions needed
                self.logger.info("Binance: Private messages handled via listenKey")
                return True
                
            elif exchange_name == 'bybit':
                # Subscribe to Bybit private channels
                subscription_messages = [
                    {"op": "subscribe", "args": ["order"]},
                    {"op": "subscribe", "args": ["execution"]},
                    {"op": "subscribe", "args": ["wallet"]},
                    {"op": "subscribe", "args": ["position"]}
                ]
                
                for msg in subscription_messages:
                    await ws.send_str(json.dumps(msg))
                    await asyncio.sleep(0.1)  # Small delay between subscriptions
                
                self.logger.info("Sent Bybit private channel subscriptions")
                return True
                
            elif exchange_name == 'mexc':
                # MEXC: Send separate subscription messages after authentication
                # Based on MEXC WebSocket documentation
                subscription_messages = [
                    {
                        "method": "SUBSCRIPTION",
                        "params": ["spot@private.orders.v3.api"]
                    },
                    {
                        "method": "SUBSCRIPTION", 
                        "params": ["spot@private.deals.v3.api"]
                    }
                ]
                
                for msg in subscription_messages:
                    await ws.send_str(json.dumps(msg))
                    await asyncio.sleep(0.1)  # Small delay between subscriptions
                
                self.logger.info("Sent MEXC private channel subscriptions")
                return True
                
            elif exchange_name == 'gateio':
                # Subscribe to Gate.io private channels
                subscription_messages = [
                    {"method": "subscribe", "params": ["spot.orders"], "id": 1001},
                    {"method": "subscribe", "params": ["spot.usertrades"], "id": 1002},
                    {"method": "subscribe", "params": ["spot.balances"], "id": 1003}
                ]
                
                for msg in subscription_messages:
                    await ws.send_str(json.dumps(msg))
                    await asyncio.sleep(0.1)
                
                self.logger.info("Sent Gate.io private channel subscriptions")
                return True
                
            elif exchange_name == 'bitget':
                # Subscribe to Bitget private channels
                subscription_messages = [
                    {"op": "subscribe", "args": [{"instType": "sp", "channel": "orders", "instId": "default"}]},
                    {"op": "subscribe", "args": [{"instType": "sp", "channel": "fills", "instId": "default"}]},
                    {"op": "subscribe", "args": [{"instType": "sp", "channel": "account", "instId": "default"}]}
                ]
                
                for msg in subscription_messages:
                    await ws.send_str(json.dumps(msg))
                    await asyncio.sleep(0.1)
                
                self.logger.info("Sent Bitget private channel subscriptions")
                return True
                
            elif exchange_name == 'hyperliquid':
                # Subscribe to additional Hyperliquid channels
                wallet_address = conn_info.get('auth_data', {}).get('walletAddress')
                if wallet_address:
                    subscription_messages = [
                        {"method": "subscribe", "subscription": {"type": "userEvents", "user": wallet_address}},
                        {"method": "subscribe", "subscription": {"type": "userFills", "user": wallet_address}},
                        {"method": "subscribe", "subscription": {"type": "userFundings", "user": wallet_address}}
                    ]
                    
                    for msg in subscription_messages:
                        await ws.send_str(json.dumps(msg))
                        await asyncio.sleep(0.1)
                    
                    self.logger.info("Sent Hyperliquid additional private subscriptions")
                return True
            
            else:
                self.logger.warning(f"No private channel subscription method for {exchange_name}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error subscribing to private channels for {exchange_name}: {e}")
            return False

# Import ReconnectionManager class if not moved to reconnection.py
try:
    from .reconnection import ReconnectionManager
except ImportError:
    # ReconnectionManager is defined in this file
    pass
