"""
Centralized WebSocket Connection Pool Manager.

Ensures only one WebSocket connection per exchange type across all strategies
and routes trade messages to the database for storage.
"""

import asyncio
import time
from typing import Dict, Set, Optional, Any, Callable, List
from dataclasses import dataclass
from enum import Enum
import structlog

from .ws_manager import WebSocketManager, WSState, WSMessageType
from ..base_connector import BaseExchangeConnector
from database.repositories import TradeRepository
from database import get_session
from order_management.enhanced_trade_sync import TradeDeduplicationManager
from utils.trade_sync_logger import TradeSyncLogger

logger = structlog.get_logger(__name__)


@dataclass
class ConnectionKey:
    """Unique key for WebSocket connections."""
    exchange_name: str
    market_type: str  # 'spot', 'future', 'perpetual'
    
    def __str__(self) -> str:
        return f"{self.exchange_name}_{self.market_type}"


class ConnectionStatus(Enum):
    """Connection status states."""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    ERROR = "error"


@dataclass
class ConnectionInfo:
    """Information about a WebSocket connection."""
    key: ConnectionKey
    connector: BaseExchangeConnector
    ws_manager: WebSocketManager
    conn_id: str
    status: ConnectionStatus
    subscribers: Set[str]  # Strategy instance IDs
    created_at: float
    last_message_time: Optional[float] = None
    error: Optional[str] = None


class WebSocketConnectionPool:
    """
    Centralized WebSocket connection pool manager.
    
    Features:
    - One connection per exchange type (e.g., binance_spot, binance_future)
    - Automatic trade message routing to database
    - Strategy subscription management
    - Connection sharing across multiple strategies
    """
    
    def __init__(self):
        """Initialize the connection pool."""
        self.connections: Dict[str, ConnectionInfo] = {}  # key -> ConnectionInfo
        self.strategy_subscriptions: Dict[str, Set[str]] = {}  # strategy_id -> set of connection keys
        self.trade_sync_logger = TradeSyncLogger()
        self._running = False
        self._stop_event = asyncio.Event()
        
        self.logger = logger.bind(component="WebSocketConnectionPool")
    
    async def start(self):
        """Start the connection pool."""
        if self._running:
            self.logger.warning("Connection pool already running")
            return
        
        self._running = True
        self._stop_event.clear()
        
        self.logger.info("WebSocket connection pool started")
    
    async def stop(self):
        """Stop the connection pool and close all connections."""
        if not self._running:
            return
        
        self._running = False
        self._stop_event.set()
        
        # Close all connections
        close_tasks = []
        for conn_info in self.connections.values():
            close_tasks.append(self._close_connection(conn_info))
        
        if close_tasks:
            self.logger.info(f"Closing {len(close_tasks)} WebSocket connections...")
            await asyncio.gather(*close_tasks, return_exceptions=True)
        
        self.connections.clear()
        self.strategy_subscriptions.clear()
        
        self.logger.info("WebSocket connection pool stopped")
    
    async def subscribe_strategy(
        self,
        strategy_id: str,
        exchange_connectors: Dict[str, BaseExchangeConnector],
        symbols: List[str]
    ) -> bool:
        """
        Subscribe a strategy to WebSocket connections for its exchanges.
        
        Args:
            strategy_id: Unique strategy instance ID
            exchange_connectors: Dictionary of exchange connectors
            symbols: List of symbols to monitor
            
        Returns:
            True if subscription successful, False otherwise
        """
        if not self._running:
            self.logger.error("Connection pool not running")
            return False
        
        self.logger.info(f"Subscribing strategy {strategy_id} to WebSocket connections")
        
        # Initialize strategy subscriptions
        if strategy_id not in self.strategy_subscriptions:
            self.strategy_subscriptions[strategy_id] = set()
        
        success_count = 0
        total_count = 0
        failed_exchanges = []
        
        for exchange_name, connector in exchange_connectors.items():
            total_count += 1
            
            # Determine market type from connector
            market_type = self._get_market_type(connector)
            connection_key = ConnectionKey(exchange_name, market_type)
            key_str = str(connection_key)
            
            try:
                # Get or create connection
                conn_info = await self._get_or_create_connection(connection_key, connector)
                
                if conn_info:
                    # Add strategy as subscriber
                    conn_info.subscribers.add(strategy_id)
                    self.strategy_subscriptions[strategy_id].add(key_str)
                    
                    self.logger.info(
                        f"Strategy {strategy_id} subscribed to {key_str} "
                        f"(total subscribers: {len(conn_info.subscribers)})"
                    )
                    success_count += 1
                else:
                    self.logger.error(f"Failed to create connection for {key_str}")
                    failed_exchanges.append(exchange_name)
                    
            except Exception as e:
                self.logger.error(f"Error subscribing strategy {strategy_id} to {key_str}: {e}")
                failed_exchanges.append(exchange_name)
        
        self.logger.info(
            f"Strategy {strategy_id} subscription complete: "
            f"{success_count}/{total_count} connections successful"
        )
        
        if failed_exchanges:
            self.logger.warning(f"Failed exchanges: {failed_exchanges}")
        
        # Return True if at least 50% of connections succeeded, or if we have at least 3 connections
        min_required = max(3, total_count // 2)
        return success_count >= min_required
    
    async def unsubscribe_strategy(self, strategy_id: str) -> bool:
        """
        Unsubscribe a strategy from all WebSocket connections.
        
        Args:
            strategy_id: Strategy instance ID
            
        Returns:
            True if unsubscription successful, False otherwise
        """
        if strategy_id not in self.strategy_subscriptions:
            self.logger.warning(f"Strategy {strategy_id} not found in subscriptions")
            return True
        
        self.logger.info(f"Unsubscribing strategy {strategy_id} from WebSocket connections")
        
        connection_keys = self.strategy_subscriptions[strategy_id].copy()
        
        for key_str in connection_keys:
            if key_str in self.connections:
                conn_info = self.connections[key_str]
                
                # Remove strategy from subscribers
                conn_info.subscribers.discard(strategy_id)
                
                self.logger.info(
                    f"Strategy {strategy_id} unsubscribed from {key_str} "
                    f"(remaining subscribers: {len(conn_info.subscribers)})"
                )
                
                # If no more subscribers, close the connection
                if not conn_info.subscribers:
                    self.logger.info(f"No more subscribers for {key_str}, closing connection")
                    await self._close_connection(conn_info)
                    del self.connections[key_str]
        
        # Clean up strategy subscriptions
        del self.strategy_subscriptions[strategy_id]
        
        self.logger.info(f"Strategy {strategy_id} unsubscribed from all connections")
        return True
    
    def get_connection_status(self) -> Dict[str, Any]:
        """
        Get status of all connections.
        
        Returns:
            Dictionary with connection status information
        """
        status = {
            'total_connections': len(self.connections),
            'total_strategies': len(self.strategy_subscriptions),
            'connections': {},
            'strategies': {}
        }
        
        # Connection details
        for key_str, conn_info in self.connections.items():
            status['connections'][key_str] = {
                'exchange': conn_info.key.exchange_name,
                'market_type': conn_info.key.market_type,
                'status': conn_info.status.value,
                'subscribers': list(conn_info.subscribers),
                'subscriber_count': len(conn_info.subscribers),
                'uptime': time.time() - conn_info.created_at,
                'last_message': conn_info.last_message_time,
                'error': conn_info.error
            }
        
        # Strategy subscriptions
        for strategy_id, connection_keys in self.strategy_subscriptions.items():
            status['strategies'][strategy_id] = {
                'connections': list(connection_keys),
                'connection_count': len(connection_keys)
            }
        
        return status
    
    async def _get_or_create_connection(
        self,
        connection_key: ConnectionKey,
        connector: BaseExchangeConnector
    ) -> Optional[ConnectionInfo]:
        """
        Get existing connection or create a new one.
        
        Args:
            connection_key: Connection key
            connector: Exchange connector
            
        Returns:
            ConnectionInfo or None if failed
        """
        key_str = str(connection_key)
        
        # Check if connection already exists
        if key_str in self.connections:
            conn_info = self.connections[key_str]
            
            # Check if connection is healthy
            if conn_info.status in [ConnectionStatus.CONNECTED, ConnectionStatus.CONNECTING]:
                self.logger.info(f"Reusing existing connection for {key_str}")
                return conn_info
            else:
                self.logger.warning(f"Existing connection for {key_str} is unhealthy, recreating")
                await self._close_connection(conn_info)
                del self.connections[key_str]
        
        # Create new connection
        self.logger.info(f"Creating new WebSocket connection for {key_str}")
        
        try:
            # Create WebSocket manager
            ws_manager = WebSocketManager({
                'ping_interval': 30.0,
                'pong_timeout': 10.0,
                'reconnect_enabled': True,
                'max_message_size': 10485760
            })
            
            await ws_manager.start()
            
            # Register trade message handler
            ws_manager.register_handler(WSMessageType.ORDER_UPDATE, self._handle_trade_message)
            ws_manager.register_handler(WSMessageType.TRADE, self._handle_trade_message)
            
            # Get private WebSocket endpoint
            endpoint = self._get_private_websocket_endpoint(connector)
            if not endpoint:
                self.logger.error(f"Failed to get WebSocket endpoint for {key_str}")
                return None
            
            # Special handling for Hyperliquid - check if we have credentials
            exchange_name = self._get_exchange_name(connector)
            if exchange_name == 'hyperliquid':
                # Check if Hyperliquid has wallet credentials
                has_wallet = (
                    (hasattr(connector, 'walletAddress') and hasattr(connector, 'privateKey')) or
                    (hasattr(connector, 'exchange') and 
                     hasattr(connector.exchange, 'walletAddress') and 
                     hasattr(connector.exchange, 'privateKey'))
                )
                
                if not has_wallet:
                    self.logger.warning(f"Hyperliquid missing wallet credentials, skipping private WebSocket for {key_str}")
                    return None
            
            # Connect to exchange
            conn_id = await ws_manager.connect_exchange(
                exchange=connector,
                endpoint=endpoint,
                conn_type='private'
            )
            
            if not conn_id:
                self.logger.error(f"Failed to connect to {key_str}")
                await ws_manager.stop()
                return None
            
            # Create connection info
            conn_info = ConnectionInfo(
                key=connection_key,
                connector=connector,
                ws_manager=ws_manager,
                conn_id=conn_id,
                status=ConnectionStatus.CONNECTING,
                subscribers=set(),
                created_at=time.time()
            )
            
            # Store connection
            self.connections[key_str] = conn_info
            
            # Wait for connection to be established
            await self._wait_for_connection(conn_info)
            
            self.logger.info(f"Successfully created WebSocket connection for {key_str}")
            return conn_info
            
        except Exception as e:
            self.logger.error(f"Failed to create connection for {key_str}: {e}")
            
            # For Hyperliquid, this might be expected if no credentials
            if exchange_name == 'hyperliquid' and 'credentials' in str(e).lower():
                self.logger.info(f"Skipping Hyperliquid private WebSocket due to missing credentials")
            
            return None
    
    async def _close_connection(self, conn_info: ConnectionInfo):
        """Close a WebSocket connection."""
        try:
            conn_info.status = ConnectionStatus.DISCONNECTED
            
            if conn_info.ws_manager:
                await conn_info.ws_manager.close_connection(conn_info.conn_id)
                await conn_info.ws_manager.stop()
            
            self.logger.info(f"Closed connection for {conn_info.key}")
            
        except Exception as e:
            self.logger.error(f"Error closing connection for {conn_info.key}: {e}")
    
    async def _wait_for_connection(self, conn_info: ConnectionInfo, timeout: float = 30.0):
        """Wait for connection to be established."""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            # Check connection status
            status = conn_info.ws_manager.get_connection_status(conn_info.conn_id)
            
            if status and status.get('state') == WSState.CONNECTED:
                conn_info.status = ConnectionStatus.CONNECTED
                self.logger.info(f"Connection established for {conn_info.key}")
                return
            elif status and status.get('state') == WSState.ERROR:
                conn_info.status = ConnectionStatus.ERROR
                conn_info.error = status.get('error')
                raise Exception(f"Connection failed: {conn_info.error}")
            
            await asyncio.sleep(0.5)
        
        # Timeout
        conn_info.status = ConnectionStatus.ERROR
        conn_info.error = "Connection timeout"
        raise Exception("Connection timeout")
    
    async def _handle_trade_message(self, message: Dict[str, Any]):
        """
        Handle trade messages from WebSocket and store in database.
        
        Args:
            message: Normalized trade message
        """
        try:
            exchange_name = message.get('exchange')
            if not exchange_name:
                return
            
            # Log trade message reception
            self.trade_sync_logger.log_trade_fetch_start(
                exchange_name, 
                message.get('symbol', 'unknown'), 
                'websocket'
            )
            
            # Convert WebSocket message to trade format
            trade_data = self._convert_websocket_to_trade(message)
            
            if trade_data:
                # Store trade in database
                await self._store_trade_message(exchange_name, trade_data)
                
                self.trade_sync_logger.log_trade_storage(
                    exchange_name,
                    trade_data.get('symbol', 'unknown'),
                    1,  # count
                    0,  # duplicates
                    'websocket'
                )
            
        except Exception as e:
            self.logger.error(f"Error handling trade message: {e}")
    
    def _convert_websocket_to_trade(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Convert WebSocket message to trade data format.
        
        Args:
            message: Normalized WebSocket message
            
        Returns:
            Trade data dictionary or None
        """
        try:
            # Extract trade information from WebSocket message
            if message.get('type') == WSMessageType.ORDER_UPDATE:
                # Order update message - check if it's a trade execution
                if message.get('status') in ['filled', 'partially_filled']:
                    return {
                        'id': message.get('id'),
                        'order_id': message.get('id'),
                        'symbol': message.get('symbol'),
                        'side': message.get('side'),
                        'amount': float(message.get('filled', 0)),
                        'price': float(message.get('average_price', 0)) or float(message.get('price', 0)),
                        'cost': float(message.get('filled', 0)) * float(message.get('average_price', 0) or message.get('price', 0)),
                        'timestamp': message.get('timestamp'),
                        'fee': {
                            'cost': float(message.get('fee', 0)),
                            'currency': message.get('fee_currency', 'USDT')
                        }
                    }
            
            elif message.get('type') == WSMessageType.TRADE:
                # Direct trade message
                return {
                    'id': message.get('id'),
                    'order_id': message.get('order_id'),
                    'symbol': message.get('symbol'),
                    'side': message.get('side'),
                    'amount': float(message.get('amount', 0)),
                    'price': float(message.get('price', 0)),
                    'cost': float(message.get('amount', 0)) * float(message.get('price', 0)),
                    'timestamp': message.get('timestamp'),
                    'fee': {
                        'cost': float(message.get('fee', 0)),
                        'currency': message.get('fee_currency', 'USDT')
                    }
                }
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error converting WebSocket message to trade: {e}")
            return None
    
    async def _store_trade_message(self, exchange_name: str, trade_data: Dict[str, Any]):
        """
        Store trade message in database using the deduplication system.
        
        Args:
            exchange_name: Exchange name
            trade_data: Trade data dictionary
        """
        try:
            async for session in get_session():
                # Use the same deduplication system as enhanced trade sync
                dedup_manager = TradeDeduplicationManager(session)
                
                # Insert trade with deduplication
                success, reason = await dedup_manager.insert_trade_with_deduplication(
                    trade_data, exchange_name
                )
                
                if success:
                    self.logger.info(f"Stored WebSocket trade: {exchange_name} {trade_data.get('symbol')} {trade_data.get('id')}")
                else:
                    self.logger.debug(f"Trade already exists: {reason}")
                
                break  # Only use first session
                
        except Exception as e:
            self.logger.error(f"Error storing trade message: {e}")
    
    def _get_market_type(self, connector: BaseExchangeConnector) -> str:
        """
        Determine market type from connector.
        
        Args:
            connector: Exchange connector
            
        Returns:
            Market type string ('spot', 'future', 'perpetual')
        """
        # Check for market_type attribute
        if hasattr(connector, 'market_type'):
            market_type = connector.market_type
            if market_type in ['future', 'futures']:
                return 'future'
            elif market_type in ['perpetual', 'perp']:
                return 'perpetual'
            else:
                return 'spot'
        
        # Check class name for hints
        class_name = connector.__class__.__name__.lower()
        if 'future' in class_name or 'perp' in class_name:
            return 'future'
        
        # Check underlying exchange if available
        if hasattr(connector, 'exchange') and connector.exchange:
            exchange_id = getattr(connector.exchange, 'id', '').lower()
            if 'usdm' in exchange_id or 'coinm' in exchange_id:
                return 'future'
        
        # Default to spot
        return 'spot'
    
    def _get_private_websocket_endpoint(self, connector: BaseExchangeConnector) -> Optional[str]:
        """
        Get private WebSocket endpoint for exchange.
        
        Args:
            connector: Exchange connector
            
        Returns:
            WebSocket endpoint URL or None
        """
        # Exchange-specific private WebSocket endpoints
        exchange_name = self._get_exchange_name(connector)
        market_type = self._get_market_type(connector)
        
        endpoints = {
            'binance': {
                'spot': 'wss://stream.binance.com:9443/ws',
                'future': 'wss://fstream.binance.com/ws'
            },
            'bybit': {
                'spot': 'wss://stream.bybit.com/v5/private',
                'future': 'wss://stream.bybit.com/v5/private'
            },
            'mexc': {
                'spot': 'wss://wbs.mexc.com/ws',
                'future': 'wss://contract.mexc.com/ws'
            },
            'gateio': {
                'spot': 'wss://api.gateio.ws/ws/v4/',
                'future': 'wss://fx-ws.gateio.ws/v4/ws'
            },
            'bitget': {
                'spot': 'wss://ws.bitget.com/spot/v1/stream',
                'future': 'wss://ws.bitget.com/mix/v1/stream'
            },
            'hyperliquid': {
                'future': 'wss://api.hyperliquid.xyz/ws',
                'perpetual': 'wss://api.hyperliquid.xyz/ws',
                'spot': 'wss://api.hyperliquid.xyz/ws'  # Fallback for any market type
            }
        }
        
        if exchange_name in endpoints:
            exchange_endpoints = endpoints[exchange_name]
            if market_type in exchange_endpoints:
                return exchange_endpoints[market_type]
            elif 'spot' in exchange_endpoints:
                return exchange_endpoints['spot']  # Fallback to spot
        
        # Special handling for Hyperliquid - always return the endpoint
        if exchange_name == 'hyperliquid':
            self.logger.info(f"Using Hyperliquid WebSocket endpoint for {market_type}")
            return 'wss://api.hyperliquid.xyz/ws'
        
        self.logger.warning(f"No WebSocket endpoint found for {exchange_name} {market_type}")
        return None
    
    def _get_exchange_name(self, connector: BaseExchangeConnector) -> str:
        """Get exchange name from connector."""
        # Try different methods to get the exchange name
        if hasattr(connector, 'name') and connector.name and connector.name != 'unknown':
            name = connector.name.lower()
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
        
        # Check the class name
        class_name = connector.__class__.__name__.lower()
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
        
        return 'unknown'


# Global connection pool instance
_connection_pool: Optional[WebSocketConnectionPool] = None


def get_connection_pool() -> WebSocketConnectionPool:
    """Get the global WebSocket connection pool instance."""
    global _connection_pool
    if _connection_pool is None:
        _connection_pool = WebSocketConnectionPool()
    return _connection_pool


async def initialize_connection_pool() -> WebSocketConnectionPool:
    """Initialize and start the global WebSocket connection pool."""
    pool = get_connection_pool()
    if not pool._running:
        await pool.start()
    return pool


async def shutdown_connection_pool():
    """Shutdown the global WebSocket connection pool."""
    global _connection_pool
    if _connection_pool and _connection_pool._running:
        await _connection_pool.stop()
        _connection_pool = None 