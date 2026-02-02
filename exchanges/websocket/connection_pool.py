"""
Centralized WebSocket Connection Pool Manager.

Ensures only one WebSocket connection per exchange type across all strategies
and routes trade messages to the database for storage.
"""

import asyncio
import time
from typing import Dict, Set, Optional, Any, Callable, List, Union
from dataclasses import dataclass
from enum import Enum
import structlog
import json
import redis.asyncio as redis

from .ws_manager import WebSocketManager, WSState, WSMessageType
from ..base_connector import BaseExchangeConnector
from database.repositories import TradeRepository
from database import get_session
from order_management.enhanced_trade_sync import TradeDeduplicationManager
from utils.trade_sync_logger import TradeSyncLogger
from trade_processor import TradeProcessor

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
    symbols: Set[str]  # Symbols to monitor
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
    
    def __init__(self, redis_url="redis://localhost:6379"):
        """Initialize the connection pool."""
        self.connections: Dict[str, ConnectionInfo] = {}  # key -> ConnectionInfo
        self.strategy_subscriptions: Dict[str, Set[str]] = {}  # strategy_id -> set of connection keys
        self.trade_sync_logger = TradeSyncLogger()
        self._running = False
        self._stop_event = asyncio.Event()
        self.redis_url = redis_url
        self.redis_client = None
        self.trade_processor = None
        
        self.logger = logger.bind(component="WebSocketConnectionPool")
        
        # Strategy notification callbacks
        self.strategy_callbacks: Dict[str, callable] = {}  # strategy_id -> callback function
        
        # Bitget-specific deduplication cache (no longer needed with fill channel)
        # Keeping for potential future use but not actively used
        self._bitget_processed_orders: Dict[str, Dict[str, Any]] = {}  # order_id -> {volume, timestamp}
    
    async def start(self):
        """Start the connection pool."""
        if self._running:
            self.logger.warning("Connection pool already running")
            return
        
        self._running = True
        self._stop_event.clear()
        
        # Ensure ms granularity in this module's logger as well
        try:
            import logging
            from logging.handlers import RotatingFileHandler
            pool_logger = logging.getLogger('WebSocketConnectionPool')
            for h in pool_logger.handlers:
                try:
                    h.setFormatter(logging.Formatter('%(asctime)s,%(msecs)03d - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
                except Exception:
                    pass
        except Exception:
            pass

        # Initialize Redis client
        self.redis_client = redis.from_url(self.redis_url)
        
        # Initialize central trade processor
        self.trade_processor = TradeProcessor(redis_url=self.redis_url)
        await self.trade_processor.initialize()
        
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
        
        if self.trade_processor:
            await self.trade_processor.close()
            
        if self.redis_client:
            await self.redis_client.close()
        
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
        
        # Create concurrent connection tasks
        connection_tasks = []
        exchange_keys = []
        
        for exchange_name, connector in exchange_connectors.items():
            # Determine market type from connector
            market_type = self._get_market_type(connector)
            base_name = exchange_name
            suffix = f"_{market_type}"
            if base_name.lower().endswith(suffix):
                base_name = base_name[:-len(suffix)]
            connection_key = ConnectionKey(base_name, market_type)
            
            # Create task for concurrent connection
            # Ensure we pass the actual WS-capable instance if available (e.g., Bitfinex pro)
            ws_connector = connector
            try:
                if hasattr(connector, 'exchange_ws') and connector.exchange_ws is not None:
                    # Create a lightweight proxy that exposes name and uses exchange_ws underneath
                    class _WSProxy:
                        def __init__(self, parent):
                            self.parent = parent
                            # keep the fully-qualified name (e.g., bitfinex_spot)
                            self.name = getattr(parent, 'name', 'unknown')
                            self.exchange = parent.exchange_ws
                        def __getattr__(self, item):
                            return getattr(self.parent, item)
                    ws_connector = _WSProxy(connector)
            except Exception:
                ws_connector = connector
            task = self._get_or_create_connection(connection_key, ws_connector)
            connection_tasks.append(task)
            exchange_keys.append((exchange_name, connection_key))
        
        total_count = len(connection_tasks)
        
        # Execute all connection tasks concurrently
        self.logger.info(f"🚀 Creating {total_count} WebSocket connections concurrently...")
        start_time = asyncio.get_event_loop().time()
        
        results = await asyncio.gather(*connection_tasks, return_exceptions=True)
        
        end_time = asyncio.get_event_loop().time()
        self.logger.info(f"⚡ Concurrent WebSocket connections completed in {end_time - start_time:.2f} seconds")
        
        # Process results
        success_count = 0
        failed_exchanges = []
        
        for i, result in enumerate(results):
            exchange_name, connection_key = exchange_keys[i]
            key_str = str(connection_key)
            
            try:
                if isinstance(result, Exception):
                    self.logger.error(f"Error subscribing strategy {strategy_id} to {key_str}: {result}")
                    failed_exchanges.append(exchange_name)
                elif result:
                    # Add strategy as subscriber
                    result.subscribers.add(strategy_id)
                    # Add symbols to connection info
                    result.symbols.update(symbols)
                    self.strategy_subscriptions[strategy_id].add(key_str)
                    
                    # Pass symbols to WebSocket manager for re-subscription
                    if result.ws_manager and result.conn_id:
                        conn_info = result.ws_manager.connections.get(result.conn_id)
                        if conn_info:
                            conn_info['symbols'] = symbols
                    
                    self.logger.info(
                        f"Strategy {strategy_id} subscribed to {key_str} "
                        f"(total subscribers: {len(result.subscribers)})"
                    )
                    success_count += 1
                else:
                    self.logger.error(f"Failed to create connection for {key_str}")
                    failed_exchanges.append(exchange_name)
                    
            except Exception as e:
                self.logger.error(f"Error processing connection result for {key_str}: {e}")
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
        
        # Clean up strategy callback
        self.unregister_strategy_callback(strategy_id)
        
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
            exchange_full = self._get_exchange_name(connector)
            root_exchange = exchange_full.split('_')[0] if '_' in exchange_full else exchange_full
            if root_exchange == 'hyperliquid':
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
                url=endpoint,
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
                symbols=set(),  # Initialize empty, will be populated by subscribe_strategy
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
            if root_exchange == 'hyperliquid' and 'credentials' in str(e).lower():
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
    
    async def _handle_trade_message(self, conn_id: str, message: Dict[str, Any]):
        """
        Handle trade messages from WebSocket and store in database.
        
        Args:
            conn_id: Connection ID (required by WebSocket manager)
            message: Normalized trade message
        """
        try:
            # Log full message for debugging
            self.logger.critical(f"FULL_TRADE_MESSAGE_RECEIVED: {json.dumps(message, indent=2)}")
            
            exchange_full = self._get_exchange_name(message.get('exchange', '').lower())
            root_exchange = exchange_full.split('_')[0] if '_' in exchange_full else exchange_full
            if not exchange_full:
                self.logger.warning("⚠️ Trade message missing exchange name")
                return
            
            # Log trade message reception
            self.logger.info(f"📥 Received trade message from {exchange_full}: "
                           f"type={message.get('type')}, "
                           f"symbol={message.get('symbol')}, "
                           f"id={message.get('id')}")
            
            self.trade_sync_logger.log_trade_fetch_start(
                exchange_full, 
                message.get('symbol', 'unknown'), 
                'websocket'
            )
            
            # Convert WebSocket message to trade format
            trade_data = self._convert_websocket_to_trade(message)
            
            if trade_data:
                self.logger.info(f"✅ Converted WebSocket message to trade data: "
                               f"order_id={trade_data.get('order_id')}, "
                               f"amount={trade_data.get('amount')}, "
                               f"side={trade_data.get('side')}")
                
                # Notify strategies FIRST (non-blocking for storage)
                strategy_count = len(self.strategy_callbacks)
                self.logger.info(f"📢 Notifying {strategy_count} strategies of trade confirmation")
                await self._notify_strategies_of_trade(trade_data)

                # Run dedup/storage in the background
                if self.trade_processor:
                    async def _bg_store(exchange_full_arg: str, trade: Dict[str, Any]):
                        try:
                            await self.trade_processor.process_trade(exchange_full_arg, trade)
                            self.trade_sync_logger.log_trade_storage(
                                exchange_full_arg,
                                trade.get('symbol', 'unknown'),
                                1,
                                0,
                                'websocket'
                            )
                        except Exception as store_err:
                            self.logger.error(f"Background trade storage error: {store_err}")
                    asyncio.create_task(_bg_store(exchange_full, trade_data))
                else:
                    self.logger.error("Trade processor not initialized!")
            else:
                self.logger.warning(f"⚠️ Failed to convert WebSocket message to trade data from {exchange_full}")
                self.logger.debug(f"Original message: {message}")
            
        except Exception as e:
            self.logger.error(f"Error handling trade message: {e}")
            self.logger.error(f"Message: {message}")
    
    def _convert_websocket_to_trade(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Convert WebSocket message to trade data format.
        
        Args:
            message: Normalized WebSocket message
            
        Returns:
            Trade data dictionary or None
        """
        try:
            self.logger.debug(f"🔄 Converting WebSocket message: type={message.get('type')}, exchange={message.get('exchange')}")
            
            raw_message = message.get('raw', {})
            exchange_full = self._get_exchange_name(message.get('exchange', '').lower())
            root_exchange = exchange_full.split('_')[0] if '_' in exchange_full else exchange_full
            msg_type = message.get('type')

            # --- Exchange-Specific Trade Detection ---
            # This logic identifies if an ORDER_UPDATE or TRADE message is a true trade execution.

            is_trade = False
            trade_payload = {}

            if msg_type == WSMessageType.TRADE:
                # Double-check for MEXC that it's not an order update
                if root_exchange == 'mexc' and 'private.orders' in raw_message.get('c', ''):
                    self.logger.debug("Ignoring MEXC order update misidentified as trade")
                    return None
                is_trade = True
                trade_payload = message # The normalized message is already a trade
            
            elif msg_type == WSMessageType.ORDER_UPDATE:
                # Binance: Look for 'executionReport' with execution type 'TRADE'
                if root_exchange == 'binance':
                    # CRITICAL FIX: Filter out non-trade messages
                    event_type = raw_message.get('e')
                    if event_type == 'outboundAccountPosition':
                        # This is a balance update, not a trade
                        self.logger.debug("Ignoring Binance balance update message")
                        return None
                    elif event_type == 'executionReport' and raw_message.get('x') == 'TRADE':
                        is_trade = True
                        trade_payload = message
                    elif event_type == 'ORDER_TRADE_UPDATE' and raw_message.get('o', {}).get('x') == 'TRADE':
                        is_trade = True
                        trade_payload = message

                # Bybit: Look for 'execution' topic
                elif root_exchange == 'bybit':
                    if 'execution' in raw_message.get('topic', ''):
                        is_trade = True
                        trade_payload = message

                # MEXC: Look for 'private.deals' channel
                elif root_exchange == 'mexc':
                    if 'private.deals' in raw_message.get('c', ''):
                        is_trade = True
                        trade_payload = message

                # Gate.io: Look for 'spot.usertrades' channel
                elif root_exchange == 'gateio':
                    if 'usertrades' in message.get('raw', {}).get('channel', ''):
                        is_trade = True
                        trade_payload = message

                # Bitget: Look for fills channel OR filled orders in orders channel
                elif root_exchange == 'bitget':
                    channel = raw_message.get('arg', {}).get('channel', '')
                    if 'fills' in channel or 'fill' in channel:
                        is_trade = True
                        trade_payload = message
                    # CRITICAL: With fill channel, we should NOT process orders channel as trades
                    # The orders channel only contains order status updates
                    # All trade data comes from the fill channel
                
                # Hyperliquid: Look for 'userFills'
                elif root_exchange == 'hyperliquid':
                    if 'userFills' in raw_message.get('channel', ''):
                        is_trade = True
                        trade_payload = message

                # Generic Fallback: If no specific logic matches, check status
                if not is_trade:
                    status = message.get('status', '').lower() if message.get('status') else ''
                    filled = float(message.get('filled', 0)) if message.get('filled') else 0
                    if status in ['filled', 'partially_filled'] and filled > 0:
                        is_trade = True
                        trade_payload = message
                        self.logger.debug(f"Converted to trade via generic status check: {exchange_full} {status} {filled}")

            if not is_trade:
                self.logger.debug(f"Message not a trade execution: type={msg_type}, exchange={exchange_full}")
                return None

            # --- Trade Data Extraction ---
            # Exchange-specific extraction blocks – these return immediately when they
            # successfully build a complete trade dictionary.

            try:
                if root_exchange == 'binance':
                    # Spot
                    if raw_message.get('e') == 'executionReport' and raw_message.get('x') == 'TRADE':
                        data = raw_message
                    # Futures (userDataStream V2)
                    elif raw_message.get('e') == 'ORDER_TRADE_UPDATE' and raw_message.get('o', {}).get('x') == 'TRADE':
                        data = raw_message['o']
                    else:
                        data = None

                    if data:
                        return {
                            'id': str(data.get('t', '')),
                            'order_id': str(data.get('i', '')),
                            'client_order_id': data.get('c') or data.get('C'),
                            'symbol': data.get('s'),
                            'side': data.get('S', '').lower(),
                            'amount': float(data.get('l', 0)),
                            'price': float(data.get('L', 0)),
                            'fee': float(data.get('n', 0)),
                            'timestamp': data.get('T'),
                            'exchange': exchange_full
                        }

                elif root_exchange == 'bybit':
                    if 'execution' in raw_message.get('topic', '') and raw_message.get('data'):
                        d = raw_message['data'][0]
                        return {
                            'id': str(d.get('execId', '')),
                            'order_id': str(d.get('orderId', '')),
                            'symbol': d.get('symbol'),
                            'side': d.get('side', '').lower(),
                            'amount': float(d.get('execQty', 0)),
                            'price': float(d.get('execPrice', 0)),
                            'fee': float(d.get('execFee', 0)),
                            'timestamp': d.get('execTime'),
                            'exchange': exchange_full
                        }

                elif root_exchange == 'mexc':
                    if raw_message.get('c', '').startswith('spot@private.deals') and raw_message.get('d'):
                        # MEXC sends a single trade object, not an array
                        d = raw_message['d'] if isinstance(raw_message['d'], dict) else raw_message['d'][0]
                        
                        # Get the trade ID from 't' field, not dealId
                        trade_id = str(d.get('t', ''))  # This is the actual trade ID like "560388376742756352X1"
                        
                        # Get symbol from outer message
                        symbol = raw_message.get('s', d.get('symbol', ''))
                        
                        # Format symbol if needed (BERAUSDT -> BERA/USDT)
                        if '/' not in symbol and symbol:
                            # Try common quote currencies
                            for quote in ['USDT', 'USDC', 'BTC', 'ETH', 'BUSD']:
                                if symbol.endswith(quote):
                                    base = symbol[:-len(quote)]
                                    if base:
                                        symbol = f"{base}/{quote}"
                                        break
                        
                        # S: 1 = Buy, 2 = Sell
                        side = 'buy' if d.get('S') == 1 else 'sell'
                        
                        return {
                            'id': trade_id,
                            'order_id': str(d.get('i', '')),  # Order ID
                            'symbol': symbol,
                            'side': side,
                            'amount': float(d.get('v', 0)),  # Volume
                            'price': float(d.get('p', 0)),   # Price
                            'fee': float(d.get('n', 0)),     # Fee amount
                            'timestamp': int(d.get('T', 0)), # Trade time
                            'exchange': exchange_full
                        }

                elif root_exchange == 'gateio':
                    if raw_message.get('channel') == 'spot.usertrades' and raw_message.get('result'):
                        d = raw_message['result'][0]
                        # Normalize Gate.io symbol from underscore to slash format
                        symbol = d.get('currency_pair', message.get('symbol'))
                        if symbol and '_' in symbol:
                            # Convert BERA_USDT to BERA/USDT
                            symbol = symbol.replace('_', '/')
                        
                        # Use timestamp from normalized message which is properly calculated
                        return {
                            'id': str(d.get('id', '')),
                            'order_id': str(d.get('order_id', d.get('text', ''))),
                            'symbol': symbol,
                            'side': d.get('side', '').lower(),
                            'amount': float(d.get('amount', 0)),
                            'price': float(d.get('price', 0)),
                            'fee': float(d.get('fee', 0)),
                            'timestamp': message.get('timestamp'),  # Use normalized timestamp
                            'exchange': exchange_full
                        }

                elif root_exchange == 'bitget':
                    channel = raw_message.get('arg', {}).get('channel', '')
                    data = raw_message.get('data', [])
                    
                    if channel in ['fills', 'fill'] and data:
                        # Handle fill channel format - each message is an individual trade
                        d = data[0] if isinstance(data, list) else data
                        
                        # Extract fee from feeDetail array
                        fee = 0.0
                        fee_details = d.get('feeDetail', [])
                        if fee_details and isinstance(fee_details, list):
                            for fee_detail in fee_details:
                                # Use totalFee field from each fee detail
                                fee += abs(float(fee_detail.get('totalFee', 0)))
                        
                        return {
                            'id': str(d.get('tradeId', '')),
                            'order_id': str(d.get('orderId', '')),
                            'symbol': d.get('symbol', message.get('symbol')),
                            'side': d.get('side', '').lower(),
                            'amount': float(d.get('size', 0)),  # Individual fill size
                            'price': float(d.get('priceAvg', 0)),  # Average price for this fill
                            'fee': fee,
                            'timestamp': int(d.get('cTime', d.get('uTime', 0))),
                            'exchange': exchange_full
                        }
                    # CRITICAL: Do NOT process orders channel as trades
                    # All trade data comes from the fill channel



            except Exception as parse_err:
                self.logger.debug(f"Exchange-specific parse failed for {exchange_full}: {parse_err}")
            


            # The 'message' object is already normalized by ws_manager.py, so we can use its fields.
            # For TRADE messages, the normalized fields are directly available
            if msg_type == WSMessageType.TRADE:
                # Use the normalized fields directly
                trade_data = {
                    'id': str(message.get('id', '')),
                    'order_id': str(message.get('order_id', '')),
                    'client_order_id': message.get('client_order_id'),
                    'symbol': message.get('symbol'),
                    'side': message.get('side', '').lower(),
                    'amount': float(message.get('amount', 0)),
                    'price': float(message.get('price', 0)),
                    'fee': float(message.get('fee', 0)) if message.get('fee') is not None else 0.0,
                    'fee_currency': message.get('fee_currency'),
                    'timestamp': message.get('timestamp'),
                    'exchange': exchange_full
                }
                # Propagate Hyperliquid snapshot flag from raw payload (userFills -> data.isSnapshot)
                if root_exchange == 'hyperliquid':
                    try:
                        is_snap = False
                        if isinstance(raw_message, dict):
                            data_obj = raw_message.get('data', {})
                            if isinstance(data_obj, dict):
                                is_snap = bool(data_obj.get('isSnapshot', False))
                        trade_data['is_snapshot'] = is_snap
                    except Exception:
                        trade_data['is_snapshot'] = False
                
                # CRITICAL FIX: Normalize Hyperliquid symbol from BERA/USDC to BERA/USDT
                if root_exchange == 'hyperliquid' and trade_data['symbol'] == 'BERA/USDC':
                    trade_data['symbol'] = 'BERA/USDT'
                    self.logger.debug(f"Normalized Hyperliquid symbol from BERA/USDC to BERA/USDT for strategy compatibility")
                
                # Validate required fields
                if not trade_data['side'] or not trade_data['symbol'] or trade_data['amount'] == 0:
                    self.logger.debug(f"Trade message missing required fields: {trade_data}")
                    return None
                    
                self.logger.info(f"✅ Converted trade message: {trade_data['side']} {trade_data['amount']} {trade_data['symbol']} @ {trade_data['price']}")
                return trade_data
            
            # For ORDER_UPDATE messages that represent trades, use the normalized fields
            side = trade_payload.get('side')
            symbol = trade_payload.get('symbol')
            # For order updates that are trades, the 'filled' key holds the trade amount.
            # For direct 'trade' messages, it's in the 'amount' key.
            amount = trade_payload.get('filled') 
            if amount is None:
                amount = trade_payload.get('amount')

            # CRITICAL FIX: Don't try to process messages without required fields
            if not side or not symbol or amount is None:
                self.logger.debug(f"Trade message missing required fields. Side: {side}, Symbol: {symbol}, Amount: {amount}")
                return None

            trade_data = {
                'id': trade_payload.get('id'),
                'order_id': trade_payload.get('order_id') or trade_payload.get('id'), # Fallback to id if order_id is missing
                'client_order_id': trade_payload.get('client_order_id'),
                'symbol': symbol,
                'side': side.lower() if side else '',
                'amount': float(amount),
                'price': float(trade_payload.get('price', 0)),
                'fee': float(trade_payload.get('fee', 0)) if trade_payload.get('fee') is not None else 0.0,
                'timestamp': trade_payload.get('timestamp'),
                'exchange': exchange_full
            }
            # Propagate Hyperliquid snapshot flag from raw payload when applicable
            if root_exchange == 'hyperliquid':
                try:
                    is_snap = False
                    if isinstance(raw_message, dict):
                        data_obj = raw_message.get('data', {})
                        if isinstance(data_obj, dict):
                            is_snap = bool(data_obj.get('isSnapshot', False))
                    trade_data['is_snapshot'] = is_snap
                except Exception:
                    trade_data['is_snapshot'] = False
            
            self.logger.info(f"✅ Converted trade message: {trade_data['side']} {trade_data['amount']} {trade_data['symbol']} @ {trade_data['price']}")
            return trade_data

        except Exception as e:
            self.logger.error(f"Error converting WebSocket message to trade: {e}")
            self.logger.error(f"Message: {message}")
            return None
    
    async def _store_trade_message(self, exchange_full: str, trade_data: Dict[str, Any]):
        """
        Store trade message in database using the deduplication system.
        
        Args:
            exchange_full: Full exchange name
            trade_data: Trade data dictionary
        """
        # --- DIAGNOSTIC LOG ---
        self.logger.critical(f"STORE_TRADE_MESSAGE_CALLED for {exchange_full} with: {trade_data}")
        # --- END DIAGNOSTIC LOG ---
        try:
            # This method is now effectively replaced by the trade_processor
            # We keep it for logging and potential future use, but the core logic
            # is now in the central trade_processor.
            if self.trade_processor:
                # The actual insertion happens in _handle_trade_message via trade_processor
                self.logger.info("Trade processing is now handled by the central TradeProcessor.")
            else:
                 self.logger.error("Trade processor not initialized!")
                
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
        exchange_full = self._get_exchange_name(connector)
        root_exchange = exchange_full.split('_')[0] if '_' in exchange_full else exchange_full
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
                'spot': 'wss://ws.bitget.com/v2/ws/private',
                'future': 'wss://ws.bitget.com/v2/ws/private'
            },
            'bitfinex': {
                # Bitfinex v2 WebSocket endpoint (same URL for public/private; auth handled at message level)
                'spot': 'wss://api.bitfinex.com/ws/2',
                'future': 'wss://api.bitfinex.com/ws/2'
            },
            'hyperliquid': {
                'future': 'wss://api.hyperliquid.xyz/ws',
                'perpetual': 'wss://api.hyperliquid.xyz/ws',
                'spot': 'wss://api.hyperliquid.xyz/ws'  # Fallback for any market type
            }
        }
        
        if root_exchange in endpoints:
            exchange_endpoints = endpoints[root_exchange]
            if market_type in exchange_endpoints:
                return exchange_endpoints[market_type]
            elif 'spot' in exchange_endpoints:
                return exchange_endpoints['spot']  # Fallback to spot
        
        # Special handling for Hyperliquid - always return the endpoint
        if root_exchange == 'hyperliquid':
            self.logger.info(f"Using Hyperliquid WebSocket endpoint for {market_type}")
            return 'wss://api.hyperliquid.xyz/ws'
        
        self.logger.warning(f"No WebSocket endpoint found for {root_exchange} {market_type}")
        return None
    
    def _get_exchange_name(self, connector: Union[BaseExchangeConnector, str]) -> str:
        """Resolve a canonical exchange name.

        Accepts either a connector **instance** or a string that already represents
        the exchange.  The logic attempts, in order:

        1. Use the explicit `name` attribute exposed by `BaseExchangeConnector`
           (this is filled from config if provided).
           – If that name contains a variant suffix (e.g. "binance_spot") we keep it.
        2. If the attribute is unset / "unknown", fall back to the *class* name
           e.g. "BinanceConnector" → "binance".
        3. If the input parameter is already a string, return it lower-cased.
        """

        # Case-1: Called with a string (already determined elsewhere)
        if isinstance(connector, str):
            return connector.lower()

        # Case-2: Connector instance – try the configured name first
        if hasattr(connector, 'name') and connector.name and connector.name.lower() != 'unknown':
            return connector.name.lower()

        # Case-3: Derive from the class name as a fallback
        class_name = connector.__class__.__name__.lower()
        for key in ['binance', 'bybit', 'mexc', 'gateio', 'bitget', 'hyperliquid']:
            if key in class_name:
                return key

        # Unrecognised – return "unknown" so the caller can handle
        return 'unknown'
    
    def register_strategy_callback(self, strategy_id: str, callback: callable) -> None:
        """
        Register a callback function for a strategy to receive trade notifications.
        
        Args:
            strategy_id: Strategy instance ID
            callback: Async function to call when trades occur
        """
        self.strategy_callbacks[strategy_id] = callback
        self.logger.info(f"Registered trade callback for strategy {strategy_id}")
    
    def unregister_strategy_callback(self, strategy_id: str) -> None:
        """
        Unregister a strategy's trade callback.
        
        Args:
            strategy_id: Strategy instance ID
        """
        if strategy_id in self.strategy_callbacks:
            del self.strategy_callbacks[strategy_id]
            self.logger.info(f"Unregistered trade callback for strategy {strategy_id}")
    
    async def _notify_strategies_of_trade(self, trade_data: Dict[str, Any]) -> None:
        """
        Notify all subscribed strategies of a trade.
        
        Args:
            trade_data: Trade data dictionary
        """
        if not self.strategy_callbacks:
            self.logger.debug("No strategy callbacks registered")
            return
        
        # Notify all strategies with registered callbacks
        for strategy_id, callback in self.strategy_callbacks.items():
            try:
                self.logger.debug(f"📞 Calling strategy {strategy_id} with trade data")
                await callback(trade_data)
                self.logger.debug(f"✅ Successfully notified strategy {strategy_id}")
            except Exception as e:
                self.logger.error(f"Error notifying strategy {strategy_id} of trade: {e}")
                self.logger.error(f"Trade data: {trade_data}")


# Global connection pool instance
_connection_pool: Optional[WebSocketConnectionPool] = None


def get_connection_pool() -> WebSocketConnectionPool:
    """Get the global WebSocket connection pool instance."""
    global _connection_pool
    if _connection_pool is None:
        _connection_pool = WebSocketConnectionPool()
    return _connection_pool


async def initialize_connection_pool(redis_url="redis://localhost:6379") -> WebSocketConnectionPool:
    """Initialize and start the global WebSocket connection pool."""
    global _connection_pool
    if _connection_pool is None:
        _connection_pool = WebSocketConnectionPool(redis_url=redis_url)
        
    if not _connection_pool._running:
        await _connection_pool.start()
    return _connection_pool


async def shutdown_connection_pool():
    """Shutdown the global WebSocket connection pool."""
    global _connection_pool
    if _connection_pool and _connection_pool._running:
        await _connection_pool.stop()
        _connection_pool = None 
