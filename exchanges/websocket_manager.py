"""
WebSocket connection manager for exchanges.

Provides robust WebSocket connection management with:
- Automatic reconnection with exponential backoff
- Heartbeat monitoring
- Error handling and logging
- Connection state tracking
"""

import asyncio
import time
import json
import random
from typing import Dict, List, Optional, Any, Callable, Awaitable
from datetime import datetime, timedelta, timezone
import structlog
from enum import Enum


logger = structlog.get_logger(__name__)


class ConnectionState(Enum):
    """WebSocket connection states."""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    CLOSING = "closing"
    CLOSED = "closed"


class WebSocketManager:
    """
    WebSocket connection manager for exchanges.
    
    Features:
    - Automatic reconnection with exponential backoff
    - Connection health monitoring with heartbeats
    - Detailed logging and metrics
    - Support for multiple subscriptions
    """
    
    def __init__(
        self,
        exchange_name: str,
        connection_id: str = None,
        max_reconnect_attempts: int = 10,
        initial_backoff: float = 1.0,
        max_backoff: float = 60.0,
        heartbeat_interval: float = 30.0,
        heartbeat_timeout: float = 10.0
    ):
        """
        Initialize WebSocket manager.
        
        Args:
            exchange_name: Name of the exchange
            connection_id: Identifier for this connection (default: random ID)
            max_reconnect_attempts: Maximum number of reconnection attempts
            initial_backoff: Initial backoff time in seconds
            max_backoff: Maximum backoff time in seconds
            heartbeat_interval: Interval between heartbeats in seconds
            heartbeat_timeout: Timeout for heartbeat responses in seconds
        """
        self.exchange_name = exchange_name
        self.connection_id = connection_id or f"{exchange_name}_{random.randint(1000, 9999)}"
        
        # Reconnection settings
        self.max_reconnect_attempts = max_reconnect_attempts
        self.initial_backoff = initial_backoff
        self.max_backoff = max_backoff
        self.reconnect_attempts = 0
        self.current_backoff = initial_backoff
        
        # Heartbeat settings
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_timeout = heartbeat_timeout
        self.last_heartbeat_sent = None
        self.last_heartbeat_received = None
        self.last_message_received = None
        
        # Connection state
        self.state = ConnectionState.DISCONNECTED
        self.is_running = False
        self.last_connection_time = None
        self.last_disconnection_time = None
        self.disconnect_reason = None
        
        # Tasks
        self.connection_task = None
        self.heartbeat_task = None
        self.message_handler_task = None
        
        # Callbacks
        self.on_connect_callback = None
        self.on_message_callback = None
        self.on_disconnect_callback = None
        self.on_error_callback = None
        self.on_reconnect_callback = None
        
        # Subscription info
        self.subscriptions = []
        
        # Performance metrics
        self.connection_attempts = 0
        self.successful_connections = 0
        self.message_count = 0
        self.error_count = 0
        
        self.logger = logger.bind(
            exchange=exchange_name,
            connection_id=self.connection_id
        )
    
    def set_callbacks(
        self,
        on_connect: Optional[Callable[[], Awaitable[None]]] = None,
        on_message: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None,
        on_disconnect: Optional[Callable[[str], Awaitable[None]]] = None,
        on_error: Optional[Callable[[Exception], Awaitable[None]]] = None,
        on_reconnect: Optional[Callable[[], Awaitable[None]]] = None
    ) -> None:
        """
        Set callbacks for WebSocket events.
        
        Args:
            on_connect: Called when connection is established
            on_message: Called when message is received
            on_disconnect: Called when connection is closed
            on_error: Called when error occurs
            on_reconnect: Called before attempting to reconnect
        """
        self.on_connect_callback = on_connect
        self.on_message_callback = on_message
        self.on_disconnect_callback = on_disconnect
        self.on_error_callback = on_error
        self.on_reconnect_callback = on_reconnect
    
    async def connect(
        self,
        connect_function: Callable[[], Awaitable[Any]],
        message_handler: Callable[[Any], Awaitable[None]],
        heartbeat_function: Optional[Callable[[], Awaitable[None]]] = None,
        subscriptions: Optional[List[Dict[str, Any]]] = None
    ) -> None:
        """
        Connect to WebSocket and start processing messages.
        
        Args:
            connect_function: Function to establish connection
            message_handler: Function to handle incoming messages
            heartbeat_function: Function to send heartbeats (optional)
            subscriptions: List of subscription data (optional)
        """
        if self.is_running:
            self.logger.warning("WebSocket manager already running")
            return
            
        self.is_running = True
        self.state = ConnectionState.CONNECTING
        self.subscriptions = subscriptions or []
        self.disconnect_reason = None
        
        # Start connection task
        self.connection_task = asyncio.create_task(
            self._connection_loop(connect_function, message_handler, heartbeat_function)
        )
        
        self.logger.info("WebSocket manager started")
    
    async def disconnect(self, reason: str = "User requested") -> None:
        """
        Disconnect from WebSocket.
        
        Args:
            reason: Reason for disconnection
        """
        if not self.is_running:
            return
            
        self.is_running = False
        self.state = ConnectionState.CLOSING
        self.disconnect_reason = reason
        
        self.logger.info("Disconnecting WebSocket", reason=reason)
        
        # Cancel all tasks
        for task in [self.connection_task, self.heartbeat_task, self.message_handler_task]:
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                    
        self.state = ConnectionState.CLOSED
        self.last_disconnection_time = datetime.now(timezone.utc)
        
        if self.on_disconnect_callback:
            try:
                await self.on_disconnect_callback(reason)
            except Exception as e:
                self.logger.error("Error in disconnect callback", error=str(e))
                
        self.logger.info("WebSocket disconnected", reason=reason)
    
    async def _connection_loop(
        self, 
        connect_function: Callable[[], Awaitable[Any]],
        message_handler: Callable[[Any], Awaitable[None]],
        heartbeat_function: Optional[Callable[[], Awaitable[None]]] = None
    ) -> None:
        """
        Main connection loop with reconnection logic.
        
        Args:
            connect_function: Function to establish connection
            message_handler: Function to handle incoming messages
            heartbeat_function: Function to send heartbeats
        """
        while self.is_running:
            try:
                self.connection_attempts += 1
                self.state = ConnectionState.CONNECTING
                
                self.logger.info("Connecting to WebSocket", 
                                attempt=self.reconnect_attempts + 1,
                                max_attempts=self.max_reconnect_attempts)
                
                # Establish connection
                ws_connection = await connect_function()
                
                # Connection successful
                self.successful_connections += 1
                self.reconnect_attempts = 0
                self.current_backoff = self.initial_backoff
                self.last_connection_time = datetime.now(timezone.utc)
                self.state = ConnectionState.CONNECTED
                
                self.logger.info("WebSocket connected")
                
                # Start heartbeat task if function provided
                if heartbeat_function:
                    self.heartbeat_task = asyncio.create_task(
                        self._heartbeat_loop(heartbeat_function)
                    )
                
                # Start message handler
                self.message_handler_task = asyncio.create_task(
                    self._handle_messages(ws_connection, message_handler)
                )
                
                # Notify connection callback
                if self.on_connect_callback:
                    try:
                        await self.on_connect_callback()
                    except Exception as e:
                        self.logger.error("Error in connect callback", error=str(e))
                
                # Wait for message handler to complete
                await self.message_handler_task
                
                # If we get here, the connection was closed
                self.logger.info("WebSocket connection closed")
                
            except asyncio.CancelledError:
                # Normal cancellation, exit loop
                break
                
            except Exception as e:
                self.error_count += 1
                self.logger.error("WebSocket connection error", error=str(e))
                
                # Notify error callback
                if self.on_error_callback:
                    try:
                        await self.on_error_callback(e)
                    except Exception as e2:
                        self.logger.error("Error in error callback", error=str(e2))
            
            # If not running, exit loop
            if not self.is_running:
                break
                
            # Check if max reconnect attempts reached
            if self.reconnect_attempts >= self.max_reconnect_attempts:
                self.logger.error("Maximum reconnection attempts reached, giving up",
                                max_attempts=self.max_reconnect_attempts)
                self.disconnect_reason = f"Max reconnection attempts ({self.max_reconnect_attempts}) reached"
                self.is_running = False
                break
                
            # Prepare for reconnection
            self.reconnect_attempts += 1
            self.state = ConnectionState.RECONNECTING
            
            # Calculate backoff with jitter
            jitter = random.uniform(0.8, 1.2)
            backoff = min(self.current_backoff * jitter, self.max_backoff)
            
            self.logger.info("Reconnecting after backoff", 
                            backoff=backoff,
                            attempt=self.reconnect_attempts,
                            max_attempts=self.max_reconnect_attempts)
            
            # Notify reconnect callback
            if self.on_reconnect_callback:
                try:
                    await self.on_reconnect_callback()
                except Exception as e:
                    self.logger.error("Error in reconnect callback", error=str(e))
                    
            # Wait before reconnecting
            await asyncio.sleep(backoff)
            
            # Increase backoff for next attempt
            self.current_backoff = min(self.current_backoff * 2, self.max_backoff)
    
    async def _heartbeat_loop(self, heartbeat_function: Callable[[], Awaitable[None]]) -> None:
        """
        Send periodic heartbeats to keep connection alive.
        
        Args:
            heartbeat_function: Function to send heartbeat
        """
        try:
            while self.is_running and self.state == ConnectionState.CONNECTED:
                # Send heartbeat
                self.last_heartbeat_sent = datetime.now(timezone.utc)
                
                try:
                    await heartbeat_function()
                except Exception as e:
                    self.logger.warning("Error sending heartbeat", error=str(e))
                    
                    # Check if connection is still alive
                    if self.last_message_received:
                        time_since_last = (datetime.now(timezone.utc) - self.last_message_received).total_seconds()
                        if time_since_last > self.heartbeat_timeout * 2:
                            self.logger.warning("No messages received recently, connection may be dead",
                                              seconds_since_last=time_since_last)
                            break
                
                # Wait for next heartbeat
                await asyncio.sleep(self.heartbeat_interval)
                
                # Check if heartbeat response received
                if self.last_heartbeat_sent and self.last_heartbeat_received:
                    if self.last_heartbeat_received < self.last_heartbeat_sent:
                        # No response to last heartbeat
                        seconds_waiting = (datetime.now(timezone.utc) - self.last_heartbeat_sent).total_seconds()
                        if seconds_waiting > self.heartbeat_timeout:
                            self.logger.warning("Heartbeat timeout, reconnecting",
                                              seconds_waiting=seconds_waiting,
                                              timeout=self.heartbeat_timeout)
                            break
                            
        except asyncio.CancelledError:
            # Normal cancellation
            pass
            
        except Exception as e:
            self.logger.error("Error in heartbeat loop", error=str(e))
            
        finally:
            # If we exit the heartbeat loop due to error, trigger reconnect
            if self.is_running and self.state == ConnectionState.CONNECTED:
                # Cancel message handler to trigger reconnect
                if self.message_handler_task and not self.message_handler_task.done():
                    self.message_handler_task.cancel()
    
    async def _handle_messages(
        self, 
        ws_connection: Any,
        message_handler: Callable[[Any], Awaitable[None]]
    ) -> None:
        """
        Handle incoming WebSocket messages.
        
        Args:
            ws_connection: WebSocket connection object
            message_handler: Function to handle messages
        """
        try:
            async for message in ws_connection:
                if not self.is_running:
                    break
                    
                self.last_message_received = datetime.now(timezone.utc)
                self.message_count += 1
                
                # Check if message is a heartbeat response
                is_heartbeat = self._is_heartbeat_message(message)
                if is_heartbeat:
                    self.last_heartbeat_received = datetime.now(timezone.utc)
                    continue
                
                # Process message
                try:
                    await message_handler(message)
                    
                    # Call user message callback
                    if self.on_message_callback:
                        await self.on_message_callback(message)
                        
                except Exception as e:
                    self.logger.error("Error processing message", error=str(e), message=str(message)[:200])
                    
        except asyncio.CancelledError:
            # Normal cancellation
            raise
            
        except Exception as e:
            self.logger.error("WebSocket message handling error", error=str(e))
            raise
    
    def _is_heartbeat_message(self, message: Any) -> bool:
        """
        Check if message is a heartbeat response.
        
        This is a basic implementation - exchanges may have different formats.
        Override this in exchange-specific implementations.
        
        Args:
            message: WebSocket message
            
        Returns:
            True if message is a heartbeat response
        """
        if isinstance(message, dict):
            return 'ping' in message or 'pong' in message or message.get('type') in ['ping', 'pong', 'heartbeat']
            
        if isinstance(message, str):
            try:
                data = json.loads(message)
                return 'ping' in data or 'pong' in data or data.get('type') in ['ping', 'pong', 'heartbeat']
            except:
                return 'ping' in message or 'pong' in message
                
        return False
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get current connection status.
        
        Returns:
            Dictionary with connection status information
        """
        uptime = None
        if self.last_connection_time and self.state == ConnectionState.CONNECTED:
            uptime = (datetime.now(timezone.utc) - self.last_connection_time).total_seconds()
            
        return {
            'exchange': self.exchange_name,
            'connection_id': self.connection_id,
            'state': self.state.value,
            'uptime': uptime,
            'reconnect_attempts': self.reconnect_attempts,
            'last_message_time': self.last_message_received.isoformat() if self.last_message_received else None,
            'last_heartbeat_time': self.last_heartbeat_received.isoformat() if self.last_heartbeat_received else None,
            'message_count': self.message_count,
            'error_count': self.error_count,
            'connection_attempts': self.connection_attempts,
            'successful_connections': self.successful_connections
        } 