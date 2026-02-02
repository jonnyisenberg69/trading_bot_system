"""
WebSocket module for exchange connections.

Provides WebSocket managers and connection pooling for real-time data.
"""

from .ws_manager import WebSocketManager, WSState, WSMessageType
from .connection_pool import (
    WebSocketConnectionPool,
    ConnectionKey,
    ConnectionStatus,
    ConnectionInfo,
    get_connection_pool,
    initialize_connection_pool,
    shutdown_connection_pool
)
from .reconnection import ReconnectionManager

__all__ = [
    'WebSocketManager',
    'WSState', 
    'WSMessageType',
    'WebSocketConnectionPool',
    'ConnectionKey',
    'ConnectionStatus',
    'ConnectionInfo',
    'get_connection_pool',
    'initialize_connection_pool',
    'shutdown_connection_pool',
    'ReconnectionManager'
]
