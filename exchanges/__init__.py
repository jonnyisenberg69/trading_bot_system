"""
Exchange module for trading bot system.

This module provides a unified interface for connecting to multiple cryptocurrency exchanges
through the base_connector abstraction and exchange-specific implementations.
"""

from .base_connector import BaseExchangeConnector
from .rate_limiter import RateLimiter
from .connectors import (
    BinanceConnector,
    HyperliquidConnector, 
    BybitConnector,
    MexcConnector,
    GateIOConnector,
    BitgetConnector
)
from .websocket import WebSocketManager

__all__ = [
    'BaseExchangeConnector',
    'RateLimiter',
    'BinanceConnector',
    'HyperliquidConnector',
    'BybitConnector', 
    'MexcConnector',
    'GateIOConnector',
    'BitgetConnector',
    'WebSocketManager'
]

# Exchange registry for dynamic loading
EXCHANGE_REGISTRY = {
    'binance': BinanceConnector,
    'hyperliquid': HyperliquidConnector,
    'bybit': BybitConnector,
    'mexc': MexcConnector,
    'gateio': GateIOConnector,
    'bitget': BitgetConnector
}

def get_exchange_connector(exchange_name: str, config: dict):
    """
    Factory function to create exchange connector instances.
    
    Args:
        exchange_name: Name of the exchange ('binance', 'bybit', etc.)
        config: Exchange configuration dictionary
        
    Returns:
        Exchange connector instance
        
    Raises:
        ValueError: If exchange not supported
    """
    if exchange_name not in EXCHANGE_REGISTRY:
        raise ValueError(f"Exchange '{exchange_name}' not supported. "
                        f"Available: {list(EXCHANGE_REGISTRY.keys())}")
    
    connector_class = EXCHANGE_REGISTRY[exchange_name]
    return connector_class(config)
