"""
Exchange connectors for cryptocurrency exchanges.

This module provides standardized connectors for various cryptocurrency exchanges,
implementing both trading and market data functionality.

Categories:
- Trading Connectors: For order execution and portfolio management
- WebSocket Connectors: For real-time market data streaming

Supported Exchanges:
- Binance: Global leading exchange with extensive market coverage
- Bybit: Derivatives-focused exchange with spot and futures
- Hyperliquid: Perpetual DEX on Ethereum L2
- MEXC: Global exchange with diverse asset listings
- Gate.io: Comprehensive trading platform
- Bitget: Copy trading and derivatives platform
- Bitfinex: Spot trading and public data
"""

from typing import Dict, Type, Union
import structlog

from .binance import BinanceConnector
from .bybit import BybitConnector  
from .hyperliquid import HyperliquidConnector
from .mexc import MexcConnector
from .gateio import GateIOConnector
from .bitget import BitgetConnector
from .bitfinex import BitfinexConnector
from .kucoin import KucoinConnector

# WebSocket market data connectors
from .binance_ws_connector import BinanceWSConnector
from .bybit_ws_connector import BybitWSConnector
from .hyperliquid_ws_connector import HyperliquidWSConnector

logger = structlog.get_logger(__name__)

# Trading connectors registry
TRADING_CONNECTORS: Dict[str, Type] = {
    'binance': BinanceConnector,
    'bybit': BybitConnector,
    'hyperliquid': HyperliquidConnector,
    'mexc': MexcConnector,
    'gateio': GateIOConnector,
    'bitget': BitgetConnector,
    'bitfinex': BitfinexConnector,
    'kucoin': KucoinConnector,
    # Aliases with market suffixes used across the codebase/strategy configs
    'hyperliquid_spot': HyperliquidConnector,
    'bitfinex_spot': BitfinexConnector,
}

# WebSocket connectors registry  
WEBSOCKET_CONNECTORS: Dict[str, Type] = {
    'binance': BinanceWSConnector,
    'bybit': BybitWSConnector,
    'hyperliquid': HyperliquidWSConnector,
}

# Combined connector registry
ALL_CONNECTORS = {
    **TRADING_CONNECTORS,
    **{f"{k}_ws": v for k, v in WEBSOCKET_CONNECTORS.items()}
}


def create_exchange_connector(exchange_name: str, config: Dict = None, websocket: bool = False) -> Union[object, None]:
    """
    Create an exchange connector instance.
    
    Args:
        exchange_name: Name of the exchange ('binance', 'bybit', etc.)
        config: Configuration dictionary for the connector
        websocket: If True, create WebSocket connector for market data
        
    Returns:
        Exchange connector instance or None if not found
        
    Example:
        # Create trading connector
        binance = create_exchange_connector('binance', {'api_key': 'xxx', 'secret': 'yyy'})
        
        # Create WebSocket connector
        binance_ws = create_exchange_connector('binance', websocket=True)
    """
    config = config or {}
    
    try:
        if websocket:
            if exchange_name in WEBSOCKET_CONNECTORS:
                connector_class = WEBSOCKET_CONNECTORS[exchange_name]
                return connector_class()
            else:
                logger.warning(f"WebSocket connector not available for {exchange_name}")
                return None
        else:
            if exchange_name in TRADING_CONNECTORS:
                connector_class = TRADING_CONNECTORS[exchange_name]
                return connector_class(config)
            else:
                logger.warning(f"Trading connector not available for {exchange_name}")
                return None
                
    except Exception as e:
        logger.error(f"Failed to create {exchange_name} connector: {e}")
        return None


def list_supported_exchanges(include_websocket: bool = True) -> Dict[str, Dict[str, bool]]:
    """
    List all supported exchanges and their capabilities.
    
    Args:
        include_websocket: Include WebSocket connector availability
        
    Returns:
        Dictionary with exchange capabilities
        
    Example:
        {
            'binance': {'trading': True, 'websocket': True},
            'bybit': {'trading': True, 'websocket': True},
            'hyperliquid': {'trading': True, 'websocket': True},
            'mexc': {'trading': True, 'websocket': False},
            'gateio': {'trading': True, 'websocket': False},
            'bitget': {'trading': True, 'websocket': False}
        }
    """
    exchanges = {}
    
    for exchange in TRADING_CONNECTORS.keys():
        exchanges[exchange] = {
            'trading': True,
            'websocket': exchange in WEBSOCKET_CONNECTORS if include_websocket else False
        }
    
    return exchanges


def get_exchange_info(exchange_name: str) -> Dict[str, Union[str, bool, list]]:
    """
    Get detailed information about an exchange.
    
    Args:
        exchange_name: Name of the exchange
        
    Returns:
        Dictionary with exchange information
    """
    exchange_details = {
        'binance': {
            'name': 'Binance',
            'description': 'Global leading cryptocurrency exchange',
            'trading': True,
            'websocket': True,
            'markets': ['spot', 'futures', 'options'],
            'features': ['high_liquidity', 'low_fees', 'advanced_trading']
        },
        'bybit': {
            'name': 'Bybit',
            'description': 'Derivatives-focused exchange with spot and futures',
            'trading': True,
            'websocket': True,
            'markets': ['spot', 'futures', 'perpetual'],
            'features': ['perpetual_trading', 'copy_trading', 'derivatives']
        },
        'hyperliquid': {
            'name': 'Hyperliquid',
            'description': 'On-chain trading platform for spot and perpetuals on Ethereum L2',
            'trading': True,
            'websocket': True,
            'markets': ['spot', 'perpetual'],
            'features': ['dex', 'l2', 'decentralized', 'perpetual_trading', 'on_chain_spot']
        },
        'mexc': {
            'name': 'MEXC',
            'description': 'Global exchange with diverse asset listings',
            'trading': True,
            'websocket': False,
            'markets': ['spot', 'futures'],
            'features': ['diverse_assets', 'new_listings', 'global_access']
        },
        'gateio': {
            'name': 'Gate.io',
            'description': 'Comprehensive trading platform',
            'trading': True,
            'websocket': False,
            'markets': ['spot', 'futures', 'options'],
            'features': ['comprehensive', 'advanced_features', 'defi_support']
        },
        'bitget': {
            'name': 'Bitget',
            'description': 'Copy trading and derivatives platform',
            'trading': True,
            'websocket': False,
            'markets': ['spot', 'futures'],
            'features': ['copy_trading', 'derivatives', 'social_trading']
        },
        'kucoin': {
            'name': 'KuCoin',
            'description': 'Global exchange with extensive altcoin listings',
            'trading': True,
            'websocket': False,
            'markets': ['spot', 'futures'],
            'features': ['altcoins', 'kcs_discounts', 'trading_bot']
        }
    }
    
    return exchange_details.get(exchange_name, {
        'name': exchange_name,
        'description': 'Unknown exchange',
        'trading': exchange_name in TRADING_CONNECTORS,
        'websocket': exchange_name in WEBSOCKET_CONNECTORS,
        'markets': [],
        'features': []
    })


# Export all public symbols
__all__ = [
    # Trading connectors
    'BinanceConnector',
    'BybitConnector', 
    'HyperliquidConnector',
    'MexcConnector',
    'GateIOConnector',
    'BitgetConnector',
    'BitfinexConnector',
    'KucoinConnector',
    
    # WebSocket connectors
    'BinanceWSConnector',
    'BybitWSConnector',
    'HyperliquidWSConnector',
    
    # Registry dictionaries
    'TRADING_CONNECTORS',
    'WEBSOCKET_CONNECTORS',
    'ALL_CONNECTORS',
    
    # Factory functions
    'create_exchange_connector',
    'list_supported_exchanges',
    'get_exchange_info'
]
