"""
Base exchange connector providing unified interface for all exchanges.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Union, Any, AsyncGenerator
from decimal import Decimal
from datetime import datetime, timezone
import asyncio
import logging
import time
from enum import Enum, auto
import structlog

logger = logging.getLogger(__name__)


class OrderType(Enum):
    """Order types."""
    MARKET = "market"
    LIMIT = "limit"
    STOP_LOSS = "stop_loss"
    TAKE_PROFIT = "take_profit"
    STOP_LIMIT = "stop_limit"
    TAKE_PROFIT_LIMIT = "take_profit_limit"
    TRAILING_STOP = "trailing_stop"


class OrderSide(Enum):
    """Order sides."""
    BUY = "buy"
    SELL = "sell"


class OrderStatus(Enum):
    """Order statuses."""
    PENDING = "pending"
    OPEN = "open"
    CLOSED = "closed"
    CANCELED = "canceled"
    EXPIRED = "expired"
    REJECTED = "rejected"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"


class BaseExchangeConnector(ABC):
    """
    Abstract base class for all exchange connectors.
    
    Provides unified interface for trading operations across different exchanges.
    All exchange-specific connectors must inherit from this class.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize exchange connector.
        
        Args:
            config: Exchange configuration including API credentials and settings
        """
        self.config = config
        self.name = config.get('name', 'unknown')
        self.sandbox = config.get('sandbox', False)
        self.api_key = config.get('api_key')
        self.secret = config.get('secret')
        self.passphrase = config.get('passphrase')  # For some exchanges
        
        self.exchange = None  # Will be set by subclasses
        self.rate_limiter = None  # Will be injected
        self.websocket_manager = None  # Will be injected
        
        # Connection state
        self.connected = False
        self.last_heartbeat = None
        
        # Rate limiting
        self.request_timestamps = {}
        self.last_request_time = 0
        
        self.logger = structlog.get_logger(__name__).bind(
            exchange=self.__class__.__name__
        )
    
    @abstractmethod
    async def connect(self) -> bool:
        """
        Connect to exchange and initialize.
        
        Returns:
            True if connection successful, False otherwise
        """
        pass
    
    @abstractmethod
    async def disconnect(self) -> bool:
        """
        Disconnect from exchange and cleanup.
        
        Returns:
            True if disconnection successful, False otherwise
        """
        pass
    
    @abstractmethod
    async def get_balance(self, currency: Optional[str] = None) -> Dict[str, Decimal]:
        """
        Get account balance.
        
        Args:
            currency: Specific currency to get balance for, None for all
            
        Returns:
            Dictionary of currency -> balance
        """
        pass
    
    @abstractmethod
    async def get_orderbook(self, symbol: str, limit: int = 100) -> Dict[str, Any]:
        """
        Get current orderbook for symbol.
        
        Args:
            symbol: Trading symbol (e.g., 'BTC/USDT')
            limit: Number of price levels to return
            
        Returns:
            Orderbook dictionary with bids/asks
        """
        pass
    
    @abstractmethod
    async def place_order(
        self,
        symbol: str,
        side: str,
        amount: Decimal,
        price: Optional[Decimal] = None,
        order_type: str = OrderType.LIMIT,
        params: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Place trading order.
        
        Args:
            symbol: Trading symbol
            side: Order side (buy/sell)
            amount: Order amount
            price: Order price (None for market orders)
            order_type: Type of order
            params: Additional exchange-specific parameters
            
        Returns:
            Order information dictionary
        """
        pass
    
    @abstractmethod
    async def cancel_order(self, order_id: str, symbol: str) -> Dict[str, Any]:
        """
        Cancel existing order.
        
        Args:
            order_id: Exchange order ID
            symbol: Trading symbol
            
        Returns:
            Cancellation result
        """
        pass
    
    @abstractmethod
    async def get_order_status(self, order_id: str, symbol: str) -> Dict[str, Any]:
        """
        Get order status.
        
        Args:
            order_id: Exchange order ID
            symbol: Trading symbol
            
        Returns:
            Order status information
        """
        pass
    
    @abstractmethod
    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get all open orders.
        
        Args:
            symbol: Filter by symbol, None for all symbols
            
        Returns:
            List of open orders
        """
        pass
    
    @abstractmethod
    async def get_trade_history(
        self, 
        symbol: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get trade history.
        
        Args:
            symbol: Filter by symbol, None for all symbols
            since: Get trades since this timestamp
            limit: Maximum number of trades to return
            
        Returns:
            List of trades
        """
        pass
    
    @abstractmethod
    async def get_positions(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get open positions (for futures exchanges).
        
        Args:
            symbol: Filter by symbol, None for all symbols
            
        Returns:
            List of positions
        """
        pass
    
    async def get_exchange_info(self) -> Dict[str, Any]:
        """
        Get exchange information (symbols, trading rules, etc.).
        
        Returns:
            Exchange information dictionary
        """
        try:
            if hasattr(self.exchange, 'load_markets'):
                await self.exchange.load_markets()
                return {
                    'symbols': list(self.exchange.markets.keys()),
                    'markets': self.exchange.markets,
                    'currencies': self.exchange.currencies,
                    'limits': getattr(self.exchange, 'limits', {}),
                    'fees': getattr(self.exchange, 'fees', {})
                }
            return {}
        except Exception as e:
            self.logger.error(f"Error getting exchange info: {e}")
            return {}
    
    async def health_check(self) -> Dict[str, Any]:
        """
        Check exchange connectivity and health.
        
        Returns:
            Health status dictionary
        """
        try:
            # Simple ping or time check
            if hasattr(self.exchange, 'fetch_time'):
                server_time = await self.exchange.fetch_time()
                local_time = datetime.now(timezone.utc).timestamp() * 1000
                latency = abs(local_time - server_time)
                
                return {
                    'status': 'healthy',
                    'connected': self.connected,
                    'server_time': server_time,
                    'latency_ms': latency,
                    'last_heartbeat': self.last_heartbeat
                }
            else:
                return {
                    'status': 'unknown',
                    'connected': self.connected,
                    'last_heartbeat': self.last_heartbeat
                }
        except Exception as e:
            self.logger.error(f"Health check failed: {e}")
            return {
                'status': 'unhealthy',
                'connected': False,
                'error': str(e),
                'last_heartbeat': self.last_heartbeat
            }
    
    def normalize_symbol(self, symbol: str) -> str:
        """
        Normalize symbol format to exchange-specific format.
        
        Args:
            symbol: Symbol in standard format (e.g., 'BTC/USDT')
            
        Returns:
            Exchange-specific symbol format
        """
        # Default implementation - override in subclasses if needed
        return symbol
    
    def denormalize_symbol(self, symbol: str) -> str:
        """
        Convert exchange-specific symbol to standard format.
        
        Args:
            symbol: Exchange-specific symbol
            
        Returns:
            Standard symbol format (e.g., 'BTC/USDT')
        """
        # Default implementation - override in subclasses if needed
        return symbol
    
    async def _check_rate_limit(self, endpoint: str, weight: int = 1) -> bool:
        """
        Check if request is within rate limits.
        
        Args:
            endpoint: API endpoint name
            weight: Request weight (some exchanges use weighted limits)
            
        Returns:
            True if within limits, False if rate limited
        """
        if self.rate_limiter:
            return await self.rate_limiter.check_limit(self.name, endpoint, weight)
        return True
    
    def _handle_error(self, error: Exception, context: str = "") -> None:
        """
        Handle and log errors with context.
        
        Args:
            error: Exception that occurred
            context: Additional context for error
        """
        error_msg = f"{context}: {str(error)}" if context else str(error)
        self.logger.error(error_msg)
    
    def __str__(self) -> str:
        """String representation of connector."""
        return f"{self.__class__.__name__}({self.name})"
    
    def __repr__(self) -> str:
        """Detailed representation of connector."""
        return (f"{self.__class__.__name__}("
                f"name='{self.name}', "
                f"connected={self.connected}, "
                f"sandbox={self.sandbox})")
