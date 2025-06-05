"""
Gate.io exchange connector implementation.

Spot trading exchange with high rate limits and good API support.
"""

import asyncio
import ccxt.pro as ccxt
from typing import Dict, List, Optional, Any
from decimal import Decimal
from datetime import datetime
import logging
import decimal

from ..base_connector import BaseExchangeConnector, OrderSide, OrderType, OrderStatus

logger = logging.getLogger(__name__)


class GateIOConnector(BaseExchangeConnector):
    """
    Gate.io exchange connector for spot trading.
    
    Features:
    - Spot trading only
    - High rate limits (900 requests/second)
    - Good API stability
    - WebSocket support for real-time data
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Gate.io connector.
        
        Args:
            config: Configuration dictionary with:
                - api_key: Gate.io API key
                - secret: Gate.io secret key
                - sandbox: Use testnet (default: False)
        """
        super().__init__(config)
        
        self.market_type = 'spot'  # Only spot trading available
        self.testnet = config.get('sandbox', False)
        
        # Initialize ccxt exchange
        self._init_exchange()
        
        # Gate.io-specific rate limits
        self.rate_limits = {
            'default': 900,  # 900 requests per second
            'order': 100,    # 100 order requests per second
            'query': 900     # 900 query requests per second
        }
    
    def _init_exchange(self):
        """Initialize ccxt exchange instance."""
        try:
            self.exchange = ccxt.gateio({
                'apiKey': self.api_key,
                'secret': self.secret,
                'sandbox': self.testnet,
                'enableRateLimit': True,
                'rateLimit': 10,  # 10ms between requests (very fast)
                'options': {
                    'defaultType': 'spot',
                }
            })
            
            self.logger.info("Initialized Gate.io spot exchange")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Gate.io exchange: {e}")
            raise
    
    async def connect(self) -> bool:
        """Connect to Gate.io and load markets."""
        try:
            # Load markets
            await self.exchange.load_markets()
            
            # Test connectivity
            server_time = await self.exchange.fetch_time()
            self.last_heartbeat = datetime.now()
            
            self.connected = True
            self.logger.info(f"Connected to Gate.io spot (server_time: {server_time})")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to connect to Gate.io: {e}")
            self.connected = False
            return False
    
    async def disconnect(self) -> bool:
        """Disconnect from Gate.io."""
        try:
            if self.exchange:
                await self.exchange.close()
            
            self.connected = False
            self.logger.info("Disconnected from Gate.io")
            return True
            
        except Exception as e:
            self.logger.error(f"Error disconnecting from Gate.io: {e}")
            return False
    
    async def get_balance(self, currency: Optional[str] = None) -> Dict[str, Decimal]:
        """Get account balance."""
        try:
            if not await self._check_rate_limit('balance'):
                raise Exception("Rate limit exceeded for balance check")
            
            balance = await self.exchange.fetch_balance()
            
            if currency:
                # Return specific currency balance
                if currency in balance:
                    return {
                        currency: Decimal(str(balance[currency].get('free', 0)))
                    }
                else:
                    return {currency: Decimal('0')}
            else:
                # Return all non-zero balances
                result = {}
                for curr, info in balance.items():
                    if isinstance(info, dict) and 'free' in info:
                        free_amount = Decimal(str(info['free']))
                        if free_amount > 0:
                            result[curr] = free_amount
                
                return result
                
        except Exception as e:
            self._handle_error(e, "get_balance")
            return {}
    
    async def get_orderbook(self, symbol: str, limit: int = 100) -> Dict[str, Any]:
        """Get orderbook for symbol."""
        try:
            if not await self._check_rate_limit('orderbook'):
                raise Exception("Rate limit exceeded for orderbook")
            
            # Normalize symbol for Gate.io
            gateio_symbol = self.normalize_symbol(symbol)
            
            orderbook = await self.exchange.fetch_order_book(gateio_symbol, limit)
            
            return {
                'symbol': symbol,
                'exchange': 'gateio',
                'timestamp': orderbook.get('timestamp'),
                'datetime': orderbook.get('datetime'),
                'bids': [[Decimal(str(price)), Decimal(str(amount))] 
                        for price, amount in orderbook['bids']],
                'asks': [[Decimal(str(price)), Decimal(str(amount))] 
                        for price, amount in orderbook['asks']]
            }
            
        except Exception as e:
            self._handle_error(e, f"get_orderbook({symbol})")
            return {}
    
    async def place_order(
        self,
        symbol: str,
        side: str,
        amount: Decimal,
        price: Optional[Decimal] = None,
        order_type: str = OrderType.LIMIT,
        params: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Place order on Gate.io."""
        try:
            if not await self._check_rate_limit('order'):
                raise Exception("Rate limit exceeded for order placement")
            
            # Normalize inputs
            gateio_symbol = self.normalize_symbol(symbol)
            amount_float = float(amount)
            
            # Additional parameters
            order_params = params or {}
            
            # Validate order type
            if order_type not in ["market", "limit"]:
                raise ValueError(f"Unsupported order type: {order_type}")

            # Check required parameters for limit orders
            if order_type == "limit" and price is None:
                raise ValueError("Price required for limit orders")

            try:
                # Place order via ccxt
                if order_type == "market":
                    order = await self.exchange.create_market_order(
                        gateio_symbol, side, amount_float, None, params=order_params
                    )
                elif order_type == "limit":
                    price_float = float(price)
                    order = await self.exchange.create_limit_order(
                        gateio_symbol, side, amount_float, price_float, params=order_params
                    )
                else:
                    raise ValueError(f"Unsupported order type: {order_type}")
                
                # Normalize response
                return self._normalize_order(order)
                
            except Exception as e:
                self._handle_error(e, f"place_order({symbol}, {side}, {amount})")
                raise
            
        except Exception as e:
            self._handle_error(e, f"place_order({symbol}, {side}, {amount})")
            raise
    
    async def cancel_order(self, order_id: str, symbol: str) -> Dict[str, Any]:
        """Cancel order."""
        try:
            if not await self._check_rate_limit('cancel'):
                raise Exception("Rate limit exceeded for order cancellation")
            
            gateio_symbol = self.normalize_symbol(symbol)
            
            order = await self.exchange.cancel_order(order_id, gateio_symbol)
            return self._normalize_order(order)
            
        except Exception as e:
            self._handle_error(e, f"cancel_order({order_id}, {symbol})")
            raise
    
    async def get_order_status(self, order_id: str, symbol: str) -> Dict[str, Any]:
        """Get order status."""
        try:
            if not await self._check_rate_limit('query'):
                raise Exception("Rate limit exceeded for order status")
            
            gateio_symbol = self.normalize_symbol(symbol)
            
            order = await self.exchange.fetch_order(order_id, gateio_symbol)
            return self._normalize_order(order)
            
        except Exception as e:
            self._handle_error(e, f"get_order_status({order_id}, {symbol})")
            return {}
    
    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get open orders."""
        try:
            if not await self._check_rate_limit('query'):
                raise Exception("Rate limit exceeded for open orders")
            
            gateio_symbol = self.normalize_symbol(symbol) if symbol else None
            
            orders = await self.exchange.fetch_open_orders(gateio_symbol)
            return [self._normalize_order(order) for order in orders]
            
        except Exception as e:
            self._handle_error(e, f"get_open_orders({symbol})")
            return []
    
    async def get_trade_history(
        self,
        symbol: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get trade history."""
        try:
            if not await self._check_rate_limit('query'):
                raise Exception("Rate limit exceeded for trade history")
            
            # FIXED: Don't normalize symbol for trade history - use original format
            # Gate.io fetch_my_trades works with standard symbols (BTC/USDT)
            since_timestamp = int(since.timestamp() * 1000) if since else None
            
            # DEBUG: Add logging to see what's happening
            self.logger.info(f"DEBUG: Calling fetch_my_trades with symbol={symbol}, since={since_timestamp}, limit={limit}")
            
            trades = await self.exchange.fetch_my_trades(
                symbol, since_timestamp, limit
            )
            
            # DEBUG: Log result
            self.logger.info(f"DEBUG: fetch_my_trades returned {len(trades)} trades")
            
            return [self._normalize_trade(trade) for trade in trades]
            
        except Exception as e:
            # DEBUG: Log the actual error
            self.logger.error(f"DEBUG: get_trade_history error: {e}")
            self._handle_error(e, f"get_trade_history({symbol})")
            return []
    
    async def get_positions(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get positions (not applicable for spot trading)."""
        return []  # Gate.io only supports spot trading
    
    def normalize_symbol(self, symbol: str) -> str:
        """Convert standard symbol format to Gate.io format."""
        # Gate.io uses underscore format: BTC/USDT -> BTC_USDT
        return symbol.replace('/', '_')
    
    def denormalize_symbol(self, symbol: str) -> str:
        """Convert Gate.io symbol to standard format."""
        # BTC_USDT -> BTC/USDT
        return symbol.replace('_', '/')
    
    def _normalize_order(self, order: Dict) -> Dict[str, Any]:
        """Normalize ccxt order to standard format."""
        
        def safe_decimal(value, default=0):
            """Safely convert value to Decimal."""
            if value is None:
                return None if default is None else Decimal(str(default))
            try:
                return Decimal(str(value))
            except (ValueError, TypeError, decimal.InvalidOperation):
                return None if default is None else Decimal(str(default))
        
        return {
            'id': order.get('id'),
            'symbol': self.denormalize_symbol(order.get('symbol', '')),
            'side': order.get('side'),
            'amount': safe_decimal(order.get('amount'), 0),
            'price': safe_decimal(order.get('price'), None),
            'filled': safe_decimal(order.get('filled'), 0),
            'remaining': safe_decimal(order.get('remaining'), 0),
            'status': self._normalize_status(order.get('status')),
            'type': order.get('type'),
            'timestamp': order.get('timestamp'),
            'datetime': order.get('datetime'),
            'fee': order.get('fee'),
            'exchange': 'gateio',
            'raw': order
        }
    
    def _normalize_trade(self, trade: Dict) -> Dict[str, Any]:
        """Normalize ccxt trade to standard format."""
        
        def safe_decimal(value, default=0):
            """Safely convert value to Decimal."""
            if value is None:
                return None if default is None else Decimal(str(default))
            try:
                return Decimal(str(value))
            except (ValueError, TypeError, decimal.InvalidOperation):
                return None if default is None else Decimal(str(default))
        
        return {
            'id': trade.get('id'),
            'order_id': trade.get('order'),
            'symbol': self.denormalize_symbol(trade.get('symbol', '')),
            'side': trade.get('side'),
            'amount': safe_decimal(trade.get('amount'), 0),
            'price': safe_decimal(trade.get('price'), 0),
            'cost': safe_decimal(trade.get('cost'), 0),
            'fee': trade.get('fee'),
            'timestamp': trade.get('timestamp'),
            'datetime': trade.get('datetime'),
            'exchange': 'gateio',
            'raw': trade
        }
    
    def _normalize_status(self, status: str) -> str:
        """Normalize ccxt order status to standard format."""
        if not status:
            return OrderStatus.PENDING
            
        status_map = {
            'open': OrderStatus.OPEN,
            'closed': OrderStatus.FILLED,
            'canceled': OrderStatus.CANCELED,
            'cancelled': OrderStatus.CANCELED,
            'rejected': OrderStatus.REJECTED,
            'new': OrderStatus.OPEN,
            'partially_filled': OrderStatus.PARTIALLY_FILLED,
            'filled': OrderStatus.FILLED
        }
        return status_map.get(status.lower(), status)
