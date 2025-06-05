"""
Binance exchange connector implementation.

Supports both spot and futures trading on Binance using ccxt.
Handles Binance-specific rate limiting and API quirks.
"""

import asyncio
import ccxt.pro as ccxt
from typing import Dict, List, Optional, Any
from decimal import Decimal
from datetime import datetime
import logging

from ..base_connector import BaseExchangeConnector, OrderSide, OrderType, OrderStatus

logger = logging.getLogger(__name__)


class BinanceConnector(BaseExchangeConnector):
    """
    Binance exchange connector supporting both spot and futures markets.
    
    Features:
    - Unified spot and futures API access
    - WebSocket support for real-time data
    - Binance-specific rate limiting (weight-based)
    - Order management with proper error handling
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Binance connector.
        
        Args:
            config: Configuration dictionary with:
                - api_key: Binance API key
                - secret: Binance secret key  
                - sandbox: Use testnet (default: False)
                - market_type: 'spot' or 'future' (default: 'spot')
                - rate_limit: Enable rate limiting (default: True)
        """
        super().__init__(config)
        
        self.market_type = config.get('market_type', 'spot')
        self.testnet = config.get('sandbox', False)
        
        # Initialize ccxt exchange
        self._init_exchange()
        
        # Binance-specific settings
        self.weight_limits = {
            'spot': 1200,  # 1200 weight per minute
            'future': 2400  # 2400 weight per minute for futures
        }
        
        self.order_rate_limits = {
            'spot': 10,    # 10 orders per second
            'future': 20   # 20 orders per second for futures
        }
    
    def _init_exchange(self):
        """Initialize ccxt exchange instance."""
        try:
            if self.market_type == 'future':
                # Use futures API
                self.exchange = ccxt.binanceusdm({
                    'apiKey': self.api_key,
                    'secret': self.secret,
                    'sandbox': self.testnet,
                    'enableRateLimit': True,
                    'rateLimit': 50,  # 50ms between requests
                    'options': {
                        'adjustForTimeDifference': True,
                        'recvWindow': 10000,
                    }
                })
            else:
                # Use spot API
                self.exchange = ccxt.binance({
                    'apiKey': self.api_key,
                    'secret': self.secret,
                    'sandbox': self.testnet,
                    'enableRateLimit': True,
                    'rateLimit': 50,  # 50ms between requests
                    'options': {
                        'adjustForTimeDifference': True,
                        'recvWindow': 10000,
                    }
                })
            
            self.logger.info(f"Initialized Binance {self.market_type} exchange")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Binance exchange: {e}")
            raise
    
    async def connect(self) -> bool:
        """Connect to Binance and load markets."""
        max_retries = 3
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                # Load markets
                await self.exchange.load_markets()
                
                # Test connectivity
                server_time = await self.exchange.fetch_time()
                self.last_heartbeat = datetime.now()
                
                self.connected = True
                self.logger.info(f"Connected to Binance {self.market_type} "
                               f"(server_time: {server_time})")
                
                return True
                
            except Exception as e:
                error_str = str(e).lower()
                
                # Check if it's a margin API error that we can retry
                if 'margin' in error_str or 'isolated' in error_str:
                    self.logger.warning(f"Binance {self.market_type} margin API error on attempt {attempt + 1}: {e}")
                    
                    if attempt < max_retries - 1:
                        self.logger.info(f"Retrying Binance {self.market_type} connection in {retry_delay} seconds...")
                        await asyncio.sleep(retry_delay)
                        continue
                else:
                    # For other errors, don't retry
                    self.logger.error(f"Failed to connect to Binance {self.market_type}: {e}")
                    self.connected = False
                    return False
        
        # If we get here, all retries failed
        self.logger.error(f"Failed to connect to Binance {self.market_type} after {max_retries} attempts")
        self.connected = False
        return False
    
    async def disconnect(self) -> bool:
        """Disconnect from Binance."""
        try:
            if self.exchange:
                await self.exchange.close()
            
            self.connected = False
            self.logger.info("Disconnected from Binance")
            return True
            
        except Exception as e:
            self.logger.error(f"Error disconnecting from Binance: {e}")
            return False
    
    async def get_balance(self, currency: Optional[str] = None) -> Dict[str, Decimal]:
        """Get account balance."""
        try:
            if not await self._check_rate_limit('balance', weight=10):
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
    
    async def get_orderbook(self, symbol: str, limit: int = 20) -> Optional[Dict[str, Any]]:
        """Get orderbook for symbol."""
        try:
            if not await self._check_rate_limit('query'):
                raise Exception("Rate limit exceeded for orderbook")
            
            binance_symbol = self.normalize_symbol(symbol)
            
            # FIXED: Use valid depth limits for different Binance APIs
            # Binance spot: 5, 10, 20, 50, 100, 500, 1000, 5000
            # Binance futures: 5, 10, 20, 50, 100, 500, 1000
            if limit <= 5:
                valid_limit = 5
            elif limit <= 10:
                valid_limit = 10
            elif limit <= 20:
                valid_limit = 20
            elif limit <= 50:
                valid_limit = 50
            elif limit <= 100:
                valid_limit = 100
            elif limit <= 500:
                valid_limit = 500
            else:
                valid_limit = 1000
            
            orderbook = await self.exchange.fetch_order_book(binance_symbol, valid_limit)
            
            # Trim to requested limit if needed
            if len(orderbook['bids']) > limit:
                orderbook['bids'] = orderbook['bids'][:limit]
            if len(orderbook['asks']) > limit:
                orderbook['asks'] = orderbook['asks'][:limit]
            
            self.logger.debug(f"Retrieved orderbook for {symbol} with {len(orderbook['bids'])} bids, {len(orderbook['asks'])} asks")
            return orderbook
            
        except Exception as e:
            self.logger.error(f"get_orderbook({symbol}): {e}")
            return None
    
    async def place_order(
        self,
        symbol: str,
        side: str,
        amount: Decimal,
        price: Optional[Decimal] = None,
        order_type: str = OrderType.LIMIT,
        params: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Place order on Binance."""
        try:
            if not await self._check_rate_limit('order', weight=1):
                raise Exception("Rate limit exceeded for order placement")
            
            # Normalize inputs
            binance_symbol = self.normalize_symbol(symbol)
            amount_float = float(amount)
            price_float = float(price) if price else None
            
            # Additional parameters
            order_params = params or {}
            
            # Validate order type
            if order_type not in ["market", "limit"]:
                raise ValueError(f"Unsupported order type: {order_type}")
            
            # Validate required parameters
            if order_type == "limit" and price_float is None:
                raise ValueError("Price required for limit orders")
            
            # Place order via ccxt
            if order_type == "market":
                order = await self.exchange.create_market_order(
                    binance_symbol, side, amount_float, price_float, params=order_params
                )
            elif order_type == "limit":
                order = await self.exchange.create_limit_order(
                    binance_symbol, side, amount_float, price_float, params=order_params
                )
            else:
                raise ValueError(f"Unsupported order type: {order_type}")
            
            # Normalize response
            return self._normalize_order(order)
            
        except Exception as e:
            self._handle_error(e, f"place_order({symbol}, {side}, {amount})")
            raise
    
    async def cancel_order(self, order_id: str, symbol: str) -> Dict[str, Any]:
        """Cancel order."""
        try:
            if not await self._check_rate_limit('cancel', weight=1):
                raise Exception("Rate limit exceeded for order cancellation")
            
            binance_symbol = self.normalize_symbol(symbol)
            
            order = await self.exchange.cancel_order(order_id, binance_symbol)
            return self._normalize_order(order)
            
        except Exception as e:
            self._handle_error(e, f"cancel_order({order_id}, {symbol})")
            raise
    
    async def get_order_status(self, order_id: str, symbol: str) -> Dict[str, Any]:
        """Get order status."""
        try:
            if not await self._check_rate_limit('query', weight=2):
                raise Exception("Rate limit exceeded for order status")
            
            binance_symbol = self.normalize_symbol(symbol)
            
            order = await self.exchange.fetch_order(order_id, binance_symbol)
            return self._normalize_order(order)
            
        except Exception as e:
            self._handle_error(e, f"get_order_status({order_id}, {symbol})")
            return {}
    
    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get open orders."""
        try:
            if not await self._check_rate_limit('query', weight=3):
                raise Exception("Rate limit exceeded for open orders")
            
            binance_symbol = self.normalize_symbol(symbol) if symbol else None
            
            orders = await self.exchange.fetch_open_orders(binance_symbol)
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
            if not await self._check_rate_limit('query', weight=10):
                raise Exception("Rate limit exceeded for trade history")
            
            binance_symbol = self.normalize_symbol(symbol) if symbol else None
            
            # Binance-specific time handling for futures API
            since_timestamp = None
            if since:
                since_timestamp = int(since.timestamp() * 1000)
                
                # For Binance futures, validate time constraints
                if self.market_type == 'future':
                    current_time = int(datetime.now().timestamp() * 1000)
                    # Ensure since is not too far in the past (max 7 days)
                    seven_days_ago = current_time - (7 * 24 * 60 * 60 * 1000)
                    if since_timestamp < seven_days_ago:
                        since_timestamp = seven_days_ago
                    
                    # Ensure since is not in the future
                    if since_timestamp > current_time:
                        since_timestamp = current_time - (60 * 60 * 1000)  # 1 hour ago
            
            # For futures, if no since provided, default to last 24 hours
            if self.market_type == 'future' and since_timestamp is None:
                current_time = int(datetime.now().timestamp() * 1000)
                since_timestamp = current_time - (24 * 60 * 60 * 1000)  # 24 hours ago
            
            trades = await self.exchange.fetch_my_trades(
                binance_symbol, since_timestamp, limit
            )
            
            return [self._normalize_trade(trade) for trade in trades]
            
        except Exception as e:
            self._handle_error(e, f"get_trade_history({symbol})")
            return []
    
    async def get_positions(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get positions (futures only)."""
        if self.market_type != 'future':
            return []
        
        try:
            if not await self._check_rate_limit('query', weight=5):
                raise Exception("Rate limit exceeded for positions")
            
            positions = await self.exchange.fetch_positions()
            
            result = []
            for position in positions:
                if symbol and position['symbol'] != self.normalize_symbol(symbol):
                    continue
                
                # Only include positions with non-zero size
                if abs(float(position.get('contracts', 0))) > 0:
                    result.append(self._normalize_position(position))
            
            return result
            
        except Exception as e:
            self._handle_error(e, f"get_positions({symbol})")
            return []
    
    def normalize_symbol(self, symbol: str) -> str:
        """Convert standard symbol format to Binance format."""
        if not symbol:
            return symbol
            
        # Handle perpetual futures format: BTC/USDT:USDT -> BTCUSDT
        if ':' in symbol:
            # Remove the settlement currency part for perpetuals
            symbol = symbol.split(':')[0]
        
        if self.market_type == 'future':
            # Futures: BTC/USDT -> BTCUSDT (no slash)
            return symbol.replace('/', '')
        else:
            # Spot: BTC/USDT stays as BTC/USDT (keep slash)
            return symbol
    
    def denormalize_symbol(self, symbol: str) -> str:
        """Convert Binance symbol to standard format."""
        if not symbol:
            return symbol
            
        if self.market_type == 'future':
            # Futures: BTCUSDT -> BTC/USDT
            if 'USDT' in symbol and '/' not in symbol:
                if symbol.endswith('USDT'):
                    base = symbol[:-4]  # Remove 'USDT'
                    return f"{base}/USDT"
            return symbol
        else:
            # Spot: Should already be in correct format, but convert if needed
            if 'USDT' in symbol and '/' not in symbol:
                if symbol.endswith('USDT'):
                    base = symbol[:-4]
                    return f"{base}/USDT"
            return symbol
    
    def _normalize_order(self, order: Dict) -> Dict[str, Any]:
        """Normalize ccxt order to standard format."""
        return {
            'id': order.get('id'),
            'symbol': order.get('symbol'),
            'side': order.get('side'),
            'amount': Decimal(str(order.get('amount', 0))),
            'price': Decimal(str(order.get('price', 0))) if order.get('price') else None,
            'filled': Decimal(str(order.get('filled', 0))),
            'remaining': Decimal(str(order.get('remaining', 0))),
            'status': self._normalize_status(order.get('status')),
            'type': order.get('type'),
            'timestamp': order.get('timestamp'),
            'datetime': order.get('datetime'),
            'fee': order.get('fee'),
            'exchange': 'binance',
            'raw': order
        }
    
    def _normalize_trade(self, trade: Dict) -> Dict[str, Any]:
        """Normalize ccxt trade to standard format."""
        # Get the symbol and denormalize it to standard format
        symbol = trade.get('symbol', '')
        
        # For Binance futures, convert BTCUSDT back to BTC/USDT format
        if self.market_type == 'future' and symbol and '/' not in symbol:
            # Common USDT pairs - convert BTCUSDT to BTC/USDT
            if symbol.endswith('USDT'):
                base = symbol[:-4]  # Remove 'USDT'
                symbol = f"{base}/USDT"
        elif self.market_type == 'spot' and symbol and '/' not in symbol:
            # For spot, also convert BTCUSDT to BTC/USDT
            if symbol.endswith('USDT'):
                base = symbol[:-4]
                symbol = f"{base}/USDT"
        
        return {
            'id': trade.get('id'),
            'order_id': trade.get('order'),
            'symbol': symbol,
            'side': trade.get('side'),
            'amount': Decimal(str(trade.get('amount', 0))),
            'price': Decimal(str(trade.get('price', 0))),
            'cost': Decimal(str(trade.get('cost', 0))),
            'fee': trade.get('fee'),
            'timestamp': trade.get('timestamp'),
            'datetime': trade.get('datetime'),
            'exchange': 'binance',
            'raw': trade
        }
    
    def _normalize_position(self, position: Dict) -> Dict[str, Any]:
        """Normalize ccxt position to standard format."""
        return {
            'symbol': position.get('symbol'),
            'side': 'long' if float(position.get('contracts', 0)) > 0 else 'short',
            'size': abs(Decimal(str(position.get('contracts', 0)))),
            'entry_price': Decimal(str(position.get('entryPrice', 0))),
            'mark_price': Decimal(str(position.get('markPrice', 0))),
            'unrealized_pnl': Decimal(str(position.get('unrealizedPnl', 0))),
            'percentage': Decimal(str(position.get('percentage', 0))),
            'timestamp': position.get('timestamp'),
            'datetime': position.get('datetime'),
            'exchange': 'binance',
            'raw': position
        }
    
    def _normalize_status(self, status: str) -> str:
        """Normalize ccxt order status to standard format."""
        status_map = {
            'open': OrderStatus.OPEN,
            'closed': OrderStatus.FILLED,
            'canceled': OrderStatus.CANCELED,
            'cancelled': OrderStatus.CANCELED,
            'rejected': OrderStatus.REJECTED
        }
        return status_map.get(status.lower(), status)
