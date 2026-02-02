"""
Bybit exchange connector implementation.

Supports both spot and futures trading on Bybit using ccxt.
Handles Bybit's unified trading API and rate limiting.
"""

import asyncio
import ccxt.pro as ccxt
from typing import Dict, List, Optional, Any
from decimal import Decimal
from datetime import datetime, timezone
import logging
import decimal

from ..base_connector import BaseExchangeConnector, OrderSide, OrderType, OrderStatus

logger = logging.getLogger(__name__)


class BybitConnector(BaseExchangeConnector):
    """
    Bybit exchange connector supporting both spot and futures markets.
    
    Features:
    - Unified spot and futures API
    - WebSocket support for real-time data
    - Bybit-specific rate limiting
    - Support for both linear and inverse futures
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Bybit connector.
        
        Args:
            config: Configuration dictionary with:
                - api_key: Bybit API key
                - secret: Bybit secret key
                - sandbox: Use testnet (default: False)
                - market_type: 'spot', 'linear', or 'inverse' (default: 'spot')
        """
        super().__init__(config)
        
        self.market_type = config.get('market_type', 'spot')
        self.testnet = config.get('sandbox', False)
        
        # Initialize ccxt exchange
        self._init_exchange()
        
        # Bybit-specific rate limits
        self.rate_limits = {
            'spot': {
                'default': 600,  # 600 requests per 5 seconds
                'order': 100     # 100 order requests per 5 seconds
            },
            'linear': {
                'default': 600,  # 600 requests per 5 seconds
                'order': 100     # 100 order requests per 5 seconds
            },
            'inverse': {
                'default': 600,  # 600 requests per 5 seconds
                'order': 100     # 100 order requests per 5 seconds
            }
        }
    
    def _init_exchange(self):
        """Initialize ccxt exchange instance."""
        try:
            # Bybit uses different APIs for different market types
            if self.market_type == 'linear':
                # Linear futures (USDT perpetuals)
                self.exchange = ccxt.bybit({
                    'apiKey': self.api_key,
                    'secret': self.secret,
                    'sandbox': self.testnet,
                    'enableRateLimit': True,
                    'rateLimit': 100,  # 100ms between requests
                    'options': {
                        'defaultType': 'linear',
                    }
                })
            elif self.market_type == 'inverse':
                # Inverse futures (coin-margined)
                self.exchange = ccxt.bybit({
                    'apiKey': self.api_key,
                    'secret': self.secret,
                    'sandbox': self.testnet,
                    'enableRateLimit': True,
                    'rateLimit': 100,
                    'options': {
                        'defaultType': 'inverse',
                    }
                })
            else:
                # Spot trading
                self.exchange = ccxt.bybit({
                    'apiKey': self.api_key,
                    'secret': self.secret,
                    'sandbox': self.testnet,
                    'enableRateLimit': True,
                    'rateLimit': 100,
                    'options': {
                        'defaultType': 'spot',
                    }
                })
            
            self.logger.info(f"Initialized Bybit {self.market_type} exchange")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Bybit exchange: {e}")
            raise
    
    async def connect(self) -> bool:
        """Connect to Bybit and load markets."""
        try:
            # Load markets
            await self.exchange.load_markets()
            
            # Test connectivity
            server_time = await self.exchange.fetch_time()
            self.last_heartbeat = datetime.now(timezone.utc)
            
            self.connected = True
            self.logger.info(f"Connected to Bybit {self.market_type} "
                           f"(server_time: {server_time})")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to connect to Bybit: {e}")
            self.connected = False
            return False
    
    async def disconnect(self) -> bool:
        """Disconnect from Bybit."""
        try:
            if self.exchange:
                await self.exchange.close()
            
            self.connected = False
            self.logger.info("Disconnected from Bybit")
            return True
            
        except Exception as e:
            self.logger.error(f"Error disconnecting from Bybit: {e}")
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
            
            # Normalize symbol for Bybit
            bybit_symbol = self.normalize_symbol(symbol)
            
            orderbook = await self.exchange.fetch_order_book(bybit_symbol, limit)
            
            return {
                'symbol': symbol,
                'exchange': 'bybit',
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
        """Place order on Bybit."""
        try:
            if not await self._check_rate_limit('order'):
                raise Exception("Rate limit exceeded for order placement")
            
            # Normalize inputs
            bybit_symbol = self.normalize_symbol(symbol)
            amount_float = float(amount)
            
            # CRITICAL FIX: Round quantity for futures markets to avoid "Qty invalid" error
            if self.market_type in ['linear', 'perp', 'future', 'inverse']:
                # Get market info to determine correct precision
                try:
                    market = self.exchange.market(bybit_symbol)
                    if market and 'precision' in market and 'amount' in market['precision']:
                        amount_precision = market['precision']['amount']
                        # Round to the correct number of decimal places
                        amount_float = round(amount_float, int(amount_precision))
                    else:
                        # Default rounding for Bybit futures (most use whole numbers or 1 decimal)
                        # For BERA perpetual, Bybit typically requires whole numbers
                        if 'BERA' in bybit_symbol.upper():
                            amount_float = round(amount_float, 0)  # Whole numbers for BERA
                        else:
                            amount_float = round(amount_float, 1)  # 1 decimal for most other futures
                    
                    self.logger.debug(f"Bybit {self.market_type}: Rounded amount from {float(amount)} to {amount_float} for {bybit_symbol}")
                except Exception as e:
                    self.logger.warning(f"Could not get market precision for {bybit_symbol}, using default rounding: {e}")
                    # Default to 1 decimal place for futures
                    amount_float = round(amount_float, 1)
            
            # Additional parameters
            order_params = params or {}
            
            # Bybit-specific parameters
            if 'time_in_force' in order_params:
                order_params['timeInForce'] = order_params.pop('time_in_force')
            
            if 'reduce_only' in order_params:
                order_params['reduceOnly'] = order_params.pop('reduce_only')
            
            # Validate order type
            if order_type not in ["market", "limit"]:
                raise ValueError(f"Unsupported order type: {order_type}")

            # Check required parameters
            if order_type == "limit" and price is None:
                raise ValueError("Price required for limit orders")

            try:
                # Place order via ccxt
                if order_type == "market":
                    order = await self.exchange.create_market_order(
                        bybit_symbol, side, amount_float, price, params=order_params
                    )
                elif order_type == "limit":
                    price_float = float(price)
                    order = await self.exchange.create_limit_order(
                        bybit_symbol, side, amount_float, price_float, params=order_params
                    )
            except Exception as e:
                self._handle_error(e, f"place_order({symbol}, {side}, {amount})")
                raise
            
            # Normalize response
            return self._normalize_order(order)
            
        except Exception as e:
            self._handle_error(e, f"place_order({symbol}, {side}, {amount})")
            raise
    
    async def cancel_order(self, order_id: str, symbol: str) -> Dict[str, Any]:
        """Cancel order."""
        try:
            if not await self._check_rate_limit('cancel'):
                raise Exception("Rate limit exceeded for order cancellation")
            
            bybit_symbol = self.normalize_symbol(symbol)
            
            order = await self.exchange.cancel_order(order_id, bybit_symbol)
            return self._normalize_order(order)
            
        except Exception as e:
            self._handle_error(e, f"cancel_order({order_id}, {symbol})")
            raise
    
    async def get_order_status(self, order_id: str, symbol: str) -> Dict[str, Any]:
        """Get order status."""
        try:
            if not await self._check_rate_limit('query'):
                raise Exception("Rate limit exceeded for order status")
            
            bybit_symbol = self.normalize_symbol(symbol)
            
            order = await self.exchange.fetch_order(order_id, bybit_symbol)
            return self._normalize_order(order)
            
        except Exception as e:
            self._handle_error(e, f"get_order_status({order_id}, {symbol})")
            return {}
    
    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get open orders."""
        try:
            if not await self._check_rate_limit('query'):
                raise Exception("Rate limit exceeded for open orders")
            
            bybit_symbol = self.normalize_symbol(symbol) if symbol else None
            
            orders = await self.exchange.fetch_open_orders(bybit_symbol)
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
            
            # Normalize symbol for Bybit API
            bybit_symbol = self.normalize_symbol(symbol) if symbol else None
            since_timestamp = int(since.timestamp() * 1000) if since else None
            
            # DEBUG: Add logging to see what's happening
            self.logger.info(f"DEBUG: Bybit {self.market_type} calling fetch_my_trades with symbol={bybit_symbol} (original: {symbol}), since={since_timestamp}, limit={limit}")
            
            trades = await self.exchange.fetch_my_trades(
                bybit_symbol, since_timestamp, limit
            )
            
            # DEBUG: Log result
            self.logger.info(f"DEBUG: Bybit {self.market_type} fetch_my_trades returned {len(trades)} trades")
            
            normalized_trades = []
            for trade in trades:
                normalized_trade = self._normalize_trade(trade)
                
                # Ensure the symbol is properly denormalized for consistency
                if normalized_trade.get('symbol'):
                    normalized_trade['symbol'] = self.denormalize_symbol(normalized_trade['symbol'])
                
                normalized_trades.append(normalized_trade)
            
            return normalized_trades
            
        except Exception as e:
            # DEBUG: Log the actual error
            self.logger.error(f"DEBUG: Bybit {self.market_type} get_trade_history error: {e}")
            self._handle_error(e, f"get_trade_history({symbol})")
            return []
    
    async def get_positions(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get positions (futures only)."""
        if self.market_type == 'spot':
            return []
        
        try:
            if not await self._check_rate_limit('query'):
                raise Exception("Rate limit exceeded for positions")
            
            # CRITICAL FIX: Bybit requires category parameter for positions
            # Determine the correct category based on market type
            if self.market_type == 'linear':
                category = 'linear'
            elif self.market_type == 'inverse':
                category = 'inverse'
            else:
                # Default to linear for perp markets
                category = 'linear'
            
            # Pass the category parameter in params
            params = {'category': category}
            
            # CRITICAL FIX: If no symbol provided, add settleCoin for linear positions
            if symbol:
                # Normalize symbol for Bybit
                bybit_symbol = self.normalize_symbol(symbol)
                params['symbol'] = bybit_symbol
            else:
                # For linear positions, specify settleCoin when no symbol is provided
                if category == 'linear':
                    params['settleCoin'] = 'USDT'  # Most common settle coin for linear positions
            
            self.logger.info(f"DEBUG: Bybit {self.market_type} fetching positions with category={category}, symbol={symbol}, params={params}")
            
            # Fetch positions with proper parameters
            positions = await self.exchange.fetch_positions(None, params=params)
            
            self.logger.info(f"DEBUG: Bybit {self.market_type} fetch_positions returned {len(positions)} positions")
            
            result = []
            for position in positions:
                # Only include positions with non-zero size
                contracts = float(position.get('contracts', 0))
                if abs(contracts) > 0:
                    normalized_position = self._normalize_position(position)
                    # Ensure proper symbol denormalization
                    if normalized_position.get('symbol'):
                        normalized_position['symbol'] = self.denormalize_symbol(normalized_position['symbol'])
                    result.append(normalized_position)
            
            self.logger.info(f"DEBUG: Bybit {self.market_type} returning {len(result)} non-zero positions")
            return result
            
        except Exception as e:
            self.logger.error(f"DEBUG: Bybit {self.market_type} get_positions error: {e}")
            self._handle_error(e, f"get_positions({symbol})")
            return []
    
    def normalize_symbol(self, symbol: str) -> str:
        """Convert standard symbol format to Bybit format."""
        if not symbol:
            return symbol
            
        if self.market_type == 'spot':
            # Spot: Keep slash format - BERA/USDT stays as BERA/USDT
            return symbol
        else:
            # Futures: Need to convert to appropriate futures format
            if self.market_type == 'linear':
                # Linear futures: BERA/USDT -> BERAUSDT (no slash)
                if '/' in symbol:
                    base, quote = symbol.split('/')
                    # For linear futures, always use USDT
                    return f"{base}USDT"
                else:
                    # Already in correct format
                    return symbol
            elif self.market_type == 'inverse':
                # Inverse futures: Different format
                if ':' in symbol:
                    base_quote = symbol.split(':')[0]  # BTC/USD
                else:
                    base_quote = symbol  # BTC/USDT
                
                return base_quote.replace('/', '')  # BTCUSD
            else:
                # Default to linear format
                if '/' in symbol:
                    base, quote = symbol.split('/')
                    return f"{base}USDT"
                return symbol
    
    def denormalize_symbol(self, symbol: str) -> str:
        """Convert Bybit symbol to standard format."""
        if not symbol:
            return symbol
            
        if self.market_type == 'spot':
            # Spot symbols should already be in correct format
            return symbol
        else:
            # Futures symbols need to be converted back
            if self.market_type == 'linear':
                # BERAUSDT -> BERA/USDT
                if 'USDT' in symbol and '/' not in symbol:
                    if symbol.endswith('USDT'):
                        base = symbol[:-4]  # Remove 'USDT'
                        return f"{base}/USDT"
                return symbol
            elif self.market_type == 'inverse':
                # BTCUSD -> BTC/USD
                if len(symbol) >= 6 and '/' not in symbol:
                    # Try to split common inverse pairs
                    if symbol.endswith('USD'):
                        base = symbol[:-3]
                        return f"{base}/USD"
                return symbol
            else:
                return symbol
    
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
            'symbol': order.get('symbol'),
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
            'exchange': 'bybit',
            'raw': order
        }
    
    def _normalize_trade(self, trade: Dict) -> Dict[str, Any]:
        """Normalize ccxt trade to standard format."""
        # Log the raw trade data for debugging
        logger.debug(f"[BYBIT] Raw trade data: id={trade.get('id')} raw={trade}")
        with open('logs/bybit_connector_raw_trades.log', 'a') as f:
            f.write(f"[BYBIT] Raw trade data: id={trade.get('id')} raw={trade}\n")
        
        # Extract client_order_id from info.orderLinkId for Bybit
        info = trade.get('info', {})
        client_order_id = info.get('orderLinkId', '')
        if not client_order_id:
            client_order_id = "manual_order"
        
        return {
            'id': trade.get('id'),
            'order_id': trade.get('order'),
            'client_order_id': client_order_id,  # Include client order ID
            'symbol': trade.get('symbol'),
            'side': trade.get('side'),
            'amount': Decimal(str(trade.get('amount', 0))),
            'price': Decimal(str(trade.get('price', 0))),
            'cost': Decimal(str(trade.get('cost', 0))),
            'fee': trade.get('fee'),
            'timestamp': trade.get('timestamp'),
            'datetime': trade.get('datetime'),
            'exchange': 'bybit',
            'raw': trade
        }
    
    def _normalize_position(self, position: Dict) -> Dict[str, Any]:
        """Normalize ccxt position to standard format."""
        
        def safe_decimal(value, default=0):
            """Safely convert value to Decimal."""
            if value is None:
                return None if default is None else Decimal(str(default))
            try:
                return Decimal(str(value))
            except (ValueError, TypeError, decimal.InvalidOperation):
                return None if default is None else Decimal(str(default))
        
        contracts = safe_decimal(position.get('contracts'), 0)
        
        return {
            'symbol': position.get('symbol'),
            'side': 'long' if contracts > 0 else 'short',
            'size': abs(contracts),
            'entry_price': safe_decimal(position.get('entryPrice'), 0),
            'mark_price': safe_decimal(position.get('markPrice'), 0),
            'unrealized_pnl': safe_decimal(position.get('unrealizedPnl'), 0),
            'percentage': safe_decimal(position.get('percentage'), 0),
            'timestamp': position.get('timestamp'),
            'datetime': position.get('datetime'),
            'exchange': 'bybit',
            'raw': position
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
