"""
KuCoin exchange connector implementation.

Spot trading exchange with excellent API support and competitive fees.
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


class KucoinConnector(BaseExchangeConnector):
    """
    KuCoin exchange connector for spot trading.
    
    Features:
    - Spot trading only
    - Good API stability
    - Competitive fees with KCS discounts
    - WebSocket support for real-time data
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize KuCoin connector.
        
        Args:
            config: Configuration dictionary with:
                - api_key: KuCoin API key
                - secret: KuCoin secret key
                - passphrase: KuCoin passphrase (required)
                - sandbox: Use testnet (default: False)
        """
        super().__init__(config)
        
        self.market_type = 'spot'  # Only spot trading available
        self.testnet = config.get('sandbox', False)
        
        # Initialize ccxt exchange
        self._init_exchange()
        
        # KuCoin-specific rate limits
        self.rate_limits = {
            'default': 100,  # 100 requests per second
            'order': 45,     # 45 order requests per second
            'query': 100     # 100 query requests per second
        }
    
    def _init_exchange(self):
        """Initialize ccxt exchange instance."""
        try:
            self.exchange = ccxt.kucoin({
                'apiKey': self.api_key,
                'secret': self.secret,
                'password': self.passphrase,  # KuCoin requires passphrase
                'sandbox': self.testnet,
                'enableRateLimit': True,
                'rateLimit': 100,  # 100ms between requests
                'options': {
                    'defaultType': 'spot',
                }
            })
            
            self.logger.info("Initialized KuCoin spot exchange")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize KuCoin exchange: {e}")
            raise
    
    async def connect(self) -> bool:
        """Connect to KuCoin and load markets."""
        try:
            # Structured, sanitized connect logging
            try:
                self.logger.info("[CONNECT START] KuCoin")
                self.logger.info(
                    f"KuCoin connect params: market_type={self.market_type} testnet={self.testnet} api_key_set={bool(self.api_key)} secret_set={bool(self.secret)} passphrase_set={bool(self.passphrase)}"
                )
            except Exception:
                pass
            # Load markets
            await self.exchange.load_markets()
            
            # Test connectivity
            server_time = await self.exchange.fetch_time()
            self.last_heartbeat = datetime.now(timezone.utc)
            
            self.connected = True
            self.logger.info(f"Connected to KuCoin spot (server_time: {server_time})")
            try:
                markets_count = len(getattr(self.exchange, 'markets', {}) or {})
                self.logger.info(f"[CONNECT OK] KuCoin markets_loaded={markets_count}")
            except Exception:
                pass
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to connect to KuCoin: {e}")
            self.connected = False
            return False

    async def watchMyTrades(
        self,
        symbol: Optional[str] = None,
        since: Optional[int] = None,
        limit: Optional[int] = None,
        params: Optional[Dict] = None
    ):
        """Watch authenticated user trades via WebSocket with detailed logging."""
        try:
            self.logger.info("[watchMyTrades] KuCoin called")
            self.logger.info(
                f"  inputs: symbol={symbol} since={since} limit={limit} params_keys={list((params or {}).keys())}"
            )
        except Exception:
            pass

        if not hasattr(self.exchange, 'watch_my_trades'):
            self.logger.warning("[watchMyTrades] KuCoin exchange.watch_my_trades not available")
            return []

        try:
            watch_symbol = self.normalize_symbol(symbol) if symbol else None
            if symbol and watch_symbol != symbol:
                self.logger.info(f"  normalized symbol: {symbol} -> {watch_symbol}")

            trades = await self.exchange.watch_my_trades(watch_symbol, since, limit, params or {})
            try:
                sample_id = None
                if trades and len(trades) > 0:
                    first = trades[0]
                    sample_id = first.get('id') if isinstance(first, dict) else None
                self.logger.info(
                    f"[watchMyTrades OK] KuCoin returned count={len(trades) if trades else 0} sample_id={sample_id}"
                )
            except Exception:
                pass
            return trades
        except Exception as e:
            self.logger.error(f"[watchMyTrades ERROR] KuCoin: {e}")
            raise
    
    async def disconnect(self) -> bool:
        """Disconnect from KuCoin."""
        try:
            if self.exchange:
                await self.exchange.close()
            
            self.connected = False
            self.logger.info("Disconnected from KuCoin")
            return True
            
        except Exception as e:
            self.logger.error(f"Error disconnecting from KuCoin: {e}")
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
                    if isinstance(info, dict) and 'total' in info:
                        free_amount = Decimal(str(info['total']))
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
            
            # Normalize symbol for KuCoin
            kucoin_symbol = self.normalize_symbol(symbol)
            
            # KuCoin requires limit to be 20 or 100
            valid_limit = 20 if limit <= 20 else 100
            
            orderbook = await self.exchange.fetch_order_book(kucoin_symbol, valid_limit)
            
            return {
                'symbol': symbol,
                'exchange': 'kucoin',
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
        """Place order on KuCoin."""
        try:
            if not await self._check_rate_limit('order'):
                raise Exception("Rate limit exceeded for order placement")
            
            # Normalize inputs
            kucoin_symbol = self.normalize_symbol(symbol)
            amount_float = float(amount)
            
            # Additional parameters
            order_params = params or {}
            
            # Validate order type
            if order_type not in ["market", "limit"]:
                raise ValueError(f"Unsupported order type: {order_type}")
            
            # Check required parameters for limit orders
            if order_type == "limit" and price is None:
                raise ValueError("Price required for limit orders")
            
            price_float = float(price) if price else None
            
            try:
                # Place order via ccxt
                if order_type == "market":
                    order = await self.exchange.create_market_order(
                        kucoin_symbol, side, amount_float, price_float, params=order_params
                    )
                elif order_type == "limit":
                    order = await self.exchange.create_limit_order(
                        kucoin_symbol, side, amount_float, price_float, params=order_params
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
            
            kucoin_symbol = self.normalize_symbol(symbol)
            
            order = await self.exchange.cancel_order(order_id, kucoin_symbol)
            return self._normalize_order(order)
            
        except Exception as e:
            self._handle_error(e, f"cancel_order({order_id}, {symbol})")
            raise
    
    async def get_order_status(self, order_id: str, symbol: str) -> Dict[str, Any]:
        """Get order status."""
        try:
            if not await self._check_rate_limit('query'):
                raise Exception("Rate limit exceeded for order status")
            
            kucoin_symbol = self.normalize_symbol(symbol)
            
            order = await self.exchange.fetch_order(order_id, kucoin_symbol)
            return self._normalize_order(order)
            
        except Exception as e:
            self._handle_error(e, f"get_order_status({order_id}, {symbol})")
            return {}
    
    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get open orders."""
        try:
            if not await self._check_rate_limit('query'):
                raise Exception("Rate limit exceeded for open orders")
            
            kucoin_symbol = self.normalize_symbol(symbol) if symbol else None
            
            orders = await self.exchange.fetch_open_orders(kucoin_symbol)
            return [self._normalize_order(order) for order in orders]
            
        except Exception as e:
            self._handle_error(e, f"get_open_orders({symbol})")
            return []
    
    async def get_trade_history(
        self,
        symbol: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: int = 100,
        until: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """
        Get trade history using the fetch_my_trades ccxt method.
        This provides actual trade executions with correct timestamps.
        """
        try:
            if not await self._check_rate_limit('query', weight=1):
                raise Exception("Rate limit exceeded for trade history")

            kucoin_symbol = self.normalize_symbol(symbol) if symbol else None
            since_timestamp = int(since.timestamp() * 1000) if since else None

            self.logger.info(
                "Fetching KuCoin trade history",
                exchange="KucoinConnector",
                symbol=kucoin_symbol,
                start=since.isoformat() if since else "None",
                limit=limit
            )

            # Use ccxt's fetch_my_trades method
            trades = await self.exchange.fetch_my_trades(
                symbol=kucoin_symbol,
                since=since_timestamp,
                limit=limit
            )

            self.logger.info(f"Successfully fetched {len(trades)} trades from KuCoin via fetch_my_trades")

            # Normalize trades to our standard format
            return [self._normalize_trade(trade) for trade in trades]

        except Exception as e:
            self.logger.error(f"Error in get_trade_history using fetch_my_trades: {e}", exc_info=True)
            self._handle_error(e, f"get_trade_history({symbol})")
            return []
    
    async def get_positions(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get open positions (not applicable for spot)."""
        return []  # KuCoin spot only, no positions
    
    def normalize_symbol(self, symbol: str) -> str:
        """Convert standard symbol format to KuCoin format.
        
        KuCoin uses dashes instead of slashes: BTC/USDT -> BTC-USDT
        """
        if not symbol:
            return symbol
        
        # Convert slash to dash for KuCoin
        # FUEL/USDT -> FUEL-USDT
        return symbol.replace('/', '-').replace(':USDT', '').replace(':USDC', '')
    
    def denormalize_symbol(self, symbol: str) -> str:
        """Convert KuCoin symbol to standard format.
        
        KuCoin uses dashes: BTC-USDT -> BTC/USDT
        """
        if not symbol:
            return symbol
        
        # Convert dash to slash for standard format
        # FUEL-USDT -> FUEL/USDT
        return symbol.replace('-', '/')
    
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
        
        # Denormalize symbol from KuCoin format
        symbol = order.get('symbol', '')
        standard_symbol = self.denormalize_symbol(symbol)
        
        return {
            'id': order.get('id'),
            'symbol': standard_symbol,  # Use standard format
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
            'exchange': 'kucoin',
            'raw': order
        }
    
    def _normalize_trade(self, trade: Dict) -> Dict[str, Any]:
        """Normalize ccxt trade to standard format."""
        # Log the raw trade data for debugging
        logger.debug(f"[KuCoin] Raw trade data: id={trade.get('id')} raw={trade}")
        
        # Extract client_order_id from info.clientOid for KuCoin
        info = trade.get('info', {})
        client_order_id = info.get('clientOid')
        if not client_order_id:
            client_order_id = "manual_order"
        
        # Denormalize symbol from KuCoin format
        symbol = trade.get('symbol', '')
        standard_symbol = self.denormalize_symbol(symbol)
        
        return {
            'id': trade.get('id'),
            'order_id': trade.get('order'),
            'client_order_id': client_order_id,  # Include client order ID
            'symbol': standard_symbol,  # Use standard format
            'side': trade.get('side'),
            'amount': Decimal(str(trade.get('amount', 0))),
            'price': Decimal(str(trade.get('price', 0))),
            'cost': Decimal(str(trade.get('cost', 0))),
            'fee': trade.get('fee'),
            'timestamp': trade.get('timestamp'),
            'datetime': trade.get('datetime'),
            'exchange': 'kucoin',
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
