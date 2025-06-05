"""
Hyperliquid exchange connector implementation.

Futures-only exchange with unique characteristics:
- On-chain perpetual futures
- No traditional API keys (uses wallet signatures)
- Volume-based rate limiting
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


class HyperliquidConnector(BaseExchangeConnector):
    """
    Hyperliquid exchange connector for perpetual futures trading.
    
    Features:
    - Futures-only trading (no spot markets)
    - On-chain settlement with fast execution
    - Wallet-based authentication
    - Volume-based rate limiting
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Hyperliquid connector.
        
        Args:
            config: Configuration dictionary with:
                - api_key: Not used (wallet-based auth)
                - secret: Private key or wallet seed
                - sandbox: Use testnet (default: False)
                - wallet_address: Ethereum wallet address
        """
        super().__init__(config)
        
        self.wallet_address = config.get('wallet_address')
        self.private_key = config.get('private_key') or config.get('secret')
        self.testnet = config.get('sandbox', False)
        
        # Initialize ccxt exchange
        self._init_exchange()
        
        # Hyperliquid-specific settings
        self.market_type = 'future'  # Only futures available
        
        # Volume-based rate limits (different from other exchanges)
        self.rate_limits = {
            'base_limit': 200,  # Base requests per second
            'volume_multiplier': 0.001  # Additional requests per 1 USDC volume
        }
    
    def _init_exchange(self):
        """Initialize ccxt exchange instance."""
        try:
            self.exchange = ccxt.hyperliquid({
                'walletAddress': self.wallet_address,  # CCXT expects 'walletAddress'
                'privateKey': self.private_key,        # CCXT expects 'privateKey'
                'sandbox': self.testnet,
                'enableRateLimit': True,
                'rateLimit': 100,  # 100ms between requests
                'options': {
                    'defaultType': 'swap',  # Perpetual futures
                }
            })
            
            self.logger.info("Initialized Hyperliquid futures exchange")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Hyperliquid exchange: {e}")
            raise
    
    async def connect(self) -> bool:
        """Connect to Hyperliquid and load markets."""
        try:
            # Load markets
            await self.exchange.load_markets()
            
            # Test connectivity with account info
            try:
                account_info = await self.exchange.fetch_balance()
                self.last_heartbeat = datetime.now()
                
                self.connected = True
                self.logger.info(f"Connected to Hyperliquid "
                               f"(wallet: {self.wallet_address[:8]}...)")
                
                return True
                
            except Exception as e:
                # If balance fails, try a simpler connectivity test
                markets = await self.exchange.fetch_markets()
                if markets:
                    self.connected = True
                    self.last_heartbeat = datetime.now()
                    self.logger.info("Connected to Hyperliquid (limited access)")
                    return True
                else:
                    raise e
            
        except Exception as e:
            self.logger.error(f"Failed to connect to Hyperliquid: {e}")
            self.connected = False
            return False
    
    async def disconnect(self) -> bool:
        """Disconnect from Hyperliquid."""
        try:
            if self.exchange:
                await self.exchange.close()
            
            self.connected = False
            self.logger.info("Disconnected from Hyperliquid")
            return True
            
        except Exception as e:
            self.logger.error(f"Error disconnecting from Hyperliquid: {e}")
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
            
            # Normalize symbol for Hyperliquid
            hl_symbol = self.normalize_symbol(symbol)
            
            orderbook = await self.exchange.fetch_order_book(hl_symbol, limit)
            
            return {
                'symbol': symbol,
                'exchange': 'hyperliquid',
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
        """Place order on Hyperliquid."""
        try:
            if not await self._check_rate_limit('order'):
                raise Exception("Rate limit exceeded for order placement")
            
            # Normalize inputs
            hl_symbol = self.normalize_symbol(symbol)
            amount_float = float(amount)
            price_float = float(price) if price else None
            
            # Validate order type
            if order_type not in ["market", "limit"]:
                raise ValueError(f"Unsupported order type: {order_type}")
            
            # Check required parameters for limit orders
            if order_type == "limit" and price_float is None:
                raise ValueError("Price required for limit orders")
            
            # Simplified parameters for Hyperliquid - avoid any extra params that might cause serialization issues
            order_params = {}
            
            # Do not add clientOrderId as it causes "Failed to deserialize the JSON body" errors on Hyperliquid
            
            # Place order via ccxt
            if order_type == "market":
                order = await self.exchange.create_market_order(
                    hl_symbol, side, amount_float, price_float, params=order_params
                )
            elif order_type == "limit":
                order = await self.exchange.create_limit_order(
                    hl_symbol, side, amount_float, price_float, params=order_params
                )
            
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
            
            hl_symbol = self.normalize_symbol(symbol)
            
            order = await self.exchange.cancel_order(order_id, hl_symbol)
            return self._normalize_order(order)
            
        except Exception as e:
            self._handle_error(e, f"cancel_order({order_id}, {symbol})")
            raise
    
    async def get_order_status(self, order_id: str, symbol: str) -> Dict[str, Any]:
        """Get order status."""
        try:
            if not await self._check_rate_limit('query'):
                raise Exception("Rate limit exceeded for order status")
            
            hl_symbol = self.normalize_symbol(symbol)
            
            order = await self.exchange.fetch_order(order_id, hl_symbol)
            return self._normalize_order(order)
            
        except Exception as e:
            self._handle_error(e, f"get_order_status({order_id}, {symbol})")
            return {}
    
    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get open orders."""
        try:
            if not await self._check_rate_limit('query'):
                raise Exception("Rate limit exceeded for open orders")
            
            hl_symbol = self.normalize_symbol(symbol) if symbol else None
            
            orders = await self.exchange.fetch_open_orders(hl_symbol)
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
            
            hl_symbol = self.normalize_symbol(symbol) if symbol else None
            since_timestamp = int(since.timestamp() * 1000) if since else None
            
            # DEBUG: Add logging to see what's happening
            self.logger.info(f"DEBUG: Hyperliquid calling fetch_my_trades with symbol={hl_symbol} (original: {symbol}), since={since_timestamp}, limit={limit}")
            
            trades = await self.exchange.fetch_my_trades(
                hl_symbol, since_timestamp, limit
            )
            
            # DEBUG: Log result
            self.logger.info(f"DEBUG: Hyperliquid fetch_my_trades returned {len(trades)} trades")
            
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
            self.logger.error(f"DEBUG: Hyperliquid get_trade_history error: {e}")
            self._handle_error(e, f"get_trade_history({symbol})")
            return []
    
    async def get_positions(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get positions."""
        try:
            if not await self._check_rate_limit('query'):
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
        """Convert standard symbol format to Hyperliquid format."""
        # For Hyperliquid perpetuals, we need to convert to COIN/USDC:USDC format
        # BERA/USDT -> BERA/USDC:USDC
        # BTC/USDT -> BTC/USDC:USDC
        
        if symbol and '/' in symbol:
            base, quote = symbol.split('/')
            
            # Convert USDT symbols to USDC:USDC format for Hyperliquid
            if quote.upper() == 'USDT':
                return f"{base}/USDC:USDC"
            elif quote.upper() == 'USDC' and ':' not in symbol:
                # If it's already USDC but not in perpetual format, add :USDC
                return f"{base}/USDC:USDC"
        
        # If already in correct format or not a recognized pattern, return as-is
        return symbol
    
    def denormalize_symbol(self, symbol: str) -> str:
        """Convert Hyperliquid symbol to standard format."""
        # Convert BERA/USDC:USDC back to BERA/USDT for consistency
        if symbol and '/USDC:USDC' in symbol:
            base = symbol.split('/')[0]
            return f"{base}/USDT"
        
        # If it's already in standard format, return as-is
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
            'exchange': 'hyperliquid',
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
            'exchange': 'hyperliquid',
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
            'symbol': self.denormalize_symbol(position.get('symbol', '')),
            'side': 'long' if contracts > 0 else 'short',
            'size': abs(contracts),
            'entry_price': safe_decimal(position.get('entryPrice'), 0),
            'mark_price': safe_decimal(position.get('markPrice'), 0),
            'unrealized_pnl': safe_decimal(position.get('unrealizedPnl'), 0),
            'percentage': safe_decimal(position.get('percentage'), 0),
            'timestamp': position.get('timestamp'),
            'datetime': position.get('datetime'),
            'exchange': 'hyperliquid',
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
            'rejected': OrderStatus.REJECTED
        }
        return status_map.get(status.lower(), status)
