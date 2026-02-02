"""
Bitfinex exchange connector implementation (public endpoints only).

Supports public market data via ccxt.pro (async).
"""

import asyncio
import ccxt.async_support as ccxt  # REST/async (supports bitfinex2)
import ccxt.pro as ccxtpro         # WebSocket (pro) - uses bitfinex (no bitfinex2 in pro)
from typing import Dict, List, Optional, Any
from decimal import Decimal
from datetime import datetime, timezone
import logging

from ..base_connector import BaseExchangeConnector

logger = logging.getLogger(__name__)

class BitfinexConnector(BaseExchangeConnector):
    """
    Bitfinex exchange connector (public endpoints only for now).
    """
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.exchange_ws = None  # Separate WS client (pro)
        self.uses_rest_v2 = False  # Track whether REST v2 is used
        self._init_exchange()

    def _init_exchange(self):
        try:
            # REST client (v2) for trading and REST data
            try:
                self.exchange = ccxt.bitfinex2({
                    'apiKey': self.config.get('api_key', ''),
                    'secret': self.config.get('secret', ''),
                    'enableRateLimit': True,
                    'rateLimit': 100,
                })
                self.uses_rest_v2 = True
            except AttributeError:
                # Fallback: use ccxt.pro bitfinex (v1) for both REST and WS
                self.exchange = ccxtpro.bitfinex({
                    'apiKey': self.config.get('api_key', ''),
                    'secret': self.config.get('secret', ''),
                    'enableRateLimit': True,
                })
                self.uses_rest_v2 = False

            # WS client (pro) for streaming (no bitfinex2 class in pro)
            try:
                self.exchange_ws = ccxtpro.bitfinex({
                    'apiKey': self.config.get('api_key', ''),
                    'secret': self.config.get('secret', ''),
                    'enableRateLimit': True,
                })
            except Exception:
                self.exchange_ws = None
            
            # Set trading mode based on config
            self.trading_enabled = bool(self.config.get('api_key') and self.config.get('secret'))
            
            if self.trading_enabled:
                if self.uses_rest_v2:
                    self.logger.info("Initialized Bitfinex (REST v2 + WS) with trading capabilities")
                else:
                    self.logger.info("Initialized Bitfinex (REST v1 via pro + WS) with trading capabilities")
            else:
                if self.uses_rest_v2:
                    self.logger.info("Initialized Bitfinex (REST v2 + WS public endpoints only)")
                else:
                    self.logger.info("Initialized Bitfinex (REST v1 via pro + WS public endpoints only)")
        except Exception as e:
            self.logger.error(f"Failed to initialize Bitfinex exchange: {e}")
            raise

    async def connect(self) -> bool:
        try:
            # Load markets on REST
            await self.exchange.load_markets()
            # Load markets on WS if available
            if self.exchange_ws is not None:
                try:
                    await self.exchange_ws.load_markets()
                except Exception:
                    # WS markets load failure should not prevent REST usage
                    pass
            # Bitfinex may not support fetchTime; consider connectivity established after load_markets
            self.last_heartbeat = datetime.now(timezone.utc)
            self.connected = True
            self.logger.info("Connected to Bitfinex (REST v2 load_markets successful)")
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to Bitfinex: {e}")
            # Ensure resources are closed on failure to avoid unclosed session warnings
            try:
                if self.exchange:
                    await self.exchange.close()
            except Exception:
                pass
            try:
                if self.exchange_ws:
                    await self.exchange_ws.close()
            except Exception:
                pass
            self.connected = False
            return False

    async def disconnect(self) -> bool:
        try:
            if self.exchange:
                await self.exchange.close()
            if self.exchange_ws:
                await self.exchange_ws.close()
            self.connected = False
            self.logger.info("Disconnected from Bitfinex")
            return True
        except Exception as e:
            self.logger.error(f"Error disconnecting from Bitfinex: {e}")
            return False

    async def get_orderbook(self, symbol: str, limit: int = 20) -> Optional[Dict[str, Any]]:
        try:
            if not await self._check_rate_limit('query'):
                raise Exception("Rate limit exceeded for orderbook")
            bitfinex_symbol = self.normalize_symbol(symbol)
            # Prefer REST fetch for stability
            orderbook = await self.exchange.fetch_order_book(bitfinex_symbol, limit)
            self.logger.debug(f"Retrieved orderbook for {symbol} with {len(orderbook['bids'])} bids, {len(orderbook['asks'])} asks")
            return orderbook
        except Exception as e:
            self.logger.error(f"get_orderbook({symbol}): {e}")
            return None

    async def get_trade_history(
        self,
        symbol: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: int = 100,
        until: Optional[datetime] = None,
        params: Optional[Dict] = None
    ) -> List[Dict[str, Any]]:
        """
        Get recent trades (public, not user trade history).
        """
        try:
            bitfinex_symbol = self.normalize_symbol(symbol) if symbol else None
            since_timestamp = int(since.timestamp() * 1000) if since else None
            # Bitfinex fetch_trades is public (not user-specific)
            trades = await self.exchange.fetch_trades(
                symbol=bitfinex_symbol,
                since=since_timestamp,
                limit=limit,
                params=params or {}
            )
            self.logger.info(f"Fetched {len(trades)} trades for {symbol} from Bitfinex.")
            return [self._normalize_trade(trade) for trade in trades]
        except Exception as e:
            self.logger.error(f"Error in Bitfinex get_trade_history: {e}")
            return []

    async def watch_trades(self, symbol: str, limit: int = 1000):
        """
        Async generator that yields normalized trades as they arrive via WebSocket.
        Usage:
            async for trade in connector.watch_trades('ETH/USDT'):
                ...
        """
        if self.exchange_ws is None:
            # Fallback: poll via REST if WS client unavailable
            while True:
                try:
                    trades = await self.exchange.fetch_trades(self.normalize_symbol(symbol), limit=limit)
                    for trade in trades:
                        yield self._normalize_tick_trade(trade, symbol)
                    await asyncio.sleep(1)
                except Exception as e:
                    self.logger.error(f"Error in watch_trades fallback for {symbol}: {e}")
                    await asyncio.sleep(2)
            return
        bitfinex_symbol = self.normalize_symbol(symbol)
        while True:
            try:
                trades = await self.exchange_ws.watch_trades(bitfinex_symbol)
                for trade in trades:
                    yield self._normalize_tick_trade(trade, symbol)
            except Exception as e:
                self.logger.error(f"Error in watch_trades for {symbol}: {e}")
                await asyncio.sleep(2)  # Backoff on error

    def normalize_symbol(self, symbol: str) -> str:
        # Bitfinex uses standard 'BTC/USDT' format (strategy maps USDT->USD upstream when needed)
        return symbol

    def _normalize_trade(self, trade: Dict) -> Dict[str, Any]:
        return {
            'id': trade.get('id'),
            'order_id': trade.get('order') or trade.get('order_id'),
            'symbol': trade.get('symbol'),
            'side': trade.get('side'),
            'amount': Decimal(str(trade.get('amount', 0))),
            'price': Decimal(str(trade.get('price', 0))),
            'cost': Decimal(str(trade.get('cost', 0))),
            'fee': trade.get('fee'),
            'timestamp': trade.get('timestamp'),
            'datetime': trade.get('datetime'),
            'exchange': 'bitfinex',
            'raw': trade
        }

    def _normalize_tick_trade(self, trade: Dict, symbol: str) -> Dict[str, Any]:
        """
        Normalize a Bitfinex trade to the standard tick/trade format for the collector/strategy.
        """
        return {
            'exchange': 'bitfinex',
            'symbol': symbol,
            'trade_id': trade.get('id'),
            'side': trade.get('side'),
            'price': float(trade.get('price', 0)),
            'amount': float(trade.get('amount', 0)),
            'timestamp': datetime.fromtimestamp(trade.get('timestamp', 0) / 1000, tz=timezone.utc),
        }
        
    def _normalize_order(self, order: Dict) -> Dict[str, Any]:
        """Normalize CCXT order to standard format."""
        return {
            'id': order.get('id'),
            'client_order_id': order.get('clientOrderId'),
            'symbol': order.get('symbol'),
            'side': order.get('side'),
            'amount': Decimal(str(order.get('amount', 0))),
            'price': Decimal(str(order.get('price', 0))) if order.get('price') else None,
            'filled': Decimal(str(order.get('filled', 0))),
            'remaining': Decimal(str(order.get('remaining', 0))),
            'status': order.get('status'),
            'type': order.get('type'),
            'timestamp': order.get('timestamp'),
            'datetime': order.get('datetime'),
            'fee': order.get('fee'),
            'exchange': 'bitfinex',
            'raw': order
        }

    # The following methods are not implemented for public-only connector
    async def get_balance(self, currency: Optional[str] = None) -> Dict[str, Decimal]:
        """
        Get account balance for a specific currency or all currencies.
        
        Args:
            currency: Currency code (e.g., 'BTC') or None for all currencies
            
        Returns:
            Dictionary of currency -> balance
        """
        if not self.trading_enabled:
            self.logger.warning("Trading not enabled: missing API credentials")
            return {} if currency is None else {currency: Decimal('0')}
            
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
            return {} if currency is None else {currency: Decimal('0')}

    async def place_order(
        self,
        symbol: str,
        side: str,
        amount: Decimal,
        price: Optional[Decimal] = None,
        order_type: str = "limit",
        params: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Place an order on Bitfinex.
        
        Args:
            symbol: Trading symbol (e.g., 'ETH/USDT')
            side: Order side ('buy' or 'sell')
            amount: Order amount
            price: Order price (None for market orders)
            order_type: Type of order ('limit' or 'market')
            params: Additional exchange-specific parameters
            
        Returns:
            Order information dictionary
        """
        if not self.trading_enabled:
            raise Exception("Trading not enabled: missing API credentials")
            
        try:
            if not await self._check_rate_limit('order', weight=1):
                raise Exception("Rate limit exceeded for order placement")
            
            # Normalize inputs
            bitfinex_symbol = self.normalize_symbol(symbol)
            amount_float = float(amount)
            price_float = float(price) if price else None
            
            # Additional parameters
            order_params = params or {}
            
            # Place order via ccxt (REST v2)
            if order_type.lower() == "market":
                order = await self.exchange.create_market_order(
                    bitfinex_symbol, side, amount_float, params=order_params
                )
            elif order_type.lower() == "limit":
                if price_float is None:
                    raise ValueError("Price required for limit orders")
                order = await self.exchange.create_limit_order(
                    bitfinex_symbol, side, amount_float, price_float, params=order_params
                )
            else:
                raise ValueError(f"Unsupported order type: {order_type}")
            
            # Normalize response
            return self._normalize_order(order)
            
        except Exception as e:
            self._handle_error(e, f"place_order({symbol}, {side}, {amount})")
            raise

    async def cancel_order(self, order_id: str, symbol: str) -> Dict[str, Any]:
        """
        Cancel an order on Bitfinex.
        
        Args:
            order_id: Exchange order ID
            symbol: Trading symbol
            
        Returns:
            Cancellation result
        """
        if not self.trading_enabled:
            raise Exception("Trading not enabled: missing API credentials")
            
        try:
            if not await self._check_rate_limit('cancel', weight=1):
                raise Exception("Rate limit exceeded for order cancellation")
            
            bitfinex_symbol = self.normalize_symbol(symbol)
            
            order = await self.exchange.cancel_order(order_id, bitfinex_symbol)
            return self._normalize_order(order)
            
        except Exception as e:
            self._handle_error(e, f"cancel_order({order_id}, {symbol})")
            raise

    async def get_order_status(self, order_id: str, symbol: str) -> Dict[str, Any]:
        """
        Get order status from Bitfinex.
        
        Args:
            order_id: Exchange order ID
            symbol: Trading symbol
            
        Returns:
            Order status information
        """
        if not self.trading_enabled:
            raise Exception("Trading not enabled: missing API credentials")
            
        try:
            if not await self._check_rate_limit('query', weight=2):
                raise Exception("Rate limit exceeded for order status")
            
            bitfinex_symbol = self.normalize_symbol(symbol)
            
            order = await self.exchange.fetch_order(order_id, bitfinex_symbol)
            return self._normalize_order(order)
            
        except Exception as e:
            self._handle_error(e, f"get_order_status({order_id}, {symbol})")
            return {}

    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get open orders from Bitfinex.
        
        Args:
            symbol: Filter by symbol, None for all symbols
            
        Returns:
            List of open orders
        """
        if not self.trading_enabled:
            raise Exception("Trading not enabled: missing API credentials")
            
        try:
            if not await self._check_rate_limit('query', weight=3):
                raise Exception("Rate limit exceeded for open orders")
            
            bitfinex_symbol = self.normalize_symbol(symbol) if symbol else None
            
            orders = await self.exchange.fetch_open_orders(bitfinex_symbol)
            return [self._normalize_order(order) for order in orders]
            
        except Exception as e:
            self._handle_error(e, f"get_open_orders({symbol})")
            return []

    async def get_positions(self, *args, **kwargs):
        raise NotImplementedError("Bitfinex public connector does not support positions.")

if __name__ == "__main__":
    import sys
    import argparse
    import redis.asyncio as redis
    import json

    async def test_bitfinex_connector(symbols=None):
        if symbols is None:
            symbols = ["ETH/USDT", "BTC/USDT"]
        print("\n=== BitfinexConnector Public Market Data Test ===\n")
        config = {"name": "bitfinex"}
        connector = BitfinexConnector(config)
        connected = await connector.connect()
        if not connected:
            print("Failed to connect to Bitfinex.")
            return
        for symbol in symbols:
            print(f"\n--- Testing symbol: {symbol} ---")
            try:
                orderbook = await connector.get_orderbook(symbol, limit=10)
                if orderbook:
                    print(f"Orderbook bids (top 3): {orderbook['bids'][:3]}")
                    print(f"Orderbook asks (top 3): {orderbook['asks'][:3]}")
                else:
                    print("No orderbook data returned.")
            except Exception as e:
                print(f"Error fetching orderbook for {symbol}: {e}")
            try:
                trades = await connector.get_trade_history(symbol=symbol, limit=5)
                print(f"Recent trades (count={len(trades)}):")
                for t in trades:
                    print(f"  {t}")
            except Exception as e:
                print(f"Error fetching trades for {symbol}: {e}")
        await connector.disconnect()
        print("\n=== BitfinexConnector Test Complete ===\n")

    async def test_watch_trades(symbol="ETH/USDT", n=10):
        print(f"\n=== BitfinexConnector Live Trade Stream Test for {symbol} ===\n")
        connector = BitfinexConnector({"name": "bitfinex"})
        await connector.connect()
        count = 0
        try:
            async for trade in connector.watch_trades(symbol):
                print(f"[{count+1}] {trade}")
                count += 1
                if count >= n:
                    break
        except Exception as e:
            print(f"Error during watch_trades: {e}")
        finally:
            await connector.disconnect()
            print("\n=== BitfinexConnector Live Trade Stream Test Complete ===\n")
            
    async def test_trading(symbol="ETH/USDT"):
        print(f"\n=== BitfinexConnector Trading Test for {symbol} ===\n")
        
        # Connect to Redis to check order keys
        redis_client = redis.from_url("redis://localhost:6379")
        
        # Initialize connector with credentials
        config = {
            "name": "bitfinex",
            "api_key": input("Enter Bitfinex API Key: "),
            "secret": input("Enter Bitfinex Secret: ")
        }
        
        connector = BitfinexConnector(config)
        await connector.connect()
        
        try:
            # 1. Check balance before trading
            balance = await connector.get_balance()
            print(f"Initial Balance: {balance}")
            
            # 2. Place market buy order for 0.01 ETH
            print("\n--- Placing market buy order ---")
            market_buy = await connector.place_order(
                symbol=symbol,
                side="buy",
                amount=Decimal("0.01"),
                order_type="market"
            )
            print(f"Market Buy Order: {market_buy['id']}")
            
            # Check Redis for order key
            order_key = f"order:bitfinex:{market_buy['id']}"
            order_exists = await redis_client.exists(order_key)
            print(f"Redis Key: {order_key} (Exists: {order_exists})")
            
            # Wait for order to complete
            print("Waiting for market buy to complete...")
            await asyncio.sleep(5)
            
            # 3. Place market sell order for 0.01 ETH
            print("\n--- Placing market sell order ---")
            market_sell = await connector.place_order(
                symbol=symbol,
                side="sell",
                amount=Decimal("0.01"),
                order_type="market"
            )
            print(f"Market Sell Order: {market_sell['id']}")
            
            # Check Redis for order key
            order_key = f"order:bitfinex:{market_sell['id']}"
            order_exists = await redis_client.exists(order_key)
            print(f"Redis Key: {order_key} (Exists: {order_exists})")
            
            # Wait for order to complete
            print("Waiting for market sell to complete...")
            await asyncio.sleep(5)
            
            # 4. Place limit order at $3500
            print("\n--- Placing limit buy order at $3500 ---")
            limit_order = await connector.place_order(
                symbol=symbol,
                side="buy",
                amount=Decimal("0.01"),
                price=Decimal("3500"),
                order_type="limit"
            )
            print(f"Limit Order: {limit_order['id']}")
            
            # Check Redis for order key
            order_key = f"order:bitfinex:{limit_order['id']}"
            order_exists = await redis_client.exists(order_key)
            print(f"Redis Key: {order_key} (Exists: {order_exists})")
            
            # Wait 3 seconds
            print("Waiting 3 seconds before canceling...")
            await asyncio.sleep(3)
            
            # 5. Cancel limit order
            print("\n--- Canceling limit order ---")
            cancel_result = await connector.cancel_order(limit_order['id'], symbol)
            print(f"Cancel Result: {cancel_result}")
            
            # Check final balance
            balance = await connector.get_balance()
            print(f"\nFinal Balance: {balance}")
            
        except Exception as e:
            print(f"Error during trading test: {e}")
        finally:
            await connector.disconnect()
            await redis_client.close()
            print("\n=== BitfinexConnector Trading Test Complete ===\n")

    parser = argparse.ArgumentParser(description="Test BitfinexConnector")
    parser.add_argument('--symbols', nargs='+', help='Symbols to test (default: ETH/USDT BTC/USDT)')
    parser.add_argument('--trading', action='store_true', help='Run trading tests (requires API keys)')
    parser.add_argument('--symbol', type=str, default='ETH/USDT', help='Symbol to test with')
    parser.add_argument('--n', type=int, default=10, help='Number of trades to print')
    
    args = parser.parse_args()
    
    if args.trading:
        asyncio.run(test_trading(args.symbol))
    else:
        # Run the public endpoint tests
        asyncio.run(test_bitfinex_connector(args.symbols))
        asyncio.run(test_watch_trades(args.symbol, args.n))