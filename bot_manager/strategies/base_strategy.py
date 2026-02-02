"""
Base trading strategy class.

All trading strategies should inherit from this base class.
"""

import asyncio
import time
import random
import subprocess
import sys
import os
import redis.asyncio as redis
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Union
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import structlog

from exchanges.base_connector import OrderType, OrderSide, OrderStatus
from exchanges.websocket import get_connection_pool

logger = structlog.get_logger(__name__)


class BaseStrategy(ABC):
    """
    Base class for all trading strategies.
    
    Provides common functionality for order management, position tracking,
    exchange interactions, and automatic WebSocket trade monitoring.
    """
    
    def __init__(
        self,
        instance_id: str,
        symbol: str,
        exchanges: List[str],
        config: Dict[str, Any]
    ):
        self.instance_id = instance_id
        self.symbol = symbol
        self.exchanges = exchanges
        self.config = config
        self.running = False
        self.logger = logger.bind(
            strategy=self.__class__.__name__,
            instance_id=instance_id,
            symbol=symbol
        )
        
        # Exchange connectors will be injected by the bot manager
        self.exchange_connectors: Dict[str, Any] = {}
        
        # WebSocket connection pool for real-time trade monitoring
        self.websocket_pool = get_connection_pool()
        self._websocket_subscribed = False
        
        # Order tracking
        self.active_orders: Dict[str, Dict[str, Any]] = {}
        self.order_history: List[Dict[str, Any]] = []
        # Simple per-exchange rate limiter: exchange -> deque of placement timestamps (seconds)
        from collections import deque
        self._order_rate_windows: Dict[str, deque] = {}
        # Exchange-specific rate limits (new orders per window_seconds)
        self._order_rate_limits = {
            'binance': {'limit': 100, 'window_seconds': 10},
        }
        # Minimum seconds between replace of the same (exchange,symbol,side) to avoid churn
        self._min_replace_interval_seconds = 1.0
        # Track last replace time: (exchange,symbol,side) -> timestamp
        self._last_replace_ts: Dict[tuple, float] = {}
        
        # Performance tracking
        self.start_time: Optional[datetime] = None  # User-configured start time (for inventory calculations)
        self.runtime_start_time: Optional[datetime] = None  # Actual runtime start time (for performance tracking)
        self.total_trades = 0
        self.total_volume = 0.0
        self.total_pnl = 0.0
        
    async def initialize(self) -> None:
        """Initialize the strategy. Called before start()."""
        self.logger.info("Initializing strategy")
        await self._validate_config()
        await self._initialize_exchange_connections()
        
    async def start(self) -> None:
        """Start the trading strategy."""
        if self.running:
            self.logger.warning("Strategy already running")
            return
            
        self.logger.info("Starting strategy")
        self.running = True
        self.runtime_start_time = datetime.now(timezone.utc)  # Track when the strategy actually started running
        
        try:
            # Ensure aggregator is running before anything else
            await self.ensure_aggregator_running()
            
            # Start WebSocket connections for real-time trade monitoring
            await self._start_websocket_monitoring()
            
            # Start strategy implementation
            await self._start_strategy()
            
        except Exception as e:
            self.logger.error(f"Error starting strategy: {e}")
            # Add full traceback for debugging
            import traceback
            self.logger.error(f"Strategy startup traceback:\n{traceback.format_exc()}")
            self.running = False
            # Clean up WebSocket connections on error
            await self._stop_websocket_monitoring()
            raise
            
    async def stop(self) -> None:
        """Stop the trading strategy."""
        if not self.running:
            self.logger.warning("Strategy not running")
            return
            
        self.logger.info("Stopping strategy")
        self.running = False
        
        try:
            # Stop strategy implementation
            await self._stop_strategy()
            
            # Cancel all orders
            await self._cancel_all_orders()
            
            # Stop WebSocket monitoring
            await self._stop_websocket_monitoring()
            
        except Exception as e:
            self.logger.error(f"Error stopping strategy: {e}")
            
    async def _start_websocket_monitoring(self) -> None:
        """Start WebSocket connections for trade monitoring."""
        try:
            self.logger.info("Starting WebSocket trade monitoring for configured exchanges")

            # Get or initialize the global WebSocket connection pool
            from exchanges.websocket.connection_pool import get_connection_pool
            self.websocket_pool = get_connection_pool()

            # Best-effort: attach a rotating file handler to the pool's logger once
            try:
                import logging
                from logging.handlers import RotatingFileHandler
                from pathlib import Path
                pool_logger = logging.getLogger('WebSocketConnectionPool')
                if not any(isinstance(h, RotatingFileHandler) for h in pool_logger.handlers):
                    package_root = Path(__file__).resolve().parents[2]
                    logs_dir = package_root / 'logs'
                    logs_dir.mkdir(parents=True, exist_ok=True)
                    handler = RotatingFileHandler(str(logs_dir / 'ws_pool.log'), maxBytes=10*1024*1024, backupCount=3)
                    handler.setLevel(logging.INFO)
                    handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
                    pool_logger.addHandler(handler)
            except Exception:
                pass

            # Start the pool if not already running
            if not getattr(self.websocket_pool, "_running", False):
                await self.websocket_pool.start()

            # Use the connectors already injected into this strategy
            if not self.exchange_connectors:
                self.logger.warning("No exchange connectors injected; skipping WS monitoring")
                return

            self.logger.info(f"Subscribing to {len(self.exchange_connectors)} exchanges: {list(self.exchange_connectors.keys())}")

            # Subscribe this strategy to its exchanges for the configured symbol
            try:
                await self.websocket_pool.subscribe_strategy(
                    strategy_id=self.instance_id,
                    exchange_connectors=self.exchange_connectors,
                    symbols=[self.symbol]
                )
            except Exception as sub_err:
                self.logger.warning(f"WS subscribe returned error (continuing): {sub_err}")

            # Register trade callback to forward fills immediately
            self.websocket_pool.register_strategy_callback(self.instance_id, self._handle_trade_confirmation)
            self._websocket_subscribed = True

            # Log connection status (best-effort)
            status = self.websocket_pool.get_connection_status()
            self.logger.info(f"WebSocket status: {status.get('total_connections', 0)} connections, {status.get('total_strategies', 0)} strategies")
        except Exception as e:
            self.logger.error(f"Error starting WebSocket monitoring: {e}")
            self.logger.warning("Continuing strategy without WebSocket monitoring")
 
    async def _stop_websocket_monitoring(self) -> None:
        """Stop WebSocket connections for this strategy."""
        if not self._websocket_subscribed:
            return
        
        try:
            self.logger.info("Stopping WebSocket trade monitoring")
            
            # Unsubscribe strategy from WebSocket connections
            await self.websocket_pool.unsubscribe_strategy(self.instance_id)
            self._websocket_subscribed = False
            
            self.logger.info("✅ WebSocket trade monitoring stopped")
            
        except Exception as e:
            self.logger.error(f"Error stopping WebSocket monitoring: {e}")

    async def _handle_trade_confirmation(self, trade_data: Dict[str, Any]) -> None:
        """Internal callback for WS pool to forward fills to strategy handler ASAP."""
        try:
            await self.on_trade_confirmation(trade_data)
        except Exception as e:
            self.logger.error(f"Error in trade callback: {e}")
        
    @abstractmethod
    async def _start_strategy(self) -> None:
        """Start strategy implementation. Must be implemented by subclasses."""
        pass
        
    @abstractmethod
    async def _stop_strategy(self) -> None:
        """Stop strategy implementation. Must be implemented by subclasses."""
        pass
        
    @abstractmethod
    async def _validate_config(self) -> None:
        """Validate strategy configuration. Must be implemented by subclasses."""
        pass
        
    async def _initialize_exchange_connections(self) -> None:
        """Initialize exchange connections."""
        # This will be handled by the bot manager
        # For now, just log
        self.logger.info(f"Initializing connections for exchanges: {self.exchanges}")
        
    async def _cancel_all_orders(self) -> None:
        """Cancel all active orders that belong to this strategy instance."""
        self.logger.info(f"Cancelling all active orders for instance {self.instance_id}")
        
        # First, clean up any orphaned orders
        orphaned_count = self._cleanup_orphaned_orders()
        if orphaned_count > 0:
            self.logger.info(f"Cleaned up {orphaned_count} orphaned orders before cancellation")
        
        # Get only orders that belong to this instance
        instance_orders = self._get_instance_orders()
        
        if not instance_orders:
            self.logger.info("No orders belonging to this instance to cancel")
            return
        
        cancel_tasks = []
        for order_id, order_info in instance_orders.items():
            exchange = order_info.get('exchange')
            if exchange and exchange in self.exchange_connectors:
                cancel_tasks.append(self._cancel_order(order_id, exchange))
                
        if cancel_tasks:
            self.logger.info(f"Cancelling {len(cancel_tasks)} orders belonging to instance {self.instance_id}")
            results = await asyncio.gather(*cancel_tasks, return_exceptions=True)
            
            # Log results
            success_count = sum(1 for result in results if result is True)
            error_count = sum(1 for result in results if isinstance(result, Exception))
            
            self.logger.info(f"Order cancellation complete: {success_count} successful, {error_count} errors")
        
        # Clear only orders that belong to this instance
        for order_id in list(self.active_orders.keys()):
            if self._order_belongs_to_instance(order_id):
                del self.active_orders[order_id]
        
    async def _cancel_order(self, order_id: str, exchange: str) -> bool:
        """Cancel a specific order with exchange-specific symbol handling and instance verification."""
        try:
            # Verify this order belongs to this instance
            if not self._order_belongs_to_instance(order_id):
                self.logger.warning(f"Attempted to cancel order {order_id} that doesn't belong to instance {self.instance_id}")
                return False
            
            connector = self.exchange_connectors.get(exchange)
            if not connector:
                self.logger.error(f"No connector for exchange: {exchange}")
                return False
            
            # Get order info
            order_info = self.active_orders.get(order_id)
            if not order_info:
                self.logger.warning(f"Order {order_id} not found in active orders")
                return False
            
            # Use the exchange symbol from order info, or convert if needed
            exchange_symbol = order_info.get('exchange_symbol')
            if not exchange_symbol:
                self.logger.error(f"Cannot determine symbol format for {exchange}")
                return False
            
            # Log cancellation attempt
            self.logger.info(f"Cancelling order {order_id} on {exchange} (symbol: {exchange_symbol}, instance: {self.instance_id})")
            
            try:
                # Use our connector's cancel_order method with exchange-specific symbol
                cancel_result = await connector.cancel_order(
                    order_id, 
                    symbol=exchange_symbol
                )
                
                # Remove from active orders
                if order_id in self.active_orders:
                    order_info = self.active_orders.pop(order_id)
                    self.logger.info(f"✅ Successfully cancelled order {order_id} on {exchange} (instance: {self.instance_id})")
                    
                    # Add to history
                    order_info['status'] = 'cancelled'
                    order_info['cancelled_at'] = datetime.now(timezone.utc).isoformat()
                    order_info['cancel_response'] = cancel_result
                    self.order_history.append(order_info)
                
                return True
                
            except Exception as exchange_error:
                # Enhanced error handling for cancellation
                error_str = str(exchange_error).lower()
                
                # Check if the order might already be filled or cancelled
                if any(keyword in error_str for keyword in ['not found', 'invalid', 'does not exist', 'not exists', 'already', 'too late to cancel', 'order filled', '订单不存在', 'unknown order sent']):
                    # Order might already be filled/cancelled, remove from active orders
                    if order_id in self.active_orders:
                        order_info = self.active_orders.pop(order_id)
                        order_info['status'] = 'filled_or_cancelled'
                        order_info['cancelled_at'] = datetime.now(timezone.utc).isoformat()
                        order_info['cancel_error'] = str(exchange_error)
                        self.order_history.append(order_info)
                        self.logger.info(f"✅ Order {order_id} appears to be already filled/cancelled on {exchange} (instance: {self.instance_id})")
                        return True
                
                # Log specific error types
                if any(keyword in error_str for keyword in ['symbol', 'market', 'pair']):
                    self.logger.error(f"❌ Symbol error cancelling order on {exchange}: {exchange_error}")
                    self.logger.info(f"Used symbol: {exchange_symbol} for order {order_id}")
                else:
                    self.logger.error(f"❌ Failed to cancel order {order_id} on {exchange}: {exchange_error}")
                
                return False
            
        except Exception as e:
            self.logger.error(f"Error cancelling order {order_id} on {exchange}: {e}")
            return False

    def _convert_side_for_exchange(self, side: str) -> str:
        """Convert internal side names to exchange-compatible format."""
        side_mapping = {
            'bid': 'buy',
            'ask': 'sell',
            'buy': 'buy',
            'sell': 'sell'
        }
        return side_mapping.get(side.lower(), side.lower())

    def _normalize_symbol_for_exchange(self, symbol: str, exchange: str) -> str:
        """Convert symbol to exchange-specific format based on actual test evidence."""
        # Remove exchange suffix from exchange name
        base_exchange = exchange.split('_')[0]
        
        # Handle special cases first
        if symbol == 'BERA/USDT':
            if base_exchange == 'hyperliquid':
                # Hyperliquid uses BERA/USDC:USDC, not BERA/USDT
                return 'BERA/USDC:USDC'
        
        # For perpetual futures, convert to proper symbol format
        if 'perp' in exchange:
            if base_exchange in ['binance', 'bybit']:
                # For Binance/Bybit futures: BERA/USDT becomes BERAUSDT (not BERAUSDTUSDT)
                if symbol == 'BERA/USDT':
                    if base_exchange == 'binance':
                        return 'BERAUSDT'  # Binance futures: remove slashes
                    elif base_exchange == 'bybit':
                        return 'BERAUSDT'  # Bybit futures: remove slashes
            elif base_exchange == 'hyperliquid':
                # Hyperliquid uses the full symbol as-is for futures
                return symbol
        
        # Now apply exchange-specific formatting for spot
        if base_exchange == 'binance':
            # Binance spot: Remove slashes -> BERAUSDT
            return symbol.replace('/', '').replace(':USDT', '').replace(':USDC', '')
        elif base_exchange == 'bybit':
            if 'spot' in exchange:
                # Bybit spot: Keep standard format -> BERA/USDT
                return symbol.replace(':USDT', '').replace(':USDC', '')
            else:
                # Bybit perp: handled above
                return symbol.replace('/', '').replace(':USDT', '').replace(':USDC', '')
        elif base_exchange == 'hyperliquid':
            # For spot, XAUT maps to UXAUT and USDT maps to USDC
            if 'spot' in exchange:
                try:
                    base, quote = symbol.split('/')
                except ValueError:
                    base, quote = symbol, 'USDT0'
                # Prefer listed symbols from the connector if available
                connector = self.exchange_connectors.get(exchange)
                ex = getattr(connector, 'exchange', None) if connector else None
                symbols = set(getattr(ex, 'symbols', []) or [])
                base_variants = (
                    ['XAUT0', 'UXAUT', 'UXAUt', 'XAUT', 'XAUt']
                    if base.upper() == 'XAUT' else [base, base.upper(), base.capitalize()]
                )
                if 'XAU' in base.upper():
                    desired_quote = 'USDT0'
                else:
                    desired_quote = 'USDC' if quote.upper() in ('USDT', 'USD', 'USDC') else quote
                for b in base_variants:
                    candidate = f"{b}/{desired_quote}"
                    if candidate in symbols:
                        return candidate
                # Fallback formatting if we didn't find a listed one
                if base.upper() == 'XAUT':
                    base = 'UXAUT'
                if 'XAU' in base.upper():
                    quote = 'USDT0'
                elif quote.upper() in ('USDT', 'USD'):
                    quote = 'USDC'
                return f"{base}/{quote}"
            # Futures handled elsewhere (generally <BASE>/USDC:USDC)
            if 'perp' in exchange:
                try:
                    base, quote = symbol.split('/')
                except ValueError:
                    base, quote = symbol, 'USDC'
                # Ensure :USDC suffix for hyperliquid perp when quoting in USDC
                if quote.upper() == 'USDC' and ':USDC' not in symbol:
                    return f"{base}/USDC:USDC"
            return symbol
        elif base_exchange == 'gateio':
            # Gate.io: Replace slash with underscore -> BERA_USDT
            return symbol.replace('/', '_').replace(':USDT', '').replace(':USDC', '')
        elif base_exchange == 'mexc':
            # MEXC: Keep standard format -> BERA/USDT
            return symbol.replace(':USDT', '').replace(':USDC', '')
        elif base_exchange == 'bitget':
            # Bitget: Keep standard format -> BERA/USDT
            return symbol.replace(':USDT', '').replace(':USDC', '')
        elif base_exchange == 'kucoin':
            # KuCoin: Replace slash with dash -> FUEL-USDT
            return symbol.replace('/', '-').replace(':USDT', '').replace(':USDC', '')
        elif base_exchange == 'bitfinex':
            # Prefer an actual listed market among USDt, USDT, then USD
            try:
                base, _ = symbol.split('/')
            except ValueError:
                base = symbol
            candidates = [
                f"{base}/USDt",
                f"{base}/USDT",
                f"{base}/USD",
            ]
            connector = self.exchange_connectors.get(exchange)
            ex = getattr(connector, 'exchange', None) if connector else None
            listed = set(getattr(ex, 'symbols', []) or [])
            for cand in candidates:
                if cand in listed:
                    return cand
            # Fallback to USDt to enforce tether quote when unknown
            return f"{base}/USDt"
        else:
            # Default: standard format
            return symbol

    def _generate_bitfinex_numeric_cid(self, client_order_id: Optional[str]) -> tuple:
        """Generate a digits-only Bitfinex v2 cid (int32) and its cidDate (UTC).

        Strategy: combine current ms timestamp, a short random segment,
        and digits from the provided client_order_id, then reduce the result
        modulo 2_147_483_647 to guarantee a signed 32-bit integer.
        We also ensure the value is not too small to reduce collision risk.
        """
        # Date portion for Bitfinex day uniqueness
        cid_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')

        # Extract digits from the external client order id, if provided
        source_digits = ''.join(ch for ch in (client_order_id or '') if ch.isdigit())
        suffix = (source_digits[-12:] if len(source_digits) >= 12 else source_digits.zfill(6))

        # Timestamp and random segment keep it unique and time-ordered
        ts_ms = int(time.time() * 1000)
        rnd = random.randint(0, 999999)
        base = f"{ts_ms}{rnd:06d}"

        # Compose a long numeric string then reduce to 32-bit signed int
        cid_str = f"{base}{suffix}"
        try:
            big_num = int(cid_str)
        except Exception:
            big_num = ts_ms * 1000000 + rnd
        cid = big_num % 2_147_483_647
        if cid < 1_000_000:  # avoid very small cids
            cid += 1_000_000
        return cid, cid_date

    def _get_exchange_specific_params(self, exchange: str, order_type: str, side: str, client_order_id: Optional[str] = None) -> dict:
        """Get exchange-specific parameters for order placement."""
        base_exchange = exchange.split('_')[0]
        params = {}
        
        if base_exchange == 'binance':
            # Binance-specific parameters
            params['newOrderRespType'] = 'RESULT'
            if 'perp' in exchange:
                params['timeInForce'] = 'GTC'  # Good Till Cancelled for futures
        elif base_exchange == 'bybit':
            # Bybit-specific parameters
            if 'perp' in exchange:
                params['category'] = 'linear'
            params['timeInForce'] = 'GTC'
        elif base_exchange == 'hyperliquid':
            # Hyperliquid-specific parameters - keep minimal
            pass
        elif base_exchange == 'gateio':
            # Gate.io-specific parameters
            if order_type == 'market' and side == 'buy':
                params['account'] = 'spot'
        elif base_exchange == 'bitget':
            # Bitget-specific parameters
            params['timeInForce'] = 'GTC'  # Good Till Cancelled
        elif base_exchange == 'mexc':
            # MEXC-specific parameters
            params['newOrderRespType'] = 'RESULT'
        elif base_exchange == 'kucoin':
            # KuCoin-specific parameters
            params['timeInForce'] = 'GTC'  # Good Till Cancelled
        elif base_exchange == 'bitfinex':
            # Bitfinex (v1 and v2): always send numeric 'cid' and 'cidDate' (v1 ignores cidDate if unsupported)
            cid, cid_date = self._generate_bitfinex_numeric_cid(client_order_id)
            params['cid'] = cid
            params['cidDate'] = cid_date
        
        return params

    def _is_symbol_supported(self, symbol: str, exchange: str) -> bool:
        """Check if symbol is supported on the exchange based on test evidence."""
        # Based on the test files, BERA is supported on all these exchanges
        # The only special case is Hyperliquid which uses BERA/USDC:USDC instead of BERA/USDT
        return True  # All exchanges in our test files support BERA with proper symbol conversion

    async def _place_order(
        self,
        exchange: str,
        side: str,
        amount: float,
        price: Optional[float],
        order_type: str = OrderType.LIMIT.value,
        client_order_id: Optional[str] = None
    ) -> Optional[str]:
        """Place an order on the specified exchange with comprehensive exchange-specific handling."""
        try:
            # Enforce per-exchange rate limit
            if not self._check_and_consume_rate_limit(exchange):
                self.logger.warning(f"Rate limit prevented order placement on {exchange}")
                return None
            # Convert symbol for exchange
            exchange_symbol = self._normalize_symbol_for_exchange(self.symbol, exchange)
            if not exchange_symbol:
                self.logger.error(f"Cannot normalize symbol {self.symbol} for {exchange} (no listed market)")
                return None

            # Generate client order ID if not provided
            if not client_order_id:
                timestamp = int(time.time() * 1000) % 1000000  # Use last 6 digits
                base_exchange = exchange.split('_')[0]
                
                if base_exchange == 'gateio':
                    # Gate.io requires client order IDs to start with 't-'
                    client_order_id = f"t-{self.__class__.__name__.lower()[:3]}_{side[:1]}_{timestamp}"
                else:
                    # Standard format for other exchanges
                    client_order_id = f"{self.__class__.__name__.lower()[:3]}_{side[:1]}_{base_exchange[:3]}_{timestamp}"

            # Get exchange-specific parameters
            exchange_params = self._get_exchange_specific_params(exchange, order_type, side, client_order_id)
            if client_order_id and exchange.split('_')[0] not in ('bitfinex',):
                # Include clientOrderId for all exchanges including Hyperliquid (now has proper encoding)
                exchange_params['clientOrderId'] = client_order_id

            # Validate order size and price
            validated_amount, validated_price = self._validate_order_size(exchange, exchange_symbol, amount, price)

            # Log full outgoing order details including exchange params
            try:
                price_str = 'market' if validated_price is None or order_type.lower() == 'market' else f"{validated_price:.8f}"
                self.logger.info(
                    f"➡️ OUTGOING ORDER -> exchange={exchange}, symbol={exchange_symbol}, side={side}, "
                    f"amount={validated_amount:.8f}, price={price_str}, type={order_type}, params={exchange_params}"
                )
            except Exception:
                # Fallback minimal log if formatting fails
                self.logger.info(
                    f"➡️ OUTGOING ORDER -> exchange={exchange}, symbol={exchange_symbol}, side={side}, type={order_type}"
                )

            # Ensure connector is available after logging the intent
            connector = self.exchange_connectors.get(exchange)
            if not connector:
                self.logger.error(
                    f"No connector available for {exchange} (attempted order already logged above)"
                )
                return None

            # Log order placement (handle market orders without price)
            if validated_price is None or order_type.lower() == 'market':
                self.logger.info(
                    f"Placing {side} order on {exchange}: {validated_amount:.2f} {exchange_symbol} @ market (client_order_id: {client_order_id})"
                )
            else:
                self.logger.info(
                    f"Placing {side} order on {exchange}: {validated_amount:.2f} {exchange_symbol} @ {validated_price:.4f} (client_order_id: {client_order_id})"
                )

            # Place order with balance-aware retry on insufficient funds
            try:
                order_result = await connector.place_order(
                    symbol=exchange_symbol,
                    side=side,
                    amount=Decimal(str(validated_amount)),
                    price=Decimal(str(validated_price)) if validated_price is not None else None,
                    order_type=order_type,
                    params=exchange_params
                )
            except Exception as first_err:
                err_str = str(first_err).lower()
                # If we hit exchange 429 rate limit, do not retry immediately
                if 'too many' in err_str or '429' in err_str or 'rate limit' in err_str:
                    self.logger.warning(f"Rate limited on {exchange}: {first_err}")
                    return None
                if 'not enough' in err_str and 'balance' in err_str:
                    # Try reducing size to available balance and retry once
                    try:
                        base, quote = (exchange_symbol.split('/') + ['USD'])[:2]
                        precision = self._get_exchange_precision(exchange, exchange_symbol)
                        min_qty = Decimal(str(precision.get('min_quantity', 0.0)))
                        qty_prec = int(precision.get('quantity_precision', 8))

                        adjusted_amount: Optional[Decimal] = None
                        if side.lower() == 'buy':
                            # Use available quote balance / price
                            bal = await connector.get_balance(quote)
                            available_quote = Decimal(str(bal.get(quote, 0)))
                            if validated_price and available_quote > 0:
                                max_amt = available_quote / Decimal(str(validated_price))
                                if max_amt > 0:
                                    adjusted_amount = max_amt
                        else:
                            # Use available base balance directly
                            bal = await connector.get_balance(base)
                            available_base = Decimal(str(bal.get(base, 0)))
                            if available_base > 0:
                                adjusted_amount = available_base

                        if adjusted_amount is not None:
                            # Round and enforce min quantity
                            adjusted_amount = Decimal(str(self._round_to_precision(float(adjusted_amount), qty_prec)))
                            if adjusted_amount < min_qty:
                                self.logger.error(
                                    f"Insufficient balance to meet min quantity on {exchange}: available -> {adjusted_amount}, min -> {min_qty}"
                                )
                                raise first_err

                            # Regenerate params (new cid for Bitfinex, etc.)
                            retry_params = self._get_exchange_specific_params(exchange, order_type, side, client_order_id)

                            # Log adjusted outgoing order
                            try:
                                price_str = 'market' if validated_price is None or order_type.lower() == 'market' else f"{validated_price:.8f}"
                                self.logger.info(
                                    f"↩️ RETRY OUTGOING ORDER (adjusted) -> exchange={exchange}, symbol={exchange_symbol}, side={side}, "
                                    f"amount={float(adjusted_amount):.8f}, price={price_str}, type={order_type}, params={retry_params}"
                                )
                            except Exception:
                                self.logger.info(
                                    f"↩️ RETRY OUTGOING ORDER (adjusted) -> exchange={exchange}, symbol={exchange_symbol}, side={side}, type={order_type}"
                                )

                            # Retry placement with adjusted size
                            order_result = await connector.place_order(
                                symbol=exchange_symbol,
                                side=side,
                                amount=Decimal(str(adjusted_amount)),
                                price=Decimal(str(validated_price)) if validated_price is not None else None,
                                order_type=order_type,
                                params=retry_params
                            )
                        else:
                            raise first_err
                    except Exception:
                        # Bubble up the original error to be logged by outer handler
                        raise first_err
                else:
                    # Not a balance error
                    raise first_err

            if order_result and 'id' in order_result:
                order_id = order_result.get('id') or order_result.get('clientOrderId') or client_order_id
                
                # Store order info with INSTANCE OWNERSHIP
                order_info = {
                    'id': order_id,
                    'instance_id': self.instance_id,  # ✅ CRITICAL: Track which instance placed this order
                    'exchange': exchange,
                    'exchange_symbol': exchange_symbol,
                    'side': side,
                    'amount': validated_amount,
                    'price': validated_price,
                    'status': 'open',
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'client_order_id': client_order_id,
                    'order_result': order_result
                }
                
                self.active_orders[order_id] = order_info
                
                if validated_price is None or order_type.lower() == 'market':
                    self.logger.info(
                        f"✅ Successfully placed {side} order on {exchange}: {validated_amount:.2f} {exchange_symbol} @ market (Order ID: {order_id}, Instance: {self.instance_id})"
                    )
                else:
                    self.logger.info(
                        f"✅ Successfully placed {side} order on {exchange}: {validated_amount:.2f} {exchange_symbol} @ {validated_price:.4f} (Order ID: {order_id}, Instance: {self.instance_id})"
                    )
                
                return order_id
            else:
                self.logger.error(f"❌ Failed to place order on {exchange}: Invalid response")
                return None
                
        except Exception as e:
            self.logger.error(f"Error placing order on {exchange}: {e}")
            return None

    def _check_and_consume_rate_limit(self, exchange: str) -> bool:
        """Return True if allowed to place a new order now; updates window state."""
        import time
        base = exchange.split('_')[0]
        cfg = self._order_rate_limits.get(base)
        if not cfg:
            return True
        limit = cfg['limit']
        window = cfg['window_seconds']
        now = time.time()
        dq = self._order_rate_windows.setdefault(base, __import__('collections').deque())
        # Drop old timestamps
        while dq and now - dq[0] > window:
            dq.popleft()
        if len(dq) >= limit:
            return False
        dq.append(now)
        return True
            
    def get_active_orders_count(self) -> int:
        """Get count of active orders."""
        return len(self.active_orders)
        
    def get_inventory_start_time(self) -> datetime:
        """
        Get the start time to use for inventory calculations.
        
        This returns the user-configured start time if available (from strategy config),
        otherwise falls back to the runtime start time.
        
        Strategies that need historical inventory tracking should parse their
        start_time from config and set self.start_time during initialization.
        
        Returns:
            datetime: The start time to use for inventory calculations
        """
        if self.start_time:
            return self.start_time
        elif self.runtime_start_time:
            return self.runtime_start_time
        else:
            # Fallback to current time if neither is set
            return datetime.now(timezone.utc)
        
    def _order_belongs_to_instance(self, order_id: str) -> bool:
        """
        Check if an order belongs to this strategy instance.
        
        Args:
            order_id: The order ID to check
            
        Returns:
            True if the order belongs to this instance, False otherwise
        """
        if order_id not in self.active_orders:
            return False
        
        order_info = self.active_orders[order_id]
        order_instance_id = order_info.get('instance_id')
        
        # If no instance_id in order (legacy orders), assume it belongs to this instance
        if order_instance_id is None:
            self.logger.warning(f"Order {order_id} has no instance_id, assuming ownership")
            return True
        
        return order_instance_id == self.instance_id
    
    def _get_instance_orders(self) -> Dict[str, Dict[str, Any]]:
        """
        Get only the orders that belong to this strategy instance.
        
        Returns:
            Dictionary of orders that belong to this instance
        """
        instance_orders = {}
        for order_id, order_info in self.active_orders.items():
            if self._order_belongs_to_instance(order_id):
                instance_orders[order_id] = order_info
        return instance_orders
    
    def _cleanup_orphaned_orders(self) -> int:
        """
        Remove orders that don't belong to this strategy instance.
        This can happen if orders from other instances somehow end up in this instance's tracking.
        
        Returns:
            Number of orphaned orders removed
        """
        orphaned_orders = []
        
        for order_id, order_info in self.active_orders.items():
            if not self._order_belongs_to_instance(order_id):
                orphaned_orders.append(order_id)
        
        for order_id in orphaned_orders:
            order_info = self.active_orders.pop(order_id)
            self.logger.warning(f"Removed orphaned order {order_id} (belongs to instance {order_info.get('instance_id', 'unknown')}, not {self.instance_id})")
        
        if orphaned_orders:
            self.logger.info(f"Cleaned up {len(orphaned_orders)} orphaned orders")
        
        return len(orphaned_orders)
        
    async def on_trade_confirmation(self, trade_data: Dict[str, Any]) -> None:
        """
        Base trade confirmation handler with instance ownership verification.
        
        This method should be overridden by specific strategies, but this base implementation
        provides the core instance checking logic to prevent cross-instance contamination.
        
        Args:
            trade_data: Trade data from WebSocket connection pool
        """
        try:
            order_id = trade_data.get('order_id')
            if not order_id:
                self.logger.debug("Trade confirmation received but no order_id found")
                return
            
            # CRITICAL: Only process trades for orders that belong to this instance
            if not self._order_belongs_to_instance(order_id):
                self.logger.debug(f"Trade confirmation for order {order_id} ignored - doesn't belong to instance {self.instance_id}")
                return
            
            # If we get here, the order belongs to this instance
            order_info = self.active_orders.get(order_id)
            if not order_info:
                self.logger.warning(f"Trade confirmation for order {order_id} but order not found in active_orders")
                return
            
            trade_amount = trade_data.get('amount', 0)
            trade_price = trade_data.get('price', 0)
            
            self.logger.info(f"✅ Trade confirmed for instance {self.instance_id}: order {order_id}, amount {trade_amount}, price {trade_price}")
            
            # Track partial fills and only remove once fully filled
            try:
                filled_amount = float(order_info.get('filled_amount', 0.0))
                original_amount = float(order_info.get('amount', 0.0))
                fill_amount = float(trade_amount or 0.0)
            except Exception:
                filled_amount = 0.0
                original_amount = 0.0
                fill_amount = 0.0

            if fill_amount <= 0 or original_amount <= 0:
                return

            filled_amount += fill_amount
            remaining_amount = original_amount - filled_amount

            order_info['filled_amount'] = filled_amount
            order_info['remaining_amount'] = remaining_amount
            order_info['last_fill_amount'] = fill_amount
            order_info['last_fill_price'] = trade_price
            order_info['last_fill_at'] = datetime.now(timezone.utc).isoformat()

            # Determine minimum remaining size to treat as filled
            min_remaining = 0.0
            try:
                exchange = order_info.get('exchange')
                symbol = order_info.get('symbol')
                if exchange and symbol:
                    min_remaining = float(self._get_exchange_precision(exchange, symbol).get('min_quantity', 0.0))
            except Exception:
                min_remaining = 0.0

            if remaining_amount <= max(min_remaining, 0.0):
            # Update order status and move to history
            order_info['status'] = 'filled'
            order_info['filled_at'] = datetime.now(timezone.utc).isoformat()
            order_info['fill_amount'] = trade_amount
            order_info['fill_price'] = trade_price
            
            # Move to order history and remove from active orders
            self.order_history.append(order_info)
            del self.active_orders[order_id]
            else:
                # Keep tracking until fully filled
                order_info['status'] = 'partial'
            
        except Exception as e:
            self.logger.error(f"Error in base trade confirmation handler: {e}")
        
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get performance statistics."""
        uptime = (
            (datetime.now(timezone.utc) - self.runtime_start_time).total_seconds()
            if self.runtime_start_time else 0
        )
        
        return {
            'uptime_seconds': uptime,
            'total_trades': self.total_trades,
            'total_volume': self.total_volume,
            'total_pnl': self.total_pnl,
            'active_orders': len(self.active_orders),
            'total_orders': len(self.order_history) + len(self.active_orders)
        }
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert strategy state to dictionary."""
        return {
            'instance_id': self.instance_id,
            'strategy': self.__class__.__name__,
            'symbol': self.symbol,
            'exchanges': self.exchanges,
            'config': self.config,
            'running': self.running,
            'performance': self.get_performance_stats(),
            'active_orders_count': len(self.active_orders),
            'active_orders': list(self.active_orders.values())
        }

    def _get_exchange_precision(self, exchange: str, symbol: str) -> dict:
        """Get exchange-specific precision requirements for symbol."""
        base_exchange = exchange.split('_')[0]
        
        # Default precision values - should be fetched from exchange market info in production
        default_precision = {
            'price_precision': 6,
            'quantity_precision': 8,
            'min_quantity': 0.001,
            'min_notional': 5.0  # $5 minimum order value
        }
        
        # Exchange-specific overrides for FUEL/USDT
        if symbol == 'FUEL/USDT':
            if base_exchange == 'bybit':
                return {
                    'price_precision': 6,
                    'quantity_precision': 2,
                    'min_quantity': 1.0,
                    'min_notional': 5.0
                }
            elif base_exchange == 'mexc':
                return {
                    'price_precision': 6,
                    'quantity_precision': 2,
                    'min_quantity': 1.0,
                    'min_notional': 5.0
                }
            elif base_exchange == 'gateio':
                return {
                    'price_precision': 6,
                    'quantity_precision': 2,
                    'min_quantity': 1.0,
                    'min_notional': 5.0
                }
            elif base_exchange == 'bitget':
                return {
                    'price_precision': 6,
                    'quantity_precision': 2,
                    'min_quantity': 1.0,
                    'min_notional': 5.0
                }
            elif base_exchange == 'kucoin':
                return {
                    'price_precision': 6,
                    'quantity_precision': 2,
                    'min_quantity': 1.0,
                    'min_notional': 5.0
                }
        
        # Exchange-specific overrides for BERA (legacy)
        if symbol == 'BERA/USDT':
            if base_exchange == 'binance':
                return {
                    'price_precision': 4,  # 4 decimal places for BERA price
                    'quantity_precision': 2,  # 2 decimal places for BERA quantity
                    'min_quantity': 1.0,  # Minimum 1 BERA
                    'min_notional': 5.0  # $5 minimum
                }
            elif base_exchange == 'bybit':
                return {
                    'price_precision': 4,
                    'quantity_precision': 2,
                    'min_quantity': 0.1,
                    'min_notional': 1.0
                }
            elif base_exchange == 'mexc':
                return {
                    'price_precision': 4,
                    'quantity_precision': 2,
                    'min_quantity': 0.1,
                    'min_notional': 1.0
                }
            elif base_exchange == 'gateio':
                return {
                    'price_precision': 4,
                    'quantity_precision': 2,
                    'min_quantity': 0.1,
                    'min_notional': 1.0
                }
            elif base_exchange == 'bitget':
                return {
                    'price_precision': 4,
                    'quantity_precision': 2,
                    'min_quantity': 0.1,
                    'min_notional': 1.0
                }
            elif base_exchange == 'hyperliquid':
                return {
                    'price_precision': 4,
                    'quantity_precision': 2,
                    'min_quantity': 0.1,
                    'min_notional': 10.0  # Updated: Hyperliquid requires $10 minimum
                }
        
        return default_precision

    def _round_to_precision(self, value: float, precision: int) -> float:
        """Round value to specified decimal places."""
        if precision <= 0:
            return round(value)
        return round(value, precision)

    def _validate_order_size(self, exchange: str, symbol: str, quantity: float, price: Optional[float]) -> tuple:
        """Validate and adjust order size according to exchange requirements."""
        precision = self._get_exchange_precision(exchange, symbol)
        
        # Round quantity to exchange precision
        rounded_quantity = self._round_to_precision(quantity, precision['quantity_precision'])
        
        # If market order (price is None), skip price rounding and notional checks
        if price is None:
            # Ensure minimum quantity
            if rounded_quantity < precision['min_quantity']:
                self.logger.warning(
                    f"Quantity {rounded_quantity} below minimum {precision['min_quantity']} for {exchange} (market). Adjusting."
                )
                rounded_quantity = precision['min_quantity']
            return rounded_quantity, None
        
        # Round price to exchange precision for limit orders
        rounded_price = self._round_to_precision(price, precision['price_precision'])
        
        # Check minimum quantity
        if rounded_quantity < precision['min_quantity']:
            self.logger.warning(
                f"Quantity {rounded_quantity} below minimum {precision['min_quantity']} for {exchange}"
            )
            rounded_quantity = precision['min_quantity']
        
        # Check minimum notional value
        notional_value = rounded_quantity * rounded_price
        if notional_value < precision['min_notional']:
            # Adjust quantity to meet minimum notional
            required_quantity = precision['min_notional'] / rounded_price
            rounded_quantity = self._round_to_precision(required_quantity, precision['quantity_precision'])
            
            # Ensure we meet minimum quantity after adjustment
            if rounded_quantity < precision['min_quantity']:
                rounded_quantity = precision['min_quantity']
            
            self.logger.info(
                f"Adjusted quantity to {rounded_quantity} to meet ${precision['min_notional']} minimum notional on {exchange}"
            )
        
        return rounded_quantity, rounded_price

    async def ensure_aggregator_running(self):
        """
        Ensure the Redis aggregator service is running. If not, start it as a subprocess.
        Uses a Redis heartbeat key set by the aggregator every 5 seconds.
        """
        redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
        r = redis.from_url(redis_url)
        try:
            heartbeat = await r.get("aggregator_service:heartbeat")
            if heartbeat:
                self.logger.info("Aggregator service heartbeat detected.")
                return
        except Exception as e:
            self.logger.warning(f"Could not check aggregator heartbeat: {e}")
        # If not running, start as subprocess
        self.logger.info("Aggregator service not detected, starting it now...")
        try:
            from pathlib import Path
            package_root = Path(__file__).resolve().parents[2]
            stdout_log = open(package_root / "aggregator_stdout.log", "ab", buffering=0)
            stderr_log = open(package_root / "aggregator_stderr.log", "ab", buffering=0)
            subprocess.Popen(
                [sys.executable, "-m", "market_data.aggregator_service"],
                cwd=str(package_root),
                stdout=stdout_log,
                stderr=stderr_log
            )
        except Exception as e:
            # Fallback to package-qualified path
            self.logger.warning(f"Failed to start aggregator with local module path: {e}. Trying package-qualified path...")
            subprocess.Popen([sys.executable, "-m", "trading_bot_system.market_data.aggregator_service"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        # Wait a moment for it to start
        for _ in range(30):  # up to ~15s
            try:
                heartbeat = await r.get("aggregator_service:heartbeat")
                if heartbeat:
                    self.logger.info("Aggregator service started successfully.")
                    return
            except Exception:
                pass
            await asyncio.sleep(0.5)
        self.logger.warning("Aggregator service may not have started correctly.")
