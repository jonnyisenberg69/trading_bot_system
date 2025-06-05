"""
Passive quoting trading strategy.

This strategy places passive orders at specified spreads around the average midpoint.
Uses WebSocket orderbook feeds for real-time market data.
"""

import asyncio
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Any
from decimal import Decimal

import structlog

from .base_strategy import BaseStrategy
from exchanges.websocket.ws_manager import WebSocketManager

logger = structlog.get_logger(__name__)


class QuoteSide(str, Enum):
    """Quote side options."""
    BOTH = "both"
    BID = "bid"
    OFFER = "offer"


class QuantityCurrency(str, Enum):
    """Quantity currency options."""
    BASE = "base"
    QUOTE = "quote"


@dataclass
class QuoteLine:
    """Represents a single quote line configuration."""
    line_id: int
    timeout_seconds: int
    drift_bps: float
    quantity: float
    quantity_randomization_factor: float
    spread_bps: float
    sides: QuoteSide
    
    # Runtime state - track orders per exchange
    bid_orders: Dict[str, str] = field(default_factory=dict)  # exchange -> order_id
    ask_orders: Dict[str, str] = field(default_factory=dict)  # exchange -> order_id
    last_midpoint: Optional[Decimal] = None
    bid_placed_at: Optional[datetime] = None
    ask_placed_at: Optional[datetime] = None
    bid_placed_price: Optional[Decimal] = None
    ask_placed_price: Optional[Decimal] = None
    
    def get_active_bid_count(self) -> int:
        """Get count of active bid orders."""
        return len(self.bid_orders)
    
    def get_active_ask_count(self) -> int:
        """Get count of active ask orders."""
        return len(self.ask_orders)
    
    def has_bid_on_exchange(self, exchange: str) -> bool:
        """Check if there's an active bid order on this exchange."""
        return exchange in self.bid_orders
    
    def has_ask_on_exchange(self, exchange: str) -> bool:
        """Check if there's an active ask order on this exchange."""
        return exchange in self.ask_orders


class PassiveQuotingStrategy(BaseStrategy):
    """
    Passive quoting strategy using WebSocket orderbook feeds.
    
    Features:
    - Places passive orders at specified spreads
    - Cancels orders on timeout or drift
    - Uses WebSocket orderbook feeds for real-time market data
    - Supports multiple quote lines with different parameters
    """
    
    def __init__(
        self,
        instance_id: str,
        symbol: str,
        exchanges: List[str],
        config: Dict[str, Any]
    ):
        super().__init__(instance_id, symbol, exchanges, config)
        
        # Strategy-specific attributes
        self.quote_lines: List[QuoteLine] = []
        self.quantity_currency: QuantityCurrency = QuantityCurrency.BASE
        self.main_task: Optional[asyncio.Task] = None
        
        # WebSocket orderbook data cache
        self.orderbook_cache: Dict[str, Dict[str, Any]] = {}
        self.ws_managers: Dict[str, WebSocketManager] = {}
        self.ws_connections: Dict[str, str] = {}  # exchange -> connection_id
        
        # Performance tracking
        self.orders_placed = 0
        self.orders_cancelled_timeout = 0
        self.orders_cancelled_drift = 0
        
    async def _validate_config(self) -> None:
        """Validate passive quoting configuration."""
        required_fields = [
            'base_coin', 'lines', 'quantity_currency', 'exchanges'
        ]
        
        for field in required_fields:
            if field not in self.config:
                raise ValueError(f"Missing required config field: {field}")
        
        # Validate quantity currency
        qty_currency = self.config.get('quantity_currency', 'base').lower()
        if qty_currency not in ['base', 'quote']:
            raise ValueError(f"Invalid quantity_currency: {qty_currency}")
        self.quantity_currency = QuantityCurrency(qty_currency)
        
        # Validate lines configuration
        lines_config = self.config.get('lines', [])
        if not lines_config or not isinstance(lines_config, list):
            raise ValueError("Lines configuration must be a non-empty list")
        
        # Parse and validate each line
        self.quote_lines = []
        for i, line_config in enumerate(lines_config):
            required_line_fields = [
                'timeout', 'drift', 'quantity', 'quantity_randomization_factor',
                'spread', 'sides'
            ]
            
            for field in required_line_fields:
                if field not in line_config:
                    raise ValueError(f"Line {i}: Missing required field '{field}'")
            
            # Validate sides
            sides = line_config['sides'].lower()
            if sides not in ['both', 'bid', 'offer']:
                raise ValueError(f"Line {i}: Invalid sides '{sides}'")
            
            quote_line = QuoteLine(
                line_id=i,
                timeout_seconds=int(line_config['timeout']),
                drift_bps=float(line_config['drift']),
                quantity=float(line_config['quantity']),
                quantity_randomization_factor=float(line_config['quantity_randomization_factor']),
                spread_bps=float(line_config['spread']),
                sides=QuoteSide(sides)
            )
            
            self.quote_lines.append(quote_line)
        
        self.logger.info(f"Validated config with {len(self.quote_lines)} quote lines")
        
    async def _start_strategy(self) -> None:
        """Start the passive quoting strategy."""
        self.logger.info("Starting passive quoting strategy with WebSocket orderbook feeds")
        
        # Initialize orderbook cache for each exchange
        for exchange in self.exchanges:
            self.orderbook_cache[exchange] = {
                'bids': [],
                'asks': [],
                'best_bid': None,
                'best_ask': None,
                'midpoint': None,
                'last_update': None,
                'symbol': self.symbol
            }
        
        # Setup WebSocket connections for orderbook feeds
        await self._setup_websocket_orderbook_feeds()
        
        # Start main strategy loop
        self.main_task = asyncio.create_task(self._strategy_loop())
        
    async def _stop_strategy(self) -> None:
        """Stop the passive quoting strategy."""
        self.logger.info("Stopping passive quoting strategy")
        
        if self.main_task:
            self.main_task.cancel()
            try:
                await self.main_task
            except asyncio.CancelledError:
                pass
        
        # Close WebSocket connections
        await self._cleanup_websocket_connections()
                
    async def _setup_websocket_orderbook_feeds(self) -> None:
        """Setup WebSocket connections for orderbook feeds."""
        self.logger.info("Setting up WebSocket connections for orderbook feeds")
        
        for exchange in self.exchanges:
            try:
                connector = self.exchange_connectors.get(exchange)
                if not connector:
                    self.logger.warning(f"No connector available for {exchange}")
                    continue
                
                # Create WebSocket manager for this exchange
                ws_manager = WebSocketManager()
                await ws_manager.start()
                self.ws_managers[exchange] = ws_manager
                
                # Register orderbook message handler
                ws_manager.register_handler('orderbook', self._handle_orderbook_message)
                
                # Get public WebSocket endpoint for orderbook data
                public_endpoint = self._get_public_websocket_endpoint(exchange)
                if not public_endpoint:
                    self.logger.warning(f"No public WebSocket endpoint for {exchange}")
                    continue
                
                # Create a mock connector with proper exchange name for WebSocket manager
                mock_connector = type('MockConnector', (), {
                    'name': self._get_exchange_base_name(exchange),
                    '__class__': type('MockConnector', (), {'__name__': f'{self._get_exchange_base_name(exchange)}connector'})
                })()
                
                # Connect to public WebSocket (no authentication needed for public orderbook data)
                conn_id = await ws_manager.connect_exchange(
                    exchange=mock_connector,
                    endpoint=public_endpoint,
                    conn_type='public'
                )
                
                if conn_id:
                    self.ws_connections[exchange] = conn_id
                    
                    # Wait longer for connection to establish and add retry logic
                    max_retries = 3
                    for attempt in range(max_retries):
                        await asyncio.sleep(2)  # Increased wait time
                        
                        # Subscribe to orderbook channel
                        success = await ws_manager.subscribe(conn_id, 'orderbook', self.symbol)
                        if success:
                            self.logger.info(f"Subscribed to {exchange} orderbook feed for {self.symbol}")
                            break
                        else:
                            if attempt < max_retries - 1:
                                self.logger.warning(f"Subscription attempt {attempt + 1} failed for {exchange}, retrying...")
                                await asyncio.sleep(1)
                            else:
                                self.logger.error(f"Failed to subscribe to {exchange} orderbook feed after {max_retries} attempts")
                else:
                    self.logger.error(f"Failed to connect to {exchange} WebSocket")
                    
            except Exception as e:
                self.logger.error(f"Error setting up WebSocket for {exchange}: {e}")
                
    def _get_exchange_base_name(self, exchange: str) -> str:
        """Get the base exchange name from the full exchange identifier."""
        # Extract base name from exchange identifiers like 'binance_spot', 'bybit_perp'
        if '_' in exchange:
            return exchange.split('_')[0]
        return exchange
                
    def _get_public_websocket_endpoint(self, exchange: str) -> Optional[str]:
        """Get public WebSocket endpoint for orderbook data."""
        base_name = self._get_exchange_base_name(exchange)
        
        # Public WebSocket endpoints for orderbook data
        endpoints = {
            'binance': 'wss://stream.binance.com:9443/ws',
            'bybit': 'wss://stream.bybit.com/v5/public/spot',
            'mexc': 'wss://wbs.mexc.com/ws',
            'gateio': 'wss://api.gateio.ws/ws/v4/',
            'bitget': 'wss://ws.bitget.com/spot/v1/stream',
            'hyperliquid': 'wss://api.hyperliquid.xyz/ws'
        }
        
        return endpoints.get(base_name)
        
    async def _handle_orderbook_message(self, message: Dict[str, Any]) -> None:
        """Handle incoming orderbook WebSocket messages."""
        try:
            exchange = message.get('exchange')
            symbol = message.get('symbol')
            
            if not exchange or symbol != self.symbol:
                return
                
            # Extract orderbook data
            bids = message.get('bids', [])
            asks = message.get('asks', [])
            
            if not bids or not asks:
                return
                
            # Convert to Decimal for precision
            best_bid = Decimal(str(bids[0][0])) if bids else None
            best_ask = Decimal(str(asks[0][0])) if asks else None
            
            if best_bid and best_ask:
                midpoint = (best_bid + best_ask) / Decimal('2')
                
                # Update orderbook cache
                self.orderbook_cache[exchange] = {
                    'bids': bids,
                    'asks': asks,
                    'best_bid': best_bid,
                    'best_ask': best_ask,
                    'midpoint': midpoint,
                    'last_update': datetime.now(),
                    'symbol': symbol
                }
                
                self.logger.debug(
                    f"Updated {exchange} orderbook: {symbol} "
                    f"Bid=${best_bid:.6f} Ask=${best_ask:.6f} Mid=${midpoint:.6f}"
                )
                
        except Exception as e:
            self.logger.error(f"Error processing orderbook message: {e}")
            
    async def _cleanup_websocket_connections(self) -> None:
        """Clean up WebSocket connections."""
        for exchange, ws_manager in self.ws_managers.items():
            try:
                await ws_manager.stop()
            except Exception as e:
                self.logger.error(f"Error stopping WebSocket manager for {exchange}: {e}")
        
        self.ws_managers.clear()
        self.ws_connections.clear()
                
    async def _strategy_loop(self) -> None:
        """Main strategy loop using WebSocket orderbook data."""
        while self.running:
            try:
                # Process each quote line
                for line in self.quote_lines:
                    await self._process_quote_line(line)
                
                # Sleep before next iteration
                await asyncio.sleep(1)  # 1 second update frequency for WebSocket data
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in strategy loop: {e}")
                await asyncio.sleep(5)  # Wait before retrying
                
    async def _process_quote_line(self, line: QuoteLine) -> None:
        """Process a single quote line."""
        # Check and cancel expired orders
        await self._check_timeouts(line)
        
        # Check and cancel drifted orders
        await self._check_drift(line)
        
        # Place new orders if needed
        await self._place_missing_orders(line)
        
    async def _check_timeouts(self, line: QuoteLine) -> None:
        """Check and cancel orders that have exceeded timeout."""
        now = datetime.now()
        timeout_delta = timedelta(seconds=line.timeout_seconds)
        
        # Check bid order timeout
        if (line.bid_orders and line.bid_placed_at and 
            now - line.bid_placed_at > timeout_delta):
            
            await self._cancel_line_order(line, 'bid', 'timeout')
            
        # Check ask order timeout
        if (line.ask_orders and line.ask_placed_at and 
            now - line.ask_placed_at > timeout_delta):
            
            await self._cancel_line_order(line, 'ask', 'timeout')
            
    async def _check_drift(self, line: QuoteLine) -> None:
        """Check and cancel orders that have drifted beyond threshold."""
        if line.last_midpoint is None:
            return
            
        # Get current average midpoint across exchanges
        current_midpoint = await self._get_average_midpoint()
        if current_midpoint is None:
            return
            
        # Calculate drift in basis points
        price_change = abs(current_midpoint - line.last_midpoint)
        drift_bps = (price_change / line.last_midpoint) * Decimal('10000')
        
        if drift_bps > Decimal(str(line.drift_bps)):
            self.logger.info(
                f"Line {line.line_id}: Drift {drift_bps:.1f}bps > {line.drift_bps}bps, cancelling orders"
            )
            
            # Cancel both orders due to drift
            if line.bid_orders:
                await self._cancel_line_order(line, 'bid', 'drift')
            if line.ask_orders:
                await self._cancel_line_order(line, 'ask', 'drift')
                
    async def _place_missing_orders(self, line: QuoteLine) -> None:
        """Place missing orders for a quote line."""
        # Get current average midpoint
        midpoint = await self._get_average_midpoint()
        if midpoint is None:
            return
            
        # Update line's last midpoint
        line.last_midpoint = midpoint
        
        # Calculate order prices
        spread_amount = midpoint * (Decimal(str(line.spread_bps)) / Decimal('10000'))
        bid_price = midpoint - spread_amount
        ask_price = midpoint + spread_amount
        
        # Place bid order if needed
        if (line.sides in [QuoteSide.BOTH, QuoteSide.BID]):
            # Check which exchanges need bid orders
            for exchange in self.exchanges:
                if not line.has_bid_on_exchange(exchange):
                    await self._place_line_order_on_exchange(line, 'bid', bid_price, midpoint, exchange)
            
        # Place ask order if needed
        if (line.sides in [QuoteSide.BOTH, QuoteSide.OFFER]):
            # Check which exchanges need ask orders
            for exchange in self.exchanges:
                if not line.has_ask_on_exchange(exchange):
                    await self._place_line_order_on_exchange(line, 'ask', ask_price, midpoint, exchange)
            
    async def _place_line_order_on_exchange(
        self, 
        line: QuoteLine, 
        side: str, 
        price: Decimal, 
        midpoint: Decimal, 
        exchange: str
    ) -> None:
        """Place an order for a specific line and side on a specific exchange."""
        try:
            # Calculate randomized quantity
            base_qty = Decimal(str(line.quantity))
            randomization = Decimal(str(line.quantity_randomization_factor)) / Decimal('100')
            qty_multiplier = Decimal('1') + Decimal(str(random.uniform(-float(randomization), float(randomization))))
            randomized_qty = base_qty * qty_multiplier
            
            # Convert quantity if needed
            if self.quantity_currency == QuantityCurrency.QUOTE:
                # Convert quote quantity to base quantity using midpoint
                final_qty = randomized_qty / midpoint
            else:
                final_qty = randomized_qty
                
            # Round quantity to appropriate precision
            # For now, use 8 decimal places - in production, get from exchange info
            final_qty = round(final_qty, 8)
            
            # Place order on the specified exchange
            timestamp_ms = int(time.time() * 1000)
            client_order_id = f"pass_quote_{side.upper()}_{timestamp_ms}"
            
            order_id = await self._place_order(
                exchange=exchange,
                side=side,
                amount=float(final_qty),
                price=float(price),
                client_order_id=client_order_id
            )
            
            if order_id:
                # Update line state
                now = datetime.now()
                if side == 'bid':
                    line.bid_orders[exchange] = order_id
                    line.bid_placed_at = now
                    line.bid_placed_price = price
                else:
                    line.ask_orders[exchange] = order_id
                    line.ask_placed_at = now
                    line.ask_placed_price = price
                    
                self.orders_placed += 1
                
                self.logger.info(
                    f"Line {line.line_id}: Placed {side} order @ {price:.4f} "
                    f"qty={final_qty:.8f} on {exchange}"
                )
                
        except Exception as e:
            self.logger.error(f"Error placing {side} order for line {line.line_id} on {exchange}: {e}")
            
    async def _cancel_line_order(self, line: QuoteLine, side: str, reason: str) -> None:
        """Cancel all orders for a specific line and side across all exchanges."""
        try:
            orders_to_cancel = line.bid_orders if side == 'bid' else line.ask_orders
            if not orders_to_cancel:
                return
                
            # Cancel on all exchanges
            cancel_tasks = []
            for exchange, order_id in orders_to_cancel.items():
                cancel_tasks.append(self._cancel_order(order_id, exchange))
                
            await asyncio.gather(*cancel_tasks, return_exceptions=True)
            
            # Update line state
            if side == 'bid':
                cancelled_count = len(line.bid_orders)
                line.bid_orders.clear()
                line.bid_placed_at = None
                line.bid_placed_price = None
            else:
                cancelled_count = len(line.ask_orders)
                line.ask_orders.clear()
                line.ask_placed_at = None
                line.ask_placed_price = None
                
            # Update stats
            if reason == 'timeout':
                self.orders_cancelled_timeout += cancelled_count
            elif reason == 'drift':
                self.orders_cancelled_drift += cancelled_count
                
            self.logger.info(
                f"Line {line.line_id}: Cancelled {cancelled_count} {side} orders due to {reason}"
            )
            
        except Exception as e:
            self.logger.error(f"Error cancelling {side} orders for line {line.line_id}: {e}")
            
    async def _get_average_midpoint(self) -> Optional[Decimal]:
        """Calculate average midpoint across all exchanges using WebSocket orderbook data."""
        midpoints = []
        
        for exchange in self.exchanges:
            data = self.orderbook_cache.get(exchange, {})
            midpoint = data.get('midpoint')
            if midpoint is not None:
                midpoints.append(midpoint)
        
        if midpoints:
            return sum(midpoints) / Decimal(str(len(midpoints)))
        return None
        
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get enhanced performance statistics."""
        base_stats = super().get_performance_stats()
        
        # Add passive quoting specific stats
        passive_stats = {
            'orders_placed': self.orders_placed,
            'orders_cancelled_timeout': self.orders_cancelled_timeout,
            'orders_cancelled_drift': self.orders_cancelled_drift,
            'quote_lines': len(self.quote_lines),
            'active_lines': sum(
                1 for line in self.quote_lines 
                if line.bid_orders or line.ask_orders
            ),
            'websocket_connections': len(self.ws_connections),
            'orderbook_exchanges': len([
                ex for ex, data in self.orderbook_cache.items()
                if data.get('last_update') is not None
            ])
        }
        
        base_stats.update(passive_stats)
        return base_stats
        
    def get_line_status(self) -> List[Dict[str, Any]]:
        """Get status of all quote lines."""
        line_status = []
        
        for line in self.quote_lines:
            status = {
                'line_id': line.line_id,
                'sides': line.sides.value,
                'spread_bps': line.spread_bps,
                'quantity': line.quantity,
                'timeout_seconds': line.timeout_seconds,
                'drift_bps': line.drift_bps,
                'bid_active': line.get_active_bid_count(),
                'ask_active': line.get_active_ask_count(),
                'bid_exchanges': list(line.bid_orders.keys()),
                'ask_exchanges': list(line.ask_orders.keys()),
                'last_midpoint': float(line.last_midpoint) if line.last_midpoint else None
            }
            
            if line.bid_placed_at:
                status['bid_age_seconds'] = (
                    datetime.now() - line.bid_placed_at
                ).total_seconds()
                
            if line.ask_placed_at:
                status['ask_age_seconds'] = (
                    datetime.now() - line.ask_placed_at
                ).total_seconds()
                
            line_status.append(status)
            
        return line_status
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert strategy state to dictionary with passive quoting details."""
        base_dict = super().to_dict()
        
        passive_dict = {
            'strategy_type': 'passive_quoting',
            'quantity_currency': self.quantity_currency.value,
            'line_status': self.get_line_status(),
            'websocket_orderbooks': {
                exchange: {
                    'best_bid': float(data.get('best_bid')) if data.get('best_bid') else None,
                    'best_ask': float(data.get('best_ask')) if data.get('best_ask') else None,
                    'midpoint': float(data.get('midpoint')) if data.get('midpoint') else None,
                    'last_update': data['last_update'].isoformat() if data.get('last_update') else None,
                    'bids_count': len(data.get('bids', [])),
                    'asks_count': len(data.get('asks', []))
                }
                for exchange, data in self.orderbook_cache.items()
            }
        }
        
        base_dict.update(passive_dict)
        return base_dict 