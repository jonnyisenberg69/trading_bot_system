"""
Market Making Strategy - Reference exchange pricing with automatic hedging
"""
import asyncio
import time
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Optional, Any
import logging
import redis.asyncio as redis
import contextlib

from bot_manager.strategies.base_strategy import BaseStrategy
from market_data.redis_orderbook_manager import RedisOrderbookManager


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
class MarketMakingLine:
    """Represents a single market making line configuration."""
    line_id: int
    timeout_seconds: int
    drift_bps: float
    quantity: float
    spread_bps: float
    sides: QuoteSide
    
    # Runtime state - track orders per exchange
    bid_orders: Dict[str, str] = field(default_factory=dict)  # exchange -> order_id
    ask_orders: Dict[str, str] = field(default_factory=dict)  # exchange -> order_id
    last_reference_price: Optional[Decimal] = None  # Reference exchange price
    bid_placed_at: Dict[str, datetime] = field(default_factory=dict)  # exchange -> datetime
    ask_placed_at: Dict[str, datetime] = field(default_factory=dict)  # exchange -> datetime
    bid_placed_price: Optional[Decimal] = None
    ask_placed_price: Optional[Decimal] = None
    
    # Hedging tracking
    pending_hedges: Dict[str, Dict[str, Any]] = field(default_factory=dict)  # order_id -> hedge_info
    
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


class MarketMakingStrategy(BaseStrategy):
    """
    Market making strategy using reference exchange pricing with automatic hedging.
    
    Features:
    - Uses reference exchange for pricing decisions
    - Places quotes on non-reference exchanges around reference price
    - Automatically hedges positions on reference exchange when filled
    - Cancels orders on timeout or drift from reference price
    - Uses Redis pub/sub for real-time market data
    - Supports multiple quote lines with different parameters
    """
    
    def __init__(
        self,
        instance_id: str,
        symbol: str,
        exchanges: List[str],
        config: Dict[str, Any],
        redis_url: str = "redis://localhost:6379"
    ):
        super().__init__(instance_id, symbol, exchanges, config)
        
        # Strategy-specific attributes
        self.quote_lines: List[MarketMakingLine] = []
        self.quantity_currency: QuantityCurrency = QuantityCurrency.BASE
        self.reference_exchange: str = ""
        self.main_task: Optional[asyncio.Task] = None
        
        # Redis orderbook manager
        self.redis_orderbook_manager = RedisOrderbookManager(redis_url)
        
        # Redis client for strategy coordination
        self.redis_client: Optional[redis.Redis] = None
        
        # Performance tracking
        self.orders_placed = 0
        self.orders_cancelled_timeout = 0
        self.orders_cancelled_drift = 0
        self.hedges_executed = 0
        self.hedge_errors = 0

        # Position tracking
        self.max_position: Optional[Decimal] = None  # In base units
        self.net_position: Decimal = Decimal('0')    # Signed base units
        self.avg_price: Optional[Decimal] = None     # Weighted average entry price for current net position
        self.realized_pnl: Decimal = Decimal('0')    # Cumulative realized PnL in quote currency
        # Per-exchange position legs (P1/P2 and fees)
        self.position_legs: Dict[str, Dict[str, Decimal]] = {}
        
        # WAPQ engine state
        self._wapq_task: Optional[asyncio.Task] = None
        self._wapq_prices: Dict[tuple, Decimal] = {}
        self._wapq_event: Optional[asyncio.Event] = None
        self._wapq_lock: Optional[asyncio.Lock] = None
        self._last_wapq_log_ts: float = 0.0
        self._fastpath_task: Optional[asyncio.Task] = None
        self._last_update_ns: int = 0
        self._last_wapq_ns: int = 0
    
    async def _validate_config(self) -> None:
        """Validate market making configuration."""
        # Defensive: flatten config if double-nested
        if 'config' in self.config and isinstance(self.config['config'], dict):
            self.logger.info("Detected double-nested config, flattening...")
            self.config = self.config['config']
        
        required_fields = [
            'base_coin', 'lines', 'quantity_currency', 'reference_exchange'
        ]
        
        for field in required_fields:
            if field not in self.config:
                raise ValueError(f"Missing required config field: {field}")
        
        # Validate reference exchange
        self.reference_exchange = self.config.get('reference_exchange', '').lower()
        if self.reference_exchange not in self.exchanges:
            raise ValueError(f"Reference exchange '{self.reference_exchange}' must be one of the selected exchanges: {self.exchanges}")
        
        # Validate quantity currency
        qty_currency = self.config.get('quantity_currency', 'base').lower()
        if qty_currency not in ['base', 'quote']:
            raise ValueError(f"Invalid quantity_currency: {qty_currency}")
        self.quantity_currency = QuantityCurrency(qty_currency)
        
        # Optional: maximum net position (base units)
        try:
            max_pos_val = self.config.get('max_position')
            if max_pos_val is not None:
                self.max_position = Decimal(str(max_pos_val))
        except Exception:
            self.max_position = None

        # Validate lines configuration
        lines_config = self.config.get('lines', [])
        if not lines_config or not isinstance(lines_config, list):
            raise ValueError("Lines configuration must be a non-empty list")
        
        # Parse and validate each line
        self.quote_lines = []
        for i, line_config in enumerate(lines_config):
            required_line_fields = [
                'timeout', 'drift', 'quantity',
                'spread', 'sides'
            ]
            
            for field in required_line_fields:
                if field not in line_config:
                    raise ValueError(f"Line {i}: Missing required field '{field}'")
            
            # Validate sides
            sides = line_config['sides'].lower()
            if sides not in ['both', 'bid', 'offer']:
                raise ValueError(f"Line {i}: Invalid sides '{sides}'")
            
            quote_line = MarketMakingLine(
                line_id=i,
                timeout_seconds=int(line_config['timeout']),
                drift_bps=float(line_config['drift']),
                quantity=float(line_config['quantity']),
                spread_bps=float(line_config['spread']),
                sides=QuoteSide(sides)
            )
            
            self.quote_lines.append(quote_line)
        
        # Optional hedge spread (bps) for price-protected hedges; defaults to first line spread or 10 bps
        try:
            self.hedge_spread_bps: float = float(self.config.get('hedge_spread', self.quote_lines[0].spread_bps if self.quote_lines else 10.0))
        except Exception:
            self.hedge_spread_bps = 10.0
        
        self.logger.info(f"Validated config with {len(self.quote_lines)} quote lines, reference exchange: {self.reference_exchange}")

        if self.max_position is not None:
            self.logger.info(f"Max position configured: {self.max_position} {self.config.get('base_coin', self.symbol.split('/')[0])}")
    
    async def _start_websocket_monitoring(self) -> None:
        """Enable WebSocket monitoring so trade confirmations trigger hedging immediately."""
        await super()._start_websocket_monitoring()
    
    async def _stop_websocket_monitoring(self) -> None:
        await super()._stop_websocket_monitoring()
    
    async def _start_strategy(self) -> None:
        """Start the market making strategy."""
        self.logger.info(f"Starting market making strategy using {self.reference_exchange} as reference exchange")
        
        # Initialize Redis client for strategy coordination
        self.redis_client = redis.from_url(self.redis_orderbook_manager.redis_url)
        
        # Start Redis orderbook manager
        await self.redis_orderbook_manager.start()
        
        # Start WAPQ computation engine
        self._wapq_event = asyncio.Event()
        self._wapq_lock = asyncio.Lock()
        try:
            # If the orderbook manager supports callbacks, trigger on any update
            if hasattr(self.redis_orderbook_manager, 'update_callbacks') and isinstance(self.redis_orderbook_manager.update_callbacks, list):
                self.redis_orderbook_manager.update_callbacks.append(self._on_md_update)
        except Exception:
            pass
        self._wapq_task = asyncio.create_task(self._wapq_loop())
        # Prepare fastpath helper
        async def _noop():
            return None
        self._fastpath_task = asyncio.create_task(_noop())
        
        # Wait for initial reference orderbook data (up to 15 seconds)
        start_wait = time.time()
        waited = 0
        while waited < 15:
            ref_symbol = self._resolve_reference_symbol()
            bid, ask = self.redis_orderbook_manager.get_best_bid_ask(self.reference_exchange, ref_symbol)
            if bid and ask:
                break
            await asyncio.sleep(0.5)
            waited = time.time() - start_wait
        if waited >= 15:
            self.logger.warning(
                f"Reference orderbook not available after {waited:.1f}s for {self.reference_exchange}; proceeding anyway"
            )
        
        # Also wait for non-reference exchanges to populate orderbooks (up to 5 seconds)
        self.logger.info("Waiting for non-reference exchange orderbooks to populate...")
        start_wait = time.time()
        waited = 0
        while waited < 5:
            all_ready = True
            for exchange in self.exchanges:
                if exchange == self.reference_exchange:
                    continue
                ex_symbol = self._resolve_symbol_for_exchange(exchange)
                bid, ask = self.redis_orderbook_manager.get_best_bid_ask(exchange, ex_symbol)
                if not bid or not ask:
                    all_ready = False
                    break
            if all_ready:
                self.logger.info(f"All exchange orderbooks ready after {waited:.1f}s")
                break
            await asyncio.sleep(0.5)
            waited = time.time() - start_wait
        if waited >= 5:
            self.logger.warning(f"Some exchange orderbooks not available after {waited:.1f}s; proceeding anyway")
        
        # Start main strategy loop
        self.main_task = asyncio.create_task(self._strategy_loop())
    
    async def _stop_strategy(self) -> None:
        """Stop the market making strategy."""
        self.logger.info("Stopping market making strategy")
        
        if self.main_task:
            self.main_task.cancel()
            try:
                await self.main_task
            except asyncio.CancelledError:
                pass
        
        # Cancel all active orders
        for line in self.quote_lines:
            if line.bid_orders:
                await self._cancel_line_order(line, 'bid', 'strategy_stop')
            if line.ask_orders:
                await self._cancel_line_order(line, 'ask', 'strategy_stop')
        
        # Stop Redis components
        await self.redis_orderbook_manager.stop()
        if self.redis_client:
            await self.redis_client.close()
        # Stop WAPQ engine
        if self._wapq_task and not self._wapq_task.done():
            self._wapq_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._wapq_task
    
    async def _strategy_loop(self) -> None:
        """Main strategy loop."""
        self.logger.info("Starting market making strategy loop with 1.5ms cycle time")
        
        while True:
            try:
                # Check for timeouts and drift, then place orders
                if self.quote_lines:
                    tasks = [self._process_quote_line(line) for line in self.quote_lines]
                    await asyncio.gather(*tasks, return_exceptions=True)
                
                await asyncio.sleep(0.0015)
            except asyncio.CancelledError:
                self.logger.info("Strategy loop cancelled")
                break
            except Exception as e:
                self.logger.error(f"Error in strategy loop: {e}")
                await asyncio.sleep(1)
    
    async def _process_quote_line(self, line: MarketMakingLine) -> None:
        """Process a single quote line."""
        try:
            await self._check_timeouts(line)
            await self._check_drift(line)
            await self._place_missing_orders(line)
        except Exception as e:
            self.logger.error(f"Error processing line {line.line_id}: {e}")
    
    async def _check_timeouts(self, line: MarketMakingLine) -> None:
        """Check and cancel orders that have timed out (per exchange)."""
        now = datetime.now(timezone.utc)
        timeout_delta = timedelta(seconds=line.timeout_seconds)
        
        for exchange in list(line.bid_orders.keys()):
            placed_at = line.bid_placed_at.get(exchange)
            if placed_at and (now - placed_at) > timeout_delta:
                self.logger.info(
                    f"Line {line.line_id}: Cancelling BID order on {exchange} due to TIMEOUT. "
                    f"Order age: {(now - placed_at).total_seconds():.2f}s > {line.timeout_seconds}s timeout."
                )
                await self._cancel_line_order(line, 'bid', 'timeout', exchange)
        
        for exchange in list(line.ask_orders.keys()):
            placed_at = line.ask_placed_at.get(exchange)
            if placed_at and (now - placed_at) > timeout_delta:
                self.logger.info(
                    f"Line {line.line_id}: Cancelling ASK order on {exchange} due to TIMEOUT. "
                    f"Order age: {(now - placed_at).total_seconds():.2f}s > {line.timeout_seconds}s timeout."
                )
                await self._cancel_line_order(line, 'ask', 'timeout', exchange)
    
    def _resolve_reference_symbol(self) -> str:
        """Resolve and cache the actual symbol key published for the reference exchange in Redis."""
        if hasattr(self, "_cached_reference_symbol") and self._cached_reference_symbol:
            return self._cached_reference_symbol
        exchange = self.reference_exchange
        candidates = []
        base_quote = self.symbol.split('/') if '/' in self.symbol else [self.symbol, 'USDT']
        base = base_quote[0]
        quote = base_quote[1] if len(base_quote) > 1 else 'USDT'
        candidates.append(self.symbol)
        if self.symbol.endswith('/USDT'):
            candidates.append(self.symbol.replace('/USDT', '/USD'))
            candidates.append(self.symbol.replace('/USDT', '/USDT0'))
        if self.symbol.endswith('/USD'):
            candidates.append(self.symbol.replace('/USD', '/USDT'))
            candidates.append(self.symbol.replace('/USD', '/USDT0'))
        if exchange.startswith('bitfinex'):
            base_variants = {base, base.upper(), base.capitalize()}
            if base.upper() == 'XAUT':
                base_variants.update({'XAUT', 'XAUt'})
            quote_variants = {quote, quote.upper(), quote.capitalize()}
            quote_variants.update({'USD', 'USDT', 'USDT0'})
            for b in base_variants:
                for q in quote_variants:
                    candidates.append(f"{b}/{q}")
        elif exchange.startswith('hyperliquid'):
            # Include Hyperliquid-specific base/quote variants (e.g., XAUT0/USDT0)
            base_variants = {base, base.upper(), base.capitalize()}
            if base.upper() == 'XAUT':
                base_variants.update({'XAUT', 'XAUt', 'XAUT0', 'UXAUT'})
            quote_variants = {quote, quote.upper(), quote.capitalize(), 'USDC', 'USDT0'}
            for b in base_variants:
                for q in quote_variants:
                    candidates.append(f"{b}/{q}")
            # Known good special case for Hyperliquid spot gold
            if base.upper() == 'XAUT':
                candidates.append('XAUT0/USDT0')
        available = set()
        if hasattr(self.redis_orderbook_manager, 'orderbooks') and exchange in self.redis_orderbook_manager.orderbooks:
            available = set(self.redis_orderbook_manager.orderbooks[exchange].keys())
        for cand in candidates:
            if cand in available:
                self._cached_reference_symbol = cand
                return cand
        return self.symbol
    
    def _resolve_symbol_for_exchange(self, exchange: str) -> str:
        """Resolve and cache the actual symbol key published for a given exchange in Redis."""
        if not hasattr(self, "_cached_exchange_symbols"):
            self._cached_exchange_symbols = {}
        if exchange in self._cached_exchange_symbols and self._cached_exchange_symbols[exchange]:
            return self._cached_exchange_symbols[exchange]
        candidates = []
        base_quote = self.symbol.split('/') if '/' in self.symbol else [self.symbol, 'USDT0']
        base = base_quote[0]
        quote = base_quote[1] if len(base_quote) > 1 else 'USDT0'
        candidates.append(self.symbol)
        quote_variants = {quote}
        quote_variants.update({quote.upper(), quote.capitalize()})
        quote_variants.update({'USD', 'USDT', 'USDC', 'USDT0'})
        base_variants = {base, base.upper(), base.capitalize()}
        if base.upper() == 'XAUT':
            base_variants.update({'XAUT', 'XAUt', 'XAUT0'})
        for b in base_variants:
            for q in quote_variants:
                candidates.append(f"{b}/{q}")
        if exchange.startswith('hyperliquid'):
            for b in base_variants:
                if 'spot' in exchange:
                    # Spot uses USDC without settlement suffix
                    candidates.append(f"{b}/USDT0")
                    # Special case for XAUT on Hyperliquid spot
                    if b.upper() == 'XAUT':
                        candidates.append('XAUT0/USDT0')
                else:
                    # Perp uses USDC:USDC
                    candidates.append(f"{b}/USDT:USDT")
        available = set()
        if hasattr(self.redis_orderbook_manager, 'orderbooks') and exchange in self.redis_orderbook_manager.orderbooks:
            available = set(self.redis_orderbook_manager.orderbooks[exchange].keys())
        # Debug logging for hyperliquid
        if exchange.startswith('hyperliquid'):
            self.logger.debug(f"Resolving symbol for {exchange}: candidates={candidates}, available={available}")
        for cand in candidates:
            if cand in available:
                self._cached_exchange_symbols[exchange] = cand
                return cand
        # If no match found, log warning
        if exchange.startswith('hyperliquid'):
            self.logger.warning(f"No symbol match found for {exchange}. Candidates: {candidates}, Available: {available}")
        return self.symbol
    
    async def _check_drift(self, line: MarketMakingLine) -> None:
        """Check and cancel orders that have drifted too far from reference price."""
        # Use latest WAPQ as the reference if available; fallback to midpoint
        current_reference_price = None
        try:
            if self.quote_lines:
                # Prefer the smallest-spread line as baseline
                base_line = sorted(self.quote_lines, key=lambda l: l.spread_bps)[0]
                wp = self._get_wapq_for_line(base_line, 'mid')
                if wp is not None:
                    current_reference_price = wp
        except Exception:
            current_reference_price = None
        if current_reference_price is None:
            ref_symbol = self._resolve_reference_symbol()
            reference_best_bid, reference_best_ask = self.redis_orderbook_manager.get_best_bid_ask(
                self.reference_exchange, ref_symbol
            )
            if not reference_best_bid or not reference_best_ask:
                return
            current_reference_price = (reference_best_bid + reference_best_ask) / 2
        if line.last_reference_price is None:
            line.last_reference_price = current_reference_price
            return
        drift_pct = abs((current_reference_price - line.last_reference_price) / line.last_reference_price) * 100
        drift_bps = drift_pct * 100
        if drift_bps > line.drift_bps:
            self.logger.info(
                f"Line {line.line_id}: Cancelling orders due to REFERENCE PRICE DRIFT. "
                f"Drift: {drift_bps:.2f} bps (allowed: {line.drift_bps:.2f} bps), "
                f"Current reference price: {current_reference_price}, Last reference price: {line.last_reference_price}"
            )
            if line.bid_orders:
                await self._cancel_line_order(line, 'bid', 'drift')
            if line.ask_orders:
                await self._cancel_line_order(line, 'ask', 'drift')
        line.last_reference_price = current_reference_price
    
    async def _place_missing_orders(self, line: MarketMakingLine) -> None:
        """Place missing orders based on reference exchange pricing.
        Uses side-specific WAPQ aligned to hedgeable liquidity:
        - bid uses WAPQ from reference bids (we'd sell into bids to hedge a filled bid)
        - ask uses WAPQ from reference asks (we'd buy into asks to hedge a filled ask)
        Midpoint is reserved for drift checks only.
        """
        spread_decimal = Decimal(str(line.spread_bps)) / Decimal('10000')
        # Side-specific WAPQ references (note: WAPQ storage keys were computed as
        # 'bid' from asks and 'ask' from bids; we intentionally invert here to
        # fetch the hedge-side liquidity WAPQ)
        bid_ref = self._get_wapq_for_line(line, 'ask')   # from reference bids
        ask_ref = self._get_wapq_for_line(line, 'bid')   # from reference asks
        # Compute per-side target quote prices from side-specific references
        bid_price = bid_ref * (Decimal('1') - spread_decimal) if bid_ref is not None else None
        ask_price = ask_ref * (Decimal('1') + spread_decimal) if ask_ref is not None else None
        order_tasks = []
        for exchange in self.exchanges:
            if exchange == self.reference_exchange:
                continue
            ex_symbol = self._resolve_symbol_for_exchange(exchange)
            best_bid, best_ask = self.redis_orderbook_manager.get_best_bid_ask(exchange, ex_symbol)
            if best_bid is None or best_ask is None:
                # Proceed without taker check if we don't have per-exchange book yet
                self.logger.debug(f"Line {line.line_id}: No best bid/ask for {exchange} (symbol: {ex_symbol}), placing using reference prices")
                exchange_bid_price = bid_price
                exchange_ask_price = ask_price
            else:
                exchange_bid_price = self._apply_taker_check(bid_price, 'bid', best_bid, best_ask) if bid_price is not None else None
                exchange_ask_price = self._apply_taker_check(ask_price, 'ask', best_bid, best_ask) if ask_price is not None else None

            # Enforce max position before placing orders
            if self.max_position is not None:
                # For BID (buy), net position increases; for OFFER (sell), it decreases
                if (line.sides in [QuoteSide.BOTH, QuoteSide.BID]
                    and not line.has_bid_on_exchange(exchange)
                    and exchange_bid_price is not None
                    and bid_ref is not None):
                    # Compute base qty using bid_ref for QUOTE->BASE conversion when needed
                    base_qty = Decimal(str(line.quantity))
                    if self.quantity_currency == QuantityCurrency.QUOTE:
                        base_qty = base_qty / bid_ref
                    base_qty = base_qty.quantize(Decimal('1.00000000'))
                    # Log a concise snapshot of prices before deciding to skip/place
                    try:
                        self.logger.info(
                            f"Line {line.line_id}: Prices snapshot before BID decision on {exchange} "
                            f"(symbol={ex_symbol}): ref_wapq_bid(hedge=bids)={bid_ref:.4f}, "
                            f"ex_bid={best_bid if best_bid is not None else 'n/a'}, "
                            f"ex_ask={best_ask if best_ask is not None else 'n/a'}, "
                            f"quote_bid={exchange_bid_price:.4f}, quote_ask={exchange_ask_price if exchange_ask_price is not None else 'n/a'}"
                        )
                    except Exception:
                        self.logger.info(
                            f"Line {line.line_id}: Prices snapshot before BID decision on {exchange} "
                            f"(symbol={ex_symbol}): ref_wapq_bid(hedge=bids)={bid_ref}, ex_bid={best_bid}, ex_ask={best_ask}, "
                            f"quote_bid={exchange_bid_price}, quote_ask={exchange_ask_price}"
                        )
                    projected = self.net_position + base_qty
                    if projected.copy_abs() > self.max_position:
                        self.logger.info(
                            f"Line {line.line_id}: Skipping BID on {exchange} due to max position. "
                            f"current={self.net_position}, qty={base_qty}, max={self.max_position}"
                        )
                    else:
                        task = self._place_line_order_on_exchange(
                            line, 'bid', exchange_bid_price, bid_ref, exchange
                        )
                        order_tasks.append(task)
                if (line.sides in [QuoteSide.BOTH, QuoteSide.OFFER]
                    and not line.has_ask_on_exchange(exchange)
                    and exchange_ask_price is not None
                    and ask_ref is not None):
                    # Compute base qty using ask_ref for QUOTE->BASE conversion when needed
                    base_qty = Decimal(str(line.quantity))
                    if self.quantity_currency == QuantityCurrency.QUOTE:
                        base_qty = base_qty / ask_ref
                    base_qty = base_qty.quantize(Decimal('1.00000000'))
                    # Log a concise snapshot of prices before ASK decision
                    try:
                        self.logger.info(
                            f"Line {line.line_id}: Prices snapshot before ASK decision on {exchange} "
                            f"(symbol={ex_symbol}): ref_wapq_ask(hedge=asks)={ask_ref:.4f}, "
                            f"ex_bid={best_bid if best_bid is not None else 'n/a'}, "
                            f"ex_ask={best_ask if best_ask is not None else 'n/a'}, "
                            f"quote_bid={exchange_bid_price if exchange_bid_price is not None else 'n/a'}, "
                            f"quote_ask={exchange_ask_price:.4f}"
                        )
                    except Exception:
                        self.logger.info(
                            f"Line {line.line_id}: Prices snapshot before ASK decision on {exchange} "
                            f"(symbol={ex_symbol}): ref_wapq_ask(hedge=asks)={ask_ref}, ex_bid={best_bid}, ex_ask={best_ask}, "
                            f"quote_bid={exchange_bid_price}, quote_ask={exchange_ask_price}"
                        )
                    projected = self.net_position - base_qty
                    if projected.copy_abs() > self.max_position:
                        self.logger.info(
                            f"Line {line.line_id}: Skipping ASK on {exchange} due to max position. "
                            f"current={self.net_position}, qty={base_qty}, max={self.max_position}"
                        )
                    else:
                        task = self._place_line_order_on_exchange(
                            line, 'ask', exchange_ask_price, ask_ref, exchange
                        )
                        order_tasks.append(task)
                continue  # Max position branch handled placing decisions

            # Default placement when no max position configured
            if (line.sides in [QuoteSide.BOTH, QuoteSide.BID] and 
                not line.has_bid_on_exchange(exchange) and
                exchange_bid_price is not None and bid_ref is not None):
                task = self._place_line_order_on_exchange(
                    line, 'bid', exchange_bid_price, bid_ref, exchange
                )
                order_tasks.append(task)
            if (line.sides in [QuoteSide.BOTH, QuoteSide.OFFER] and 
                not line.has_ask_on_exchange(exchange) and
                exchange_ask_price is not None and ask_ref is not None):
                task = self._place_line_order_on_exchange(
                    line, 'ask', exchange_ask_price, ask_ref, exchange
                )
                order_tasks.append(task)
        if order_tasks:
            await asyncio.gather(*order_tasks, return_exceptions=True)

    async def _wapq_loop(self) -> None:
        """Continuously compute WAPQ-based reference prices per line using the reference exchange orderbook.
        - For BID quotes: use reference ASK liquidity (best to worst)
        - For OFFER quotes: use reference BID liquidity (best to worst)
        Ensures non-overlapping allocation among lines per side (ordered by tighter spread first).
        """
        # Lightweight polling with event-triggered bursts
        while True:
            try:
                # Wait briefly for an update event or timeout to poll
                if self._wapq_event is not None:
                    try:
                        await asyncio.wait_for(self._wapq_event.wait(), timeout=0.01)
                    except asyncio.TimeoutError:
                        pass
                    finally:
                        if self._wapq_event.is_set():
                            self._wapq_event.clear()

                ref_symbol = self._resolve_reference_symbol()
                ob = None
                try:
                    if hasattr(self.redis_orderbook_manager, 'get_orderbook'):
                        ob = self.redis_orderbook_manager.get_orderbook(self.reference_exchange, ref_symbol)
                except Exception:
                    ob = None
                if not ob or not ob.get('bids') or not ob.get('asks'):
                    await asyncio.sleep(0.05)
                    continue

                # Prepare parsed levels
                bids = self._parse_levels(ob.get('bids', []), reverse=True)  # best bid to worse (high to low)
                asks = self._parse_levels(ob.get('asks', []), reverse=False) # best ask to worse (low to high)
                if not bids or not asks:
                    await asyncio.sleep(0.02)
                    continue

                # Compute base quantities for each line once
                line_infos = []
                for line in self.quote_lines:
                    try:
                        base_qty = Decimal(str(line.quantity))
                        # Estimate base qty from current mid if quantity currency is quote
                        if self.quantity_currency == QuantityCurrency.QUOTE:
                            best_bid = bids[0][0]
                            best_ask = asks[0][0]
                            mid = (best_bid + best_ask) / 2
                            if mid > 0:
                                base_qty = base_qty / mid
                        # Normalize
                        base_qty = max(Decimal('0'), base_qty).quantize(Decimal('1.00000000'))
                        line_infos.append((line, base_qty))
                    except Exception:
                        line_infos.append((line, Decimal('0')))

                # Order lines per side by tighter spread first, then line_id
                bid_lines = [li for li in line_infos if li[0].sides in [QuoteSide.BOTH, QuoteSide.BID]]
                ask_lines = [li for li in line_infos if li[0].sides in [QuoteSide.BOTH, QuoteSide.OFFER]]
                bid_lines.sort(key=lambda t: (t[0].spread_bps, t[0].line_id))
                ask_lines.sort(key=lambda t: (t[0].spread_bps, t[0].line_id))

                # Allocate non-overlapping liquidity for BID lines using asks
                ask_ptr = 0
                ask_rem = asks[0][1] if asks else Decimal('0')
                local_prices: Dict[tuple, Decimal] = {}
                filled_bid_qty: Dict[int, Decimal] = {}
                # Detailed allocation tracking for numeric validation
                bid_alloc_levels: Dict[int, List[tuple]] = {}
                for line, qty in bid_lines:
                    if qty <= 0:
                        continue
                    total_cost = Decimal('0')
                    filled = Decimal('0')
                    # Consume from asks
                    while filled < qty and ask_ptr < len(asks):
                        price, vol = asks[ask_ptr]
                        avail = ask_rem if ask_ptr < len(asks) else Decimal('0')
                        if avail <= 0:
                            # move to next level
                            ask_ptr += 1
                            if ask_ptr < len(asks):
                                ask_rem = asks[ask_ptr][1]
                            continue
                        take = min(qty - filled, avail)
                        total_cost += take * price
                        filled += take
                        ask_rem -= take
                        # record allocation at this level
                        if take > 0:
                            bid_alloc_levels.setdefault(line.line_id, []).append((price, take))
                        if ask_rem <= 0:
                            ask_ptr += 1
                            if ask_ptr < len(asks):
                                ask_rem = asks[ask_ptr][1]
                    if filled > 0:
                        local_prices[(line.line_id, 'bid')] = (total_cost / filled).quantize(Decimal('1.00000000'))
                    filled_bid_qty[line.line_id] = filled

                # Allocate non-overlapping liquidity for ASK lines using bids
                bid_ptr = 0
                bid_rem = bids[0][1] if bids else Decimal('0')
                filled_ask_qty: Dict[int, Decimal] = {}
                ask_alloc_levels: Dict[int, List[tuple]] = {}
                for line, qty in ask_lines:
                    if qty <= 0:
                        continue
                    total_cost = Decimal('0')
                    filled = Decimal('0')
                    # Consume from bids (sell into bids)
                    while filled < qty and bid_ptr < len(bids):
                        price, vol = bids[bid_ptr]
                        avail = bid_rem if bid_ptr < len(bids) else Decimal('0')
                        if avail <= 0:
                            bid_ptr += 1
                            if bid_ptr < len(bids):
                                bid_rem = bids[bid_ptr][1]
                            continue
                        take = min(qty - filled, avail)
                        total_cost += take * price
                        filled += take
                        bid_rem -= take
                        # record allocation at this level
                        if take > 0:
                            ask_alloc_levels.setdefault(line.line_id, []).append((price, take))
                        if bid_rem <= 0:
                            bid_ptr += 1
                            if bid_ptr < len(bids):
                                bid_rem = bids[bid_ptr][1]
                    if filled > 0:
                        local_prices[(line.line_id, 'ask')] = (total_cost / filled).quantize(Decimal('1.00000000'))
                    filled_ask_qty[line.line_id] = filled

                # Store a baseline mid for drift and logging convenience
                try:
                    mid = ((bids[0][0] + asks[0][0]) / 2).quantize(Decimal('1.00000000'))
                    local_prices[('baseline', 'mid')] = mid
                except Exception:
                    pass

                # Publish results atomically
                if self._wapq_lock:
                    async with self._wapq_lock:
                        self._wapq_prices = local_prices
                else:
                    self._wapq_prices = local_prices

                # Fast path: trigger immediate line processing once per WAPQ publish (non-blocking)
                try:
                    # latency measurement: update -> wapq publish (disabled verbose log)
                    self._last_wapq_ns = time.perf_counter_ns()
                    # if self._last_update_ns:
                    #     delta_us = (self._last_wapq_ns - self._last_update_ns) / 1000.0
                    #     self.logger.debug(f"⏱ WAPQ latency: {delta_us:.3f} µs from update to publish")
                    if self.quote_lines and (self._fastpath_task is None or self._fastpath_task.done()):
                        self._fastpath_task = asyncio.create_task(self._fastpath_run_once())
                except Exception:
                    pass

                # Throttled WAPQ logging for numerical validation (disabled)
                # now_ts = time.time()
                # if now_ts - self._last_wapq_log_ts >= 1.0:
                #     self._last_wapq_log_ts = now_ts
                #     try:
                #         parts: List[str] = []
                #         line_ids = sorted({li[0].line_id for li in line_infos})
                #         for lid in line_ids:
                #             bid_px = local_prices.get((lid, 'bid'))
                #             ask_px = local_prices.get((lid, 'ask'))
                #             b_qty = filled_bid_qty.get(lid, Decimal('0'))
                #             a_qty = filled_ask_qty.get(lid, Decimal('0'))
                #             def fmt(d: Optional[Decimal]) -> str:
                #                 return f"{d:.8f}" if isinstance(d, Decimal) else "n/a"
                #             parts.append(
                #                 f"L{lid}(bid={fmt(bid_px)},bqty={fmt(b_qty)}; ask={fmt(ask_px)},aqty={fmt(a_qty)})"
                #             )
                #         baseline_mid = local_prices.get(('baseline', 'mid'))
                #         self.logger.info(
                #             f"WAPQ summary: ref={fmt(baseline_mid)} | " + ", ".join(parts)
                #         )
                #         def fmt_pair_list(pairs: Optional[List[tuple]]) -> str:
                #             if not pairs:
                #                 return "[]"
                #             return "[" + ",".join([f"{p:.4f}x{q:.8f}" for p, q in pairs]) + "]"
                #         detail_parts: List[str] = []
                #         for lid in line_ids:
                #             detail_parts.append(
                #                 f"L{lid} bid_alloc=" + fmt_pair_list(bid_alloc_levels.get(lid, [])) +
                #                 f" avg=" + (f"{local_prices.get((lid,'bid')):.8f}" if (lid, 'bid') in local_prices else "n/a") +
                #                 f" qty=" + (f"{filled_bid_qty.get(lid, Decimal('0')):.8f}" if lid in filled_bid_qty else "0.00000000")
                #             )
                #         # self.logger.info("WAPQ detail (BID): " + "; ".join(detail_parts))
                #         detail_parts = []
                #         for lid in line_ids:
                #             detail_parts.append(
                #                 f"L{lid} ask_alloc=" + fmt_pair_list(ask_alloc_levels.get(lid, [])) +
                #                 f" avg=" + (f"{local_prices.get((lid,'ask')):.8f}" if (lid, 'ask') in local_prices else "n/a") +
                #                 f" qty=" + (f"{filled_ask_qty.get(lid, Decimal('0')):.8f}" if lid in filled_ask_qty else "0.00000000")
                #             )
                #         # self.logger.info("WAPQ detail (ASK): " + "; ".join(detail_parts))
                #     except Exception:
                #         pass

            except asyncio.CancelledError:
                break
            except Exception as e:
                try:
                    self.logger.debug(f"WAPQ loop error: {e}")
                except Exception:
                    pass
                await asyncio.sleep(0.05)

    def _on_md_update(self, *_args, **_kwargs):
        """Market data update callback: record timestamp and trigger WAPQ event."""
        try:
            self._last_update_ns = time.perf_counter_ns()
            if self._wapq_event is not None:
                self._wapq_event.set()
        except Exception:
            pass

    def _parse_levels(self, levels: List[Any], reverse: bool) -> List[tuple]:
        """Parse orderbook levels to list of (price Decimal, size Decimal), sorted best-first.
        levels may be [[price, amount], ...] with str/float/Decimal.
        """
        parsed: List[tuple] = []
        for lvl in levels:
            try:
                price = Decimal(str(lvl[0]))
                size = Decimal(str(lvl[1]))
                if size > 0 and price > 0:
                    parsed.append((price, size))
            except Exception:
                continue
        parsed.sort(key=lambda x: x[0], reverse=reverse)
        return parsed

    def _get_wapq_for_line(self, line: MarketMakingLine, kind: str) -> Optional[Decimal]:
        """Return last computed WAPQ for a line and side, or baseline mid when kind=='mid'."""
        try:
            prices = self._wapq_prices
            if not prices:
                return None
            if kind == 'mid':
                # Prefer line side-specific if both computed, else baseline mid
                # Use average of available side WAPQs if present
                bid_key = (line.line_id, 'bid')
                ask_key = (line.line_id, 'ask')
                if bid_key in prices and ask_key in prices:
                    return ((prices[bid_key] + prices[ask_key]) / 2).quantize(Decimal('1.00000000'))
                if bid_key in prices:
                    return prices[bid_key]
                if ask_key in prices:
                    return prices[ask_key]
                return prices.get(('baseline', 'mid'))
            else:
                return prices.get((line.line_id, kind))
        except Exception:
            return None
    
    def _apply_taker_check(self, quote_price: Decimal, side: str, best_bid: Decimal, best_ask: Decimal) -> Optional[Decimal]:
        """Apply taker check to ensure we don't cross the spread."""
        min_tick_size = Decimal('0.000001')
        if side == 'bid':
            if quote_price >= best_ask:
                adjusted_price = best_ask - min_tick_size
                self.logger.debug(f"Taker check: Bid price {quote_price} >= best ask {best_ask}, adjusted to {adjusted_price}")
                return adjusted_price
            return quote_price
        else:
            if quote_price <= best_bid:
                adjusted_price = best_bid + min_tick_size
                self.logger.debug(f"Taker check: Ask price {quote_price} <= best bid {best_bid}, adjusted to {adjusted_price}")
                return adjusted_price
            return quote_price

    def _apply_fill_to_position(self, side: str, amount: Decimal, price: Decimal) -> Decimal:
        """Update net position and average price with a new fill. Returns realized PnL for this fill."""
        if amount <= 0:
            return Decimal('0')
        realized = Decimal('0')
        old_net = self.net_position
        old_avg = self.avg_price if self.avg_price is not None else price
        # Realized PnL occurs when reducing existing exposure
        if old_net > 0 and side == 'sell':
            realized = (price - old_avg) * min(amount, old_net)
        elif old_net < 0 and side == 'buy':
            realized = (old_avg - price) * min(amount, abs(old_net))

        delta = amount if side == 'buy' else -amount
        new_pos = old_net + delta
        # If increasing same direction or starting from zero, recompute weighted average
        if old_net == 0 or (old_net > 0 and delta > 0) or (old_net < 0 and delta < 0):
            if self.avg_price is None or old_net == 0:
                self.avg_price = price
            else:
                weighted_cost = (old_avg * abs(old_net)) + (price * abs(delta))
                total_size = abs(old_net) + abs(delta)
                self.avg_price = (weighted_cost / total_size) if total_size > 0 else price
        else:
            # Reducing position; if we flip sign, set avg to fill price for the residual position
            if (old_net > 0 and new_pos < 0) or (old_net < 0 and new_pos > 0):
                self.avg_price = price
            # Otherwise keep avg as is

        self.net_position = new_pos
        self.realized_pnl += realized
        # Log the updated position
        try:
            self.logger.info(
                f"📊 Position update: net={self.net_position:.8f} base, avg_price={self.avg_price if self.avg_price is not None else 'n/a'}, realized_pnl={self.realized_pnl:.8f}"
            )
        except Exception:
            self.logger.info(f"📊 Position update: net={self.net_position} base, avg_price={self.avg_price}, realized_pnl={self.realized_pnl}")
        return realized

    def _get_base_quote(self) -> tuple[str, str]:
        """Return base and quote currency symbols from self.symbol."""
        try:
            base, quote = self.symbol.split('/')
        except ValueError:
            base, quote = self.symbol, 'USDT'
        return base.upper(), quote.upper()

    def _update_position_legs(
        self,
        exchange: str,
        side: str,
        amount: Decimal,
        price: Decimal,
        fee_cost: Optional[Decimal] = None,
        fee_currency: Optional[str] = None,
    ) -> None:
        """Update P1/P2 legs and fees for a specific exchange."""
        if exchange not in self.position_legs:
            self.position_legs[exchange] = {
                'p1': Decimal('0'),
                'p2': Decimal('0'),
                'p1_fee': Decimal('0'),
                'p2_fee': Decimal('0'),
            }
        legs = self.position_legs[exchange]
        # P1/P2 updates
        if side == 'buy':
            legs['p1'] += amount
            legs['p2'] -= (amount * price)
        else:
            legs['p1'] -= amount
            legs['p2'] += (amount * price)
        # Fees
        if fee_cost is not None and fee_currency:
            base, quote = self._get_base_quote()
            fee_ccy = fee_currency.upper()
            if fee_ccy == base:
                legs['p1_fee'] += fee_cost
            elif fee_ccy == quote:
                legs['p2_fee'] += fee_cost
            else:
                # Unknown fee currency; attribute to quote as approximation
                legs['p2_fee'] += fee_cost

    def _avg_price_from_legs(self, legs: Dict[str, Decimal]) -> Optional[Decimal]:
        """Compute average price from position legs."""
        if legs['p1'] == 0:
            return None
        try:
            return -(legs['p2'] + legs['p2_fee']) / (legs['p1'] + legs['p1_fee'])
        except Exception:
            return None

    def _log_positions_snapshot(self) -> None:
        """Log per-exchange and net positions snapshot with P1/P2 legs and avg price."""
        try:
            # Per-exchange
            for ex, legs in self.position_legs.items():
                avg_px = self._avg_price_from_legs(legs)
                self.logger.info(
                    f"📒 {ex} position: P1={legs['p1']:.8f}, P2={legs['p2']:.8f}, "
                    f"P1_fee={legs['p1_fee']:.8f}, P2_fee={legs['p2_fee']:.8f}, avg={avg_px if avg_px is not None else 'n/a'}"
                )
            # Net aggregation
            net = {'p1': Decimal('0'), 'p2': Decimal('0'), 'p1_fee': Decimal('0'), 'p2_fee': Decimal('0')}
            for legs in self.position_legs.values():
                net['p1'] += legs['p1']
                net['p2'] += legs['p2']
                net['p1_fee'] += legs['p1_fee']
                net['p2_fee'] += legs['p2_fee']
            net_avg = self._avg_price_from_legs(net)
            self.logger.info(
                f"🧾 NET position: P1={net['p1']:.8f}, P2={net['p2']:.8f}, "
                f"P1_fee={net['p1_fee']:.8f}, P2_fee={net['p2_fee']:.8f}, avg={net_avg if net_avg is not None else 'n/a'}"
            )
        except Exception:
            pass

    async def _place_line_order_on_exchange(
        self, 
        line: MarketMakingLine, 
        side: str, 
        price: Decimal, 
        reference_price: Decimal, 
        exchange: str
    ) -> None:
        """Place an order for a specific line and side on a specific exchange."""
        try:
            base_qty = Decimal(str(line.quantity))
            if self.quantity_currency == QuantityCurrency.QUOTE:
                final_qty = base_qty / reference_price
            else:
                final_qty = base_qty
            final_qty = round(final_qty, 8)
            timestamp_ms = int(time.time() * 1000)
            client_order_id = f"mm_quote_{side.upper()}_{timestamp_ms}"
            order_side = 'buy' if side == 'bid' else 'sell'
            order_id = await self._place_order(
                exchange=exchange,
                side=order_side,
                amount=float(final_qty),
                price=float(price),
                client_order_id=client_order_id
            )
            if order_id:
                now = datetime.now(timezone.utc)
                if side == 'bid':
                    line.bid_orders[exchange] = order_id
                    line.bid_placed_at[exchange] = now
                    line.bid_placed_price = price
                else:
                    line.ask_orders[exchange] = order_id
                    line.ask_placed_at[exchange] = now
                    line.ask_placed_price = price
                self.orders_placed += 1
                self.logger.info(
                    f"Line {line.line_id}: Placed {side} order @ {price:.4f} "
                    f"qty={final_qty:.8f} on {exchange} (ref: {reference_price:.4f})"
                )
        except Exception as e:
            self.logger.error(f"Error placing {side} order for line {line.line_id} on {exchange}: {e}")
    
    async def _cancel_line_order(self, line: MarketMakingLine, side: str, reason: str, exchange: Optional[str] = None) -> None:
        """Cancel orders for a specific line, side, and (optionally) exchange only."""
        try:
            if side == 'bid':
                orders_to_cancel = line.bid_orders
                placed_at = line.bid_placed_at
            else:
                orders_to_cancel = line.ask_orders
                placed_at = line.ask_placed_at
            if not orders_to_cancel:
                return
            cancel_tasks = []
            if exchange:
                order_id = orders_to_cancel.get(exchange)
                if order_id:
                    self.logger.info(
                        f"Line {line.line_id}: Cancelling {side.upper()} order on {exchange} due to {reason.upper()} (order_id: {order_id})"
                    )
                    cancel_tasks.append(self._cancel_order(order_id, exchange))
            else:
                for ex, order_id in orders_to_cancel.items():
                    self.logger.info(
                        f"Line {line.line_id}: Cancelling {side.upper()} order on {ex} due to {reason.upper()} (order_id: {order_id})"
                    )
                    cancel_tasks.append(self._cancel_order(order_id, ex))
            await asyncio.gather(*cancel_tasks, return_exceptions=True)
            if exchange:
                if exchange in orders_to_cancel:
                    del orders_to_cancel[exchange]
                if exchange in placed_at:
                    del placed_at[exchange]
                if side == 'bid' and not orders_to_cancel:
                    line.bid_placed_price = None
                if side == 'ask' and not orders_to_cancel:
                    line.ask_placed_price = None
            else:
                orders_to_cancel.clear()
                placed_at.clear()
                if side == 'bid':
                    line.bid_placed_price = None
                else:
                    line.ask_placed_price = None
            if reason == 'timeout':
                self.orders_cancelled_timeout += 1
            elif reason == 'drift':
                self.orders_cancelled_drift += 1
            self.logger.info(
                f"Line {line.line_id}: Cancelled {side} order(s) on {exchange if exchange else 'all exchanges'} due to {reason}"
            )
        except Exception as e:
            self.logger.error(f"Error cancelling {side} orders for line {line.line_id} on {exchange if exchange else 'all'}: {e}")
    
    async def on_trade_confirmation(self, trade_data: Dict[str, Any]) -> None:
        """Handle trade confirmation and execute hedging."""
        try:
            exchange = trade_data.get('exchange', '').lower()
            side = trade_data.get('side', '').lower()
            amount = Decimal(str(trade_data.get('amount', 0)))
            price = Decimal(str(trade_data.get('price', 0)))
            order_id = trade_data.get('order_id', '')

            # Skip snapshot/backfill trades (e.g., Hyperliquid userFills initial dump)
            try:
                if trade_data.get('is_snapshot') is True:
                    self.logger.info("Ignoring snapshot/backfill trade message for hedging/position updates")
                    return
            except Exception:
                pass

            # Update position based on any trade we see (all subscribed exchanges)
            last_realized = Decimal('0')
            if side in ('buy', 'sell') and amount and price:
                last_realized = self._apply_fill_to_position(side, amount, price)
                # Per-exchange P1/P2 legs update
                fee_cost = None
                fee_currency = None
                try:
                    # Prefer structured fee if provided
                    if isinstance(trade_data.get('fee'), dict):
                        fee_cost = Decimal(str(trade_data['fee'].get('cost', 0)))
                        fee_currency = trade_data['fee'].get('currency')
                    else:
                        # Fall back to flat fields
                        raw_fee = trade_data.get('fee')
                        fee_cost = Decimal(str(raw_fee)) if raw_fee is not None else None
                        fee_currency = trade_data.get('fee_currency')
                except Exception:
                    pass
                self._update_position_legs(exchange, side, amount, price, fee_cost, fee_currency)
            if exchange == self.reference_exchange:
                # Hedge fill received; log concise PnL/position snapshot
                try:
                    self.logger.info(
                        f"🧮 Hedge fill recorded: side={side}, amt={amount:.8f}, px={price:.4f}; "
                        f"net={self.net_position:.8f}, avg={self.avg_price if self.avg_price is not None else 'n/a'}, "
                        f"realized_this={last_realized:.8f}, realized_total={self.realized_pnl:.8f}"
                    )
                except Exception:
                    self.logger.info(
                        f"🧮 Hedge fill recorded: side={side}, amt={amount}, px={price}; net={self.net_position}, avg={self.avg_price}, realized_this={last_realized}, realized_total={self.realized_pnl}"
                    )
                # Also log per-exchange and net P1/P2 legs snapshot
                self._log_positions_snapshot()
                self.logger.debug(f"Trade on reference exchange {exchange}, no hedging needed")
                return
            if not amount or not price:
                self.logger.warning(f"Invalid trade data for hedging: {trade_data}")
                return
            hedge_side = 'sell' if side == 'buy' else 'buy'
            await self._execute_hedge(exchange, hedge_side, amount, price, order_id)
        except Exception as e:
            self.logger.error(f"Error in trade confirmation handler: {e}")
    
    async def _execute_hedge(self, source_exchange: str, hedge_side: str, amount: Decimal, price: Decimal, order_id: str) -> None:
        """Execute hedge trade on reference exchange."""
        try:
            # Compute hedge limit from the source execution price
            hedge_bps = Decimal(str(self.hedge_spread_bps)) / Decimal('10000')
            if hedge_side == 'sell':
                # Original was a buy; target a higher sell price by hedge spread
                target_px = price * (Decimal('1') + hedge_bps)
            else:
                # Original was a sell; target a lower buy price by hedge spread
                target_px = price * (Decimal('1') - hedge_bps)

            # Optionally enforce crossing using reference book if available
            ref_symbol = self._resolve_reference_symbol()
            best_bid, best_ask = self.redis_orderbook_manager.get_best_bid_ask(self.reference_exchange, ref_symbol)
            if not best_bid or not best_ask:
                try:
                    connector = self.exchange_connectors.get(self.reference_exchange)
                    if connector:
                        ob = await connector.get_orderbook(ref_symbol, limit=5)
                        if ob and ob.get('bids') and ob.get('asks'):
                            best_bid = Decimal(str(ob['bids'][0][0]))
                            best_ask = Decimal(str(ob['asks'][0][0]))
                except Exception:
                    pass
            # Start with target price and clamp to be marketable if we know the book
            limit_px = target_px
            if best_bid and best_ask:
                if hedge_side == 'sell':
                    # Sell must be <= best_bid to cross
                    if limit_px > best_bid:
                        limit_px = best_bid
                else:
                    # Buy must be >= best_ask to cross
                    if limit_px < best_ask:
                        limit_px = best_ask
            # Place a limit order with price protection (still crosses to execute fast)
            self.logger.info(
                f"🔄 Executing hedge: {hedge_side.upper()} {amount} @ {limit_px:.4f} (limit) on {self.reference_exchange} "
                f"(source: {source_exchange} {hedge_side} {amount} @ {price})"
            )
            hedge_order_id = await self._place_order(
                exchange=self.reference_exchange,
                side=hedge_side,
                amount=float(amount),
                price=float(limit_px),
                order_type='limit',
                client_order_id=f"hedge_{order_id}_{int(time.time() * 1000)}"
            )
            if hedge_order_id:
                self.hedges_executed += 1
                self.logger.info(
                    f"✅ Hedge executed: {hedge_side.upper()} {amount} on {self.reference_exchange} (order_id: {hedge_order_id})"
                )
            else:
                self.hedge_errors += 1
                self.logger.error(f"❌ Failed to execute hedge on {self.reference_exchange}")
        except Exception as e:
            self.hedge_errors += 1
            self.logger.error(f"Error executing hedge: {e}")
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get enhanced performance statistics."""
        base_stats = super().get_performance_stats()
        market_making_stats = {
            'orders_placed': self.orders_placed,
            'orders_cancelled_timeout': self.orders_cancelled_timeout,
            'orders_cancelled_drift': self.orders_cancelled_drift,
            'hedges_executed': self.hedges_executed,
            'hedge_errors': self.hedge_errors,
            'quote_lines': len(self.quote_lines),
            'active_lines': sum(
                1 for line in self.quote_lines 
                if line.bid_orders or line.ask_orders
            ),
            'reference_exchange': self.reference_exchange,
            'available_exchanges': len(self.redis_orderbook_manager.get_available_exchanges()),
            'orderbook_status': self.redis_orderbook_manager.get_status()
        }
        base_stats.update(market_making_stats)
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
                'last_reference_price': float(line.last_reference_price) if line.last_reference_price else None,
                'pending_hedges': len(line.pending_hedges)
            }
            if line.bid_placed_at:
                try:
                    ages = [
                        (datetime.now(timezone.utc) - placed_at).total_seconds()
                        for placed_at in line.bid_placed_at.values()
                        if placed_at is not None
                    ]
                    status['bid_age_seconds'] = min(ages) if ages else None
                except Exception:
                    status['bid_age_seconds'] = None
            if line.ask_placed_at:
                try:
                    ages = [
                        (datetime.now(timezone.utc) - placed_at).total_seconds()
                        for placed_at in line.ask_placed_at.values()
                        if placed_at is not None
                    ]
                    status['ask_age_seconds'] = min(ages) if ages else None
                except Exception:
                    status['ask_age_seconds'] = None
            line_status.append(status)
        return line_status
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert strategy state to dictionary with market making details."""
        base_dict = super().to_dict()
        market_making_dict = {
            'strategy_type': 'market_making',
            'quantity_currency': self.quantity_currency.value,
            'reference_exchange': self.reference_exchange,
            'line_status': self.get_line_status(),
            'redis_orderbook_status': self.redis_orderbook_manager.get_status()
        }
        base_dict.update(market_making_dict)
        return base_dict
