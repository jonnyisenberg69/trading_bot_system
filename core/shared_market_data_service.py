"""
Shared Market Data Service - Centralized, optimized market data for multi-pair strategies.

This service provides shared, high-performance market data infrastructure for multiple
trading pairs and strategies, optimizing for:
- Minimal latency (sub-millisecond updates)
- Resource efficiency (shared connections, batched processing)
- Scalability (hundreds of pairs with minimal overhead)
"""

import asyncio
import time
from dataclasses import dataclass, field
from decimal import Decimal
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Set, Callable, Tuple
from collections import defaultdict, deque
import logging
import redis.asyncio as redis
import json

logger = logging.getLogger(__name__)


@dataclass
class MarketDataSubscription:
    """Represents a strategy's subscription to market data."""
    strategy_id: str
    symbols: Set[str]
    exchanges: Set[str]
    update_callback: Optional[Callable] = None
    priority: int = 1  # Higher priority gets faster updates
    last_update: float = field(default_factory=time.time)


@dataclass
class OptimizedOrderbookLevel:
    """Memory-optimized orderbook level."""
    price: int  # Store as integer (price * 1e8) for performance
    quantity: int  # Store as integer (quantity * 1e8)
    exchange_mask: int  # Bitmask for contributing exchanges
    
    def get_price_decimal(self) -> Decimal:
        """Convert back to Decimal for calculations."""
        return Decimal(self.price) / Decimal('100000000')
    
    def get_quantity_decimal(self) -> Decimal:
        """Convert back to Decimal for calculations."""
        return Decimal(self.quantity) / Decimal('100000000')


@dataclass
class FastOrderbook:
    """Ultra-fast orderbook representation optimized for multi-pair processing."""
    symbol: str
    bids: List[OptimizedOrderbookLevel] = field(default_factory=list)
    asks: List[OptimizedOrderbookLevel] = field(default_factory=list)
    best_bid: Optional[Decimal] = None
    best_ask: Optional[Decimal] = None
    midpoint: Optional[Decimal] = None
    last_update_ns: int = 0  # Nanosecond timestamp for ultra-precise latency tracking
    contributing_exchanges: int = 0  # Bitmask of contributing exchanges
    update_count: int = 0
    
    def get_spread_bps(self) -> Optional[Decimal]:
        """Get spread in basis points."""
        if self.best_bid and self.best_ask and self.midpoint:
            return (self.best_ask - self.best_bid) / self.midpoint * Decimal('10000')
        return None


class SharedMarketDataService:
    """
    Centralized market data service optimized for multi-pair, low-latency operation.
    
    Key optimizations:
    - Single Redis connection pool shared across all strategies
    - Event-driven updates instead of polling loops
    - Batched orderbook aggregation
    - Memory-optimized data structures
    - Priority-based update distribution
    - Sub-millisecond latency targeting
    """
    
    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        max_symbols: int = 500,
        max_exchanges: int = 20
    ):
        self.redis_url = redis_url
        self.max_symbols = max_symbols
        self.max_exchanges = max_exchanges
        self.logger = logger
        
        # Shared Redis connection pool
        self.redis_pool: Optional[redis.ConnectionPool] = None
        self.redis_client: Optional[redis.Redis] = None
        self.pubsub: Optional[redis.client.PubSub] = None
        
        # Ultra-fast orderbook storage
        self.fast_orderbooks: Dict[str, FastOrderbook] = {}  # symbol -> orderbook
        self.exchange_orderbooks: Dict[Tuple[str, str], Dict[str, Any]] = {}  # (exchange, symbol) -> raw data
        
        # Subscription management
        self.subscriptions: Dict[str, MarketDataSubscription] = {}  # strategy_id -> subscription
        self.symbol_subscribers: Dict[str, Set[str]] = defaultdict(set)  # symbol -> strategy_ids
        
        # Exchange bitmask mapping for efficiency
        self.exchange_to_bit: Dict[str, int] = {}
        self.bit_to_exchange: Dict[int, str] = {}
        self._exchange_counter = 0
        
        # Performance tracking
        self.update_latencies: deque = deque(maxlen=1000)  # Track update latencies in microseconds
        self.total_updates = 0
        self.aggregation_time_ns = 0
        
        # Background tasks
        self.running = False
        self.listen_task: Optional[asyncio.Task] = None
        self.aggregation_task: Optional[asyncio.Task] = None
        self.heartbeat_task: Optional[asyncio.Task] = None
        
        # Batching optimization
        self.pending_updates: Dict[str, float] = {}  # symbol -> last_update_time
        self.batch_size = 50  # Process up to 50 symbols per batch
        self.batch_timeout_ms = 0.5  # Maximum 0.5ms batching delay
        
        # Priority queue for high-priority updates
        self.priority_updates: asyncio.Queue = asyncio.Queue(maxsize=1000)
        
    async def start(self):
        """Start the shared market data service."""
        if self.running:
            return
            
        self.running = True
        self.logger.info("Starting Shared Market Data Service (optimized for multi-pair)")
        
        # Initialize Redis connection pool
        self.redis_pool = redis.ConnectionPool.from_url(
            self.redis_url, 
            max_connections=20,  # Shared pool
            retry_on_timeout=True,
            socket_keepalive=True,
            socket_keepalive_options={}
        )
        self.redis_client = redis.Redis(connection_pool=self.redis_pool)
        
        # Test connection
        await self.redis_client.ping()
        
        # Initialize pubsub with shared connection
        self.pubsub = self.redis_client.pubsub()
        await self.pubsub.subscribe('orderbook_updates')
        
        # Start background tasks
        self.listen_task = asyncio.create_task(self._optimized_listen_loop())
        self.aggregation_task = asyncio.create_task(self._batched_aggregation_loop())
        self.heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        
        self.logger.info("Shared Market Data Service started with optimized multi-pair architecture")
    
    async def stop(self):
        """Stop the shared market data service."""
        if not self.running:
            return
            
        self.running = False
        
        # Cancel tasks
        tasks = [self.listen_task, self.aggregation_task, self.heartbeat_task]
        for task in tasks:
            if task:
                task.cancel()
        
        if any(tasks):
            await asyncio.gather(*tasks, return_exceptions=True)
        
        # Close connections
        if self.pubsub:
            await self.pubsub.unsubscribe('orderbook_updates')
            await self.pubsub.close()
        
        if self.redis_client:
            await self.redis_client.close()
        
        if self.redis_pool:
            await self.redis_pool.disconnect()
        
        self.logger.info("Shared Market Data Service stopped")
    
    async def subscribe_strategy(
        self, 
        strategy_id: str, 
        symbols: List[str], 
        exchanges: List[str],
        update_callback: Optional[Callable] = None,
        priority: int = 1
    ):
        """
        Subscribe a strategy to market data with optimized resource sharing.
        
        Args:
            strategy_id: Unique strategy identifier
            symbols: List of symbols to subscribe to
            exchanges: List of exchanges to monitor
            update_callback: Optional callback for real-time updates
            priority: Update priority (higher = faster updates)
        """
        # Register exchange bitmasks for memory efficiency
        for exchange in exchanges:
            if exchange not in self.exchange_to_bit:
                bit = 1 << self._exchange_counter
                self.exchange_to_bit[exchange] = bit
                self.bit_to_exchange[bit] = exchange
                self._exchange_counter += 1
        
        # Create subscription
        subscription = MarketDataSubscription(
            strategy_id=strategy_id,
            symbols=set(symbols),
            exchanges=set(exchanges),
            update_callback=update_callback,
            priority=priority
        )
        
        self.subscriptions[strategy_id] = subscription
        
        # Update symbol subscribers
        for symbol in symbols:
            self.symbol_subscribers[symbol].add(strategy_id)
        
        self.logger.info(
            f"Strategy {strategy_id} subscribed to {len(symbols)} symbols, "
            f"{len(exchanges)} exchanges (priority: {priority})"
        )
    
    async def unsubscribe_strategy(self, strategy_id: str):
        """Unsubscribe a strategy from market data."""
        if strategy_id not in self.subscriptions:
            return
        
        subscription = self.subscriptions[strategy_id]
        
        # Remove from symbol subscribers
        for symbol in subscription.symbols:
            self.symbol_subscribers[symbol].discard(strategy_id)
            # Clean up empty symbol entries
            if not self.symbol_subscribers[symbol]:
                del self.symbol_subscribers[symbol]
                # Remove fast orderbook if no more subscribers
                if symbol in self.fast_orderbooks:
                    del self.fast_orderbooks[symbol]
        
        # Remove subscription
        del self.subscriptions[strategy_id]
        
        self.logger.info(f"Strategy {strategy_id} unsubscribed")
    
    async def _optimized_listen_loop(self):
        """Optimized Redis listener with minimal latency."""
        while self.running:
            try:
                # Non-blocking message processing
                message = await asyncio.wait_for(
                    self.pubsub.get_message(ignore_subscribe_messages=True),
                    timeout=0.001  # 1ms timeout to prevent blocking
                )
                
                if message and message['type'] == 'message':
                    start_ns = time.perf_counter_ns()
                    
                    try:
                        update = json.loads(message['data'])
                        await self._process_update_ultra_fast(update)
                        
                        # Track latency in microseconds
                        latency_us = (time.perf_counter_ns() - start_ns) / 1000
                        self.update_latencies.append(latency_us)
                        self.total_updates += 1
                        
                    except Exception as e:
                        self.logger.error(f"Error processing update: {e}")
                
                # Yield control immediately to prevent blocking
                await asyncio.sleep(0)
                
            except asyncio.TimeoutError:
                # No message available, yield control
                await asyncio.sleep(0)
            except Exception as e:
                self.logger.error(f"Error in optimized listen loop: {e}")
                await asyncio.sleep(0.001)
    
    async def _process_update_ultra_fast(self, update: Dict[str, Any]):
        """Process orderbook update with ultra-low latency."""
        try:
            exchange = update['exchange']
            symbol = update.get('symbol')
            
            if not symbol or symbol not in self.symbol_subscribers:
                return  # Skip symbols nobody is subscribed to
            
            # Store raw exchange data
            key = (exchange, symbol)
            self.exchange_orderbooks[key] = update
            
            # Mark symbol for batched aggregation
            self.pending_updates[symbol] = time.time()
            
            # For high-priority subscribers, trigger immediate update
            subscribers = self.symbol_subscribers[symbol]
            high_priority_subscribers = [
                self.subscriptions[sid] for sid in subscribers 
                if self.subscriptions[sid].priority > 2
            ]
            
            if high_priority_subscribers:
                # Push to priority queue for immediate processing
                try:
                    self.priority_updates.put_nowait((symbol, time.perf_counter_ns()))
                except asyncio.QueueFull:
                    pass  # Drop update if queue full
        
        except Exception as e:
            self.logger.error(f"Error in ultra-fast update processing: {e}")
    
    async def _batched_aggregation_loop(self):
        """Batched aggregation loop for optimal resource utilization."""
        while self.running:
            try:
                # Process priority updates first
                try:
                    while True:
                        symbol, timestamp_ns = self.priority_updates.get_nowait()
                        await self._aggregate_symbol_immediate(symbol)
                except asyncio.QueueEmpty:
                    pass
                
                # Batch process pending updates
                if self.pending_updates:
                    # Select symbols for batch processing
                    current_time = time.time()
                    symbols_to_process = []
                    
                    for symbol, last_update in list(self.pending_updates.items()):
                        # Process if enough time has passed or batch is full
                        if (current_time - last_update > self.batch_timeout_ms / 1000 or 
                            len(symbols_to_process) >= self.batch_size):
                            symbols_to_process.append(symbol)
                            del self.pending_updates[symbol]
                    
                    # Process batch concurrently
                    if symbols_to_process:
                        aggregation_start = time.perf_counter_ns()
                        tasks = [
                            self._aggregate_symbol_immediate(symbol) 
                            for symbol in symbols_to_process
                        ]
                        await asyncio.gather(*tasks, return_exceptions=True)
                        
                        # Track aggregation performance
                        self.aggregation_time_ns = time.perf_counter_ns() - aggregation_start
                
                # Minimal sleep to yield control
                await asyncio.sleep(0)  # Yield immediately
                
            except Exception as e:
                self.logger.error(f"Error in batched aggregation loop: {e}")
                await asyncio.sleep(0.001)
    
    async def _aggregate_symbol_immediate(self, symbol: str):
        """Immediate symbol aggregation for minimal latency."""
        try:
            # Collect all exchange data for this symbol
            exchange_data = []
            contributing_exchanges = 0
            
            for (exchange, sym), orderbook in self.exchange_orderbooks.items():
                if sym == symbol:
                    if orderbook.get('bids') and orderbook.get('asks'):
                        exchange_data.append((exchange, orderbook))
                        # Set bit for this exchange
                        if exchange in self.exchange_to_bit:
                            contributing_exchanges |= self.exchange_to_bit[exchange]
            
            if not exchange_data:
                return
            
            # Ultra-fast aggregation using integer arithmetic
            aggregated_bids = defaultdict(int)  # price_int -> total_quantity_int
            aggregated_asks = defaultdict(int)
            
            for exchange, orderbook in exchange_data:
                # Process bids
                for bid_level in orderbook.get('bids', [])[:20]:  # Only top 20 levels for speed
                    try:
                        price_int = int(float(bid_level[0]) * 100000000)  # Convert to int
                        qty_int = int(float(bid_level[1]) * 100000000)
                        aggregated_bids[price_int] += qty_int
                    except (ValueError, IndexError):
                        continue
                
                # Process asks
                for ask_level in orderbook.get('asks', [])[:20]:  # Only top 20 levels for speed
                    try:
                        price_int = int(float(ask_level[0]) * 100000000)
                        qty_int = int(float(ask_level[1]) * 100000000)
                        aggregated_asks[price_int] += qty_int
                    except (ValueError, IndexError):
                        continue
            
            # Create fast orderbook
            fast_book = FastOrderbook(
                symbol=symbol,
                last_update_ns=time.perf_counter_ns(),
                contributing_exchanges=contributing_exchanges,
                update_count=self.fast_orderbooks.get(symbol, FastOrderbook(symbol)).update_count + 1
            )
            
            # Convert back to optimized levels (top 10 only for memory efficiency)
            if aggregated_bids:
                sorted_bids = sorted(aggregated_bids.items(), reverse=True)[:10]
                fast_book.bids = [
                    OptimizedOrderbookLevel(price=price_int, quantity=qty_int, exchange_mask=contributing_exchanges)
                    for price_int, qty_int in sorted_bids
                ]
                fast_book.best_bid = Decimal(sorted_bids[0][0]) / Decimal('100000000')
            
            if aggregated_asks:
                sorted_asks = sorted(aggregated_asks.items())[:10]
                fast_book.asks = [
                    OptimizedOrderbookLevel(price=price_int, quantity=qty_int, exchange_mask=contributing_exchanges)
                    for price_int, qty_int in sorted_asks
                ]
                fast_book.best_ask = Decimal(sorted_asks[0][0]) / Decimal('100000000')
            
            # Calculate midpoint
            if fast_book.best_bid and fast_book.best_ask:
                fast_book.midpoint = (fast_book.best_bid + fast_book.best_ask) / Decimal('2')
            
            # Store fast orderbook
            self.fast_orderbooks[symbol] = fast_book
            
            # Notify subscribers with priority-based ordering
            await self._notify_subscribers_optimized(symbol, fast_book)
            
        except Exception as e:
            self.logger.error(f"Error in immediate symbol aggregation for {symbol}: {e}")
    
    async def _notify_subscribers_optimized(self, symbol: str, fast_book: FastOrderbook):
        """Notify subscribers with priority-based optimization."""
        try:
            subscribers = self.symbol_subscribers.get(symbol, set())
            if not subscribers:
                return
            
            # Group by priority for batched notifications
            priority_groups = defaultdict(list)
            for strategy_id in subscribers:
                subscription = self.subscriptions.get(strategy_id)
                if subscription and subscription.update_callback:
                    priority_groups[subscription.priority].append(subscription)
            
            # Process highest priority first
            for priority in sorted(priority_groups.keys(), reverse=True):
                subscriptions = priority_groups[priority]
                
                # Batch notify same-priority subscribers
                notification_tasks = []
                for subscription in subscriptions:
                    try:
                        # Create minimal update object for callback
                        update_data = {
                            'symbol': symbol,
                            'best_bid': fast_book.best_bid,
                            'best_ask': fast_book.best_ask,
                            'midpoint': fast_book.midpoint,
                            'spread_bps': fast_book.get_spread_bps(),
                            'update_ns': fast_book.last_update_ns,
                            'contributing_exchanges': fast_book.contributing_exchanges
                        }
                        
                        # Non-blocking callback
                        task = asyncio.create_task(subscription.update_callback(update_data))
                        notification_tasks.append(task)
                        
                    except Exception as e:
                        self.logger.error(f"Error creating notification task: {e}")
                
                # Don't wait for callbacks to complete - fire and forget for minimal latency
                # They run in background
        
        except Exception as e:
            self.logger.error(f"Error notifying subscribers for {symbol}: {e}")
    
    async def _heartbeat_loop(self):
        """Heartbeat loop for service health monitoring."""
        while self.running:
            try:
                # Update service health metrics
                await self.redis_client.setex(
                    "shared_market_data:heartbeat",
                    5,  # 5 second TTL
                    json.dumps({
                        'timestamp': datetime.now(timezone.utc).isoformat(),
                        'total_updates': self.total_updates,
                        'tracked_symbols': len(self.fast_orderbooks),
                        'active_subscriptions': len(self.subscriptions),
                        'avg_latency_us': sum(self.update_latencies) / len(self.update_latencies) if self.update_latencies else 0,
                        'aggregation_time_us': self.aggregation_time_ns / 1000
                    })
                )
                
                await asyncio.sleep(1)  # 1 second heartbeat
                
            except Exception as e:
                self.logger.error(f"Error in heartbeat loop: {e}")
                await asyncio.sleep(1)
    
    # Public API for strategies
    
    def get_fast_orderbook(self, symbol: str) -> Optional[FastOrderbook]:
        """Get ultra-fast orderbook for a symbol."""
        return self.fast_orderbooks.get(symbol)
    
    def get_best_bid_ask(self, symbol: str) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        """Get best bid/ask with minimal latency."""
        fast_book = self.fast_orderbooks.get(symbol)
        if fast_book:
            return fast_book.best_bid, fast_book.best_ask
        return None, None
    
    def get_midpoint(self, symbol: str) -> Optional[Decimal]:
        """Get midpoint with minimal latency."""
        fast_book = self.fast_orderbooks.get(symbol)
        return fast_book.midpoint if fast_book else None
    
    def calculate_wapq_fast(
        self, 
        symbol: str, 
        side: str, 
        quantity: Decimal,
        max_levels: int = 5
    ) -> Optional[Decimal]:
        """
        Calculate WAPQ with minimal latency using optimized data structures.
        
        Args:
            symbol: Trading symbol
            side: 'bid' or 'ask'
            quantity: Quantity to price
            max_levels: Maximum levels to consider (for speed)
        """
        fast_book = self.fast_orderbooks.get(symbol)
        if not fast_book:
            return None
        
        levels = fast_book.asks if side == 'bid' else fast_book.bids
        if not levels:
            return None
        
        total_cost = 0  # Use int arithmetic for speed
        remaining_qty = int(float(quantity) * 100000000)
        
        for level in levels[:max_levels]:
            if remaining_qty <= 0:
                break
            
            take_qty = min(remaining_qty, level.quantity)
            total_cost += take_qty * level.price
            remaining_qty -= take_qty
        
        filled_qty = int(float(quantity) * 100000000) - remaining_qty
        if filled_qty > 0:
            # Convert back to Decimal
            wapq_int = total_cost // filled_qty
            return Decimal(wapq_int) / Decimal('100000000')
        
        return None
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get performance statistics for monitoring."""
        return {
            'total_updates': self.total_updates,
            'tracked_symbols': len(self.fast_orderbooks),
            'active_subscriptions': len(self.subscriptions),
            'avg_latency_us': sum(self.update_latencies) / len(self.update_latencies) if self.update_latencies else 0,
            'p95_latency_us': sorted(self.update_latencies)[int(0.95 * len(self.update_latencies))] if len(self.update_latencies) > 20 else 0,
            'aggregation_time_us': self.aggregation_time_ns / 1000,
            'symbols_by_subscriber_count': {
                symbol: len(subscribers) 
                for symbol, subscribers in self.symbol_subscribers.items()
            },
            'memory_usage': {
                'fast_orderbooks': len(self.fast_orderbooks),
                'exchange_orderbooks': len(self.exchange_orderbooks),
                'pending_updates': len(self.pending_updates)
            }
        }


# Global shared service instance
_shared_service: Optional[SharedMarketDataService] = None


async def get_shared_market_data_service() -> SharedMarketDataService:
    """Get or create the global shared market data service."""
    global _shared_service
    
    if _shared_service is None:
        _shared_service = SharedMarketDataService()
        await _shared_service.start()
    
    return _shared_service


async def shutdown_shared_market_data_service():
    """Shutdown the global shared market data service."""
    global _shared_service
    
    if _shared_service:
        await _shared_service.stop()
        _shared_service = None
