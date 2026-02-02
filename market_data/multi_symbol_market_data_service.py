"""
Multi-Symbol Market Data Service - Enhanced market data service supporting multiple symbols and cross-exchange analysis.

This service extends the current market data architecture to support:
- Multiple symbols per service instance
- Cross-symbol relationship tracking
- Multi-reference exchange pricing
- Synthetic instrument construction
"""

import asyncio
import json
import logging
import time
from typing import Dict, List, Set, Optional, Tuple, Callable, Any
from dataclasses import dataclass, field
from decimal import Decimal
from datetime import datetime, timezone
import redis.asyncio as redis
import ccxt.pro
from collections import defaultdict, deque
import numpy as np

from .redis_orderbook_manager import RedisOrderbookManager


@dataclass
class SymbolConfig:
    """Configuration for a symbol across exchanges."""
    base_symbol: str  # e.g., 'BERA/USDT'
    exchanges: List[Dict[str, str]]  # [{'name': 'binance', 'type': 'spot', 'symbol': 'BERA/USDT'}, ...]
    priority: int = 1  # Higher priority symbols get more resources
    depth: int = 100  # Orderbook depth
    is_synthetic: bool = False  # Whether this is a synthetic instrument
    components: Optional[List[str]] = None  # For synthetic instruments


@dataclass 
class CrossExchangeSpread:
    """Cross-exchange spread data."""
    symbol: str
    exchange_a: str
    exchange_b: str
    spread_bps: Decimal
    arbitrage_opportunity: bool
    timestamp: float
    liquidity_a: Decimal
    liquidity_b: Decimal


@dataclass
class PriceRelationship:
    """Price relationship between symbols (correlation, cointegration, etc.)."""
    symbol_a: str
    symbol_b: str
    correlation: float
    spread_mean: Decimal
    spread_std: Decimal
    last_updated: float
    z_score: Optional[float] = None  # Current z-score of spread


class MultiSymbolMarketDataService:
    """
    Enhanced market data service supporting multiple symbols and cross-exchange analysis.
    
    Key features:
    - Multi-symbol orderbook management
    - Cross-exchange spread monitoring
    - Synthetic instrument pricing
    - Real-time relationship tracking
    - Intelligent reference exchange selection
    """
    
    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        symbol_configs: Optional[List[SymbolConfig]] = None,
        exchange_connectors: Optional[Dict[str, Any]] = None
    ):
        self.redis_url = redis_url
        self.redis_client: Optional[redis.Redis] = None
        self.logger = logging.getLogger(__name__)
        
        # Symbol configurations
        self.symbol_configs: Dict[str, SymbolConfig] = {}
        if symbol_configs:
            for config in symbol_configs:
                self.symbol_configs[config.base_symbol] = config
        
        # Exchange connections - use existing connectors if provided
        self.exchange_connectors = exchange_connectors or {}
        self.use_existing_connectors = bool(exchange_connectors)
        self.exchanges: Dict[str, ccxt.pro.Exchange] = {}
        self.exchange_symbols: Dict[str, Set[str]] = defaultdict(set)  # exchange -> symbols
        
        # Market data storage
        self.orderbooks: Dict[Tuple[str, str], Dict[str, Any]] = {}  # (exchange, symbol) -> orderbook
        self.price_cache: Dict[Tuple[str, str], Decimal] = {}  # (exchange, symbol) -> midprice
        self.spread_cache: Dict[str, List[CrossExchangeSpread]] = {}  # symbol -> spreads
        
        # Cross-symbol relationships
        self.price_relationships: Dict[Tuple[str, str], PriceRelationship] = {}
        self.correlation_window = deque(maxlen=1000)  # Price history for correlation
        
        # Reference exchange intelligence
        self.reference_exchange_scores: Dict[Tuple[str, str], float] = {}  # (exchange, symbol) -> score
        self.liquidity_tracker: Dict[Tuple[str, str], List[float]] = defaultdict(list)
        
        # Tasks and control
        self.tasks: List[asyncio.Task] = []
        self.running = False
        self.shutdown_event = asyncio.Event()
        
        # Callbacks for pricing updates
        self.price_update_callbacks: List[Callable] = []
        self.spread_update_callbacks: List[Callable] = []
        self.relationship_update_callbacks: List[Callable] = []

    async def start(self):
        """Start the enhanced market data service."""
        if self.running:
            return
            
        self.logger.info("Starting Multi-Symbol Market Data Service...")
        self.running = True
        
        # Connect to Redis
        self.redis_client = redis.from_url(self.redis_url)
        await self.redis_client.ping()
        
        # Initialize exchanges
        if self.use_existing_connectors:
            await self._setup_existing_connectors()
        else:
            await self._initialize_exchanges()
        
        # Start monitoring tasks
        await self._start_monitoring_tasks()
        
        # Start analysis tasks
        self._start_analysis_tasks()
        
        self.logger.info(f"Multi-Symbol Market Data Service started with {len(self.symbol_configs)} symbols")

    async def _setup_existing_connectors(self):
        """Setup using existing exchange connectors from strategy."""
        self.logger.info(f"Using existing exchange connectors: {list(self.exchange_connectors.keys())}")
        
        # Map connector IDs to exchange keys for consistency
        for connector_id, connector in self.exchange_connectors.items():
            exchange_key = connector_id  # Use connector_id as exchange_key
            
            # Get symbols for this exchange from our configs
            symbols_for_exchange = set()
            for symbol_config in self.symbol_configs.values():
                for exchange_info in symbol_config.exchanges:
                        if f"{exchange_info['name']}_{exchange_info.get('type', 'spot')}" == connector_id:
                            base_symbol = symbol_config.base_symbol
                            # Check if this symbol is available on this specific exchange type (normalize for the exchange)
                            autodiscovered_configs = await self._autodiscover_available_symbols()
                            available_symbols = autodiscovered_configs.get(connector_id, [])
                            normalized_base = self._normalize_symbol_for_exchange(base_symbol, connector_id)
                            normalized_avails = [self._normalize_symbol_for_exchange(s, connector_id) for s in available_symbols]
                            
                            symbol_available = any(self._symbols_match(normalized_base, avail) for avail in normalized_avails)
                            if symbol_available:
                                normalized_symbol = normalized_base
                                symbols_for_exchange.add(normalized_symbol)
                                self.logger.info(f"✅ Will monitor {base_symbol} -> {normalized_symbol} on {connector_id}")
                            else:
                                self.logger.debug(f"⏭️ Skipping {base_symbol} on {connector_id} - not available in {available_symbols}")
            
            self.exchange_symbols[exchange_key] = symbols_for_exchange
        
        # Log what we'll be monitoring
        total_monitor_tasks = sum(len(symbols) for symbols in self.exchange_symbols.values())
        self.logger.info(f"🎯 Setup complete: will start {total_monitor_tasks} orderbook monitoring tasks")
        for exchange_key, symbols in self.exchange_symbols.items():
            self.logger.info(f"  📊 {exchange_key}: {len(symbols)} symbols -> {list(symbols)}")
        
        self.logger.info(f"Setup complete with {len(self.exchange_connectors)} existing connectors")

    async def _initialize_exchanges(self):
        """Initialize exchange connections with autodiscovery for available symbols."""
        # First, autodiscover which symbols are actually available on each exchange
        autodiscovered_configs = await self._autodiscover_available_symbols()
        
        # Collect all unique exchanges needed
        exchange_configs = {}
        
        for symbol_config in self.symbol_configs.values():
            for exchange_info in symbol_config.exchanges:
                exchange_key = f"{exchange_info['name']}_{exchange_info.get('type', 'spot')}"
                base_exchange = exchange_info['name']
                
                # Check if this symbol is actually available on this exchange
                base_symbol = symbol_config.base_symbol
                available_symbols = autodiscovered_configs.get(base_exchange, [])
                
                # Check if this symbol is actually available on this exchange (normalize for the specific exchange_key)
                normalized_base = self._normalize_symbol_for_exchange(base_symbol, exchange_key)
                normalized_avails = [self._normalize_symbol_for_exchange(s, exchange_key) for s in available_symbols]
                symbol_available = any(self._symbols_match(normalized_base, avail) for avail in normalized_avails)
                
                if not symbol_available:
                    self.logger.debug(f"⏭️ Skipping {base_symbol} on {exchange_key} - not available")
                    continue
                
                # Normalize symbol for this specific exchange
                normalized_symbol = self._normalize_symbol_for_exchange(base_symbol, exchange_key)
                
                if exchange_key not in exchange_configs:
                    exchange_configs[exchange_key] = {
                        'name': exchange_info['name'],
                        'type': exchange_info.get('type', 'spot'),
                        'symbols': set()
                    }
                
                exchange_configs[exchange_key]['symbols'].add(normalized_symbol)
                self.logger.info(f"✅ Added {base_symbol} -> {normalized_symbol} to {exchange_key} (autodiscovered)")
        
        # Initialize exchanges
        class_map = {
            'binance': ccxt.pro.binance,
            'bybit': ccxt.pro.bybit,
            'hyperliquid': ccxt.pro.hyperliquid,
            'mexc': ccxt.pro.mexc,
            'gateio': ccxt.pro.gate,
            'bitget': ccxt.pro.bitget,
            'bitfinex': ccxt.pro.bitfinex,
        }
        
        for exchange_key, config in exchange_configs.items():
            try:
                exchange_class = class_map.get(config['name'])
                if not exchange_class:
                    continue
                
                exchange = exchange_class({'enableRateLimit': True})
                await exchange.load_markets()
                
                self.exchanges[exchange_key] = exchange
                self.exchange_symbols[exchange_key] = config['symbols']
                
                self.logger.info(f"Initialized {exchange_key} with {len(config['symbols'])} symbols")
                
            except Exception as e:
                self.logger.error(f"Failed to initialize {exchange_key}: {e}")

    async def _start_monitoring_tasks(self):
        """Start orderbook monitoring for all symbols."""
        if self.use_existing_connectors:
            # Use existing connectors for orderbook data
            for exchange_key, connector in self.exchange_connectors.items():
                for symbol in self.exchange_symbols[exchange_key]:
                    task = asyncio.create_task(
                        self._monitor_symbol_orderbook_with_connector(exchange_key, symbol, connector)
                    )
                    self.tasks.append(task)
        else:
            # Use ccxt.pro exchanges
            for exchange_key, exchange in self.exchanges.items():
                for symbol in self.exchange_symbols[exchange_key]:
                    task = asyncio.create_task(
                        self._monitor_symbol_orderbook(exchange_key, symbol)
                    )
                    self.tasks.append(task)

    def _start_analysis_tasks(self):
        """Start background analysis tasks."""
        # Cross-exchange spread analysis
        spread_task = asyncio.create_task(self._analyze_cross_exchange_spreads())
        self.tasks.append(spread_task)
        
        # Price relationship tracking
        relationship_task = asyncio.create_task(self._track_price_relationships())
        self.tasks.append(relationship_task)
        
        # Reference exchange scoring
        scoring_task = asyncio.create_task(self._score_reference_exchanges())
        self.tasks.append(scoring_task)

    async def _monitor_symbol_orderbook(self, exchange_key: str, symbol: str):
        """Monitor orderbook for a specific exchange-symbol pair."""
        exchange = self.exchanges[exchange_key]
        
        while self.running:
            try:
                # Get symbol config to determine depth
                symbol_config = None
                for config in self.symbol_configs.values():
                    if any(ex.get('symbol', config.base_symbol) == symbol for ex in config.exchanges):
                        symbol_config = config
                        break
                
                # Use exchange-specific depth limits
                base_exchange = exchange_key.split('_')[0]
                if base_exchange == 'bybit':
                    if 'spot' in exchange_key:
                        depth = 50  # Bybit spot: use 50 (valid limit)
                    else:  # perp
                        depth = 50  # Bybit perp: use 50 (valid limit)
                elif base_exchange == 'hyperliquid':
                    depth = 20  # Hyperliquid: smaller depth
                else:
                    depth = symbol_config.depth if symbol_config else 100
                
                # Watch orderbook
                orderbook = await exchange.watch_order_book(symbol, limit=depth)
                
                if orderbook and orderbook.get('bids') and orderbook.get('asks'):
                    # Store orderbook
                    key = (exchange_key, symbol)
                    self.orderbooks[key] = orderbook
                    
                    # Update price cache
                    best_bid = Decimal(str(orderbook['bids'][0][0]))
                    best_ask = Decimal(str(orderbook['asks'][0][0]))
                    midprice = (best_bid + best_ask) / Decimal('2')
                    self.price_cache[key] = midprice
                    
                    # Track liquidity for reference exchange scoring
                    bid_liquidity = sum(Decimal(str(level[1])) for level in orderbook['bids'][:10])
                    self.liquidity_tracker[key].append(float(bid_liquidity))
                    if len(self.liquidity_tracker[key]) > 100:
                        self.liquidity_tracker[key].pop(0)
                    
                    # Publish to Redis
                    await self._publish_enhanced_orderbook_update(exchange_key, symbol, orderbook)
                    
                    # Trigger callbacks
                    for callback in self.price_update_callbacks:
                        try:
                            await callback(exchange_key, symbol, orderbook)
                        except Exception as e:
                            self.logger.error(f"Error in price update callback: {e}")
                            
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error monitoring {exchange_key} {symbol}: {e}")
                await asyncio.sleep(1)

    async def _analyze_cross_exchange_spreads(self):
        """Continuously analyze spreads between exchanges."""
        while self.running:
            try:
                # Group orderbooks by symbol
                symbol_orderbooks = defaultdict(list)
                for (exchange, symbol), orderbook in self.orderbooks.items():
                    symbol_orderbooks[symbol].append((exchange, orderbook))
                
                # Analyze spreads for each symbol
                for symbol, exchange_books in symbol_orderbooks.items():
                    if len(exchange_books) < 2:
                        continue
                    
                    spreads = []
                    for i, (exchange_a, book_a) in enumerate(exchange_books):
                        for j, (exchange_b, book_b) in enumerate(exchange_books[i+1:], i+1):
                            spread = self._calculate_cross_exchange_spread(
                                symbol, exchange_a, book_a, exchange_b, book_b
                            )
                            if spread:
                                spreads.append(spread)
                    
                    if spreads:
                        self.spread_cache[symbol] = spreads
                        
                        # Trigger callbacks
                        for callback in self.spread_update_callbacks:
                            try:
                                await callback(symbol, spreads)
                            except Exception as e:
                                self.logger.error(f"Error in spread update callback: {e}")
                
                await asyncio.sleep(0.1)  # 100ms cycle
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in spread analysis: {e}")
                await asyncio.sleep(1)

    def _calculate_cross_exchange_spread(
        self, 
        symbol: str, 
        exchange_a: str, 
        book_a: Dict[str, Any],
        exchange_b: str, 
        book_b: Dict[str, Any]
    ) -> Optional[CrossExchangeSpread]:
        """Calculate spread between two exchanges for a symbol."""
        try:
            # Get best prices
            bid_a = Decimal(str(book_a['bids'][0][0]))
            ask_a = Decimal(str(book_a['asks'][0][0]))
            bid_b = Decimal(str(book_b['bids'][0][0]))
            ask_b = Decimal(str(book_b['asks'][0][0]))
            
            mid_a = (bid_a + ask_a) / Decimal('2')
            mid_b = (bid_b + ask_b) / Decimal('2')
            
            # Calculate spread in basis points
            spread_bps = abs(mid_a - mid_b) / ((mid_a + mid_b) / Decimal('2')) * Decimal('10000')
            
            # Check for arbitrage opportunity (simplified)
            arbitrage_opportunity = (bid_a > ask_b * Decimal('1.001')) or (bid_b > ask_a * Decimal('1.001'))
            
            # Calculate available liquidity
            liquidity_a = sum(Decimal(str(level[1])) for level in book_a['bids'][:5])
            liquidity_b = sum(Decimal(str(level[1])) for level in book_b['bids'][:5])
            
            return CrossExchangeSpread(
                symbol=symbol,
                exchange_a=exchange_a,
                exchange_b=exchange_b,
                spread_bps=spread_bps,
                arbitrage_opportunity=arbitrage_opportunity,
                timestamp=datetime.now(timezone.utc).timestamp(),
                liquidity_a=liquidity_a,
                liquidity_b=liquidity_b
            )
            
        except (IndexError, KeyError, ValueError):
            return None

    async def _track_price_relationships(self):
        """Track correlations and relationships between symbols."""
        price_history = defaultdict(deque)  # symbol -> price history
        
        while self.running:
            try:
                # Collect current prices
                current_prices = {}
                for (exchange, symbol), price in self.price_cache.items():
                    # Use highest liquidity exchange for each symbol
                    if symbol not in current_prices:
                        current_prices[symbol] = price
                    else:
                        # Compare liquidity and use better exchange
                        current_key = (exchange, symbol)
                        current_liquidity = np.mean(self.liquidity_tracker.get(current_key, [0]))
                        
                        # Find existing exchange for comparison
                        existing_liquidity = 0
                        for (ex, sym), cached_price in self.price_cache.items():
                            if sym == symbol and cached_price == current_prices[symbol]:
                                existing_key = (ex, sym)
                                existing_liquidity = np.mean(self.liquidity_tracker.get(existing_key, [0]))
                                break
                        
                        if current_liquidity > existing_liquidity:
                            current_prices[symbol] = price
                
                # Update price history
                for symbol, price in current_prices.items():
                    price_history[symbol].append(float(price))
                    if len(price_history[symbol]) > 1000:
                        price_history[symbol].popleft()
                
                # Calculate relationships for symbols with sufficient history
                symbols = list(current_prices.keys())
                for i, symbol_a in enumerate(symbols):
                    for symbol_b in symbols[i+1:]:
                        if (len(price_history[symbol_a]) > 50 and 
                            len(price_history[symbol_b]) > 50):
                            
                            relationship = self._calculate_price_relationship(
                                symbol_a, symbol_b, price_history[symbol_a], price_history[symbol_b]
                            )
                            
                            if relationship:
                                key = (symbol_a, symbol_b)
                                self.price_relationships[key] = relationship
                
                await asyncio.sleep(1)  # 1 second cycle
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error tracking price relationships: {e}")
                await asyncio.sleep(1)

    def _calculate_price_relationship(
        self, 
        symbol_a: str, 
        symbol_b: str, 
        prices_a: deque, 
        prices_b: deque
    ) -> Optional[PriceRelationship]:
        """Calculate price relationship between two symbols."""
        try:
            # Ensure equal length
            min_length = min(len(prices_a), len(prices_b))
            a_array = np.array(list(prices_a)[-min_length:])
            b_array = np.array(list(prices_b)[-min_length:])
            
            # Calculate correlation
            correlation = np.corrcoef(a_array, b_array)[0, 1]
            
            # Calculate spread statistics
            spread = a_array - b_array
            spread_mean = Decimal(str(np.mean(spread)))
            spread_std = Decimal(str(np.std(spread)))
            
            # Current z-score
            current_spread = float(prices_a[-1]) - float(prices_b[-1])
            z_score = (current_spread - float(spread_mean)) / float(spread_std) if spread_std > 0 else None
            
            return PriceRelationship(
                symbol_a=symbol_a,
                symbol_b=symbol_b,
                correlation=float(correlation) if not np.isnan(correlation) else 0.0,
                spread_mean=spread_mean,
                spread_std=spread_std,
                last_updated=datetime.now(timezone.utc).timestamp(),
                z_score=z_score
            )
            
        except Exception:
            return None

    async def _score_reference_exchanges(self):
        """Score exchanges for their suitability as reference exchanges."""
        while self.running:
            try:
                # Calculate scores based on liquidity, spread tightness, and uptime
                for (exchange, symbol), _ in self.orderbooks.items():
                    orderbook = self.orderbooks.get((exchange, symbol))
                    if not orderbook:
                        continue
                    
                    score = 0.0
                    
                    # Liquidity score (40% of total)
                    liquidity_history = self.liquidity_tracker.get((exchange, symbol), [])
                    if liquidity_history:
                        avg_liquidity = np.mean(liquidity_history)
                        score += min(avg_liquidity / 1000, 40)  # Cap at 40 points
                    
                    # Spread tightness score (30% of total)
                    if orderbook.get('bids') and orderbook.get('asks'):
                        bid = Decimal(str(orderbook['bids'][0][0]))
                        ask = Decimal(str(orderbook['asks'][0][0]))
                        spread_bps = (ask - bid) / ((bid + ask) / 2) * Decimal('10000')
                        # Lower spread = higher score
                        spread_score = max(30 - float(spread_bps), 0)
                        score += min(spread_score, 30)
                    
                    # Uptime/reliability score (30% of total) - simplified
                    score += 30  # Assume good uptime for now
                    
                    self.reference_exchange_scores[(exchange, symbol)] = score
                
                await asyncio.sleep(10)  # Update every 10 seconds
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error scoring reference exchanges: {e}")
                await asyncio.sleep(10)

    async def _publish_enhanced_orderbook_update(
        self, 
        exchange: str, 
        symbol: str, 
        orderbook: Dict[str, Any]
    ):
        """Publish enhanced orderbook update to Redis with cross-exchange context."""
        try:
            # Basic update
            update = {
                'exchange': exchange,
                'symbol': symbol,
                'timestamp': orderbook.get('timestamp', int(datetime.now(timezone.utc).timestamp() * 1000)),
                'bids': [[str(level[0]), str(level[1])] for level in orderbook['bids']],
                'asks': [[str(level[0]), str(level[1])] for level in orderbook['asks']],
            }
            
            if orderbook['bids'] and orderbook['asks']:
                best_bid = Decimal(str(orderbook['bids'][0][0]))
                best_ask = Decimal(str(orderbook['asks'][0][0]))
                midpoint = (best_bid + best_ask) / 2
                
                update.update({
                    'best_bid': float(best_bid),
                    'best_ask': float(best_ask),
                    'midpoint': float(midpoint),
                    'spread_bps': float((best_ask - best_bid) / midpoint * Decimal('10000')),
                })
            
            # Add cross-exchange context
            spreads = self.spread_cache.get(symbol, [])
            if spreads:
                relevant_spreads = [s for s in spreads if s.exchange_a == exchange or s.exchange_b == exchange]
                if relevant_spreads:
                    update['cross_exchange_spreads'] = [
                        {
                            'other_exchange': s.exchange_b if s.exchange_a == exchange else s.exchange_a,
                            'spread_bps': float(s.spread_bps),
                            'arbitrage_opportunity': s.arbitrage_opportunity
                        }
                        for s in relevant_spreads
                    ]
            
            # Add reference exchange score
            score = self.reference_exchange_scores.get((exchange, symbol), 0)
            update['reference_score'] = score
            
            # Store orderbook data in Redis for direct access
            redis_key = f"orderbook:{exchange}:{symbol}"
            await self.redis_client.set(
                redis_key,
                json.dumps(update),
                ex=10  # Expire after 10 seconds to ensure fresh data
            )
            
            # Publish to Redis - use standard channel that RedisOrderbookManager listens to
            await self.redis_client.publish('orderbook_updates', json.dumps(update))
            await self.redis_client.publish(f'orderbook_updates:{exchange}', json.dumps(update))
            
            # Also publish to enhanced channel for advanced features
            await self.redis_client.publish('enhanced_orderbook_updates', json.dumps(update))
            await self.redis_client.publish(f'enhanced_orderbook_updates:{exchange}', json.dumps(update))
            
        except Exception as e:
            self.logger.error(f"Error publishing enhanced orderbook update: {e}")

    # Public API methods for strategies to use
    
    def get_best_reference_exchange(self, symbol: str) -> Optional[str]:
        """Get the best reference exchange for a symbol based on scoring."""
        best_exchange = None
        best_score = 0
        
        for (exchange, sym), score in self.reference_exchange_scores.items():
            if sym == symbol and score > best_score:
                best_score = score
                best_exchange = exchange
                
        return best_exchange
    
    def get_cross_exchange_spreads(self, symbol: str) -> List[CrossExchangeSpread]:
        """Get current cross-exchange spreads for a symbol."""
        return self.spread_cache.get(symbol, [])
    
    def get_price_relationships(self, symbol: str) -> List[PriceRelationship]:
        """Get price relationships involving this symbol."""
        relationships = []
        for (sym_a, sym_b), relationship in self.price_relationships.items():
            if sym_a == symbol or sym_b == symbol:
                relationships.append(relationship)
        return relationships
    
    def get_synthetic_price(self, components: List[Tuple[str, float]]) -> Optional[Decimal]:
        """
        Calculate synthetic instrument price from components.
        
        Args:
            components: List of (symbol, weight) tuples
            
        Returns:
            Synthetic price if all components available
        """
        try:
            total_price = Decimal('0')
            total_weight = Decimal('0')
            
            for symbol, weight in components:
                # Get best price from highest-scored exchange
                best_exchange = self.get_best_reference_exchange(symbol)
                if not best_exchange:
                    return None
                
                price = self.price_cache.get((best_exchange, symbol))
                if price is None:
                    return None
                
                weight_decimal = Decimal(str(weight))
                total_price += price * weight_decimal
                total_weight += weight_decimal
            
            return total_price / total_weight if total_weight > 0 else None
            
        except Exception as e:
            self.logger.error(f"Error calculating synthetic price: {e}")
            return None
    
    async def stop(self):
        """Stop the market data service."""
        if not self.running:
            return
            
        self.running = False
        self.shutdown_event.set()
        
        # Cancel all tasks
        for task in self.tasks:
            if not task.done():
                task.cancel()
        
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        
        # Close exchanges
        for exchange in self.exchanges.values():
            try:
                await exchange.close()
            except Exception as e:
                self.logger.error(f"Error closing exchange: {e}")
        
        # Close Redis connection
        if self.redis_client:
            await self.redis_client.close()
        
        self.logger.info("Multi-Symbol Market Data Service stopped")
    
    async def _autodiscover_available_symbols(self) -> Dict[str, List[str]]:
        """Autodiscover which symbols are actually available on each exchange.
        
        This method now uses the actual symbols from symbol_configs instead of 
        hardcoding only BERA pairs.
        """
        discovered_symbols = {}
        
        # Get base symbols from our configs
        configured_base_symbols = set()
        for config in self.symbol_configs.values():
            configured_base_symbols.add(config.base_symbol)
        
        self.logger.info(f"🔍 Autodiscovering for configured symbols: {configured_base_symbols}")
        
        # Common reference pairs per exchange (with correct TRY direction)
        exchange_reference_pairs = {
            'binance': ['BTC/USDT', 'ETH/USDT', 'BNB/USDT', 'USDC/USDT', 'USDT/TRY', 'FDUSD/USDT'],
            'bybit': ['BTC/USDT', 'ETH/USDT', 'USDC/USDT'], 
            'mexc': ['BTC/USDT', 'ETH/USDT', 'USDC/USDT'],
            'bitget': ['BTC/USDT', 'ETH/USDT', 'USDC/USDT'],
            'gateio': ['BTC/USDT', 'ETH/USDT', 'USDC/USDT'],
            'kucoin': ['BTC/USDT', 'ETH/USDT', 'USDC/USDT'],
            'hyperliquid': []  # Hyperliquid doesn't have USDT pairs
        }
        
        # Build exchange-type specific symbols
        exchange_types = [
            'binance_spot', 'binance_perp', 'bybit_spot', 'bybit_perp',
            'mexc_spot', 'bitget_spot', 'gateio_spot', 'kucoin_spot',
            'hyperliquid_spot', 'hyperliquid_perp'
        ]
        
        for exchange_type in exchange_types:
            available_symbols = []
            base_exchange = exchange_type.split('_')[0]
            
            # Add all configured symbols - assume they're available on spot exchanges
            # (the actual availability will be verified when placing orders)
            if 'spot' in exchange_type:
                for symbol in configured_base_symbols:
                    available_symbols.append(symbol)
            elif 'perp' in exchange_type:
                # For perps, only add USDT pairs (most common)
                for symbol in configured_base_symbols:
                    if '/USDT' in symbol:
                        available_symbols.append(symbol)
            
            # Add reference pairs (only for spot exchanges)
            if 'spot' in exchange_type:
                reference_pairs = exchange_reference_pairs.get(base_exchange, [])
                available_symbols.extend(reference_pairs)
            
            discovered_symbols[exchange_type] = list(set(available_symbols))
            
            self.logger.info(f"🔍 {exchange_type}: Autodiscovered {len(available_symbols)} symbols: {available_symbols[:5]}...")
        
        # Also maintain base exchange names for backward compatibility
        for base_exchange in ['binance', 'bybit', 'mexc', 'bitget', 'gateio', 'kucoin', 'hyperliquid']:
            # Combine spot and perp symbols for legacy lookups
            spot_symbols = discovered_symbols.get(f"{base_exchange}_spot", [])
            perp_symbols = discovered_symbols.get(f"{base_exchange}_perp", [])
            discovered_symbols[base_exchange] = list(set(spot_symbols + perp_symbols))
        
        return discovered_symbols
    
    def _symbols_match(self, symbol1: str, symbol2: str) -> bool:
        """Check if two symbols represent the same trading pair."""
        # Normalize symbols for comparison
        norm1 = symbol1.replace('/', '').replace(':', '').replace('_', '').upper()
        norm2 = symbol2.replace('/', '').replace(':', '').replace('_', '').upper()
        return norm1 == norm2
    
    def _normalize_symbol_for_exchange(self, symbol: str, exchange_key: str) -> str:
        """Normalize symbol for specific exchange."""
        base_exchange = exchange_key.split('_')[0]
        
        # Handle special cases first
        if symbol == 'BERA/USDT':
            if base_exchange == 'hyperliquid':
                if 'perp' in exchange_key:
                    return 'BERA/USDC:USDC'  # Hyperliquid perp
                else:
                    return 'BERA/USDC'  # Hyperliquid spot
        
        # Handle BERA/USDC for hyperliquid perp (needs :USDC suffix)
        if symbol == 'BERA/USDC' and base_exchange == 'hyperliquid' and 'perp' in exchange_key:
            return 'BERA/USDC:USDC'  # Hyperliquid perp requires :USDC suffix
        
        # For perpetual futures, remove slashes for most exchanges
        if 'perp' in exchange_key:
            if base_exchange in ['binance', 'bybit']:
                # Remove slashes: BERA/USDT -> BERAUSDT
                return symbol.replace('/', '').replace(':', '')
            elif base_exchange == 'hyperliquid':
                # Keep format but use USDC
                if symbol.endswith('/USDT'):
                    return symbol.replace('/USDT', '/USDC:USDC')
                return symbol
        
        # For spot exchanges
        if base_exchange == 'binance':
            # Remove slashes for all pairs: BERA/USDT -> BERAUSDT, USDT/TRY -> USDTTRY
            return symbol.replace('/', '').replace(':', '')
        elif base_exchange == 'gateio':
            # Use underscores: BERA/USDT -> BERA_USDT
            return symbol.replace('/', '_').replace(':', '_')
        elif base_exchange == 'hyperliquid':
            # Use USDC instead of USDT for Hyperliquid
            if symbol.endswith('/USDT'):
                return symbol.replace('/USDT', '/USDC')
            return symbol
        
        # Default: keep standard format for other exchanges (Bybit spot, MEXC, Bitget)
        return symbol
    
    def _denormalize_symbol(self, normalized_symbol: str, exchange_key: str) -> str:
        """Convert normalized symbol back to standard format."""
        base_exchange = exchange_key.split('_')[0]
        
        # Reverse the normalization logic
        if base_exchange == 'binance':
            # BERAUSDT -> BERA/USDT, USDTTRY -> USDT/TRY
            if 'BERA' in normalized_symbol:
                if normalized_symbol == 'BERAUSDT':
                    return 'BERA/USDT'
                elif normalized_symbol == 'BERAUSDC':
                    return 'BERA/USDC'
                elif normalized_symbol == 'BERABTC':
                    return 'BERA/BTC'
                elif normalized_symbol == 'BERABNB':
                    return 'BERA/BNB'
                elif normalized_symbol == 'BERATRY':
                    return 'BERA/TRY'
                elif normalized_symbol == 'BERAFDUSD':
                    return 'BERA/FDUSD'
            elif normalized_symbol == 'USDTTRY':
                return 'USDT/TRY'
            elif normalized_symbol == 'BTCUSDT':
                return 'BTC/USDT'
            elif normalized_symbol == 'ETHUSDT':
                return 'ETH/USDT'
            elif normalized_symbol == 'BNBUSDT':
                return 'BNB/USDT'
            elif normalized_symbol == 'USDCUSDT':
                return 'USDC/USDT'
            elif normalized_symbol == 'FDUSDUSDT':
                return 'FDUSD/USDT'
        elif base_exchange == 'gateio':
            # BERA_USDT -> BERA/USDT
            return normalized_symbol.replace('_', '/')
        elif base_exchange == 'hyperliquid':
            # Handle Hyperliquid's special cases
            if normalized_symbol == 'BERA/USDC:USDC':
                return 'BERA/USDC'  # Map back to standard USDC format
            elif normalized_symbol == 'BERA/USDC':
                return 'BERA/USDC'  # Already correct format
        
        # For other exchanges or if no specific rule, return as-is
        return normalized_symbol
    
    async def _monitor_symbol_orderbook_with_connector(self, exchange_key: str, symbol: str, connector: Any):
        """Monitor orderbook using connector and publish to Redis for other services."""
        self.logger.info(f"📊 Monitoring {exchange_key} {symbol} using direct connector")
        
        while self.running:
            try:
                # Get orderbook directly from connector
                normalized_symbol = self._normalize_symbol_for_exchange(symbol, exchange_key)
                
                # Use connector to get orderbook
                orderbook = await connector.get_orderbook(normalized_symbol)
                
                if orderbook and orderbook.get('bids') and orderbook.get('asks'):
                    # Store orderbook data locally
                    key = (exchange_key, symbol)
                    self.orderbooks[key] = orderbook
                    
                    # Update price cache
                    best_bid = Decimal(str(orderbook['bids'][0][0]))
                    best_ask = Decimal(str(orderbook['asks'][0][0]))
                    midprice = (best_bid + best_ask) / Decimal('2')
                    self.price_cache[key] = midprice
                    
                    # Track liquidity for reference exchange scoring
                    bid_liquidity = sum(Decimal(str(level[1])) for level in orderbook['bids'][:10])
                    self.liquidity_tracker[key].append(float(bid_liquidity))
                    if len(self.liquidity_tracker[key]) > 100:
                        self.liquidity_tracker[key].pop(0)
                    
                    # CRITICAL: Publish to Redis so other services can access this data
                    await self._publish_enhanced_orderbook_update(exchange_key, symbol, orderbook)
                    
                    # Trigger callbacks
                    for callback in self.price_update_callbacks:
                        try:
                            await callback(exchange_key, symbol, orderbook)
                        except Exception as e:
                            self.logger.error(f"Error in price update callback: {e}")
                
                # Check every 1 second for direct connector fetching
                await asyncio.sleep(1.0)
                            
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error monitoring {exchange_key} {symbol}: {e}")
                await asyncio.sleep(2)
