"""
Enhanced Aggregated Orderbook Manager - Supports combined books with liquidity shares for the new strategy.

This module implements the data processing requirements from NEW_BOT.txt:
- BASE/USDT combined book across selected venues
- BASE/NON-USDT quote books (BASE/BTC, BASE/ETH)  
- NON-USDT/USDT books for cross-conversion
- Per-level aggregation with component liquidity shares
- Volume statistics integration
"""

import asyncio
import json
import time
from dataclasses import dataclass, field
from decimal import Decimal
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Tuple, Set
from collections import defaultdict, deque
import logging
import redis.asyncio as redis

from .redis_orderbook_manager import RedisOrderbookManager
# Removed VolumeConfig import - using existing proven coefficient system

logger = logging.getLogger(__name__)


@dataclass
class LiquidityLevel:
    """Represents a single orderbook level with liquidity share tracking."""
    price: Decimal
    total_quantity: Decimal
    exchange_shares: Dict[str, Decimal] = field(default_factory=dict)  # exchange -> quantity
    weighted_price: Optional[Decimal] = None  # Volume-weighted price at this level
    
    def get_exchange_percentage(self, exchange: str) -> float:
        """Get percentage share of this level's liquidity from an exchange."""
        if self.total_quantity == 0:
            return 0.0
        quantity = self.exchange_shares.get(exchange, Decimal('0'))
        return float(quantity / self.total_quantity * 100)
    
    def get_dominant_exchange(self) -> Optional[str]:
        """Get exchange with largest share at this level."""
        if not self.exchange_shares:
            return None
        return max(self.exchange_shares.keys(), key=lambda x: self.exchange_shares[x])


@dataclass
class AggregatedOrderbook:
    """Aggregated orderbook with liquidity share tracking."""
    symbol: str
    bids: List[LiquidityLevel] = field(default_factory=list)
    asks: List[LiquidityLevel] = field(default_factory=list)
    contributing_exchanges: Set[str] = field(default_factory=set)
    last_updated: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    aggregation_method: str = "volume_weighted"
    
    def get_best_bid(self) -> Optional[LiquidityLevel]:
        """Get best bid level."""
        return self.bids[0] if self.bids else None
    
    def get_best_ask(self) -> Optional[LiquidityLevel]:
        """Get best ask level."""
        return self.asks[0] if self.asks else None
    
    def get_midpoint(self) -> Optional[Decimal]:
        """Get midpoint price."""
        best_bid = self.get_best_bid()
        best_ask = self.get_best_ask()
        
        if best_bid and best_ask:
            return (best_bid.price + best_ask.price) / Decimal('2')
        return None
    
    def get_liquidity_at_level(self, side: str, level_index: int) -> Optional[LiquidityLevel]:
        """Get liquidity level at specific index."""
        levels = self.bids if side == 'bid' else self.asks
        if 0 <= level_index < len(levels):
            return levels[level_index]
        return None
    
    def calculate_wapq(self, side: str, quantity: Decimal) -> Optional[Tuple[Decimal, Dict[str, Decimal]]]:
        """
        Calculate Volume-Weighted Average Price for Quantity (WAPQ) with exchange attribution.
        
        Args:
            side: 'bid' or 'ask'
            quantity: Quantity to price
            
        Returns:
            Tuple of (wapq_price, exchange_quantities) or None if insufficient liquidity
        """
        levels = self.asks if side == 'bid' else self.bids  # Cross sides for execution
        
        if not levels:
            return None
        
        total_cost = Decimal('0')
        remaining_quantity = quantity
        exchange_quantities: Dict[str, Decimal] = defaultdict(lambda: Decimal('0'))
        
        for level in levels:
            if remaining_quantity <= 0:
                break
            
            # Take quantity from this level
            take_quantity = min(remaining_quantity, level.total_quantity)
            
            # Distribute across exchanges proportionally
            for exchange, exchange_qty in level.exchange_shares.items():
                if exchange_qty > 0:
                    exchange_proportion = exchange_qty / level.total_quantity
                    exchange_take = take_quantity * exchange_proportion
                    exchange_quantities[exchange] += exchange_take
                    
                    # Add to total cost
                    total_cost += exchange_take * level.price
            
            remaining_quantity -= take_quantity
        
        if quantity - remaining_quantity > 0:
            # Calculate WAPQ
            filled_quantity = quantity - remaining_quantity
            wapq = total_cost / filled_quantity
            return wapq, dict(exchange_quantities)
        
        return None


@dataclass
class CrossCurrencyPath:
    """Represents a cross-currency conversion path."""
    from_symbol: str  # e.g., 'BERA/BTC'
    to_symbol: str    # e.g., 'BERA/USDT' 
    intermediate_symbol: str  # e.g., 'BTC/USDT'
    conversion_rate: Optional[Decimal] = None
    last_updated: Optional[datetime] = None
    
    def is_valid(self, max_age_seconds: int = 10) -> bool:
        """Check if conversion path is valid and fresh."""
        if self.conversion_rate is None or self.last_updated is None:
            return False
        
        age = (datetime.now(timezone.utc) - self.last_updated).total_seconds()
        return age <= max_age_seconds


class EnhancedAggregatedOrderbookManager:
    """
    Enhanced orderbook manager with aggregation and cross-currency support.
    
    Features:
    - Multi-venue orderbook aggregation with liquidity shares
    - Cross-currency conversion path management
    - Volume coefficient integration
    - Smart pricing selection (aggregated vs component venues)
    - Real-time WAPQ calculations with exchange attribution
    """
    
    def __init__(
        self,
        symbols: List[str],
        exchanges: List[str],
        redis_url: str = "redis://localhost:6379",
        exchange_connectors: Optional[Dict[str, Any]] = None
    ):
        self.symbols = symbols
        self.exchanges = exchanges
        self.redis_url = redis_url
        self.exchange_connectors = exchange_connectors or {}
        self.logger = logger
        
        # Base orderbook manager (fallback to Redis if exchange connectors not available)
        self.redis_manager = RedisOrderbookManager(redis_url)
        
        # Direct market data from exchange connectors
        self.use_direct_connectors = bool(self.exchange_connectors)
        
        # Override redis_manager's get_best_bid_ask method if using direct connectors
        if self.use_direct_connectors:
            self.redis_manager.get_best_bid_ask = self._get_best_bid_ask_override
        
        # Note: Volume coefficient functionality moved to strategy level using existing proven system
        
        # Enhanced storage
        self.aggregated_books: Dict[str, AggregatedOrderbook] = {}  # symbol -> aggregated book
        self.cross_currency_paths: Dict[Tuple[str, str], CrossCurrencyPath] = {}
        
        # Direct connector data cache
        self.connector_orderbooks: Dict[Tuple[str, str], Dict[str, Any]] = {}  # (exchange, symbol) -> orderbook
        
        # Real-time processing
        self.aggregation_tasks: Dict[str, asyncio.Task] = {}
        self.connector_fetch_task: Optional[asyncio.Task] = None
        self.running = False
        
        # Performance tracking
        self.aggregation_stats = {
            'books_aggregated': 0,
            'wapq_calculations': 0,
            'cross_conversions': 0,
            'last_aggregation_time_ms': 0.0
        }
        
        # Update callbacks
        self.aggregation_callbacks: List[Any] = []  # Callbacks when aggregated books update
        
    async def start(self):
        """Start the enhanced aggregated orderbook manager."""
        if self.running:
            return
            
        self.running = True
        self.logger.info("Starting Enhanced Aggregated Orderbook Manager")
        
        # Start base Redis manager
        await self.redis_manager.start()
        
        # Volume tracking now handled by existing proven system at strategy level
        
        # Register for orderbook updates if using Redis
        if not self.use_direct_connectors:
            self.redis_manager.register_update_callback(self._on_orderbook_update)
        
        # Start aggregation tasks for each symbol
        for symbol in self.symbols:
            task = asyncio.create_task(self._aggregate_symbol_loop(symbol))
            self.aggregation_tasks[symbol] = task
        
        # Start cross-currency path maintenance
        asyncio.create_task(self._maintain_cross_currency_paths())
        
        # Start direct connector fetching if using connectors
        if self.use_direct_connectors:
            self.connector_fetch_task = asyncio.create_task(self._fetch_connector_data_loop())
            self.logger.info(f"Using direct exchange connectors: {list(self.exchange_connectors.keys())}")
        else:
            self.logger.info("Using Redis orderbook manager for market data")
        
        self.logger.info(f"Enhanced Aggregated Orderbook Manager started with {len(self.symbols)} symbols")
    
    async def stop(self):
        """Stop the enhanced aggregated orderbook manager."""
        if not self.running:
            return
            
        self.running = False
        
        # Cancel aggregation tasks
        for task in self.aggregation_tasks.values():
            task.cancel()
        
        # Cancel connector fetch task
        if self.connector_fetch_task:
            self.connector_fetch_task.cancel()
        
        tasks_to_wait = list(self.aggregation_tasks.values())
        if self.connector_fetch_task:
            tasks_to_wait.append(self.connector_fetch_task)
        
        if tasks_to_wait:
            await asyncio.gather(*tasks_to_wait, return_exceptions=True)
        
        # Stop components
        if not self.use_direct_connectors:
            await self.redis_manager.stop()
        
        self.logger.info("Enhanced Aggregated Orderbook Manager stopped")
    
    async def _on_orderbook_update(self, exchange: str, update: Dict[str, Any]):
        """Handle orderbook updates and update volume statistics."""
        try:
            symbol = update.get('symbol')
            if not symbol or symbol not in self.symbols:
                return
            
            # Volume tracking now handled by existing proven system at strategy level
            
            # Trigger immediate aggregation for this symbol
            # We don't await this to avoid blocking the callback
            asyncio.create_task(self._aggregate_symbol_immediate(symbol))
            
        except Exception as e:
            self.logger.error(f"Error in orderbook update callback: {e}")
    
    async def _aggregate_symbol_loop(self, symbol: str):
        """Continuous aggregation loop for a specific symbol."""
        while self.running:
            try:
                await self._aggregate_symbol_immediate(symbol)
                await asyncio.sleep(0.1)  # 100ms aggregation cycle
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in aggregation loop for {symbol}: {e}")
                await asyncio.sleep(1)
    
    async def _aggregate_symbol_immediate(self, symbol: str):
        """Immediately aggregate orderbook for a symbol."""
        try:
            start_time = time.perf_counter()
            
            # Collect orderbooks from all exchanges for this symbol
            exchange_books = {}
            for exchange in self.exchanges:
                # Try to resolve symbol for this exchange
                resolved_symbol = self._resolve_symbol_for_exchange(symbol, exchange)
                orderbook = self.redis_manager.get_orderbook(exchange, resolved_symbol)
                if orderbook and orderbook.get('bids') and orderbook.get('asks'):
                    exchange_books[exchange] = orderbook
            
            if not exchange_books:
                return
            
            # Aggregate orderbook levels with liquidity shares
            aggregated_book = self._aggregate_orderbook_levels(symbol, exchange_books)
            
            if aggregated_book:
                self.aggregated_books[symbol] = aggregated_book
                
                # Update stats
                self.aggregation_stats['books_aggregated'] += 1
                self.aggregation_stats['last_aggregation_time_ms'] = (time.perf_counter() - start_time) * 1000
                
                # Trigger callbacks
                for callback in self.aggregation_callbacks:
                    try:
                        if asyncio.iscoroutinefunction(callback):
                            await callback(symbol, aggregated_book)
                        else:
                            callback(symbol, aggregated_book)
                    except Exception as e:
                        self.logger.error(f"Error in aggregation callback: {e}")
        
        except Exception as e:
            self.logger.error(f"Error aggregating {symbol}: {e}")
    
    def _aggregate_orderbook_levels(
        self, 
        symbol: str, 
        exchange_books: Dict[str, Dict[str, Any]]
    ) -> AggregatedOrderbook:
        """
        Aggregate orderbook levels from multiple exchanges with liquidity share tracking.
        
        Implements per-level aggregation as described in NEW_BOT.txt:
        "merge all selected exchanges; track component liquidity shares per level"
        """
        # Collect all bid and ask levels from all exchanges
        all_bids: Dict[Decimal, Dict[str, Decimal]] = defaultdict(lambda: defaultdict(Decimal))  # price -> exchange -> quantity
        all_asks: Dict[Decimal, Dict[str, Decimal]] = defaultdict(lambda: defaultdict(Decimal))  # price -> exchange -> quantity
        
        contributing_exchanges = set()
        
        for exchange, orderbook in exchange_books.items():
            contributing_exchanges.add(exchange)
            
            # Process bids
            for bid_level in orderbook.get('bids', []):
                try:
                    price = Decimal(str(bid_level[0]))
                    quantity = Decimal(str(bid_level[1]))
                    all_bids[price][exchange] += quantity
                except (ValueError, IndexError):
                    continue
            
            # Process asks
            for ask_level in orderbook.get('asks', []):
                try:
                    price = Decimal(str(ask_level[0]))
                    quantity = Decimal(str(ask_level[1]))
                    all_asks[price][exchange] += quantity
                except (ValueError, IndexError):
                    continue
        
        # Create aggregated levels with liquidity shares
        aggregated_bids = []
        for price in sorted(all_bids.keys(), reverse=True):  # Best bid first (highest price)
            exchange_quantities = all_bids[price]
            total_quantity = sum(exchange_quantities.values())
            
            if total_quantity > 0:
                level = LiquidityLevel(
                    price=price,
                    total_quantity=total_quantity,
                    exchange_shares=dict(exchange_quantities)
                )
                aggregated_bids.append(level)
        
        aggregated_asks = []
        for price in sorted(all_asks.keys()):  # Best ask first (lowest price)
            exchange_quantities = all_asks[price]
            total_quantity = sum(exchange_quantities.values())
            
            if total_quantity > 0:
                level = LiquidityLevel(
                    price=price,
                    total_quantity=total_quantity,
                    exchange_shares=dict(exchange_quantities)
                )
                aggregated_asks.append(level)
        
        return AggregatedOrderbook(
            symbol=symbol,
            bids=aggregated_bids,
            asks=aggregated_asks,
            contributing_exchanges=contributing_exchanges,
            aggregation_method="volume_weighted"
        )
    
    def _resolve_symbol_for_exchange(self, symbol: str, exchange: str) -> str:
        """Resolve symbol format for specific exchange."""
        # Use similar logic to base_strategy.py
        base_exchange = exchange.split('_')[0]
        
        # Handle special cases for different exchanges
        if base_exchange == 'hyperliquid':
            if symbol == 'BERA/USDT':
                return 'BERA/USDC:USDC'  # Hyperliquid uses USDC
        elif base_exchange == 'binance':
            # Binance removes slashes for spot
            return symbol.replace('/', '')
        elif base_exchange == 'gateio':
            # Gate.io uses underscores
            return symbol.replace('/', '_')
        elif base_exchange == 'bitfinex':
            # Bitfinex uses USDt notation
            if symbol.endswith('/USDT'):
                base = symbol.split('/')[0]
                return f"{base}/USDt"
        
        # Default format
        return symbol
    
    async def _maintain_cross_currency_paths(self):
        """Maintain cross-currency conversion paths."""
        while self.running:
            try:
                # Update conversion paths for all symbol combinations
                for i, symbol_a in enumerate(self.symbols):
                    for symbol_b in self.symbols[i+1:]:
                        await self._update_cross_currency_path(symbol_a, symbol_b)
                
                await asyncio.sleep(5)  # Update every 5 seconds
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error maintaining cross-currency paths: {e}")
                await asyncio.sleep(10)
    
    async def _update_cross_currency_path(self, symbol_a: str, symbol_b: str):
        """Update cross-currency conversion path between two symbols."""
        try:
            # Parse symbols
            parts_a = symbol_a.split('/')
            parts_b = symbol_b.split('/')
            
            if len(parts_a) != 2 or len(parts_b) != 2:
                return
            
            base_a, quote_a = parts_a
            base_b, quote_b = parts_b
            
            # If same base currency, convert quote currencies
            if base_a == base_b:
                intermediate_symbol = f"{quote_a}/{quote_b}"
                
                # Get conversion rate
                aggregated_book = self.get_aggregated_orderbook(intermediate_symbol)
                if aggregated_book:
                    midpoint = aggregated_book.get_midpoint()
                    if midpoint:
                        path = CrossCurrencyPath(
                            from_symbol=symbol_a,
                            to_symbol=symbol_b,
                            intermediate_symbol=intermediate_symbol,
                            conversion_rate=midpoint,
                            last_updated=datetime.now(timezone.utc)
                        )
                        self.cross_currency_paths[(symbol_a, symbol_b)] = path
            
            # If same quote currency, convert base currencies
            elif quote_a == quote_b:
                intermediate_symbol = f"{base_a}/{base_b}"
                
                aggregated_book = self.get_aggregated_orderbook(intermediate_symbol)
                if aggregated_book:
                    midpoint = aggregated_book.get_midpoint()
                    if midpoint:
                        path = CrossCurrencyPath(
                            from_symbol=symbol_a,
                            to_symbol=symbol_b,
                            intermediate_symbol=intermediate_symbol,
                            conversion_rate=midpoint,
                            last_updated=datetime.now(timezone.utc)
                        )
                        self.cross_currency_paths[(symbol_a, symbol_b)] = path
        
        except Exception as e:
            self.logger.debug(f"Could not create cross-currency path {symbol_a} -> {symbol_b}: {e}")
    
    # Public API methods
    
    def get_aggregated_orderbook(self, symbol: str) -> Optional[AggregatedOrderbook]:
        """Get aggregated orderbook for a symbol."""
        return self.aggregated_books.get(symbol)
    
    def get_aggregated_wapq(
        self, 
        symbol: str, 
        side: str, 
        quantity: Decimal
    ) -> Optional[Tuple[Decimal, Dict[str, Decimal]]]:
        """
        Get WAPQ from aggregated orderbook with exchange attribution.
        
        Args:
            symbol: Trading symbol
            side: 'bid' or 'ask'
            quantity: Quantity to price
            
        Returns:
            Tuple of (wapq_price, exchange_quantities) or None
        """
        aggregated_book = self.get_aggregated_orderbook(symbol)
        if not aggregated_book:
            return None
        
        result = aggregated_book.calculate_wapq(side, quantity)
        if result:
            self.aggregation_stats['wapq_calculations'] += 1
        
        return result
    
    def get_component_venue_wapq(
        self,
        symbol: str,
        side: str,
        quantity: Decimal,
        selected_exchanges: Optional[List[str]] = None
    ) -> Optional[Tuple[Decimal, Dict[str, Decimal]]]:
        """
        Get WAPQ from selected component venues only.
        
        Implements smart pricing selection from NEW_BOT.txt:
        "Single/selected venues (e.g., 'Gate only', or any subset)"
        """
        if selected_exchanges is None:
            selected_exchanges = self.exchanges
        
        # Filter aggregated book to only include selected exchanges
        aggregated_book = self.get_aggregated_orderbook(symbol)
        if not aggregated_book:
            return None
        
        # Create filtered book with only selected exchanges
        filtered_bids = []
        for level in aggregated_book.bids:
            filtered_shares = {
                ex: qty for ex, qty in level.exchange_shares.items() 
                if ex in selected_exchanges
            }
            if filtered_shares:
                total_qty = sum(filtered_shares.values())
                filtered_level = LiquidityLevel(
                    price=level.price,
                    total_quantity=total_qty,
                    exchange_shares=filtered_shares
                )
                filtered_bids.append(filtered_level)
        
        filtered_asks = []
        for level in aggregated_book.asks:
            filtered_shares = {
                ex: qty for ex, qty in level.exchange_shares.items() 
                if ex in selected_exchanges
            }
            if filtered_shares:
                total_qty = sum(filtered_shares.values())
                filtered_level = LiquidityLevel(
                    price=level.price,
                    total_quantity=total_qty,
                    exchange_shares=filtered_shares
                )
                filtered_asks.append(filtered_level)
        
        # Calculate WAPQ using filtered levels
        levels = filtered_asks if side == 'bid' else filtered_bids
        
        if not levels:
            return None
        
        total_cost = Decimal('0')
        remaining_quantity = quantity
        exchange_quantities: Dict[str, Decimal] = defaultdict(lambda: Decimal('0'))
        
        for level in levels:
            if remaining_quantity <= 0:
                break
            
            take_quantity = min(remaining_quantity, level.total_quantity)
            
            for exchange, exchange_qty in level.exchange_shares.items():
                if exchange_qty > 0:
                    exchange_proportion = exchange_qty / level.total_quantity
                    exchange_take = take_quantity * exchange_proportion
                    exchange_quantities[exchange] += exchange_take
                    total_cost += exchange_take * level.price
            
            remaining_quantity -= take_quantity
        
        if quantity - remaining_quantity > 0:
            filled_quantity = quantity - remaining_quantity
            wapq = total_cost / filled_quantity
            return wapq, dict(exchange_quantities)
        
        return None
    
    def get_cross_currency_conversion(self, from_symbol: str, to_symbol: str) -> Optional[Decimal]:
        """
        Get cross-currency conversion rate.
        
        Implements cross-conversion from NEW_BOT.txt:
        "NON-USDT/USDT books for cross-conversion"
        """
        path = self.cross_currency_paths.get((from_symbol, to_symbol))
        if path and path.is_valid():
            self.aggregation_stats['cross_conversions'] += 1
            return path.conversion_rate
        
        # Try reverse path
        reverse_path = self.cross_currency_paths.get((to_symbol, from_symbol))
        if reverse_path and reverse_path.is_valid() and reverse_path.conversion_rate:
            self.aggregation_stats['cross_conversions'] += 1
            return Decimal('1') / reverse_path.conversion_rate
        
        return None
    
    def get_liquidity_shares_at_price(
        self, 
        symbol: str, 
        side: str, 
        target_price: Decimal
    ) -> Dict[str, float]:
        """
        Get liquidity shares from each exchange at a specific price level.
        
        Used for smart routing as described in NEW_BOT.txt:
        "Route orders using the pricing decision's component liquidity weights"
        """
        aggregated_book = self.get_aggregated_orderbook(symbol)
        if not aggregated_book:
            return {}
        
        levels = aggregated_book.bids if side == 'bid' else aggregated_book.asks
        
        # Find closest level to target price
        closest_level = None
        min_price_diff = None
        
        for level in levels:
            price_diff = abs(level.price - target_price)
            if min_price_diff is None or price_diff < min_price_diff:
                min_price_diff = price_diff
                closest_level = level
        
        if closest_level:
            # Return percentage shares
            return {
                exchange: closest_level.get_exchange_percentage(exchange)
                for exchange in closest_level.exchange_shares.keys()
            }
        
        return {}
    
    # Volume coefficient methods removed - using existing proven system at strategy level
    
    def register_aggregation_callback(self, callback):
        """Register callback for aggregated orderbook updates."""
        self.aggregation_callbacks.append(callback)
    
    async def _fetch_connector_data_loop(self):
        """Background loop to fetch orderbook data from exchange connectors."""
        while self.running:
            try:
                for symbol in self.symbols:
                    for exchange in self.exchanges:
                        if exchange in self.exchange_connectors:
                            await self._fetch_and_cache_orderbook(exchange, symbol)
                
                # Fetch every 100ms for real-time data
                await asyncio.sleep(0.1)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in connector data fetch loop: {e}")
                await asyncio.sleep(1)
    
    async def _fetch_and_cache_orderbook(self, exchange: str, symbol: str):
        """Fetch and cache orderbook data from a specific exchange."""
        connector = self.exchange_connectors.get(exchange)
        if not connector:
            return
        
        try:
            # Normalize symbol for exchange
            from ..bot_manager.strategies.base_strategy import BaseStrategy
            normalized_symbol = self._normalize_symbol_for_exchange(symbol, exchange)
            
            # Get orderbook from exchange connector
            orderbook = await connector.fetch_order_book(normalized_symbol, limit=20)
            
            if orderbook and orderbook.get('bids') and orderbook.get('asks'):
                # Cache the orderbook data
                cache_key = (exchange, symbol)
                self.connector_orderbooks[cache_key] = {
                    'bids': orderbook['bids'],
                    'asks': orderbook['asks'],
                    'timestamp': orderbook.get('timestamp', int(time.time() * 1000)),
                    'symbol': symbol,
                    'exchange': exchange
                }
                
        except Exception as e:
            self.logger.debug(f"Error fetching orderbook from {exchange} for {symbol}: {e}")
    
    def _normalize_symbol_for_exchange(self, symbol: str, exchange: str) -> str:
        """Normalize symbol for exchange (simplified version of base strategy method)."""
        base_exchange = exchange.split('_')[0]
        
        # Handle special cases first
        if symbol == 'BERA/USDT':
            if base_exchange == 'hyperliquid':
                return 'BERA/USDC:USDC'
        
        # For perpetual futures
        if 'perp' in exchange:
            if base_exchange in ['binance', 'bybit']:
                if symbol == 'BERA/USDT':
                    return 'BERAUSDT'
        
        # For spot exchanges
        if base_exchange == 'binance':
            return symbol.replace('/', '')
        elif base_exchange == 'gateio':
            return symbol.replace('/', '_')
        
        # Default: return as-is
        return symbol
    
    def _get_best_bid_ask_override(self, exchange: str, symbol: str) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        """Override method for RedisOrderbookManager to use cached connector data."""
        if not self.use_direct_connectors:
            return None, None
            
        cache_key = (exchange, symbol)
        orderbook = self.connector_orderbooks.get(cache_key)
        
        if not orderbook:
            return None, None
        
        try:
            bids = orderbook.get('bids', [])
            asks = orderbook.get('asks', [])
            
            if not bids or not asks:
                return None, None
            
            best_bid = Decimal(str(bids[0][0])) if bids else None
            best_ask = Decimal(str(asks[0][0])) if asks else None
            
            return best_bid, best_ask
            
        except Exception as e:
            self.logger.debug(f"Error getting cached orderbook for {exchange} {symbol}: {e}")
            return None, None
    
    async def get_best_bid_ask_direct(self, exchange: str, symbol: str) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        """Get best bid/ask directly from exchange connector."""
        if not self.use_direct_connectors:
            return None, None
            
        connector = self.exchange_connectors.get(exchange)
        if not connector:
            return None, None
        
        try:
            # Get orderbook from exchange connector
            orderbook = await connector.fetch_order_book(symbol, limit=1)
            
            if not orderbook or not orderbook.get('bids') or not orderbook.get('asks'):
                return None, None
            
            best_bid = Decimal(str(orderbook['bids'][0][0])) if orderbook['bids'] else None
            best_ask = Decimal(str(orderbook['asks'][0][0])) if orderbook['asks'] else None
            
            return best_bid, best_ask
            
        except Exception as e:
            self.logger.debug(f"Error getting orderbook from {exchange} for {symbol}: {e}")
            return None, None
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get comprehensive statistics."""
        base_stats = self.aggregation_stats.copy()
        
        # Volume stats now handled by existing proven system at strategy level
        
        # Add symbol-specific aggregation info
        symbol_info = {}
        for symbol in self.symbols:
            book = self.get_aggregated_orderbook(symbol)
            if book:
                symbol_info[symbol] = {
                    'contributing_exchanges': len(book.contributing_exchanges),
                    'bid_levels': len(book.bids),
                    'ask_levels': len(book.asks),
                    'last_updated': book.last_updated.isoformat(),
                    'midpoint': float(book.get_midpoint()) if book.get_midpoint() else None
                }
        
        base_stats['symbol_aggregation'] = symbol_info
        base_stats['cross_currency_paths'] = len(self.cross_currency_paths)
        base_stats['running'] = self.running
        
        return base_stats
