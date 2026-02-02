"""
Multi-Reference Pricing Engine - Intelligent pricing engine supporting multiple reference exchanges and advanced WAPQ calculations.

This engine extends the current WAPQ approach from market_making.py to support:
- Multiple reference exchanges with dynamic weighting
- Cross-exchange liquidity aggregation
- Synthetic instrument pricing
- Risk-adjusted pricing based on correlations
- Real-time reference exchange selection
"""

import asyncio
import time
from typing import Dict, List, Set, Optional, Tuple, Callable, Any, Union
from dataclasses import dataclass, field
from decimal import Decimal
from datetime import datetime, timezone
from enum import Enum
import logging
import statistics
from collections import defaultdict, deque

# Avoid circular imports by defining data structures locally
logger = logging.getLogger(__name__)


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
    z_score: Optional[float] = None


class PricingMethod(Enum):
    """Pricing methods supported by the engine."""
    SINGLE_REFERENCE = "single_reference"  # Traditional single exchange reference
    MULTI_REFERENCE_WEIGHTED = "multi_reference_weighted"  # Weighted average of multiple references
    LIQUIDITY_WEIGHTED = "liquidity_weighted"  # Weight by available liquidity
    VOLATILITY_ADJUSTED = "volatility_adjusted"  # Adjust for volatility differences
    CORRELATION_ADJUSTED = "correlation_adjusted"  # Adjust based on cross-asset correlations
    SYNTHETIC = "synthetic"  # Synthetic instrument pricing



@dataclass
class PricingContext:
    """Context information for pricing decisions."""
    symbol: str
    side: str  # 'bid', 'ask', or 'mid'
    quantity: Decimal
    max_exchanges: int = 5
    min_confidence: float = 0.7
    pricing_method: PricingMethod = PricingMethod.MULTI_REFERENCE_WEIGHTED
    risk_adjustment: bool = True
    

@dataclass
class PricingResult:
    """Result from pricing calculation."""
    symbol: str
    side: str
    price: Optional[Decimal]
    confidence: float
    contributing_exchanges: List[str]
    exchange_weights: Dict[str, float]
    liquidity_available: Decimal
    pricing_method: PricingMethod
    calculation_time_ms: float
    warnings: List[str] = field(default_factory=list)


@dataclass
class MultiLegPricingRequest:
    """Request for multi-leg pricing (e.g., spread trades, synthetic instruments)."""
    legs: List[Tuple[str, Decimal, str]]  # (symbol, quantity, side) for each leg
    hedge_symbol: Optional[str] = None  # Symbol to hedge against
    max_correlation_risk: float = 0.3  # Maximum correlation exposure
    pricing_context: Optional[PricingContext] = None


class MultiReferencePricingEngine:
    """
    Advanced pricing engine supporting multiple reference exchanges and complex pricing strategies.
    
    Key features:
    - Multi-reference WAPQ calculation with intelligent weighting
    - Real-time reference exchange selection based on market conditions
    - Cross-exchange arbitrage detection and pricing
    - Synthetic instrument pricing from components
    - Risk-adjusted pricing based on correlations and volatility
    - Support for multi-leg trades (spreads, butterflies, etc.)
    """
    
    def __init__(
        self, 
        market_data_service: Any,  # Accept any market data service interface
        redis_url: str = "redis://localhost:6379"
    ):
        self.market_data_service = market_data_service
        self.redis_url = redis_url
        self.logger = logger
        
        # Direct orderbook access - no scoring needed
        # We'll use orderbooks directly from market_data_service
        
        # Pricing cache
        self.pricing_cache: Dict[str, Dict[str, PricingResult]] = {}  # symbol -> side -> result
        self.cache_ttl = 0.1  # 100ms cache TTL
        
        # WAPQ calculation cache for performance
        self.wapq_prices: Dict[Tuple[str, str, str], Decimal] = {}  # (symbol, side, quantity) -> price
        self.wapq_lock = asyncio.Lock()
        self.wapq_task: Optional[asyncio.Task] = None
        
        # Performance tracking
        self.pricing_stats = {
            'requests': 0,
            'cache_hits': 0,
            'calculation_time_ms': deque(maxlen=1000),
            'confidence_scores': deque(maxlen=1000)
        }
        
        # Background tasks
        self.running = False
        self.tasks: List[asyncio.Task] = []

    async def start(self):
        """Start the pricing engine."""
        if self.running:
            return
            
        self.running = True
        self.logger.info("Starting Multi-Reference Pricing Engine")
        
        # Register callbacks with market data service if supported
        if hasattr(self.market_data_service, 'price_update_callbacks'):
            self.market_data_service.price_update_callbacks.append(self._on_price_update)
        if hasattr(self.market_data_service, 'spread_update_callbacks'):
            self.market_data_service.spread_update_callbacks.append(self._on_spread_update)
        
        # Start background tasks
        self.wapq_task = asyncio.create_task(self._wapq_calculation_loop())
        
        self.logger.info("Multi-Reference Pricing Engine started")

    async def stop(self):
        """Stop the pricing engine."""
        if not self.running:
            return
            
        self.running = False
        
        # Cancel tasks
        if self.wapq_task:
            self.wapq_task.cancel()
            
        for task in self.tasks:
            task.cancel()
            
        if self.tasks:
            await asyncio.gather(*[self.wapq_task] + self.tasks, return_exceptions=True)
        
        self.logger.info("Multi-Reference Pricing Engine stopped")

    async def get_price(
        self, 
        symbol: str, 
        side: str, 
        quantity: Decimal,
        pricing_context: Optional[PricingContext] = None
    ) -> PricingResult:
        """
        Get price for a symbol using multi-reference pricing.
        
        Args:
            symbol: Trading symbol
            side: 'bid', 'ask', or 'mid'
            quantity: Quantity to price
            pricing_context: Optional pricing context
            
        Returns:
            PricingResult with calculated price and metadata
        """
        start_time = time.perf_counter()
        self.pricing_stats['requests'] += 1
        
        # Use provided context or create default
        if pricing_context is None:
            pricing_context = PricingContext(
                symbol=symbol,
                side=side,
                quantity=quantity
            )
        
        # Check cache first
        cache_key = f"{symbol}_{side}_{float(quantity)}"
        cached_result = self._get_cached_result(symbol, side, cache_key)
        if cached_result:
            self.pricing_stats['cache_hits'] += 1
            return cached_result
        
        # Calculate price based on method
        result = await self._calculate_price(pricing_context)
        
        # Update performance stats
        calculation_time = (time.perf_counter() - start_time) * 1000
        result.calculation_time_ms = calculation_time
        self.pricing_stats['calculation_time_ms'].append(calculation_time)
        self.pricing_stats['confidence_scores'].append(result.confidence)
        
        # Cache result
        self._cache_result(symbol, side, cache_key, result)
        
        return result

    async def get_multi_leg_price(
        self, 
        request: MultiLegPricingRequest
    ) -> Dict[str, PricingResult]:
        """
        Price a multi-leg trade (spread, synthetic, etc.).
        
        Args:
            request: Multi-leg pricing request
            
        Returns:
            Dictionary of symbol -> PricingResult for each leg
        """
        results = {}
        total_correlation_risk = 0.0
        
        # Price each leg
        for symbol, quantity, side in request.legs:
            context = request.pricing_context or PricingContext(
                symbol=symbol,
                side=side,
                quantity=quantity,
                pricing_method=PricingMethod.CORRELATION_ADJUSTED
            )
            
            result = await self.get_price(symbol, side, quantity, context)
            results[symbol] = result
            
            # Calculate correlation risk if hedge symbol specified
            if request.hedge_symbol and request.hedge_symbol != symbol:
                correlation = self.correlation_matrix.get((symbol, request.hedge_symbol), 0)
                leg_risk = abs(correlation) * float(quantity) / 1000  # Simplified risk calc
                total_correlation_risk += leg_risk
        
        # Add warnings if correlation risk is high
        if total_correlation_risk > request.max_correlation_risk:
            warning = f"High correlation risk: {total_correlation_risk:.3f} > {request.max_correlation_risk}"
            for result in results.values():
                result.warnings.append(warning)
        
        return results

    async def calculate_conversion_price(
        self, 
        base_symbol: str, 
        target_symbol: str,
        quantity: Decimal = Decimal('100')
    ) -> Optional[Decimal]:
        """
        Calculate conversion price from base_symbol to target_symbol.
        
        Args:
            base_symbol: Source symbol (e.g., 'BERA/USDC')
            target_symbol: Target symbol (e.g., 'BERA/USDT')
            quantity: Quantity to convert
            
        Returns:
            Conversion rate if path exists, None otherwise
        """
        try:
            # Parse symbols to find conversion path
            base_parts = base_symbol.split('/')
            target_parts = target_symbol.split('/')
            
            if len(base_parts) != 2 or len(target_parts) != 2:
                return None
            
            base_base, base_quote = base_parts
            target_base, target_quote = target_parts
            
            # If same base currency, convert quote currency
            if base_base == target_base:
                # Need quote conversion: base_quote -> target_quote
                conversion_symbol = f"{base_quote}/{target_quote}"
                
                # Get conversion rate
                conversion_result = await self.get_price(conversion_symbol, 'mid', quantity)
                return conversion_result.price
            
            # If same quote currency, convert base currency
            elif base_quote == target_quote:
                # Need base conversion: base_base -> target_base
                conversion_symbol = f"{base_base}/{target_base}"
                
                conversion_result = await self.get_price(conversion_symbol, 'mid', quantity)
                return conversion_result.price
            
            # Cross conversion needed (more complex)
            else:
                # Try common quote currency (USDT) as intermediate
                usdt_base = f"{base_quote}/USDT"
                usdt_target = f"{target_quote}/USDT"
                
                base_to_usdt = await self.get_price(usdt_base, 'mid', quantity)
                target_from_usdt = await self.get_price(usdt_target, 'mid', quantity)
                
                if base_to_usdt.price and target_from_usdt.price:
                    # Convert: base_quote -> USDT -> target_quote
                    return base_to_usdt.price / target_from_usdt.price
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error calculating conversion from {base_symbol} to {target_symbol}: {e}")
            return None

    async def get_usdt_equivalent_price(
        self, 
        symbol: str, 
        quantity: Decimal = Decimal('100')
    ) -> Optional[Decimal]:
        """
        Get USDT equivalent price for any BERA symbol using direct orderbook conversion.
        
        Args:
            symbol: Symbol to price (e.g., 'BERA/BTC', 'BERA/USDC', 'BERA/TRY')
            quantity: Quantity to price
            
        Returns:
            USDT equivalent price using direct orderbook math
        """
        try:
            # If already USDT pair, get direct WAPQ from all available exchanges
            if symbol.endswith('/USDT'):
                return await self._get_multi_exchange_wapq(symbol, 'mid', quantity)
            
            # For non-USDT pairs, do direct conversion using orderbooks
            parts = symbol.split('/')
            if len(parts) != 2:
                return None
            
            base_currency, quote_currency = parts  # e.g., 'BERA', 'BTC'
            
            # Step 1: Get BERA price in quote currency using available orderbooks
            base_price = await self._get_multi_exchange_wapq(symbol, 'mid', quantity)
            if not base_price:
                return None
            
            # Step 2: Convert quote currency to USDT using available orderbooks
            conversion_symbol = f"{quote_currency}/USDT"
            conversion_rate = await self._get_multi_exchange_wapq(conversion_symbol, 'mid', quantity)
            
            if conversion_rate:
                return base_price * conversion_rate
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error getting USDT equivalent for {symbol}: {e}")
            return None

    async def _get_multi_exchange_wapq(
        self, 
        symbol: str, 
        side: str, 
        quantity: Decimal
    ) -> Optional[Decimal]:
        """
        Get WAPQ using all available exchanges for a symbol (no scoring, just use all data).
        
        Args:
            symbol: Trading symbol
            side: 'bid', 'ask', or 'mid'
            quantity: Quantity to calculate WAPQ for
            
        Returns:
            Volume-weighted average price across all exchanges with this symbol
        """
        if not hasattr(self.market_data_service, 'orderbooks'):
            return None
        
        # Find all exchanges that have this symbol (tolerant to formatting differences)
        available_orderbooks = {}
        for (exchange, sym), orderbook in self.market_data_service.orderbooks.items():
            if self._symbols_match(sym, symbol) and orderbook.get('bids') and orderbook.get('asks'):
                available_orderbooks[exchange] = orderbook
        
        if not available_orderbooks:
            return None
        
        # Calculate WAPQ for each exchange
        exchange_wapqs = {}
        exchange_volumes = {}
        
        for exchange, orderbook in available_orderbooks.items():
            wapq = await self._calculate_exchange_wapq(exchange, symbol, side, quantity)
            if wapq:
                exchange_wapqs[exchange] = wapq
                # Use total orderbook volume as weight (sum of top 10 levels)
                if side in ('bid', 'buy'):
                    volume = sum(Decimal(str(level[1])) for level in orderbook.get('asks', [])[:10])
                elif side in ('ask', 'sell'):
                    volume = sum(Decimal(str(level[1])) for level in orderbook.get('bids', [])[:10])
                else:  # mid
                    bid_volume = sum(Decimal(str(level[1])) for level in orderbook.get('bids', [])[:10])
                    ask_volume = sum(Decimal(str(level[1])) for level in orderbook.get('asks', [])[:10])
                    volume = (bid_volume + ask_volume) / Decimal('2')
                exchange_volumes[exchange] = volume
        
        if not exchange_wapqs:
            return None
        
        # Volume-weighted average across exchanges
        total_weighted_price = Decimal('0')
        total_volume = Decimal('0')
        
        for exchange, wapq in exchange_wapqs.items():
            volume = exchange_volumes[exchange]
            total_weighted_price += wapq * volume
            total_volume += volume
        
        if total_volume > 0:
            return total_weighted_price / total_volume
        
        # Fallback to simple average if no volume data
        return sum(exchange_wapqs.values()) / len(exchange_wapqs)

    async def _calculate_price(self, context: PricingContext) -> PricingResult:
        """Calculate price based on pricing context."""
        symbol = context.symbol
        side = context.side
        quantity = context.quantity
        method = context.pricing_method
        
        if method == PricingMethod.SINGLE_REFERENCE:
            return await self._calculate_single_reference_price(context)
        elif method == PricingMethod.MULTI_REFERENCE_WEIGHTED:
            return await self._calculate_multi_reference_weighted_price(context)
        elif method == PricingMethod.LIQUIDITY_WEIGHTED:
            return await self._calculate_liquidity_weighted_price(context)
        elif method == PricingMethod.VOLATILITY_ADJUSTED:
            return await self._calculate_volatility_adjusted_price(context)
        elif method == PricingMethod.CORRELATION_ADJUSTED:
            return await self._calculate_correlation_adjusted_price(context)
        elif method == PricingMethod.SYNTHETIC:
            return await self._calculate_synthetic_price(context)
        else:
            # Default to multi-reference weighted
            return await self._calculate_multi_reference_weighted_price(context)

    async def _calculate_multi_reference_weighted_price(self, context: PricingContext) -> PricingResult:
        """
        Calculate price using all available exchanges with volume-based weighting.
        Returns native quote currency prices, not USDT-equivalent prices.
        """
        symbol = context.symbol
        side = context.side
        quantity = context.quantity
        
        # Get WAPQ using all available exchanges for the NATIVE symbol (not USDT conversion)
        final_price = await self._get_multi_exchange_wapq(symbol, side, quantity)
        
        if not final_price:
            return PricingResult(
                symbol=symbol,
                side=side,
                price=None,
                confidence=0.0,
                contributing_exchanges=[],
                exchange_weights={},
                liquidity_available=Decimal('0'),
                pricing_method=context.pricing_method,
                calculation_time_ms=0.0,
                warnings=["No orderbook data available for symbol"]
            )
        
        # Get contributing exchanges and their data
        contributing_exchanges = []
        exchange_weights = {}
        total_liquidity = Decimal('0')
        
        if hasattr(self.market_data_service, 'orderbooks'):
            total_volume = Decimal('0')
            exchange_volumes = {}
            
            for (exchange, sym), orderbook in self.market_data_service.orderbooks.items():
                if self._symbols_match(sym, symbol) and orderbook.get('bids') and orderbook.get('asks'):
                    contributing_exchanges.append(exchange)
                    
                    # Calculate volume for weighting
                    if side in ('bid', 'buy'):
                        volume = sum(Decimal(str(level[1])) for level in orderbook.get('asks', [])[:10])
                    elif side in ('ask', 'sell'):
                        volume = sum(Decimal(str(level[1])) for level in orderbook.get('bids', [])[:10])
                    else:  # mid
                        bid_volume = sum(Decimal(str(level[1])) for level in orderbook.get('bids', [])[:10])
                        ask_volume = sum(Decimal(str(level[1])) for level in orderbook.get('asks', [])[:10])
                        volume = (bid_volume + ask_volume) / Decimal('2')
                    
                    exchange_volumes[exchange] = volume
                    total_volume += volume
                    total_liquidity += volume
            
            # Calculate normalized weights
            for exchange in contributing_exchanges:
                if total_volume > 0:
                    exchange_weights[exchange] = float(exchange_volumes[exchange] / total_volume)
                else:
                    exchange_weights[exchange] = 1.0 / len(contributing_exchanges)
        
        # Calculate confidence based on number of exchanges and liquidity
        confidence = min(0.9, 0.5 + (len(contributing_exchanges) * 0.1) + min(float(total_liquidity) / 10000, 0.3))
        
        return PricingResult(
            symbol=symbol,
            side=side,
            price=final_price,
            confidence=confidence,
            contributing_exchanges=contributing_exchanges,
            exchange_weights=exchange_weights,
            liquidity_available=total_liquidity,
            pricing_method=context.pricing_method,
            calculation_time_ms=0.0
        )

    async def _calculate_exchange_wapq(
        self, 
        exchange: str, 
        symbol: str, 
        side: str, 
        quantity: Decimal
    ) -> Optional[Decimal]:
        """
        Calculate WAPQ for a specific exchange and symbol.
        This is similar to the WAPQ calculation in market_making.py but for any exchange.
        """
        # Get orderbook from market data service, tolerant to symbol formatting
        orderbook = self._get_exchange_orderbook(exchange, symbol)
        if not orderbook:
            return None
        
        # Parse levels based on side
        if side in ('bid', 'buy'):
            # For bid pricing, we consume asks (we're buying)
            levels = orderbook.get('asks', [])
        elif side in ('ask', 'sell'):
            # For ask pricing, we consume bids (we're selling)
            levels = orderbook.get('bids', [])
        else:  # mid
            # For mid pricing, take average of bid and ask
            bids = orderbook.get('bids', [])
            asks = orderbook.get('asks', [])
            if bids and asks:
                best_bid = Decimal(str(bids[0][0]))
                best_ask = Decimal(str(asks[0][0]))
                return (best_bid + best_ask) / Decimal('2')
            return None
        
        if not levels:
            return None
        
        # Calculate WAPQ by consuming liquidity up to quantity
        total_cost = Decimal('0')
        total_quantity = Decimal('0')
        
        for price_str, amount_str in levels:
            price = Decimal(str(price_str))
            available_amount = Decimal(str(amount_str))
            
            if total_quantity >= quantity:
                break
            
            # Take what we need or what's available
            take_amount = min(available_amount, quantity - total_quantity)
            
            total_cost += take_amount * price
            total_quantity += take_amount
        
        if total_quantity == 0:
            return None
        
        return total_cost / total_quantity

    def _get_exchange_liquidity(self, exchange: str, symbol: str, side: str) -> Decimal:
        """Get available liquidity for an exchange-symbol-side combination."""
        orderbook = self._get_exchange_orderbook(exchange, symbol)
        if not orderbook:
            return Decimal('0')
        
        # Sum liquidity in top 10 levels
        if side in ('bid', 'buy'):
            levels = orderbook.get('asks', [])[:10]
        elif side in ('ask', 'sell'):
            levels = orderbook.get('bids', [])[:10]
        else:  # mid - average of both sides
            bid_levels = orderbook.get('bids', [])[:5]
            ask_levels = orderbook.get('asks', [])[:5]
            bid_liq = sum(Decimal(str(level[1])) for level in bid_levels)
            ask_liq = sum(Decimal(str(level[1])) for level in ask_levels)
            return (bid_liq + ask_liq) / Decimal('2')
        
        return sum(Decimal(str(level[1])) for level in levels)

    def _symbols_match(self, symbol_a: str, symbol_b: str) -> bool:
        """Check if two symbols represent the same pair regardless of separators/case."""
        def normalize(s: str) -> str:
            return s.replace('/', '').replace(':', '').replace('_', '').upper()
        return normalize(symbol_a) == normalize(symbol_b)

    def _get_exchange_orderbook(self, exchange: str, symbol: str) -> Optional[Dict[str, Any]]:
        """Fetch orderbook for an exchange-symbol, matching across formatting differences."""
        # Direct lookup first
        ob = self.market_data_service.orderbooks.get((exchange, symbol))
        if ob:
            return ob
        # Fallback: scan for matching variant
        for (ex, sym), orderbook in self.market_data_service.orderbooks.items():
            if ex == exchange and self._symbols_match(sym, symbol):
                return orderbook
        return None


    # Simplified implementations for other pricing methods
    async def _calculate_single_reference_price(self, context: PricingContext) -> PricingResult:
        """Calculate price using single exchange with highest volume."""
        symbol = context.symbol
        
        # Find exchange with highest volume for this symbol
        best_exchange = None
        best_volume = Decimal('0')
        
        if hasattr(self.market_data_service, 'orderbooks'):
            for (exchange, sym), orderbook in self.market_data_service.orderbooks.items():
                if self._symbols_match(sym, symbol) and orderbook.get('bids') and orderbook.get('asks'):
                    bid_volume = sum(Decimal(str(level[1])) for level in orderbook.get('bids', [])[:10])
                    ask_volume = sum(Decimal(str(level[1])) for level in orderbook.get('asks', [])[:10])
                    total_volume = bid_volume + ask_volume
                    
                    if total_volume > best_volume:
                        best_volume = total_volume
                        best_exchange = exchange
        
        if not best_exchange:
            return PricingResult(
                symbol=symbol,
                side=context.side,
                price=None,
                confidence=0.0,
                contributing_exchanges=[],
                exchange_weights={},
                liquidity_available=Decimal('0'),
                pricing_method=context.pricing_method,
                calculation_time_ms=0.0,
                warnings=["No orderbook data available"]
            )
        
        price = await self._calculate_exchange_wapq(
            best_exchange, symbol, context.side, context.quantity
        )
        
        return PricingResult(
            symbol=symbol,
            side=context.side,
            price=price,
            confidence=0.8 if price else 0.0,
            contributing_exchanges=[best_exchange] if price else [],
            exchange_weights={best_exchange: 1.0} if price else {},
            liquidity_available=best_volume,
            pricing_method=context.pricing_method,
            calculation_time_ms=0.0
        )

    async def _calculate_liquidity_weighted_price(self, context: PricingContext) -> PricingResult:
        """Calculate price weighted by available liquidity."""
        # Implementation would weight exchanges by their available liquidity
        # This is similar to multi_reference_weighted but uses pure liquidity weighting
        return await self._calculate_multi_reference_weighted_price(context)

    async def _calculate_volatility_adjusted_price(self, context: PricingContext) -> PricingResult:
        """Calculate price adjusted for volatility differences."""
        # Implementation would adjust pricing based on recent volatility
        return await self._calculate_multi_reference_weighted_price(context)

    async def _calculate_correlation_adjusted_price(self, context: PricingContext) -> PricingResult:
        """Calculate price adjusted for correlations with other assets."""
        # Implementation would consider correlations with other symbols in portfolio
        return await self._calculate_multi_reference_weighted_price(context)

    async def _calculate_synthetic_price(self, context: PricingContext) -> PricingResult:
        """Calculate synthetic instrument price from components."""
        # Implementation would construct price from multiple underlying components
        return await self._calculate_multi_reference_weighted_price(context)

    # Background task methods
    async def _wapq_calculation_loop(self):
        """
        Background WAPQ calculation loop for caching common calculations.
        Pre-calculates WAPQ prices for common quantities to improve response times.
        """
        while self.running:
            try:
                # Get all available symbols from market data service
                if hasattr(self.market_data_service, 'orderbooks'):
                    symbols = set()
                    for (exchange, symbol) in self.market_data_service.orderbooks.keys():
                        symbols.add(symbol)
                    
                    # Pre-calculate WAPQ for common quantities
                    common_quantities = [Decimal('100'), Decimal('1000'), Decimal('10000')]
                    sides = ['bid', 'ask', 'mid']
                    
                    for symbol in symbols:
                        for quantity in common_quantities:
                            for side in sides:
                                try:
                                    # Calculate and cache multi-exchange WAPQ
                                    wapq = await self._get_multi_exchange_wapq(symbol, side, quantity)
                                    if wapq:
                                        cache_key = (symbol, side, str(quantity))
                                        self.wapq_prices[cache_key] = wapq
                                        
                                except Exception as e:
                                    self.logger.debug(f"Error caching WAPQ for {symbol} {side} {quantity}: {e}")
                
                await asyncio.sleep(0.1)  # 100ms cycle - less frequent since no complex scoring
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in WAPQ calculation loop: {e}")
                await asyncio.sleep(0.1)

    # Cache management
    def _get_cached_result(self, symbol: str, side: str, cache_key: str) -> Optional[PricingResult]:
        """Get cached pricing result if still valid."""
        if symbol not in self.pricing_cache:
            return None
        
        if side not in self.pricing_cache[symbol]:
            return None
        
        cached_result = self.pricing_cache[symbol].get(cache_key)
        if not cached_result:
            return None
        
        # Check if cache is still valid
        # cached_result.calculation_time_ms is the timestamp when result was calculated
        current_time = time.time()
        cache_timestamp = getattr(cached_result, 'cache_timestamp', current_time - self.cache_ttl - 1)
        if current_time - cache_timestamp > self.cache_ttl:
            return None
        
        return cached_result

    def _cache_result(self, symbol: str, side: str, cache_key: str, result: PricingResult):
        """Cache pricing result."""
        if symbol not in self.pricing_cache:
            self.pricing_cache[symbol] = {}
        
        if side not in self.pricing_cache[symbol]:
            self.pricing_cache[symbol][side] = {}
        
        # Add cache timestamp for proper cache validation
        result.cache_timestamp = time.time()
        self.pricing_cache[symbol][side][cache_key] = result

    # Callback handlers
    async def _on_price_update(self, exchange: str, symbol: str, orderbook: Dict[str, Any]):
        """Handle price updates from market data service."""
        # Clear relevant cache entries
        if symbol in self.pricing_cache:
            self.pricing_cache[symbol].clear()

    async def _on_spread_update(self, symbol: str, spreads: List[CrossExchangeSpread]):
        """Handle spread updates from market data service."""
        # Could trigger re-weighting of reference exchanges based on spreads
        pass

    # Public API for getting pricing statistics
    def get_pricing_stats(self) -> Dict[str, Any]:
        """Get pricing engine performance statistics."""
        stats = dict(self.pricing_stats)
        
        if self.pricing_stats['calculation_time_ms']:
            stats['avg_calculation_time_ms'] = statistics.mean(self.pricing_stats['calculation_time_ms'])
            # Use manual percentile calculation instead of numpy
            calc_times = sorted(self.pricing_stats['calculation_time_ms'])
            p95_index = int(0.95 * len(calc_times))
            stats['p95_calculation_time_ms'] = calc_times[p95_index] if calc_times else 0
        
        if self.pricing_stats['confidence_scores']:
            stats['avg_confidence'] = statistics.mean(self.pricing_stats['confidence_scores'])
        
        stats['cache_hit_rate'] = (
            self.pricing_stats['cache_hits'] / max(self.pricing_stats['requests'], 1)
        )
        
        return stats
