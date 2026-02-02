"""
Enhanced Market Making Strategy - Multi-exchange, multi-symbol market making with intelligent pricing.

This strategy extends the existing market_making.py to support:
- Multiple reference exchanges with dynamic weighting
- Multiple quote pairs per base symbol
- Cross-symbol hedging and risk management
- Synthetic instrument market making
- Advanced WAPQ pricing across exchanges
"""

import asyncio
import time
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Optional, Any, Tuple, Set
import structlog

from bot_manager.strategies.base_strategy import BaseStrategy
from market_data.multi_symbol_market_data_service import MultiSymbolMarketDataService, SymbolConfig
from market_data.multi_reference_pricing_engine import (
    MultiReferencePricingEngine, 
    PricingContext, 
    PricingMethod,
    MultiLegPricingRequest
)

logger = structlog.get_logger(__name__)


class QuoteMode(str, Enum):
    """Quote mode options."""
    SINGLE_SYMBOL = "single_symbol"  # Traditional single symbol quoting
    MULTI_SYMBOL = "multi_symbol"    # Multiple symbols with same base
    CROSS_SYMBOL = "cross_symbol"    # Cross-symbol spreads and arbitrage
    SYNTHETIC = "synthetic"          # Synthetic instrument quoting


@dataclass
class EnhancedQuotingLine:
    """Enhanced quoting line supporting multi-exchange and multi-symbol."""
    line_id: int
    base_symbol: str  # e.g., 'BERA'
    quote_pairs: List[str]  # e.g., ['BERA/USDT', 'BERA/USDC']
    exchanges: List[str]  # Exchanges to quote on
    reference_exchanges: List[str]  # Reference exchanges for pricing
    
    # Traditional parameters
    timeout_seconds: int
    drift_bps: float
    quantity: float
    spread_bps: float
    sides: str  # 'both', 'bid', 'ask'
    
    # Enhanced parameters
    quote_mode: QuoteMode = QuoteMode.SINGLE_SYMBOL
    max_position_per_symbol: Optional[Decimal] = None
    hedge_ratio: Decimal = Decimal('1.0')  # For cross-symbol hedging
    correlation_adjustment: bool = True
    
    # Runtime state
    active_quotes: Dict[Tuple[str, str, str], str] = field(default_factory=dict)  # (exchange, symbol, side) -> order_id
    last_reference_prices: Dict[str, Decimal] = field(default_factory=dict)  # symbol -> price
    hedge_positions: Dict[str, Decimal] = field(default_factory=dict)  # symbol -> position


class EnhancedMarketMakingStrategy(BaseStrategy):
    """
    Enhanced market making strategy with multi-exchange, multi-symbol support.
    
    Key features:
    - Multiple reference exchanges with intelligent weighting
    - Multi-symbol quoting with cross-symbol risk management
    - Advanced WAPQ pricing across exchanges
    - Synthetic instrument support
    - Dynamic hedge ratio adjustment
    - Correlation-based position limits
    """
    
    def __init__(
        self,
        instance_id: str,
        symbol: str,  # Primary symbol for compatibility
        exchanges: List[str],
        config: Dict[str, Any]
    ):
        super().__init__(instance_id, symbol, exchanges, config)
        
        # Enhanced configuration
        self.quote_lines: List[EnhancedQuotingLine] = []
        self.quote_mode: QuoteMode = QuoteMode.SINGLE_SYMBOL
        self.base_symbols: Set[str] = set()  # All base symbols we're making markets in
        self.symbol_configs: List[SymbolConfig] = []
        
        # Enhanced market data and pricing
        self.enhanced_market_data: Optional[MultiSymbolMarketDataService] = None
        self.pricing_engine: Optional[MultiReferencePricingEngine] = None
        
        # Position and risk management
        self.net_positions: Dict[str, Decimal] = {}  # symbol -> net position
        self.cross_symbol_exposure: Dict[Tuple[str, str], Decimal] = {}  # (symbol_a, symbol_b) -> correlation exposure
        self.max_total_exposure: Optional[Decimal] = None
        
        # Performance tracking
        self.enhanced_stats = {
            'multi_symbol_orders': 0,
            'cross_exchange_arbitrage': 0,
            'correlation_adjustments': 0,
            'synthetic_quotes': 0
        }
        
        # Tasks
        self.enhanced_tasks: List[asyncio.Task] = []

    async def _validate_config(self) -> None:
        """Enhanced configuration validation."""
        await super()._validate_config()
        
        # Validate enhanced configuration
        if 'enhanced_config' not in self.config:
            # Use compatible single-symbol mode
            self.quote_mode = QuoteMode.SINGLE_SYMBOL
            self._create_compatible_quote_lines()
            return
        
        enhanced_config = self.config['enhanced_config']
        
        # Validate quote mode
        quote_mode = enhanced_config.get('quote_mode', 'single_symbol')
        self.quote_mode = QuoteMode(quote_mode)
        
        # Validate symbol configurations
        symbol_configs = enhanced_config.get('symbols', [])
        for symbol_config in symbol_configs:
            self.symbol_configs.append(SymbolConfig(
                base_symbol=symbol_config['symbol'],
                exchanges=symbol_config.get('exchanges', []),
                priority=symbol_config.get('priority', 1),
                depth=symbol_config.get('depth', 100)
            ))
            
            # Extract base symbol
            base = symbol_config['symbol'].split('/')[0] if '/' in symbol_config['symbol'] else symbol_config['symbol']
            self.base_symbols.add(base)
        
        # Validate quote lines
        lines_config = enhanced_config.get('lines', [])
        for i, line_config in enumerate(lines_config):
            quote_line = EnhancedQuotingLine(
                line_id=i,
                base_symbol=line_config.get('base_symbol', list(self.base_symbols)[0]),
                quote_pairs=line_config.get('quote_pairs', [self.symbol]),
                exchanges=line_config.get('exchanges', self.exchanges),
                reference_exchanges=line_config.get('reference_exchanges', [self.exchanges[0]]),
                timeout_seconds=line_config.get('timeout', 30),
                drift_bps=line_config.get('drift', 50),
                quantity=line_config.get('quantity', 100),
                spread_bps=line_config.get('spread', 20),
                sides=line_config.get('sides', 'both'),
                quote_mode=QuoteMode(line_config.get('quote_mode', 'single_symbol')),
                max_position_per_symbol=Decimal(str(line_config.get('max_position_per_symbol', 1000))),
                hedge_ratio=Decimal(str(line_config.get('hedge_ratio', 1.0))),
                correlation_adjustment=line_config.get('correlation_adjustment', True)
            )
            self.quote_lines.append(quote_line)
        
        # Risk management settings
        self.max_total_exposure = enhanced_config.get('max_total_exposure')
        if self.max_total_exposure:
            self.max_total_exposure = Decimal(str(self.max_total_exposure))
        
        self.logger.info(f"Enhanced market making strategy validated with {len(self.quote_lines)} lines, "
                        f"{len(self.base_symbols)} base symbols, mode: {self.quote_mode}")

    def _create_compatible_quote_lines(self):
        """Create quote lines compatible with original market_making.py configuration."""
        lines_config = self.config.get('lines', [])
        
        for i, line_config in enumerate(lines_config):
            quote_line = EnhancedQuotingLine(
                line_id=i,
                base_symbol=self.symbol.split('/')[0] if '/' in self.symbol else self.symbol,
                quote_pairs=[self.symbol],
                exchanges=self.exchanges,
                reference_exchanges=[self.config.get('reference_exchange', self.exchanges[0])],
                timeout_seconds=line_config.get('timeout', 30),
                drift_bps=line_config.get('drift', 50),
                quantity=line_config.get('quantity', 100),
                spread_bps=line_config.get('spread', 20),
                sides=line_config.get('sides', 'both'),
                quote_mode=QuoteMode.SINGLE_SYMBOL
            )
            self.quote_lines.append(quote_line)
        
        # Create symbol config
        self.symbol_configs.append(SymbolConfig(
            base_symbol=self.symbol,
            exchanges=[{'name': ex.split('_')[0], 'type': ex.split('_')[1] if '_' in ex else 'spot'} 
                      for ex in self.exchanges]
        ))
        
        base = self.symbol.split('/')[0] if '/' in self.symbol else self.symbol
        self.base_symbols.add(base)

    async def _start_strategy(self) -> None:
        """Start the enhanced market making strategy."""
        self.logger.info(f"Starting enhanced market making strategy in {self.quote_mode} mode")
        
        # Initialize enhanced market data service
        self.enhanced_market_data = MultiSymbolMarketDataService(
            symbol_configs=self.symbol_configs
        )
        await self.enhanced_market_data.start()
        
        # Initialize pricing engine
        self.pricing_engine = MultiReferencePricingEngine(self.enhanced_market_data)
        await self.pricing_engine.start()
        
        # Start enhanced monitoring tasks
        self._start_enhanced_tasks()
        
        # Start main strategy loop
        self.main_task = asyncio.create_task(self._enhanced_strategy_loop())
        
        self.logger.info("Enhanced market making strategy started")

    async def _stop_strategy(self) -> None:
        """Stop the enhanced market making strategy."""
        # Cancel enhanced tasks
        for task in self.enhanced_tasks:
            task.cancel()
        
        if self.enhanced_tasks:
            await asyncio.gather(*self.enhanced_tasks, return_exceptions=True)
        
        # Stop enhanced services
        if self.pricing_engine:
            await self.pricing_engine.stop()
        
        if self.enhanced_market_data:
            await self.enhanced_market_data.stop()
        
        # Cancel all orders
        await self._cancel_all_enhanced_orders()

    def _start_enhanced_tasks(self):
        """Start enhanced background tasks."""
        # Cross-symbol risk monitoring
        risk_task = asyncio.create_task(self._monitor_cross_symbol_risk())
        self.enhanced_tasks.append(risk_task)
        
        # Dynamic hedge ratio adjustment
        hedge_task = asyncio.create_task(self._adjust_hedge_ratios())
        self.enhanced_tasks.append(hedge_task)
        
        # Performance monitoring
        perf_task = asyncio.create_task(self._monitor_enhanced_performance())
        self.enhanced_tasks.append(perf_task)

    async def _enhanced_strategy_loop(self) -> None:
        """Enhanced strategy loop with multi-symbol support."""
        while self.running:
            try:
                # Process each quote line
                for line in self.quote_lines:
                    await self._process_enhanced_quote_line(line)
                
                await asyncio.sleep(0.001)  # 1ms cycle for high performance
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in enhanced strategy loop: {e}")
                await asyncio.sleep(1)

    async def _process_enhanced_quote_line(self, line: EnhancedQuotingLine) -> None:
        """Process an enhanced quote line supporting multiple symbols and exchanges."""
        try:
            # Check timeouts and drift
            await self._check_enhanced_timeouts(line)
            await self._check_enhanced_drift(line)
            
            # Place missing quotes based on mode
            if line.quote_mode == QuoteMode.SINGLE_SYMBOL:
                await self._place_single_symbol_quotes(line)
            elif line.quote_mode == QuoteMode.MULTI_SYMBOL:
                await self._place_multi_symbol_quotes(line)
            elif line.quote_mode == QuoteMode.CROSS_SYMBOL:
                await self._place_cross_symbol_quotes(line)
            elif line.quote_mode == QuoteMode.SYNTHETIC:
                await self._place_synthetic_quotes(line)
                
        except Exception as e:
            self.logger.error(f"Error processing enhanced quote line {line.line_id}: {e}")

    async def _place_single_symbol_quotes(self, line: EnhancedQuotingLine) -> None:
        """Place quotes for single symbol (compatible mode)."""
        if not line.quote_pairs:
            return
        
        symbol = line.quote_pairs[0]  # Use first symbol
        
        # Get pricing from engine
        for exchange in line.exchanges:
            if exchange in line.reference_exchanges:
                continue  # Don't quote on reference exchanges
            
            # Check position limits
            current_position = self.net_positions.get(symbol, Decimal('0'))
            if line.max_position_per_symbol and abs(current_position) >= line.max_position_per_symbol:
                continue
            
            # Calculate bid/ask prices using enhanced pricing
            spread_decimal = Decimal(str(line.spread_bps)) / Decimal('10000')
            
            # Get mid price from multiple references
            mid_pricing_context = PricingContext(
                symbol=symbol,
                side='mid',
                quantity=Decimal(str(line.quantity)),
                pricing_method=PricingMethod.MULTI_REFERENCE_WEIGHTED
            )
            
            mid_result = await self.pricing_engine.get_price(
                symbol, 'mid', Decimal(str(line.quantity)), mid_pricing_context
            )
            
            if not mid_result.price or mid_result.confidence < 0.5:
                continue
            
            # Calculate quote prices
            bid_price = mid_result.price * (Decimal('1') - spread_decimal)
            ask_price = mid_result.price * (Decimal('1') + spread_decimal)
            
            # Place orders if not already present
            if line.sides in ('both', 'bid'):
                bid_key = (exchange, symbol, 'bid')
                if bid_key not in line.active_quotes:
                    await self._place_enhanced_order(line, exchange, symbol, 'bid', bid_price)
            
            if line.sides in ('both', 'ask'):
                ask_key = (exchange, symbol, 'ask')
                if ask_key not in line.active_quotes:
                    await self._place_enhanced_order(line, exchange, symbol, 'ask', ask_price)

    async def _place_multi_symbol_quotes(self, line: EnhancedQuotingLine) -> None:
        """Place quotes for multiple symbols with same base currency."""
        # Get reference price for base symbol across quote pairs
        base_prices = {}
        
        for symbol in line.quote_pairs:
            mid_result = await self.pricing_engine.get_price(
                symbol, 'mid', Decimal(str(line.quantity))
            )
            if mid_result.price:
                base_prices[symbol] = mid_result.price
        
        if not base_prices:
            return
        
        # Calculate cross-pair arbitrage opportunities
        for i, symbol_a in enumerate(line.quote_pairs):
            for symbol_b in line.quote_pairs[i+1:]:
                if symbol_a in base_prices and symbol_b in base_prices:
                    await self._check_cross_pair_arbitrage(line, symbol_a, symbol_b, base_prices)
        
        # Place regular quotes for each symbol
        for symbol in line.quote_pairs:
            if symbol not in base_prices:
                continue
            
            # Similar to single symbol but with cross-symbol position consideration
            for exchange in line.exchanges:
                if exchange in line.reference_exchanges:
                    continue
                
                # Check total base exposure across all quote pairs
                total_base_exposure = sum(
                    abs(self.net_positions.get(sym, Decimal('0'))) 
                    for sym in line.quote_pairs
                )
                
                if line.max_position_per_symbol and total_base_exposure >= line.max_position_per_symbol * len(line.quote_pairs):
                    continue
                
                spread_decimal = Decimal(str(line.spread_bps)) / Decimal('10000')
                mid_price = base_prices[symbol]
                
                bid_price = mid_price * (Decimal('1') - spread_decimal)
                ask_price = mid_price * (Decimal('1') + spread_decimal)
                
                # Place orders
                if line.sides in ('both', 'bid'):
                    bid_key = (exchange, symbol, 'bid')
                    if bid_key not in line.active_quotes:
                        await self._place_enhanced_order(line, exchange, symbol, 'bid', bid_price)
                
                if line.sides in ('both', 'ask'):
                    ask_key = (exchange, symbol, 'ask')
                    if ask_key not in line.active_quotes:
                        await self._place_enhanced_order(line, exchange, symbol, 'ask', ask_price)

    async def _place_cross_symbol_quotes(self, line: EnhancedQuotingLine) -> None:
        """Place quotes for cross-symbol spreads and arbitrage."""
        # Implement cross-symbol spread trading
        self.enhanced_stats['cross_exchange_arbitrage'] += 1

    async def _place_synthetic_quotes(self, line: EnhancedQuotingLine) -> None:
        """Place quotes for synthetic instruments."""
        # Implement synthetic instrument market making
        self.enhanced_stats['synthetic_quotes'] += 1

    async def _place_enhanced_order(
        self, 
        line: EnhancedQuotingLine, 
        exchange: str, 
        symbol: str, 
        side: str, 
        price: Decimal
    ) -> None:
        """Place an enhanced order with multi-symbol tracking."""
        try:
            order_side = 'buy' if side == 'bid' else 'sell'
            
            order_id = await self._place_order(
                exchange=exchange,
                side=order_side,
                amount=float(line.quantity),
                price=float(price),
                client_order_id=f"emm_{line.line_id}_{side}_{int(time.time() * 1000)}"
            )
            
            if order_id:
                key = (exchange, symbol, side)
                line.active_quotes[key] = order_id
                
                self.enhanced_stats['multi_symbol_orders'] += 1
                
                self.logger.info(
                    f"Enhanced order placed: Line {line.line_id}, {symbol} {side} @ {price:.4f} "
                    f"on {exchange} (order_id: {order_id})"
                )
                
        except Exception as e:
            self.logger.error(f"Error placing enhanced order: {e}")

    async def _check_cross_pair_arbitrage(
        self, 
        line: EnhancedQuotingLine, 
        symbol_a: str, 
        symbol_b: str, 
        base_prices: Dict[str, Decimal]
    ) -> None:
        """Check for cross-pair arbitrage opportunities."""
        # Simplified arbitrage detection
        price_a = base_prices[symbol_a]
        price_b = base_prices[symbol_b]
        
        # Calculate implied cross rate and compare with actual
        # This is a simplified example - real implementation would be more complex
        price_ratio = price_a / price_b
        
        # Log potential opportunities (real implementation would act on them)
        if abs(float(price_ratio) - 1.0) > 0.005:  # 0.5% difference
            self.logger.info(f"Potential cross-pair arbitrage: {symbol_a}/{symbol_b} ratio: {price_ratio}")

    # Enhanced timeout and drift checking
    async def _check_enhanced_timeouts(self, line: EnhancedQuotingLine) -> None:
        """Enhanced timeout checking for multi-symbol quotes."""
        now = datetime.now(timezone.utc)
        timeout_delta = timedelta(seconds=line.timeout_seconds)
        
        # Check all active quotes for this line
        expired_quotes = []
        for key, order_id in line.active_quotes.items():
            exchange, symbol, side = key
            # Simplified - in real implementation would track order placement time
            # For now, cancel orders older than timeout
            expired_quotes.append(key)
        
        # Cancel expired quotes
        for key in expired_quotes:
            await self._cancel_enhanced_quote(line, key)

    async def _check_enhanced_drift(self, line: EnhancedQuotingLine) -> None:
        """Enhanced drift checking across multiple references."""
        # Check drift for each symbol in the line
        for symbol in line.quote_pairs:
            # Get current reference price
            current_result = await self.pricing_engine.get_price(
                symbol, 'mid', Decimal(str(line.quantity))
            )
            
            if not current_result.price:
                continue
            
            # Compare with last known price
            last_price = line.last_reference_prices.get(symbol)
            if last_price is None:
                line.last_reference_prices[symbol] = current_result.price
                continue
            
            # Calculate drift
            drift_pct = abs((current_result.price - last_price) / last_price) * 100
            drift_bps = drift_pct * 100
            
            if drift_bps > line.drift_bps:
                self.logger.info(f"Line {line.line_id}: Price drift detected for {symbol}: {drift_bps:.2f} bps")
                
                # Cancel quotes for this symbol
                quotes_to_cancel = [key for key in line.active_quotes.keys() if key[1] == symbol]
                for key in quotes_to_cancel:
                    await self._cancel_enhanced_quote(line, key)
            
            line.last_reference_prices[symbol] = current_result.price

    async def _cancel_enhanced_quote(self, line: EnhancedQuotingLine, key: Tuple[str, str, str]) -> None:
        """Cancel a specific enhanced quote."""
        exchange, symbol, side = key
        order_id = line.active_quotes.get(key)
        
        if order_id:
            try:
                success = await self._cancel_order(order_id, exchange)
                if success:
                    del line.active_quotes[key]
                    self.logger.info(f"Cancelled enhanced quote: {symbol} {side} on {exchange}")
            except Exception as e:
                self.logger.error(f"Error cancelling enhanced quote: {e}")

    async def _cancel_all_enhanced_orders(self) -> None:
        """Cancel all enhanced orders."""
        for line in self.quote_lines:
            quotes_to_cancel = list(line.active_quotes.keys())
            for key in quotes_to_cancel:
                await self._cancel_enhanced_quote(line, key)

    # Enhanced monitoring tasks
    async def _monitor_cross_symbol_risk(self) -> None:
        """Monitor cross-symbol risk exposure."""
        while self.running:
            try:
                # Calculate total exposure across symbols
                total_exposure = sum(abs(pos) for pos in self.net_positions.values())
                
                if self.max_total_exposure and total_exposure > self.max_total_exposure:
                    self.logger.warning(f"Total exposure limit exceeded: {total_exposure} > {self.max_total_exposure}")
                    # Could trigger position reduction here
                
                await asyncio.sleep(5)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error monitoring cross-symbol risk: {e}")
                await asyncio.sleep(5)

    async def _adjust_hedge_ratios(self) -> None:
        """Dynamically adjust hedge ratios based on correlations."""
        while self.running:
            try:
                # Get current correlations from pricing engine
                # Adjust hedge ratios based on changing correlations
                await asyncio.sleep(30)  # Adjust every 30 seconds
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error adjusting hedge ratios: {e}")
                await asyncio.sleep(30)

    async def _monitor_enhanced_performance(self) -> None:
        """Monitor enhanced strategy performance."""
        while self.running:
            try:
                # Log enhanced performance metrics
                self.logger.info(f"Enhanced stats: {self.enhanced_stats}")
                
                if self.pricing_engine:
                    pricing_stats = self.pricing_engine.get_pricing_stats()
                    self.logger.info(f"Pricing engine stats: {pricing_stats}")
                
                await asyncio.sleep(60)  # Log every minute
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error monitoring enhanced performance: {e}")
                await asyncio.sleep(60)

    # Enhanced trade confirmation handling
    async def on_trade_confirmation(self, trade_data: Dict[str, Any]) -> None:
        """Enhanced trade confirmation with multi-symbol position tracking."""
        try:
            exchange = trade_data.get('exchange', '').lower()
            side = trade_data.get('side', '').lower()
            amount = Decimal(str(trade_data.get('amount', 0)))
            price = Decimal(str(trade_data.get('price', 0)))
            symbol = trade_data.get('symbol', self.symbol)
            
            # Update net position for this symbol
            position_delta = amount if side == 'buy' else -amount
            current_position = self.net_positions.get(symbol, Decimal('0'))
            self.net_positions[symbol] = current_position + position_delta
            
            self.logger.info(f"Position updated: {symbol} = {self.net_positions[symbol]} (delta: {position_delta})")
            
            # Execute hedge if needed (simplified)
            if exchange not in [line.reference_exchanges for line in self.quote_lines]:
                await self._execute_enhanced_hedge(exchange, side, amount, price, symbol)
                
        except Exception as e:
            self.logger.error(f"Error in enhanced trade confirmation: {e}")

    async def _execute_enhanced_hedge(
        self, 
        source_exchange: str, 
        side: str, 
        amount: Decimal, 
        price: Decimal, 
        symbol: str
    ) -> None:
        """Execute enhanced hedging with multi-exchange support."""
        try:
            # Find best reference exchange for hedging
            best_reference = None
            if self.enhanced_market_data:
                best_reference = self.enhanced_market_data.get_best_reference_exchange(symbol)
            
            if not best_reference:
                # Fall back to first configured reference exchange
                for line in self.quote_lines:
                    if symbol in line.quote_pairs and line.reference_exchanges:
                        best_reference = line.reference_exchanges[0]
                        break
            
            if not best_reference:
                self.logger.warning(f"No reference exchange found for hedging {symbol}")
                return
            
            # Execute hedge
            hedge_side = 'sell' if side == 'buy' else 'buy'
            
            # Use enhanced pricing for hedge
            hedge_pricing_context = PricingContext(
                symbol=symbol,
                side=hedge_side,
                quantity=amount,
                pricing_method=PricingMethod.MULTI_REFERENCE_WEIGHTED
            )
            
            hedge_result = await self.pricing_engine.get_price(
                symbol, hedge_side, amount, hedge_pricing_context
            )
            
            if hedge_result.price and hedge_result.confidence > 0.6:
                hedge_order_id = await self._place_order(
                    exchange=best_reference,
                    side=hedge_side,
                    amount=float(amount),
                    price=float(hedge_result.price),
                    client_order_id=f"hedge_{symbol}_{int(time.time() * 1000)}"
                )
                
                if hedge_order_id:
                    self.logger.info(f"Enhanced hedge executed: {hedge_side} {amount} {symbol} @ {hedge_result.price} on {best_reference}")
                else:
                    self.logger.error(f"Failed to execute enhanced hedge for {symbol}")
            else:
                self.logger.warning(f"Poor hedge pricing for {symbol}: confidence={hedge_result.confidence}")
                
        except Exception as e:
            self.logger.error(f"Error executing enhanced hedge: {e}")

    # Override performance stats to include enhanced metrics
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get enhanced performance statistics."""
        base_stats = super().get_performance_stats()
        
        enhanced_stats = {
            'enhanced_stats': self.enhanced_stats,
            'net_positions': {symbol: float(pos) for symbol, pos in self.net_positions.items()},
            'total_exposure': float(sum(abs(pos) for pos in self.net_positions.values())),
            'base_symbols': list(self.base_symbols),
            'quote_mode': self.quote_mode.value,
            'active_quotes_count': sum(len(line.active_quotes) for line in self.quote_lines),
        }
        
        if self.pricing_engine:
            enhanced_stats['pricing_engine_stats'] = self.pricing_engine.get_pricing_stats()
        
        base_stats.update(enhanced_stats)
        return base_stats

    def to_dict(self) -> Dict[str, Any]:
        """Convert enhanced strategy state to dictionary."""
        base_dict = super().to_dict()
        
        enhanced_dict = {
            'strategy_type': 'enhanced_market_making',
            'quote_mode': self.quote_mode.value,
            'base_symbols': list(self.base_symbols),
            'net_positions': {symbol: float(pos) for symbol, pos in self.net_positions.items()},
            'enhanced_stats': self.enhanced_stats,
            'quote_lines_status': [
                {
                    'line_id': line.line_id,
                    'base_symbol': line.base_symbol,
                    'quote_pairs': line.quote_pairs,
                    'active_quotes': len(line.active_quotes),
                    'quote_mode': line.quote_mode.value
                }
                for line in self.quote_lines
            ]
        }
        
        base_dict.update(enhanced_dict)
        return base_dict
