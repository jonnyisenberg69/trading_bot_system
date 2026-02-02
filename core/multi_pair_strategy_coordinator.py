"""
Multi-Pair Strategy Coordinator - Optimized coordination for running multiple strategies efficiently.

This coordinator manages multiple trading pairs with shared resources and optimized processing:
- Shared market data services
- Batched coefficient calculations  
- Priority-based order processing
- Resource pooling and connection management
"""

import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Set, Callable, Tuple
from collections import defaultdict, deque
import logging
from decimal import Decimal

from .shared_market_data_service import SharedMarketDataService, get_shared_market_data_service
from .shared_database_pool import SharedDatabasePool, get_shared_database_pool, OptimizedTradeDataManager
from ..market_data_collection.moving_averages import MovingAverageCalculator, MovingAverageConfig
from ..market_data_collection.exchange_coefficients import SimpleExchangeCoefficientCalculator

logger = logging.getLogger(__name__)


@dataclass
class StrategyRegistration:
    """Registration information for a strategy in the coordinator."""
    strategy_id: str
    strategy_instance: Any  # The actual strategy instance
    symbols: List[str]
    exchanges: List[str]
    priority: int = 1  # Higher priority gets more CPU time
    last_update: float = field(default_factory=time.time)
    coefficient_config: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PairGroup:
    """Group of strategies trading the same pair for optimized processing."""
    symbol: str
    strategies: List[str] = field(default_factory=list)
    shared_ma_calculator: Optional[MovingAverageCalculator] = None
    shared_coefficient_calculator: Optional[SimpleExchangeCoefficientCalculator] = None
    last_coefficient_update: float = 0
    update_frequency: float = 10.0  # seconds between coefficient updates


class MultiPairStrategyCoordinator:
    """
    Coordinator for running multiple trading pairs efficiently with shared resources.
    
    Key optimizations:
    - Single shared market data service for all strategies
    - Shared coefficient calculations per trading pair
    - Batched database operations
    - Priority-based processing queues
    - Resource pooling and connection sharing
    """
    
    def __init__(
        self,
        max_strategies: int = 100,
        max_pairs: int = 50,
        latency_target_ms: float = 1.0
    ):
        self.max_strategies = max_strategies
        self.max_pairs = max_pairs
        self.latency_target_ms = latency_target_ms
        self.logger = logger
        
        # Strategy management
        self.registered_strategies: Dict[str, StrategyRegistration] = {}
        self.pair_groups: Dict[str, PairGroup] = {}  # symbol -> pair group
        
        # Shared services
        self.shared_market_data: Optional[SharedMarketDataService] = None
        self.shared_database: Optional[SharedDatabasePool] = None
        self.optimized_trade_manager: Optional[OptimizedTradeDataManager] = None
        
        # Processing optimization
        self.processing_queues: Dict[int, asyncio.Queue] = {}  # priority -> queue
        self.batch_processors: Dict[int, asyncio.Task] = {}  # priority -> task
        
        # Performance tracking
        self.total_strategies = 0
        self.total_pairs = 0
        self.processing_latencies: deque = deque(maxlen=1000)
        self.resource_usage_history: deque = deque(maxlen=100)
        
        # Background tasks
        self.running = False
        self.coordinator_task: Optional[asyncio.Task] = None
        self.coefficient_batch_task: Optional[asyncio.Task] = None
        self.performance_monitor_task: Optional[asyncio.Task] = None
    
    async def start(self):
        """Start the multi-pair coordinator with optimized shared services."""
        if self.running:
            return
            
        self.running = True
        self.logger.info("Starting Multi-Pair Strategy Coordinator")
        
        # Initialize shared services
        self.shared_market_data = await get_shared_market_data_service()
        self.shared_database = await get_shared_database_pool()
        self.optimized_trade_manager = OptimizedTradeDataManager()
        await self.optimized_trade_manager.initialize()
        
        # Initialize processing queues by priority
        for priority in [1, 2, 3, 4, 5]:  # 5 priority levels
            self.processing_queues[priority] = asyncio.Queue(maxsize=1000)
            self.batch_processors[priority] = asyncio.create_task(
                self._priority_batch_processor(priority)
            )
        
        # Start background tasks
        self.coordinator_task = asyncio.create_task(self._coordinator_loop())
        self.coefficient_batch_task = asyncio.create_task(self._coefficient_batch_loop())
        self.performance_monitor_task = asyncio.create_task(self._performance_monitor_loop())
        
        self.logger.info("Multi-Pair Strategy Coordinator started with optimized shared services")
    
    async def stop(self):
        """Stop the coordinator and clean up shared resources."""
        if not self.running:
            return
            
        self.running = False
        
        # Cancel background tasks
        tasks = [
            self.coordinator_task,
            self.coefficient_batch_task, 
            self.performance_monitor_task
        ] + list(self.batch_processors.values())
        
        for task in tasks:
            if task:
                task.cancel()
        
        if any(tasks):
            await asyncio.gather(*tasks, return_exceptions=True)
        
        # Unsubscribe all strategies
        for strategy_id in list(self.registered_strategies.keys()):
            await self.unregister_strategy(strategy_id)
        
        self.logger.info("Multi-Pair Strategy Coordinator stopped")
    
    async def register_strategy(
        self,
        strategy_id: str,
        strategy_instance: Any,
        symbols: List[str],
        exchanges: List[str],
        priority: int = 1,
        coefficient_config: Optional[Dict[str, Any]] = None
    ):
        """
        Register a strategy for optimized multi-pair coordination.
        
        Args:
            strategy_id: Unique strategy identifier
            strategy_instance: The strategy instance
            symbols: Trading symbols for this strategy
            exchanges: Exchanges for this strategy
            priority: Processing priority (1-5, higher = more important)
            coefficient_config: Configuration for coefficient calculations
        """
        if len(self.registered_strategies) >= self.max_strategies:
            raise ValueError(f"Maximum strategies ({self.max_strategies}) already registered")
        
        # Register strategy
        registration = StrategyRegistration(
            strategy_id=strategy_id,
            strategy_instance=strategy_instance,
            symbols=symbols,
            exchanges=exchanges,
            priority=priority,
            coefficient_config=coefficient_config or {}
        )
        
        self.registered_strategies[strategy_id] = registration
        
        # Subscribe to shared market data
        await self.shared_market_data.subscribe_strategy(
            strategy_id=strategy_id,
            symbols=symbols,
            exchanges=exchanges,
            update_callback=self._create_strategy_update_callback(strategy_id),
            priority=priority
        )
        
        # Setup shared coefficient calculations per trading pair
        for symbol in symbols:
            await self._setup_pair_group(symbol, strategy_id, coefficient_config or {})
        
        self.total_strategies += 1
        self.total_pairs = len(self.pair_groups)
        
        self.logger.info(
            f"Registered strategy {strategy_id} for {len(symbols)} symbols, "
            f"priority {priority} (total strategies: {self.total_strategies})"
        )
    
    async def unregister_strategy(self, strategy_id: str):
        """Unregister a strategy and clean up resources."""
        if strategy_id not in self.registered_strategies:
            return
        
        registration = self.registered_strategies[strategy_id]
        
        # Unsubscribe from market data
        await self.shared_market_data.unsubscribe_strategy(strategy_id)
        
        # Remove from pair groups
        for symbol in registration.symbols:
            if symbol in self.pair_groups:
                pair_group = self.pair_groups[symbol]
                if strategy_id in pair_group.strategies:
                    pair_group.strategies.remove(strategy_id)
                
                # Clean up empty pair groups
                if not pair_group.strategies:
                    del self.pair_groups[symbol]
        
        # Remove registration
        del self.registered_strategies[strategy_id]
        
        self.total_strategies -= 1
        self.total_pairs = len(self.pair_groups)
        
        self.logger.info(f"Unregistered strategy {strategy_id}")
    
    async def _setup_pair_group(self, symbol: str, strategy_id: str, coefficient_config: Dict[str, Any]):
        """Setup shared coefficient calculation for a trading pair."""
        if symbol not in self.pair_groups:
            # Create new pair group with shared coefficient calculation
            pair_group = PairGroup(symbol=symbol)
            
            # Create shared MA calculator for this pair
            time_periods = coefficient_config.get('time_periods', ['5min', '15min', '30min'])
            ma_configs = self._create_ma_configs_from_periods(time_periods)
            pair_group.shared_ma_calculator = MovingAverageCalculator(ma_configs)
            
            # Create shared coefficient calculator
            pair_group.shared_coefficient_calculator = SimpleExchangeCoefficientCalculator(
                ma_calculator=pair_group.shared_ma_calculator,
                calculation_method=coefficient_config.get('coefficient_method', 'min'),
                min_coefficient=coefficient_config.get('min_coefficient', 0.2),
                max_coefficient=coefficient_config.get('max_coefficient', 3.0)
            )
            
            self.pair_groups[symbol] = pair_group
        
        # Add strategy to pair group
        self.pair_groups[symbol].strategies.append(strategy_id)
    
    def _create_ma_configs_from_periods(self, time_periods: List[str]) -> List[MovingAverageConfig]:
        """Create MA configs from time periods (optimized for multi-pair)."""
        configs = []
        
        for period_str in time_periods:
            period_points = self._convert_time_period_to_points(period_str)
            if period_points > 0:
                # Limit configs for multi-pair efficiency
                configs.extend([
                    MovingAverageConfig(period=period_points, ma_type='sma', volume_type='quote'),
                    MovingAverageConfig(period=period_points, ma_type='ewma', volume_type='quote')
                ])
        
        return configs
    
    def _convert_time_period_to_points(self, period_str: str) -> int:
        """Convert time period to data points."""
        conversions = {
            '30s': 3, '1min': 6, '5min': 30, '15min': 90, '30min': 180, '60min': 360
        }
        return conversions.get(period_str, 0)
    
    def _create_strategy_update_callback(self, strategy_id: str) -> Callable:
        """Create optimized update callback for a strategy."""
        async def callback(update_data: Dict[str, Any]):
            try:
                # Add to processing queue based on strategy priority
                registration = self.registered_strategies.get(strategy_id)
                if registration:
                    priority = registration.priority
                    
                    # Create processing task
                    task_data = {
                        'type': 'market_update',
                        'strategy_id': strategy_id,
                        'data': update_data,
                        'timestamp_ns': time.perf_counter_ns()
                    }
                    
                    try:
                        self.processing_queues[priority].put_nowait(task_data)
                    except asyncio.QueueFull:
                        # Drop update if queue full (better than blocking)
                        self.logger.warning(f"Dropped update for {strategy_id} - queue full")
            
            except Exception as e:
                self.logger.error(f"Error in strategy update callback: {e}")
        
        return callback
    
    async def _priority_batch_processor(self, priority: int):
        """Process updates for a specific priority level."""
        queue = self.processing_queues[priority]
        
        while self.running:
            try:
                # Batch collect updates
                updates = []
                deadline = time.time() + 0.001  # 1ms batching window
                
                # Collect updates until deadline or batch full
                while time.time() < deadline and len(updates) < self.batch_size:
                    try:
                        update = await asyncio.wait_for(queue.get(), timeout=0.0001)  # 0.1ms timeout
                        updates.append(update)
                    except asyncio.TimeoutError:
                        break
                
                # Process batch if we have updates
                if updates:
                    processing_start = time.perf_counter_ns()
                    
                    # Group updates by strategy for efficient processing
                    strategy_updates = defaultdict(list)
                    for update in updates:
                        strategy_updates[update['strategy_id']].append(update)
                    
                    # Process each strategy's updates concurrently
                    tasks = []
                    for strategy_id, strategy_update_list in strategy_updates.items():
                        task = self._process_strategy_updates(strategy_id, strategy_update_list)
                        tasks.append(task)
                    
                    # Execute all strategy updates in parallel
                    await asyncio.gather(*tasks, return_exceptions=True)
                    
                    # Track processing latency
                    processing_time_us = (time.perf_counter_ns() - processing_start) / 1000
                    self.processing_latencies.append(processing_time_us)
                
                # Minimal yield to prevent CPU hogging
                await asyncio.sleep(0)
                
            except Exception as e:
                self.logger.error(f"Error in priority {priority} batch processor: {e}")
                await asyncio.sleep(0.001)
    
    async def _process_strategy_updates(self, strategy_id: str, updates: List[Dict[str, Any]]):
        """Process batched updates for a specific strategy."""
        try:
            registration = self.registered_strategies.get(strategy_id)
            if not registration:
                return
            
            strategy = registration.strategy_instance
            
            # Check if strategy has optimized batch update method
            if hasattr(strategy, 'process_batch_market_updates'):
                await strategy.process_batch_market_updates(updates)
            else:
                # Fallback: process updates individually (legacy compatibility)
                for update in updates:
                    if hasattr(strategy, '_on_market_data_update'):
                        try:
                            await strategy._on_market_data_update(update['data'])
                        except Exception as e:
                            self.logger.error(f"Error in strategy update: {e}")
        
        except Exception as e:
            self.logger.error(f"Error processing strategy updates for {strategy_id}: {e}")
    
    async def _coefficient_batch_loop(self):
        """Batched coefficient calculation for all pairs."""
        while self.running:
            try:
                current_time = time.time()
                
                # Identify pair groups that need coefficient updates
                pairs_to_update = []
                
                for symbol, pair_group in self.pair_groups.items():
                    if current_time - pair_group.last_coefficient_update > pair_group.update_frequency:
                        pairs_to_update.append((symbol, pair_group))
                
                if pairs_to_update:
                    # Batch collect trade data for all pairs that need updates
                    symbols = [symbol for symbol, _ in pairs_to_update]
                    all_exchanges = set()
                    
                    for _, pair_group in pairs_to_update:
                        for strategy_id in pair_group.strategies:
                            registration = self.registered_strategies.get(strategy_id)
                            if registration:
                                all_exchanges.update(registration.exchanges)
                    
                    # Single database query for all pairs
                    start_time = datetime.now(timezone.utc) - timedelta(hours=1)
                    
                    strategy_symbols = {}
                    strategy_exchanges = {}
                    
                    for symbol, pair_group in pairs_to_update:
                        for strategy_id in pair_group.strategies:
                            strategy_symbols[strategy_id] = [symbol]
                            registration = self.registered_strategies.get(strategy_id)
                            strategy_exchanges[strategy_id] = list(registration.exchanges) if registration else []
                    
                    # Batch query
                    all_trade_data = await self.optimized_trade_manager.get_trades_for_multiple_strategies(
                        strategy_symbols, strategy_exchanges, start_time
                    )
                    
                    # Update coefficients for all pairs concurrently
                    coefficient_tasks = []
                    for symbol, pair_group in pairs_to_update:
                        task = self._update_pair_coefficients(symbol, pair_group, all_trade_data)
                        coefficient_tasks.append(task)
                    
                    await asyncio.gather(*coefficient_tasks, return_exceptions=True)
                
                await asyncio.sleep(5)  # Check every 5 seconds
                
            except Exception as e:
                self.logger.error(f"Error in coefficient batch loop: {e}")
                await asyncio.sleep(5)
    
    async def _update_pair_coefficients(
        self, 
        symbol: str, 
        pair_group: PairGroup, 
        all_trade_data: Dict[str, Any]
    ):
        """Update coefficients for a trading pair using shared calculation."""
        try:
            # Process trade data for each exchange trading this symbol
            exchanges_processed = set()
            
            for strategy_id in pair_group.strategies:
                strategy_trade_data = all_trade_data.get(strategy_id, {})
                
                for (trade_symbol, exchange), trades in strategy_trade_data.items():
                    if trade_symbol.startswith(symbol.split('/')[0]) and exchange not in exchanges_processed:
                        # Update MA for this exchange using shared calculator
                        if trades:
                            volume_data = pair_group.shared_ma_calculator.calculate_volume_data(trades)
                            pair_group.shared_ma_calculator.update_moving_averages(
                                symbol, exchange, volume_data, datetime.now(timezone.utc)
                            )
                        
                        exchanges_processed.add(exchange)
            
            # Calculate coefficients for all exchanges using shared calculator
            updated_coefficients = {}
            for exchange in exchanges_processed:
                coefficient = pair_group.shared_coefficient_calculator.calculate_coefficient(symbol, exchange)
                if coefficient is not None:
                    updated_coefficients[exchange] = coefficient
            
            # Distribute coefficients to all strategies trading this pair
            if updated_coefficients:
                for strategy_id in pair_group.strategies:
                    registration = self.registered_strategies.get(strategy_id)
                    if registration and hasattr(registration.strategy_instance, 'update_exchange_coefficients'):
                        # Push coefficient update to strategy
                        await registration.strategy_instance.update_exchange_coefficients(updated_coefficients)
            
            # Update last update time
            pair_group.last_coefficient_update = time.time()
            
            self.logger.debug(f"Updated coefficients for {symbol}: {len(updated_coefficients)} exchanges")
            
        except Exception as e:
            self.logger.error(f"Error updating pair coefficients for {symbol}: {e}")
    
    async def _coordinator_loop(self):
        """Main coordinator loop for resource optimization."""
        while self.running:
            try:
                # Monitor resource usage
                await self._monitor_resource_usage()
                
                # Optimize processing queues based on load
                await self._optimize_queue_processing()
                
                # Clean up inactive strategies
                await self._cleanup_inactive_strategies()
                
                await asyncio.sleep(1)  # Coordinator runs every second
                
            except Exception as e:
                self.logger.error(f"Error in coordinator loop: {e}")
                await asyncio.sleep(1)
    
    async def _monitor_resource_usage(self):
        """Monitor and log resource usage for optimization."""
        try:
            # Get shared service stats
            market_data_stats = self.shared_market_data.get_performance_stats()
            database_stats = self.shared_database.get_pool_stats()
            
            # Calculate processing efficiency
            avg_processing_latency = (
                sum(self.processing_latencies) / len(self.processing_latencies)
                if self.processing_latencies else 0
            )
            
            resource_snapshot = {
                'timestamp': time.time(),
                'total_strategies': self.total_strategies,
                'total_pairs': self.total_pairs,
                'market_data_updates': market_data_stats['total_updates'],
                'avg_market_data_latency_us': market_data_stats['avg_latency_us'],
                'database_queries': database_stats['total_queries'],
                'database_cache_hit_rate': database_stats['cache_stats']['hit_rate'],
                'avg_processing_latency_us': avg_processing_latency,
                'active_db_sessions': database_stats['active_sessions']
            }
            
            self.resource_usage_history.append(resource_snapshot)
            
            # Log performance summary every 10 seconds
            if hasattr(self, '_last_perf_log') and time.time() - self._last_perf_log < 10:
                return
                
            self._last_perf_log = time.time()
            
            self.logger.info(
                f"📊 Multi-Pair Performance: {self.total_strategies} strategies, {self.total_pairs} pairs, "
                f"avg_latency={avg_processing_latency:.1f}μs, "
                f"cache_hit={database_stats['cache_stats']['hit_rate']:.1%}, "
                f"updates/s={market_data_stats['total_updates']/60:.0f}"
            )
            
        except Exception as e:
            self.logger.error(f"Error monitoring resource usage: {e}")
    
    async def _optimize_queue_processing(self):
        """Optimize queue processing based on current load."""
        # This could implement dynamic queue priority adjustments
        # based on processing latencies and resource usage
        pass
    
    async def _cleanup_inactive_strategies(self):
        """Clean up strategies that haven't been active recently."""
        current_time = time.time()
        inactive_threshold = 300  # 5 minutes
        
        inactive_strategies = []
        for strategy_id, registration in self.registered_strategies.items():
            if current_time - registration.last_update > inactive_threshold:
                if hasattr(registration.strategy_instance, 'running'):
                    if not registration.strategy_instance.running:
                        inactive_strategies.append(strategy_id)
        
        for strategy_id in inactive_strategies:
            self.logger.info(f"Cleaning up inactive strategy: {strategy_id}")
            await self.unregister_strategy(strategy_id)
    
    async def _performance_monitor_loop(self):
        """Monitor and optimize performance continuously."""
        while self.running:
            try:
                # Check if we're meeting latency targets
                if self.processing_latencies:
                    p95_latency = sorted(self.processing_latencies)[int(0.95 * len(self.processing_latencies))]
                    
                    if p95_latency > self.latency_target_ms * 1000:  # Convert to microseconds
                        self.logger.warning(
                            f"⚠️  P95 latency {p95_latency:.1f}μs exceeds target {self.latency_target_ms}ms. "
                            f"Consider reducing pair count or optimizing further."
                        )
                
                await asyncio.sleep(10)  # Monitor every 10 seconds
                
            except Exception as e:
                self.logger.error(f"Error in performance monitor: {e}")
                await asyncio.sleep(10)
    
    # Public API for strategies
    
    async def get_shared_coefficients(self, strategy_id: str, symbol: str) -> Dict[str, float]:
        """Get shared coefficients for a strategy and symbol."""
        registration = self.registered_strategies.get(strategy_id)
        if not registration:
            return {}
        
        pair_group = self.pair_groups.get(symbol)
        if not pair_group or not pair_group.shared_coefficient_calculator:
            return {}
        
        # Get coefficients for all exchanges this strategy uses
        coefficients = {}
        for exchange in registration.exchanges:
            coeff = pair_group.shared_coefficient_calculator.get_coefficient(symbol, exchange)
            coefficients[exchange] = coeff
        
        return coefficients
    
    def get_coordinator_stats(self) -> Dict[str, Any]:
        """Get comprehensive coordinator statistics."""
        return {
            'total_strategies': self.total_strategies,
            'total_pairs': self.total_pairs,
            'max_strategies': self.max_strategies,
            'max_pairs': self.max_pairs,
            'latency_target_ms': self.latency_target_ms,
            'processing_latencies': {
                'count': len(self.processing_latencies),
                'avg_us': sum(self.processing_latencies) / len(self.processing_latencies) if self.processing_latencies else 0,
                'p95_us': sorted(self.processing_latencies)[int(0.95 * len(self.processing_latencies))] if len(self.processing_latencies) > 20 else 0
            },
            'queue_stats': {
                priority: queue.qsize() 
                for priority, queue in self.processing_queues.items()
            },
            'pair_groups': {
                symbol: len(pair_group.strategies) 
                for symbol, pair_group in self.pair_groups.items()
            },
            'shared_services': {
                'market_data': self.shared_market_data.get_performance_stats() if self.shared_market_data else {},
                'database': self.shared_database.get_pool_stats() if self.shared_database else {}
            }
        }


# Global coordinator instance
_coordinator: Optional[MultiPairStrategyCoordinator] = None


async def get_multi_pair_coordinator() -> MultiPairStrategyCoordinator:
    """Get or create the global multi-pair coordinator."""
    global _coordinator
    
    if _coordinator is None:
        _coordinator = MultiPairStrategyCoordinator()
        await _coordinator.start()
    
    return _coordinator


async def shutdown_multi_pair_coordinator():
    """Shutdown the global multi-pair coordinator."""
    global _coordinator
    
    if _coordinator:
        await _coordinator.stop()
        _coordinator = None
