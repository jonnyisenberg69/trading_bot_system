"""
Shared Database Connection Pool - Optimized database access for multi-pair strategies.

This module provides shared database connection pooling to minimize resource usage
and optimize query performance across multiple strategies and trading pairs.
"""

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Tuple, AsyncGenerator
from decimal import Decimal
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.pool import QueuePool
from sqlalchemy import text
import os
from contextlib import asynccontextmanager
from collections import defaultdict

logger = logging.getLogger(__name__)


class SharedDatabasePool:
    """
    Shared database connection pool optimized for multi-pair trading strategies.
    
    Features:
    - Single connection pool shared across all strategies
    - Query result caching for frequently accessed data
    - Batch query optimization
    - Connection health monitoring
    - Automatic failover and reconnection
    """
    
    def __init__(
        self,
        max_connections: int = 20,
        cache_ttl_seconds: int = 30,
        batch_size: int = 100
    ):
        self.max_connections = max_connections
        self.cache_ttl_seconds = cache_ttl_seconds
        self.batch_size = batch_size
        self.logger = logger
        
        # Database connection
        self.engine = None
        self.session_maker = None
        
        # Query result caching
        self.query_cache: Dict[str, Tuple[Any, float]] = {}  # query_key -> (result, timestamp)
        self.cache_hits = 0
        self.cache_misses = 0
        
        # Batch processing
        self.pending_queries: Dict[str, List[Tuple[str, Dict[str, Any], asyncio.Future]]] = defaultdict(list)
        self.batch_task: Optional[asyncio.Task] = None
        
        # Performance tracking
        self.total_queries = 0
        self.query_times: List[float] = []
        self.active_sessions = 0
        
        # Connection health
        self.last_health_check = time.time()
        self.health_check_interval = 30  # seconds
        self.connection_failures = 0
        
        self.running = False
    
    async def initialize(self):
        """Initialize the shared database pool."""
        if self.running:
            return
        
        try:
            # Get database URL
            db_url = self._get_database_url()
            
            # Create optimized engine with connection pooling
            self.engine = create_async_engine(
                db_url,
                poolclass=QueuePool,
                pool_size=self.max_connections // 2,  # Core connections
                max_overflow=self.max_connections // 2,  # Overflow connections
                pool_pre_ping=True,  # Verify connections before use
                pool_recycle=3600,  # Recycle connections every hour
                echo=False,  # Disable SQL logging for performance
                future=True
            )
            
            # Create session maker
            self.session_maker = async_sessionmaker(
                self.engine,
                class_=AsyncSession,
                expire_on_commit=False
            )
            
            # Test connection
            async with self.session_maker() as session:
                await session.execute(text("SELECT 1"))
            
            self.running = True
            
            # Start batch processing task
            self.batch_task = asyncio.create_task(self._batch_processing_loop())
            
            self.logger.info(f"Shared database pool initialized with {self.max_connections} max connections")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize shared database pool: {e}")
            raise
    
    async def shutdown(self):
        """Shutdown the shared database pool."""
        if not self.running:
            return
        
        self.running = False
        
        # Cancel batch task
        if self.batch_task:
            self.batch_task.cancel()
            try:
                await self.batch_task
            except asyncio.CancelledError:
                pass
        
        # Close engine
        if self.engine:
            await self.engine.dispose()
        
        self.logger.info("Shared database pool shutdown complete")
    
    def _get_database_url(self) -> str:
        """Get database URL with optimized parameters."""
        # Check for environment variable first
        db_url = os.getenv('TRADE_DATA_DATABASE_URL')
        if db_url:
            return db_url
        
        # Build URL with connection pooling optimizations
        pg_host = os.getenv('POSTGRES_HOST', 'localhost')
        pg_port = os.getenv('POSTGRES_PORT', '5432')
        pg_user = os.getenv('POSTGRES_USER', 'jonnyisenberg')
        pg_password = os.getenv('POSTGRES_PASSWORD', 'hello')
        pg_database = os.getenv('POSTGRES_DB', 'trading_system_data')
        
        # Add connection pooling and performance parameters
        return (
            f"postgresql+asyncpg://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_database}"
            "?server_settings=jit%3Doff"  # Disable JIT for consistent performance
            "&server_settings=shared_preload_libraries%3D''"  # Minimal extensions
            "&command_timeout=10"  # 10 second timeout
            "&statement_cache_size=0"  # Disable statement cache for multi-query workload
        )
    
    @asynccontextmanager
    async def get_session(self) -> AsyncGenerator[AsyncSession, None]:
        """Get database session from shared pool."""
        if not self.running:
            raise RuntimeError("Database pool not initialized")
        
        session = None
        try:
            self.active_sessions += 1
            session = self.session_maker()
            yield session
        except Exception as e:
            if session:
                await session.rollback()
            self.connection_failures += 1
            raise
        finally:
            if session:
                await session.close()
            self.active_sessions -= 1
    
    async def execute_cached_query(
        self, 
        query_key: str, 
        query: str, 
        params: Dict[str, Any],
        cache_ttl: Optional[int] = None
    ) -> List[Any]:
        """
        Execute query with caching for frequently accessed data.
        
        Args:
            query_key: Unique key for caching
            query: SQL query text
            params: Query parameters
            cache_ttl: Cache TTL override
            
        Returns:
            Query results
        """
        # Check cache first
        if query_key in self.query_cache:
            result, timestamp = self.query_cache[query_key]
            ttl = cache_ttl or self.cache_ttl_seconds
            
            if time.time() - timestamp < ttl:
                self.cache_hits += 1
                return result
        
        # Execute query
        start_time = time.time()
        
        async with self.get_session() as session:
            result = await session.execute(text(query), params)
            rows = result.fetchall()
        
        # Track performance
        query_time = time.time() - start_time
        self.query_times.append(query_time)
        if len(self.query_times) > 1000:
            self.query_times = self.query_times[-500:]  # Keep last 500
        
        self.total_queries += 1
        self.cache_misses += 1
        
        # Cache result
        self.query_cache[query_key] = (rows, time.time())
        
        # Cleanup old cache entries
        if len(self.query_cache) > 1000:
            self._cleanup_cache()
        
        return rows
    
    async def batch_execute_queries(
        self, 
        queries: List[Tuple[str, Dict[str, Any]]]
    ) -> List[List[Any]]:
        """
        Execute multiple queries in a single session for efficiency.
        
        Args:
            queries: List of (query, params) tuples
            
        Returns:
            List of query results
        """
        results = []
        start_time = time.time()
        
        async with self.get_session() as session:
            for query_text, params in queries:
                result = await session.execute(text(query_text), params)
                results.append(result.fetchall())
        
        # Track batch performance
        batch_time = time.time() - start_time
        self.logger.debug(f"Batch executed {len(queries)} queries in {batch_time:.3f}s")
        
        return results
    
    async def get_recent_trades_optimized(
        self,
        symbols: List[str],
        exchanges: List[str], 
        start_time: datetime,
        end_time: Optional[datetime] = None
    ) -> Dict[Tuple[str, str], List[Dict[str, Any]]]:
        """
        Get recent trades for multiple symbols/exchanges with single query.
        
        Optimized for multi-pair strategies by batching symbol/exchange combinations.
        """
        if end_time is None:
            end_time = datetime.now(timezone.utc)
        
        # Convert to naive datetime for database
        start_naive = start_time.replace(tzinfo=None) if start_time.tzinfo else start_time
        end_naive = end_time.replace(tzinfo=None) if end_time.tzinfo else end_time
        
        # Build optimized query for multiple symbols/exchanges
        symbol_patterns = []
        for symbol in symbols:
            base_symbol = symbol
            symbol_patterns.extend([
                base_symbol,
                f"{base_symbol}-PERP",
                f"{base_symbol}:USDT",
                base_symbol.replace('/USDT', '/USDC:USDC')
            ])
        
        # Use IN clause for efficiency
        placeholders = ','.join([f':symbol_{i}' for i in range(len(symbol_patterns))])
        exchange_placeholders = ','.join([f':exchange_{i}' for i in range(len(exchanges))])
        
        query = f"""
            SELECT exchange, symbol, side, amount, price, timestamp, fee_cost, fee_currency, cost
            FROM risk_trades
            WHERE symbol IN ({placeholders})
            AND exchange IN ({exchange_placeholders})  
            AND timestamp BETWEEN :start_time AND :end_time
            ORDER BY timestamp ASC
        """
        
        # Build parameters
        params = {
            'start_time': start_naive,
            'end_time': end_naive
        }
        
        for i, symbol_pattern in enumerate(symbol_patterns):
            params[f'symbol_{i}'] = symbol_pattern
        
        for i, exchange in enumerate(exchanges):
            params[f'exchange_{i}'] = exchange
        
        # Execute with caching
        cache_key = f"recent_trades:{hash(str(sorted(symbols)))}:{hash(str(sorted(exchanges)))}:{start_time.timestamp()}"
        rows = await self.execute_cached_query(cache_key, query, params, cache_ttl=10)  # 10 second cache
        
        # Group results by (symbol, exchange)
        grouped_results = defaultdict(list)
        
        for row in rows:
            trade_dict = {
                'exchange': row.exchange,
                'symbol': row.symbol, 
                'side': row.side,
                'amount': float(row.amount),
                'price': float(row.price),
                'timestamp': row.timestamp,
                'fee_cost': float(row.fee_cost) if row.fee_cost else None,
                'fee_currency': row.fee_currency,
                'cost': float(row.cost) if hasattr(row, 'cost') and row.cost else None
            }
            
            grouped_results[(row.symbol, row.exchange)].append(trade_dict)
        
        return dict(grouped_results)
    
    def _cleanup_cache(self):
        """Clean up old cache entries."""
        current_time = time.time()
        expired_keys = [
            key for key, (_, timestamp) in self.query_cache.items()
            if current_time - timestamp > self.cache_ttl_seconds * 2
        ]
        
        for key in expired_keys:
            del self.query_cache[key]
        
        if expired_keys:
            self.logger.debug(f"Cleaned up {len(expired_keys)} expired cache entries")
    
    async def _batch_processing_loop(self):
        """Background loop for processing batched queries."""
        while self.running:
            try:
                # Process any pending batch queries
                if self.pending_queries:
                    for query_type, queries in self.pending_queries.items():
                        if queries:
                            # Execute batch
                            query_tuples = [(query, params) for query, params, _ in queries]
                            results = await self.batch_execute_queries(query_tuples)
                            
                            # Set results on futures
                            for i, (_, _, future) in enumerate(queries):
                                if not future.cancelled():
                                    future.set_result(results[i])
                            
                            # Clear processed queries
                            self.pending_queries[query_type].clear()
                
                await asyncio.sleep(0.01)  # 10ms batch window
                
            except Exception as e:
                self.logger.error(f"Error in batch processing loop: {e}")
                await asyncio.sleep(0.1)
    
    def get_pool_stats(self) -> Dict[str, Any]:
        """Get connection pool statistics."""
        pool_stats = {}
        
        if self.engine and hasattr(self.engine.pool, 'size'):
            pool = self.engine.pool
            pool_stats = {
                'pool_size': pool.size(),
                'checked_in': pool.checkedin(),
                'checked_out': pool.checkedout(),
                'overflow': pool.overflow(),
                'invalid': pool.invalid()
            }
        
        return {
            'connection_pool': pool_stats,
            'active_sessions': self.active_sessions,
            'total_queries': self.total_queries,
            'connection_failures': self.connection_failures,
            'cache_stats': {
                'hits': self.cache_hits,
                'misses': self.cache_misses,
                'hit_rate': self.cache_hits / max(self.cache_hits + self.cache_misses, 1),
                'cached_queries': len(self.query_cache)
            },
            'performance': {
                'avg_query_time_ms': sum(self.query_times) / len(self.query_times) * 1000 if self.query_times else 0,
                'p95_query_time_ms': sorted(self.query_times)[int(0.95 * len(self.query_times))] * 1000 if len(self.query_times) > 20 else 0
            }
        }


# Global shared database pool
_shared_db_pool: Optional[SharedDatabasePool] = None


async def get_shared_database_pool() -> SharedDatabasePool:
    """Get or create the global shared database pool."""
    global _shared_db_pool
    
    if _shared_db_pool is None:
        _shared_db_pool = SharedDatabasePool()
        await _shared_db_pool.initialize()
    
    return _shared_db_pool


async def shutdown_shared_database_pool():
    """Shutdown the global shared database pool."""
    global _shared_db_pool
    
    if _shared_db_pool:
        await _shared_db_pool.shutdown()
        _shared_db_pool = None


@asynccontextmanager
async def get_optimized_session() -> AsyncGenerator[AsyncSession, None]:
    """Get an optimized database session from the shared pool."""
    pool = await get_shared_database_pool()
    async with pool.get_session() as session:
        yield session


class OptimizedTradeDataManager:
    """
    Optimized trade data manager using shared database pool.
    
    Replaces individual TradeDataManager instances with shared, efficient access.
    """
    
    def __init__(self):
        self.logger = logger
        self._initialized = False
    
    async def initialize(self):
        """Initialize using shared pool (no-op, pool handles initialization)."""
        self._initialized = True
    
    async def get_trades_for_multiple_strategies(
        self,
        strategy_symbols: Dict[str, List[str]],  # strategy_id -> symbols
        strategy_exchanges: Dict[str, List[str]],  # strategy_id -> exchanges
        start_time: datetime,
        end_time: Optional[datetime] = None
    ) -> Dict[str, Dict[Tuple[str, str], List[Dict[str, Any]]]]:
        """
        Get trades for multiple strategies in a single optimized query.
        
        Returns:
            Dict[strategy_id, Dict[(symbol, exchange), trades]]
        """
        try:
            # Collect all unique symbols and exchanges
            all_symbols = set()
            all_exchanges = set()
            
            for symbols in strategy_symbols.values():
                all_symbols.update(symbols)
            
            for exchanges in strategy_exchanges.values():
                all_exchanges.update(exchanges)
            
            # Single query for all data
            pool = await get_shared_database_pool()
            trade_data = await pool.get_recent_trades_optimized(
                list(all_symbols), list(all_exchanges), start_time, end_time
            )
            
            # Distribute results to strategies
            strategy_results = {}
            
            for strategy_id in strategy_symbols.keys():
                strategy_results[strategy_id] = {}
                
                strategy_syms = strategy_symbols[strategy_id]
                strategy_exs = strategy_exchanges[strategy_id]
                
                for (symbol, exchange), trades in trade_data.items():
                    # Check if this strategy needs this data
                    symbol_matches = any(
                        symbol == s or symbol.startswith(s.split('/')[0]) 
                        for s in strategy_syms
                    )
                    exchange_matches = exchange in strategy_exs
                    
                    if symbol_matches and exchange_matches:
                        strategy_results[strategy_id][(symbol, exchange)] = trades
            
            return strategy_results
            
        except Exception as e:
            self.logger.error(f"Error getting trades for multiple strategies: {e}")
            return {}
    
    async def close(self):
        """Close (no-op, shared pool handles cleanup)."""
        pass
