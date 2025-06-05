"""
Trade Synchronization System

Simple, efficient system for keeping trade data synchronized with exchanges:
- Periodically fetches trades from last sync time to present
- Fills in any missing trades automatically  
- On startup, performs historical backfill to earliest possible date
- Tracks sync state per exchange/symbol for efficient incremental updates

Supported Exchanges:
- Binance (spot + futures)
- Bybit (spot + futures) 
- Hyperliquid (perpetual)
- MEXC (spot)
- Gate.io (spot)
- Bitget (spot)
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any, Tuple
from decimal import Decimal
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
import structlog
import uuid

from .tracking import PositionManager
from database.repositories.trade_repository import TradeRepository
from database.repositories.position_repository import PositionRepository
from exchanges.connectors import (
    TRADING_CONNECTORS, 
    create_exchange_connector,
    list_supported_exchanges
)
from exchanges.base_connector import BaseExchangeConnector
from database.repositories import TradeRepository
from order_management.tracking import PositionManager


logger = structlog.get_logger(__name__)


class SyncStatus(str, Enum):
    """Status of sync operation."""
    PENDING = 'pending'
    RUNNING = 'running'
    COMPLETED = 'completed'
    FAILED = 'failed'


@dataclass
class SyncResult:
    """Result of a trade sync operation."""
    id: str
    exchange: str
    symbol: str
    start_time: datetime
    end_time: datetime
    status: SyncStatus
    trades_fetched: int = 0
    trades_saved: int = 0
    trades_skipped: int = 0
    processing_time_seconds: float = 0
    error_message: Optional[str] = None
    last_trade_time: Optional[datetime] = None
    
    def __post_init__(self):
        if not self.id:
            self.id = str(uuid.uuid4())


class TradeSync:
    """
    Simple trade synchronization system.
    
    Keeps internal trade data synchronized with exchange trade history by:
    - Fetching trades from last sync time to present
    - Saving any new trades to database
    - Tracking sync state per exchange/symbol
    - Performing historical backfill on startup
    """
    
    def __init__(
        self,
        exchange_connectors: Dict[str, Any],
        trade_repository: TradeRepository,
        position_manager: Optional[PositionManager] = None
    ):
        """
        Initialize trade sync system.
        
        Args:
            exchange_connectors: Dictionary of exchange connector instances
            trade_repository: Repository for trade data
            position_manager: Position manager for updating positions
        """
        self.exchange_connectors = exchange_connectors
        self.trade_repository = trade_repository
        self.position_manager = position_manager
        
        # Sync state
        self.active_syncs = {}
        self.sync_history = []
        
        # Configuration
        self.config = {
            'sync_interval_minutes': 5,  # How often to sync
            'backfill_days': 30,  # How far back to sync on startup
            'batch_size': 1000,  # Trades to fetch per API call
            'rate_limit_delay': 0.1,  # Delay between API calls (seconds)
            'historical_limit_days': 365,  # Max days to look back for historical data
            'symbols_to_sync': 'auto'  # 'auto' or list of symbols
        }
        
        # Exchange capabilities and limits
        self.exchange_limits = {
            'binance': {'max_days_history': 1000, 'rate_limit': 0.1},
            'bybit': {'max_days_history': 730, 'rate_limit': 0.1},
            'hyperliquid': {'max_days_history': 365, 'rate_limit': 0.1},
            'mexc': {'max_days_history': 180, 'rate_limit': 0.2},
            'gateio': {'max_days_history': 365, 'rate_limit': 0.2},
            'bitget': {'max_days_history': 365, 'rate_limit': 0.2}
        }
        
        self.logger = logger.bind(component="TradeSync")
        self.logger.info("Trade sync system initialized")
        
    async def sync_exchange_incremental(
        self,
        exchange: str,
        symbol: Optional[str] = None
    ) -> List[SyncResult]:
        """
        Perform incremental sync for an exchange (fetch only new trades).
        
        Args:
            exchange: Exchange name
            symbol: Specific symbol (None for all symbols)
            
        Returns:
            List of sync results
        """
        if exchange not in self.exchange_connectors:
            raise ValueError(f"Exchange {exchange} not available")
            
        results = []
        
        # Get symbols to sync
        symbols = await self._get_symbols_to_sync(exchange, symbol)
        
        for sym in symbols:
            try:
                result = await self._sync_symbol_incremental(exchange, sym)
                results.append(result)
                
                # Rate limiting
                await asyncio.sleep(self.config['rate_limit_delay'])
                
            except Exception as e:
                self.logger.error(f"Failed to sync {sym} on {exchange}: {e}")
                error_result = SyncResult(
                    id=str(uuid.uuid4()),
                    exchange=exchange,
                    symbol=sym,
                    start_time=datetime.utcnow(),
                    end_time=datetime.utcnow(),
                    status=SyncStatus.FAILED,
                    error_message=str(e)
                )
                results.append(error_result)
        
        return results
    
    async def sync_exchange_historical(
        self,
        exchange: str,
        symbol: Optional[str] = None,
        days_back: Optional[int] = None
    ) -> List[SyncResult]:
        """
        Perform historical sync for an exchange (backfill from earliest date).
        
        Args:
            exchange: Exchange name
            symbol: Specific symbol (None for all symbols)
            days_back: Days to look back (None for max possible)
            
        Returns:
            List of sync results
        """
        if exchange not in self.exchange_connectors:
            raise ValueError(f"Exchange {exchange} not available")
            
        # Determine how far back to go
        if days_back is None:
            days_back = min(
                self.config['historical_limit_days'],
                self.exchange_limits.get(exchange, {}).get('max_days_history', 365)
            )
            
        start_time = datetime.utcnow() - timedelta(days=days_back)
        end_time = datetime.utcnow()
        
        self.logger.info(f"Starting historical sync for {exchange}",
                        days_back=days_back, start_time=start_time)
        
        results = []
        symbols = await self._get_symbols_to_sync(exchange, symbol)
        
        for sym in symbols:
            try:
                result = await self._sync_symbol_historical(exchange, sym, start_time, end_time)
                results.append(result)
                
                # Rate limiting
                await asyncio.sleep(self.config['rate_limit_delay'])
                
            except Exception as e:
                self.logger.error(f"Failed historical sync for {sym} on {exchange}: {e}")
                error_result = SyncResult(
                    id=str(uuid.uuid4()),
                    exchange=exchange,
                    symbol=sym,
                    start_time=start_time,
                    end_time=end_time,
                    status=SyncStatus.FAILED,
                    error_message=str(e)
                )
                results.append(error_result)
        
        return results
    
    async def sync_all_exchanges_incremental(
        self,
        symbol: Optional[str] = None
    ) -> Dict[str, List[SyncResult]]:
        """
        Perform incremental sync across all exchanges.
        
        Args:
            symbol: Specific symbol to sync
            
        Returns:
            Dictionary mapping exchange names to sync results
        """
        all_results = {}
        
        # Process all exchanges in parallel
        tasks = []
        for exchange in self.exchange_connectors:
            task = asyncio.create_task(
                self.sync_exchange_incremental(exchange, symbol)
            )
            tasks.append((exchange, task))
            
        # Collect results
        for exchange, task in tasks:
            try:
                results = await task
                all_results[exchange] = results
            except Exception as e:
                self.logger.error(f"Failed to sync {exchange}: {e}")
                all_results[exchange] = []
        
        return all_results
    
    async def sync_all_exchanges_historical(
        self,
        symbol: Optional[str] = None,
        days_back: Optional[int] = None
    ) -> Dict[str, List[SyncResult]]:
        """
        Perform historical sync across all exchanges.
        
        Args:
            symbol: Specific symbol to sync
            days_back: Days to look back
            
        Returns:
            Dictionary mapping exchange names to sync results
        """
        all_results = {}
        
        # Process exchanges sequentially to avoid overwhelming APIs
        for exchange in self.exchange_connectors:
            try:
                results = await self.sync_exchange_historical(exchange, symbol, days_back)
                all_results[exchange] = results
                
                # Longer delay between exchanges for historical sync
                await asyncio.sleep(1.0)
                
            except Exception as e:
                self.logger.error(f"Failed historical sync for {exchange}: {e}")
                all_results[exchange] = []
        
        return all_results
    
    async def _sync_symbol_incremental(self, exchange: str, symbol: str) -> SyncResult:
        """Sync a specific symbol incrementally."""
        start_time = datetime.utcnow()
        
        # Get last sync time for this exchange/symbol
        exchange_id = await self._get_exchange_id(exchange)
        last_sync = await self.trade_repository.get_last_trade_sync(exchange_id, symbol)
        
        if last_sync:
            since = last_sync.last_sync_time
        else:
            # First sync - go back a reasonable amount
            since = datetime.utcnow() - timedelta(days=self.config['backfill_days'])
        
        now = datetime.utcnow()
        
        result = SyncResult(
            id=str(uuid.uuid4()),
            exchange=exchange,
            symbol=symbol,
            start_time=since,
            end_time=now,
            status=SyncStatus.RUNNING
        )
        
        self.active_syncs[result.id] = result
        
        try:
            # Fetch trades from exchange
            connector = self.exchange_connectors[exchange]
            trades = await connector.get_trade_history(
                symbol=symbol,
                since=since,
                limit=self.config['batch_size']
            )
            
            result.trades_fetched = len(trades)
            
            # Save new trades to database
            saved_count = 0
            skipped_count = 0
            last_trade_time = None
            
            for trade in trades:
                try:
                    # Check if trade already exists
                    existing = await self.trade_repository.get_trade_by_exchange_id(
                        exchange_id, trade.get('id', '')
                    )
                    
                    if existing:
                        skipped_count += 1
                        continue
                    
                    # Save new trade
                    saved_trade = await self.trade_repository.save_trade(trade, exchange_id)
                    if saved_trade:
                        saved_count += 1
                        
                        # Update position if position manager available
                        if self.position_manager:
                            await self.position_manager.update_from_trade(exchange, trade)
                        
                        # Track latest trade time
                        trade_time = self._parse_trade_timestamp(trade)
                        if last_trade_time is None or trade_time > last_trade_time:
                            last_trade_time = trade_time
                            
                except Exception as e:
                    self.logger.warning(f"Failed to save trade {trade.get('id', 'unknown')}: {e}")
                    continue
            
            result.trades_saved = saved_count
            result.trades_skipped = skipped_count
            result.last_trade_time = last_trade_time
            result.status = SyncStatus.COMPLETED
            
            # Update sync state
            if last_trade_time:
                await self.trade_repository.update_trade_sync(
                    exchange_id, symbol, last_trade_time
                )
            else:
                await self.trade_repository.update_trade_sync(
                    exchange_id, symbol, now
                )
            
            self.logger.info(f"Incremental sync completed for {exchange}/{symbol}",
                           fetched=result.trades_fetched,
                           saved=result.trades_saved,
                           skipped=result.trades_skipped)
            
        except Exception as e:
            result.status = SyncStatus.FAILED
            result.error_message = str(e)
            self.logger.error(f"Incremental sync failed for {exchange}/{symbol}: {e}")
            
        finally:
            # Update timing and cleanup
            end_time = datetime.utcnow()
            result.processing_time_seconds = (end_time - start_time).total_seconds()
            
            # Move to history
            self.sync_history.append(result)
            if result.id in self.active_syncs:
                del self.active_syncs[result.id]
        
        return result
    
    async def _sync_symbol_historical(
        self, 
        exchange: str, 
        symbol: str, 
        start_time: datetime, 
        end_time: datetime
    ) -> SyncResult:
        """Sync a specific symbol for a historical time range."""
        sync_start = datetime.utcnow()
        
        result = SyncResult(
            id=str(uuid.uuid4()),
            exchange=exchange,
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            status=SyncStatus.RUNNING
        )
        
        self.active_syncs[result.id] = result
        
        try:
            connector = self.exchange_connectors[exchange]
            exchange_id = await self._get_exchange_id(exchange)
            
            # Fetch trades in chunks if time range is large
            current_time = start_time
            total_saved = 0
            total_fetched = 0
            total_skipped = 0
            last_trade_time = None
            
            while current_time < end_time:
                # Fetch batch
                chunk_end = min(current_time + timedelta(days=7), end_time)  # 7-day chunks
                
                trades = await connector.get_trade_history(
                    symbol=symbol,
                    since=current_time,
                    limit=self.config['batch_size']
                )
                
                # Filter trades to chunk timeframe
                chunk_trades = [
                    trade for trade in trades
                    if current_time <= self._parse_trade_timestamp(trade) <= chunk_end
                ]
                
                total_fetched += len(chunk_trades)
                
                # Save trades
                for trade in chunk_trades:
                    try:
                        # Check if exists
                        existing = await self.trade_repository.get_trade_by_exchange_id(
                            exchange_id, trade.get('id', '')
                        )
                        
                        if existing:
                            total_skipped += 1
                            continue
                        
                        # Save new trade
                        saved_trade = await self.trade_repository.save_trade(trade, exchange_id)
                        if saved_trade:
                            total_saved += 1
                            
                            # Update position
                            if self.position_manager:
                                await self.position_manager.update_from_trade(exchange, trade)
                            
                            # Track latest time
                            trade_time = self._parse_trade_timestamp(trade)
                            if last_trade_time is None or trade_time > last_trade_time:
                                last_trade_time = trade_time
                                
                    except Exception as e:
                        self.logger.warning(f"Failed to save historical trade: {e}")
                        continue
                
                # Move to next chunk
                current_time = chunk_end
                
                # Rate limiting between chunks
                await asyncio.sleep(self.config['rate_limit_delay'] * 2)
            
            result.trades_fetched = total_fetched
            result.trades_saved = total_saved
            result.trades_skipped = total_skipped
            result.last_trade_time = last_trade_time
            result.status = SyncStatus.COMPLETED
            
            # Update sync state
            await self.trade_repository.update_trade_sync(
                exchange_id, symbol, last_trade_time or end_time
            )
            
            self.logger.info(f"Historical sync completed for {exchange}/{symbol}",
                           fetched=total_fetched,
                           saved=total_saved,
                           skipped=total_skipped,
                           time_range=f"{start_time} to {end_time}")
            
        except Exception as e:
            result.status = SyncStatus.FAILED
            result.error_message = str(e)
            self.logger.error(f"Historical sync failed for {exchange}/{symbol}: {e}")
            
        finally:
            # Update timing and cleanup
            sync_end = datetime.utcnow()
            result.processing_time_seconds = (sync_end - sync_start).total_seconds()
            
            # Move to history
            self.sync_history.append(result)
            if result.id in self.active_syncs:
                del self.active_syncs[result.id]
        
        return result
    
    async def start_continuous_sync(self) -> None:
        """Start continuous incremental sync across all exchanges."""
        self.logger.info("Starting continuous trade sync")
        
        while True:
            try:
                # Run incremental sync
                results = await self.sync_all_exchanges_incremental()
                
                # Log summary
                total_saved = sum(
                    sum(r.trades_saved for r in exchange_results)
                    for exchange_results in results.values()
                )
                
                if total_saved > 0:
                    self.logger.info(f"Continuous sync saved {total_saved} new trades")
                
                # Wait for next sync
                await asyncio.sleep(self.config['sync_interval_minutes'] * 60)
                
            except Exception as e:
                self.logger.error(f"Error in continuous sync: {e}")
                await asyncio.sleep(60)  # Wait 1 minute before retrying
    
    # Helper methods
    async def _get_symbols_to_sync(self, exchange: str, symbol: Optional[str]) -> List[str]:
        """Get list of symbols to sync for an exchange."""
        if symbol:
            return [symbol]
        
        if self.config['symbols_to_sync'] == 'auto':
            # Get active symbols from database or exchange
            return await self._get_active_symbols(exchange)
        else:
            return self.config['symbols_to_sync']
    
    async def _get_active_symbols(self, exchange: str) -> List[str]:
        """Get list of actively traded symbols for an exchange."""
        # This could query database for symbols with recent activity
        # or use exchange API to get active markets
        # For now, return common trading pairs
        return ['BTC/USDT', 'ETH/USDT', 'BTC/USDC:USDC']
    
    async def _get_exchange_id(self, exchange: str) -> int:
        """Get exchange ID from database."""
        exchange_map = {
            'binance_spot': 1, 'binance_perp': 1,
            'bybit_spot': 2, 'bybit_perp': 2,
            'hyperliquid_perp': 3,
            'mexc_spot': 4,
            'gateio_spot': 5,
            'bitget_spot': 6
        }
        return exchange_map.get(exchange, 0)
    
    def _parse_trade_timestamp(self, trade: Dict[str, Any]) -> datetime:
        """Parse trade timestamp to datetime."""
        timestamp = trade.get('timestamp', trade.get('datetime'))
        if isinstance(timestamp, int):
            return datetime.fromtimestamp(timestamp / 1000)
        elif isinstance(timestamp, str):
            return datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        return datetime.utcnow()
    
    # Public API methods
    def get_sync_summary(self) -> Dict[str, Any]:
        """Get overall sync system summary."""
        return {
            'active_syncs': len(self.active_syncs),
            'total_syncs': len(self.sync_history),
            'supported_exchanges': list(self.exchange_connectors.keys()),
            'config': self.config,
            'exchange_limits': self.exchange_limits
        }
    
    async def get_sync_status(self, sync_id: str) -> Optional[SyncResult]:
        """Get status of a specific sync operation."""
        if sync_id in self.active_syncs:
            return self.active_syncs[sync_id]
        
        for result in self.sync_history:
            if result.id == sync_id:
                return result
        
        return None
    
    async def get_recent_sync_results(
        self, 
        exchange: Optional[str] = None,
        hours: int = 24
    ) -> List[SyncResult]:
        """Get recent sync results."""
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)
        results = []
        
        for result in self.sync_history:
            if result.start_time >= cutoff_time:
                if exchange is None or result.exchange == exchange:
                    results.append(result)
        
        return results


# Convenience functions for easy startup
async def perform_startup_sync(
    exchange_connectors: Dict[str, Any],
    trade_repository: TradeRepository,
    position_manager: Optional[PositionManager] = None,
    days_back: int = 30
) -> Dict[str, List[SyncResult]]:
    """
    Perform initial startup sync across all exchanges.
    
    This should be called when your trading system starts up to ensure
    all recent trades are captured in the database.
    
    Args:
        exchange_connectors: Dictionary of exchange connectors
        trade_repository: Trade repository instance
        position_manager: Position manager instance
        days_back: Days to look back for historical sync
        
    Returns:
        Dictionary mapping exchange names to sync results
    """
    trade_sync = TradeSync(exchange_connectors, trade_repository, position_manager)
    
    logger.info(f"Performing startup sync for all exchanges ({days_back} days back)")
    
    results = await trade_sync.sync_all_exchanges_historical(days_back=days_back)
    
    total_saved = sum(
        sum(r.trades_saved for r in exchange_results)
        for exchange_results in results.values()
    )
    
    logger.info(f"Startup sync completed - saved {total_saved} trades total")
    
    return results


async def start_periodic_sync(
    exchange_connectors: Dict[str, Any],
    trade_repository: TradeRepository,
    position_manager: Optional[PositionManager] = None,
    interval_minutes: int = 5
) -> None:
    """
    Start periodic incremental sync that runs continuously.
    
    This should be run as a background task to keep trades synchronized.
    
    Args:
        exchange_connectors: Dictionary of exchange connectors
        trade_repository: Trade repository instance
        position_manager: Position manager instance
        interval_minutes: Minutes between sync cycles
    """
    trade_sync = TradeSync(exchange_connectors, trade_repository, position_manager)
    trade_sync.config['sync_interval_minutes'] = interval_minutes
    
    logger.info(f"Starting periodic sync every {interval_minutes} minutes")
    
    await trade_sync.start_continuous_sync()


async def sync_exchange_trades(
    exchange_name: str,
    connector: Any,
    position_manager: Optional[PositionManager] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    symbols: List[str] = None
) -> Dict[str, Any]:
    """
    Sync trades for a specific exchange within a time range.
    
    This is a convenience function for the API to trigger trade sync
    without needing to create a full TradeSync instance.
    
    Args:
        exchange_name: Name of the exchange
        connector: Exchange connector instance
        position_manager: Position manager instance
        start_time: Start time for sync
        end_time: End time for sync
        symbols: List of symbols to sync (None for default symbols)
        
    Returns:
        Dictionary with sync results
    """
    from database.repositories import TradeRepository
    from database import get_session
    
    # Get a database session
    async for session in get_session():
        trade_repository = TradeRepository(session)
        break
    
    # Create TradeSync instance
    trade_sync = TradeSync(
        exchange_connectors={exchange_name: connector},
        trade_repository=trade_repository,
        position_manager=position_manager
    )
    
    # If no time range specified, do incremental sync
    if start_time is None or end_time is None:
        results = await trade_sync.sync_exchange_incremental(exchange_name)
    else:
        # Do historical sync for the time range
        # Note: The existing historical sync doesn't support custom time ranges
        # So we'll do a manual sync for now
        results = []
        
        # Get symbols to sync
        if symbols is None:
            symbols = ['BTC/USDT', 'ETH/USDT']  # Default symbols
        
        for symbol in symbols:
            try:
                # Fetch trades from exchange for this time range
                trades = await connector.get_trade_history(
                    symbol=symbol,
                    since=start_time,
                    limit=1000  # Reasonable limit
                )
                
                # Filter trades to the exact time range
                filtered_trades = []
                for trade in trades:
                    trade_time = trade_sync._parse_trade_timestamp(trade)
                    if start_time <= trade_time <= end_time:
                        filtered_trades.append(trade)
                
                # Save trades and update positions
                saved_count = 0
                exchange_id = await trade_sync._get_exchange_id(exchange_name)
                
                for trade in filtered_trades:
                    try:
                        # Check if trade exists
                        existing = await trade_repository.get_trade_by_exchange_id(
                            exchange_id, trade.get('id', '')
                        )
                        
                        if not existing:
                            # Save new trade
                            saved_trade = await trade_repository.save_trade(trade, exchange_id)
                            if saved_trade:
                                saved_count += 1
                                
                                # Update position
                                if position_manager:
                                    await position_manager.update_from_trade(exchange_name, trade)
                    except Exception as e:
                        logger.warning(f"Failed to save trade: {e}")
                        continue
                
                # Create result
                result = SyncResult(
                    id=str(uuid.uuid4()),
                    exchange=exchange_name,
                    symbol=symbol,
                    start_time=start_time,
                    end_time=end_time,
                    status=SyncStatus.COMPLETED,
                    trades_fetched=len(filtered_trades),
                    trades_saved=saved_count,
                    trades_skipped=len(filtered_trades) - saved_count
                )
                results.append(result)
                
            except Exception as e:
                logger.error(f"Error syncing {symbol} on {exchange_name}: {e}")
                result = SyncResult(
                    id=str(uuid.uuid4()),
                    exchange=exchange_name,
                    symbol=symbol,
                    start_time=start_time,
                    end_time=end_time,
                    status=SyncStatus.FAILED,
                    error_message=str(e)
                )
                results.append(result)
    
    # Calculate summary
    total_fetched = sum(r.trades_fetched for r in results)
    total_saved = sum(r.trades_saved for r in results)
    
    return {
        'exchange': exchange_name,
        'trades_count': total_saved,
        'trades_fetched': total_fetched,
        'symbols_synced': len(results),
        'results': [
            {
                'symbol': r.symbol,
                'status': r.status.value,
                'trades_fetched': r.trades_fetched,
                'trades_saved': r.trades_saved,
                'error': r.error_message
            } for r in results
        ]
    } 