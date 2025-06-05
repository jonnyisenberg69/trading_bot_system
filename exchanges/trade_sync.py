"""
Trade synchronization module.

Provides functionality for synchronizing trades from exchanges:
- Historical trade synchronization via REST API
- Real-time trade updates via WebSocket
- De-duplication of trades from different sources
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any, Callable, Set
from datetime import datetime, timedelta
import time
import structlog
from decimal import Decimal

from database.repositories.trade_repository import TradeRepository
from database.repositories.position_repository import PositionRepository
from database.models import Exchange, Trade

logger = structlog.get_logger(__name__)


class TradeSynchronizer:
    """
    Trade synchronizer for keeping trade data up to date.
    
    Features:
    - Syncs historical trades from exchanges via REST API
    - Listens for real-time trade updates via WebSocket
    - De-duplicates trades from different sources
    - Updates positions based on trade data
    """
    
    def __init__(
        self,
        exchange_connectors: Dict[str, Any],
        trade_repository: TradeRepository,
        position_repository: PositionRepository,
        exchange_id_map: Dict[str, int],
        bot_instance_id: Optional[int] = None
    ):
        """
        Initialize trade synchronizer.
        
        Args:
            exchange_connectors: Dictionary of exchange connectors
            trade_repository: Trade repository
            position_repository: Position repository
            exchange_id_map: Mapping from exchange name to exchange ID
            bot_instance_id: Bot instance ID (optional)
        """
        self.exchange_connectors = exchange_connectors
        self.trade_repository = trade_repository
        self.position_repository = position_repository
        self.exchange_id_map = exchange_id_map
        self.bot_instance_id = bot_instance_id
        
        self.ws_tasks = {}  # Exchange -> Symbol -> Task
        self.sync_tasks = {}  # Exchange -> Symbol -> Task
        self.running = False
        
        # Set of symbols being tracked per exchange
        self.tracked_symbols = {}  # Exchange -> Set of symbols
        
        # In-memory cache of processed trade IDs to avoid duplicates
        self._processed_trades = set()  # Set of (exchange, trade_id) tuples
        
        self.logger = logger.bind(component="TradeSynchronizer")
        
    async def start(self, symbols_by_exchange: Dict[str, List[str]]):
        """
        Start trade synchronization.
        
        Args:
            symbols_by_exchange: Dictionary of symbols to track by exchange
        """
        self.running = True
        self.logger.info("Starting trade synchronization")
        
        # Initialize tracked symbols
        self.tracked_symbols = {}
        for exchange, symbols in symbols_by_exchange.items():
            if exchange in self.exchange_connectors:
                self.tracked_symbols[exchange] = set(symbols)
                
        # Start historical sync and websocket listeners for each exchange and symbol
        for exchange, symbols in self.tracked_symbols.items():
            self.ws_tasks[exchange] = {}
            self.sync_tasks[exchange] = {}
            
            for symbol in symbols:
                # Start historical sync
                self.sync_tasks[exchange][symbol] = asyncio.create_task(
                    self._sync_historical_trades(exchange, symbol)
                )
                
                # Start websocket listener
                self.ws_tasks[exchange][symbol] = asyncio.create_task(
                    self._listen_for_trades(exchange, symbol)
                )
                
        # Start position processor
        self.position_processor_task = asyncio.create_task(self._process_positions())
        
        self.logger.info("Trade synchronization started")
        
    async def stop(self):
        """Stop trade synchronization."""
        self.running = False
        self.logger.info("Stopping trade synchronization")
        
        # Cancel all tasks
        for exchange in self.ws_tasks:
            for symbol, task in self.ws_tasks[exchange].items():
                if not task.done():
                    task.cancel()
                    
        for exchange in self.sync_tasks:
            for symbol, task in self.sync_tasks[exchange].items():
                if not task.done():
                    task.cancel()
                    
        if hasattr(self, 'position_processor_task') and not self.position_processor_task.done():
            self.position_processor_task.cancel()
            
        # Wait for all tasks to complete
        for exchange in self.ws_tasks:
            for symbol, task in self.ws_tasks[exchange].items():
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                    
        for exchange in self.sync_tasks:
            for symbol, task in self.sync_tasks[exchange].items():
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                    
        try:
            await self.position_processor_task
        except asyncio.CancelledError:
            pass
            
        self.logger.info("Trade synchronization stopped")
        
    async def add_symbol(self, exchange: str, symbol: str):
        """
        Add a symbol to track for an exchange.
        
        Args:
            exchange: Exchange name
            symbol: Symbol to track
        """
        if exchange not in self.exchange_connectors:
            self.logger.warning(f"Exchange {exchange} not available")
            return
            
        # Initialize tracked symbols for exchange if needed
        if exchange not in self.tracked_symbols:
            self.tracked_symbols[exchange] = set()
            self.ws_tasks[exchange] = {}
            self.sync_tasks[exchange] = {}
            
        # Skip if already tracking
        if symbol in self.tracked_symbols[exchange]:
            self.logger.info(f"Already tracking {symbol} on {exchange}")
            return
            
        # Add to tracked symbols
        self.tracked_symbols[exchange].add(symbol)
        
        # Start historical sync
        self.sync_tasks[exchange][symbol] = asyncio.create_task(
            self._sync_historical_trades(exchange, symbol)
        )
        
        # Start websocket listener
        self.ws_tasks[exchange][symbol] = asyncio.create_task(
            self._listen_for_trades(exchange, symbol)
        )
        
        self.logger.info(f"Now tracking {symbol} on {exchange}")
        
    async def remove_symbol(self, exchange: str, symbol: str):
        """
        Remove a symbol from tracking for an exchange.
        
        Args:
            exchange: Exchange name
            symbol: Symbol to stop tracking
        """
        if (exchange not in self.tracked_symbols or 
            symbol not in self.tracked_symbols[exchange]):
            return
            
        # Remove from tracked symbols
        self.tracked_symbols[exchange].remove(symbol)
        
        # Cancel tasks
        if exchange in self.ws_tasks and symbol in self.ws_tasks[exchange]:
            task = self.ws_tasks[exchange][symbol]
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            del self.ws_tasks[exchange][symbol]
            
        if exchange in self.sync_tasks and symbol in self.sync_tasks[exchange]:
            task = self.sync_tasks[exchange][symbol]
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            del self.sync_tasks[exchange][symbol]
            
        self.logger.info(f"Stopped tracking {symbol} on {exchange}")
        
    async def _sync_historical_trades(self, exchange: str, symbol: str):
        """
        Synchronize historical trades for a symbol on an exchange.
        
        Args:
            exchange: Exchange name
            symbol: Symbol to synchronize
        """
        exchange_id = self.exchange_id_map.get(exchange)
        if not exchange_id:
            self.logger.error(f"Unknown exchange ID for {exchange}")
            return
            
        self.logger.info(f"Starting historical trade sync for {symbol} on {exchange}")
        
        try:
            # Get last sync record
            last_sync = await self.trade_repository.get_last_trade_sync(exchange_id, symbol)
            
            # Default to 24 hours ago if no previous sync
            since = None
            last_trade_id = None
            
            if last_sync:
                since = last_sync.last_sync_time
                last_trade_id = last_sync.last_trade_id
                
            else:
                # If no previous sync, start from 24 hours ago
                since = datetime.utcnow() - timedelta(days=1)
                
            # While running, periodically sync historical trades
            while self.running:
                try:
                    connector = self.exchange_connectors[exchange]
                    
                    # Fetch trades since last sync
                    trades = await connector.get_trade_history(
                        symbol=symbol,
                        since=since,
                        limit=1000  # Most exchanges limit to 1000 trades per request
                    )
                    
                    if trades:
                        self.logger.info(f"Fetched {len(trades)} historical trades for {symbol} on {exchange}")
                        
                        # Process trades
                        for trade_data in trades:
                            await self._process_trade(exchange, trade_data)
                            
                        # Update last sync info
                        last_trade = trades[-1]
                        last_trade_timestamp = last_trade.get('timestamp', None)
                        
                        if last_trade_timestamp:
                            if isinstance(last_trade_timestamp, int):
                                since = datetime.fromtimestamp(last_trade_timestamp / 1000)
                            else:
                                since = datetime.utcnow()
                                
                        last_trade_id = last_trade.get('id', None)
                        
                        # Update sync record
                        await self.trade_repository.update_trade_sync(
                            exchange_id=exchange_id,
                            symbol=symbol,
                            last_sync_time=since,
                            last_trade_id=last_trade_id
                        )
                    else:
                        # No new trades, update sync time to now
                        await self.trade_repository.update_trade_sync(
                            exchange_id=exchange_id,
                            symbol=symbol,
                            last_sync_time=datetime.utcnow(),
                            last_trade_id=last_trade_id
                        )
                        
                except Exception as e:
                    self.logger.error(f"Error syncing historical trades for {symbol} on {exchange}: {e}")
                    
                # Wait before next sync (adjust based on exchange rate limits)
                await asyncio.sleep(60)  # 1 minute between syncs
                
        except asyncio.CancelledError:
            self.logger.info(f"Historical trade sync cancelled for {symbol} on {exchange}")
            raise
            
        except Exception as e:
            self.logger.error(f"Unexpected error in historical trade sync for {symbol} on {exchange}: {e}")
            
    async def _listen_for_trades(self, exchange: str, symbol: str):
        """
        Listen for real-time trade updates via WebSocket.
        
        Args:
            exchange: Exchange name
            symbol: Symbol to listen for
        """
        exchange_id = self.exchange_id_map.get(exchange)
        if not exchange_id:
            self.logger.error(f"Unknown exchange ID for {exchange}")
            return
            
        self.logger.info(f"Starting WebSocket trade listener for {symbol} on {exchange}")
        
        try:
            connector = self.exchange_connectors[exchange]
            
            # Check if exchange supports WebSocket
            if not hasattr(connector, 'watch_my_trades'):
                self.logger.warning(f"Exchange {exchange} does not support WebSocket for trades")
                return
                
            # While running, maintain WebSocket connection
            while self.running:
                try:
                    # For now, just poll for trades since WebSocket implementation 
                    # may not be available for all exchanges
                    trades = await connector.get_trade_history(
                        symbol=symbol,
                        limit=10  # Just get recent trades
                    )
                    
                    if trades:
                        for trade in trades:
                            if not self.running:
                                break
                            await self._process_trade(exchange, trade)
                        
                    # Wait before next poll
                    await asyncio.sleep(10)  # Poll every 10 seconds
                        
                except Exception as e:
                    self.logger.error(f"Trade polling error for {symbol} on {exchange}: {e}")
                    
                    # Wait before reconnecting
                    await asyncio.sleep(30)
                    
        except asyncio.CancelledError:
            self.logger.info(f"WebSocket trade listener cancelled for {symbol} on {exchange}")
            
            # Close WebSocket connection if possible
            try:
                if hasattr(connector, 'close'):
                    await connector.close()
            except:
                pass
                
            raise
            
        except Exception as e:
            self.logger.error(f"Unexpected error in WebSocket trade listener for {symbol} on {exchange}: {e}")
            
    async def _process_trade(self, exchange: str, trade_data: Dict[str, Any]):
        """
        Process a trade and save to database.
        
        Args:
            exchange: Exchange name
            trade_data: Trade data dictionary
        """
        exchange_id = self.exchange_id_map.get(exchange)
        if not exchange_id:
            self.logger.error(f"Unknown exchange ID for {exchange}")
            return
            
        trade_id = trade_data.get('id', '')
        
        # Skip if trade_id is empty or invalid
        if not trade_id:
            self.logger.warning(f"Trade missing ID: {trade_data}")
            return
            
        # Skip if already processed (in-memory cache)
        cache_key = (exchange, trade_id)
        if cache_key in self._processed_trades:
            self.logger.debug(f"Trade {trade_id} already processed (cache)")
            return
            
        # Check if trade already exists in database
        try:
            existing_trade = await self.trade_repository.get_trade_by_exchange_id(
                exchange_id=exchange_id,
                external_trade_id=trade_id
            )
            
            if existing_trade:
                self.logger.debug(f"Trade {trade_id} already exists in database")
                # Add to cache to avoid future database checks
                self._processed_trades.add(cache_key)
                return
                
        except Exception as e:
            self.logger.error(f"Error checking for existing trade {trade_id}: {e}")
            return
            
        # Save trade to database
        try:
            trade = await self.trade_repository.save_trade(
                trade_data=trade_data,
                exchange_id=exchange_id,
                bot_instance_id=self.bot_instance_id
            )
            
            if trade:
                # Add to processed cache
                self._processed_trades.add(cache_key)
                
                # Limit cache size to avoid memory issues
                if len(self._processed_trades) > 10000:
                    # Remove oldest entries (this is a simple approach, could be improved)
                    self._processed_trades = set(list(self._processed_trades)[-5000:])
                    
                self.logger.info(f"Saved new trade {trade_id} for {exchange}")
                    
        except Exception as e:
            self.logger.error(f"Error saving trade {trade_id} for {exchange}: {e}")
            
    async def _process_positions(self):
        """Process unprocessed trades to update positions."""
        try:
            while self.running:
                try:
                    # Get unprocessed trades
                    trades = await self.trade_repository.get_unprocessed_trades(limit=100)
                    
                    if not trades:
                        # No trades to process, wait before checking again
                        await asyncio.sleep(5)
                        continue
                        
                    self.logger.info(f"Processing {len(trades)} unprocessed trades for positions")
                    
                    # Group trades by exchange and symbol
                    trades_by_exchange_symbol = {}
                    for trade in trades:
                        key = (trade.exchange_id, trade.symbol)
                        if key not in trades_by_exchange_symbol:
                            trades_by_exchange_symbol[key] = []
                        trades_by_exchange_symbol[key].append(trade)
                        
                    # Process trades for each exchange and symbol
                    for (exchange_id, symbol), symbol_trades in trades_by_exchange_symbol.items():
                        for trade in symbol_trades:
                            # Calculate position deltas
                            p1_delta = trade.p1_delta
                            p2_delta = trade.p2_delta
                            
                            # Fee deltas
                            p1_fee_delta = 0
                            p2_fee_delta = 0
                            
                            base_currency, quote_currency = symbol.split('/')
                            
                            if trade.fee_currency == base_currency:
                                p1_fee_delta = trade.fee_cost
                            elif trade.fee_currency == quote_currency:
                                p2_fee_delta = trade.fee_cost
                                
                            # Update position
                            await self.position_repository.update_position(
                                exchange_id=exchange_id,
                                symbol=symbol,
                                p1_delta=p1_delta,
                                p2_delta=p2_delta,
                                p1_fee_delta=p1_fee_delta,
                                p2_fee_delta=p2_fee_delta,
                                bot_instance_id=self.bot_instance_id
                            )
                            
                    # Mark trades as processed
                    trade_ids = [trade.id for trade in trades]
                    await self.trade_repository.mark_trades_as_processed(trade_ids)
                    
                except Exception as e:
                    self.logger.error(f"Error processing positions: {e}")
                    
                # Sleep to avoid high CPU usage
                await asyncio.sleep(1)
                
        except asyncio.CancelledError:
            self.logger.info("Position processor cancelled")
            raise
            
        except Exception as e:
            self.logger.error(f"Unexpected error in position processor: {e}")
            
    def get_tracked_symbols(self, exchange: Optional[str] = None) -> Dict[str, Set[str]]:
        """
        Get currently tracked symbols.
        
        Args:
            exchange: Optional exchange to filter by
            
        Returns:
            Dictionary of exchange -> set of symbols
        """
        if exchange:
            return {exchange: self.tracked_symbols.get(exchange, set())}
        return self.tracked_symbols 