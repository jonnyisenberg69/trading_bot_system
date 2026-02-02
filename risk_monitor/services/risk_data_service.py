import asyncio
import logging
import json
import time
from typing import Dict, Any, Union
from datetime import datetime, timezone

import ccxt.pro as ccxtpro
from utils.symbol_utils import normalize_symbol

logger = logging.getLogger(__name__)


class RiskDataCollectorService:
    """
    Manages WebSocket connections to exchanges using ccxtpro to monitor
    trades, balances, and positions.
    """

    def __init__(self, exchange_configs: Dict[str, Dict[str, Any]], start_timestamp_ms: int = None,
                 realized_pnl_repo=None, balance_history_repo=None, inventory_price_history_repo=None, exchange_id_map=None, trading_symbols=None):
        self.exchange_configs = exchange_configs
        self.start_timestamp_ms = start_timestamp_ms
        self.trading_symbols = trading_symbols or []  # Accept trading symbols from caller
        self._exchanges: Dict[str, ccxtpro.Exchange] = {}
        self._tasks: Dict[str, asyncio.Task] = {}
        self.is_running = False

        # State management
        self._positions: Dict[str, Dict[str, Any]] = {}
        self._ticker_tasks: Dict[str, asyncio.Task] = {}
        self._processed_trade_ids: set = set()
        self._local_balances: Dict[str, Dict[str, Any]] = {}

        # Persistence repositories
        self.realized_pnl_repo = realized_pnl_repo
        self.balance_history_repo = balance_history_repo
        self.inventory_price_history_repo = inventory_price_history_repo

        self.exchange_id_map = exchange_id_map or {}

        # Timeout for loading markets (seconds)
        self.MARKET_LOAD_TIMEOUT = 30

    def _ensure_naive_datetime(self, dt):
        """Convert timezone-aware datetime to naive UTC datetime for database operations."""
        if dt is None:
            return None
        if dt.tzinfo is not None:
            return dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt

    async def _initialize_exchanges(self):
        """Initializes one ccxtpro exchange instance per base exchange name."""
        logger.info("Initializing ccxtpro exchange instances...")

        async def _initialize_single_exchange(base_name, config_group):
            """Helper function to initialize one exchange instance."""
            try:
                exchange_class = getattr(ccxtpro, base_name)
                # The 'creds' dictionary now holds all authentication and config options
                exchange = exchange_class(config_group['creds'])
                self._exchanges[base_name] = exchange
                logger.info(f"Initialized {base_name} exchange instance successfully.")
            except Exception as e:
                logger.error(f"Failed to initialize {base_name}: {e}", exc_info=True)

        tasks = [_initialize_single_exchange(name, config) for name, config in self.exchange_configs.items()]
        await asyncio.gather(*tasks)

    async def start(self):
        if self.is_running: return
        await self._initialize_exchanges()
        if not self._exchanges:
            logger.error("No exchanges initialized. Exiting.")
            return

        self.is_running = True
        logger.info("Loading initial state for all accounts...")
        await self._load_initial_state()

        logger.info("Starting main watcher tasks for all exchanges.")
        for base_name, exchange in self._exchanges.items():
            config_group = self.exchange_configs[base_name]
            task = asyncio.create_task(self._run_exchange_watchers(base_name, exchange, config_group))
            self._tasks[base_name] = task

    async def stop(self):
        if not self.is_running: return
        self.is_running = False
        all_tasks = list(self._tasks.values()) + list(self._ticker_tasks.values())
        for task in all_tasks:
            task.cancel()
        await asyncio.gather(*all_tasks, return_exceptions=True)
        close_tasks = [ex.close() for ex in self._exchanges.values()]
        await asyncio.gather(*close_tasks, return_exceptions=True)
        logger.info("Service stopped.")

    async def _run_exchange_watchers(self, base_name: str, exchange: ccxtpro.Exchange, config_group: Dict):
        """Runs concurrent watchers for trades and balances for a single exchange instance."""
        watchers = []
        markets_to_watch = config_group.get('markets', {})

        # --- Trade Watchers (one per market type) ---
        for market_type, unique_name in markets_to_watch.items():
            watchers.append(self._watch_trades_loop(unique_name, exchange, market_type))

        # --- Balance Watchers ---
        if exchange.has.get('watchBalance'):
            # For exchanges like Bybit, we need separate watchers per category
            if base_name == 'bybit':
                for market_type, unique_name in markets_to_watch.items():
                    watchers.append(self._watch_balance_loop(unique_name, exchange, market_type))
            # For exchanges like Binance, one watcher gives all balances
            else:
                watchers.append(self._watch_balance_loop(base_name, exchange, markets_to_watch))

        # Polling for Hyperliquid is special and managed separately if needed
        if base_name == 'hyperliquid':
             # The unique name for hyperliquid is 'hyperliquid_perp'
             watchers.append(self._watch_balance_polling_loop('hyperliquid_perp', exchange, interval=10))

        if watchers:
            await asyncio.gather(*watchers)
        else:
            logger.warning(f"No watchers configured for {base_name}. It may not be monitored.")

    async def _load_initial_state(self):
        if self.start_timestamp_ms:
            await self._fetch_historical_trades()
            await self._fetch_funding_history()
        
        # This loop needs to iterate over the unique market names from the config
        for base_name, config_group in self.exchange_configs.items():
            exchange = self._exchanges[base_name]
            for market_type, unique_name in config_group['markets'].items():
                try:
                    logger.info(f"Fetching initial balance for {unique_name}...")
                    params = {}
                    # For hyperliquid, we still need to pass the user param
                    if 'hyperliquid' in unique_name:
                        params['user'] = exchange.apiKey

                    # For other exchanges, fetchBalance might need a type hint
                    if base_name in ['bybit', 'binance']:
                        params['type'] = market_type

                    balance = await exchange.fetch_balance(params=params)
                    await self.handle_balance(unique_name, balance, market_type, is_initial=True)
                except Exception as e:
                    logger.error(f"Could not fetch initial balance for {unique_name}: {e}")

            # NOTE: Removed fetch_positions logic.
            # Positions are now built exclusively from historical trades via fetchMyTrades
            # or from the live feed if no start time is provided. This avoids ambiguity
            # from context-less position snapshots.
            # market_type = exchange.options.get('defaultType')
            # if market_type == 'spot': continue
            # if not exchange.has.get('fetchPositions'): continue
            
            # try:
            #     params = {}
            #     if name == 'hyperliquid_perp': params['user'] = exchange.apiKey
            #     positions = await exchange.fetch_positions(params=params)
            #     for pos in positions:
            #         if not pos.get('symbol'): continue
            #         self._bootstrap_position_from_fetch(name, pos)
            # except Exception as e:
            #     logger.error(f"Could not fetch initial positions for {name}: {e}")

    def _bootstrap_position_from_fetch(self, exchange_name: str, position_data: dict):
        symbol = position_data.get('symbol')
        pos_key = f"{exchange_name}_{symbol}"

        amount = float(position_data.get('contracts', position_data.get('amount', 0)))
        if position_data.get('side') == 'short': amount *= -1
        
        entry_price = float(position_data.get('entryPrice', 0))
        total_cost = amount * entry_price

        self._positions[pos_key] = {'amount': amount, 'total_cost': total_cost, 'p1_fee': 0.0, 'p2_fee': 0.0}
        logger.info(f"  > Loaded position for {symbol}: {amount}")
        
        if abs(amount) > 1e-9:
            task = asyncio.create_task(self._watch_ticker_loop(exchange_name, symbol))
            self._ticker_tasks[pos_key] = task

    async def _fetch_funding_history(self):
        if not self.start_timestamp_ms: return
        logger.info(f"Fetching funding history since {self.start_timestamp_ms}...")
        
        async def fetch_for_exchange(base_name, config_group):
            exchange = self._exchanges.get(base_name)
            if not exchange:
                return
                
            # Check if the exchange instance supports fetchFundingHistory
            if not exchange.has.get('fetchFundingHistory'):
                logger.info(f"Skipping funding history for {base_name}: method not supported.")
                return

            # Check if any of the markets for this exchange are derivatives (not spot)
            has_derivatives = False
            for market_type in config_group.get('markets', {}).keys():
                if market_type not in ['spot']:
                    has_derivatives = True
                    break
                    
            if not has_derivatives:
                logger.info(f"Skipping funding history for {base_name}: only spot markets configured.")
                return
                
            try:
                logger.info(f"Fetching funding history for {base_name}...")
                params = {}
                # Add user parameter for hyperliquid
                if base_name == 'hyperliquid':
                    params['user'] = exchange.apiKey
                    
                # For Gate.io, ensure we're only fetching for swap/future markets
                if base_name == 'gateio':
                    # Gate.io requires specific market type in options
                    exchange.options['defaultType'] = 'swap'
                    
                funding_history = await asyncio.wait_for(
                    exchange.fetch_funding_history(since=self.start_timestamp_ms, params=params),
                    timeout=30
                )
                for payment in funding_history:
                    self.handle_funding_payment(base_name, payment)
            except asyncio.TimeoutError:
                logger.error(f"Timeout while fetching funding history for {base_name}.")
            except Exception as e:
                logger.error(f"Could not fetch funding history for {base_name}: {e}")

        tasks = [fetch_for_exchange(base_name, config_group) for base_name, config_group in self.exchange_configs.items()]
        await asyncio.gather(*tasks)

    async def _fetch_historical_trades(self):
        logger.info(f"Fetching historical trades since {self.start_timestamp_ms}...")

        # This now iterates over grouped configs
        tasks = []
        for base_name, config_group in self.exchange_configs.items():
            exchange = self._exchanges[base_name]
            for market_type, unique_name in config_group['markets'].items():
                 tasks.append(self._fetch_trades_for_market(unique_name, exchange, market_type))
        await asyncio.gather(*tasks)

    async def _fetch_trades_for_market(self, unique_name: str, exchange: ccxtpro.Exchange, market_type: str):
        """Fetches historical trades for a specific market on an exchange."""
        if not exchange.has.get('fetchMyTrades'):
            logger.info(f"Skipping {unique_name}: fetchMyTrades not supported.")
            return

        try:
            # Set options for this specific fetch if needed, e.g., for Binance
            if exchange.id == 'binance':
                exchange.options['defaultType'] = market_type

            markets = await asyncio.wait_for(exchange.load_markets(True), timeout=self.MARKET_LOAD_TIMEOUT)
            
            # If we have specific trading symbols, filter for those
            if self.trading_symbols:
                relevant_symbols = []
                for symbol in markets:
                    # Check if this market symbol matches any of our trading symbols
                    # Handle both exact matches and base currency matches
                    for trading_symbol in self.trading_symbols:
                        if (symbol == trading_symbol or 
                            symbol.startswith(trading_symbol.split('/')[0] + '/') or
                            trading_symbol in symbol):
                            if markets[symbol].get(market_type):
                                relevant_symbols.append(symbol)
                                break
            else:
                # If no specific symbols configured, fetch trades for all symbols
                # This will get all historical trades for the account
                relevant_symbols = [s for s in markets if markets[s].get(market_type)]
            
            if not relevant_symbols:
                logger.info(f"No relevant '{market_type}' symbols for {unique_name}.")
                return

            tasks = [self._fetch_trades_for_symbol(exchange, symbol) for symbol in relevant_symbols]
            results = await asyncio.gather(*tasks)
            all_trades_for_market = [trade for sublist in results for trade in sublist]

            if all_trades_for_market:
                logger.info(f"Processing {len(all_trades_for_market)} historical trades for {unique_name}...")
                all_trades_for_market.sort(key=lambda t: t['timestamp'])
                await self.handle_trades(unique_name, all_trades_for_market, is_historical=True)
        except asyncio.TimeoutError:
            logger.error(f"Timeout while fetching historical trades for {unique_name}. Skipping this market.")
        except Exception as e:
            logger.error(f"An error occurred during historical trade fetching for {unique_name}: {e}", exc_info=True)

    async def _fetch_trades_for_symbol(self, exchange, symbol):
        trades_for_symbol = []
        since = self.start_timestamp_ms
        limit = 100
        params = {}
        if exchange.id == 'hyperliquid': params['user'] = exchange.apiKey
        
        try:
            while True:
                trades = await exchange.fetch_my_trades(symbol=symbol, since=since, limit=limit, params=params)
                if not trades: break
                trades_for_symbol.extend(trades)
                since = trades[-1]['timestamp'] + 1
                if len(trades) < limit: break
        except Exception as e:
            logger.debug(f"Could not fetch trades for symbol {symbol} on {exchange.id}. Info: {e}")
        return trades_for_symbol
            
    async def _watch_trades_loop(self, unique_name: str, exchange: ccxtpro.Exchange, market_type: str):
        params = {}
        if exchange.id == 'bybit':
            params['category'] = market_type
        # Add user parameter for hyperliquid
        if exchange.id == 'hyperliquid':
            params['user'] = exchange.apiKey

        while self.is_running:
            try:
                # We may need to set options here too for binance to get the right stream
                if exchange.id == 'binance':
                    exchange.options['defaultType'] = market_type
                trades = await exchange.watch_my_trades(params=params)
                await self.handle_trades(unique_name, trades)
            except Exception as e:
                if not isinstance(e, asyncio.CancelledError): logger.error(f"Error in {unique_name} trade watcher: {e}", exc_info=True); await asyncio.sleep(5)
                else: break

    async def _watch_balance_loop(self, name: str, exchange: ccxtpro.Exchange, market_info: Union[str, Dict]):
        params = {}
        if exchange.id == 'bybit':
            params['category'] = market_info # This will be market_type string
        
        while self.is_running:
            try:
                balance = await exchange.watch_balance(params=params)
                await self.handle_balance(name, balance, market_info)
            except Exception as e:
                if not isinstance(e, asyncio.CancelledError): logger.error(f"Error in {name} balance watcher: {e}", exc_info=True); await asyncio.sleep(5)
                else: break

    async def _watch_balance_polling_loop(self, name: str, exchange: ccxtpro.Exchange, interval: int):
        """Periodically fetches balance via REST for exchanges without websocket support."""
        while self.is_running:
            try:
                params = {}
                if name == 'hyperliquid_perp':
                    params['user'] = exchange.apiKey
                balance = await exchange.fetch_balance(params=params)
                # Pass the unique name as market_info for the polling loop
                await self.handle_balance(name, balance, name)
            except Exception as e:
                if not isinstance(e, asyncio.CancelledError):
                    logger.error(f"Error in balance polling loop for {name}: {e}")
            
            await asyncio.sleep(interval)

    async def _watch_ticker_loop(self, exchange_name: str, symbol: str):
        exchange = self._exchanges.get(exchange_name)
        if not exchange: return
        
        # Don't normalize the symbol here - it should already be normalized by the caller
        # Just use the symbol as provided
        
        logger.info(f"Starting ticker watcher for {symbol} on {exchange_name}.")
        while self.is_running:
            try:
                ticker = await exchange.watch_ticker(symbol)
                await self.handle_price_update(exchange_name, ticker)
            except Exception as e:
                if not isinstance(e, asyncio.CancelledError): logger.error(f"Error in ticker watcher for {symbol} on {exchange_name}: {e}", exc_info=True)
                break
        logger.info(f"Stopping ticker watcher for {symbol} on {exchange_name}.")
              
    async def handle_trades(self, exchange_name: str, trades: list, is_historical: bool = False):
        for trade in trades:
            if not is_historical and self.start_timestamp_ms and trade.get('timestamp') < self.start_timestamp_ms: continue
            unique_trade_key = (exchange_name, trade.get('id'))
            if unique_trade_key in self._processed_trade_ids: continue
            self._processed_trade_ids.add(unique_trade_key)
            
            symbol = trade.get('symbol')
            if not symbol: continue
            
            # --- Symbol Normalization ---
            # Use the dedicated utility to normalize the symbol.
            symbol = normalize_symbol(symbol)
            
            pos_key = f"{exchange_name}_{symbol}"
            position = self._positions.get(pos_key, {
                'amount': 0.0, 
                'total_cost': 0.0,
                'p1_fee': 0.0,  # Base currency fee
                'p2_fee': 0.0   # Quote currency fee
            })
            
            # --- State before the trade ---
            old_amount = position.get('amount', 0.0)
            old_total_cost = position.get('total_cost', 0.0)
            
            trade_amount = float(trade['amount'])
            trade_price = float(trade['price'])
            trade_cost = trade.get('cost', trade_amount * trade_price)
            side_multiplier = 1 if trade['side'] == 'buy' else -1

            # --- Update position state ---
            position['amount'] += (trade_amount * side_multiplier)
            position['total_cost'] += (trade_cost * side_multiplier)

            # --- Handle fees ---
            fee_info = trade.get('fee')
            if fee_info:
                fee_cost = float(fee_info.get('cost', 0))
                fee_currency = fee_info.get('currency')

                if '/' in symbol:
                    is_spot = 'spot' in exchange_name
                    base_currency, quote_currency = symbol.split('/')
                    
                    if fee_currency == base_currency:
                        position['p1_fee'] += fee_cost
                        # For spot trades, the fee paid in base currency reduces the actual amount of base received/held.
                        if is_spot:
                            # This correctly adjusts the position size to reflect the actual holdings after fees.
                            position['amount'] -= fee_cost
                    elif fee_currency == quote_currency:
                        position['p2_fee'] += fee_cost
                        position['total_cost'] += fee_cost
            
            self._positions[pos_key] = position

            log_prefix = "HISTORICAL " if is_historical else ""
            logger.info(f"{log_prefix}TRADE >> {exchange_name} | {symbol} | {trade['side'].upper()} {trade_amount} @ {trade_price}")

            # --- Persist updated position to inventory history ---
            if self.inventory_price_history_repo:
                new_amount = position['amount']
                # Include fees in average price calculation
                avg_entry_price = (position['total_cost'] + position['p2_fee']) / (new_amount + position['p1_fee']) if (new_amount + position['p1_fee']) != 0 else 0
                exchange_id = self.exchange_id_map.get(exchange_name)
                
                trade_ts_ms = trade.get('timestamp')
                record_timestamp = datetime.utcfromtimestamp(trade_ts_ms / 1000) if trade_ts_ms else datetime.utcnow()
                
                # Make sure the timestamp is naive for database operations
                if record_timestamp.tzinfo is not None:
                    record_timestamp = record_timestamp.replace(tzinfo=None)
                    
                logger.info(f"Persisting inventory for {exchange_name} {symbol}: size={new_amount}, avg_price={avg_entry_price}")
                
                await self.inventory_price_history_repo.add_inventory_price(
                    exchange_id=exchange_id,
                    symbol=symbol,
                    avg_price=avg_entry_price,
                    size=new_amount,
                    unrealized_pnl=0.0, # Unrealized PNL is zero at the moment of the trade
                    bot_instance_id=None,
                    timestamp=record_timestamp
                )

            # --- Realized P&L Calculation on Close ---
            new_amount = position['amount']
            if old_amount != 0 and (new_amount == 0 or (old_amount > 0 and new_amount < 0) or (old_amount < 0 and new_amount > 0)):
                # Use the fee-adjusted cost basis for accurate P&L
                avg_entry_price = (old_total_cost + position['p2_fee']) / (old_amount + position['p1_fee']) if (old_amount + position['p1_fee']) != 0 else 0
                
                # Determine the amount of the old position that was closed
                closed_amount = -old_amount
                
                realized_pnl = (trade_price - avg_entry_price) * closed_amount
                
                logger.info(
                    f"** REALIZED P&L >> {exchange_name} | {symbol} | "
                    f"Closed: {closed_amount:.4f} at {trade_price:.4f} | "
                    f"Realized P&L: {realized_pnl:.4f} **"
                )
                # Persist realized PNL event
                if self.realized_pnl_repo:
                    exchange_id = self.exchange_id_map.get(exchange_name)
                    
                    # Make sure the timestamp is naive for database operations
                    if record_timestamp.tzinfo is not None:
                        record_timestamp = record_timestamp.replace(tzinfo=None)
                        
                    await self.realized_pnl_repo.add_pnl_event(
                        exchange_id=exchange_id,
                        symbol=symbol,
                        realized_pnl=realized_pnl,
                        event_type="close" if new_amount == 0 else "flip",
                        trade_id=None, # Should be trade['id']
                        bot_instance_id=None,
                        timestamp=record_timestamp
                    )
                
                # If the position flipped, reset the cost basis for the new position
                if new_amount != 0:
                    # The new position's cost is the remainder of the flip trade
                    position['total_cost'] = new_amount * trade_price
                    # Reset fee accumulators for the new position leg
                    position['p1_fee'] = 0
                    position['p2_fee'] = 0
                    self._positions[pos_key] = position
                else:
                    # If fully closed, clear the position state
                    self._positions.pop(pos_key, None)

            # --- Dynamic Ticker Management ---
            is_open = pos_key in self._positions and abs(self._positions[pos_key]['amount']) > 1e-9
            if is_open and pos_key not in self._ticker_tasks:
                task = asyncio.create_task(self._watch_ticker_loop(exchange_name, symbol))
                self._ticker_tasks[pos_key] = task
            elif not is_open and pos_key in self._ticker_tasks:
                task = self._ticker_tasks.pop(pos_key, None)
                if task: task.cancel()

    async def handle_price_update(self, exchange_name: str, ticker: Dict[str, Any]):
        symbol = ticker.get('symbol')
        price = ticker.get('last')
        if not (symbol and price): return
        
        # --- Symbol Normalization ---
        # Use the dedicated utility to normalize the symbol.
        symbol = normalize_symbol(symbol)
        
        pos_key = f"{exchange_name}_{symbol}"
        position = self._positions.get(pos_key)
        if not position or abs(position['amount']) < 1e-9: return
        
        amount = position['amount']
        avg_entry_price = (position['total_cost'] + position['p2_fee']) / (amount + position['p1_fee']) if (amount + position['p1_fee']) != 0 else 0
        unrealized_pnl = (price - avg_entry_price) * amount
        
        logger.info(
            f"POSITION UPDATE >> {exchange_name} | {symbol} | "
            f"Exposure: {amount:.4f} | Avg Entry: {avg_entry_price:.4f} | "
            f"Current Price: {price:.4f} | Unrealized P&L: {unrealized_pnl:.4f}"
        )
        if self.inventory_price_history_repo:
            exchange_id = self.exchange_id_map.get(exchange_name)
            
            # Create a naive datetime for database operations
            record_timestamp = datetime.utcnow()
            if record_timestamp.tzinfo is not None:
                record_timestamp = record_timestamp.replace(tzinfo=None)
                
            await self.inventory_price_history_repo.add_inventory_price(
                exchange_id=exchange_id,
                symbol=symbol,
                avg_price=avg_entry_price,
                size=amount,
                unrealized_pnl=unrealized_pnl,
                bot_instance_id=None,
                timestamp=record_timestamp # Use current time for price updates
            )

    def handle_funding_payment(self, exchange_name: str, payment: Dict[str, Any]):
        """Handles a funding payment event."""
        logger.info(
            f"FUNDING PAYMENT >> {exchange_name} | {payment.get('symbol')} | "
            f"Amount: {payment.get('amount'):.4f} {payment.get('currency')} | "
            f"Timestamp: {payment.get('datetime')}"
        )

    async def handle_balance(self, name: str, balance: Dict[str, Any], market_info: Union[str, Dict], is_initial: bool = False):
        """
        Dispatches balance updates to the appropriate processing function
        based on the exchange's data structure.
        """
        # For exchanges with unified balance streams (like Binance)
        if isinstance(market_info, dict):
            base_name = name
            for market_type, unique_name in market_info.items():
                # For Binance, balance object keys are 'spot', 'future', etc.
                if market_type in balance:
                    scoped_balance = balance[market_type]
                    await self._process_single_balance(unique_name, scoped_balance, is_initial)
        # For exchanges with scoped balance streams (like Bybit)
        else:
            unique_name = name
            await self._process_single_balance(unique_name, balance, is_initial)

    async def _process_single_balance(self, unique_name: str, balance_data: Dict[str, Any], is_initial: bool):
        """
        Handles a single, scoped balance update.
        Includes reconciliation logic for spot exchanges.
        """
        self._local_balances[unique_name] = balance_data
        log_prefix = "INITIAL " if is_initial else ""
        
        # Count non-zero balances
        non_zero_currencies = []
        
        # Determine if it's a spot exchange for reconciliation
        is_spot = 'spot' in unique_name
        
        # Iterate over a copy of items to prevent "dictionary changed size during iteration" error
        for currency, details in list(balance_data.items()):
            if isinstance(details, dict) and details.get('total') is not None:
                total_balance = float(details.get('total', 0))
                
                # Only log non-zero balances
                if total_balance > 0:
                    non_zero_currencies.append(currency)
        
        # Log header with summary
        if non_zero_currencies or is_initial:
            logger.info(f"{log_prefix}BALANCE UPDATE >> Exchange: {unique_name} | Non-zero currencies: {len(non_zero_currencies)}")
            
        # Now log the actual balances
        for currency, details in list(balance_data.items()):
            if isinstance(details, dict) and details.get('total') is not None:
                total_balance = float(details.get('total', 0))
                
                # Only log non-zero balances
                if total_balance > 0:
                    logger.info(f"  {currency}: Total={total_balance:.4f}, Free={details.get('free', 0):.4f}, Used={details.get('used', 0):.4f}")
                
                # --- Balance Reconciliation Logic for Spot Exchanges ---
                if is_spot:
                    # For spot balances, reconcile with our internal position state.
                    for pos_key, internal_pos in self._positions.items():
                        # Check if the position is on the current exchange
                        if pos_key.startswith(unique_name):
                            pos_symbol = internal_pos.get('symbol')
                            if pos_symbol and pos_symbol.startswith(currency + '/'):
                                # This position's base currency matches the balance update.
                                internal_size = internal_pos.get('amount', 0.0)
                                
                                # Compare and reconcile if discrepancy is significant
                                if abs(internal_size - total_balance) > 1e-9: # Using a small tolerance
                                    logger.warning(
                                        f"RECONCILIATION >> Discrepancy found for {pos_symbol} on {unique_name}. "
                                        f"Internal size: {internal_size:.6f}, Exchange balance: {total_balance:.6f}. "
                                        f"Adjusting internal state."
                                    )
                                    
                                    # Adjust total_cost proportionally before changing the size
                                    if internal_size != 0:
                                        old_avg_price = internal_pos['total_cost'] / internal_size
                                        internal_pos['total_cost'] = old_avg_price * total_balance
                                    
                                    # Force-set the size to match the exchange balance
                                    internal_pos['amount'] = total_balance
                                    self._positions[pos_key] = internal_pos

                # Persist balance history
                if self.balance_history_repo and (is_initial or total_balance > 0):
                    exchange_id = self.exchange_id_map.get(unique_name)
                    
                    # Use naive datetime for database operations
                    record_timestamp = datetime.utcnow()
                    if record_timestamp.tzinfo is not None:
                        record_timestamp = record_timestamp.replace(tzinfo=None)
                        
                    await self.balance_history_repo.add_balance(
                        exchange_id=exchange_id,
                        currency=currency,
                        total=total_balance,
                        free=details.get('free'),
                        used=details.get('used'),
                        bot_instance_id=None,
                        timestamp=record_timestamp
                    )

