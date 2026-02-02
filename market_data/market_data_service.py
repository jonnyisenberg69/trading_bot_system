"""
Market Data Service - Connects to exchanges and publishes orderbook updates to Redis
"""
import asyncio
import json
import logging
import os
import signal
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional
import ccxt.pro
import redis.asyncio as redis
from decimal import Decimal

class MarketDataService:
    """Service that connects to exchanges and publishes market data to Redis with multi-leg pricing support"""
    
    def __init__(self, redis_url: str = "redis://localhost:6379", service_config: Optional[dict] = None):
        self.logger = logging.getLogger(__name__)
        self.redis_url = redis_url
        self.redis_client: Optional[redis.Redis] = None
        self.exchanges: Dict[str, ccxt.pro.Exchange] = {}
        self.tasks: List[asyncio.Task] = []
        self.shutdown_event = asyncio.Event()
        # Exchange configurations with explicit max depth/limit
        self.exchange_configs: Dict[str, Dict] = {}
        
        # Multi-leg pricing support
        self.base_quote_currency = 'USDT'  # Default base quote currency for pricing
        self.conversion_pairs: Dict[str, List[str]] = {}  # exchange -> [conversion_symbols]
        self.orderbook_cache: Dict[str, Dict] = {}  # (exchange, symbol) -> orderbook for WAPQ calculations
        
        # Build from provided service config if present; else keep empty to avoid wrong symbols
        if service_config:
            self._apply_service_config(service_config)

    def _apply_service_config(self, service_config: dict) -> None:
        """Build exchange configs dynamically from service config (symbol + exchanges)."""
        try:
            symbol = service_config.get('symbol', 'BERA/USDT')
            exchanges = service_config.get('exchanges', [])
            # Map base exchange name to ccxt.pro class and default depth
            class_map = {
                'binance': (ccxt.pro.binance, 1000),
                'bybit': (ccxt.pro.bybit, 200),
                'hyperliquid': (ccxt.pro.hyperliquid, 20),
                'mexc': (ccxt.pro.mexc, 100),
                'gateio': (ccxt.pro.gate, 100),
                'bitget': (ccxt.pro.bitget, 50),
                'bitfinex': (ccxt.pro.bitfinex, 100),
            }
            # Helper to map symbol per exchange
            def map_symbol(base_symbol: str, ex_name: str, ex_type: str) -> str:
                try:
                    base, quote = base_symbol.split('/')
                except ValueError:
                    base, quote = base_symbol, 'USDT'
                ex_name_l = ex_name.lower()
                ex_type_l = ex_type.lower()
                # Perp settlement formats
                if ex_type_l in ('perp', 'future', 'futures'):
                    if ex_name_l in ('binance', 'bybit'):
                        return f"{base}/USDT:USDT"
                    # Hyperliquid uses standard symbol for perp
                    if ex_name_l == 'hyperliquid':
                        return f"{base}/USDC:USDC" if quote.upper() in ('USDT', 'USD') else f"{base}/{quote}"
                # Spot formats
                if ex_name_l == 'bitfinex':
                    # Bitfinex uses USD and XAUt ticker for Tether Gold
                    if base.upper() == 'XAUT':
                        return f"XAUt/USD"
                    return f"{base}/USD" if quote.upper() == 'USDT' else f"{base}/{quote}"
                if ex_name_l == 'hyperliquid':
                    # Hyperliquid spot uses USDT0 quote; XAUT base is XAUT0
                    if ex_type_l == 'spot':
                        if base.upper() == 'XAUT':
                            return 'XAUT0/USDT0'
                        return f"{base}/USDT0" if quote.upper() in ('USDT0','USDT', 'USD') else f"{base}/{quote}"
                    # Futures use USDC:USDC settlement
                    return f"{base}/USDC:USDC" if quote.upper() in ('USDT', 'USD', 'USDC') else f"{base}/{quote}"
                # Defaults keep provided symbol
                return base_symbol
            built: Dict[str, Dict] = {}
            for ex in exchanges:
                name = ex.get('name')
                ex_type = ex.get('type', 'spot')
                if not name:
                    continue
                key = f"{name}_{ex_type}"
                if name not in class_map:
                    continue
                ex_class, default_depth = class_map[name]
                built[key] = {
                    'class': ex_class,
                    'options': {'enableRateLimit': True} if name != 'binance' else {'enableRateLimit': True},
                    'symbol': map_symbol(symbol, name, ex_type),
                    'depth': default_depth
                }
                # Special options for derivatives
                if name == 'binance' and ex_type == 'perp':
                    built[key]['options'] = {'enableRateLimit': True, 'options': {'defaultType': 'future'}}
                if name == 'bybit' and ex_type == 'perp':
                    built[key]['options'] = {'enableRateLimit': True, 'options': {'defaultType': 'linear'}}
            self.exchange_configs = built
            self.logger.info(f"Built exchange configs for {len(self.exchange_configs)} exchanges from service config")
        except Exception as e:
            self.logger.error(f"Failed to apply service config: {e}")
    
    async def start(self):
        """Start the market data service"""
        self.logger.info("Starting Market Data Service...")
        
        # Connect to Redis
        self.redis_client = redis.from_url(self.redis_url)
        await self.redis_client.ping()
        self.logger.info("Connected to Redis")
        
        # Initialize exchanges
        await self._initialize_exchanges()
        
        # Start orderbook monitoring tasks
        await self._start_monitoring_tasks()
        
        self.logger.info(f"Market Data Service started with {len(self.exchanges)} exchanges")
    
    async def stop(self):
        """Stop the market data service"""
        self.logger.info("Stopping Market Data Service...")
        
        # Set shutdown event
        self.shutdown_event.set()
        
        # Cancel all tasks
        for task in self.tasks:
            if not task.done():
                task.cancel()
        
        # Wait for tasks to complete
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        
        # Close exchanges
        for exchange_id, exchange in self.exchanges.items():
            try:
                await exchange.close()
                self.logger.info(f"Closed {exchange_id}")
            except Exception as e:
                self.logger.error(f"Error closing {exchange_id}: {e}")
        
        # Close Redis connection
        if self.redis_client:
            await self.redis_client.close()
        
        self.logger.info("Market Data Service stopped")
    
    async def _initialize_exchanges(self):
        """Initialize all exchange connections"""
        for exchange_id, config in self.exchange_configs.items():
            try:
                exchange = config['class'](config['options'])
                await exchange.load_markets()
                
                # Verify the symbol exists
                symbol = config['symbol']
                # Log Hyperliquid symbol checking
                if 'hyperliquid' in exchange_id:
                    self.logger.info(f"HYPERLIQUID INIT: Checking symbol {symbol} for {exchange_id}")
                    self.logger.info(f"HYPERLIQUID INIT: Available markets: {list(exchange.markets.keys())[:10]}...")  # Show first 10
                if symbol not in exchange.markets:
                    # Try to resolve close variants: base case variants and USD/USDT/USDC quotes
                    resolved_symbol = None
                    try:
                        base, quote = symbol.split('/')
                    except ValueError:
                        base, quote = symbol, 'USDT'
                    base_variants = {base, base.upper(), base.capitalize()}
                    if base.upper() == 'XAUT':
                        base_variants.update({'XAUt', 'XAUT0'})
                    quote_variants = {quote, quote.upper(), quote.capitalize(), 'USD', 'USDT', 'USDT0','USDC'}
                    for b in base_variants:
                        for q in quote_variants:
                            candidate = f"{b}/{q}"
                            if candidate in exchange.markets:
                                resolved_symbol = candidate
                                break
                        if resolved_symbol:
                            break
                    if resolved_symbol:
                        self.logger.info(f"{exchange_id}: Resolved symbol {symbol} -> {resolved_symbol}")
                        # Update config and use resolved symbol
                        self.exchange_configs[exchange_id]['symbol'] = resolved_symbol
                        symbol = resolved_symbol
                    else:
                        self.logger.warning(f"{exchange_id}: Symbol {symbol} not found, skipping")
                        continue
                
                self.exchanges[exchange_id] = exchange
                self.logger.info(f"Initialized {exchange_id} for {symbol}")
                
            except Exception as e:
                self.logger.error(f"Failed to initialize {exchange_id}: {e}")
    
    async def _start_monitoring_tasks(self):
        """Start orderbook monitoring tasks for all exchanges"""
        for exchange_id, exchange in self.exchanges.items():
            symbol = self.exchange_configs[exchange_id]['symbol']
            
            # Create orderbook monitoring task
            task = asyncio.create_task(
                self._monitor_orderbook(exchange_id, exchange, symbol)
            )
            self.tasks.append(task)
    
    async def _monitor_orderbook(self, exchange_id: str, exchange: ccxt.pro.Exchange, symbol: str):
        """Monitor orderbook for a specific exchange and publish updates"""
        self.logger.info(f"Starting orderbook monitoring for {exchange_id} {symbol}")
        depth = self.exchange_configs[exchange_id].get('depth', 1000)
        
        # Temporary workaround for Bitfinex WebSocket issues
        use_rest_only = False
        if 'bitfinex' in exchange_id.lower():
            self.logger.warning(f"Bitfinex WebSocket may have parsing issues with current CCXT version. Consider using REST-only mode.")
            # Uncomment the next line to force REST-only mode for Bitfinex
            # use_rest_only = True
        # Special handling for Hyperliquid
        if 'hyperliquid' in exchange_id:
            self.logger.info(f"Using Hyperliquid integration for {exchange_id}")
            params = {'nSigFigs': 2}
            limit = 20  # Hyperliquid max
            local_orderbook = None
            while not self.shutdown_event.is_set():
                try:
                    # 1. Fetch full snapshot via REST with nSigFigs=2
                    self.logger.info(f"{exchange_id}: Fetching REST snapshot with nSigFigs=2, limit=20")
                    snapshot = await exchange.fetch_order_book(symbol, limit=limit, params=params)
                    self.logger.info(f"{exchange_id}: REST snapshot received: {len(snapshot['bids'])} bids, {len(snapshot['asks'])} asks (requested limit={limit})")
                    if not snapshot['bids'] or not snapshot['asks']:
                        self.logger.warning(f"{exchange_id}: Empty snapshot, retrying in 2s...")
                        await asyncio.sleep(2)
                        continue
                    local_orderbook = snapshot
                    # Log the number of levels and a sample before publishing
                    self.logger.info(f"{exchange_id} REST local_orderbook['bids'] length: {len(local_orderbook['bids'])}")
                    self.logger.info(f"{exchange_id} REST local_orderbook['asks'] length: {len(local_orderbook['asks'])}")
                    self.logger.info(f"{exchange_id} REST local_orderbook['bids'] sample: {local_orderbook['bids'][:20]}")
                    self.logger.info(f"{exchange_id} REST local_orderbook['asks'] sample: {local_orderbook['asks'][:20]}")
                    # Publish initial snapshot
                    best_bid = local_orderbook['bids'][0][0]
                    best_ask = local_orderbook['asks'][0][0]
                    midpoint = (best_bid + best_ask) / 2
                    update = {
                        'exchange': exchange_id,
                        'symbol': symbol,
                        'timestamp': local_orderbook['timestamp'] or int(datetime.now(timezone.utc).timestamp() * 1000),
                        'best_bid': float(best_bid),
                        'best_ask': float(best_ask),
                        'midpoint': float(midpoint),
                        'bid_amount': float(local_orderbook['bids'][0][1]),
                        'ask_amount': float(local_orderbook['asks'][0][1]),
                        'bids': [[str(level[0]), str(level[1])] for level in local_orderbook['bids']],
                        'asks': [[str(level[0]), str(level[1])] for level in local_orderbook['asks']]
                    }
                    await self._publish_orderbook_update(update)
                    
                    # Cache orderbook for multi-leg WAPQ calculations
                    orderbook_key = f"{exchange_id}:{symbol}"
                    self.orderbook_cache[orderbook_key] = local_orderbook
                    
                    # Auto-detect and ensure conversion pairs are available
                    await self.ensure_conversion_pairs(symbol, exchange_id)
                    
                    self.logger.info(f"{exchange_id}: Published initial snapshot with {len(local_orderbook['bids'])} bids, {len(local_orderbook['asks'])} asks.")
                    # Extra logging for Hyperliquid
                    if 'hyperliquid' in exchange_id:
                        self.logger.info(f"HYPERLIQUID DEBUG: Publishing {exchange_id} with symbol={symbol}, bid={best_bid}, ask={best_ask}")
                    print(f"{exchange_id} publishing: {len(local_orderbook['bids'])} bids, {len(local_orderbook['asks'])} asks")
                    # 2. Start WebSocket stream for incremental updates
                    while not self.shutdown_event.is_set():
                        try:
                            ws_orderbook = await exchange.watch_order_book(symbol, limit=limit, params=params)
                            self.logger.info(f"{exchange_id}: WebSocket update received: {len(ws_orderbook['bids'])} bids, {len(ws_orderbook['asks'])} asks (requested limit={limit})")
                            local_orderbook = ws_orderbook
                            # Log the number of levels and a sample before publishing
                            self.logger.info(f"{exchange_id} WS local_orderbook['bids'] length: {len(local_orderbook['bids'])}")
                            self.logger.info(f"{exchange_id} WS local_orderbook['asks'] length: {len(local_orderbook['asks'])}")
                            self.logger.info(f"{exchange_id} WS local_orderbook['bids'] sample: {local_orderbook['bids'][:20]}")
                            self.logger.info(f"{exchange_id} WS local_orderbook['asks'] sample: {local_orderbook['asks'][:20]}")
                            if local_orderbook['bids'] and local_orderbook['asks']:
                                best_bid = local_orderbook['bids'][0][0]
                                best_ask = local_orderbook['asks'][0][0]
                                midpoint = (best_bid + best_ask) / 2
                                update = {
                                    'exchange': exchange_id,
                                    'symbol': symbol,
                                    'timestamp': local_orderbook['timestamp'] or int(datetime.now(timezone.utc).timestamp() * 1000),
                                    'best_bid': float(best_bid),
                                    'best_ask': float(best_ask),
                                    'midpoint': float(midpoint),
                                    'bid_amount': float(local_orderbook['bids'][0][1]),
                                    'ask_amount': float(local_orderbook['asks'][0][1]),
                                    'bids': [[str(level[0]), str(level[1])] for level in local_orderbook['bids']],
                                    'asks': [[str(level[0]), str(level[1])] for level in local_orderbook['asks']]
                                }
                                await self._publish_orderbook_update(update)
                                
                                # Cache updated orderbook for multi-leg WAPQ calculations
                                orderbook_key = f"{exchange_id}:{symbol}"
                                self.orderbook_cache[orderbook_key] = local_orderbook
                                
                                # Extra logging for Hyperliquid WebSocket updates
                                if 'hyperliquid' in exchange_id:
                                    self.logger.info(f"HYPERLIQUID WS DEBUG: Publishing {exchange_id} with symbol={symbol}, bid={best_bid}, ask={best_ask}")
                                self.logger.debug(
                                    f"{exchange_id}: bid={best_bid:.4f}, ask={best_ask:.4f}, mid={midpoint:.4f}, bids={len(local_orderbook['bids'])}, asks={len(local_orderbook['asks'])}"
                                )
                        except asyncio.CancelledError:
                            self.logger.info(f"Orderbook monitoring cancelled for {exchange_id}")
                            break
                        except Exception as e:
                            self.logger.error(f"Error in Hyperliquid WebSocket orderbook for {exchange_id}: {e}")
                            self.logger.info(f"{exchange_id}: Re-syncing with REST snapshot in 2s...")
                            await asyncio.sleep(2)
                            break  # Break inner loop to re-fetch snapshot
                except asyncio.CancelledError:
                    self.logger.info(f"Orderbook monitoring cancelled for {exchange_id}")
                    break
                except Exception as e:
                    self.logger.error(f"Error monitoring {exchange_id} orderbook: {e}")
                    await asyncio.sleep(2)
            self.logger.info(f"Orderbook monitoring stopped for {exchange_id}")
            return
        local_orderbook = None
        while not self.shutdown_event.is_set():
            try:
                # 1. Fetch full snapshot via REST
                self.logger.info(f"{exchange_id}: Fetching REST snapshot with depth={depth}")
                snapshot = await exchange.fetch_order_book(symbol, limit=depth)
                self.logger.info(f"{exchange_id}: REST snapshot received: {len(snapshot['bids'])} bids, {len(snapshot['asks'])} asks (requested limit={depth})")
                if not snapshot['bids'] or not snapshot['asks']:
                    self.logger.warning(f"{exchange_id}: Empty snapshot, retrying in 2s...")
                    await asyncio.sleep(2)
                    continue
                local_orderbook = snapshot
                # Log the number of levels and a sample before publishing
                self.logger.info(f"{exchange_id} REST local_orderbook['bids'] length: {len(local_orderbook['bids'])}")
                self.logger.info(f"{exchange_id} REST local_orderbook['asks'] length: {len(local_orderbook['asks'])}")
                self.logger.info(f"{exchange_id} REST local_orderbook['bids'] sample: {local_orderbook['bids'][:20]}")
                self.logger.info(f"{exchange_id} REST local_orderbook['asks'] sample: {local_orderbook['asks'][:20]}")
                # Publish initial snapshot
                best_bid = local_orderbook['bids'][0][0]
                best_ask = local_orderbook['asks'][0][0]
                midpoint = (best_bid + best_ask) / 2
                update = {
                    'exchange': exchange_id,
                    'symbol': symbol,
                    'timestamp': local_orderbook['timestamp'] or int(datetime.now(timezone.utc).timestamp() * 1000),
                    'best_bid': float(best_bid),
                    'best_ask': float(best_ask),
                    'midpoint': float(midpoint),
                    'bid_amount': float(local_orderbook['bids'][0][1]),
                    'ask_amount': float(local_orderbook['asks'][0][1]),
                    'bids': [[str(level[0]), str(level[1])] for level in local_orderbook['bids']],
                    'asks': [[str(level[0]), str(level[1])] for level in local_orderbook['asks']]
                }
                await self._publish_orderbook_update(update)
                
                # Cache orderbook for multi-leg WAPQ calculations
                orderbook_key = f"{exchange_id}:{symbol}"
                self.orderbook_cache[orderbook_key] = local_orderbook
                
                # Auto-detect and ensure conversion pairs are available
                await self.ensure_conversion_pairs(symbol, exchange_id)
                
                self.logger.info(f"{exchange_id}: Published initial snapshot with {len(local_orderbook['bids'])} bids, {len(local_orderbook['asks'])} asks.")
                print(f"{exchange_id} publishing: {len(local_orderbook['bids'])} bids, {len(local_orderbook['asks'])} asks")
                # 2. Start WebSocket stream for incremental updates (or REST polling if WebSocket has issues)
                while not self.shutdown_event.is_set():
                    try:
                        if use_rest_only:
                            # REST polling mode
                            await asyncio.sleep(1)  # Poll every 1 second
                            ws_orderbook = await exchange.fetch_order_book(symbol, limit=depth)
                            self.logger.debug(f"{exchange_id}: REST poll update received: {len(ws_orderbook['bids'])} bids, {len(ws_orderbook['asks'])} asks")
                        else:
                            # WebSocket mode
                            ws_orderbook = await exchange.watch_order_book(symbol, limit=depth)
                            self.logger.info(f"{exchange_id}: WebSocket update received: {len(ws_orderbook['bids'])} bids, {len(ws_orderbook['asks'])} asks (requested limit={depth})")
                        # Merge incremental update into local_orderbook (ccxt.pro does this for you)
                        local_orderbook = ws_orderbook
                        # Log the number of levels and a sample before publishing
                        self.logger.info(f"{exchange_id} WS local_orderbook['bids'] length: {len(local_orderbook['bids'])}")
                        self.logger.info(f"{exchange_id} WS local_orderbook['asks'] length: {len(local_orderbook['asks'])}")
                        self.logger.info(f"{exchange_id} WS local_orderbook['bids'] sample: {local_orderbook['bids'][:20]}")
                        self.logger.info(f"{exchange_id} WS local_orderbook['asks'] sample: {local_orderbook['asks'][:20]}")
                        if local_orderbook['bids'] and local_orderbook['asks']:
                            best_bid = local_orderbook['bids'][0][0]
                            best_ask = local_orderbook['asks'][0][0]
                            midpoint = (best_bid + best_ask) / 2
                            update = {
                                'exchange': exchange_id,
                                'symbol': symbol,
                                'timestamp': local_orderbook['timestamp'] or int(datetime.now(timezone.utc).timestamp() * 1000),
                                'best_bid': float(best_bid),
                                'best_ask': float(best_ask),
                                'midpoint': float(midpoint),
                                'bid_amount': float(local_orderbook['bids'][0][1]),
                                'ask_amount': float(local_orderbook['asks'][0][1]),
                                'bids': [[str(level[0]), str(level[1])] for level in local_orderbook['bids']],
                                'asks': [[str(level[0]), str(level[1])] for level in local_orderbook['asks']]
                            }
                            await self._publish_orderbook_update(update)
                            
                            # Cache updated orderbook for multi-leg WAPQ calculations
                            orderbook_key = f"{exchange_id}:{symbol}"
                            self.orderbook_cache[orderbook_key] = local_orderbook
                            
                            self.logger.debug(
                                f"{exchange_id}: bid={best_bid:.4f}, ask={best_ask:.4f}, mid={midpoint:.4f}, bids={len(local_orderbook['bids'])}, asks={len(local_orderbook['asks'])}"
                            )
                    except asyncio.CancelledError:
                        self.logger.info(f"Orderbook monitoring cancelled for {exchange_id}")
                        break
                    except Exception as e:
                        self.logger.error(f"Error in WebSocket orderbook for {exchange_id}: {e}")
                        # Log more details for Bitfinex errors
                        if 'bitfinex' in exchange_id.lower() and 'invalid literal' in str(e):
                            self.logger.error(f"Bitfinex CCXT parsing error - this may be a CCXT library bug. Error details: {type(e).__name__}: {e}")
                        self.logger.info(f"{exchange_id}: Re-syncing with REST snapshot in 2s...")
                        await asyncio.sleep(2)
                        break  # Break inner loop to re-fetch snapshot
            except asyncio.CancelledError:
                self.logger.info(f"Orderbook monitoring cancelled for {exchange_id}")
                break
            except Exception as e:
                self.logger.error(f"Error monitoring {exchange_id} orderbook: {e}")
                await asyncio.sleep(2)
        self.logger.info(f"Orderbook monitoring stopped for {exchange_id}")
    
    async def _publish_orderbook_update(self, update: dict):
        """Publish orderbook update to Redis and store for multi-leg conversion lookups"""
        try:
            # Publish to general orderbook channel
            await self.redis_client.publish(
                'orderbook_updates',
                json.dumps(update)
            )
            
            # Publish to exchange-specific channel
            await self.redis_client.publish(
                f'orderbook_updates:{update["exchange"]}',
                json.dumps(update)
            )
            
            # Store orderbook data for multi-leg conversion lookups
            redis_key = f"orderbook:{update['exchange']}:{update['symbol']}"
            await self.redis_client.set(
                redis_key,
                json.dumps(update),
                ex=60  # Expire after 60 seconds
            )
            
        except Exception as e:
            self.logger.error(f"Error publishing orderbook update: {e}")

            # Multi-leg pricing methods
    def get_base_quote_currency(self, exchange_id: str) -> str:
        """
        Get the base quote currency for an exchange (usually USDT).
        
        Args:
            exchange_id: Exchange identifier (e.g., 'binance_spot')
            
        Returns:
            Base quote currency symbol (e.g., 'USDT')
        """
        # Most exchanges use USDT as base quote currency
        # Could be made configurable per exchange if needed
        return 'USDT'
    def needs_conversion(self, symbol: str, exchange_id: str) -> bool:
        """
        Check if a symbol needs conversion to base quote currency.
        
        Args:
            symbol: Trading symbol (e.g., 'BERA/BTC')
            exchange_id: Exchange identifier
            
        Returns:
            True if conversion needed (quote != base_quote_currency)
        """
        try:
            if '/' not in symbol:
                return False
                
            base, quote = symbol.split('/')
            
            # Map exchange-specific quote currency variations
            base_quote_variants = {
                self.base_quote_currency,  # USDT
                'USD',  # Some exchanges use USD instead of USDT
                'USDC'  # Sometimes considered equivalent
            }
            
            return quote not in base_quote_variants
            
        except Exception:
            return False
    
    def get_conversion_symbol(self, symbol: str, exchange_id: str) -> Optional[str]:
        """
        Get the conversion pair needed to convert to base quote currency.
        
        Args:
            symbol: Original symbol (e.g., 'BERA/BTC')
            exchange_id: Exchange identifier
            
        Returns:
            Conversion symbol (e.g., 'BTC/USDT') or None if no conversion needed
        """
        try:
            if not self.needs_conversion(symbol, exchange_id):
                return None
                
            base, quote = symbol.split('/')
            conversion_symbol = f"{quote}/{self.base_quote_currency}"
            
            return conversion_symbol
            
        except Exception:
            return None
    
    async def calculate_multi_leg_wapq(
        self, 
        symbol: str, 
        exchange_id: str, 
        side: str, 
        quantity: Decimal
    ) -> Optional[Decimal]:
        """
        Calculate multi-leg WAPQ for symbol conversion to base quote currency.
        
        Args:
            symbol: Trading symbol (e.g., 'BERA/BTC')
            exchange_id: Exchange identifier
            side: 'bid', 'ask', or 'mid'
            quantity: Quantity to calculate WAPQ for
            
        Returns:
            WAPQ price in base quote currency (USDT) or None if calculation fails
        """
        try:
            # If no conversion needed, calculate direct WAPQ
            if not self.needs_conversion(symbol, exchange_id):
                return await self._calculate_direct_wapq(symbol, exchange_id, side, quantity)
            
            # Get conversion pair
            conversion_symbol = self.get_conversion_symbol(symbol, exchange_id)
            if not conversion_symbol:
                return None
            
            # Step 1: Calculate WAPQ for original symbol (e.g., BERA/BTC)
            original_wapq = await self._calculate_direct_wapq(symbol, exchange_id, side, quantity)
            if not original_wapq:
                return None
            
            # Step 2: Handle reverse pairs (like USDT/TRY instead of TRY/USDT)
            base, quote = symbol.split('/')
            base_quote = self.get_base_quote_currency(exchange_id)
            
            # Check if we need reverse conversion
            reverse_currencies = ['TRY', 'JPY', 'KRW', 'RUB']
            is_reverse = quote in reverse_currencies
            
            if is_reverse:
                # Use reverse pair: USDT/TRY instead of TRY/USDT
                actual_conversion_symbol = f"{base_quote}/{quote}"
            else:
                # Use standard pair: BTC/USDT
                actual_conversion_symbol = conversion_symbol
                
            # Calculate WAPQ for conversion pair (try Redis first, then local cache)
            conversion_wapq = await self._get_conversion_wapq_from_redis(actual_conversion_symbol, exchange_id, quantity)
            if not conversion_wapq:
                # Fallback to local cache (in case it's in the same service)
                conversion_wapq = await self._calculate_direct_wapq(actual_conversion_symbol, exchange_id, 'mid', quantity)
            
            if not conversion_wapq:
                self.logger.warning(f"No WAPQ available for conversion pair {actual_conversion_symbol}")
                return None
            
            # Step 3: Calculate final converted price
            if is_reverse:
                # For reverse pairs: BERA/TRY ÷ USDT/TRY = BERA/USDT
                # (since USDT/TRY tells us how many TRY per USDT, we divide)
                converted_price = original_wapq / conversion_wapq
                self.logger.debug(
                    f"Multi-leg conversion (REVERSE): {symbol} ${original_wapq:.4f} ÷ {actual_conversion_symbol} ${conversion_wapq:.4f} = ${converted_price:.4f}"
                )
            else:
                # Standard: BERA/BTC × BTC/USDT = BERA/USDT
                converted_price = original_wapq * conversion_wapq
                self.logger.debug(
                    f"Multi-leg conversion (STANDARD): {symbol} ${original_wapq:.4f} × {actual_conversion_symbol} ${conversion_wapq:.4f} = ${converted_price:.4f}"
                )
            
            return converted_price
            
        except Exception as e:
            self.logger.error(f"Error calculating multi-leg WAPQ for {symbol} on {exchange_id}: {e}")
            return None
    
    async def _get_conversion_wapq_from_redis(self, conversion_symbol: str, exchange_id: str, quantity: Decimal) -> Optional[Decimal]:
        """
        Get conversion pair WAPQ from Redis (where other services publish their data).
        
        Args:
            conversion_symbol: Conversion pair symbol (e.g., 'BTC/USDT', 'USDT/TRY')
            exchange_id: Exchange identifier
            quantity: Quantity to calculate WAPQ for
            
        Returns:
            WAPQ price for conversion or None if not available
        """
        try:
            if not self.redis_client:
                return None
            
            # Try to get orderbook data from Redis
            redis_key = f"orderbook:{exchange_id}:{conversion_symbol}"
            orderbook_data = await self.redis_client.get(redis_key)
            
            if not orderbook_data:
                # Try alternative Redis key format
                redis_key = f"orderbook_updates:{exchange_id}:{conversion_symbol}"
                orderbook_data = await self.redis_client.get(redis_key)
            
            if not orderbook_data:
                return None
            
            # Parse orderbook from Redis
            import json
            orderbook = json.loads(orderbook_data)
            
            if not orderbook.get('bids') or not orderbook.get('asks'):
                return None
            
            # Calculate WAPQ from Redis orderbook data
            # For mid pricing, take average of best bid and ask
            best_bid = Decimal(str(orderbook['bids'][0][0]))
            best_ask = Decimal(str(orderbook['asks'][0][0]))
            mid_price = (best_bid + best_ask) / Decimal('2')
            
            self.logger.debug(f"Got conversion rate from Redis: {conversion_symbol} = ${mid_price:.6f}")
            return mid_price
            
        except Exception as e:
            self.logger.debug(f"Error getting conversion WAPQ from Redis for {conversion_symbol}: {e}")
            return None
    
    async def _calculate_direct_wapq(
        self, 
        symbol: str, 
        exchange_id: str, 
        side: str, 
        quantity: Decimal
    ) -> Optional[Decimal]:
        """
        Calculate WAPQ for a single symbol on an exchange.
        
        Args:
            symbol: Trading symbol
            exchange_id: Exchange identifier  
            side: 'bid', 'ask', or 'mid'
            quantity: Quantity for WAPQ calculation
            
        Returns:
            WAPQ price or None if calculation fails
        """
        try:
            # Get cached orderbook
            orderbook_key = f"{exchange_id}:{symbol}"
            orderbook = self.orderbook_cache.get(orderbook_key)
            
            if not orderbook or not orderbook.get('bids') or not orderbook.get('asks'):
                return None
            
            # Calculate WAPQ based on side
            if side in ('bid', 'buy'):
                # For bid pricing, we consume asks (we're buying)
                levels = orderbook.get('asks', [])
            elif side in ('ask', 'sell'):
                # For ask pricing, we consume bids (we're selling)
                levels = orderbook.get('bids', [])
            else:  # mid
                # For mid pricing, take average of best bid and ask
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
            
        except Exception as e:
            self.logger.error(f"Error calculating direct WAPQ for {symbol} on {exchange_id}: {e}")
            return None
    
    async def ensure_conversion_pairs(self, symbol: str, exchange_id: str):
        """
        Ensure conversion pairs are available for multi-leg pricing.
        Handles both normal (BTC/USDT) and reverse (USDT/TRY) pairs.
        
        Args:
            symbol: Primary symbol being traded (e.g., 'BERA/TRY')
            exchange_id: Exchange identifier
        """
        try:
            conversion_symbol = self.get_conversion_symbol(symbol, exchange_id)
            if not conversion_symbol:
                return  # No conversion needed
            
            # For reverse pairs like TRY, we need to check both directions
            base, quote = symbol.split('/')
            base_quote = self.get_base_quote_currency(exchange_id)
            
            # Standard: QUOTE/BASE_QUOTE (e.g., BTC/USDT)
            standard_conversion = f"{quote}/{base_quote}"
            # Reverse: BASE_QUOTE/QUOTE (e.g., USDT/TRY)
            reverse_conversion = f"{base_quote}/{quote}"
            
            # Determine which conversion to try first
            reverse_currencies = ['TRY', 'JPY', 'KRW', 'RUB']  # Currencies commonly quoted in reverse
            conversion_to_use = reverse_conversion if quote in reverse_currencies else standard_conversion
            
            # Check if we already have the conversion pair
            if exchange_id not in self.conversion_pairs:
                self.conversion_pairs[exchange_id] = []
            
            if conversion_to_use not in self.conversion_pairs[exchange_id]:
                self.conversion_pairs[exchange_id].append(conversion_to_use)
                
                # Start monitoring the conversion pair
                self.logger.info(f"Adding conversion pair {conversion_to_use} for {symbol} on {exchange_id} (reverse pair detection)")
                
                # Add to exchange config and start monitoring
                if exchange_id in self.exchange_configs:
                    exchange_config = self.exchange_configs[exchange_id]
                    exchange = self.exchanges[exchange_id]
                    
                    # Start monitoring task for conversion pair
                    task = asyncio.create_task(
                        self._monitor_orderbook(exchange_id, conversion_to_use, exchange, exchange_config['depth'])
                    )
                    self.tasks.append(task)
                    
        except Exception as e:
            self.logger.error(f"Error ensuring conversion pairs for {symbol} on {exchange_id}: {e}")


async def main():
    """Main function to run the market data service"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Market Data Service')
    parser.add_argument('--config', help='Path to service config file')
    parser.add_argument('--account-hash', help='Account hash for this service instance')
    
    args = parser.parse_args()
    
    # Setup logging
    log_filename = f'market_data_service_{args.account_hash[:8] if args.account_hash else "default"}.log'
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        filename=log_filename
    )
    
    # Load config if provided
    config = {}
    if args.config and os.path.exists(args.config):
        with open(args.config, 'r') as f:
            config = json.load(f)
    
    service = MarketDataService(
        config.get('redis_url', 'redis://localhost:6379'),
        service_config=config
    )
    
    # Setup signal handlers
    def signal_handler(sig, frame):
        logging.info(f"Received signal {sig}, shutting down...")
        asyncio.create_task(service.stop())
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        await service.start()
        
        # Wait for shutdown
        await service.shutdown_event.wait()
        
    except Exception as e:
        logging.error(f"Error in market data service: {e}")
    finally:
        await service.stop()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Market Data Service')
    parser.add_argument('--config', help='Path to service config file')
    parser.add_argument('--account-hash', help='Account hash for this service instance')
    
    args = parser.parse_args()
    
    if args.config:
        print(f"Starting Market Data Service for account {args.account_hash[:8] if args.account_hash else 'default'}...")
        print(f"Using config: {args.config}")
    else:
        print("Starting Market Data Service...")
        print("This service will connect to exchanges and publish orderbook data to Redis")
    
    print("Press Ctrl+C to stop")
    
    asyncio.run(main()) 
