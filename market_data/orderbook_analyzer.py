"""
Real-time orderbook analysis for multiple exchanges.

Fetches and analyzes orderbook data to calculate:
- Total liquidity at different price levels
- Spread and mid-price
- Price impact for different order sizes
- Orderbook imbalance
"""

import asyncio
import time
from typing import Dict, List, Optional, Any, Tuple, Set, Callable
from decimal import Decimal
import statistics
from datetime import datetime, timedelta, timezone
import structlog
import pandas as pd
import numpy as np
from collections import defaultdict

from exchanges.websocket_manager import WebSocketManager

logger = structlog.get_logger(__name__)


class OrderbookData:
    """Container for orderbook data with analysis methods."""
    
    def __init__(
        self,
        exchange: str,
        symbol: str,
        timestamp: float,
        bids: List[List[Decimal]],
        asks: List[List[Decimal]],
        last_update_id: Optional[int] = None
    ):
        """
        Initialize orderbook data.
        
        Args:
            exchange: Exchange name
            symbol: Trading symbol
            timestamp: Timestamp in seconds
            bids: List of [price, amount] bid levels
            asks: List of [price, amount] ask levels
            last_update_id: Last update ID (for some exchanges)
        """
        self.exchange = exchange
        self.symbol = symbol
        self.timestamp = timestamp
        self.bids = bids  # [[price, amount], ...]
        self.asks = asks  # [[price, amount], ...]
        self.last_update_id = last_update_id
        self.datetime = datetime.fromtimestamp(timestamp)
        
        # Derived metrics (calculated on demand)
        self._best_bid_cache = None
        self._best_ask_cache = None
        self._midprice_cache = None
        self._spread_cache = None
        self._liquidity_cache = {}
        
    @property
    def best_bid(self) -> Optional[Decimal]:
        """Get best bid price."""
        if self._best_bid_cache is None:
            if self.bids:
                self._best_bid_cache = self.bids[0][0]
        return self._best_bid_cache
        
    @property
    def best_ask(self) -> Optional[Decimal]:
        """Get best ask price."""
        if self._best_ask_cache is None:
            if self.asks:
                self._best_ask_cache = self.asks[0][0]
        return self._best_ask_cache
        
    @property
    def midprice(self) -> Optional[Decimal]:
        """Get midprice."""
        if self._midprice_cache is None:
            if self.best_bid and self.best_ask:
                self._midprice_cache = (self.best_bid + self.best_ask) / Decimal('2')
        return self._midprice_cache
        
    @property
    def spread(self) -> Optional[Decimal]:
        """Get bid-ask spread."""
        if self._spread_cache is None:
            if self.best_bid and self.best_ask:
                self._spread_cache = self.best_ask - self.best_bid
        return self._spread_cache
        
    @property
    def spread_pct(self) -> Optional[Decimal]:
        """Get bid-ask spread as percentage of midprice."""
        if self.midprice and self.spread:
            return (self.spread / self.midprice) * Decimal('100')
        return None
        
    def get_liquidity(self, depth: Decimal = Decimal('0.01')) -> Tuple[Decimal, Decimal]:
        """
        Get liquidity at specified depth from midprice.
        
        Args:
            depth: Depth as fraction of price (e.g., 0.01 = 1%)
            
        Returns:
            Tuple of (bid_liquidity, ask_liquidity)
        """
        # Check cache
        cache_key = str(depth)
        if cache_key in self._liquidity_cache:
            return self._liquidity_cache[cache_key]
            
        if not self.midprice:
            return Decimal('0'), Decimal('0')
            
        bid_threshold = self.midprice * (Decimal('1') - depth)
        ask_threshold = self.midprice * (Decimal('1') + depth)
        
        bid_liquidity = Decimal('0')
        for price, amount in self.bids:
            if price >= bid_threshold:
                bid_liquidity += amount
            else:
                break
                
        ask_liquidity = Decimal('0')
        for price, amount in self.asks:
            if price <= ask_threshold:
                ask_liquidity += amount
            else:
                break
                
        result = (bid_liquidity, ask_liquidity)
        self._liquidity_cache[cache_key] = result
        return result
        
    def get_liquidity_in_quote(self, depth: Decimal = Decimal('0.01')) -> Tuple[Decimal, Decimal]:
        """
        Get liquidity at specified depth from midprice in quote currency.
        
        Args:
            depth: Depth as fraction of price (e.g., 0.01 = 1%)
            
        Returns:
            Tuple of (bid_liquidity_quote, ask_liquidity_quote)
        """
        if not self.midprice:
            return Decimal('0'), Decimal('0')
            
        bid_threshold = self.midprice * (Decimal('1') - depth)
        ask_threshold = self.midprice * (Decimal('1') + depth)
        
        bid_liquidity = Decimal('0')
        for price, amount in self.bids:
            if price >= bid_threshold:
                bid_liquidity += amount * price
            else:
                break
                
        ask_liquidity = Decimal('0')
        for price, amount in self.asks:
            if price <= ask_threshold:
                ask_liquidity += amount * price
            else:
                break
                
        return bid_liquidity, ask_liquidity
        
    def get_price_impact(self, order_size: Decimal, side: str = 'buy') -> Decimal:
        """
        Calculate price impact of an order.
        
        Args:
            order_size: Order size in base currency
            side: Order side ('buy' or 'sell')
            
        Returns:
            Price impact as percentage
        """
        if not self.midprice:
            return Decimal('0')
            
        if side.lower() == 'buy':
            book_side = self.asks
            total_size = Decimal('0')
            total_cost = Decimal('0')
            
            for price, amount in book_side:
                if total_size >= order_size:
                    break
                    
                available = min(amount, order_size - total_size)
                total_size += available
                total_cost += available * price
                
            if total_size > 0:
                avg_price = total_cost / total_size
                return ((avg_price / self.midprice) - Decimal('1')) * Decimal('100')
            
        else:  # sell
            book_side = self.bids
            total_size = Decimal('0')
            total_revenue = Decimal('0')
            
            for price, amount in book_side:
                if total_size >= order_size:
                    break
                    
                available = min(amount, order_size - total_size)
                total_size += available
                total_revenue += available * price
                
            if total_size > 0:
                avg_price = total_revenue / total_size
                return (Decimal('1') - (avg_price / self.midprice)) * Decimal('100')
                
        return Decimal('0')
        
    def get_orderbook_imbalance(self, levels: int = 10) -> Decimal:
        """
        Calculate orderbook imbalance.
        
        Args:
            levels: Number of price levels to consider
            
        Returns:
            Imbalance as decimal between -1 (all asks) and 1 (all bids)
        """
        bid_volume = Decimal('0')
        ask_volume = Decimal('0')
        
        for i, (price, amount) in enumerate(self.bids):
            if i >= levels:
                break
            bid_volume += amount
            
        for i, (price, amount) in enumerate(self.asks):
            if i >= levels:
                break
            ask_volume += amount
            
        total_volume = bid_volume + ask_volume
        if total_volume == Decimal('0'):
            return Decimal('0')
            
        return (bid_volume - ask_volume) / total_volume
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert orderbook data to dictionary."""
        base, quote = self.symbol.split('/')
        
        bid_liquidity, ask_liquidity = self.get_liquidity()
        bid_liquidity_quote, ask_liquidity_quote = self.get_liquidity_in_quote()
        
        return {
            'exchange': self.exchange,
            'symbol': self.symbol,
            'base': base,
            'quote': quote,
            'timestamp': self.timestamp,
            'datetime': self.datetime.isoformat(),
            'best_bid': float(self.best_bid) if self.best_bid else None,
            'best_ask': float(self.best_ask) if self.best_ask else None,
            'midprice': float(self.midprice) if self.midprice else None,
            'spread': float(self.spread) if self.spread else None,
            'spread_pct': float(self.spread_pct) if self.spread_pct else None,
            'bid_liquidity': float(bid_liquidity),
            'ask_liquidity': float(ask_liquidity),
            'bid_liquidity_quote': float(bid_liquidity_quote),
            'ask_liquidity_quote': float(ask_liquidity_quote),
            'total_liquidity_base': float(bid_liquidity + ask_liquidity),
            'total_liquidity_quote': float(bid_liquidity_quote + ask_liquidity_quote),
            'imbalance': float(self.get_orderbook_imbalance()),
            'bids_count': len(self.bids),
            'asks_count': len(self.asks)
        }
        
    def print_summary(self) -> None:
        """Print summary of orderbook data."""
        bid_liq, ask_liq = self.get_liquidity()
        bid_liq_quote, ask_liq_quote = self.get_liquidity_in_quote()
        
        base, quote = self.symbol.split('/')
        
        print(f"\n===== {self.exchange} {self.symbol} Orderbook =====")
        print(f"Time: {self.datetime.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Best Bid: {self.best_bid} {quote}")
        print(f"Best Ask: {self.best_ask} {quote}")
        print(f"Mid Price: {self.midprice} {quote}")
        print(f"Spread: {self.spread} {quote} ({self.spread_pct:.4f}%)")
        print(f"Liquidity (1% depth):")
        print(f"  Bid: {bid_liq:.6f} {base} ({bid_liq_quote:.2f} {quote})")
        print(f"  Ask: {ask_liq:.6f} {base} ({ask_liq_quote:.2f} {quote})")
        print(f"  Total: {bid_liq + ask_liq:.6f} {base} ({bid_liq_quote + ask_liq_quote:.2f} {quote})")
        print(f"Price Impact (1 {base}):")
        print(f"  Buy: {self.get_price_impact(Decimal('1'), 'buy'):.4f}%")
        print(f"  Sell: {self.get_price_impact(Decimal('1'), 'sell'):.4f}%")
        print(f"Orderbook Imbalance: {self.get_orderbook_imbalance():.4f}")
        print("=" * 40)


class OrderbookAnalyzer:
    """
    Orderbook analyzer for multiple exchanges.
    
    Features:
    - Real-time orderbook data collection
    - Cross-exchange liquidity analysis
    - Best execution price discovery
    - Price impact analysis
    - Market quality metrics
    """
    
    def __init__(self):
        """Initialize orderbook analyzer."""
        self.ws_managers = {}  # exchange -> WebSocketManager
        self.orderbooks = {}  # exchange_symbol -> OrderbookData
        self.running = False
        self.subscriptions = set()  # set of (exchange, symbol)
        
        # Callbacks
        self.on_orderbook_update_callbacks = []
        
        # Analysis results
        self.market_summaries = {}  # symbol -> MarketSummary
        self.last_analysis_time = {}  # symbol -> timestamp
        
        self.logger = logger.bind(component="OrderbookAnalyzer")
        
    async def start(self) -> None:
        """Start orderbook analyzer."""
        if self.running:
            return
            
        self.running = True
        self.logger.info("Starting orderbook analyzer")
        
    async def stop(self) -> None:
        """Stop orderbook analyzer."""
        if not self.running:
            return
            
        self.running = False
        self.logger.info("Stopping orderbook analyzer")
        
        # Disconnect all WebSocket managers
        for ws_manager in self.ws_managers.values():
            await ws_manager.disconnect("Analyzer stopping")
            
    def register_orderbook_callback(self, callback: Callable[[OrderbookData], None]) -> None:
        """
        Register callback for orderbook updates.
        
        Args:
            callback: Function to call on orderbook update
        """
        self.on_orderbook_update_callbacks.append(callback)
        
    async def subscribe_orderbook(
        self,
        exchange_connector: Any,
        symbol: str,
        depth: int = 20
    ) -> bool:
        """
        Subscribe to orderbook updates for a symbol on an exchange.
        
        Args:
            exchange_connector: Exchange connector
            symbol: Trading symbol
            depth: Orderbook depth
            
        Returns:
            True if subscription successful
        """
        exchange_name = exchange_connector.__class__.__name__.replace('Connector', '').lower()
        
        # Check if already subscribed
        key = (exchange_name, symbol)
        if key in self.subscriptions:
            self.logger.info(f"Already subscribed to {exchange_name} {symbol} orderbook")
            return True
            
        # Get WebSocketManager for this exchange or create a new one
        if exchange_name not in self.ws_managers:
            self.ws_managers[exchange_name] = WebSocketManager(
                exchange_name=exchange_name,
                connection_id=f"orderbook_{exchange_name}"
            )
            
        ws_manager = self.ws_managers[exchange_name]
        
        # Define connect function for this subscription
        async def connect_orderbook():
            self.logger.info(f"Connecting to {exchange_name} {symbol} orderbook")
            return await exchange_connector.exchange.watch_order_book(symbol, depth)
            
        # Define message handler
        async def handle_orderbook_message(message):
            try:
                # Extract orderbook data
                timestamp = message.get('timestamp', time.time() * 1000) / 1000
                bids = [[Decimal(str(price)), Decimal(str(amount))] for price, amount in message.get('bids', [])]
                asks = [[Decimal(str(price)), Decimal(str(amount))] for price, amount in message.get('asks', [])]
                last_update_id = message.get('nonce')
                
                # Create OrderbookData
                orderbook = OrderbookData(
                    exchange=exchange_name,
                    symbol=symbol,
                    timestamp=timestamp,
                    bids=bids,
                    asks=asks,
                    last_update_id=last_update_id
                )
                
                # Store orderbook
                self.orderbooks[f"{exchange_name}_{symbol}"] = orderbook
                
                # Notify callbacks
                for callback in self.on_orderbook_update_callbacks:
                    try:
                        callback(orderbook)
                    except Exception as e:
                        self.logger.error(f"Error in orderbook callback: {e}")
                        
                # Analyze market data
                await self._analyze_market_data(symbol)
                
            except Exception as e:
                self.logger.error(f"Error processing orderbook: {e}")
                
        # Define heartbeat function
        async def send_heartbeat():
            # Most exchange websockets don't need manual heartbeats
            # This is just a placeholder
            pass
            
        # Start WebSocket connection
        await ws_manager.connect(
            connect_function=connect_orderbook,
            message_handler=handle_orderbook_message,
            heartbeat_function=send_heartbeat
        )
        
        # Add to subscriptions
        self.subscriptions.add(key)
        
        self.logger.info(f"Subscribed to {exchange_name} {symbol} orderbook")
        return True
        
    async def fetch_orderbook(
        self,
        exchange_connector: Any,
        symbol: str,
        depth: int = 20
    ) -> Optional[OrderbookData]:
        """
        Fetch orderbook data once (REST API).
        
        Args:
            exchange_connector: Exchange connector
            symbol: Trading symbol
            depth: Orderbook depth
            
        Returns:
            OrderbookData if successful, None otherwise
        """
        exchange_name = exchange_connector.__class__.__name__.replace('Connector', '').lower()
        
        try:
            self.logger.info(f"Fetching {exchange_name} {symbol} orderbook")
            
            # Fetch orderbook
            orderbook = await exchange_connector.get_orderbook(symbol, depth)
            
            # Extract data
            timestamp = orderbook.get('timestamp', time.time() * 1000) / 1000
            bids = [[Decimal(str(price)), Decimal(str(amount))] for price, amount in orderbook.get('bids', [])]
            asks = [[Decimal(str(price)), Decimal(str(amount))] for price, amount in orderbook.get('asks', [])]
            
            # Create OrderbookData
            orderbook_data = OrderbookData(
                exchange=exchange_name,
                symbol=symbol,
                timestamp=timestamp,
                bids=bids,
                asks=asks
            )
            
            # Store orderbook
            self.orderbooks[f"{exchange_name}_{symbol}"] = orderbook_data
            
            # Analyze market data
            await self._analyze_market_data(symbol)
            
            return orderbook_data
            
        except Exception as e:
            self.logger.error(f"Error fetching orderbook: {e}")
            return None
            
    async def _analyze_market_data(self, symbol: str) -> None:
        """
        Analyze market data for a symbol across all exchanges.
        
        Args:
            symbol: Trading symbol
        """
        # Get all orderbooks for this symbol
        symbol_orderbooks = []
        for key, orderbook in self.orderbooks.items():
            if orderbook.symbol == symbol:
                symbol_orderbooks.append(orderbook)
                
        if not symbol_orderbooks:
            return
            
        # Create market summary
        summary = self._create_market_summary(symbol, symbol_orderbooks)
        
        # Store summary
        self.market_summaries[symbol] = summary
        self.last_analysis_time[symbol] = time.time()
        
    def _create_market_summary(
        self,
        symbol: str,
        orderbooks: List[OrderbookData]
    ) -> Dict[str, Any]:
        """
        Create market summary from orderbooks.
        
        Args:
            symbol: Trading symbol
            orderbooks: List of orderbooks
            
        Returns:
            Market summary dictionary
        """
        if not orderbooks:
            return {}
            
        # Extract data
        best_bids = [ob.best_bid for ob in orderbooks if ob.best_bid is not None]
        best_asks = [ob.best_ask for ob in orderbooks if ob.best_ask is not None]
        
        if not best_bids or not best_asks:
            return {}
            
        # Calculate metrics
        global_best_bid = max(best_bids)
        global_best_ask = min(best_asks)
        global_midprice = (global_best_bid + global_best_ask) / Decimal('2')
        
        # Gather exchange data
        exchange_data = {}
        for ob in orderbooks:
            exchange_data[ob.exchange] = {
                'best_bid': float(ob.best_bid) if ob.best_bid else None,
                'best_ask': float(ob.best_ask) if ob.best_ask else None,
                'midprice': float(ob.midprice) if ob.midprice else None,
                'spread': float(ob.spread) if ob.spread else None,
                'spread_pct': float(ob.spread_pct) if ob.spread_pct else None
            }
            
        # Calculate liquidity
        bid_liquidity = Decimal('0')
        ask_liquidity = Decimal('0')
        bid_liquidity_quote = Decimal('0')
        ask_liquidity_quote = Decimal('0')
        
        for ob in orderbooks:
            b_liq, a_liq = ob.get_liquidity()
            b_liq_q, a_liq_q = ob.get_liquidity_in_quote()
            
            bid_liquidity += b_liq
            ask_liquidity += a_liq
            bid_liquidity_quote += b_liq_q
            ask_liquidity_quote += a_liq_q
            
        # Create summary
        base, quote = symbol.split('/')
        
        return {
            'symbol': symbol,
            'base': base,
            'quote': quote,
            'timestamp': time.time(),
            'datetime': datetime.now(timezone.utc).isoformat(),
            'exchange_count': len(orderbooks),
            'exchanges': list(exchange_data.keys()),
            'global_best_bid': float(global_best_bid),
            'global_best_ask': float(global_best_ask),
            'global_midprice': float(global_midprice),
            'global_spread': float(global_best_ask - global_best_bid),
            'global_spread_pct': float(((global_best_ask - global_best_bid) / global_midprice) * Decimal('100')),
            'exchange_data': exchange_data,
            'total_bid_liquidity': float(bid_liquidity),
            'total_ask_liquidity': float(ask_liquidity),
            'total_bid_liquidity_quote': float(bid_liquidity_quote),
            'total_ask_liquidity_quote': float(ask_liquidity_quote),
            'total_liquidity_base': float(bid_liquidity + ask_liquidity),
            'total_liquidity_quote': float(bid_liquidity_quote + ask_liquidity_quote)
        }
        
    def print_market_summary(self, symbol: str) -> None:
        """
        Print market summary for a symbol.
        
        Args:
            symbol: Trading symbol
        """
        if symbol not in self.market_summaries:
            print(f"No market summary available for {symbol}")
            return
            
        summary = self.market_summaries[symbol]
        base, quote = symbol.split('/')
        
        print(f"\n===== Market Summary for {symbol} =====")
        print(f"Time: {summary['datetime']}")
        print(f"Exchanges: {', '.join(summary['exchanges'])}")
        print(f"Global Best Bid: {summary['global_best_bid']:.8f} {quote}")
        print(f"Global Best Ask: {summary['global_best_ask']:.8f} {quote}")
        print(f"Global Mid Price: {summary['global_midprice']:.8f} {quote}")
        print(f"Global Spread: {summary['global_spread']:.8f} {quote} ({summary['global_spread_pct']:.4f}%)")
        print(f"Total Liquidity (1% depth):")
        print(f"  Bid: {summary['total_bid_liquidity']:.6f} {base} ({summary['total_bid_liquidity_quote']:.2f} {quote})")
        print(f"  Ask: {summary['total_ask_liquidity']:.6f} {base} ({summary['total_ask_liquidity_quote']:.2f} {quote})")
        print(f"  Total: {summary['total_liquidity_base']:.6f} {base} ({summary['total_liquidity_quote']:.2f} {quote})")
        
        print("\nExchange Comparison:")
        print(f"{'Exchange':<10} {'Bid':<12} {'Ask':<12} {'Spread':<12} {'Spread %':<10}")
        print("-" * 60)
        
        for exchange, data in summary['exchange_data'].items():
            print(f"{exchange:<10} {data['best_bid']:<12.8f} {data['best_ask']:<12.8f} "
                  f"{data['spread']:<12.8f} {data['spread_pct']:<10.4f}")
            
        print("=" * 60)
        
    def get_orderbook(self, exchange: str, symbol: str) -> Optional[OrderbookData]:
        """
        Get orderbook data for a specific exchange and symbol.
        
        Args:
            exchange: Exchange name
            symbol: Trading symbol
            
        Returns:
            OrderbookData if available, None otherwise
        """
        key = f"{exchange}_{symbol}"
        return self.orderbooks.get(key)
        
    def get_market_summary(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get market summary for a symbol.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            Market summary dictionary if available, None otherwise
        """
        return self.market_summaries.get(symbol)
        
    def get_best_bid_exchange(self, symbol: str) -> Tuple[Optional[str], Optional[Decimal]]:
        """
        Find exchange with best bid price.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            Tuple of (exchange_name, best_bid_price)
        """
        best_exchange = None
        best_price = None
        
        for key, orderbook in self.orderbooks.items():
            if orderbook.symbol == symbol and orderbook.best_bid:
                if best_price is None or orderbook.best_bid > best_price:
                    best_price = orderbook.best_bid
                    best_exchange = orderbook.exchange
                    
        return best_exchange, best_price
        
    def get_best_ask_exchange(self, symbol: str) -> Tuple[Optional[str], Optional[Decimal]]:
        """
        Find exchange with best ask price.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            Tuple of (exchange_name, best_ask_price)
        """
        best_exchange = None
        best_price = None
        
        for key, orderbook in self.orderbooks.items():
            if orderbook.symbol == symbol and orderbook.best_ask:
                if best_price is None or orderbook.best_ask < best_price:
                    best_price = orderbook.best_ask
                    best_exchange = orderbook.exchange
                    
        return best_exchange, best_price 