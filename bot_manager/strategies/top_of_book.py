"""
Top of Book Strategy - Inventory-based market making at best bid/offer

This strategy aims to be at best bid and/or offer to buy/sell out of excess inventory,
without crossing the spread and without self-matching. It uses hourly rates with
price bands to control execution timing.
"""

import asyncio
import json
import time
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Dict, List, Optional, Any, Tuple
import structlog

from bot_manager.strategies.base_strategy import BaseStrategy
from order_management.execution_engine import ExecutionEngine
from order_management.order_manager import OrderManager
from order_management.tracking import PositionManager
from exchanges.base_connector import OrderType, OrderSide, OrderStatus


class SideOption(str, Enum):
    """Side options for quoting."""
    BOTH = "both"
    BID = "bid"
    OFFER = "offer"


class HourlyRateEntry:
    """Represents a single hourly rate configuration entry."""
    
    def __init__(
        self,
        side: str,  # "bid" or "offer"
        max_min_price: Decimal,  # Max price for bid, Min price for offer
        hourly_rate: Decimal,
        rate_currency: str,  # "base" or "quote"
        drift_bps: float,
        timeout_seconds: int
    ):
        self.side = side
        self.max_min_price = max_min_price
        self.hourly_rate = hourly_rate
        self.rate_currency = rate_currency
        self.drift_bps = drift_bps
        self.timeout_seconds = timeout_seconds
        
        # Tracking state
        self.time_in_zone: float = 0.0  # Total time spent in valid price zone (hours)
        self.last_zone_entry: Optional[datetime] = None
        self.active_orders: Dict[str, str] = {}  # exchange -> order_id
        self.zone_entry_execution_snapshot: Optional[Decimal] = None  # Execution level when entering zone
        
        # Order tracking per exchange (like passive_quoting)
        self.order_placed_at: Dict[str, datetime] = {}  # exchange -> placement_time
        self.order_placed_price: Dict[str, Decimal] = {}  # exchange -> placement_price
        self.last_midpoint_per_exchange: Dict[str, Decimal] = {}  # exchange -> last_midpoint
        
    def __repr__(self):
        return f"HourlyRateEntry(side={self.side}, max_min_price={self.max_min_price}, hourly_rate={self.hourly_rate}, rate_currency={self.rate_currency}, drift_bps={self.drift_bps}, timeout_seconds={self.timeout_seconds})"


class TopOfBookStrategy(BaseStrategy):
    """
    Top of Book strategy for inventory-based market making.
    
    Features:
    - Quotes at best bid/offer to manage excess inventory
    - Uses percentage of excess inventory to determine total target
    - Hourly rates with price bands control execution timing
    - Never crosses spread or self-matches
    - Supports both bid and offer sides
    """
    
    def __init__(
        self,
        instance_id: str,
        symbol: str,
        exchanges: List[str],
        config: Dict[str, Any],
        redis_url: str = "redis://localhost:6379"
    ):
        super().__init__(instance_id, symbol, exchanges, config)
        
        # Strategy-specific attributes
        self.base_coin: str = ""
        self.quote_coin: str = ""
        self.start_time: Optional[datetime] = None  # User inputted start time for inventory calculation
        self.sides: SideOption = SideOption.BOTH
        self.target_inventory: Decimal = Decimal('0')
        self.excess_inventory_percentage: float = 0.0
        self.spread_bps: int = 0
        self.taker_check: bool = True
        
        # Hourly rate entries (separate for bid and offer)
        self.bid_hourly_rates: List[HourlyRateEntry] = []
        self.offer_hourly_rates: List[HourlyRateEntry] = []
        
        # Execution components
        self.position_manager = None
        self.execution_engine = None
        self.order_manager = None
        
        # Redis connection
        self.redis_url = redis_url
        self.redis_client = None
        
        # Cooldown tracking
        self.in_cooldown = False
        self.cooldown_until: Optional[datetime] = None
        
        # Position tracking
        self.current_inventory: Decimal = Decimal('0')
        self.inventory_price: Optional[Decimal] = None
        self.excess_inventory: Decimal = Decimal('0')
        self.target_excess_amount: Decimal = Decimal('0')  # Amount to trade based on percentage
        
        # Live position data (updated incrementally in O(1))
        self.live_position_data = {
            'inventory_price': None,
            'realized_pnl': 0.0,
            'unrealized_pnl': 0.0,
            'last_updated': None
        }
        
        # O(1) position tracking state
        self._position_lots = {
            'long_lots': [],  # [(size, price, timestamp), ...] - for FIFO/LIFO
            'short_lots': [],  # [(size, price, timestamp), ...]
            'total_realized_pnl': 0.0,
            'position_size': 0.0,  # For average cost method
            'position_cost': 0.0,  # For average cost method
            'last_trade_id': None,  # Track last processed trade to avoid duplicates
            'initialized': False
        }
        
        # Main task
        self.main_task: Optional[asyncio.Task] = None
        self.cooldown_task: Optional[asyncio.Task] = None
        self.position_calc_task: Optional[asyncio.Task] = None
        
        # Performance tracking
        self.orders_placed = 0
        self.orders_filled = 0
        self.orders_cancelled_timeout = 0
        self.orders_cancelled_drift = 0
        self.orders_skipped_inventory_protection = 0  # Track orders skipped due to inventory price protection
        self.total_executed_base: Decimal = Decimal('0')
        self.total_executed_quote: Decimal = Decimal('0')
        
    async def _validate_config(self) -> None:
        """Validate Top of Book configuration."""
        required_fields = [
            'start_time', 'sides', 'target_inventory', 'excess_inventory_percentage',
            'spread_bps', 'taker_check', 'accounting_method'
        ]
        
        for field in required_fields:
            if field not in self.config:
                raise ValueError(f"Missing required config field: {field}")
        
        # Parse basic configuration with robust start_time handling
        start_time_str = self.config.get('start_time')
        self.logger.info(f"DEBUG: Raw start_time from config: '{start_time_str}' (type: {type(start_time_str)})")
        
        if start_time_str:
            try:
                if isinstance(start_time_str, str):
                    self.logger.info(f"DEBUG: Parsing string start_time: '{start_time_str}'")
                    
                    if 'T' in start_time_str and start_time_str.endswith('Z'):
                        self.logger.info("DEBUG: Detected Z-ending ISO format")
                        self.start_time = datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))
                    elif 'T' in start_time_str and '+' in start_time_str:
                        self.logger.info("DEBUG: Detected ISO format with timezone")
                        self.start_time = datetime.fromisoformat(start_time_str)
                    elif 'T' in start_time_str and len(start_time_str) == 16:
                        # Handle datetime-local format "2025-06-18T15:27" (exactly 16 characters)
                        self.logger.info(f"DEBUG: Detected datetime-local format (len={len(start_time_str)})")
                        naive_dt = datetime.strptime(start_time_str, "%Y-%m-%dT%H:%M")
                        self.start_time = naive_dt.replace(tzinfo=timezone.utc)
                        self.logger.info(f"DEBUG: Parsed datetime-local to: {self.start_time}")
                    elif 'T' in start_time_str:
                        # Handle other ISO format without timezone info
                        self.logger.info("DEBUG: Detected other ISO format")
                        naive_dt = datetime.fromisoformat(start_time_str)
                        self.start_time = naive_dt.replace(tzinfo=timezone.utc)
                    else:
                        # Handle formats like "2025-06-18, 01:27 PM"
                        self.logger.info("DEBUG: Trying non-ISO formats")
                        try:
                            # Try "2025-06-18, 01:27 PM" format
                            naive_dt = datetime.strptime(start_time_str, "%Y-%m-%d, %I:%M %p")
                            self.start_time = naive_dt.replace(tzinfo=timezone.utc)
                            self.logger.info("DEBUG: Parsed PM/AM format")
                        except ValueError:
                            # Fallback to standard format
                            naive_dt = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M")
                            self.start_time = naive_dt.replace(tzinfo=timezone.utc)
                            self.logger.info("DEBUG: Parsed standard format")
                elif isinstance(start_time_str, datetime):
                    self.logger.info(f"DEBUG: start_time is already a datetime object: {start_time_str}")
                    if start_time_str.tzinfo is None:
                        self.start_time = start_time_str.replace(tzinfo=timezone.utc)
                    else:
                        self.start_time = start_time_str.astimezone(timezone.utc)
                else:
                    self.logger.error(f"DEBUG: Unexpected start_time type: {type(start_time_str)}")
                    self.start_time = datetime.now(timezone.utc)
            except Exception as e:
                self.logger.error(f"Could not parse start_time '{start_time_str}': {e}")
                self.logger.error(f"DEBUG: Exception type: {type(e).__name__}")
                import traceback
                self.logger.error(f"DEBUG: Traceback: {traceback.format_exc()}")
                self.start_time = datetime.now(timezone.utc)
        else:
            self.logger.warning("DEBUG: No start_time in config, using current time")
            self.start_time = datetime.now(timezone.utc)
            
        self.logger.info(f"Parsed start_time: {self.start_time} UTC")
        
        # Parse other configuration fields
        self.sides = SideOption(self.config['sides'])
        self.target_inventory = Decimal(str(self.config['target_inventory']))
        self.excess_inventory_percentage = float(self.config['excess_inventory_percentage'])
        self.spread_bps = int(self.config['spread_bps'])
        self.taker_check = bool(self.config['taker_check'])
        self.accounting_method = self.config.get('accounting_method', 'FIFO')  # Default to FIFO
        
        # Parse symbol
        if '/' in self.symbol:
            self.base_coin, self.quote_coin = self.symbol.split('/')
        else:
            raise ValueError(f"Invalid symbol format: {self.symbol}")
        
        # Validate hourly rates
        bid_rates = self.config.get('bid_hourly_rates', [])
        offer_rates = self.config.get('offer_hourly_rates', [])
        
        if self.sides in [SideOption.BOTH, SideOption.BID] and not bid_rates:
            raise ValueError("Bid hourly rates required when bid side is enabled")
        
        if self.sides in [SideOption.BOTH, SideOption.OFFER] and not offer_rates:
            raise ValueError("Offer hourly rates required when offer side is enabled")
        
        # Parse bid hourly rates
        for rate_config in bid_rates:
            required_rate_fields = ['max_price', 'hourly_rate', 'rate_currency', 'drift_bps', 'timeout_seconds']
            for field in required_rate_fields:
                if field not in rate_config:
                    raise ValueError(f"Missing required field '{field}' in bid hourly rate")
            
            rate_entry = HourlyRateEntry(
                side="bid",
                max_min_price=Decimal(str(rate_config['max_price'])),
                hourly_rate=Decimal(str(rate_config['hourly_rate'])),
                rate_currency=rate_config['rate_currency'],
                drift_bps=float(rate_config['drift_bps']),
                timeout_seconds=int(rate_config['timeout_seconds'])
            )
            self.bid_hourly_rates.append(rate_entry)
        
        # Parse offer hourly rates
        for rate_config in offer_rates:
            required_rate_fields = ['min_price', 'hourly_rate', 'rate_currency', 'drift_bps', 'timeout_seconds']
            for field in required_rate_fields:
                if field not in rate_config:
                    raise ValueError(f"Missing required field '{field}' in offer hourly rate")
            
            rate_entry = HourlyRateEntry(
                side="offer",
                max_min_price=Decimal(str(rate_config['min_price'])),
                hourly_rate=Decimal(str(rate_config['hourly_rate'])),
                rate_currency=rate_config['rate_currency'],
                drift_bps=float(rate_config['drift_bps']),
                timeout_seconds=int(rate_config['timeout_seconds'])
            )
            self.offer_hourly_rates.append(rate_entry)
        
        self.logger.info(f"Validated config with {len(self.bid_hourly_rates)} bid rates and {len(self.offer_hourly_rates)} offer rates")
        
    async def _start_strategy(self) -> None:
        """Start the top of book strategy."""
        self.logger.info("Starting top of book strategy")
        
        # Initialize execution components
        await self._initialize_execution_components()
        
        # Calculate initial inventory and position
        await self._update_inventory_and_position()
        
        # Start cooldown monitoring
        self.cooldown_task = asyncio.create_task(self._monitor_cooldown_signals())
        
        # Initialize position tracking from historical data
        await self._initialize_position_tracking()
        
        # Ensure live data is initialized even without trades
        await self._update_live_data_from_position_state()
        
        # Start main strategy loop
        self.main_task = asyncio.create_task(self._strategy_loop())
        
        # Publish initial performance data
        await self._publish_performance_data()
        
    async def _stop_strategy(self) -> None:
        """Stop the top of book strategy."""
        self.logger.info("Stopping top of book strategy")
        
        if self.main_task:
            self.main_task.cancel()
            try:
                await self.main_task
            except asyncio.CancelledError:
                pass
        
        if hasattr(self, 'cooldown_task') and self.cooldown_task:
            self.cooldown_task.cancel()
            try:
                await self.cooldown_task
            except asyncio.CancelledError:
                pass
        
        # Position tracking cleanup is automatic - no background tasks to cancel
        
        # Cancel all active orders
        await self._cancel_all_strategy_orders()
        
        # Clean up components
        if self.redis_client:
            # Remove performance data from Redis
            try:
                await self.redis_client.delete(self.performance_key)
            except Exception as e:
                self.logger.error(f"Error cleaning up Redis performance data: {e}")
            
            await self.redis_client.aclose()
            
    async def _initialize_execution_components(self) -> None:
        """Initialize execution components."""
        # Initialize position manager
        self.position_manager = PositionManager()
        await self.position_manager.start()
        
        # Set up trade repository for position calculations
        try:
            from database import get_session
            from database.repositories import TradeRepository
            
            # Get database session and set up trade repository
            async for session in get_session():
                trade_repository = TradeRepository(session)
                self.position_manager.set_trade_repository(trade_repository)
                self.logger.info("Trade repository set up for position calculations")
                break  # Only need the first session
        except Exception as e:
            self.logger.warning(f"Could not set up trade repository: {e}")
        
        # Initialize order manager
        self.order_manager = OrderManager(
            exchange_connectors=self.exchange_connectors,
            position_manager=self.position_manager
        )
        
        # Initialize execution engine
        self.execution_engine = ExecutionEngine(
            order_manager=self.order_manager,
            exchange_connectors=self.exchange_connectors
        )
        
        await self.execution_engine.start()
        
        # Initialize Redis client
        import redis.asyncio as redis
        self.redis_client = redis.from_url(self.redis_url)
        
        # Set up Redis key for performance data
        self.performance_key = f"bot_performance:{self.instance_id}"
        
        # Wait for initial orderbook data
        await asyncio.sleep(2)
        
    async def _monitor_cooldown_signals(self) -> None:
        """Monitor Redis for cooldown signals from aggressive TWAP strategies."""
        try:
            cooldown_key = f"strategy_cooldown:{self.symbol}"
            self.logger.info(f"Starting cooldown monitoring for key: {cooldown_key}")
            
            while self.running:
                try:
                    # Check for cooldown signal
                    cooldown_data = await self.redis_client.get(cooldown_key)
                    
                    if cooldown_data:
                        try:
                            import json
                            data = json.loads(cooldown_data)
                            cooldown_until = datetime.fromisoformat(data['cooldown_until'])
                            
                            # Check if cooldown is still active
                            if cooldown_until > datetime.now(timezone.utc):
                                if not self.in_cooldown:
                                    self.logger.info(f"Entering cooldown from {data.get('strategy', 'unknown')} until {cooldown_until}")
                                    await self._cancel_all_strategy_orders()
                                self.in_cooldown = True
                                self.cooldown_until = cooldown_until
                            else:
                                if self.in_cooldown:
                                    self.logger.info("Cooldown period ended, resuming top of book strategy")
                                self.in_cooldown = False
                                self.cooldown_until = None
                        except Exception as e:
                            self.logger.error(f"Error processing cooldown signal: {e}")
                    else:
                        if self.in_cooldown:
                            self.logger.info("Cooldown signal cleared, resuming top of book strategy")
                        self.in_cooldown = False
                        self.cooldown_until = None
                    
                    await asyncio.sleep(1)  # Check every second
                    
                except asyncio.CancelledError:
                    self.logger.info("Cooldown monitoring cancelled")
                    break
                except Exception as e:
                    self.logger.error(f"Error in cooldown monitoring: {e}")
                    await asyncio.sleep(5)
                    
        except Exception as e:
            self.logger.error(f"Error starting cooldown monitoring: {e}")
        
    async def _strategy_loop(self) -> None:
        """Main strategy execution loop."""
        self.logger.info("Starting strategy loop")
        
        while self.running:
            try:
                # Check if in cooldown
                if self.in_cooldown and self.cooldown_until:
                    current_time = datetime.now(timezone.utc)
                    if current_time < self.cooldown_until:
                        await asyncio.sleep(1)
                        continue
                    else:
                        self.in_cooldown = False
                        self.cooldown_until = None
                        self.logger.info("Cooldown period ended, resuming top of book strategy")
                
                # Clean up any orphaned orders first
                self._cleanup_orphaned_rate_entry_orders()
                
                # Update inventory and position from database
                await self._update_inventory_and_position()
                
                # Get current midpoint for live data updates
                current_midpoint = await self._get_current_midpoint()
                # Always update live position data, with or without current price
                if current_midpoint:
                    await self._update_live_data_from_position_state(float(current_midpoint))
                else:
                    await self._update_live_data_from_position_state()
                
                # Process bid side if enabled
                if self.sides in [SideOption.BOTH, SideOption.BID]:
                    await self._process_bid_side()
                
                # Process offer side if enabled
                if self.sides in [SideOption.BOTH, SideOption.OFFER]:
                    await self._process_offer_side()
                
                # Wait before next iteration
                await asyncio.sleep(1)
                
            except Exception as e:
                self.logger.error(f"Error in strategy loop: {e}")
                await asyncio.sleep(5)
                
    async def _update_inventory_and_position(self) -> None:
        """Update current inventory and position from database."""
        try:
            # Calculate current inventory from risk_trades table
            self.current_inventory = await self._calculate_current_inventory()
            
            # Calculate inventory price
            await self._calculate_inventory_price()
            
            # Calculate excess inventory
            self.excess_inventory = self.current_inventory - self.target_inventory
            
            # Calculate target amount to trade (percentage of excess)
            self.target_excess_amount = abs(self.excess_inventory) * Decimal(str(self.excess_inventory_percentage / 100))
            
            self.logger.debug(
                f"Inventory update - Current: {self.current_inventory}, "
                f"Target: {self.target_inventory}, Excess: {self.excess_inventory}, "
                f"Target amount: {self.target_excess_amount}"
            )
            
        except Exception as e:
            self.logger.error(f"Error updating inventory: {e}")
            
    async def _calculate_current_inventory(self) -> Decimal:
        """Calculate current inventory from risk_trades table."""
        try:
            from database import get_session
            from sqlalchemy import text
            
            async for session in get_session():
                try:
                    # Begin fresh transaction
                    await session.begin()
                    
                    # Get the start time for inventory calculations
                    start_time_for_db = self.get_inventory_start_time()
                    # Convert timezone-aware datetime to timezone-naive for database query
                    if start_time_for_db.tzinfo is not None:
                        start_time_for_db = start_time_for_db.astimezone(timezone.utc).replace(tzinfo=None)
                    
                    self.logger.info(f"🔍 Calculating inventory for {self.symbol} since {start_time_for_db} using risk_trades table")
                    
                    # Query risk_trades table including perpetual variants
                    # Get individual trades to properly account for fees
                    query = text("""
                        SELECT side, amount, fee_cost, fee_currency
                        FROM risk_trades
                        WHERE (
                            symbol = :symbol
                            OR symbol = :symbol_perp
                            OR symbol = :symbol_usdt
                            OR symbol = :symbol_usdc
                            OR symbol LIKE :symbol_pattern
                        )
                        AND timestamp >= :start_time
                        ORDER BY timestamp ASC
                    """)
                    
                    # Prepare symbol variants
                    base_symbol = self.symbol  # e.g., BERA/USDT
                    symbol_perp = f"{base_symbol}-PERP"  # e.g., BERA/USDT-PERP
                    symbol_usdt = f"{base_symbol}:USDT"  # e.g., BERA/USDT:USDT
                    symbol_usdc = base_symbol.replace('/USDT', '/USDC:USDC')  # e.g., BERA/USDC:USDC
                    symbol_pattern = f"{base_symbol.split('/')[0]}/%"  # e.g., BERA/%
                    
                    result = await session.execute(
                        query,
                        {
                            "symbol": base_symbol,
                            "symbol_perp": symbol_perp,
                            "symbol_usdt": symbol_usdt,
                            "symbol_usdc": symbol_usdc,
                            "symbol_pattern": symbol_pattern,
                            "start_time": start_time_for_db
                        }
                    )
                    trades = result.fetchall()
                    
                    # Calculate net inventory accounting for fees
                    inventory = Decimal('0')
                    fees_in_base = Decimal('0')
                    
                    for trade in trades:
                        amount = Decimal(str(trade.amount))
                        
                        # Add/subtract trade amount based on side
                        if trade.side.lower() == 'buy':
                            inventory += amount
                        else:  # sell
                            inventory -= amount
                        
                        # Account for fees paid in base coin
                        if trade.fee_cost and trade.fee_currency:
                            fee_amount = Decimal(str(trade.fee_cost))
                            fee_currency = trade.fee_currency.upper()
                            
                            # Check if fee is in base coin
                            if fee_currency == self.base_coin.upper() or fee_currency == self.base_coin:
                                # Subtract fees from inventory (fees reduce our net position)
                                inventory -= fee_amount
                                fees_in_base += fee_amount
                    
                    self.logger.info(f"📊 Found {len(trades)} trades in risk_trades for {self.symbol} (including perps) since {start_time_for_db}")
                    self.logger.info(f"📊 Net inventory: {inventory} {self.base_coin} (fees paid in {self.base_coin}: {fees_in_base})")
                    
                    # Commit the read-only transaction
                    await session.commit()
                    return inventory
                    
                except Exception as e:
                    await session.rollback()
                    self.logger.error(f"Error in inventory calculation: {e}")
                    raise e
                finally:
                    await session.close()
                
        except Exception as e:
            self.logger.error(f"Error calculating inventory from risk_trades: {e}")
            return Decimal('0')
            
    async def _calculate_inventory_price(self) -> None:
        """Calculate inventory price using the O(1) position tracking."""
        try:
            if not self._position_lots['initialized']:
                self.inventory_price = None
                self.logger.info("📊 Position tracking not initialized yet")
                return
                    
            # Get inventory price from O(1) calculation
            if self.accounting_method == 'AVERAGE_COST':
                inventory_price = self._get_average_cost_inventory_price()
            else:  # FIFO or LIFO
                inventory_price = self._get_lot_based_inventory_price()
            
            if inventory_price > 0:
                self.inventory_price = Decimal(str(inventory_price))
                self.logger.debug(f"📊 Inventory price ({self.accounting_method}): {self.inventory_price} {self.quote_coin}")
            else:
                self.inventory_price = None
                self.logger.debug("📊 No net inventory, cannot calculate price")
                    
        except Exception as e:
            self.logger.error(f"Error calculating inventory price: {e}")
            self.inventory_price = None
            
    async def _process_bid_side(self) -> None:
        """Process bid side quoting logic."""
        # Check if we should be quoting bid side based on inventory
        if self.excess_inventory >= 0:
            # We're long or neutral, no need to buy more
            await self._cancel_side_orders("bid")
            return
            
        # We're short, should quote bid to buy
        current_price = await self._get_current_midpoint()
        if not current_price:
            return
            
        # Find active bid rate entry
        active_rate = self._get_active_rate_entry("bid", current_price)
        if not active_rate:
            await self._cancel_side_orders("bid")
            return
            
        # Update time in zone
        self._update_time_in_zone(active_rate, current_price)
        
        # Check if we should place/maintain orders
        await self._process_rate_entry(active_rate, current_price)
        
    async def _process_offer_side(self) -> None:
        """Process offer side quoting logic."""
        # Check if we should be quoting offer side based on inventory
        if self.excess_inventory <= 0:
            # We're short or neutral, no need to sell more
            await self._cancel_side_orders("offer")
            return
            
        # We're long, should quote offer to sell
        current_price = await self._get_current_midpoint()
        if not current_price:
            return
            
        # Find active offer rate entry
        active_rate = self._get_active_rate_entry("offer", current_price)
        if not active_rate:
            await self._cancel_side_orders("offer")
            return
            
        # Update time in zone
        self._update_time_in_zone(active_rate, current_price)
        
        # Check if we should place/maintain orders
        await self._process_rate_entry(active_rate, current_price)
        
    def _get_active_rate_entry(self, side: str, current_price: Decimal) -> Optional[HourlyRateEntry]:
        """Get the active hourly rate entry for the given side and price."""
        rates = self.bid_hourly_rates if side == "bid" else self.offer_hourly_rates
        
        # Find all matching rates and select the most appropriate one
        matching_rates = []
        
        for rate_entry in rates:
            if side == "bid":
                # For bid: quote if current price is below max price
                if current_price <= rate_entry.max_min_price:
                    matching_rates.append(rate_entry)
            else:  # offer
                # For offer: quote if current price is above min price
                if current_price >= rate_entry.max_min_price:
                    matching_rates.append(rate_entry)
        
        if not matching_rates:
            return None
        
        # Select the most appropriate rate based on price proximity
        if side == "bid":
            # For bid: choose the rate with the LOWEST max_price that still allows quoting
            # This gives the tightest constraint and most specific rate for current price
            best_rate = min(matching_rates, key=lambda r: r.max_min_price)
        else:  # offer
            # For offer: choose the rate with the HIGHEST min_price that still allows quoting
            # This gives the tightest constraint and most specific rate for current price
            best_rate = max(matching_rates, key=lambda r: r.max_min_price)
        
        self.logger.debug(
            f"Selected {side} rate for price {current_price}: "
            f"{'max_price' if side == 'bid' else 'min_price'}={best_rate.max_min_price}, "
            f"hourly_rate={best_rate.hourly_rate} "
            f"(from {len(matching_rates)} matching rates)"
        )
        
        return best_rate
        
    def _update_time_in_zone(self, rate_entry: HourlyRateEntry, current_price: Decimal) -> None:
        """Update time spent in valid price zone for the rate entry."""
        now = datetime.now(timezone.utc)
        
        # Reset time tracking for other rates when switching to this rate
        self._reset_inactive_rate_time_tracking(rate_entry, now)
        
        # Check if we should be tracking time based on inventory price and spread
        should_track_time = self._should_track_time_for_side(rate_entry.side, current_price)
        
        if rate_entry.last_zone_entry is None and should_track_time:
            # Fresh entry into this price zone - reset everything for a clean start
            # This ensures we don't carry over time from previous entries into this zone
            rate_entry.time_in_zone = 0.0  # Reset time to start fresh
            rate_entry.zone_entry_execution_snapshot = None  # Will be set below
            
            if self.runtime_start_time and now >= self.runtime_start_time:
                rate_entry.last_zone_entry = now  # Use current time, not runtime start
                
                # Take a snapshot of current execution levels when entering this zone
                # This allows us to track execution progress only since entering this specific zone
                asyncio.create_task(self._set_zone_entry_execution_snapshot(rate_entry))
                
                self.logger.info(f"🔄 FRESH ENTRY into {rate_entry.side} rate zone (price: {rate_entry.max_min_price}) - resetting time and execution tracking")
            else:
                # Strategy runtime hasn't started yet, don't track time
                return
        else:
            if should_track_time:
                # Continue tracking time in current zone
                elapsed_hours = (now - rate_entry.last_zone_entry).total_seconds() / 3600
                
                # Only add positive elapsed time (in case of clock adjustments)
                if elapsed_hours > 0:
                    rate_entry.time_in_zone += elapsed_hours
                    self.logger.debug(f"Added {elapsed_hours:.4f} hours to {rate_entry.side} rate zone, total: {rate_entry.time_in_zone:.4f}h")
                
                rate_entry.last_zone_entry = now
            else:
                return
            
        self.logger.debug(
            f"Time in zone updated for {rate_entry.side} rate (price: {rate_entry.max_min_price}): {rate_entry.time_in_zone:.4f} hours "
            f"(zone entry: {rate_entry.last_zone_entry})"
        )
        
    def _should_track_time_for_side(self, side: str, current_price: Decimal) -> bool:
        """
        Determine if we should track time for the given side based on current price vs inventory price.
        
        For bid (buy) side: Track time when current price is BELOW inventory price minus spread
        (we want to buy below our cost basis)
        
        For offer (sell) side: Track time when current price is ABOVE inventory price plus spread  
        (we want to sell above our cost basis)
        """
        if not self.inventory_price:
            # No inventory price available, don't track time
            return False
            
        spread_decimal = Decimal(str(self.spread_bps / 10000))
        
        if side == "bid":
            # For bid: track time when current price is below inventory price minus spread
            # This means we can buy below our cost basis (profitable)
            threshold_price = self.inventory_price * (Decimal('1') - spread_decimal)
            return current_price <= threshold_price
        else:  # offer
            # For offer: track time when current price is above inventory price plus spread
            # This means we can sell above our cost basis (profitable)
            threshold_price = self.inventory_price * (Decimal('1') + spread_decimal)
            return current_price >= threshold_price
        
    async def _set_zone_entry_execution_snapshot(self, rate_entry: HourlyRateEntry) -> None:
        """Set execution snapshot when entering a zone to track progress only since zone entry."""
        try:
            executed_amounts = await self._calculate_executed_since_runtime_start()
            
            if rate_entry.side == "bid":
                rate_entry.zone_entry_execution_snapshot = executed_amounts["bid_executed"]
            else:  # offer
                rate_entry.zone_entry_execution_snapshot = executed_amounts["offer_executed"]
                
            self.logger.info(
                f"📊 Set execution snapshot for {rate_entry.side} zone (price: {rate_entry.max_min_price}): "
                f"{rate_entry.zone_entry_execution_snapshot}"
            )
        except Exception as e:
            self.logger.error(f"Error setting zone entry execution snapshot: {e}")
            rate_entry.zone_entry_execution_snapshot = Decimal('0')
        
    def _reset_inactive_rate_time_tracking(self, active_rate: HourlyRateEntry, current_time: datetime) -> None:
        """Reset time tracking for rates that are not currently active."""
        # Get all rates for the same side
        rates = self.bid_hourly_rates if active_rate.side == "bid" else self.offer_hourly_rates
        
        for rate_entry in rates:
            if rate_entry != active_rate and rate_entry.last_zone_entry is not None:
                # This rate was previously active but is no longer the selected rate
                # Log the final state before resetting
                self.logger.info(
                    f"🔄 EXITING {rate_entry.side} rate zone (price: {rate_entry.max_min_price}): "
                    f"final time: {rate_entry.time_in_zone:.4f}h"
                )
                
                # Complete reset when leaving a zone - no time accumulation across entries
                rate_entry.last_zone_entry = None
                rate_entry.time_in_zone = 0.0  # Reset time completely
                rate_entry.zone_entry_execution_snapshot = None  # Reset execution snapshot
                
                # Cancel any active orders for this inactive rate
                if rate_entry.active_orders:
                    self.logger.info(f"Cancelling orders for inactive {rate_entry.side} rate (price: {rate_entry.max_min_price})")
                    # Schedule cancellation in the next iteration to avoid blocking
                    asyncio.create_task(self._cancel_rate_entry_orders(rate_entry))
        
    async def _process_rate_entry(self, rate_entry: HourlyRateEntry, current_price: Decimal) -> None:
        """Process a specific hourly rate entry."""
        # Calculate how much we should have executed by now based on time in THIS zone
        target_executed_in_zone = rate_entry.hourly_rate * Decimal(str(rate_entry.time_in_zone))
        
        # Get actual executed amounts since entering THIS specific zone
        if rate_entry.zone_entry_execution_snapshot is not None:
            # Get current total execution
            current_executed_amounts = await self._calculate_executed_since_runtime_start()
            
            # Determine which side we're processing
            if rate_entry.side == "bid":
                current_total_executed = current_executed_amounts["bid_executed"]
            else:  # offer
                current_total_executed = current_executed_amounts["offer_executed"]
            
            # Calculate execution progress since entering this zone
            actual_executed_in_zone = current_total_executed - rate_entry.zone_entry_execution_snapshot
        else:
            # Fallback if snapshot not set yet (shouldn't happen in normal operation)
            self.logger.warning(f"Zone entry execution snapshot not set for {rate_entry.side} rate, using zero")
            actual_executed_in_zone = Decimal('0')
        
        # Don't exceed our overall target (percentage of excess inventory)
        target_executed_in_zone = min(target_executed_in_zone, self.target_excess_amount)
        
        # Calculate how much more we need to execute in this zone
        remaining_to_execute = target_executed_in_zone - actual_executed_in_zone
        
        if remaining_to_execute <= Decimal('0'):
            # Nothing more to execute in this zone, cancel orders
            await self._cancel_rate_entry_orders(rate_entry)
            self.logger.debug(
                f"No more execution needed for {rate_entry.side} zone (price: {rate_entry.max_min_price}): "
                f"target_in_zone={target_executed_in_zone}, actual_in_zone={actual_executed_in_zone}, "
                f"time_in_zone={rate_entry.time_in_zone:.4f}h"
            )
            return
            
        self.logger.debug(
            f"Processing {rate_entry.side} rate entry (price: {rate_entry.max_min_price}): "
            f"time_in_zone={rate_entry.time_in_zone:.4f}h, "
            f"target_in_zone={target_executed_in_zone}, actual_in_zone={actual_executed_in_zone}, "
            f"remaining={remaining_to_execute}"
        )
        
        # Check if we need to place new orders
        await self._manage_rate_entry_orders(rate_entry, remaining_to_execute, current_price)
        
    async def _manage_rate_entry_orders(
        self, 
        rate_entry: HourlyRateEntry, 
        size: Decimal, 
        current_price: Decimal
    ) -> None:
        """Manage orders for a specific rate entry."""
        # Don't place new orders during cooldown
        if self.in_cooldown:
            return
            
        # Calculate quote price
        quote_price = await self._calculate_quote_price(rate_entry.side, current_price)
        if not quote_price:
            # Quote price calculation returned None - likely due to inventory protection
            self.logger.debug(
                f"No quote price available for {rate_entry.side} side - "
                f"likely due to inventory protection or market conditions"
            )
            return
            
        # Check for timeouts and drift
        await self._check_rate_entry_timeouts_and_drift(rate_entry, current_price)
        
        # Place orders on exchanges that don't have active orders
        for exchange in self.exchanges:
            if exchange not in rate_entry.active_orders:
                await self._place_rate_entry_order(rate_entry, exchange, size, quote_price)
                
    async def _calculate_quote_price(self, side: str, current_price: Decimal) -> Optional[Decimal]:
        """Calculate the quote price for the given side."""
        try:
            # Get best bid/offer for each exchange
            best_prices = []
            
            for exchange in self.exchanges:
                orderbook = await self._get_exchange_orderbook(exchange)
                if not orderbook:
                    continue
                    
                if side == "bid":
                    # For bid: quote at best bid + 1 tick
                    if orderbook.get('bids'):
                        best_bid = Decimal(str(orderbook['bids'][0][0]))
                        tick_size = self._get_tick_size(exchange)
                        quote_price = best_bid + tick_size
                        best_prices.append(quote_price)
                else:  # offer
                    # For offer: quote at best offer - 1 tick
                    if orderbook.get('asks'):
                        best_offer = Decimal(str(orderbook['asks'][0][0]))
                        tick_size = self._get_tick_size(exchange)
                        quote_price = best_offer - tick_size
                        best_prices.append(quote_price)
            
            if not best_prices:
                return None
                
            # Use the most competitive price (highest for bid, lowest for offer)
            if side == "bid":
                quote_price = max(best_prices)
            else:
                quote_price = min(best_prices)
                
            # Taker check - ensure we don't cross the spread
            if self.taker_check:
                quote_price = await self._apply_taker_check(quote_price, side)
            
            # HARD RULE: Never sell below inventory price or buy above inventory price
            # This overrides all other pricing logic including taker checks and market competition
            # Note: If user enters negative spread_bps, this allows buying above or selling below inventory price
            if self.inventory_price:
                spread_decimal = Decimal(str(self.spread_bps / 10000))
                
                if side == "bid":
                    # Never buy above inventory price (minus spread)
                    max_bid_price = self.inventory_price * (Decimal('1') - spread_decimal)
                    if quote_price > max_bid_price:
                        self.logger.info(
                            f"🛡️ INVENTORY PROTECTION: Bid price {quote_price} would exceed max allowed {max_bid_price} "
                            f"(inventory price {self.inventory_price} - {self.spread_bps} bps spread). "
                            f"Skipping bid order to avoid buying above inventory price."
                        )
                        self.orders_skipped_inventory_protection += 1
                        return None  # Don't place order if it would buy above inventory price
                else:  # offer
                    # Never sell below inventory price (plus spread)
                    min_offer_price = self.inventory_price * (Decimal('1') + spread_decimal)
                    if quote_price < min_offer_price:
                        self.logger.info(
                            f"🛡️ INVENTORY PROTECTION: Offer price {quote_price} would be below min allowed {min_offer_price} "
                            f"(inventory price {self.inventory_price} + {self.spread_bps} bps spread). "
                            f"Skipping offer order to avoid selling below inventory price."
                        )
                        self.orders_skipped_inventory_protection += 1
                        return None  # Don't place order if it would sell below inventory price
                
            return quote_price
            
        except Exception as e:
            self.logger.error(f"Error calculating quote price: {e}")
            return None
            
    async def _apply_taker_check(self, quote_price: Decimal, side: str) -> Decimal:
        """Apply taker check to ensure we don't cross the spread."""
        try:
            for exchange in self.exchanges:
                orderbook = await self._get_exchange_orderbook(exchange)
                if not orderbook:
                    continue
                    
                if side == "bid" and orderbook.get('asks'):
                    # For bid: don't exceed best offer
                    best_offer = Decimal(str(orderbook['asks'][0][0]))
                    tick_size = self._get_tick_size(exchange)
                    max_bid = best_offer - tick_size
                    quote_price = min(quote_price, max_bid)
                    
                elif side == "offer" and orderbook.get('bids'):
                    # For offer: don't go below best bid
                    best_bid = Decimal(str(orderbook['bids'][0][0]))
                    tick_size = self._get_tick_size(exchange)
                    min_offer = best_bid + tick_size
                    quote_price = max(quote_price, min_offer)
                    
            return quote_price
            
        except Exception as e:
            self.logger.error(f"Error in taker check: {e}")
            return quote_price
            
    def _get_tick_size(self, exchange: str) -> Decimal:
        """Get tick size for the exchange (simplified implementation)."""
        # This should be implemented based on exchange-specific tick sizes
        # For now, use a default small value
        return Decimal('0.0001')
        
    async def _get_exchange_orderbook(self, exchange: str) -> Optional[Dict]:
        """Get orderbook for a specific exchange."""
        try:
            connector = self.exchange_connectors.get(exchange)
            if connector:
                return await connector.get_orderbook(self.symbol)
            return None
        except Exception as e:
            self.logger.error(f"Error getting orderbook for {exchange}: {e}")
            return None
            
    async def _get_current_midpoint(self) -> Optional[Decimal]:
        """Get current aggregated midpoint price."""
        try:
            midpoints = []
            for exchange in self.exchanges:
                orderbook = await self._get_exchange_orderbook(exchange)
                if orderbook and orderbook.get('bids') and orderbook.get('asks'):
                    bid = Decimal(str(orderbook['bids'][0][0]))
                    ask = Decimal(str(orderbook['asks'][0][0]))
                    midpoint = (bid + ask) / Decimal('2')
                    midpoints.append(midpoint)
                    
            if midpoints:
                return sum(midpoints) / len(midpoints)
            return None
            
        except Exception as e:
            self.logger.error(f"Error getting midpoint: {e}")
            return None
            
    async def _place_rate_entry_order(
        self, 
        rate_entry: HourlyRateEntry, 
        exchange: str, 
        size: Decimal, 
        price: Decimal
    ) -> None:
        """Place an order for a specific rate entry."""
        try:
            # Convert side: 'bid' -> 'buy', 'offer' -> 'sell'
            order_side = 'buy' if rate_entry.side == 'bid' else 'sell'
            
            # Place order using base strategy method (which handles Gate.io client order ID requirements)
            order_id = await self._place_order(
                exchange=exchange,
                side=order_side,
                amount=float(size),
                price=float(price)
                # No need to specify client_order_id - base method will generate appropriate one
            )
            
            if order_id:
                now = datetime.now(timezone.utc)
                rate_entry.active_orders[exchange] = order_id
                rate_entry.order_placed_at[exchange] = now
                rate_entry.order_placed_price[exchange] = price
                self.orders_placed += 1
                
                self.logger.info(
                    f"Placed {rate_entry.side} order on {exchange}: "
                    f"size={size}, price={price}, order_id={order_id}"
                )
                
        except Exception as e:
            self.logger.error(f"Error placing order: {e}")
            
    async def _check_rate_entry_timeouts_and_drift(
        self, 
        rate_entry: HourlyRateEntry, 
        current_price: Decimal
    ) -> None:
        """Check for timeouts and drift for a rate entry."""
        await self._check_rate_entry_timeouts(rate_entry)
        await self._check_rate_entry_drift(rate_entry, current_price)
        
    async def _check_rate_entry_timeouts(self, rate_entry: HourlyRateEntry) -> None:
        """Check and cancel orders that have timed out (per exchange)."""
        now = datetime.now(timezone.utc)
        timeout_delta = timedelta(seconds=rate_entry.timeout_seconds)
        
        # Check timeouts per exchange
        for exchange in list(rate_entry.active_orders.keys()):
            order_id = rate_entry.active_orders[exchange]
            
            # Check if order still exists in base strategy tracking
            if order_id not in self.active_orders:
                # Order already cancelled/filled, clean up our tracking
                self.logger.debug(f"Order {order_id} not in base strategy tracking, cleaning up rate entry tracking")
                self._cleanup_rate_entry_order_tracking(rate_entry, exchange)
                continue
                
            placed_at = rate_entry.order_placed_at.get(exchange)
            if placed_at and (now - placed_at) > timeout_delta:
                self.logger.info(
                    f"Rate entry {rate_entry.side}: Cancelling order on {exchange} due to TIMEOUT. "
                    f"Order age: {(now - placed_at).total_seconds():.2f}s > {rate_entry.timeout_seconds}s timeout. "
                    f"(Order ID: {order_id})"
                )
                # Try to cancel the order
                success = await self._cancel_order(order_id, exchange)
                self.logger.debug(f"Cancel order {order_id} result: {success}, still in base tracking: {order_id in self.active_orders}")
                
                # Check if order was removed from base strategy tracking (even if cancel returned False)
                if success or order_id not in self.active_orders:
                    # Clean up our tracking - order is gone one way or another
                    self.logger.debug(f"Cleaning up rate entry tracking for order {order_id} on {exchange}")
                    self._cleanup_rate_entry_order_tracking(rate_entry, exchange)
                    # Update statistics only if we actually attempted cancellation
                    if success:
                        self.orders_cancelled_timeout += 1
                        
    async def _check_rate_entry_drift(self, rate_entry: HourlyRateEntry, current_price: Decimal) -> None:
        """Check and cancel orders that have drifted too far from current midpoint (per exchange)."""
        # Check drift per exchange
        for exchange in list(rate_entry.active_orders.keys()):
            order_id = rate_entry.active_orders[exchange]
            
            # Check if order still exists in base strategy tracking
            if order_id not in self.active_orders:
                # Order already cancelled/filled, clean up our tracking
                self.logger.debug(f"Order {order_id} not in base strategy tracking, cleaning up rate entry tracking")
                self._cleanup_rate_entry_order_tracking(rate_entry, exchange)
                continue
                
            placed_price = rate_entry.order_placed_price.get(exchange)
            if not placed_price:
                continue
                
            # Calculate drift from placement price to current midpoint
            drift_pct = abs((current_price - placed_price) / placed_price) * 100
            drift_bps = drift_pct * 100
            
            if drift_bps > rate_entry.drift_bps:
                self.logger.info(
                    f"Rate entry {rate_entry.side}: Cancelling order on {exchange} due to DRIFT. "
                    f"Drift: {drift_bps:.2f} bps (allowed: {rate_entry.drift_bps:.2f} bps), "
                    f"Placed price: {placed_price}, Current price: {current_price} "
                    f"(Order ID: {order_id})"
                )
                # Try to cancel the order
                success = await self._cancel_order(order_id, exchange)
                self.logger.debug(f"Cancel order {order_id} result: {success}, still in base tracking: {order_id in self.active_orders}")
                
                # Check if order was removed from base strategy tracking (even if cancel returned False)
                if success or order_id not in self.active_orders:
                    # Clean up our tracking - order is gone one way or another
                    self.logger.debug(f"Cleaning up rate entry tracking for order {order_id} on {exchange}")
                    self._cleanup_rate_entry_order_tracking(rate_entry, exchange)
                    # Update statistics only if we actually attempted cancellation
                    if success:
                        self.orders_cancelled_drift += 1
                    
    def _cleanup_rate_entry_order_tracking(self, rate_entry: HourlyRateEntry, exchange: str) -> None:
        """Clean up order tracking data for a specific exchange."""
        if exchange in rate_entry.active_orders:
            del rate_entry.active_orders[exchange]
        if exchange in rate_entry.order_placed_at:
            del rate_entry.order_placed_at[exchange]
        if exchange in rate_entry.order_placed_price:
            del rate_entry.order_placed_price[exchange]
        
    async def _cancel_rate_entry_orders(self, rate_entry: HourlyRateEntry) -> None:
        """Cancel all orders for a specific rate entry."""
        for exchange, order_id in list(rate_entry.active_orders.items()):
            success = await self._cancel_order(order_id, exchange)
            if success:
                # Clean up all tracking data for this exchange
                self._cleanup_rate_entry_order_tracking(rate_entry, exchange)
                
    async def _cancel_side_orders(self, side: str) -> None:
        """Cancel all orders for a specific side."""
        rates = self.bid_hourly_rates if side == "bid" else self.offer_hourly_rates
        for rate_entry in rates:
            await self._cancel_rate_entry_orders(rate_entry)
            
    async def _cancel_all_strategy_orders(self) -> None:
        """Cancel all strategy orders."""
        await self._cancel_side_orders("bid")
        await self._cancel_side_orders("offer")
        
    async def on_trade_confirmation(self, trade_data: Dict[str, Any]) -> None:
        """Handle trade confirmation from WebSocket with O(1) position updates."""
        try:
            # Call base class method for instance ownership verification
            await super().on_trade_confirmation(trade_data)
            
            # Basic trade confirmation logging and statistics
            side = trade_data.get('side', '').lower()
            quantity = Decimal(str(trade_data.get('quantity', 0)))
            price = trade_data.get('price', 0)
            order_id = trade_data.get('order_id')
            
            # Update basic statistics
            self.orders_filled += 1
            
            if side == "buy":
                self.total_executed_base += quantity
            else:
                self.total_executed_base -= quantity
                
            self.logger.info(
                f"✅ Top of book trade confirmed for instance {self.instance_id}: "
                f"{side.upper()} {quantity} {self.base_coin} @ {price} "
                f"(Order ID: {order_id}) - updating position incrementally"
            )
            
            # Process trade incrementally for O(1) position tracking
            trade_for_processing = {
                'amount': float(quantity),
                'price': float(price),
                'side': side,
                'timestamp': datetime.now(timezone.utc),
                'fee': 0.0  # Fee will be captured separately if available
            }
            await self._process_new_trade_incremental(trade_for_processing)
            
            # Remove order from active orders tracking
            for rate_entry in (self.bid_hourly_rates + self.offer_hourly_rates):
                for exchange, active_order_id in list(rate_entry.active_orders.items()):
                    if active_order_id == order_id:
                        # Clean up all tracking data for this exchange
                        self._cleanup_rate_entry_order_tracking(rate_entry, exchange)
                        self.logger.debug(f"Removed completed order {order_id} from {rate_entry.side} rate entry")
                        break
                    
        except Exception as e:
            self.logger.error(f"Error handling trade confirmation: {e}")
            
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get strategy performance statistics."""
        base_stats = super().get_performance_stats()
        
        strategy_stats = {
            "current_inventory": float(self.current_inventory),
            "target_inventory": float(self.target_inventory),
            "excess_inventory": float(self.excess_inventory),
            "target_excess_amount": float(self.target_excess_amount),
            "inventory_price": float(self.inventory_price) if self.inventory_price else None,
            "orders_placed": self.orders_placed,
            "orders_filled": self.orders_filled,
            "orders_cancelled_timeout": self.orders_cancelled_timeout,
            "orders_cancelled_drift": self.orders_cancelled_drift,
            "orders_skipped_inventory_protection": self.orders_skipped_inventory_protection,
            "total_executed_base": float(self.total_executed_base),
            "user_start_time": self.start_time.isoformat() if self.start_time else None,
            "strategy_runtime_start": self.runtime_start_time.isoformat() if self.runtime_start_time else None,
            "accounting_method": self.accounting_method,
            "in_cooldown": self.in_cooldown,
            "cooldown_until": self.cooldown_until.isoformat() if self.cooldown_until else None,
            "live_position_data": self.live_position_data,
            "position_tracking_initialized": self._position_lots['initialized'],
            "bid_rates_status": [
                {
                    "max_price": float(rate.max_min_price),
                    "hourly_rate": float(rate.hourly_rate),
                    "time_in_zone": rate.time_in_zone,
                    "active_orders": len(rate.active_orders),
                    "is_currently_active": rate.last_zone_entry is not None,
                    "target_executed": float(rate.hourly_rate * Decimal(str(rate.time_in_zone))),
                    "zone_entry_execution_snapshot": float(rate.zone_entry_execution_snapshot) if rate.zone_entry_execution_snapshot else None,
                    "rate_currency": rate.rate_currency,
                    "drift_bps": rate.drift_bps,
                    "timeout_seconds": rate.timeout_seconds,
                    "zone_entry_time": rate.last_zone_entry.isoformat() if rate.last_zone_entry else None
                }
                for rate in self.bid_hourly_rates
            ],
            "offer_rates_status": [
                {
                    "min_price": float(rate.max_min_price),
                    "hourly_rate": float(rate.hourly_rate),
                    "time_in_zone": rate.time_in_zone,
                    "active_orders": len(rate.active_orders),
                    "is_currently_active": rate.last_zone_entry is not None,
                    "target_executed": float(rate.hourly_rate * Decimal(str(rate.time_in_zone))),
                    "zone_entry_execution_snapshot": float(rate.zone_entry_execution_snapshot) if rate.zone_entry_execution_snapshot else None,
                    "rate_currency": rate.rate_currency,
                    "drift_bps": rate.drift_bps,
                    "timeout_seconds": rate.timeout_seconds,
                    "zone_entry_time": rate.last_zone_entry.isoformat() if rate.last_zone_entry else None
                }
                for rate in self.offer_hourly_rates
            ]
        }
        
        return {**base_stats, **strategy_stats}
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert strategy to dictionary representation."""
        base_dict = super().to_dict()
        
        strategy_dict = {
            "sides": self.sides.value,
            "target_inventory": float(self.target_inventory),
            "excess_inventory_percentage": self.excess_inventory_percentage,
            "spread_bps": self.spread_bps,
            "current_inventory": float(self.current_inventory),
            "excess_inventory": float(self.excess_inventory),
            "performance": self.get_performance_stats()
        }
        
        return {**base_dict, **strategy_dict}

    async def _calculate_executed_since_runtime_start(self) -> Dict[str, Decimal]:
        """Calculate how much we've executed since this strategy instance started running."""
        try:
            from database import get_session
            from sqlalchemy import text
            
            if not self.runtime_start_time:
                return {"bid_executed": Decimal('0'), "offer_executed": Decimal('0')}
            
            async for session in get_session():
                try:
                    # Begin fresh transaction
                    await session.begin()
                    
                    # Convert timezone-aware datetime to timezone-naive for database query
                    runtime_start_for_db = self.runtime_start_time
                    if runtime_start_for_db.tzinfo is not None:
                        runtime_start_for_db = runtime_start_for_db.astimezone(timezone.utc).replace(tzinfo=None)
                    
                    self.logger.debug(f"🔍 Calculating executed amounts since strategy runtime start: {runtime_start_for_db}")
                    
                    # Query all trades for this symbol since strategy runtime start
                    # Don't filter by strategy - track all trades like other strategies do
                    query = text("""
                        SELECT side, SUM(amount) as total_amount
                        FROM risk_trades
                        WHERE (
                            symbol = :symbol
                            OR symbol = :symbol_perp
                            OR symbol = :symbol_usdt
                            OR symbol = :symbol_usdc
                            OR symbol LIKE :symbol_pattern
                        )
                        AND timestamp >= :start_time
                        GROUP BY side
                    """)
                    
                    # Prepare symbol variants
                    base_symbol = self.symbol  # e.g., BERA/USDT
                    symbol_perp = f"{base_symbol}-PERP"  # e.g., BERA/USDT-PERP
                    symbol_usdt = f"{base_symbol}:USDT"  # e.g., BERA/USDT:USDT
                    symbol_usdc = base_symbol.replace('/USDT', '/USDC:USDC')  # e.g., BERA/USDC:USDC
                    symbol_pattern = f"{base_symbol.split('/')[0]}/%"  # e.g., BERA/%
                    
                    result = await session.execute(
                        query,
                        {
                            "symbol": base_symbol,
                            "symbol_perp": symbol_perp,
                            "symbol_usdt": symbol_usdt,
                            "symbol_usdc": symbol_usdc,
                            "symbol_pattern": symbol_pattern,
                            "start_time": runtime_start_for_db
                        }
                    )
                    rows = result.fetchall()
                    
                    bid_executed = Decimal('0')
                    offer_executed = Decimal('0')
                    
                    for row in rows:
                        amount = Decimal(str(row.total_amount))
                        if row.side.lower() == 'buy':
                            bid_executed = amount
                        else:  # sell
                            offer_executed = amount
                    
                    # Commit the read-only transaction
                    await session.commit()
                    
                    self.logger.debug(
                        f"📊 Executed since runtime start: bid={bid_executed}, offer={offer_executed}"
                    )
                    
                    return {"bid_executed": bid_executed, "offer_executed": offer_executed}
                    
                except Exception as e:
                    await session.rollback()
                    self.logger.error(f"Error in executed amounts calculation: {e}")
                    raise e
                finally:
                    await session.close()
                
        except Exception as e:
            self.logger.error(f"Error calculating executed amounts since runtime start: {e}")
            return {"bid_executed": Decimal('0'), "offer_executed": Decimal('0')}

    def _cleanup_orphaned_rate_entry_orders(self) -> int:
        """
        Clean up orders that exist in rate entry tracking but not in base strategy tracking.
        This can happen when WebSocket trade confirmations are missed or error handling fails.
        
        Returns:
            Number of orphaned orders cleaned up
        """
        orphaned_count = 0
        
        # Check bid rate entries
        for rate_entry in self.bid_hourly_rates:
            for exchange in list(rate_entry.active_orders.keys()):
                order_id = rate_entry.active_orders[exchange]
                if order_id not in self.active_orders:
                    self.logger.warning(f"Found orphaned order in bid rate entry: {order_id} on {exchange}")
                    self._cleanup_rate_entry_order_tracking(rate_entry, exchange)
                    orphaned_count += 1
        
        # Check offer rate entries  
        for rate_entry in self.offer_hourly_rates:
            for exchange in list(rate_entry.active_orders.keys()):
                order_id = rate_entry.active_orders[exchange]
                if order_id not in self.active_orders:
                    self.logger.warning(f"Found orphaned order in offer rate entry: {order_id} on {exchange}")
                    self._cleanup_rate_entry_order_tracking(rate_entry, exchange)
                    orphaned_count += 1
        
        if orphaned_count > 0:
            self.logger.info(f"Cleaned up {orphaned_count} orphaned orders from rate entry tracking")
        
        return orphaned_count

    async def _initialize_position_tracking(self) -> None:
        """Initialize position tracking from historical trades - runs once at startup."""
        try:
            self.logger.info(f"Initializing O(1) position tracking for {self.symbol} using {self.accounting_method} method")
            
            # Get all historical trades since start time
            trades = await self._get_trades_for_position_calc()
            
            if not trades:
                self.logger.info("No historical trades found, starting with empty position")
                self._position_lots['initialized'] = True
                # Initialize live data even with no trades
                await self._update_live_data_from_position_state()
                return
            
            # Build initial position state from historical trades
            if self.accounting_method == 'AVERAGE_COST':
                self._initialize_average_cost_position(trades)
            else:  # FIFO or LIFO
                self._initialize_lot_based_position(trades)
            
            # Calculate initial live data
            await self._update_live_data_from_position_state()
            
            self._position_lots['initialized'] = True
            self.logger.info(f"Position tracking initialized with {len(trades)} historical trades")
            
        except Exception as e:
            self.logger.error(f"Error initializing position tracking: {e}")
            # Fallback to empty position
            self._position_lots['initialized'] = True
            
    def _initialize_average_cost_position(self, trades: List[Dict]) -> None:
        """Initialize average cost position state from historical trades."""
        realized_pnl = 0.0
        position_size = 0.0
        position_cost = 0.0
        
        for trade in trades:
            trade_size = trade['amount']
            trade_price = trade['price']
            
            # Apply fees
            if trade.get('fee', 0) > 0:
                fee_per_unit = trade['fee'] / trade_size
                if trade['side'] == 'buy':
                    trade_price += fee_per_unit
                else:
                    trade_price -= fee_per_unit
            
            if trade['side'] == 'buy':
                if position_size < 0:  # Closing short
                    closing_size = min(trade_size, abs(position_size))
                    if closing_size > 0:
                        avg_short_price = abs(position_cost / position_size) if position_size != 0 else 0
                        pnl = closing_size * (avg_short_price - trade_price)
                        realized_pnl += pnl
                        
                        position_size += closing_size
                        position_cost += closing_size * avg_short_price
                        
                        remaining_size = trade_size - closing_size
                        if remaining_size > 0:
                            position_size += remaining_size
                            position_cost += remaining_size * trade_price
                    else:
                        position_size += trade_size
                        position_cost += trade_size * trade_price
                else:
                    position_size += trade_size
                    position_cost += trade_size * trade_price
            else:  # sell
                if position_size > 0:  # Closing long
                    closing_size = min(trade_size, position_size)
                    if closing_size > 0:
                        avg_long_price = position_cost / position_size if position_size != 0 else 0
                        pnl = closing_size * (trade_price - avg_long_price)
                        realized_pnl += pnl
                        
                        position_size -= closing_size
                        position_cost -= closing_size * avg_long_price
                        
                        remaining_size = trade_size - closing_size
                        if remaining_size > 0:
                            position_size -= remaining_size
                            position_cost -= remaining_size * trade_price
                    else:
                        position_size -= trade_size
                        position_cost -= trade_size * trade_price
                else:
                    position_size -= trade_size
                    position_cost -= trade_size * trade_price
        
        self._position_lots['total_realized_pnl'] = realized_pnl
        self._position_lots['position_size'] = position_size
        self._position_lots['position_cost'] = position_cost
        self._position_lots['last_trade_id'] = trades[-1].get('id') if trades else None
        
    def _initialize_lot_based_position(self, trades: List[Dict]) -> None:
        """Initialize FIFO/LIFO position state from historical trades."""
        long_lots = []
        short_lots = []
        realized_pnl = 0.0
        
        for trade in trades:
            trade_size = trade['amount']
            trade_price = trade['price']
            
            # Apply fees
            if trade.get('fee', 0) > 0:
                fee_per_unit = trade['fee'] / trade_size
                if trade['side'] == 'buy':
                    trade_price += fee_per_unit
                else:
                    trade_price -= fee_per_unit
            
            if trade['side'] == 'buy':
                remaining_size = trade_size
                
                # Close short positions
                while remaining_size > 0 and short_lots:
                    if self.accounting_method == 'FIFO':
                        short_lot = short_lots.pop(0)  # Oldest first
                    else:  # LIFO
                        short_lot = short_lots.pop()   # Newest first
                    
                    lot_size, lot_price, lot_timestamp = short_lot
                    
                    if remaining_size >= lot_size:
                        pnl = lot_size * (lot_price - trade_price)
                        realized_pnl += pnl
                        remaining_size -= lot_size
                    else:
                        pnl = remaining_size * (lot_price - trade_price)
                        realized_pnl += pnl
                        if self.accounting_method == 'FIFO':
                            short_lots.insert(0, (lot_size - remaining_size, lot_price, lot_timestamp))
                        else:  # LIFO
                            short_lots.append((lot_size - remaining_size, lot_price, lot_timestamp))
                        remaining_size = 0
                
                # Add remaining as long position
                if remaining_size > 0:
                    long_lots.append((remaining_size, trade_price, trade['timestamp']))
                    
            else:  # sell
                remaining_size = trade_size
                
                # Close long positions
                while remaining_size > 0 and long_lots:
                    if self.accounting_method == 'FIFO':
                        long_lot = long_lots.pop(0)  # Oldest first
                    else:  # LIFO
                        long_lot = long_lots.pop()   # Newest first
                    
                    lot_size, lot_price, lot_timestamp = long_lot
                    
                    if remaining_size >= lot_size:
                        pnl = lot_size * (trade_price - lot_price)
                        realized_pnl += pnl
                        remaining_size -= lot_size
                    else:
                        pnl = remaining_size * (trade_price - lot_price)
                        realized_pnl += pnl
                        if self.accounting_method == 'FIFO':
                            long_lots.insert(0, (lot_size - remaining_size, lot_price, lot_timestamp))
                        else:  # LIFO
                            long_lots.append((lot_size - remaining_size, lot_price, lot_timestamp))
                        remaining_size = 0
                
                # Add remaining as short position
                if remaining_size > 0:
                    short_lots.append((remaining_size, trade_price, trade['timestamp']))
        
        self._position_lots['long_lots'] = long_lots
        self._position_lots['short_lots'] = short_lots
        self._position_lots['total_realized_pnl'] = realized_pnl
        self._position_lots['last_trade_id'] = trades[-1].get('id') if trades else None
            
    async def _get_trades_for_position_calc(self) -> List[Dict]:
        """Get trades for position calculation."""
        try:
            from database import get_session
            from sqlalchemy import text
            
            async for session in get_session():
                try:
                    await session.begin()
                    
                    # Get start time for calculations
                    start_time_for_db = self.get_inventory_start_time()
                    if start_time_for_db.tzinfo is not None:
                        start_time_for_db = start_time_for_db.astimezone(timezone.utc).replace(tzinfo=None)
                    
                    # Query trades including all symbol variants
                    query = text("""
                        SELECT timestamp, side, amount, price, fee_cost, fee_currency
                        FROM risk_trades
                        WHERE (
                            symbol = :symbol
                            OR symbol = :symbol_perp
                            OR symbol = :symbol_usdt
                            OR symbol = :symbol_usdc
                            OR symbol LIKE :symbol_pattern
                        )
                        AND timestamp >= :start_time
                        ORDER BY timestamp ASC
                    """)
                    
                    # Prepare symbol variants
                    base_symbol = self.symbol
                    symbol_perp = f"{base_symbol}-PERP"
                    symbol_usdt = f"{base_symbol}:USDT"
                    symbol_usdc = base_symbol.replace('/USDT', '/USDC:USDC')
                    symbol_pattern = f"{base_symbol.split('/')[0]}/%"
                    
                    result = await session.execute(
                        query,
                        {
                            "symbol": base_symbol,
                            "symbol_perp": symbol_perp,
                            "symbol_usdt": symbol_usdt,
                            "symbol_usdc": symbol_usdc,
                            "symbol_pattern": symbol_pattern,
                            "start_time": start_time_for_db
                        }
                    )
                    rows = result.fetchall()
                    
                    trades = []
                    for row in rows:
                        trades.append({
                            'timestamp': row.timestamp,
                            'side': row.side,
                            'amount': float(row.amount),
                            'price': float(row.price),
                            'fee': float(row.fee_cost) if row.fee_cost else 0.0,
                            'fee_currency': row.fee_currency
                        })
                    
                    await session.commit()
                    return trades
                    
                except Exception as e:
                    await session.rollback()
                    raise e
                finally:
                    await session.close()
                    
        except Exception as e:
            self.logger.error(f"Error getting trades for position calculation: {e}")
            return []
            
    def _calculate_position_with_method(self, trades: List[Dict], method: str) -> Tuple[float, float]:
        """Calculate position using FIFO, LIFO, or Average Cost method."""
        if not trades:
            return 0.0, 0.0
            
        if method == 'LIFO':
            return self._calculate_lifo_position(trades)
        elif method == 'AVERAGE_COST':
            return self._calculate_average_cost_position(trades)
        else:  # FIFO (default)
            return self._calculate_fifo_position(trades)
            
    def _calculate_fifo_position(self, trades: List[Dict]) -> Tuple[float, float]:
        """Calculate position and realized PnL using FIFO method."""
        long_lots = []  # [(size, price, timestamp), ...]
        short_lots = []  # [(size, price, timestamp), ...]
        realized_pnl = 0.0
        
        for trade in trades:
            trade_size = trade['amount']
            trade_price = trade['price']
            
            # Apply fees to trade price
            if trade.get('fee', 0) > 0:
                fee_per_unit = trade['fee'] / trade_size
                if trade['side'] == 'buy':
                    trade_price += fee_per_unit
                else:
                    trade_price -= fee_per_unit
            
            if trade['side'] == 'buy':
                remaining_size = trade_size
                
                # Close short positions (FIFO - oldest first)
                while remaining_size > 0 and short_lots:
                    short_lot = short_lots.pop(0)  # Take oldest
                    lot_size, lot_price, lot_timestamp = short_lot
                    
                    if remaining_size >= lot_size:
                        # Close entire short lot
                        pnl = lot_size * (lot_price - trade_price)
                        realized_pnl += pnl
                        remaining_size -= lot_size
                    else:
                        # Partial close
                        pnl = remaining_size * (lot_price - trade_price)
                        realized_pnl += pnl
                        short_lots.insert(0, (lot_size - remaining_size, lot_price, lot_timestamp))
                        remaining_size = 0
                
                # Add remaining as long position
                if remaining_size > 0:
                    long_lots.append((remaining_size, trade_price, trade['timestamp']))
                    
            else:  # sell
                remaining_size = trade_size
                
                # Close long positions (FIFO - oldest first)
                while remaining_size > 0 and long_lots:
                    long_lot = long_lots.pop(0)  # Take oldest
                    lot_size, lot_price, lot_timestamp = long_lot
                    
                    if remaining_size >= lot_size:
                        # Close entire long lot
                        pnl = lot_size * (trade_price - lot_price)
                        realized_pnl += pnl
                        remaining_size -= lot_size
                    else:
                        # Partial close
                        pnl = remaining_size * (trade_price - lot_price)
                        realized_pnl += pnl
                        long_lots.insert(0, (lot_size - remaining_size, lot_price, lot_timestamp))
                        remaining_size = 0
                
                # Add remaining as short position
                if remaining_size > 0:
                    short_lots.append((remaining_size, trade_price, trade['timestamp']))
        
        # Calculate weighted average entry price of remaining lots
        if long_lots:
            total_size = sum(lot[0] for lot in long_lots)
            total_cost = sum(lot[0] * lot[1] for lot in long_lots)
            entry_price = total_cost / total_size if total_size > 0 else 0
        elif short_lots:
            total_size = sum(lot[0] for lot in short_lots)
            total_cost = sum(lot[0] * lot[1] for lot in short_lots)
            entry_price = total_cost / total_size if total_size > 0 else 0
        else:
            entry_price = 0.0
            
        return entry_price, realized_pnl
        
    def _calculate_lifo_position(self, trades: List[Dict]) -> Tuple[float, float]:
        """Calculate position and realized PnL using LIFO method."""
        long_lots = []  # [(size, price, timestamp), ...]
        short_lots = []  # [(size, price, timestamp), ...]
        realized_pnl = 0.0
        
        for trade in trades:
            trade_size = trade['amount']
            trade_price = trade['price']
            
            # Apply fees to trade price
            if trade.get('fee', 0) > 0:
                fee_per_unit = trade['fee'] / trade_size
                if trade['side'] == 'buy':
                    trade_price += fee_per_unit
                else:
                    trade_price -= fee_per_unit
            
            if trade['side'] == 'buy':
                remaining_size = trade_size
                
                # Close short positions (LIFO - newest first)
                while remaining_size > 0 and short_lots:
                    short_lot = short_lots.pop()  # Take newest
                    lot_size, lot_price, lot_timestamp = short_lot
                    
                    if remaining_size >= lot_size:
                        # Close entire short lot
                        pnl = lot_size * (lot_price - trade_price)
                        realized_pnl += pnl
                        remaining_size -= lot_size
                    else:
                        # Partial close
                        pnl = remaining_size * (lot_price - trade_price)
                        realized_pnl += pnl
                        short_lots.append((lot_size - remaining_size, lot_price, lot_timestamp))
                        remaining_size = 0
                
                # Add remaining as long position
                if remaining_size > 0:
                    long_lots.append((remaining_size, trade_price, trade['timestamp']))
                    
            else:  # sell
                remaining_size = trade_size
                
                # Close long positions (LIFO - newest first)
                while remaining_size > 0 and long_lots:
                    long_lot = long_lots.pop()  # Take newest
                    lot_size, lot_price, lot_timestamp = long_lot
                    
                    if remaining_size >= lot_size:
                        # Close entire long lot
                        pnl = lot_size * (trade_price - lot_price)
                        realized_pnl += pnl
                        remaining_size -= lot_size
                    else:
                        # Partial close
                        pnl = remaining_size * (trade_price - lot_price)
                        realized_pnl += pnl
                        long_lots.append((lot_size - remaining_size, lot_price, lot_timestamp))
                        remaining_size = 0
                
                # Add remaining as short position
                if remaining_size > 0:
                    short_lots.append((remaining_size, trade_price, trade['timestamp']))
        
        # Calculate weighted average entry price of remaining lots
        if long_lots:
            total_size = sum(lot[0] for lot in long_lots)
            total_cost = sum(lot[0] * lot[1] for lot in long_lots)
            entry_price = total_cost / total_size if total_size > 0 else 0
        elif short_lots:
            total_size = sum(lot[0] for lot in short_lots)
            total_cost = sum(lot[0] * lot[1] for lot in short_lots)
            entry_price = total_cost / total_size if total_size > 0 else 0
        else:
            entry_price = 0.0
            
        return entry_price, realized_pnl
        
    def _calculate_average_cost_position(self, trades: List[Dict]) -> Tuple[float, float]:
        """Calculate position and realized PnL using Average Cost method."""
        realized_pnl = 0.0
        position_size = 0.0
        position_cost = 0.0
        
        for trade in trades:
            trade_size = trade['amount']
            trade_price = trade['price']
            
            # Apply fees to trade price
            if trade.get('fee', 0) > 0:
                fee_per_unit = trade['fee'] / trade_size
                if trade['side'] == 'buy':
                    trade_price += fee_per_unit
                else:
                    trade_price -= fee_per_unit
            
            if trade['side'] == 'buy':
                # Check if closing short position
                if position_size < 0:
                    closing_size = min(trade_size, abs(position_size))
                    
                    if closing_size > 0:
                        avg_short_price = abs(position_cost / position_size) if position_size != 0 else 0
                        pnl = closing_size * (avg_short_price - trade_price)
                        realized_pnl += pnl
                        
                        position_size += closing_size
                        position_cost += closing_size * avg_short_price
                        
                        remaining_size = trade_size - closing_size
                        if remaining_size > 0:
                            position_size += remaining_size
                            position_cost += remaining_size * trade_price
                    else:
                        position_size += trade_size
                        position_cost += trade_size * trade_price
                else:
                    position_size += trade_size
                    position_cost += trade_size * trade_price
                    
            else:  # sell
                # Check if closing long position
                if position_size > 0:
                    closing_size = min(trade_size, position_size)
                    
                    if closing_size > 0:
                        avg_long_price = position_cost / position_size if position_size != 0 else 0
                        pnl = closing_size * (trade_price - avg_long_price)
                        realized_pnl += pnl
                        
                        position_size -= closing_size
                        position_cost -= closing_size * avg_long_price
                        
                        remaining_size = trade_size - closing_size
                        if remaining_size > 0:
                            position_size -= remaining_size
                            position_cost -= remaining_size * trade_price
                    else:
                        position_size -= trade_size
                        position_cost -= trade_size * trade_price
                else:
                    position_size -= trade_size
                    position_cost -= trade_size * trade_price
        
        # Calculate average entry price
        entry_price = abs(position_cost / position_size) if position_size != 0 else 0.0
        
        return entry_price, realized_pnl
        
    async def _update_live_data_from_position_state(self, current_price: Optional[float] = None) -> None:
        """Update live position data from current position state - O(1) operation."""
        try:
            # Calculate inventory price based on current position state
            if self.accounting_method == 'AVERAGE_COST':
                inventory_price = self._get_average_cost_inventory_price()
            else:  # FIFO or LIFO
                inventory_price = self._get_lot_based_inventory_price()
            
            # Calculate unrealized PnL if current price is provided
            unrealized_pnl = 0.0
            if current_price and inventory_price and self.current_inventory != 0:
                if self.current_inventory > 0:  # Long position
                    unrealized_pnl = float(self.current_inventory) * (current_price - inventory_price)
                else:  # Short position
                    unrealized_pnl = abs(float(self.current_inventory)) * (inventory_price - current_price)
            
            # Update live data
            self.live_position_data = {
                'inventory_price': inventory_price,
                'realized_pnl': self._position_lots['total_realized_pnl'],
                'unrealized_pnl': unrealized_pnl,
                'last_updated': datetime.now(timezone.utc).isoformat()
            }
            
            self.logger.debug(f"📊 Live position data updated: inventory_price={inventory_price}, realized_pnl={self._position_lots['total_realized_pnl']}, unrealized_pnl={unrealized_pnl}")
            
            # Publish performance data to Redis for API access
            await self._publish_performance_data()
            
        except Exception as e:
            self.logger.error(f"Error updating live data from position state: {e}")
            
    async def _publish_performance_data(self) -> None:
        """Publish current performance data to Redis for API access."""
        try:
            if not self.redis_client:
                return
                
            # Get current performance stats
            performance_data = self.get_performance_stats()
            
            # Add basic instance info
            full_data = {
                "instance_id": self.instance_id,
                "strategy": "top_of_book",  # Use the config key, not the class name
                "symbol": self.symbol,
                "exchanges": self.exchanges,
                "config": self.config,
                "status": "running" if self.running else "stopped",
                "started_at": self.runtime_start_time.isoformat() if self.runtime_start_time else None,
                "last_updated": datetime.now(timezone.utc).isoformat(),
                "performance": performance_data
            }
            
            # Store in Redis with 60 second expiry
            await self.redis_client.setex(
                self.performance_key,
                60,  # Expire after 60 seconds
                json.dumps(full_data, default=str)
            )
            
        except Exception as e:
            self.logger.error(f"Error publishing performance data to Redis: {e}")
            
    def _get_average_cost_inventory_price(self) -> float:
        """Get inventory price for average cost method - O(1)."""
        position_size = self._position_lots['position_size']
        position_cost = self._position_lots['position_cost']
        
        if position_size != 0:
            return abs(position_cost / position_size)
        return 0.0
        
    def _get_lot_based_inventory_price(self) -> float:
        """Get inventory price for FIFO/LIFO methods - O(1)."""
        long_lots = self._position_lots['long_lots']
        short_lots = self._position_lots['short_lots']
        
        if long_lots:
            total_size = sum(lot[0] for lot in long_lots)
            total_cost = sum(lot[0] * lot[1] for lot in long_lots)
            return total_cost / total_size if total_size > 0 else 0.0
        elif short_lots:
            total_size = sum(lot[0] for lot in short_lots)
            total_cost = sum(lot[0] * lot[1] for lot in short_lots)
            return total_cost / total_size if total_size > 0 else 0.0
        
        return 0.0
        
    async def _process_new_trade_incremental(self, trade_data: Dict) -> None:
        """Process a new trade incrementally - O(1) operation."""
        if not self._position_lots['initialized']:
            return
            
        try:
            trade_size = float(trade_data.get('amount', 0))
            trade_price = float(trade_data.get('price', 0))
            trade_side = trade_data.get('side', '').lower()
            trade_timestamp = trade_data.get('timestamp', datetime.now(timezone.utc))
            
            # Apply fees if available
            fee = float(trade_data.get('fee', 0))
            if fee > 0:
                fee_per_unit = fee / trade_size
                if trade_side == 'buy':
                    trade_price += fee_per_unit
                else:
                    trade_price -= fee_per_unit
            
            # Update position state based on accounting method
            if self.accounting_method == 'AVERAGE_COST':
                self._process_trade_average_cost(trade_size, trade_price, trade_side)
            else:  # FIFO or LIFO
                self._process_trade_lot_based(trade_size, trade_price, trade_side, trade_timestamp)
            
            # Update live data immediately (unrealized PnL will be calculated during quote process)
            await self._update_live_data_from_position_state()
            
            self.logger.debug(f"Processed new trade incrementally: {trade_side} {trade_size} @ {trade_price}")
            
        except Exception as e:
            self.logger.error(f"Error processing trade incrementally: {e}")
            
    def _process_trade_average_cost(self, trade_size: float, trade_price: float, trade_side: str) -> None:
        """Process trade using average cost method - O(1)."""
        if trade_side == 'buy':
            if self._position_lots['position_size'] < 0:  # Closing short
                closing_size = min(trade_size, abs(self._position_lots['position_size']))
                if closing_size > 0:
                    avg_short_price = abs(self._position_lots['position_cost'] / self._position_lots['position_size']) if self._position_lots['position_size'] != 0 else 0
                    pnl = closing_size * (avg_short_price - trade_price)
                    self._position_lots['total_realized_pnl'] += pnl
                    
                    self._position_lots['position_size'] += closing_size
                    self._position_lots['position_cost'] += closing_size * avg_short_price
                    
                    remaining_size = trade_size - closing_size
                    if remaining_size > 0:
                        self._position_lots['position_size'] += remaining_size
                        self._position_lots['position_cost'] += remaining_size * trade_price
                else:
                    self._position_lots['position_size'] += trade_size
                    self._position_lots['position_cost'] += trade_size * trade_price
            else:
                self._position_lots['position_size'] += trade_size
                self._position_lots['position_cost'] += trade_size * trade_price
        else:  # sell
            if self._position_lots['position_size'] > 0:  # Closing long
                closing_size = min(trade_size, self._position_lots['position_size'])
                if closing_size > 0:
                    avg_long_price = self._position_lots['position_cost'] / self._position_lots['position_size'] if self._position_lots['position_size'] != 0 else 0
                    pnl = closing_size * (trade_price - avg_long_price)
                    self._position_lots['total_realized_pnl'] += pnl
                    
                    self._position_lots['position_size'] -= closing_size
                    self._position_lots['position_cost'] -= closing_size * avg_long_price
                    
                    remaining_size = trade_size - closing_size
                    if remaining_size > 0:
                        self._position_lots['position_size'] -= remaining_size
                        self._position_lots['position_cost'] -= remaining_size * trade_price
                else:
                    self._position_lots['position_size'] -= trade_size
                    self._position_lots['position_cost'] -= trade_size * trade_price
            else:
                self._position_lots['position_size'] -= trade_size
                self._position_lots['position_cost'] -= trade_size * trade_price
                
    def _process_trade_lot_based(self, trade_size: float, trade_price: float, trade_side: str, trade_timestamp) -> None:
        """Process trade using FIFO/LIFO method - O(1) amortized."""
        long_lots = self._position_lots['long_lots']
        short_lots = self._position_lots['short_lots']
        
        if trade_side == 'buy':
            remaining_size = trade_size
            
            # Close short positions
            while remaining_size > 0 and short_lots:
                if self.accounting_method == 'FIFO':
                    short_lot = short_lots.pop(0)  # Oldest first
                else:  # LIFO
                    short_lot = short_lots.pop()   # Newest first
                
                lot_size, lot_price, lot_timestamp = short_lot
                
                if remaining_size >= lot_size:
                    pnl = lot_size * (lot_price - trade_price)
                    self._position_lots['total_realized_pnl'] += pnl
                    remaining_size -= lot_size
                else:
                    pnl = remaining_size * (lot_price - trade_price)
                    self._position_lots['total_realized_pnl'] += pnl
                    if self.accounting_method == 'FIFO':
                        short_lots.insert(0, (lot_size - remaining_size, lot_price, lot_timestamp))
                    else:  # LIFO
                        short_lots.append((lot_size - remaining_size, lot_price, lot_timestamp))
                    remaining_size = 0
            
            # Add remaining as long position
            if remaining_size > 0:
                long_lots.append((remaining_size, trade_price, trade_timestamp))
                
        else:  # sell
            remaining_size = trade_size
            
            # Close long positions
            while remaining_size > 0 and long_lots:
                if self.accounting_method == 'FIFO':
                    long_lot = long_lots.pop(0)  # Oldest first
                else:  # LIFO
                    long_lot = long_lots.pop()   # Newest first
                
                lot_size, lot_price, lot_timestamp = long_lot
                
                if remaining_size >= lot_size:
                    pnl = lot_size * (trade_price - lot_price)
                    self._position_lots['total_realized_pnl'] += pnl
                    remaining_size -= lot_size
                else:
                    pnl = remaining_size * (trade_price - lot_price)
                    self._position_lots['total_realized_pnl'] += pnl
                    if self.accounting_method == 'FIFO':
                        long_lots.insert(0, (lot_size - remaining_size, lot_price, lot_timestamp))
                    else:  # LIFO
                        long_lots.append((lot_size - remaining_size, lot_price, lot_timestamp))
                    remaining_size = 0
            
            # Add remaining as short position
            if remaining_size > 0:
                short_lots.append((remaining_size, trade_price, trade_timestamp))