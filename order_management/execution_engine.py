"""
Order execution engine for smart order routing across exchanges.

Provides advanced order execution capabilities:
- Smart order routing across multiple exchanges
- Order splitting and aggregation
- Best price discovery
- Execution algorithms
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any, Union, Callable, Tuple
from decimal import Decimal
import uuid
import time
from datetime import datetime
import structlog
import heapq
from enum import Enum

from .order import Order, OrderStatus, OrderSide, OrderType
from .order_manager import OrderManager
from market_data.orderbook import OrderBook


logger = structlog.get_logger(__name__)


class ExecutionStrategy(str, Enum):
    """Execution strategy enum."""
    BEST_PRICE = 'best_price'  # Route to exchange with best price
    BEST_LIQUIDITY = 'best_liquidity'  # Route to exchange with most liquidity
    LOWEST_FEES = 'lowest_fees'  # Route to exchange with lowest fees
    SPLIT_EVENLY = 'split_evenly'  # Split order evenly across exchanges
    SPLIT_BY_LIQUIDITY = 'split_by_liquidity'  # Split proportionally to liquidity
    PRICE_OPTIMIZED_LIQUIDITY = 'price_optimized_liquidity'  # Optimize price for a given liquidity requirement
    MINIMAL_MARKET_IMPACT = 'minimal_market_impact'  # Minimize price impact by using as little liquidity as possible
    ICEBERG = 'iceberg'  # Show only a portion of order at a time
    TWAP = 'twap'  # Time-weighted average price
    VWAP = 'vwap'  # Volume-weighted average price


class ExecutionEngine:
    """
    Order execution engine for smart order routing and algorithmic execution.
    
    Provides advanced order execution capabilities including:
    - Smart order routing across multiple exchanges
    - Order splitting and aggregation
    - Best price discovery
    - Execution algorithms (TWAP, VWAP, Iceberg, etc.)
    """
    
    def __init__(
        self,
        order_manager: OrderManager,
        exchange_connectors: Dict[str, Any],
        orderbooks: Dict[str, Dict[str, OrderBook]] = None
    ):
        """
        Initialize execution engine.
        
        Args:
            order_manager: Order manager instance
            exchange_connectors: Dictionary of exchange connectors
            orderbooks: Dictionary of orderbooks by exchange and symbol
        """
        self.order_manager = order_manager
        self.exchange_connectors = exchange_connectors
        self.orderbooks = orderbooks or {}
        
        # Execution algorithms
        self.algo_tasks = {}
        
        # Exchange metadata
        self.exchange_fees = {}
        self.exchange_latencies = {}
        
        # Exchange connector mapping for spot/perp variants
        self.exchange_mapping = self._build_exchange_mapping(exchange_connectors)
        
        # Register callbacks
        self.order_manager.register_order_update_callback(self._on_order_update)
        self.order_manager.register_order_fill_callback(self._on_order_fill)
        
        self.logger = logger.bind(component="ExecutionEngine")
        self.logger.info("Execution engine initialized")
        self.logger.info(f"Available exchange mappings: {self.exchange_mapping}")
        
    def _build_exchange_mapping(self, exchange_connectors: Dict[str, Any]) -> Dict[str, str]:
        """
        Build mapping from normalized exchange names (with market type) to connector keys.
        
        Args:
            exchange_connectors: Dictionary of exchange connectors
            
        Returns:
            Dictionary mapping normalized exchange names to connector keys
        """
        mapping = {}
        
        for connector_key in exchange_connectors.keys():
            # Extract exchange name and market type from connector key
            # Expected formats: 'binance', 'binanceusdm', 'bybit_spot', etc.
            parts = connector_key.split('_')
            exchange_name = parts[0]
            
            # Determine market type from connector key
            market_type = 'spot'  # Default
            if len(parts) > 1:
                if parts[1] in ['futures', 'perp', 'future', 'swap', 'perpetual']:
                    market_type = 'perp'
                else:
                    market_type = parts[1]
            elif any(m in connector_key.lower() for m in ['usdm', 'futures', 'perp', 'future', 'swap']):
                market_type = 'perp'
                
            # Store in mapping
            normalized_key = f"{exchange_name}_{market_type}"
            mapping[normalized_key] = connector_key
            
            # Also store the original key for direct access
            mapping[connector_key] = connector_key
            
        return mapping
        
    def get_connector_key(self, exchange: str) -> Optional[str]:
        """
        Get connector key for exchange name.
        
        Args:
            exchange: Exchange name, possibly with market type (e.g., 'binance_spot')
            
        Returns:
            Connector key if found, None otherwise
        """
        # Direct match
        if exchange in self.exchange_connectors:
            return exchange
            
        # Check mapping
        if exchange in self.exchange_mapping:
            return self.exchange_mapping[exchange]
            
        # Try to infer mapping
        if '_' in exchange:
            # Exchange with market type specified
            base_name, market_type = exchange.split('_', 1)
            
            # Normalize market type
            if market_type in ['futures', 'future', 'perpetual', 'swap', 'perp']:
                market_type = 'perp'
                
            # Look for matching connector
            normalized_key = f"{base_name}_{market_type}"
            if normalized_key in self.exchange_mapping:
                return self.exchange_mapping[normalized_key]
                
            # Try fuzzy matching
            for key in self.exchange_mapping:
                if key.startswith(base_name) and market_type in key:
                    return self.exchange_mapping[key]
        else:
            # Just exchange name, try to find a match
            for key in self.exchange_mapping:
                if key.startswith(exchange):
                    return self.exchange_mapping[key]
                    
        return None
        
    async def start(self) -> None:
        """Start execution engine and background tasks."""
        self.logger.info("Starting execution engine")
        
        # Load exchange fees and metadata
        await self._load_exchange_metadata()
        
    async def stop(self) -> None:
        """Stop execution engine and cancel ongoing algorithmic executions."""
        self.logger.info("Stopping execution engine")
        
        # Cancel all algorithmic execution tasks
        for task in self.algo_tasks.values():
            task.cancel()
            
        # Wait for all tasks to complete
        if self.algo_tasks:
            await asyncio.gather(*self.algo_tasks.values(), return_exceptions=True)
            
    async def execute_order(
        self,
        symbol: str,
        side: Union[str, OrderSide],
        amount: Union[str, Decimal],
        order_type: Union[str, OrderType] = OrderType.LIMIT,
        price: Optional[Union[str, Decimal]] = None,
        strategy: Union[str, ExecutionStrategy] = ExecutionStrategy.BEST_PRICE,
        exchanges: Optional[List[str]] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> Order:
        """
        Execute order using specified strategy.
        
        Args:
            symbol: Trading symbol
            side: Buy or sell
            amount: Order amount
            order_type: Type of order
            price: Order price (required for limit orders)
            strategy: Execution strategy to use
            exchanges: Specific exchanges to use (None for all available)
            params: Additional strategy-specific parameters
                For SPLIT_BY_LIQUIDITY:
                    - liquidity_type: 'base' or 'quote' (default: 'base')
                    - min_liquidity_percent: Minimum percent of liquidity to place on an exchange (default: 0.05)
                    - include_spot: Whether to include spot markets (default: True)
                    - include_perp: Whether to include perpetual futures markets (default: True)
                For PRICE_OPTIMIZED_LIQUIDITY:
                    - target_price: Target price to optimize for (default: order price)
                    - max_slippage_percent: Maximum slippage percentage allowed (default: 0.5)
                    - liquidity_type: 'base' or 'quote' (default: 'base')
                For MINIMAL_MARKET_IMPACT:
                    - max_price_impact_percent: Maximum price impact percentage allowed (default: 0.5)
                    - liquidity_type: 'base' or 'quote' (default: 'base')
                    - price_levels: Number of price levels to use for impact calculation (default: 10)
            
        Returns:
            Parent order with execution details
        """
        # Convert to proper types
        if isinstance(amount, str):
            amount = Decimal(amount)
        if price is not None and isinstance(price, str):
            price = Decimal(price)
        if isinstance(strategy, str):
            strategy = ExecutionStrategy(strategy)
        if isinstance(side, str):
            side = OrderSide(side)
        if isinstance(order_type, str):
            order_type = OrderType(order_type)
            
        # Create params dict if not provided
        params = params or {}
        
        # For SPLIT_BY_LIQUIDITY, enforce limit orders only
        if strategy == ExecutionStrategy.SPLIT_BY_LIQUIDITY and order_type != OrderType.LIMIT:
            self.logger.warning(f"SPLIT_BY_LIQUIDITY only supports limit orders. Changing from {order_type} to LIMIT.")
            order_type = OrderType.LIMIT
            
        # For PRICE_OPTIMIZED_LIQUIDITY, enforce limit orders only
        if strategy == ExecutionStrategy.PRICE_OPTIMIZED_LIQUIDITY and order_type != OrderType.LIMIT:
            self.logger.warning(f"PRICE_OPTIMIZED_LIQUIDITY only supports limit orders. Changing from {order_type} to LIMIT.")
            order_type = OrderType.LIMIT
            
        # For MINIMAL_MARKET_IMPACT, enforce limit orders only
        if strategy == ExecutionStrategy.MINIMAL_MARKET_IMPACT and order_type != OrderType.LIMIT:
            self.logger.warning(f"MINIMAL_MARKET_IMPACT only supports limit orders. Changing from {order_type} to LIMIT.")
            order_type = OrderType.LIMIT
        
        # Create parent order
        parent_order = await self.order_manager.create_order(
            symbol=symbol,
            side=side,
            amount=amount,
            order_type=order_type,
            price=price,
            params=params,
            smart_route=True  # Mark as smart routed
        )
        
        # Filter exchanges if specified
        available_exchanges = exchanges or list(self.exchange_connectors.keys())
        
        # Filter to exchanges that support the symbol
        valid_exchanges = []
        for exchange in available_exchanges:
            if exchange not in self.exchange_connectors:
                continue
                
            connector = self.exchange_connectors[exchange]
            try:
                if await connector.has_symbol(symbol):
                    valid_exchanges.append(exchange)
            except Exception as e:
                self.logger.warning(f"Error checking symbol {symbol} on {exchange}: {e}")
                
        if not valid_exchanges:
            parent_order.reject(f"No valid exchanges found for symbol {symbol}")
            return parent_order
        
        # Execute based on strategy
        if strategy == ExecutionStrategy.BEST_PRICE:
            await self._execute_best_price(parent_order, valid_exchanges)
        elif strategy == ExecutionStrategy.BEST_LIQUIDITY:
            await self._execute_best_liquidity(parent_order, valid_exchanges)
        elif strategy == ExecutionStrategy.LOWEST_FEES:
            await self._execute_lowest_fees(parent_order, valid_exchanges)
        elif strategy == ExecutionStrategy.SPLIT_EVENLY:
            await self._execute_split_evenly(parent_order, valid_exchanges)
        elif strategy == ExecutionStrategy.SPLIT_BY_LIQUIDITY:
            await self._execute_split_by_liquidity(parent_order, valid_exchanges)
        elif strategy == ExecutionStrategy.PRICE_OPTIMIZED_LIQUIDITY:
            await self._execute_price_optimized_liquidity(parent_order, valid_exchanges)
        elif strategy == ExecutionStrategy.MINIMAL_MARKET_IMPACT:
            await self._execute_minimal_market_impact(parent_order, valid_exchanges)
        elif strategy in [ExecutionStrategy.TWAP, ExecutionStrategy.VWAP, ExecutionStrategy.ICEBERG]:
            # Start algorithmic execution
            await self._start_algo_execution(parent_order, strategy, valid_exchanges, params)
        else:
            # Default to best price
            await self._execute_best_price(parent_order, valid_exchanges)
            
        return parent_order

    async def _execute_best_price(self, parent_order: Order, exchanges: List[str]) -> None:
        """
        Execute order on exchange with best price.
        
        Args:
            parent_order: Parent order to execute
            exchanges: List of valid exchanges
        """
        symbol = parent_order.symbol
        side = parent_order.side
        price_key = 'asks' if side == OrderSide.BUY else 'bids'
        
        best_price = None
        best_exchange = None
        
        # Check orderbooks first if available
        for exchange in exchanges:
            if exchange in self.orderbooks and symbol in self.orderbooks[exchange]:
                book = self.orderbooks[exchange][symbol]
                
                # Get best price
                if price_key == 'asks' and book.asks:
                    exchange_price = book.asks[0][0]  # Best ask price
                elif price_key == 'bids' and book.bids:
                    exchange_price = book.bids[0][0]  # Best bid price
                else:
                    continue
                    
                # Compare with current best
                if best_price is None or (side == OrderSide.BUY and exchange_price < best_price) or \
                   (side == OrderSide.SELL and exchange_price > best_price):
                    best_price = exchange_price
                    best_exchange = exchange
        
        # If no valid orderbooks or no price found, fetch orderbooks
        if best_exchange is None:
            orderbooks = {}
            
            # Fetch orderbooks
            for exchange in exchanges:
                try:
                    connector = self.exchange_connectors[exchange]
                    book = await connector.get_orderbook(symbol)
                    
                    if not book or price_key not in book or not book[price_key]:
                        continue
                        
                    orderbooks[exchange] = book
                except Exception as e:
                    self.logger.warning(f"Error fetching orderbook for {symbol} on {exchange}: {e}")
                    
            # Find best price
            for exchange, book in orderbooks.items():
                if not book[price_key]:
                    continue
                    
                exchange_price = book[price_key][0][0]
                
                if best_price is None or (side == OrderSide.BUY and exchange_price < best_price) or \
                   (side == OrderSide.SELL and exchange_price > best_price):
                    best_price = exchange_price
                    best_exchange = exchange
        
        if best_exchange is None:
            parent_order.reject(f"No exchange with valid orderbook for {symbol}")
            return
            
        # Execute on best exchange
        await self._create_child_order(
            parent_order=parent_order,
            exchange=best_exchange,
            amount=parent_order.amount,
            price=parent_order.price
        )
    
    async def _execute_best_liquidity(self, parent_order: Order, exchanges: List[str]) -> None:
        """
        Execute order on exchange with best liquidity.
        
        Args:
            parent_order: Parent order to execute
            exchanges: List of valid exchanges
        """
        symbol = parent_order.symbol
        side = parent_order.side
        amount = parent_order.amount
        price_key = 'asks' if side == OrderSide.BUY else 'bids'
        
        best_liquidity = Decimal('0')
        best_exchange = None
        
        # Check orderbooks
        for exchange in exchanges:
            try:
                # Get orderbook
                if exchange in self.orderbooks and symbol in self.orderbooks[exchange]:
                    book = self.orderbooks[exchange][symbol]
                else:
                    connector = self.exchange_connectors[exchange]
                    book_data = await connector.get_orderbook(symbol)
                    if not book_data or price_key not in book_data:
                        continue
                    book = book_data
                
                # Calculate available liquidity within price range
                liquidity = Decimal('0')
                
                # For market orders, consider all liquidity
                if parent_order.order_type == OrderType.MARKET:
                    liquidity = sum(Decimal(str(level[1])) for level in book[price_key])
                else:
                    # For limit orders, consider liquidity at or better than limit price
                    for level in book[price_key]:
                        level_price = Decimal(str(level[0]))
                        level_amount = Decimal(str(level[1]))
                        
                        if (side == OrderSide.BUY and level_price <= parent_order.price) or \
                           (side == OrderSide.SELL and level_price >= parent_order.price):
                            liquidity += level_amount
                
                # Find exchange with most liquidity
                if liquidity > best_liquidity:
                    best_liquidity = liquidity
                    best_exchange = exchange
                    
            except Exception as e:
                self.logger.warning(f"Error calculating liquidity for {symbol} on {exchange}: {e}")
        
        if best_exchange is None:
            parent_order.reject(f"No exchange with sufficient liquidity for {symbol}")
            return
            
        # Execute on exchange with best liquidity
        await self._create_child_order(
            parent_order=parent_order,
            exchange=best_exchange,
            amount=amount,
            price=parent_order.price
        )
    
    async def _execute_lowest_fees(self, parent_order: Order, exchanges: List[str]) -> None:
        """
        Execute order on exchange with lowest fees.
        
        Args:
            parent_order: Parent order to execute
            exchanges: List of valid exchanges
        """
        # Find exchange with lowest fees
        lowest_fee = None
        best_exchange = None
        
        for exchange in exchanges:
            # Check if exchange supports the symbol
            if exchange not in self.exchange_fees:
                continue
                
            fee = self.exchange_fees.get(exchange, {}).get(parent_order.symbol, None)
            if fee is None:
                continue
                
            if lowest_fee is None or fee < lowest_fee:
                lowest_fee = fee
                best_exchange = exchange
        
        if best_exchange is None:
            # Fall back to first valid exchange
            best_exchange = exchanges[0] if exchanges else None
            
        if best_exchange is None:
            parent_order.reject(f"No valid exchange for {parent_order.symbol}")
            return
            
        # Execute on exchange with lowest fees
        await self._create_child_order(
            parent_order=parent_order,
            exchange=best_exchange,
            amount=parent_order.amount,
            price=parent_order.price
        )
    
    async def _execute_split_evenly(self, parent_order: Order, exchanges: List[str]) -> None:
        """
        Split order evenly across exchanges.
        
        Args:
            parent_order: Parent order to execute
            exchanges: List of valid exchanges
        """
        if not exchanges:
            parent_order.reject("No valid exchanges for split execution")
            return
            
        # Calculate amount per exchange
        total_exchanges = len(exchanges)
        amount_per_exchange = parent_order.amount / Decimal(total_exchanges)
        
        # Create child orders
        tasks = []
        for exchange in exchanges:
            tasks.append(
                self._create_child_order(
                    parent_order=parent_order,
                    exchange=exchange,
                    amount=amount_per_exchange,
                    price=parent_order.price
                )
            )
            
        # Wait for all child orders to be placed
        await asyncio.gather(*tasks)
    
    async def _execute_split_by_liquidity(self, parent_order: Order, exchanges: List[str]) -> None:
        """
        Split order across exchanges proportionally to available liquidity.
        
        Args:
            parent_order: Parent order to execute
            exchanges: List of valid exchanges
        """
        symbol = parent_order.symbol
        side = parent_order.side
        total_amount = parent_order.amount
        price = parent_order.price
        params = parent_order.params or {}
        
        # Get liquidity type (base or quote)
        liquidity_type = params.get('liquidity_type', 'base')
        min_liquidity_percent = Decimal(str(params.get('min_liquidity_percent', 0.05)))
        include_spot = params.get('include_spot', True)
        include_perp = params.get('include_perp', True)
        
        price_key = 'asks' if side == OrderSide.BUY else 'bids'
        
        # Filter exchanges based on spot/perp preferences
        filtered_exchanges = []
        for exchange in exchanges:
            # Check if exchange is in format 'exchange_type' (e.g., 'binance_spot', 'binance_perp')
            if '_' in exchange:
                exchange_name, market_type = exchange.split('_', 1)
                if (market_type == 'spot' and not include_spot) or \
                   (market_type in ['perp', 'future', 'futures'] and not include_perp):
                    continue
            filtered_exchanges.append(exchange)
            
        if not filtered_exchanges:
            parent_order.reject(f"No valid exchanges after filtering spot/perp preferences")
            return
            
        # Collect liquidity information
        exchange_liquidity = {}
        total_liquidity = Decimal('0')
        
        for exchange in filtered_exchanges:
            try:
                # Get connector key for exchange
                exchange_key = self.get_connector_key(exchange)
                if not exchange_key:
                    self.logger.warning(f"No connector found for {exchange}")
                    continue
                
                # Get orderbook
                connector = self.exchange_connectors[exchange_key]
                if exchange_key in self.orderbooks and symbol in self.orderbooks[exchange_key]:
                    book = self.orderbooks[exchange_key][symbol]
                else:
                    book_data = await connector.get_orderbook(symbol)
                    if not book_data or price_key not in book_data:
                        continue
                    book = book_data
                
                # Calculate available liquidity
                liquidity = Decimal('0')
                
                # For limit orders, consider liquidity at or better than limit price
                for level in book[price_key]:
                    level_price = Decimal(str(level[0]))
                    level_amount = Decimal(str(level[1]))
                    
                    if (side == OrderSide.BUY and level_price <= price) or \
                       (side == OrderSide.SELL and level_price >= price):
                        if liquidity_type == 'base':
                            # Use base currency amount
                            liquidity += level_amount
                        else:  # 'quote'
                            # Use quote currency amount (price * amount)
                            liquidity += level_price * level_amount
                
                if liquidity > 0:
                    # Store with original exchange name for clarity in logs
                    exchange_liquidity[exchange] = {
                        'liquidity': liquidity,
                        'connector_key': exchange_key
                    }
                    total_liquidity += liquidity
                    
            except Exception as e:
                self.logger.warning(f"Error calculating liquidity for {symbol} on {exchange}: {e}")
        
        if not exchange_liquidity:
            parent_order.reject(f"No exchange with liquidity for {symbol}")
            return
            
        # Log liquidity distribution
        self.logger.info(f"Liquidity distribution for {symbol} ({liquidity_type}):")
        for exchange, data in exchange_liquidity.items():
            percentage = (data['liquidity'] / total_liquidity) * 100
            self.logger.info(f"  {exchange} (connector: {data['connector_key']}): {data['liquidity']} ({percentage:.2f}%)")
            
        # Split order proportionally to liquidity
        tasks = []
        remaining_amount = total_amount
        
        # Sort exchanges by liquidity (highest first)
        sorted_exchanges = sorted(
            [(exchange, data) for exchange, data in exchange_liquidity.items()], 
            key=lambda x: x[1]['liquidity'], 
            reverse=True
        )
        
        # First pass: Allocate to exchanges with significant liquidity
        allocated_exchanges = []
        for exchange, data in sorted_exchanges:
            proportion = data['liquidity'] / total_liquidity
            
            # Skip exchanges with too little liquidity
            if proportion < min_liquidity_percent:
                self.logger.info(f"Skipping {exchange} due to insufficient liquidity proportion: {proportion:.2f}")
                continue
                
            exchange_amount = total_amount * proportion
            
            # Ensure minimum order size (using 0.00001 as a generic minimum)
            if exchange_amount >= Decimal('0.00001'):
                allocated_exchanges.append((exchange, data['connector_key'], exchange_amount))
                remaining_amount -= exchange_amount
        
        # Second pass: If we couldn't allocate all amount, try to allocate the remainder
        if remaining_amount > Decimal('0') and allocated_exchanges:
            # Distribute remaining amount proportionally among allocated exchanges
            total_allocated = sum(amount for _, _, amount in allocated_exchanges)
            for i, (exchange, connector_key, amount) in enumerate(allocated_exchanges):
                if total_allocated > 0:
                    additional = (amount / total_allocated) * remaining_amount
                    allocated_exchanges[i] = (exchange, connector_key, amount + additional)
        
        # Place orders on selected exchanges
        for exchange, connector_key, amount in allocated_exchanges:
            self.logger.info(f"Placing order on {exchange} (connector: {connector_key}): {amount} at {price}")
            
            tasks.append(
                self._create_child_order(
                    parent_order=parent_order,
                    exchange=connector_key,
                    amount=amount,
                    price=price
                )
            )
        
        if not tasks:
            parent_order.reject(f"No valid allocations for {symbol}")
            return
            
        # Wait for all child orders to be placed
        await asyncio.gather(*tasks)
    
    async def _start_algo_execution(
        self, 
        parent_order: Order, 
        strategy: ExecutionStrategy,
        exchanges: List[str],
        params: Dict[str, Any]
    ) -> None:
        """
        Start algorithmic execution.
        
        Args:
            parent_order: Parent order to execute
            strategy: Execution strategy
            exchanges: List of valid exchanges
            params: Strategy parameters
        """
        # Create task for algorithmic execution
        if strategy == ExecutionStrategy.TWAP:
            task = asyncio.create_task(
                self._execute_twap(parent_order, exchanges, params)
            )
        elif strategy == ExecutionStrategy.VWAP:
            task = asyncio.create_task(
                self._execute_vwap(parent_order, exchanges, params)
            )
        elif strategy == ExecutionStrategy.ICEBERG:
            task = asyncio.create_task(
                self._execute_iceberg(parent_order, exchanges, params)
            )
        else:
            parent_order.reject(f"Unsupported algorithm: {strategy}")
            return
            
        # Store task for cancellation
        self.algo_tasks[parent_order.id] = task
        
        # Mark order as being algorithmically executed
        parent_order.params['algo_execution'] = strategy.value
        
    async def _execute_twap(
        self, 
        parent_order: Order, 
        exchanges: List[str],
        params: Dict[str, Any]
    ) -> None:
        """
        Execute order using Time-Weighted Average Price algorithm.
        
        Args:
            parent_order: Parent order to execute
            exchanges: List of valid exchanges
            params: TWAP parameters
        """
        # Get TWAP parameters
        duration = params.get('duration', 3600)  # Default 1 hour
        num_slices = params.get('num_slices', 12)  # Default 12 slices
        
        # Calculate slice size and interval
        total_amount = parent_order.amount
        slice_amount = total_amount / Decimal(num_slices)
        interval = duration / num_slices
        
        try:
            # Execute slices at regular intervals
            for i in range(num_slices):
                # Check if parent order was cancelled
                if parent_order.status == OrderStatus.CANCELLED:
                    break
                    
                # Select best exchange for this slice
                await self._execute_best_price(
                    Order(
                        symbol=parent_order.symbol,
                        side=parent_order.side,
                        amount=slice_amount,
                        order_type=parent_order.order_type,
                        price=parent_order.price,
                        client_order_id=f"{parent_order.id}_slice_{i}"
                    ),
                    exchanges
                )
                
                # Wait for next interval
                await asyncio.sleep(interval)
                
            # Update parent order status
            if parent_order.status != OrderStatus.CANCELLED:
                parent_order.status = OrderStatus.FILLED
                
        except asyncio.CancelledError:
            # Execution was cancelled
            self.logger.info(f"TWAP execution cancelled for order {parent_order.id}")
            
        except Exception as e:
            self.logger.error(f"Error in TWAP execution for order {parent_order.id}: {e}")
            parent_order.reject(f"TWAP execution error: {str(e)}")
            
        finally:
            # Remove task from active tasks
            if parent_order.id in self.algo_tasks:
                del self.algo_tasks[parent_order.id]
    
    async def _execute_vwap(
        self, 
        parent_order: Order, 
        exchanges: List[str],
        params: Dict[str, Any]
    ) -> None:
        """
        Execute order using Volume-Weighted Average Price algorithm.
        
        Args:
            parent_order: Parent order to execute
            exchanges: List of valid exchanges
            params: VWAP parameters
        """
        # Get VWAP parameters
        duration = params.get('duration', 3600)  # Default 1 hour
        num_slices = params.get('num_slices', 12)  # Default 12 slices
        
        # TODO: Implement historical volume profile analysis for more accurate VWAP
        # For now, use simple approximation with increasing volumes
        
        total_amount = parent_order.amount
        interval = duration / num_slices
        
        try:
            # Create a simple volume profile (triangular distribution)
            volume_profile = []
            for i in range(num_slices):
                # Higher volume in the middle
                if i < num_slices / 2:
                    volume = (i + 1) / (num_slices / 2)
                else:
                    volume = (num_slices - i) / (num_slices / 2)
                volume_profile.append(volume)
                
            # Normalize to sum to 1
            total_volume = sum(volume_profile)
            volume_profile = [v / total_volume for v in volume_profile]
            
            # Execute slices according to volume profile
            for i, volume_ratio in enumerate(volume_profile):
                # Check if parent order was cancelled
                if parent_order.status == OrderStatus.CANCELLED:
                    break
                    
                slice_amount = total_amount * Decimal(str(volume_ratio))
                
                # Select best exchange for this slice
                await self._execute_best_price(
                    Order(
                        symbol=parent_order.symbol,
                        side=parent_order.side,
                        amount=slice_amount,
                        order_type=parent_order.order_type,
                        price=parent_order.price,
                        client_order_id=f"{parent_order.id}_slice_{i}"
                    ),
                    exchanges
                )
                
                # Wait for next interval
                await asyncio.sleep(interval)
                
            # Update parent order status
            if parent_order.status != OrderStatus.CANCELLED:
                parent_order.status = OrderStatus.FILLED
                
        except asyncio.CancelledError:
            # Execution was cancelled
            self.logger.info(f"VWAP execution cancelled for order {parent_order.id}")
            
        except Exception as e:
            self.logger.error(f"Error in VWAP execution for order {parent_order.id}: {e}")
            parent_order.reject(f"VWAP execution error: {str(e)}")
            
        finally:
            # Remove task from active tasks
            if parent_order.id in self.algo_tasks:
                del self.algo_tasks[parent_order.id]
                
    async def _execute_iceberg(
        self, 
        parent_order: Order, 
        exchanges: List[str],
        params: Dict[str, Any]
    ) -> None:
        """
        Execute order using Iceberg algorithm (showing only part of order at a time).
        
        Args:
            parent_order: Parent order to execute
            exchanges: List of valid exchanges
            params: Iceberg parameters
        """
        # Get Iceberg parameters
        display_size = params.get('display_size')
        exchange = params.get('exchange')
        
        if not display_size:
            # Default to 10% of total order size
            display_size = parent_order.amount * Decimal('0.1')
            
        # Use specified exchange or best price
        if not exchange or exchange not in exchanges:
            # Find exchange with best price
            await self._get_best_price_exchange(parent_order.symbol, parent_order.side, exchanges)
            
        if not exchange and exchanges:
            exchange = exchanges[0]
            
        if not exchange:
            parent_order.reject("No valid exchange for iceberg execution")
            return
            
        total_amount = parent_order.amount
        remaining = total_amount
        
        try:
            while remaining > Decimal('0') and parent_order.status != OrderStatus.CANCELLED:
                # Calculate size for this slice
                slice_size = min(display_size, remaining)
                
                # Create and place child order
                child_order = await self.order_manager.create_order(
                    symbol=parent_order.symbol,
                    side=parent_order.side,
                    amount=slice_size,
                    order_type=parent_order.order_type,
                    price=parent_order.price,
                    exchange=exchange,
                    client_order_id=f"{parent_order.id}_slice_{uuid.uuid4()}"
                )
                
                # Link to parent
                child_order.set_as_child(parent_order.id)
                parent_order.add_child_order(child_order.id)
                
                # Wait for child order to complete
                while child_order.status not in [OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED]:
                    # Check if parent was cancelled
                    if parent_order.status == OrderStatus.CANCELLED:
                        await self.order_manager.cancel_order(child_order.id)
                        break
                        
                    # Wait and refresh status
                    await asyncio.sleep(1)
                    child_order = await self.order_manager.get_order(child_order.id)
                    
                # Update remaining amount
                filled = child_order.filled_amount
                remaining -= filled
                
                # If nothing was filled and not cancelled, there might be an issue
                if filled == Decimal('0') and child_order.status != OrderStatus.CANCELLED:
                    self.logger.warning(f"Iceberg slice not filled for order {parent_order.id}")
                    # Wait before retrying
                    await asyncio.sleep(5)
                    
            # Update parent order status
            if remaining <= Decimal('0') and parent_order.status != OrderStatus.CANCELLED:
                parent_order.status = OrderStatus.FILLED
                
        except asyncio.CancelledError:
            # Execution was cancelled
            self.logger.info(f"Iceberg execution cancelled for order {parent_order.id}")
            
        except Exception as e:
            self.logger.error(f"Error in Iceberg execution for order {parent_order.id}: {e}")
            parent_order.reject(f"Iceberg execution error: {str(e)}")
            
        finally:
            # Remove task from active tasks
            if parent_order.id in self.algo_tasks:
                del self.algo_tasks[parent_order.id]

    # Helper methods
    async def _create_child_order(
        self, 
        parent_order: Order, 
        exchange: str,
        amount: Decimal,
        price: Optional[Decimal] = None
    ) -> Order:
        """
        Create and place child order on specific exchange.
        
        Args:
            parent_order: Parent order
            exchange: Exchange to place order on
            amount: Order amount
            price: Order price
            
        Returns:
            Created child order
        """
        # Create child order
        child_order = await self.order_manager.create_order(
            symbol=parent_order.symbol,
            side=parent_order.side,
            amount=amount,
            order_type=parent_order.order_type,
            price=price or parent_order.price,
            exchange=exchange,
            time_in_force=parent_order.time_in_force,
            reduce_only=parent_order.reduce_only,
            post_only=parent_order.post_only,
            leverage=parent_order.leverage,
            margin_type=parent_order.margin_type,
            params=parent_order.params.copy(),
            client_order_id=f"{parent_order.id}_child_{uuid.uuid4()}"
        )
        
        # Set parent/child relationship
        child_order.set_as_child(parent_order.id)
        parent_order.add_child_order(child_order.id)
        
        return child_order
    
    async def _get_best_price_exchange(
        self,
        symbol: str,
        side: OrderSide,
        exchanges: List[str]
    ) -> Optional[str]:
        """
        Find exchange with best price for symbol and side.
        
        Args:
            symbol: Trading symbol
            side: Buy or sell
            exchanges: List of exchanges to check
            
        Returns:
            Exchange with best price, or None if no valid exchanges
        """
        price_key = 'asks' if side == OrderSide.BUY else 'bids'
        best_price = None
        best_exchange = None
        
        for exchange in exchanges:
            try:
                # Check if orderbook is already cached
                if exchange in self.orderbooks and symbol in self.orderbooks[exchange]:
                    book = self.orderbooks[exchange][symbol]
                else:
                    # Fetch orderbook
                    connector = self.exchange_connectors[exchange]
                    book_data = await connector.get_orderbook(symbol)
                    if not book_data or price_key not in book_data or not book_data[price_key]:
                        continue
                    book = book_data
                
                # Get best price
                if not book[price_key]:
                    continue
                
                exchange_price = Decimal(str(book[price_key][0][0]))
                
                # Compare with current best
                if best_price is None or (side == OrderSide.BUY and exchange_price < best_price) or \
                   (side == OrderSide.SELL and exchange_price > best_price):
                    best_price = exchange_price
                    best_exchange = exchange
                    
            except Exception as e:
                self.logger.warning(f"Error getting best price for {symbol} on {exchange}: {e}")
                
        return best_exchange
    
    async def _load_exchange_metadata(self) -> None:
        """Load exchange metadata including fees and latencies."""
        # Initialize exchange fees
        for exchange in self.exchange_connectors:
            self.exchange_fees[exchange] = {}
            self.exchange_latencies[exchange] = 0
            
            try:
                # Get exchange info
                connector = self.exchange_connectors[exchange]
                exchange_info = await connector.get_exchange_info()
                
                # Extract fee information
                if 'trading_fees' in exchange_info:
                    self.exchange_fees[exchange] = exchange_info['trading_fees']
                    
                # Initialize latencies
                self.exchange_latencies[exchange] = exchange_info.get('latency', 0)
                
            except Exception as e:
                self.logger.warning(f"Error loading metadata for {exchange}: {e}")
    
    async def _on_order_update(self, order: Order) -> None:
        """
        Handle order updates.
        
        Args:
            order: Updated order
        """
        # Check if order is a child order
        if order.parent_order_id and order.parent_order_id in self.order_manager.orders_by_id:
            parent_order = self.order_manager.orders_by_id[order.parent_order_id]
            
            # Update parent order status based on children
            if order.status == OrderStatus.FILLED:
                # Check if all child orders are filled
                all_filled = True
                total_filled = Decimal('0')
                
                for child_id in parent_order.child_orders:
                    if child_id in self.order_manager.orders_by_id:
                        child = self.order_manager.orders_by_id[child_id]
                        if child.status != OrderStatus.FILLED:
                            all_filled = False
                        total_filled += child.filled_amount
                
                # Update parent order filled amount
                parent_order.filled_amount = total_filled
                parent_order.remaining_amount = parent_order.amount - total_filled
                
                # If all children filled, mark parent as filled
                if all_filled:
                    parent_order._update_status(OrderStatus.FILLED, "All child orders filled")
                else:
                    parent_order._update_status(OrderStatus.PARTIALLY_FILLED, f"Filled: {total_filled}/{parent_order.amount}")
            
            elif order.status == OrderStatus.PARTIALLY_FILLED:
                # Update parent order filled amount
                total_filled = Decimal('0')
                
                for child_id in parent_order.child_orders:
                    if child_id in self.order_manager.orders_by_id:
                        child = self.order_manager.orders_by_id[child_id]
                        total_filled += child.filled_amount
                
                parent_order.filled_amount = total_filled
                parent_order.remaining_amount = parent_order.amount - total_filled
                
                parent_order._update_status(OrderStatus.PARTIALLY_FILLED, f"Filled: {total_filled}/{parent_order.amount}")
            
            elif order.status == OrderStatus.CANCELLED:
                # Check if all children are cancelled
                all_cancelled = True
                
                for child_id in parent_order.child_orders:
                    if child_id in self.order_manager.orders_by_id:
                        child = self.order_manager.orders_by_id[child_id]
                        if child.status not in [OrderStatus.CANCELLED, OrderStatus.REJECTED]:
                            all_cancelled = False
                
                # If all children cancelled and no fills, mark parent as cancelled
                if all_cancelled and parent_order.filled_amount == Decimal('0'):
                    parent_order._update_status(OrderStatus.CANCELLED, "All child orders cancelled")
    
    async def _on_order_fill(self, order: Order, fill_data: Dict[str, Any]) -> None:
        """
        Handle order fills.
        
        Args:
            order: Filled order
            fill_data: Fill data
        """
        # Update parent order for child fills
        if order.parent_order_id and order.parent_order_id in self.order_manager.orders_by_id:
            parent_order = self.order_manager.orders_by_id[order.parent_order_id]
            
            # Update parent order filled amount
            total_filled = Decimal('0')
            avg_price = Decimal('0')
            total_cost = Decimal('0')
            
            for child_id in parent_order.child_orders:
                if child_id in self.order_manager.orders_by_id:
                    child = self.order_manager.orders_by_id[child_id]
                    child_filled = child.filled_amount
                    total_filled += child_filled
                    
                    # Calculate average price
                    if child.avg_fill_price:
                        total_cost += child_filled * child.avg_fill_price
            
            parent_order.filled_amount = total_filled
            parent_order.remaining_amount = parent_order.amount - total_filled
            
            # Calculate average fill price
            if total_filled > 0:
                parent_order.avg_fill_price = total_cost / total_filled
            
            # Update status
            if total_filled >= parent_order.amount:
                parent_order._update_status(OrderStatus.FILLED, "Order completely filled")
            elif total_filled > 0:
                parent_order._update_status(OrderStatus.PARTIALLY_FILLED, f"Filled: {total_filled}/{parent_order.amount}")
                
    async def cancel_execution(self, order_id: str) -> bool:
        """
        Cancel order execution.
        
        Args:
            order_id: Order ID to cancel
            
        Returns:
            True if cancellation was successful
        """
        # Cancel algorithmic execution task
        if order_id in self.algo_tasks:
            self.algo_tasks[order_id].cancel()
            del self.algo_tasks[order_id]
            
        # Cancel the order and all child orders
        return await self.order_manager.cancel_order(order_id)
        
    async def modify_execution(
        self, 
        order_id: str,
        new_price: Optional[Decimal] = None,
        new_amount: Optional[Decimal] = None
    ) -> bool:
        """
        Modify order execution.
        
        Args:
            order_id: Order ID to modify
            new_price: New price
            new_amount: New amount
            
        Returns:
            True if modification was successful
        """
        if order_id not in self.order_manager.orders_by_id:
            return False
            
        order = self.order_manager.orders_by_id[order_id]
        
        # Cannot modify filled or cancelled orders
        if order.status in [OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED]:
            return False
            
        # For algorithmic execution, cancel and restart
        if order_id in self.algo_tasks:
            # Get execution parameters
            strategy = ExecutionStrategy(order.params.get('algo_execution', 'best_price'))
            
            # Cancel current execution
            await self.cancel_execution(order_id)
            
            # Update order
            if new_price is not None:
                order.price = new_price
                
            if new_amount is not None:
                order.amount = new_amount
                order.remaining_amount = new_amount - order.filled_amount
                
            # Restart execution
            valid_exchanges = []
            for exchange in self.exchange_connectors:
                try:
                    connector = self.exchange_connectors[exchange]
                    if await connector.has_symbol(order.symbol):
                        valid_exchanges.append(exchange)
                except:
                    pass
                    
            await self._start_algo_execution(order, strategy, valid_exchanges, order.params)
            return True
            
        # For regular execution, cancel and replace all child orders
        success = True
        
        # Cancel all child orders
        for child_id in order.child_orders:
            if child_id in self.order_manager.orders_by_id:
                child_success = await self.order_manager.cancel_order(child_id)
                success = success and child_success
                
        # Update order
        if new_price is not None:
            order.price = new_price
            
        if new_amount is not None:
            order.amount = new_amount
            order.remaining_amount = new_amount - order.filled_amount
            
        # Create new child orders
        # (Use the same execution strategy that was used originally)
        if 'execution_strategy' in order.params:
            strategy = ExecutionStrategy(order.params['execution_strategy'])
        else:
            strategy = ExecutionStrategy.BEST_PRICE
            
        valid_exchanges = []
        for exchange in self.exchange_connectors:
            try:
                connector = self.exchange_connectors[exchange]
                if await connector.has_symbol(order.symbol):
                    valid_exchanges.append(exchange)
            except:
                pass
                
        if strategy == ExecutionStrategy.BEST_PRICE:
            await self._execute_best_price(order, valid_exchanges)
        elif strategy == ExecutionStrategy.BEST_LIQUIDITY:
            await self._execute_best_liquidity(order, valid_exchanges)
        elif strategy == ExecutionStrategy.LOWEST_FEES:
            await self._execute_lowest_fees(order, valid_exchanges)
        elif strategy == ExecutionStrategy.SPLIT_EVENLY:
            await self._execute_split_evenly(order, valid_exchanges)
        elif strategy == ExecutionStrategy.SPLIT_BY_LIQUIDITY:
            await self._execute_split_by_liquidity(order, valid_exchanges)
        else:
            await self._execute_best_price(order, valid_exchanges)
            
        return success

    async def _execute_price_optimized_liquidity(self, parent_order: Order, exchanges: List[str]) -> None:
        """
        Execute order by optimizing price for a given liquidity requirement.
        
        This strategy finds the best possible price across exchanges while ensuring
        there is enough liquidity to fill the entire order. It prioritizes price over
        exchange diversification.
        
        Args:
            parent_order: Parent order to execute
            exchanges: List of valid exchanges
        """
        symbol = parent_order.symbol
        side = parent_order.side
        total_amount = parent_order.amount
        price = parent_order.price
        params = parent_order.params or {}
        
        # Get parameters
        liquidity_type = params.get('liquidity_type', 'base')
        target_price = params.get('target_price', price)
        max_slippage_percent = Decimal(str(params.get('max_slippage_percent', 0.5))) / 100
        include_spot = params.get('include_spot', True)
        include_perp = params.get('include_perp', True)
        
        # For buy orders, we want to minimize price (lower is better)
        # For sell orders, we want to maximize price (higher is better)
        is_buy = side == OrderSide.BUY
        price_key = 'asks' if is_buy else 'bids'
        
        # Calculate acceptable price range
        if is_buy:
            max_acceptable_price = price * (1 + max_slippage_percent)
        else:
            max_acceptable_price = price * (1 - max_slippage_percent)
            
        # Filter exchanges based on spot/perp preferences
        filtered_exchanges = []
        for exchange in exchanges:
            if '_' in exchange:
                exchange_name, market_type = exchange.split('_', 1)
                if (market_type == 'spot' and not include_spot) or \
                   (market_type in ['perp', 'future', 'futures'] and not include_perp):
                    continue
            filtered_exchanges.append(exchange)
            
        if not filtered_exchanges:
            parent_order.reject(f"No valid exchanges after filtering spot/perp preferences")
            return
            
        # Collect orderbook data from all exchanges
        exchange_orderbooks = {}
        
        for exchange in filtered_exchanges:
            try:
                # Get connector key for exchange
                exchange_key = self.get_connector_key(exchange)
                if not exchange_key:
                    self.logger.warning(f"No connector found for {exchange}")
                    continue
                    
                # Get orderbook
                connector = self.exchange_connectors[exchange_key]
                if exchange_key in self.orderbooks and symbol in self.orderbooks[exchange_key]:
                    book = self.orderbooks[exchange_key][symbol]
                else:
                    book_data = await connector.get_orderbook(symbol)
                    if not book_data or price_key not in book_data:
                        continue
                    book = book_data
                    
                exchange_orderbooks[exchange] = {
                    'book': book,
                    'connector_key': exchange_key
                }
                    
            except Exception as e:
                self.logger.warning(f"Error fetching orderbook for {symbol} on {exchange}: {e}")
                
        if not exchange_orderbooks:
            parent_order.reject(f"Could not fetch valid orderbooks for {symbol}")
            return
            
        # Collect all price levels from all exchanges
        all_levels = []
        
        for exchange, data in exchange_orderbooks.items():
            book = data['book']
            connector_key = data['connector_key']
            
            for i, level in enumerate(book[price_key]):
                level_price = Decimal(str(level[0]))
                level_amount = Decimal(str(level[1]))
                
                # Skip if price is outside acceptable range
                if (is_buy and level_price > max_acceptable_price) or \
                   (not is_buy and level_price < max_acceptable_price):
                    continue
                    
                # Calculate quote amount if needed
                quote_amount = level_price * level_amount
                
                # Use either base or quote amount based on liquidity_type
                amount = level_amount if liquidity_type == 'base' else quote_amount
                
                all_levels.append({
                    'exchange': exchange,
                    'connector_key': connector_key,
                    'price': level_price,
                    'amount': amount,
                    'base_amount': level_amount,
                    'quote_amount': quote_amount,
                    'level': i
                })
        
        # Sort levels by price (ascending for buy, descending for sell)
        all_levels.sort(key=lambda x: x['price'], reverse=not is_buy)
        
        # Log available levels
        self.logger.info(f"Available price levels for {symbol} ({liquidity_type}):")
        for level in all_levels[:10]:  # Log first 10 levels
            self.logger.info(f"  {level['exchange']} (level {level['level']}): "
                           f"price={level['price']}, amount={level['amount']}")
            
        # Allocate order to best price levels until filled
        allocations = []
        remaining_amount = total_amount
        
        for level in all_levels:
            if remaining_amount <= 0:
                break
                
            # Calculate how much we can allocate to this level
            level_amount = level['base_amount']
            alloc_amount = min(remaining_amount, level_amount)
            
            # Add allocation
            allocations.append({
                'exchange': level['exchange'],
                'connector_key': level['connector_key'],
                'price': level['price'],
                'amount': alloc_amount
            })
            
            # Update remaining amount
            remaining_amount -= alloc_amount
            
        # If we couldn't fill the entire order
        if remaining_amount > 0:
            self.logger.warning(f"Could not find enough liquidity to fill entire order. "
                              f"Remaining: {remaining_amount}/{total_amount}")
            
        # Group allocations by exchange
        exchange_allocations = {}
        for alloc in allocations:
            exchange = alloc['exchange']
            connector_key = alloc['connector_key']
            
            if exchange not in exchange_allocations:
                exchange_allocations[exchange] = {
                    'connector_key': connector_key,
                    'total_amount': Decimal('0'),
                    'allocations': []
                }
                
            exchange_allocations[exchange]['total_amount'] += alloc['amount']
            exchange_allocations[exchange]['allocations'].append(alloc)
            
        # Log allocation plan
        self.logger.info(f"Allocation plan for {symbol}:")
        for exchange, data in exchange_allocations.items():
            avg_price = sum(a['price'] * a['amount'] for a in data['allocations']) / data['total_amount']
            self.logger.info(f"  {exchange}: {data['total_amount']} at avg price {avg_price}")
            
        # Place orders on exchanges
        tasks = []
        for exchange, data in exchange_allocations.items():
            connector_key = data['connector_key']
            amount = data['total_amount']
            
            # Calculate volume-weighted average price for this exchange
            vwap = sum(a['price'] * a['amount'] for a in data['allocations']) / amount
            
            # Use either the original limit price or the VWAP, depending on which is better for the user
            order_price = price
            if (is_buy and vwap < price) or (not is_buy and vwap > price):
                # If we can get a better price, use it
                order_price = vwap
                
            self.logger.info(f"Placing order on {exchange} (connector: {connector_key}): "
                           f"{amount} at {order_price} (VWAP: {vwap})")
            
            tasks.append(
                self._create_child_order(
                    parent_order=parent_order,
                    exchange=connector_key,
                    amount=amount,
                    price=order_price
                )
            )
            
        if not tasks:
            parent_order.reject(f"No valid allocations for {symbol}")
            return
            
        # Wait for all child orders to be placed
        await asyncio.gather(*tasks)

    async def _execute_minimal_market_impact(self, parent_order: Order, exchanges: List[str]) -> None:
        """
        Execute order by minimizing market impact, spending as little as possible.
        
        This strategy carefully distributes the order across exchanges to minimize
        price impact. It uses a layered approach, using only a portion of available
        liquidity at each price level across all exchanges.
        
        Args:
            parent_order: Parent order to execute
            exchanges: List of valid exchanges
        """
        symbol = parent_order.symbol
        side = parent_order.side
        total_amount = parent_order.amount
        price = parent_order.price
        params = parent_order.params or {}
        
        # Get parameters
        liquidity_type = params.get('liquidity_type', 'base')
        max_price_impact_percent = Decimal(str(params.get('max_price_impact_percent', 0.5))) / 100
        price_levels = int(params.get('price_levels', 10))
        include_spot = params.get('include_spot', True)
        include_perp = params.get('include_perp', True)
        
        # For buy orders, we want to minimize price increase
        # For sell orders, we want to minimize price decrease
        is_buy = side == OrderSide.BUY
        price_key = 'asks' if is_buy else 'bids'
        
        # Calculate maximum acceptable price
        if is_buy:
            max_acceptable_price = price * (1 + max_price_impact_percent)
        else:
            max_acceptable_price = price * (1 - max_price_impact_percent)
            
        # Filter exchanges based on spot/perp preferences
        filtered_exchanges = []
        for exchange in exchanges:
            if '_' in exchange:
                exchange_name, market_type = exchange.split('_', 1)
                if (market_type == 'spot' and not include_spot) or \
                   (market_type in ['perp', 'future', 'futures'] and not include_perp):
                    continue
            filtered_exchanges.append(exchange)
            
        if not filtered_exchanges:
            parent_order.reject(f"No valid exchanges after filtering spot/perp preferences")
            return
            
        # Collect orderbook data from all exchanges
        exchange_orderbooks = {}
        
        for exchange in filtered_exchanges:
            try:
                # Get connector key for exchange
                exchange_key = self.get_connector_key(exchange)
                if not exchange_key:
                    self.logger.warning(f"No connector found for {exchange}")
                    continue
                    
                # Get orderbook
                connector = self.exchange_connectors[exchange_key]
                if exchange_key in self.orderbooks and symbol in self.orderbooks[exchange_key]:
                    book = self.orderbooks[exchange_key][symbol]
                else:
                    book_data = await connector.get_orderbook(symbol)
                    if not book_data or price_key not in book_data:
                        continue
                    book = book_data
                    
                exchange_orderbooks[exchange] = {
                    'book': book,
                    'connector_key': exchange_key
                }
                    
            except Exception as e:
                self.logger.warning(f"Error fetching orderbook for {symbol} on {exchange}: {e}")
                
        if not exchange_orderbooks:
            parent_order.reject(f"Could not fetch valid orderbooks for {symbol}")
            return
        
        # Organize orderbook data by price level across exchanges
        # This helps us use a small portion of liquidity at each price level
        price_levels_map = {}
        
        for exchange, data in exchange_orderbooks.items():
            book = data['book']
            connector_key = data['connector_key']
            
            for i, level in enumerate(book[price_key][:price_levels]):  # Limit to specified price levels
                level_price = Decimal(str(level[0]))
                level_amount = Decimal(str(level[1]))
                
                # Skip if price is outside acceptable range
                if (is_buy and level_price > max_acceptable_price) or \
                   (not is_buy and level_price < max_acceptable_price):
                    continue
                
                # Create or update price level entry
                if level_price not in price_levels_map:
                    price_levels_map[level_price] = []
                    
                price_levels_map[level_price].append({
                    'exchange': exchange,
                    'connector_key': connector_key,
                    'amount': level_amount,
                    'price': level_price,
                    'level': i
                })
        
        # Sort price levels (ascending for buy, descending for sell)
        sorted_prices = sorted(price_levels_map.keys(), reverse=not is_buy)
        
        # Log available price levels
        self.logger.info(f"Available price levels for {symbol}:")
        for price_level in sorted_prices[:5]:  # Log first 5 price levels
            total_amount_at_level = sum(item['amount'] for item in price_levels_map[price_level])
            self.logger.info(f"  Price {price_level}: Total amount {total_amount_at_level} "
                           f"across {len(price_levels_map[price_level])} exchanges")
        
        # Allocate order using minimal impact approach
        allocations = []
        remaining_amount = total_amount
        
        # First pass: Use a small percentage of each price level across all exchanges
        for price_level in sorted_prices:
            if remaining_amount <= 0:
                break
                
            exchanges_at_level = price_levels_map[price_level]
            total_level_amount = sum(item['amount'] for item in exchanges_at_level)
            
            # Use only a portion of available liquidity at this level
            # The deeper we go into the orderbook, the less we want to use
            usage_factor = Decimal('0.2')  # Start with using 20% of available liquidity
            
            usable_amount = total_level_amount * usage_factor
            alloc_amount = min(remaining_amount, usable_amount)
            
            if alloc_amount <= 0:
                continue
                
            # Distribute proportionally across exchanges at this price level
            for item in exchanges_at_level:
                exchange_proportion = item['amount'] / total_level_amount
                exchange_alloc = alloc_amount * exchange_proportion
                
                # Ensure minimum order size
                if exchange_alloc >= Decimal('0.00001'):
                    allocations.append({
                        'exchange': item['exchange'],
                        'connector_key': item['connector_key'],
                        'price': price_level,
                        'amount': exchange_alloc
                    })
                    
            # Update remaining amount
            remaining_amount -= alloc_amount
        
        # Second pass: If we still have remaining amount, use more aggressive allocation
        if remaining_amount > 0:
            self.logger.info(f"First pass allocation insufficient, proceeding with second pass. "
                           f"Remaining: {remaining_amount}")
            
            for price_level in sorted_prices:
                if remaining_amount <= 0:
                    break
                    
                exchanges_at_level = price_levels_map[price_level]
                total_level_amount = sum(item['amount'] for item in exchanges_at_level)
                
                # Use more of the available liquidity
                usage_factor = Decimal('0.5')  # Use 50% of available liquidity
                
                usable_amount = total_level_amount * usage_factor
                alloc_amount = min(remaining_amount, usable_amount)
                
                if alloc_amount <= 0:
                    continue
                    
                # Distribute proportionally across exchanges at this price level
                for item in exchanges_at_level:
                    exchange_proportion = item['amount'] / total_level_amount
                    exchange_alloc = alloc_amount * exchange_proportion
                    
                    # Ensure minimum order size
                    if exchange_alloc >= Decimal('0.00001'):
                        # Check if we already have an allocation for this exchange
                        existing = False
                        for alloc in allocations:
                            if alloc['exchange'] == item['exchange'] and alloc['price'] == price_level:
                                alloc['amount'] += exchange_alloc
                                existing = True
                                break
                                
                        if not existing:
                            allocations.append({
                                'exchange': item['exchange'],
                                'connector_key': item['connector_key'],
                                'price': price_level,
                                'amount': exchange_alloc
                            })
                        
                # Update remaining amount
                remaining_amount -= alloc_amount
        
        # If we still have remaining amount, warn but proceed with what we have
        if remaining_amount > 0:
            self.logger.warning(f"Could not allocate entire order while maintaining minimal market impact. "
                              f"Unallocated: {remaining_amount}/{total_amount}")
            
        # Group allocations by exchange
        exchange_allocations = {}
        for alloc in allocations:
            exchange = alloc['exchange']
            connector_key = alloc['connector_key']
            
            if exchange not in exchange_allocations:
                exchange_allocations[exchange] = {
                    'connector_key': connector_key,
                    'total_amount': Decimal('0'),
                    'allocations': []
                }
                
            exchange_allocations[exchange]['total_amount'] += alloc['amount']
            exchange_allocations[exchange]['allocations'].append(alloc)
            
        # Log allocation plan
        self.logger.info(f"Allocation plan for {symbol} (minimal impact):")
        for exchange, data in exchange_allocations.items():
            avg_price = sum(a['price'] * a['amount'] for a in data['allocations']) / data['total_amount']
            self.logger.info(f"  {exchange}: {data['total_amount']} at avg price {avg_price}")
            
        # Place orders on exchanges
        tasks = []
        for exchange, data in exchange_allocations.items():
            connector_key = data['connector_key']
            amount = data['total_amount']
            
            # Calculate volume-weighted average price for this exchange
            vwap = sum(a['price'] * a['amount'] for a in data['allocations']) / amount
            
            # Use the VWAP price for this exchange
            self.logger.info(f"Placing order on {exchange} (connector: {connector_key}): "
                           f"{amount} at {vwap}")
            
            tasks.append(
                self._create_child_order(
                    parent_order=parent_order,
                    exchange=connector_key,
                    amount=amount,
                    price=vwap
                )
            )
            
        if not tasks:
            parent_order.reject(f"No valid allocations for {symbol}")
            return
            
        # Wait for all child orders to be placed
        await asyncio.gather(*tasks)
