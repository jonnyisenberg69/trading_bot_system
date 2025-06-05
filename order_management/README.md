# Order Management and Execution System

This module provides comprehensive order management and smart execution capabilities for the trading bot system.

## Components

1. **Order**: Unified order model with support for all order types, statuses, and parameters
2. **OrderManager**: Manages orders across multiple exchanges with unified interface
3. **ExecutionEngine**: Provides smart order routing and advanced execution strategies

## Execution Strategies

The `ExecutionEngine` supports several execution strategies:

- **BEST_PRICE**: Routes order to exchange with best price
- **BEST_LIQUIDITY**: Routes order to exchange with most liquidity
- **LOWEST_FEES**: Routes order to exchange with lowest fees
- **SPLIT_EVENLY**: Splits order evenly across exchanges
- **SPLIT_BY_LIQUIDITY**: Splits order proportionally to available liquidity
- **PRICE_OPTIMIZED_LIQUIDITY**: Optimizes price for a given liquidity requirement
- **MINIMAL_MARKET_IMPACT**: Minimizes price impact by using as little liquidity as possible
- **ICEBERG**: Shows only a portion of order at a time
- **TWAP**: Time-Weighted Average Price algorithm
- **VWAP**: Volume-Weighted Average Price algorithm

## Usage Examples

### Basic Order Execution

```python
# Initialize necessary components
order_manager = OrderManager(exchange_connectors)
execution_engine = ExecutionEngine(order_manager, exchange_connectors, orderbooks)

# Execute order with best price strategy
parent_order = await execution_engine.execute_order(
    symbol='BTC/USDT',
    side=OrderSide.BUY,
    amount=Decimal('0.1'),
    order_type=OrderType.LIMIT,
    price=Decimal('50000'),
    strategy=ExecutionStrategy.BEST_PRICE
)
```

### SPLIT_BY_LIQUIDITY Strategy

The `SPLIT_BY_LIQUIDITY` strategy splits orders across exchanges proportionally to their available liquidity. This is especially useful for large orders to minimize market impact.

```python
# Split order by base currency liquidity
parent_order = await execution_engine.execute_order(
    symbol='ETH/USDT',
    side=OrderSide.BUY,
    amount=Decimal('10.0'),
    order_type=OrderType.LIMIT,  # Always uses limit orders
    price=Decimal('3000'),
    strategy=ExecutionStrategy.SPLIT_BY_LIQUIDITY,
    exchanges=['binance_spot', 'binance_perp', 'bybit_spot', 'bybit_perp'],  # Specify market types
    params={
        'liquidity_type': 'base',  # Use base currency for liquidity calculation
        'min_liquidity_percent': 0.05,  # Skip exchanges with less than 5% of total liquidity
        'include_spot': True,  # Include spot markets
        'include_perp': True   # Include perpetual futures markets
    }
)

# Split order by quote currency liquidity
parent_order = await execution_engine.execute_order(
    symbol='BTC/USDT',
    side=OrderSide.SELL,
    amount=Decimal('1.0'),
    order_type=OrderType.LIMIT,
    price=Decimal('50000'),
    strategy=ExecutionStrategy.SPLIT_BY_LIQUIDITY,
    exchanges=['binance_spot', 'bybit_spot'],  # Only use spot markets
    params={
        'liquidity_type': 'quote',  # Use quote currency for liquidity calculation (price * amount)
        'min_liquidity_percent': 0.1,  # Skip exchanges with less than 10% of total liquidity
        'include_spot': True,
        'include_perp': False  # Exclude perpetual futures markets
    }
)
```

### PRICE_OPTIMIZED_LIQUIDITY Strategy

The `PRICE_OPTIMIZED_LIQUIDITY` strategy optimizes for price while ensuring sufficient liquidity. It prioritizes getting the best possible price for your order by searching across all exchanges.

```python
# Buy with price-optimized liquidity
parent_order = await execution_engine.execute_order(
    symbol='BTC/USDT',
    side=OrderSide.BUY,
    amount=Decimal('2.0'),
    order_type=OrderType.LIMIT,
    price=Decimal('50000'),
    strategy=ExecutionStrategy.PRICE_OPTIMIZED_LIQUIDITY,
    exchanges=['binance_spot', 'binance_perp', 'bybit_spot', 'bybit_perp'],
    params={
        'max_slippage_percent': 0.3,  # Maximum slippage from limit price (0.3%)
        'liquidity_type': 'base',  # Use base currency for liquidity calculation
        'include_spot': True,
        'include_perp': True
    }
)
```

### MINIMAL_MARKET_IMPACT Strategy

The `MINIMAL_MARKET_IMPACT` strategy minimizes price impact by using as little liquidity as possible from each price level across exchanges. This is ideal for executing large orders with minimal market disturbance.

```python
# Sell with minimal market impact
parent_order = await execution_engine.execute_order(
    symbol='ETH/USDT',
    side=OrderSide.SELL,
    amount=Decimal('50.0'),  # Large order
    order_type=OrderType.LIMIT,
    price=Decimal('3000'),
    strategy=ExecutionStrategy.MINIMAL_MARKET_IMPACT,
    exchanges=['binance_spot', 'binance_perp', 'bybit_spot', 'bybit_perp'],
    params={
        'max_price_impact_percent': 0.2,  # Maximum allowable price impact (0.2%)
        'price_levels': 15,  # Use up to 15 price levels per exchange
        'liquidity_type': 'base',
        'include_spot': True,
        'include_perp': True
    }
)
```

### Exchange Naming Convention

When using exchange names with market types, use the following format:

- `{exchange}_spot`: For spot markets (e.g., `binance_spot`, `bybit_spot`)
- `{exchange}_perp`: For perpetual futures markets (e.g., `binance_perp`, `bybit_perp`)

The execution engine will automatically map these names to the appropriate exchange connectors.

## Advanced Features

### Order Cancellation

```python
# Cancel an order
success = await execution_engine.cancel_execution(order_id)
```

### Order Modification

```python
# Modify an existing order
success = await execution_engine.modify_execution(
    order_id=order_id,
    new_price=Decimal('51000'),
    new_amount=Decimal('0.15')
)
``` 