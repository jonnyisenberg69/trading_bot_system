# Position Tracking System

The position tracking system is responsible for monitoring and calculating trading positions across multiple exchanges and symbols. It provides a unified view of your trading positions, including net positions that account for both long and short positions across different exchanges.

## Key Components

1. **Position**: Tracks a position for a specific symbol on a specific exchange
2. **PositionManager**: Manages positions across all exchanges and symbols

## Position Tracking Logic

The system tracks the following values for each position:

- **P1**: Change in base coin (signed: positive for buying base, negative for selling)
- **P2**: Change in quote coin (signed: positive for selling, negative for buying)
- **P1 fee**: Absolute value of fee charged in base coin
- **P2 fee**: Absolute value of fee charged in quote coin

### Average Price Calculation

The average price is calculated using the formula:
```
avg_price = -(p2 + p2_fee) / p1
```

This formula ensures correct average price calculation regardless of whether you're buying or selling, and properly accounts for fees.

### Example

Imagine buying 1 BTC at 50,000 USDT with a 0.001 BTC fee:

- P1 = +1 BTC
- P2 = -50,000 USDT
- P1 fee = 0.001 BTC
- P2 fee = 0 USDT
- Avg price = -(-50,000 + 0) / 1 = 50,000 USDT/BTC

If you later sell 0.5 BTC at 52,000 USDT with a 13 USDT fee:

- P1 = +1 - 0.5 = +0.5 BTC
- P2 = -50,000 + 26,000 = -24,000 USDT
- P1 fee = 0.001 BTC
- P2 fee = 13 USDT
- Avg price = -(-24,000 + 13) / 0.5 = 48,026 USDT/BTC

## Net Position Calculation

The system also calculates net positions across all exchanges for each symbol by summing the P1, P2, P1 fee, and P2 fee values. This allows you to see your total exposure for each symbol.

## Integration with Order Management

The position tracking system integrates with the order management system through the `update_from_order` method. When an order is filled, the position tracking system automatically updates the relevant position.

## Usage

### Initialize Position Manager

```python
from order_management import PositionManager

# Initialize position manager
position_manager = PositionManager(data_dir="data/positions")
await position_manager.start()
```

### Update Position from Order Fill

```python
# Update position from order fill
await position_manager.update_from_order(order, fill_data)
```

### Update Position from Trade Data

```python
# Update position from trade data
await position_manager.update_from_trade("binance_spot", {
    'symbol': 'BTC/USDT',
    'id': 'trade1',
    'side': 'buy',
    'amount': 1.0,
    'price': 50000.0,
    'cost': 50000.0,
    'fee': {
        'cost': 0.001,
        'currency': 'BTC'
    }
})
```

### Get Position Information

```python
# Get position for specific exchange and symbol
position = position_manager.get_position("binance_spot", "BTC/USDT")

# Get all positions
positions = position_manager.get_all_positions()

# Get net position for symbol
net_position = position_manager.get_net_position("BTC/USDT")

# Get all net positions
net_positions = position_manager.get_all_net_positions()

# Get position summary
summary = position_manager.summarize_positions()
```

### Position Persistence

Positions are automatically saved to disk for persistence across restarts:

```python
# Save positions to disk
await position_manager.save_positions()

# Load positions from disk
await position_manager.load_positions()
```

## Exchange Naming Convention

The position tracking system uses the following naming convention for exchanges:

- `{exchange}_spot`: For spot markets (e.g., `binance_spot`, `bybit_spot`)
- `{exchange}_perp`: For perpetual futures markets (e.g., `binance_perp`, `bybit_perp`)

This ensures proper separation between spot and perpetual markets for the same exchange. 