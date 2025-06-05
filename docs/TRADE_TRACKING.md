# Trade Tracking and Position Monitoring

This document describes the trade tracking and position monitoring system in the trading bot.

## Overview

The trade tracking and position monitoring system is responsible for:

1. Tracking all trades executed on all supported exchanges
2. Calculating positions based on trade data
3. Providing real-time position information for trading strategies
4. Ensuring no trades are missed through redundant synchronization methods

## Components

The system consists of several components:

- **Database Models**: Define the structure for storing trades and positions
- **Repositories**: Provide access to the database for trades and positions
- **Trade Synchronizer**: Ensures all trades are captured from exchanges
- **Position Manager**: Calculates and tracks positions based on trades

## How Trade Tracking Works

The system uses a dual approach to ensure no trades are missed:

### 1. REST API Synchronization

- Periodically fetches historical trades from each exchange
- Tracks the last synchronized trade ID and timestamp
- Handles pagination and rate limiting
- Ensures all historical trades are captured

### 2. WebSocket Real-time Updates

- Maintains WebSocket connections to exchanges
- Receives real-time trade notifications
- Processes trades immediately as they occur
- Provides low-latency position updates

## Position Calculation

Positions are calculated using the following values:

- **P1**: Change in base coin (positive for buying base, negative for selling)
- **P2**: Change in quote coin (positive for selling, negative for buying)
- **P1 Fee**: Absolute value of fee charged in base coin
- **P2 Fee**: Absolute value of fee charged in quote coin

The average price is calculated as:
```
avg_price = -(p2 + p2_fee) / p1
```

## Database Schema

### Trades Table

Stores all trade information:

- `id`: Primary key
- `exchange_id`: Foreign key to exchanges table
- `bot_instance_id`: Foreign key to bot instances (optional)
- `exchange_trade_id`: Original trade ID from the exchange
- `symbol`: Trading symbol (e.g., BTC/USDT)
- `side`: buy or sell
- `amount`: Trade amount in base currency
- `price`: Execution price
- `cost`: Trade cost in quote currency
- `fee_cost`: Fee amount
- `fee_currency`: Currency of the fee
- `timestamp`: When the trade occurred
- `p1_delta`: Change in base currency
- `p2_delta`: Change in quote currency
- `processed`: Whether the trade has been processed for position calculation

### Positions Table

Stores current positions:

- `id`: Primary key
- `exchange_id`: Foreign key to exchanges table
- `bot_instance_id`: Foreign key to bot instances (optional)
- `symbol`: Trading symbol (e.g., BTC/USDT)
- `p1`: Base coin amount
- `p2`: Quote coin amount
- `p1_fee`: Fee in base coin
- `p2_fee`: Fee in quote currency
- `avg_price`: Average price
- `size`: Position size
- `side`: long, short, or null
- `is_open`: Whether the position is open
- `entry_time`: When the position was opened
- `last_update_time`: When the position was last updated

## Usage

### Initialization

```python
# Initialize database
await init_db()

# Create repositories
async for session in get_session():
    trade_repository = TradeRepository(session)
    position_repository = PositionRepository(session)
    break

# Create trade synchronizer
trade_sync = TradeSynchronizer(
    exchange_connectors=exchange_connectors,
    trade_repository=trade_repository,
    position_repository=position_repository,
    exchange_id_map=exchange_id_map
)

# Start tracking trades
await trade_sync.start({
    'binance_spot': ['BTC/USDT', 'ETH/USDT'],
    'bybit_perp': ['BTC/USDT']
})
```

### Querying Positions

```python
# Get position for a specific exchange and symbol
position = await position_repository.get_position(
    exchange_id=1,
    symbol='BTC/USDT'
)

# Get all open positions
positions = await position_repository.get_open_positions()

# Calculate net position across exchanges
net_position = await position_repository.calculate_net_position('BTC/USDT')
```

### Querying Trades

```python
# Get recent trades for a symbol
trades = await trade_repository.get_trades_by_symbol(
    symbol='BTC/USDT',
    exchange_id=1,
    limit=10
)

# Get trade statistics
stats = await trade_repository.get_trade_statistics(
    symbol='BTC/USDT',
    start_time=datetime.utcnow() - timedelta(days=7)
)
```

## Fault Tolerance

The system is designed to be fault-tolerant:

- Trades are persisted in the database to survive restarts
- Trade synchronization uses both REST API and WebSockets for redundancy
- Last synchronization points are saved to resume after interruptions
- Deduplication ensures the same trade isn't processed multiple times

## Performance Considerations

- The system uses asynchronous I/O for high throughput
- In-memory caching reduces database load
- Batch processing improves performance for position updates
- Rate limiting respects exchange API constraints 