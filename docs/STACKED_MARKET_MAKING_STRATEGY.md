# Stacked Market Making Strategy

## Overview

The Stacked Market Making Strategy is an advanced multi-line market making implementation that follows the design specifications outlined in `NEW_BOT.txt`. This strategy supports dual line types, sophisticated inventory management, and intelligent pricing across multiple venues.

## Key Features

### 1. Dual Line Architecture

#### Top of Book (TOB) Lines
- **Pricing**: Uses local exchange TOB pricing (`local bid + min_tick` for bids, `local ask - min_tick` for asks)
- **Definition**: Defined by min/max price zones rather than fixed spreads
- **Logic**: Independent per exchange, fast execution focused
- **Configuration**: Hourly quantity rates, coefficient-based adjustments

#### Passive Lines  
- **Pricing**: Uses aggregated book pricing from combined BASE/USDT orderbook
- **Definition**: Defined by base spread from reference price with continuous quoting
- **Logic**: Cross-exchange liquidity ladder maintenance
- **Configuration**: Mid-spread with min/max bounds, quantity with randomization

### 2. Inventory Management System

#### Inventory Coefficient Calculation
```
Coefficient = (excess_inventory - target_inventory) / target_inventory
```
- **Range**: -1 (max short) to +1 (max long), 0 at target
- **Usage**: Multiplied by spreads and quantities for position-aware adjustments
- **Example**:
  - Target: 1000 BERA, Current: 1500 BERA → Coefficient = +0.5 (lean short)
  - Target: 1000 BERA, Current: 500 BERA → Coefficient = -0.5 (lean long)

#### Inventory Price Methods
1. **Manual**: User-provided fixed inventory price
2. **Accounting**: Weighted average cost of current positions
3. **Mark-to-Market**: Current market price (updates real-time)

#### Position Risk Controls
- Maximum inventory deviation limits
- Side-specific quoting logic based on inventory price vs reference price
- Automatic position rebalancing suggestions

### 3. Volume Coefficient System (Using Existing Proven Technology)

#### Integration with Existing System
- **Uses**: `MovingAverageCalculator` from `volume_weighted_top_of_book.py`
- **Proven**: Production-tested O(1) SMA/EWMA calculations
- **Database**: Integrated with existing `TradeDataManager` and `risk_trades` table
- **Volume Types**: base, quote, imbalance, ratio, both (comprehensive coverage)

#### Coefficient Calculation (Existing Proven Logic)
- **Method**: Ratio-based calculation between shorter/longer period MAs
- **Options**: min, max, or mid of all calculated ratios
- **Bounds**: Configurable min/max coefficient limits (default: 0.2 - 3.0)
- **Per-Exchange**: Individual coefficients calculated per exchange

### 4. Enhanced Market Data Processing

#### Multi-Symbol Orderbook Aggregation
- **Combined Books**: BASE/USDT aggregated across selected venues
- **Cross-Currency**: BASE/NON-USDT books (BASE/BTC, BASE/ETH)
- **Conversion**: NON-USDT/USDT books for cross-conversion
- **Liquidity Shares**: Per-level tracking of component exchange contributions

#### Smart Pricing Selection
The strategy can choose between multiple pricing sources:

1. **Aggregated Price**: Combined book from all exchanges
2. **Component Venues**: Selected subset of exchanges (e.g., "Gate only")
3. **Liquidity Weighted**: Weight by available depth at each venue
4. **Single Venue**: Focus on specific exchange

### 5. Advanced Strategy Logic

#### Spread & Size Shaping
Spreads adjust based on:
- **Volume Coefficient**: Market activity level (busier → configurable tighter/wider)
- **Inventory**: Lean away from risk (widen toward overweight side)
- **Local Conditions**: Volatility and drift vs reference

Quantities change with:
- **Inventory Targets**: Position within allowed bands
- **Volume Coefficient**: Increase size during high-volume periods
- **Venue Risk/Limits**: Per-exchange balance considerations

#### Mode Fusion: Volume-Weighted + Passive
- Simultaneous operation of TOB and passive lines
- Independent configuration per line type
- TOB lines can be toggled on/off per side
- Passive lines maintain continuous ladders

## Implementation Architecture

### Core Components

1. **StackedMarketMakingStrategy**: Main strategy orchestrator
2. **InventoryManager**: Position and coefficient tracking
3. **VolumeCoefficiengEngine**: Volume statistics and coefficient calculation
4. **EnhancedAggregatedOrderbookManager**: Multi-venue book aggregation
5. **MultiReferencePricingEngine**: Advanced pricing calculations

### Data Flow

```
Market Data → Volume Engine → Inventory Manager
     ↓              ↓              ↓
Aggregated Books → Smart Pricing → Coefficient Calc
     ↓              ↓              ↓
TOB Lines ←→ Strategy Core ←→ Passive Lines
     ↓              ↓              ↓
Local Exchange Orders ← → Aggregated Orders
```

### Configuration Structure

```json
{
  "base_coin": "BERA",
  "quote_currencies": ["USDT", "BTC", "ETH"],
  "exchanges": ["binance", "bybit", "gateio"],
  
  "inventory": {
    "target_inventory": "1000.0",
    "max_inventory_deviation": "500.0", 
    "inventory_price_method": "accounting",
    "start_time": "2024-01-01T00:00:00Z"
  },
  
  "tob_lines": [{
    "hourly_quantity": "50.0",
    "spread_bps": "30.0",
    "timeout_seconds": 30,
    "coefficient_method": "volume",
    "sides": "both"
  }],
  
  "passive_lines": [{
    "mid_spread_bps": "15.0",
    "quantity": "25.0",
    "timeout_seconds": 120,
    "randomization_factor": "0.03",
    "quantity_coefficient_method": "volume"
  }],
  
  "time_periods": ["5min", "15min", "30min"],
  "coefficient_method": "min",
  "min_coefficient": "0.2",
  "max_coefficient": "3.0"
}
```

## Usage Examples

### Basic Setup

```python
from bot_manager.strategies.stacked_market_making import StackedMarketMakingStrategy
from config.stacked_market_making_config import ConfigTemplates

# Use pre-defined template
config = ConfigTemplates.conservative_bera_usdt()

# Create strategy instance
strategy = StackedMarketMakingStrategy(
    instance_id="stacked_bera_001",
    symbol="BERA/USDT", 
    exchanges=["binance", "bybit", "gateio"],
    config=config
)

# Start strategy
await strategy.initialize()
await strategy.start()
```

### Custom Configuration

```python
from config.stacked_market_making_config import StackedMarketMakingConfigBuilder
from bot_manager.strategies.inventory_manager import InventoryPriceMethod

# Build custom configuration
config = (StackedMarketMakingConfigBuilder()
    .set_basic_info('BERA', ['USDT'], ['binance', 'bybit'])
    .set_inventory_config(
        target_inventory=Decimal('1500'),
        max_deviation=Decimal('750'),
        price_method=InventoryPriceMethod.ACCOUNTING
    )
    .add_tob_line(
        hourly_quantity=Decimal('75'),
        spread_bps=Decimal('25'),
        coefficient_method='both'
    )
    .add_passive_line(
        mid_spread_bps=Decimal('12'),
        quantity=Decimal('40'),
        randomization_factor=Decimal('0.04')
    )
    .build()
)
```

## Monitoring and Performance

### Key Metrics

- **Inventory Coefficient**: Real-time position risk metric (-1 to +1)
- **Volume Coefficients**: Per-venue activity metrics
- **Line Performance**: Orders placed, fills, cancellations per line type
- **Smart Pricing Decisions**: Source selection efficiency
- **Cross-Currency Conversions**: Hedge execution statistics

### Performance Logging

The strategy provides comprehensive logging:
- Inventory state changes with coefficient updates
- Order placement with coefficient application
- Volume coefficient adjustments
- Smart pricing source selections
- Cross-venue liquidity share distributions

### Real-Time Monitoring

```python
# Get strategy state
state = strategy.get_strategy_state()

# Check inventory status
inventory = state['inventory_state']
print(f"Current inventory: {inventory['current_inventory']}")
print(f"Inventory coefficient: {inventory['inventory_coefficient']}")

# Check line status  
for tob_line in state['tob_lines']:
    print(f"TOB Line {tob_line['line_id']}: {tob_line['active_orders']} active orders")

for passive_line in state['passive_lines']:
    print(f"Passive Line {passive_line['line_id']}: coeff={passive_line['current_spread_coefficient']}")
```

## Integration with Existing System

### Market Data Integration
- **Compatible**: Works with existing `RedisOrderbookManager`
- **Enhanced**: Adds aggregation and cross-venue analytics  
- **Extensible**: Supports multi-symbol and cross-currency requirements

### Exchange Integration
- **Inherited**: Uses `BaseStrategy` order management
- **Exchange-Specific**: Handles symbol format conversion per venue
- **Error Handling**: Robust order placement and cancellation

### Risk Integration
- **Position Limits**: Integrates with max position controls
- **Inventory Bounds**: Enforces target ± max deviation limits
- **Taker Protection**: Prevents crossing spreads unless intended

## Comparison to Existing Strategies

| Feature | Market Making | Passive Quoting | Stacked Market Making |
|---------|---------------|-----------------|----------------------|
| Line Types | Single reference-based | Single passive | Dual (TOB + Passive) |
| Inventory Management | Basic position limits | None | Advanced coefficient system |
| Pricing Source | Single reference exchange | Average midpoint | Smart multi-source selection |
| Volume Adaptation | None | None | Dynamic coefficient adjustments |
| Cross-Currency | Manual hedging | None | Automatic hedge legs |
| Configuration Complexity | Medium | Low | High |
| Use Case | Simple arbitrage | Basic liquidity | Advanced market making |

## Future Enhancements

1. **Hedging Logic**: Complete implementation of cross-currency hedging
2. **Machine Learning**: Integrate ML models for coefficient optimization
3. **Risk Models**: Advanced correlation and volatility adjustments
4. **Performance Analytics**: Strategy performance attribution and optimization
5. **Dynamic Reconfiguration**: Runtime strategy parameter adjustments

## Troubleshooting

### Common Issues

1. **No Inventory Coefficient Updates**
   - Check inventory configuration
   - Verify trade confirmations are being received
   - Ensure inventory manager is receiving position updates

2. **Missing Aggregated Pricing**
   - Verify enhanced orderbook manager is running
   - Check Redis connectivity and orderbook data
   - Ensure required symbols are being tracked

3. **Volume Coefficients Not Updating**
   - Check volume engine configuration
   - Verify trade volume data is being captured
   - Ensure baseline volume is properly set

4. **Orders Not Placing**
   - Check taker check logic and local orderbook data
   - Verify exchange connectivity and symbol formatting
   - Check inventory limits and side quoting logic

### Debug Commands

```python
# Check inventory state
inventory_state = strategy.inventory_manager.get_inventory_state()
print(f"Inventory coefficient: {inventory_state.inventory_coefficient}")

# Check volume coefficients (using existing proven system)
coeff_status = strategy.coefficient_calculator.get_status()
print(f"Coefficient calculator status: {coeff_status}")

# Check exchange-specific coefficients
for exchange in strategy.exchanges:
    coeff = strategy.exchange_coefficients.get(exchange, 1.0)
    print(f"{exchange} coefficient: {coeff:.4f}")

# Check MA system status
ma_status = strategy.ma_calculator.get_status()
print(f"MA system: {ma_status['total_ma_configs']} configs, {ma_status['total_crossovers_detected']} crossovers")

# Check aggregated orderbook
agg_book = strategy.enhanced_orderbook_manager.get_aggregated_orderbook("BERA/USDT")
if agg_book:
    print(f"Aggregated book: {len(agg_book.contributing_exchanges)} exchanges")
```
