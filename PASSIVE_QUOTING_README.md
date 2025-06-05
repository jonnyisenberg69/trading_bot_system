# Passive Quoting Trading Bot System

A comprehensive passive market-making strategy implementation with multi-line configuration, real-time drift management, and sub-5-second configuration updates.

## 🚀 Features

### Core Strategy Features
- ✅ **Multi-line Quote Management**: Configure multiple quote lines with different parameters
- ✅ **Timeout-based Cancellation**: Automatically cancel orders after specified time periods
- ✅ **Drift-based Cancellation**: Cancel orders when market moves beyond drift threshold
- ✅ **Quantity Randomization**: Add randomness to order sizes to avoid predictable patterns
- ✅ **Multi-exchange Support**: Quote simultaneously on multiple exchanges
- ✅ **Flexible Order Sides**: Configure each line for bids only, asks only, or both
- ✅ **Precision Handling**: Automatic rounding to exchange-specific precision requirements

### System Features
- ✅ **Quick Configuration Updates**: Stop bot → Cancel orders → Update config → Restart in <5 seconds
- ✅ **Configuration Presets**: Save and load different strategy configurations
- ✅ **Real-time Monitoring**: Live performance tracking and order status
- ✅ **Client Order ID Format**: `pass_quote_{SIDE}_{timestamp_unix_ms}`
- ✅ **Modern Web UI**: Intuitive React-based configuration interface
- ✅ **REST API**: Complete API for programmatic control
- ✅ **Configuration Persistence**: JSON-based configuration storage

## 📋 Configuration Parameters

### Global Configuration (ALL LINES)
- **Base Coin**: The base asset symbol (e.g., BTC, ETH)
- **Quantity Currency**: Whether quantities are in base or quote currency
- **Exchanges**: List of exchanges to trade on

### Per-Line Configuration (EACH LINE)
- **Timeout (seconds)**: Maximum time before canceling orders
- **Drift (bps)**: Price movement threshold for cancellation (basis points)
- **Quantity**: Order size
- **Quantity Randomization Factor (%)**: Randomization percentage (0-100%)
- **Spread (bps)**: Distance from midpoint in basis points
- **Sides**: Order placement sides (both/bid/offer)

## 🏗️ Architecture

```
trading_bot_system/
├── bot_manager/
│   └── strategies/
│       ├── base_strategy.py         # Base strategy class
│       └── passive_quoting.py       # Passive quoting implementation
├── api/
│   ├── routes/
│   │   ├── bot_routes.py           # Bot management endpoints
│   │   └── strategy_routes.py      # Strategy-specific endpoints
│   └── services/
│       └── bot_manager.py          # Bot lifecycle management
├── frontend/
│   └── src/
│       ├── components/
│       │   ├── PassiveQuotingConfig.js  # Configuration UI
│       │   └── BotManager.js            # Bot management UI
│       └── services/
│           └── api.js              # API client
└── config/
    └── config.json                 # System configuration
```

## 🚀 Quick Start

### 1. Test the System
```bash
python test_passive_quoting_system.py
```

### 2. Start with One Command
```bash
python start_passive_quoting_system.py
```

### 3. Manual Setup

#### Start API Server
```bash
python api/main.py
```

#### Start Frontend (in new terminal)
```bash
cd frontend
npm install
npm start
```

#### Access the System
- Frontend: http://localhost:3000
- Passive Quoting Config: http://localhost:3000/passive-quoting
- API Documentation: http://localhost:8000/docs

## 📊 Example Configuration

### Conservative Setup
```json
{
  "base_coin": "BTC",
  "quantity_currency": "base",
  "exchanges": ["binance"],
  "lines": [
    {
      "timeout": 600,
      "drift": 100,
      "quantity": 0.001,
      "quantity_randomization_factor": 5,
      "spread": 100,
      "sides": "both"
    }
  ]
}
```

### Aggressive Multi-line Setup
```json
{
  "base_coin": "ETH", 
  "quantity_currency": "quote",
  "exchanges": ["binance", "bybit", "mexc"],
  "lines": [
    {
      "timeout": 60,
      "drift": 20,
      "quantity": 100,
      "quantity_randomization_factor": 20,
      "spread": 10,
      "sides": "both"
    },
    {
      "timeout": 120,
      "drift": 30,
      "quantity": 200,
      "quantity_randomization_factor": 25,
      "spread": 20,
      "sides": "both"
    }
  ]
}
```

## 🎯 Strategy Logic

### Order Placement
1. **Calculate Midpoint**: Average midpoint across selected exchanges
2. **Apply Spread**: `bid_price = midpoint - spread`, `ask_price = midpoint + spread`
3. **Randomize Quantity**: Apply randomization factor to base quantity
4. **Currency Conversion**: Convert quote currency quantities to base using midpoint
5. **Place Orders**: Submit orders with client ID format `pass_quote_{SIDE}_{timestamp}`

### Order Management
1. **Timeout Check**: Cancel orders older than timeout threshold
2. **Drift Check**: Cancel orders if midpoint moves beyond drift threshold
3. **Replace Orders**: Place new orders at current midpoint with new spreads

### Performance Tracking
- Orders placed count
- Orders cancelled by timeout/drift
- Active quote lines
- Real-time P&L (when connected to real exchanges)

## 🔧 API Endpoints

### Strategy Configuration
- `GET /api/strategies/passive-quoting/config` - Get default configuration
- `POST /api/strategies/passive-quoting/config` - Update default configuration
- `GET /api/strategies/passive-quoting/presets` - Get configuration presets
- `POST /api/strategies/passive-quoting/presets/{name}` - Save configuration preset

### Bot Management
- `POST /api/strategies/passive-quoting/create` - Create new passive quoting bot
- `POST /api/strategies/passive-quoting/quick-update` - Quick configuration update
- `GET /api/strategies/passive-quoting/{id}/status` - Get detailed bot status

### General Bot Operations
- `GET /api/bots/` - List all bots
- `POST /api/bots/{id}/start` - Start bot
- `POST /api/bots/{id}/stop` - Stop bot
- `DELETE /api/bots/{id}` - Delete bot

## 💡 Usage Examples

### Creating a Bot via API
```python
import requests

config = {
    "base_coin": "BTC",
    "quantity_currency": "base", 
    "exchanges": ["binance", "bybit"],
    "lines": [
        {
            "timeout": 300,
            "drift": 50,
            "quantity": 0.01,
            "quantity_randomization_factor": 10,
            "spread": 25,
            "sides": "both"
        }
    ]
}

response = requests.post(
    "http://localhost:8000/api/strategies/passive-quoting/create",
    json={"symbol": "BTC/USDT", "config": config}
)
```

### Quick Configuration Update
```python
# Update existing bot configuration in <5 seconds
response = requests.post(
    "http://localhost:8000/api/strategies/passive-quoting/quick-update",
    json={
        "instance_id": "passive_quoting_BTCUSDT_1234567890",
        "config": updated_config
    }
)
```

## 🎨 Frontend Features

### Configuration Interface
- **Visual Line Editor**: Add/remove quote lines with visual controls
- **Preset Management**: Save and load configuration presets
- **Exchange Selection**: Multi-select interface for exchange configuration
- **Real-time Validation**: Immediate feedback on configuration errors
- **Advanced/Simple Views**: Toggle between detailed and simplified interfaces

### Monitoring Dashboard
- **Bot Status**: Real-time status of all passive quoting bots
- **Performance Metrics**: Orders placed, cancelled, active lines
- **Quick Actions**: Start, stop, delete, and quick-update bots
- **Configuration Summary**: View current bot configurations

## 🔍 Monitoring & Debugging

### Bot Status Indicators
- 🟢 **Running**: Bot is active and placing orders
- 🟡 **Starting**: Bot is initializing
- 🔴 **Stopped**: Bot is not active
- ⚠️ **Error**: Bot encountered an error

### Performance Metrics
- **Uptime**: How long the bot has been running
- **Orders Placed**: Total number of orders submitted
- **Cancellation Reasons**: Breakdown by timeout vs drift
- **Active Lines**: Number of quote lines currently active

### Log Analysis
All bot activities are logged with structured logging:
```
2025-06-04 09:55:57 [info] Line 0: Placed bid orders @ 44987.5000 qty=0.01100000 on 2 exchanges
2025-06-04 09:55:58 [info] Line 1: Cancelled ask order due to drift
```

## 🚨 Risk Management

### Built-in Safeguards
- **Maximum Order Count**: Each line limited to 2 orders (bid + ask)
- **Drift Protection**: Automatic cancellation when market moves
- **Timeout Protection**: Prevents stale orders from remaining active
- **Quantity Limits**: Configurable per-line quantity controls

### Best Practices
1. **Start Conservative**: Begin with wider spreads and longer timeouts
2. **Monitor Performance**: Watch cancellation rates and adjust drift thresholds
3. **Use Multiple Lines**: Spread risk across different spread levels
4. **Regular Updates**: Use quick-update feature to adapt to market conditions

## 🛠️ Development

### Adding New Features
1. **Strategy Extensions**: Extend `BaseStrategy` class
2. **API Endpoints**: Add routes to `strategy_routes.py`
3. **Frontend Components**: Create React components in `components/`
4. **Configuration**: Update `config.json` schema

### Testing
```bash
# Run system tests
python test_passive_quoting_system.py

# Run specific component tests
python -m pytest tests/unit/test_passive_quoting.py
```

## 📈 Performance Optimization

### Configuration Tips
- **Optimal Timeouts**: 60-600 seconds depending on market volatility
- **Drift Thresholds**: 10-100 bps based on spread sizes
- **Randomization**: 5-25% to avoid predictable patterns
- **Multiple Lines**: 2-5 lines for optimal market coverage

### System Performance
- **Quick Updates**: <5 second configuration changes
- **Order Latency**: Sub-second order placement (when connected)
- **Memory Usage**: ~50MB per bot instance
- **API Response**: <100ms for most endpoints

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For questions, issues, or feature requests:
1. Check the logs for error messages
2. Review the API documentation at `/docs`
3. Run the test suite to verify system integrity
4. Create an issue with detailed reproduction steps

---

**Ready to start passive quoting? Run `python start_passive_quoting_system.py` and begin trading! 🚀** 