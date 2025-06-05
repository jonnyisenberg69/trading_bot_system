<<<<<<< HEAD
# trading_bot_system
=======
# Crypto Trading Bot System

A comprehensive multi-exchange cryptocurrency trading bot system with robust position monitoring and real-time market data analysis.

## Features

- **Multi-Exchange Support**: Connects to Binance, Bybit, OKX, Hyperliquid and more
- **Position Monitoring**: Tracks positions across exchanges
- **WebSocket Integration**: Real-time data with robust reconnection logic
- **Market Analysis**: Cross-exchange liquidity and price analysis
- **Order Management**: Place and track orders across exchanges
- **Database Integration**: PostgreSQL for persistent storage
- **Logging**: Detailed logging with structlog

## Architecture

The system is composed of several modules:

- **Exchange Connectors**: Interface with exchange APIs
- **Market Data**: Real-time market data collection and analysis
- **Order Management**: Order placement and tracking
- **Position Management**: Position calculation and tracking
- **Bot Manager**: Trading strategy execution
- **API**: REST API for monitoring and control
- **Database**: Data persistence

## Setup Instructions

### Prerequisites

- Python 3.9+
- PostgreSQL
- Redis

### Installation

1. Clone the repository:
   ```
   git clone https://github.com/your-username/trading_bot_system.git
   cd trading_bot_system
   ```

2. Create and activate a virtual environment:
   ```
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

4. Set up environment variables:
   ```
   cp .env.example .env
   # Edit .env with your API keys and configuration
   ```

5. Initialize the database:
   ```
   alembic upgrade head
   ```

## Usage

### Market Data Monitoring

Monitor real-time market data from multiple exchanges:

```bash
python scripts/monitor_market_data.py --symbols BTC/USDT,ETH/USDT --exchanges binance,bybit,okx
```

Options:
- `--symbols`: Comma-separated list of symbols to monitor (default: BTC/USDT,ETH/USDT)
- `--exchanges`: Comma-separated list of exchanges to monitor (default: binance,bybit,okx)
- `--log-dir`: Directory for log files (default: logs/market_data)
- `--log-interval`: Console logging interval in seconds (default: 5.0)
- `--depth`: Orderbook depth to fetch (default: 20)

### Running the Trading Bot

```bash
python main.py
```

### API Server

Start the API server for monitoring and control:

```bash
uvicorn api.main:app --reload
```

The API is accessible at http://localhost:8000. API documentation is available at http://localhost:8000/docs.

## Configuration

Configuration is managed through environment variables. See `.env.example` for available options.

Key configuration options:

- `DATABASE_URL`: PostgreSQL connection URL
- `REDIS_URL`: Redis connection URL
- `EXCHANGE_API_KEYS`: Exchange API keys and secrets
- `LOG_LEVEL`: Logging level (default: INFO)

## Logging

Logs are stored in the `logs/` directory:

- `logs/market_data/`: Market data logs
  - `json/`: Raw market data in JSON format
  - `csv/`: Processed market data in CSV format for analysis

## Development

### Running Tests

```bash
pytest
```

To run specific tests:

```bash
pytest tests/unit/test_position_calculator.py
```

### Adding a New Exchange

1. Create a new connector in `exchanges/connectors/`
2. Implement the required methods (see existing connectors for reference)
3. Add the connector to the factory in `exchanges/factory.py`

## License

This project is licensed under the MIT License - see the LICENSE file for details. 
>>>>>>> master
