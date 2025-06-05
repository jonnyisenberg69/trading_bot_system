# Trading Bot System Frontend

A modern React-based frontend for managing cryptocurrency trading bots and exchange connections.

## 🚀 Quick Start

### Prerequisites

- **Python 3.8+** with pip
- **Node.js 16+** with npm
- **Required system packages** (for CCXT and other dependencies)

### Installation & Startup

1. **Clone and navigate to the project:**
   ```bash
   cd trading_bot_system
   ```

2. **Run the startup script:**
   ```bash
   ./start_frontend.sh
   ```

   This script will:
   - Install Python dependencies in a virtual environment
   - Install Node.js dependencies for the frontend
   - Start the FastAPI backend server on port 8000
   - Start the React development server on port 3000

3. **Access the application:**
   - **Frontend:** http://localhost:3000
   - **API Backend:** http://localhost:8000
   - **API Documentation:** http://localhost:8000/docs

## 📋 Features

### Dashboard
- **System Overview:** Real-time status of bots and exchanges
- **Running Bot Monitoring:** See which strategies are active
- **Exchange Connection Status:** Monitor API connectivity
- **Quick Actions:** Start bots, test connections, refresh data

### Exchange Management
- **Connection Testing:** Test API credentials and connectivity
- **Credential Management:** Securely update API keys and secrets
- **Status Monitoring:** Real-time connection health
- **Multi-Exchange Support:** Binance, Bybit, Hyperliquid, MEXC, Gate.io, Bitget

### Bot Management
- **Create Bots:** Set up new trading instances with strategies
- **Control Operations:** Start, stop, and delete bot instances
- **Strategy Selection:** Market Making, Arbitrage, Grid Trading, DCA
- **Multi-Exchange Trading:** Run bots across multiple exchanges simultaneously

## 🏗️ Architecture

### Backend (FastAPI)
```
api/
├── main.py                 # FastAPI application entry point
├── services/
│   ├── bot_manager.py      # Bot instance management
│   └── exchange_manager.py # Exchange connection management
└── routes/
    ├── bot_routes.py       # Bot API endpoints
    ├── exchange_routes.py  # Exchange API endpoints
    └── system_routes.py    # System status endpoints
```

### Frontend (React)
```
frontend/src/
├── App.js                  # Main application component
├── components/
│   ├── Dashboard.js        # System overview dashboard
│   ├── ExchangeStatus.js   # Exchange management interface
│   ├── BotManager.js       # Bot creation and control
│   └── Navbar.js          # Navigation component
└── services/
    └── api.js             # API client and service functions
```

## 🔧 API Endpoints

### System Status
- `GET /api/system/status` - Overall system health
- `GET /api/system/dashboard` - Dashboard overview data
- `GET /api/system/health` - Health check endpoint

### Bot Management
- `GET /api/bots/` - List all bot instances
- `POST /api/bots/` - Create new bot instance
- `POST /api/bots/{id}/start` - Start bot instance
- `POST /api/bots/{id}/stop` - Stop bot instance
- `DELETE /api/bots/{id}` - Delete bot instance

### Exchange Management
- `GET /api/exchanges/status` - Exchange connection status
- `POST /api/exchanges/{id}/test` - Test specific exchange
- `POST /api/exchanges/{id}/credentials` - Update API credentials
- `POST /api/exchanges/test-all` - Test all exchanges

## 🔑 Exchange Configuration

The system supports these exchanges with API credential management:

### Supported Exchanges
| Exchange    | Spot | Perpetual | WebSocket | Status |
|-------------|------|-----------|-----------|---------|
| Binance     | ✅   | ✅        | ✅        | Active  |
| Bybit       | ✅   | ✅        | ✅        | Active  |
| Hyperliquid | ❌   | ✅        | ✅        | Active  |
| MEXC        | ✅   | ✅        | ❌        | Active  |
| Gate.io     | ✅   | ✅        | ❌        | Active  |
| Bitget      | ✅   | ✅        | ❌        | Active  |

### API Key Setup

1. **Go to Exchange Settings** in the frontend
2. **Click the Settings button** for each exchange
3. **Enter your API credentials:**
   - API Key
   - API Secret
   - Enable/Disable Testnet
4. **Test the connection** to verify credentials

**Security Note:** API keys are stored temporarily in memory and not persisted to disk in this development version.

## 🤖 Bot Strategies

### Available Strategies

1. **Market Making**
   - Places buy/sell orders around current market price
   - Profits from bid-ask spread
   - Suitable for liquid markets

2. **Arbitrage**
   - Finds price differences between exchanges
   - Automatically executes profitable trades
   - Requires multiple connected exchanges

3. **Grid Trading**
   - Places multiple orders at different price levels
   - Profits from market volatility
   - Good for ranging markets

4. **Dollar Cost Averaging (DCA)**
   - Regular purchases regardless of price
   - Reduces impact of volatility
   - Long-term investment strategy

### Creating a Bot

1. **Navigate to Bot Management**
2. **Click "New Bot"**
3. **Select Strategy** from dropdown
4. **Choose Trading Symbol** (e.g., BTC/USDT)
5. **Select Exchanges** (must be connected)
6. **Click "Create Bot"**

The bot will be created in "stopped" state. Use the play button to start trading.

## 🛠️ Development

### Manual Development Setup

If you prefer to run components separately:

1. **Backend:**
   ```bash
   cd api
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r ../requirements.txt
   python main.py
   ```

2. **Frontend:**
   ```bash
   cd frontend
   npm install
   npm start
   ```

### Environment Variables

Optional environment variables for configuration:

```bash
# API Configuration
REACT_APP_API_URL=http://localhost:8000

# Exchange API Keys (for testing)
BINANCE_SPOT_API_KEY=your_key_here
BINANCE_SPOT_API_SECRET=your_secret_here
BYBIT_SPOT_API_KEY=your_key_here
BYBIT_SPOT_API_SECRET=your_secret_here
```

## 🔍 Troubleshooting

### Common Issues

1. **Backend not starting:**
   - Check Python dependencies: `pip install -r requirements.txt`
   - Verify Python version: `python --version` (3.8+ required)

2. **Frontend compilation errors:**
   - Clear npm cache: `npm cache clean --force`
   - Delete node_modules: `rm -rf frontend/node_modules && cd frontend && npm install`

3. **Exchange connection failures:**
   - Verify API credentials are correct
   - Check if IP is whitelisted on exchange
   - Ensure testnet settings match your API keys

4. **CORS errors:**
   - Backend automatically allows localhost:3000
   - For production, update CORS settings in `api/main.py`

### Log Files

- **Backend logs:** Console output from FastAPI server
- **Frontend logs:** Browser console (F12 Developer Tools)
- **System logs:** Check the logs/ directory for detailed system logs

## 🚦 Status Indicators

### System Status
- 🟢 **Green:** All systems operational, bots running
- 🟡 **Yellow:** Exchanges connected, no active bots
- 🔴 **Red:** No exchanges connected

### Exchange Status
- **Connected:** API credentials valid, market data available
- **Invalid Credentials:** API keys incorrect or expired
- **No Credentials:** API keys not configured
- **Error:** Network or API issues

### Bot Status
- **Running:** Bot is actively trading
- **Stopped:** Bot is created but not active
- **Starting:** Bot initialization in progress
- **Stopping:** Bot shutdown in progress
- **Error:** Bot encountered an issue

## 📊 Performance

### System Requirements
- **Minimum:** 2GB RAM, 2 CPU cores
- **Recommended:** 4GB RAM, 4 CPU cores
- **Storage:** 1GB for logs and data

### Scalability
- **Concurrent Bots:** Limited by exchange rate limits
- **Exchange Connections:** All supported exchanges simultaneously
- **WebSocket Connections:** Real-time market data for active pairs

## 🔐 Security Considerations

### Development Mode
- API keys stored in memory only
- CORS enabled for localhost development
- Debug mode enabled

### Production Deployment
- Implement proper API key encryption and storage
- Configure secure CORS origins
- Enable HTTPS/TLS
- Implement authentication and authorization
- Set up proper logging and monitoring

---

For questions or issues, check the system logs or create an issue in the project repository. 