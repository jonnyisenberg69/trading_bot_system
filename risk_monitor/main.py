import asyncio
import logging
import os
import sys
from dotenv import load_dotenv
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

# Add project root to the Python path to resolve imports
# This allows the script to be run from any directory
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Now that the project root is on the path, we can use absolute imports
from risk_monitor.services.risk_data_service import RiskDataService
from config.exchange_keys import get_all_exchange_configs
from database.connection import init_db, get_database_url
from database.repositories.realized_pnl_repository import RealizedPNLRepository
from database.repositories.balance_history_repository import BalanceHistoryRepository
from database.repositories.inventory_price_history_repository import InventoryPriceHistoryRepository
from database.models import Exchange
from sqlalchemy import select

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def parse_args():
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(description="Risk Monitoring Service")
    parser.add_argument(
        '--start-time',
        type=str,
        help="The UTC start time to fetch historical trades from, format: YYYY-MM-DD-HH:MM"
    )
    parser.add_argument(
        '--symbols',
        type=str,
        nargs='+',
        help="Specific symbols to monitor (e.g., BTC/USDT ETH/USDT). If not specified, will get from bot configs."
    )
    return parser.parse_args()

def get_trading_symbols_from_bots():
    """Get unique trading symbols from bot configurations."""
    symbols = set()
    bot_data_dir = Path(project_root) / "data" / "bots"
    
    if bot_data_dir.exists():
        for file_path in bot_data_dir.glob("*.json"):
            # Skip config files
            if file_path.name.endswith("_config.json"):
                continue
                
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    if 'symbol' in data:
                        symbols.add(data['symbol'])
            except Exception as e:
                logger.warning(f"Failed to read bot config {file_path}: {e}")
    
    # Also check API bot data directory
    api_bot_data_dir = Path(project_root) / "api" / "data" / "bots"
    if api_bot_data_dir.exists():
        for file_path in api_bot_data_dir.glob("*.json"):
            if file_path.name.endswith("_config.json"):
                continue
                
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    if 'symbol' in data:
                        symbols.add(data['symbol'])
            except Exception as e:
                logger.warning(f"Failed to read bot config {file_path}: {e}")
    
    return list(symbols)

async def main():
    """
    Initializes and runs the Risk Data Collector service.
    This service uses ccxtpro to connect to exchanges and monitor account data.
    """
    args = parse_args()
    start_timestamp_ms = None
    if args.start_time:
        try:
            # Parse the string and convert to a UTC timestamp in milliseconds
            dt_object = datetime.strptime(args.start_time, "%Y-%m-%d-%H:%M").replace(tzinfo=timezone.utc)
            start_timestamp_ms = int(dt_object.timestamp() * 1000)
            logger.info(f"Received start time: {args.start_time}, using timestamp: {start_timestamp_ms}")
        except ValueError:
            logger.error("Invalid start-time format. Please use YYYY-MM-DD-HH:MM. Exiting.")
            return
    else:
        start_timestamp_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        logger.info(f"No start-time provided, using current time: {datetime.fromtimestamp(start_timestamp_ms / 1000, tz=timezone.utc)}")

    # Get trading symbols
    if args.symbols:
        trading_symbols = args.symbols
        logger.info(f"Using provided symbols: {trading_symbols}")
    else:
        trading_symbols = get_trading_symbols_from_bots()
        if not trading_symbols:
            logger.warning("No symbols found in bot configurations, defaulting to BERA/USDT")
            trading_symbols = ["BERA/USDT"]
        else:
            logger.info(f"Found trading symbols from bot configs: {trading_symbols}")

    logger.info("Starting Risk Monitoring Service...")

    # Use the central configuration to get all exchange keys
    all_exchange_configs = get_all_exchange_configs()

    if not all_exchange_configs:
        logger.error("No API credentials found from get_all_exchange_configs(). Exiting.")
        return

    # Create a clean configuration for ccxtpro, grouped by base exchange
    grouped_configs = {}
    for name, config in all_exchange_configs.items():
        # a name is like 'binance_spot' or 'bybit_perp'
        base_name = name.split('_')[0]
        market_type = config.get('market_type')

        if not base_name or not market_type:
            logger.warning(f"Skipping config '{name}' due to missing base_name or market_type.")
            continue

        if base_name not in grouped_configs:
            creds = {
                'apiKey': config.get('api_key') or config.get('wallet_address'),
                'secret': config.get('secret'),
                'options': {}
            }
            if 'passphrase' in config:
                creds['password'] = config['passphrase']
            
            # Add exchange-wide options
            if base_name == 'mexc':
                creds['recvWindow'] = 60000
                creds['options'].setdefault('ws', {})['pingInterval'] = 20000
            if base_name == 'bitget':
                creds['recvWindow'] = 60000
            if base_name == 'gateio':
                 creds['options']['fetchMarkets'] = ['spot']

            grouped_configs[base_name] = {'creds': creds, 'markets': {}}

        # Add market type and its unique name
        # For Bybit, the category is 'linear' for perps. For binance, it's 'future'.
        # The 'market_type' from our config should map to ccxt's internal name.
        ccxt_market_type = market_type
        if base_name == 'bybit' and market_type == 'perp':
            ccxt_market_type = 'linear'
        if base_name == 'binance' and market_type == 'perp':
            ccxt_market_type = 'future'
            
        grouped_configs[base_name]['markets'][ccxt_market_type] = name
    
    logger.info(f"Initializing services for: {list(grouped_configs.keys())}")

    # Initialize DB and repositories
    db_url = get_database_url()
    session_maker = await init_db(db_url)
    if not session_maker:
        logger.error("Database session maker not initialized. Exiting.")
        return
        
    realized_pnl_repo = RealizedPNLRepository(session_maker)
    balance_history_repo = BalanceHistoryRepository(session_maker)
    inventory_price_history_repo = InventoryPriceHistoryRepository(session_maker)
    async with session_maker() as session:
        # Fetch all exchanges and build a mapping from name to id
        result = await session.execute(select(Exchange.id, Exchange.name))
        exchange_id_map = {name: id for id, name in result.all()}

        service = RiskDataService(
            grouped_configs,
            start_timestamp_ms=start_timestamp_ms,
            realized_pnl_repo=realized_pnl_repo,
            balance_history_repo=balance_history_repo,
            inventory_price_history_repo=inventory_price_history_repo,
            exchange_id_map=exchange_id_map,
            trading_symbols=trading_symbols  # Pass symbols to the service
        )
        try:
            await service.start()
            # Keep the service running indefinitely
            await asyncio.Event().wait()
        except KeyboardInterrupt:
            logger.info("Shutdown signal received.")
        except Exception as e:
            logger.error(f"An unexpected error occurred in the main service: {e}", exc_info=True)
        finally:
            logger.info("Stopping Risk Monitoring Service...")
            await service.stop()
            logger.info("Service stopped.")

if __name__ == "__main__":
    # Note: ccxtpro uses asyncio, so we run our main function in an asyncio loop.
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Program interrupted by user.") 
