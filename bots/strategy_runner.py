#!/usr/bin/env python3
"""
Strategy Runner - Executes trading bot strategies.

This module is responsible for actually running trading strategies
as separate processes with proper logging and monitoring.
"""

import asyncio
import json
import sys
import signal
import argparse
from pathlib import Path
from datetime import datetime
import structlog

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from api.services.exchange_manager import ExchangeManager
from config.settings import load_config
from bot_manager.strategies.passive_quoting import PassiveQuotingStrategy
from bot_manager.strategies.aggressive_twap import AggressiveTwapStrategy
from bot_manager.strategies.aggressive_twap_v2 import AggressiveTwapV2Strategy
from bot_manager.strategies.targeted_sellbot import TargetedSellbotStrategy
from bot_manager.strategies.targeted_buybot import TargetedBuybotStrategy
from bot_manager.strategies.top_of_book import TopOfBookStrategy
from bot_manager.strategies.market_making import MarketMakingStrategy
from bot_manager.strategies.volume_weighted_top_of_book import VolumeWeightedTopOfBookStrategy
from bot_manager.strategies.stacked_market_making import StackedMarketMakingStrategy
from market_data_collection.service_manager import TradeCollectionServiceManager


class StrategyRunner:
    """
    Runs a specific trading strategy instance.
    """
    
    def __init__(self, config_file: str, instance_id: str):
        self.config_file = Path(config_file)
        self.instance_id = instance_id
        self.strategy = None
        self.exchange_manager = None
        self.running = False
        self.service_manager = TradeCollectionServiceManager()
        self.account_hash = None
        
        # Set up logging
        self.logger = structlog.get_logger("StrategyRunner").bind(
            instance_id=instance_id
        )
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        self.logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.running = False
        
    async def run(self):
        """Run the strategy."""
        try:
            # Load configuration
            config_data = await self._load_config()
            
            # Initialize exchange manager
            await self._initialize_exchange_manager()
            
            # Ensure market data service is running
            await self._ensure_market_data_service(config_data)
            
            # Create strategy instance
            self.strategy = await self._create_strategy(config_data)
            
            # Initialize strategy first
            self.logger.info("Initializing strategy")
            await self.strategy.initialize()
            
            # Inject exchange connectors into strategy
            await self._inject_exchange_connectors(config_data)
            
            # Start strategy
            self.logger.info("Starting strategy execution")
            self.running = True
            
            await self.strategy.start()
            
            self.logger.info("Strategy started successfully, entering main loop")
            
            # Keep running until stopped
            while self.running:
                await asyncio.sleep(1)
                
        except Exception as e:
            self.logger.error(f"Strategy execution failed: {e}")
            raise
        finally:
            # Clean shutdown
            if self.strategy:
                self.logger.info("Stopping strategy")
                await self.strategy.stop()
            
            # Clean up market data service connection if needed
            # Note: For stacked market making, the strategy cleans up its own connections
                
            if self.exchange_manager:
                self.logger.info("Stopping exchange manager")
                await self.exchange_manager.stop()
                
    async def _initialize_exchange_manager(self):
        """Initialize the exchange manager."""
        try:
            # Load exchange configuration
            config = load_config()
            
            # Create exchange manager with configuration
            self.exchange_manager = ExchangeManager(config)
            
            # Start exchange manager
            await self.exchange_manager.start()
            
            self.logger.info("Exchange manager initialized and started")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize exchange manager: {e}")
            raise
            
    async def _ensure_market_data_service(self, config_data: dict) -> None:
        """Ensure market data service is running for this strategy."""
        try:
            strategy_type = config_data.get('strategy', '')
            
            # For stacked market making, the strategy handles its own market data
            if strategy_type == 'stacked_market_making':
                self.logger.info("Stacked market making strategy handles its own market data, skipping external service setup")
                return
            
            # For other strategies, ensure the trade collection service is running
            if self.service_manager.ensure_service_running():
                self.logger.info("Trade collection service is running")
            else:
                self.logger.warning("Failed to start trade collection service, strategy may have limited functionality")
            
        except Exception as e:
            self.logger.error(f"Failed to ensure market data service: {e}")
            raise
            

            

            

            
    async def _inject_exchange_connectors(self, config_data: dict):
        """Inject exchange connectors into the strategy."""
        try:
            if not self.exchange_manager or not self.strategy:
                self.logger.error("Exchange manager or strategy not initialized")
                return
                
            # Get the exchange connection IDs from config
            exchange_ids = config_data.get('exchanges', [])
            
            # Get connectors for these exchanges
            connectors = self.exchange_manager.get_exchange_connectors(exchange_ids)
            
            # Inject connectors into strategy
            self.strategy.exchange_connectors = connectors
            
            if connectors:
                self.logger.info(f"Injected {len(connectors)} exchange connectors: {list(connectors.keys())}")
            else:
                self.logger.warning("No exchange connectors available - bot will run in simulation mode")
                
        except Exception as e:
            self.logger.error(f"Failed to inject exchange connectors: {e}")
            # Don't raise - let strategy run in simulation mode
            
    async def _load_config(self) -> dict:
        """Load strategy configuration from file."""
        try:
            with open(self.config_file, 'r') as f:
                config_data = json.load(f)
                
            self.logger.info(f"Loaded configuration: {config_data['strategy']} for {config_data['symbol']}")
            return config_data
            
        except Exception as e:
            self.logger.error(f"Failed to load config from {self.config_file}: {e}")
            raise
            
    async def _create_strategy(self, config_data: dict):
        """Create the appropriate strategy instance."""
        strategy_type = config_data['strategy']
        symbol = config_data['symbol']
        exchanges = config_data['exchanges']
        config = config_data['config']
        
        self.logger.info(f"Creating {strategy_type} strategy for {symbol} on {len(exchanges)} exchanges")
        
        if strategy_type == 'passive_quoting':
            return PassiveQuotingStrategy(
                instance_id=self.instance_id,
                symbol=symbol,
                exchanges=exchanges,
                config=config
            )
        elif strategy_type == 'aggressive_twap':
            return AggressiveTwapStrategy(
                instance_id=self.instance_id,
                symbol=symbol,
                exchanges=exchanges,
                config=config
            )
        elif strategy_type == 'aggressive_twap_v2':
            return AggressiveTwapV2Strategy(
                instance_id=self.instance_id,
                symbol=symbol,
                exchanges=exchanges,
                config=config
            )
        elif strategy_type == 'targeted_sellbot':
            return TargetedSellbotStrategy(
                instance_id=self.instance_id,
                symbol=symbol,
                exchanges=exchanges,
                config=config
            )
        elif strategy_type == 'targeted_buybot':
            return TargetedBuybotStrategy(
                instance_id=self.instance_id,
                symbol=symbol,
                exchanges=exchanges,
                config=config
            )
        elif strategy_type == 'top_of_book':
            return TopOfBookStrategy(
                instance_id=self.instance_id,
                symbol=symbol,
                exchanges=exchanges,
                config=config
            )
        elif strategy_type == 'volume_weighted_top_of_book':
            return VolumeWeightedTopOfBookStrategy(
                instance_id=self.instance_id,
                symbol=symbol,
                exchanges=exchanges,
                config=config
            )
        elif strategy_type == 'market_making':
            return MarketMakingStrategy(
                instance_id=self.instance_id,
                symbol=symbol,
                exchanges=exchanges,
                config=config
            )
        elif strategy_type == 'stacked_market_making':
            return StackedMarketMakingStrategy(
                instance_id=self.instance_id,
                symbol=symbol,
                exchanges=exchanges,
                config=config
            )
        else:
            raise ValueError(f"Unknown strategy type: {strategy_type}")


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Run a trading strategy')
    parser.add_argument('--config', required=True, help='Path to strategy config file')
    parser.add_argument('--instance-id', required=True, help='Bot instance ID')
    parser.add_argument('--log-level', default='INFO', help='Log level')
    
    args = parser.parse_args()
    
    # Configure logging
    import logging
    from logging.handlers import RotatingFileHandler
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s,%(msecs)03d - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Also write all logs and prints to a per-run file under package logs/
    try:
        package_root = Path(__file__).parent.parent
        logs_dir = package_root / 'logs'
        logs_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        logfile_path = logs_dir / f"strategy_{args.instance_id}_{timestamp}.log"

        # Rotating file handler for stdlib logging
        file_handler = RotatingFileHandler(str(logfile_path), maxBytes=10 * 1024 * 1024, backupCount=5)
        file_handler.setLevel(getattr(logging, args.log_level))
        file_handler.setFormatter(logging.Formatter('%(asctime)s,%(msecs)03d - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
        logging.getLogger().addHandler(file_handler)

        # Tee stdout/stderr so any prints also go to the same file
        class _Tee:
            def __init__(self, stream, file_obj):
                self._stream = stream
                self._file = file_obj
            def write(self, data):
                try:
                    self._stream.write(data)
                except Exception:
                    pass
                try:
                    self._file.write(data)
                    self._file.flush()
                except Exception:
                    pass
            def flush(self):
                try:
                    self._stream.flush()
                except Exception:
                    pass
                try:
                    self._file.flush()
                except Exception:
                    pass

        log_file_handle = open(logfile_path, 'a', buffering=1)
        sys.stdout = _Tee(sys.stdout, log_file_handle)
        sys.stderr = _Tee(sys.stderr, log_file_handle)
    except Exception:
        # If file logging setup fails, continue with console-only logging
        pass
    
    # Create and run strategy
    runner = StrategyRunner(args.config, args.instance_id)
    
    try:
        await runner.run()
    except KeyboardInterrupt:
        print("\nShutdown requested by user")
    except Exception as e:
        print(f"Strategy runner failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    asyncio.run(main()) 
