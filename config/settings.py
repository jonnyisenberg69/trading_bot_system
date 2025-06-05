"""
Configuration settings for the trading bot system.

Handles loading configuration from files and environment variables.
"""

import os
import json
from pathlib import Path
from typing import Dict, Any, Optional
import logging
import structlog

logger = structlog.get_logger(__name__)

# Default configuration file path
DEFAULT_CONFIG_PATH = os.path.join(Path(__file__).parent, "config.json")


def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load configuration from file and environment variables.
    
    Args:
        config_path: Path to configuration file (default: config/config.json)
        
    Returns:
        Dictionary with configuration settings
    """
    config_path = config_path or DEFAULT_CONFIG_PATH
    
    # Default configuration
    config = {
        'exchanges': [
            {
                'name': 'binance',
                'type': 'spot',
                'api_key': os.environ.get('BINANCE_SPOT_API_KEY', ''),
                'api_secret': os.environ.get('BINANCE_SPOT_API_SECRET', ''),
                'testnet': True
            },
            {
                'name': 'binance',
                'type': 'perp',
                'api_key': os.environ.get('BINANCE_PERP_API_KEY', ''),
                'api_secret': os.environ.get('BINANCE_PERP_API_SECRET', ''),
                'testnet': True
            },
            {
                'name': 'bybit',
                'type': 'spot',
                'api_key': os.environ.get('BYBIT_SPOT_API_KEY', ''),
                'api_secret': os.environ.get('BYBIT_SPOT_API_SECRET', ''),
                'testnet': True
            },
            {
                'name': 'bybit',
                'type': 'perp',
                'api_key': os.environ.get('BYBIT_PERP_API_KEY', ''),
                'api_secret': os.environ.get('BYBIT_PERP_API_SECRET', ''),
                'testnet': True
            }
        ],
        'database': {
            'type': 'sqlite',
            'path': os.environ.get('DB_PATH', 'data/trading_bot.db')
        },
        'position_tracking': {
            'data_dir': os.environ.get('POSITION_DATA_DIR', 'data/positions')
        },
        'logging': {
            'level': os.environ.get('LOG_LEVEL', 'INFO'),
            'file': os.environ.get('LOG_FILE', 'logs/trading_bot.log')
        }
    }
    
    # Load configuration from file if exists
    try:
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                file_config = json.load(f)
                
                # Update config with file values
                _deep_update(config, file_config)
                
            logger.info(f"Loaded configuration from {config_path}")
        else:
            logger.warning(f"Configuration file {config_path} not found, using defaults")
            
    except Exception as e:
        logger.error(f"Error loading configuration file: {e}")
        
    return config


def _deep_update(target: Dict[str, Any], source: Dict[str, Any]) -> Dict[str, Any]:
    """
    Deep update target dictionary with values from source.
    
    Args:
        target: Target dictionary to update
        source: Source dictionary with values
        
    Returns:
        Updated target dictionary
    """
    for key, value in source.items():
        if key in target and isinstance(target[key], dict) and isinstance(value, dict):
            _deep_update(target[key], value)
        else:
            target[key] = value
            
    return target
