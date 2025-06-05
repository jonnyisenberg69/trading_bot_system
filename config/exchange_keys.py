"""
Exchange API Keys Configuration

IMPORTANT: This file contains real API keys for testing purposes.
In production, these should be stored in environment variables or secure key management.
"""

# Exchange API configurations
EXCHANGE_CONFIGS = {
    'binance_spot': {
        'api_key': 'mcdiHnZKQbbq6yhTjp4rR93oA1rK1KQYqm4gjajwSojoJWdShU1R9L8mYrZWpkkX',
        'secret': 'ttwjMWgpNtzOC3gMBbhsMlF6JbHvqgiiwtVqd1TqBy8akExCnptpHm0Kg1C9yhEp',
        'market_type': 'spot',
        'sandbox': False
    },
    
    'binance_perp': {
        'api_key': 'zeA9rKP1EQCLr7SkUHJiIkIpOp7KlE40XYG1R36zx0XmN87BUtO6oLHHKGaxXfYS',
        'secret': 'BV54UHT9gOEe5fFYjAkaHwPixuCXi0mpZiGbBhVOAmJEHXxEKIyWx10qXSL3onv5',
        'market_type': 'future',
        'sandbox': False
    },
    
    'bybit_spot': {
        'api_key': 'I1AS8daXDozAiv4zJ5',
        'secret': '10kfpJChihGkFAxzTLgInzYkzm7zz9Q8iyNv',
        'market_type': 'spot',
        'sandbox': False
    },
    
    'bybit_perp': {
        'api_key': 'ZBxruH0Als3Dif01IU',
        'secret': 'xLR07099AfnFhDgkTJukE9P7rycsRK2akO32',
        'market_type': 'linear',
        'sandbox': False
    },
    
    'mexc_spot': {
        'api_key': 'mx0vglirnOU5nGgACu',
        'secret': '3a6c6058a86c4816bb8149e77863f441',
        'market_type': 'spot',
        'sandbox': False
    },
    
    'gateio_spot': {
        'api_key': '8c12b0c10c26dddf6a55c5d7e8b1f415',
        'secret': '5fc5ebca602f6ff4f74a289d175cd09c1172cc70e25970a0be96c702f8b8028a',
        'market_type': 'spot',
        'sandbox': False
    },
    
    'bitget_spot': {
        'api_key': 'bg_b4b13f63073c7287a50d040d5cb76171',
        'secret': '74f08cf05d7286257ef5a001391fbac8412ecf50490eb23838e2f02e0d2f9b39',
        'passphrase': 'bitgetpass',
        'market_type': 'spot',
        'sandbox': False
    },
    
    'hyperliquid_perp': {
        'wallet_address': '0x59a7eC7a658777225F7123B2c8420b6D5EC9D64d',
        'private_key': '837d255237050164809436a1202068ed71c4f1fc32814d4942d522c249f16042',
        'secret': '837d255237050164809436a1202068ed71c4f1fc32814d4942d522c249f16042',
        'api_key': '0x59a7eC7a658777225F7123B2c8420b6D5EC9D64d',
        'market_type': 'perp',
        'sandbox': False
    }
}

def get_exchange_config(exchange_name: str) -> dict:
    """
    Get exchange configuration by name.
    
    Args:
        exchange_name: Name of the exchange
        
    Returns:
        Configuration dictionary
    """
    return EXCHANGE_CONFIGS.get(exchange_name, {})

def get_all_exchange_configs() -> dict:
    """Get all exchange configurations."""
    return EXCHANGE_CONFIGS.copy()

def get_available_exchanges() -> list:
    """Get list of available exchange names."""
    return list(EXCHANGE_CONFIGS.keys()) 