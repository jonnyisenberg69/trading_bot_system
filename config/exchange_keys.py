"""
Exchange API Keys Configuration

IMPORTANT: This file contains real API keys for testing purposes.
In production, these should be stored in environment variables or secure key management.
"""

# Exchange API configurations
EXCHANGE_CONFIGS = {
    'binance_spot': {
        'api_key': 'lh4pYYEyQDZyI16nEHauJqo0KSipvuqC1I0jx2kFQ0jyxDmWzbc2WShr85n6TZlI',
        'secret': 'Y1yd5PfiworFo4nB7A8GHxcXVQkj6pVdlyMt0tRkc8rI3R26HPr7Q9KgqSroBvcz',
        'market_type': 'spot',
        'sandbox': False
    },
    
    'bybit_spot': {
        'api_key': 'mDxsCAwRe5nGlhtPDI',
        'secret': 'KBJDe3YWlhULH3nYzSo9Md7Io0qBkCJXVYLf',
        'market_type': 'spot',
        'sandbox': False
    },
    
    'kucoin_spot': {
        'api_key': '69814a9676a75f0001f489e6',
        'secret': 'be2a50be-be21-4dc9-b0e8-beaf057968ff',
        'passphrase': 'Kucoin123',
        'market_type': 'spot',
        'sandbox': False
    },
    
    'mexc_spot': {
        'api_key': 'mx0vgli1uvupxk3LWE',
        'secret': '20eebe052d094c3eb3083ba8d3dee0ce',
        'market_type': 'spot',
        'sandbox': False
    },
    
    'gateio_spot': {
        'api_key': '295433222c76908a7e1146222b2653c8',
        'secret': '9a4a32131b354a70e67287babc7f63e7ab164ac0f031a64b3fc16df30cbace0a',
        'market_type': 'spot',
        'sandbox': False
    },
    
    'bitget_spot': {
        'api_key': 'bg_4eb9c21cfaf319fd9b651d4e980cb40a',
        'secret': '56d207682f56449a026b517f1a2c2454dceb3efac7cebae7ea4c6116495ac8b8',
        'passphrase': 'Bitget123',
        'market_type': 'spot',
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
