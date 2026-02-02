"""
Exchange API Keys Configuration

IMPORTANT: This file contains real API keys for testing purposes.
In production, these should be stored in environment variables or secure key management.
"""

# Exchange API configurations
EXCHANGE_CONFIGS = {
    'bybit_spot': {
        'api_key': 'I1AS8daXDozAiv4zJ5',
        'secret': '10kfpJChihGkFAxzTLgInzYkzm7zz9Q8iyNv',
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
    },

    'hyperliquid_spot': {
        'wallet_address': '0xbA52b1BD928d1a471030d3D4F7BB1991c76679C4',
        'private_key': '7918a7d4d79f79ac045ea54f7e1ee65c909113d1f742fd3247034a980f8c962c',
        'secret': '0xe0894c5fc0d90670844a348795669a6f29f43ff7849c911cc08d139711836fc9',
        'api_key': '0xccFBeA3725fd479D574863c85b933C17E4B40116',
        'market_type': 'spot',
        'sandbox': False
    },    

    'bitfinex_spot': {
        'api_key': 'b29d9696f7d804e808bea48d7df588652cb8c1f35fd',
        'secret': '4ac39ec043cd1375cf63b3e81c8b2842e0ecac78b53',
        'market_type': 'spot',
        'sandbox': False
    }
    
    # 'meteora_dlmm': {
    #     'wallet_address': '3AqSGatvcQ94GUr323gXv2mkdExNX1Wndi2dFedX21GY',  # Solana wallet public key (base58)
    #     'private_key': '3qzAA4MU9YSVRsyEXje3BHreVhfBVsihtLEBwTnVACeqSrx6SQwcbQdkNv6Eq3N7jKqMnSmUj5wAj121NewDzt5J',     # Solana wallet private key (base58 or hex with 0x prefix)
    #     'rpc_url': 'https://api.mainnet-beta.solana.com',  # Or use a private RPC
    #     'market_type': 'dex',
    #     'sandbox': False
    # }
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
