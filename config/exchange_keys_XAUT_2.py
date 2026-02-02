"""
Exchange API Keys Configuration

IMPORTANT: This file contains real API keys for testing purposes.
In production, these should be stored in environment variables or secure key management.
"""

# Exchange API configurations
EXCHANGE_CONFIGS = {
    'hyperliquid_perp': {
        'wallet_address': '0xE498854bCb99F575788C93288F9eD727F02fd137',
        'private_key': '8b87f16fc0525cfacb9c13db6d0c095b3fd7fe6d5f78edd4d10a4acd3aecf2af',
        'secret': '8b87f16fc0525cfacb9c13db6d0c095b3fd7fe6d5f78edd4d10a4acd3aecf2af',
        'api_key': '0x0029f12dC732e725d9720080A778DC424bef6672',
        'market_type': 'perp',
        'sandbox': False
    },

    'hyperliquid_spot': {
        'wallet_address': '0xE498854bCb99F575788C93288F9eD727F02fd137',
        'private_key': '8b87f16fc0525cfacb9c13db6d0c095b3fd7fe6d5f78edd4d10a4acd3aecf2af',
        'secret': '8b87f16fc0525cfacb9c13db6d0c095b3fd7fe6d5f78edd4d10a4acd3aecf2af',
        'api_key': '0x0029f12dC732e725d9720080A778DC424bef6672',
        'market_type': 'spot',
        'sandbox': False
    },

    'bitfinex_spot': {
        'api_key': '390ebfd6454b7c08baa37db143e130d127216e51f67',
        'secret': '50ec60f38f8b43e59d025fc04714e510c652119a16c',
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
