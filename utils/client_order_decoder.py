"""Client Order ID decoder utility."""

import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


class ClientOrderDecoder:
    """Decode client order IDs to extract strategy information."""
    
    # Strategy prefixes in hex
    STRATEGY_PREFIXES = {
        "61747761700000": "aggressive_twap",  # "atwap" + padding
        "7061737369766500": "passive_quoting",  # "passive" + padding (future)
        "6d616b65720000": "market_maker",  # "maker" + padding (future)
    }
    
    @staticmethod
    def decode_hyperliquid_cloid(cloid: str) -> Dict[str, Any]:
        """
        Decode Hyperliquid client order ID to extract strategy information.
        
        Args:
            cloid: Client order ID (e.g., "0x61747761700000abc123...")
            
        Returns:
            Dictionary with decoded information:
            - strategy_type: The strategy type (e.g., "aggressive_twap")
            - original_cloid: The original client order ID
            - decoded_cloid: Human-readable version
        """
        try:
            if not cloid or not cloid.startswith("0x"):
                return {
                    "strategy_type": "unknown",
                    "original_cloid": cloid,
                    "decoded_cloid": cloid
                }
            
            # Remove 0x prefix
            hex_string = cloid[2:]
            
            # Extract strategy prefix (first 14 hex chars)
            if len(hex_string) >= 14:
                strategy_prefix = hex_string[:14]
                strategy_type = ClientOrderDecoder.STRATEGY_PREFIXES.get(strategy_prefix, "unknown")
                
                # Create human-readable version
                if strategy_type != "unknown":
                    # Extract unique part
                    unique_part = hex_string[14:] if len(hex_string) > 14 else ""
                    decoded_cloid = f"{strategy_type}_{unique_part[:8]}"  # Use first 8 chars of unique part
                else:
                    decoded_cloid = cloid  # Keep original if unknown
                
                return {
                    "strategy_type": strategy_type,
                    "original_cloid": cloid,
                    "decoded_cloid": decoded_cloid
                }
            else:
                return {
                    "strategy_type": "unknown",
                    "original_cloid": cloid,
                    "decoded_cloid": cloid
                }
                
        except Exception as e:
            logger.error(f"Error decoding Hyperliquid client order ID {cloid}: {e}")
            return {
                "strategy_type": "unknown",
                "original_cloid": cloid,
                "decoded_cloid": cloid
            }
    
    @staticmethod
    def decode_client_order_id(cloid: str, exchange: str) -> Dict[str, Any]:
        """
        Decode client order ID based on exchange format.
        
        Args:
            cloid: Client order ID
            exchange: Exchange name
            
        Returns:
            Dictionary with decoded information
        """
        if not cloid:
            return {
                "strategy_type": "unknown",
                "original_cloid": cloid,
                "decoded_cloid": cloid or "manual_order"
            }
        
        # Special handling for Hyperliquid
        if exchange and "hyperliquid" in exchange.lower():
            return ClientOrderDecoder.decode_hyperliquid_cloid(cloid)
        
        # For other exchanges, extract strategy from prefix
        if cloid.startswith("atwap"):
            return {
                "strategy_type": "aggressive_twap",
                "original_cloid": cloid,
                "decoded_cloid": cloid
            }
        elif cloid.startswith("passive"):
            return {
                "strategy_type": "passive_quoting",
                "original_cloid": cloid,
                "decoded_cloid": cloid
            }
        elif cloid.startswith("mm"):
            return {
                "strategy_type": "market_maker",
                "original_cloid": cloid,
                "decoded_cloid": cloid
            }
        else:
            return {
                "strategy_type": "manual" if cloid == "manual_order" else "unknown",
                "original_cloid": cloid,
                "decoded_cloid": cloid
            }
    
    @staticmethod
    def is_strategy_trade(client_order_id: str, strategy_type: str) -> bool:
        """
        Check if a trade belongs to a specific strategy type.
        
        Args:
            client_order_id: The client order ID from the database
            strategy_type: The strategy type to check (e.g., "aggressive_twap")
            
        Returns:
            True if the trade belongs to the specified strategy
        """
        if not client_order_id:
            return False
        
        # For decoded IDs, check prefix
        if client_order_id.startswith(f"{strategy_type}_"):
            return True
        
        # For legacy non-decoded IDs
        if strategy_type == "aggressive_twap" and client_order_id.startswith("atwap"):
            return True
        elif strategy_type == "passive_quoting" and client_order_id.startswith("passive"):
            return True
        elif strategy_type == "market_maker" and client_order_id.startswith("mm"):
            return True
        
        return False
    
    @staticmethod
    def get_strategy_trades(trades: list, strategy_type: str) -> list:
        """
        Filter trades by strategy type.
        
        Args:
            trades: List of trade objects (must have client_order_id attribute)
            strategy_type: The strategy type to filter by
            
        Returns:
            List of trades belonging to the specified strategy
        """
        strategy_trades = []
        
        for trade in trades:
            client_order_id = getattr(trade, 'client_order_id', None)
            if ClientOrderDecoder.is_strategy_trade(client_order_id, strategy_type):
                strategy_trades.append(trade)
        
        return strategy_trades 