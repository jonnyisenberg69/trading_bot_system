"""
Symbol utilities for handling exchange-specific trading pair formatting.

This module provides functions to normalize trading pairs across different exchanges
and handle their specific requirements for symbol formatting.
"""

import re
from typing import Dict, Optional, Tuple


class SymbolMapper:
    """
    Handles symbol mapping and normalization across different exchanges.
    """
    
    # Exchange-specific symbol formatting rules
    EXCHANGE_FORMATS = {
        'binance': {
            'separator': '',
            'case': 'upper',
            'special_mappings': {}
        },
        'bybit': {
            'separator': '',
            'case': 'upper', 
            'special_mappings': {}
        },
        'mexc': {
            'separator': '',
            'case': 'upper',
            'special_mappings': {}
        },
        'gateio': {
            'separator': '_',
            'case': 'upper',
            'special_mappings': {}
        },
        'bitget': {
            'separator': '',
            'case': 'upper',
            'special_mappings': {}
        },
        'hyperliquid': {
            'separator': '-',
            'case': 'upper',
            'special_mappings': {
                'BTC/USDT': 'BTC-USD',
                'ETH/USDT': 'ETH-USD',
                # Add more Hyperliquid-specific mappings as needed
            }
        }
    }
    
    @staticmethod
    def normalize_symbol(symbol: str) -> str:
        """
        Normalize a trading symbol to standard format (BASE/QUOTE).
        
        Args:
            symbol: Trading symbol in any format
            
        Returns:
            Normalized symbol in BASE/QUOTE format
        """
        if not symbol:
            return ''
            
        # Remove common separators and standardize
        normalized = symbol.upper()
        
        # Handle different separator formats
        for sep in ['-', '_', '']:
            if sep in normalized and '/' not in normalized:
                parts = normalized.split(sep)
                if len(parts) == 2:
                    return f"{parts[0]}/{parts[1]}"
        
        # If already in BASE/QUOTE format, return as-is
        if '/' in normalized:
            parts = normalized.split('/')
            if len(parts) == 2:
                return f"{parts[0]}/{parts[1]}"
        
        return normalized
    
    @staticmethod
    def get_base_quote(symbol: str) -> Tuple[str, str]:
        """
        Extract base and quote currencies from a trading symbol.
        
        Args:
            symbol: Trading symbol in any format
            
        Returns:
            Tuple of (base_currency, quote_currency)
        """
        normalized = SymbolMapper.normalize_symbol(symbol)
        
        if '/' in normalized:
            parts = normalized.split('/')
            return parts[0], parts[1]
        
        return '', ''
    
    @staticmethod
    def format_for_exchange(symbol: str, exchange_name: str) -> str:
        """
        Format a trading symbol for a specific exchange.
        
        Args:
            symbol: Trading symbol in normalized BASE/QUOTE format
            exchange_name: Name of the exchange
            
        Returns:
            Symbol formatted for the specific exchange
        """
        normalized = SymbolMapper.normalize_symbol(symbol)
        exchange_name = exchange_name.lower()
        
        if exchange_name not in SymbolMapper.EXCHANGE_FORMATS:
            return normalized
        
        format_rules = SymbolMapper.EXCHANGE_FORMATS[exchange_name]
        
        # Check for special mappings first
        if normalized in format_rules['special_mappings']:
            return format_rules['special_mappings'][normalized]
        
        # Extract base and quote
        base, quote = SymbolMapper.get_base_quote(normalized)
        if not base or not quote:
            return normalized
        
        # Apply case formatting
        if format_rules['case'] == 'upper':
            base = base.upper()
            quote = quote.upper()
        elif format_rules['case'] == 'lower':
            base = base.lower()
            quote = quote.lower()
        
        # Apply separator
        separator = format_rules['separator']
        if separator == '/':
            return f"{base}/{quote}"
        else:
            return f"{base}{separator}{quote}"
    
    @staticmethod
    def validate_symbol(symbol: str) -> bool:
        """
        Validate if a symbol is in correct format.
        
        Args:
            symbol: Trading symbol to validate
            
        Returns:
            True if valid, False otherwise
        """
        if not symbol:
            return False
        
        normalized = SymbolMapper.normalize_symbol(symbol)
        base, quote = SymbolMapper.get_base_quote(normalized)
        
        return bool(base and quote and len(base) > 0 and len(quote) > 0)
    
    @staticmethod
    def get_exchange_symbols(base_symbol: str, exchanges: list) -> Dict[str, str]:
        """
        Get exchange-specific symbol formats for multiple exchanges.
        
        Args:
            base_symbol: Base trading symbol in BASE/QUOTE format
            exchanges: List of exchange names
            
        Returns:
            Dictionary mapping exchange names to their specific symbol formats
        """
        result = {}
        
        for exchange in exchanges:
            try:
                formatted = SymbolMapper.format_for_exchange(base_symbol, exchange)
                result[exchange] = formatted
            except Exception as e:
                # If formatting fails, use the original symbol
                result[exchange] = base_symbol
        
        return result


# Convenience functions for easy use
def normalize_symbol(symbol: str) -> str:
    """Convenience function to normalize a symbol."""
    return SymbolMapper.normalize_symbol(symbol)


def format_for_exchange(symbol: str, exchange: str) -> str:
    """Convenience function to format symbol for exchange."""
    return SymbolMapper.format_for_exchange(symbol, exchange)


def validate_symbol(symbol: str) -> bool:
    """Convenience function to validate a symbol."""
    return SymbolMapper.validate_symbol(symbol)


def get_base_quote(symbol: str) -> Tuple[str, str]:
    """Convenience function to get base and quote currencies."""
    return SymbolMapper.get_base_quote(symbol)


def get_exchange_symbols(symbol: str, exchanges: list) -> Dict[str, str]:
    """Convenience function to get exchange-specific symbols."""
    return SymbolMapper.get_exchange_symbols(symbol, exchanges)


def get_normalized_symbol(symbol: str) -> str:
    """Get the base symbol, e.g., 'BTC' from 'BTC/USDT'."""
    return SymbolMapper.get_normalized_symbol(symbol)


def get_normalized_symbol_for_aggregation(exchange_symbol: str, exchange_id: str) -> str:
    """Convert exchange-specific symbol back to normalized symbol for position aggregation."""
    
    # Perpetual futures contracts
    if 'perp' in exchange_id or 'future' in exchange_id:
        if 'USDT' in exchange_symbol and '/' not in exchange_symbol:
            base = exchange_symbol.replace('USDT', '')
            return f"{base}/USDT-PERP"
        if '/' in exchange_symbol:
            base = exchange_symbol.split('/')[0]
            return f"{base}/USDT-PERP"
        # Fallback for other perpetuals
        return f"{exchange_symbol}-PERP"

    # Spot markets
    elif 'spot' in exchange_id:
        if 'USDT' in exchange_symbol and '/' not in exchange_symbol:
            base = exchange_symbol.replace('USDT', '')
            return f"{base}/USDT"
        if '/' in exchange_symbol:
            base = exchange_symbol.split('/')[0]
            return f"{base}/USDT"
    
    # Default/fallback case
    return exchange_symbol

def convert_symbol_for_exchange(symbol: str, exchange_id: str) -> str:
    """Convert symbol to exchange-specific format."""
    
    # For perpetual exchanges, convert BERA/USDT to appropriate format
    if 'perp' in exchange_id or 'future' in exchange_id:
        if exchange_id == 'hyperliquid_perp':
            # Hyperliquid uses BERA/USDC:USDC for perpetuals
            if symbol == 'BERA/USDT':
                return 'BERA/USDC:USDC'
            elif '/' in symbol and ':' not in symbol:
                base, quote = symbol.split('/')
                # Convert any symbol to USDC:USDC format for Hyperliquid
                return f"{base}/USDC:USDC"
            else:
                return f"{symbol}/USDC:USDC"
        elif exchange_id == 'binance_perp':
            # Binance perp uses BERAUSDT format (no slash)
            if '/' in symbol:
                base, quote = symbol.split('/')
                return f"{base}USDT"  # Force USDT for Binance futures
        elif exchange_id == 'bybit_perp':
            # Bybit perp uses BERUSDT format (different from spot)
            if '/' in symbol:
                base, quote = symbol.split('/')
                # Use linear perpetual symbol format for Bybit
                return f"{base}USDT"  # Bybit perp uses BERUSDT, not BERA/USDT
                
    # For spot markets, use proper spot symbol formats
    elif 'spot' in exchange_id:
        if exchange_id == 'bybit_spot':
            # Bybit spot keeps the slash format: BERA/USDT
            return symbol
        elif exchange_id == 'binance_spot':
            # Binance spot also keeps slash format: BERA/USDT  
            return symbol
        elif exchange_id in ['mexc_spot', 'gateio_spot', 'bitget_spot']:
            # Other spot exchanges keep slash format
            return symbol
            
    # Default: return original symbol
    return symbol 
