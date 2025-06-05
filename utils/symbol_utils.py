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