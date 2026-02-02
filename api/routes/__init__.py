"""
API Routes Package.

Contains FastAPI route definitions for the trading bot system.
"""

# Don't import modules here to avoid circular imports
# The modules will be imported directly in main.py

__all__ = ["bot_routes", "exchange_routes", "system_routes", "strategy_routes", "position_routes", "market_data", "trade_collection", "coefficient_routes"]
