"""
API Routes Package.

Contains FastAPI route definitions for the trading bot system.
"""

from . import bot_routes, exchange_routes, system_routes, strategy_routes

__all__ = ["bot_routes", "exchange_routes", "system_routes", "strategy_routes"]
