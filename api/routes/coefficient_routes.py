"""
API routes for coefficient management and monitoring.

These routes now provide access to bot-specific coefficients since each
volume-weighted bot instance calculates its own coefficients.
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone
import redis.asyncio as redis
from enum import Enum
import json

router = APIRouter()


class CalculationMethod(str, Enum):
    """Supported coefficient calculation methods."""
    MIN = "min"
    MAX = "max"
    MID = "mid"


@router.get("/current/{bot_id}")
async def get_bot_coefficients(
    bot_id: str,
    redis_client: redis.Redis = Depends(lambda: router.redis_client)
) -> Dict[str, Any]:
    """
    Get current coefficients for a specific bot instance.
    
    Since each bot calculates its own coefficients, we need the bot ID.
    """
    try:
        # Get all coefficients for this bot
        pattern = f"bot_coefficient:{bot_id}:*"
        keys = []
        
        async for key in redis_client.scan_iter(match=pattern):
            keys.append(key)
        
        if not keys:
            return {
                "bot_id": bot_id,
                "coefficients": {},
                "message": "No coefficients found for this bot. Bot may not be running or hasn't calculated coefficients yet."
            }
        
        coefficients = {}
        for key in keys:
            data = await redis_client.get(key)
            if data:
                coeff_data = json.loads(data)
                symbol = coeff_data.get('symbol', 'unknown')
                exchange = coeff_data.get('exchange', 'unknown')
                coefficients[f"{symbol}_{exchange}"] = coeff_data
        
        return {
            "bot_id": bot_id,
            "coefficients": coefficients,
            "count": len(coefficients),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get bot coefficients: {str(e)}")


@router.get("/status")
async def get_coefficient_status(
    redis_client: redis.Redis = Depends(lambda: router.redis_client)
) -> Dict[str, Any]:
    """
    Get overall status of coefficient calculations across all bots.
    
    This endpoint shows which bots are calculating coefficients.
    """
    try:
        # Scan for all bot coefficient keys
        bot_stats = {}
        pattern = "bot_coefficient:*"
        
        async for key in redis_client.scan_iter(match=pattern):
            key_str = key.decode() if isinstance(key, bytes) else key
            parts = key_str.split(':')
            if len(parts) >= 4:
                bot_id = parts[1]
                
                if bot_id not in bot_stats:
                    bot_stats[bot_id] = {
                        'coefficient_count': 0,
                        'last_update': None,
                        'exchanges': set()
                    }
                
                # Get coefficient data
                data = await redis_client.get(key)
                if data:
                    coeff_data = json.loads(data)
                    bot_stats[bot_id]['coefficient_count'] += 1
                    bot_stats[bot_id]['exchanges'].add(coeff_data.get('exchange', 'unknown'))
                    
                    # Track latest update time
                    timestamp = coeff_data.get('timestamp')
                    if timestamp:
                        if not bot_stats[bot_id]['last_update'] or timestamp > bot_stats[bot_id]['last_update']:
                            bot_stats[bot_id]['last_update'] = timestamp
        
        # Convert sets to lists for JSON serialization
        for bot_id, stats in bot_stats.items():
            stats['exchanges'] = list(stats['exchanges'])
        
        return {
            "status": "operational",
            "total_bots_with_coefficients": len(bot_stats),
            "bot_statistics": bot_stats,
            "note": "Each volume-weighted bot calculates its own coefficients independently",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "total_bots_with_coefficients": 0,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }


@router.get("/bot/{bot_id}/history")
async def get_bot_coefficient_history(
    bot_id: str,
    symbol: str,
    exchange: str,
    redis_client: redis.Redis = Depends(lambda: router.redis_client)
) -> Dict[str, Any]:
    """
    Get coefficient history for a specific bot/symbol/exchange combination.
    
    Note: This only shows the latest coefficient since historical data
    is not persisted in the current implementation.
    """
    try:
        key = f"bot_coefficient:{bot_id}:{symbol}:{exchange}"
        data = await redis_client.get(key)
        
        if not data:
            return {
                "bot_id": bot_id,
                "symbol": symbol,
                "exchange": exchange,
                "history": [],
                "message": "No coefficient data found"
            }
        
        coeff_data = json.loads(data)
        
        return {
            "bot_id": bot_id,
            "symbol": symbol,
            "exchange": exchange,
            "current_coefficient": coeff_data.get('coefficient'),
            "last_updated": coeff_data.get('timestamp'),
            "calculation_method": coeff_data.get('calculation_method', 'unknown'),
            "history": [coeff_data],  # Only current value available
            "note": "Historical data not persisted in current implementation"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get coefficient history: {str(e)}")


@router.get("/calculation-methods")
async def get_calculation_methods() -> Dict[str, Any]:
    """Get available coefficient calculation methods."""
    return {
        "methods": [
            {
                "name": "min",
                "description": "Use the minimum ratio across all moving averages (most conservative)"
            },
            {
                "name": "max", 
                "description": "Use the maximum ratio across all moving averages (most aggressive)"
            },
            {
                "name": "mid",
                "description": "Use the midpoint between min and max ratios (balanced approach)"
            }
        ],
        "default": "min",
        "note": "Each bot can specify its own calculation method"
    }


@router.get("/health")
async def coefficient_health_check(
    redis_client: redis.Redis = Depends(lambda: router.redis_client)
) -> Dict[str, Any]:
    """Health check for coefficient system."""
    try:
        # Check Redis connectivity
        await redis_client.ping()
        redis_status = "healthy"
    except Exception as e:
        redis_status = f"unhealthy: {str(e)}"
    
    return {
        "service": "coefficient_management",
        "status": "healthy" if redis_status == "healthy" else "degraded",
        "redis_status": redis_status,
        "architecture": "per-bot calculation",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


@router.get("/bot/{bot_id}/exchanges/{symbol}")
async def get_bot_exchange_coefficients(
    bot_id: str,
    symbol: str,
    redis_client: redis.Redis = Depends(lambda: router.redis_client)
) -> Dict[str, Any]:
    """Get coefficients across all exchanges for a bot and symbol."""
    try:
        pattern = f"bot_coefficient:{bot_id}:{symbol}:*"
        coefficients = {}
        
        async for key in redis_client.scan_iter(match=pattern):
            data = await redis_client.get(key)
            if data:
                coeff_data = json.loads(data)
                exchange = coeff_data.get('exchange', 'unknown')
                coefficients[exchange] = {
                    'coefficient': coeff_data.get('coefficient'),
                    'last_updated': coeff_data.get('timestamp'),
                    'method': coeff_data.get('calculation_method', 'unknown')
                }
        
        if not coefficients:
            return {
                "bot_id": bot_id,
                "symbol": symbol,
                "exchanges": {},
                "message": "No coefficients found. Bot may not be running or hasn't calculated coefficients yet."
            }
        
        # Find best and worst exchanges
        sorted_exchanges = sorted(coefficients.items(), key=lambda x: x[1]['coefficient'], reverse=True)
        
        return {
            "bot_id": bot_id,
            "symbol": symbol,
            "exchanges": coefficients,
            "best_exchange": sorted_exchanges[0][0] if sorted_exchanges else None,
            "worst_exchange": sorted_exchanges[-1][0] if sorted_exchanges else None,
            "coefficient_range": {
                "min": sorted_exchanges[-1][1]['coefficient'] if sorted_exchanges else None,
                "max": sorted_exchanges[0][1]['coefficient'] if sorted_exchanges else None
            },
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get exchange coefficients: {str(e)}")


@router.post("/clear/{bot_id}")
async def clear_bot_coefficients(
    bot_id: str,
    redis_client: redis.Redis = Depends(lambda: router.redis_client)
) -> Dict[str, Any]:
    """
    Clear all coefficients for a specific bot.
    
    This is useful when restarting a bot with new configuration.
    """
    try:
        pattern = f"bot_coefficient:{bot_id}:*"
        deleted_count = 0
        
        async for key in redis_client.scan_iter(match=pattern):
            await redis_client.delete(key)
            deleted_count += 1
        
        return {
            "bot_id": bot_id,
            "deleted_count": deleted_count,
            "message": f"Cleared {deleted_count} coefficient entries for bot {bot_id}",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to clear coefficients: {str(e)}")


# Legacy endpoints for backward compatibility (return empty/default responses)

@router.get("/current/{symbol}")
async def get_current_coefficients_legacy(symbol: str) -> Dict[str, Any]:
    """
    Legacy endpoint - coefficients are now per-bot, not global.
    """
    return {
        "symbol": symbol,
        "coefficients": {},
        "message": "This endpoint is deprecated. Coefficients are now calculated per bot instance. Use /api/coefficients/current/{bot_id} instead.",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


@router.post("/refresh/{symbol}")
async def refresh_coefficients_legacy(symbol: str) -> Dict[str, Any]:
    """
    Legacy endpoint - coefficients are now managed by individual bots.
    """
    return {
        "symbol": symbol,
        "status": "deprecated",
        "message": "Coefficient calculation is now handled by individual bot instances. Each bot refreshes its own coefficients automatically.",
        "timestamp": datetime.now(timezone.utc).isoformat()
    } 