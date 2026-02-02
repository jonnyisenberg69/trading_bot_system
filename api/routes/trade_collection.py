"""
API routes for trade data collection service management.
"""

from fastapi import APIRouter, HTTPException, Query
from typing import Dict, Any
import structlog

from market_data_collection.startup import (
    get_trade_collection_status,
    restart_trade_collection_service,
    stop_trade_collection_service,
    get_trade_collection_logs
)
from market_data_collection.database import TradeDataManager

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/trade-collection", tags=["Trade Collection"])


@router.get("/status")
async def get_service_status() -> Dict[str, Any]:
    """Get the current status of the trade collection service."""
    try:
        status = get_trade_collection_status()
        return {
            "success": True,
            "data": status
        }
    except Exception as e:
        logger.error(f"Error getting trade collection status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/restart")
async def restart_service() -> Dict[str, Any]:
    """Restart the trade collection service."""
    try:
        success = restart_trade_collection_service()
        
        if success:
            return {
                "success": True,
                "message": "Trade collection service restarted successfully"
            }
        else:
            raise HTTPException(
                status_code=500, 
                detail="Failed to restart trade collection service"
            )
    except Exception as e:
        logger.error(f"Error restarting trade collection service: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/stop")
async def stop_service() -> Dict[str, Any]:
    """Stop the trade collection service."""
    try:
        success = stop_trade_collection_service()
        
        if success:
            return {
                "success": True,
                "message": "Trade collection service stopped successfully"
            }
        else:
            raise HTTPException(
                status_code=500, 
                detail="Failed to stop trade collection service"
            )
    except Exception as e:
        logger.error(f"Error stopping trade collection service: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/logs")
async def get_service_logs(lines: int = Query(50, ge=1, le=1000)) -> Dict[str, Any]:
    """Get recent logs from the trade collection service."""
    try:
        logs = get_trade_collection_logs(lines)
        return {
            "success": True,
            "data": {
                "logs": logs,
                "lines_requested": lines
            }
        }
    except Exception as e:
        logger.error(f"Error getting trade collection logs: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats/{symbol}")
async def get_trade_stats(
    symbol: str,
    exchange: str = Query(None, description="Specific exchange (optional)"),
    hours_back: int = Query(24, ge=1, le=168, description="Hours to look back (1-168)")
) -> Dict[str, Any]:
    """Get trade statistics for a symbol."""
    try:
        db_manager = TradeDataManager()
        await db_manager.initialize()
        
        stats = await db_manager.get_trade_stats(
            symbol=symbol,
            exchange=exchange,
            hours_back=hours_back
        )
        
        await db_manager.close()
        
        return {
            "success": True,
            "data": {
                "symbol": symbol,
                "exchange": exchange,
                "stats": stats
            }
        }
    except Exception as e:
        logger.error(f"Error getting trade stats for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/recent-trades/{symbol}")
async def get_recent_trades(
    symbol: str,
    exchange: str = Query(None, description="Specific exchange (optional)"),
    limit: int = Query(100, ge=1, le=1000, description="Number of trades to return"),
    hours_back: int = Query(24, ge=1, le=168, description="Hours to look back (1-168)")
) -> Dict[str, Any]:
    """Get recent trades for a symbol."""
    try:
        db_manager = TradeDataManager()
        await db_manager.initialize()
        
        trades = await db_manager.get_recent_trades(
            symbol=symbol,
            exchange=exchange,
            limit=limit,
            hours_back=hours_back
        )
        
        await db_manager.close()
        
        return {
            "success": True,
            "data": {
                "symbol": symbol,
                "exchange": exchange,
                "trades": trades,
                "count": len(trades)
            }
        }
    except Exception as e:
        logger.error(f"Error getting recent trades for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 