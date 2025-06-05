"""
Position management API routes.

Provides endpoints for monitoring and managing trading positions
across all connected exchanges.

Now uses PostgreSQL as the single source of truth for trade data.
Positions are calculated on-demand from database trades.
"""

from typing import Dict, Any, List, Optional
from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel, Field
import structlog
from datetime import datetime

from order_management.tracking import PositionManager
from database.repositories import TradeRepository
from database import get_session

logger = structlog.get_logger(__name__)
router = APIRouter()


# Dependency injection placeholder - will be set by main.py
def get_position_manager() -> PositionManager:
    """Get position manager dependency - will be overridden by main.py"""
    raise HTTPException(status_code=500, detail="Position manager not available")

def get_exchange_manager_for_positions():
    """Get exchange manager for position initialization."""
    try:
        # Try to import the global exchange manager instance
        import main
        if hasattr(main, 'get_exchange_manager_instance'):
            return main.get_exchange_manager_instance()
        else:
            # Fallback - try to get from globals
            if hasattr(main, 'exchange_manager'):
                return main.exchange_manager
    except:
        pass
    return None


async def get_trade_repository():
    """Get trade repository with database session."""
    async for session in get_session():
        return TradeRepository(session)


@router.get("/", summary="Get all positions")
async def get_all_positions(
    symbol: Optional[str] = Query(None, description="Filter by symbol"),
    exchange: Optional[str] = Query(None, description="Filter by exchange"),
    position_manager: PositionManager = Depends(get_position_manager)
):
    """Get all positions, optionally filtered by symbol or exchange."""
    try:
        # Set up trade repository
        async for session in get_session():
            trade_repository = TradeRepository(session)
            position_manager.set_trade_repository(trade_repository)
            
            # Get positions from database
            if symbol:
                positions = await position_manager.get_all_positions(symbol)
            else:
                positions = await position_manager.get_all_positions()
                
            # Filter by exchange if specified
            if exchange:
                positions = [pos for pos in positions if pos.exchange == exchange]
            
            # Convert to dictionaries
            position_data = [pos.to_dict() for pos in positions]
            
            return {
                "positions": position_data,
                "count": len(position_data),
                "filters": {
                    "symbol": symbol,
                    "exchange": exchange
                }
            }
    except Exception as e:
        logger.error(f"Error getting positions: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/summary", summary="Get position summary")
async def get_position_summary(
    position_manager: PositionManager = Depends(get_position_manager)
):
    """Get comprehensive position summary calculated from database trades."""
    try:
        # Get exchange manager to ensure all exchanges are represented
        exchange_manager = get_exchange_manager_for_positions()
        
        # Set up trade repository
        async for session in get_session():
            trade_repository = TradeRepository(session)
            position_manager.set_trade_repository(trade_repository)
            
            if exchange_manager:
                # Get connected exchange connectors
                connections = exchange_manager.get_all_connections()
                sync_connectors = {}
                for conn in connections:
                    if conn.has_credentials and conn.connector and conn.status.value == "connected":
                        sync_connectors[conn.connection_id] = conn.connector
                
                # Ensure all exchanges have position metadata for BERA/USDT
                if sync_connectors:
                    position_manager.ensure_exchange_positions(sync_connectors, "BERA/USDT")
            
            # Calculate summary from database trades
            summary = await position_manager.summarize_positions()
            
            return {
                "summary": summary,
                "timestamp": datetime.now().isoformat()
            }
    except Exception as e:
        logger.error(f"Error getting position summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/net", summary="Get net positions")
async def get_net_positions(
    symbol: Optional[str] = Query(None, description="Filter by symbol"),
    position_manager: PositionManager = Depends(get_position_manager)
):
    """Get net positions across all exchanges calculated from database trades."""
    try:
        # Get exchange manager to ensure all exchanges are represented
        exchange_manager = get_exchange_manager_for_positions()
        
        # Set up trade repository
        async for session in get_session():
            trade_repository = TradeRepository(session)
            position_manager.set_trade_repository(trade_repository)
            
            if exchange_manager:
                # Get connected exchange connectors
                connections = exchange_manager.get_all_connections()
                sync_connectors = {}
                for conn in connections:
                    if conn.has_credentials and conn.connector and conn.status.value == "connected":
                        sync_connectors[conn.connection_id] = conn.connector
                
                # Ensure all exchanges have position metadata
                if sync_connectors:
                    if symbol:
                        position_manager.ensure_exchange_positions(sync_connectors, symbol)
                    else:
                        position_manager.ensure_exchange_positions(sync_connectors, "BERA/USDT")
            
            # Calculate net positions from database trades
            if symbol:
                net_position = await position_manager.get_net_position(symbol)
                return {
                    "net_positions": {symbol: net_position},
                    "count": 1 if net_position['is_open'] else 0
                }
            else:
                net_positions = await position_manager.get_all_net_positions()
                open_positions = {k: v for k, v in net_positions.items() if v['is_open']}
                
                return {
                    "net_positions": net_positions,
                    "open_positions": open_positions,
                    "count": len(open_positions)
                }
    except Exception as e:
        logger.error(f"Error getting net positions: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/export/json", summary="Export positions to JSON")
async def export_positions(
    position_manager: PositionManager = Depends(get_position_manager)
):
    """Export all position data as JSON calculated from database trades."""
    try:
        # Get exchange manager to ensure all exchanges are represented
        exchange_manager = get_exchange_manager_for_positions()
        
        # Set up trade repository
        async for session in get_session():
            trade_repository = TradeRepository(session)
            position_manager.set_trade_repository(trade_repository)
            
            if exchange_manager:
                # Get connected exchange connectors
                connections = exchange_manager.get_all_connections()
                sync_connectors = {}
                for conn in connections:
                    if conn.has_credentials and conn.connector and conn.status.value == "connected":
                        sync_connectors[conn.connection_id] = conn.connector
                
                # Ensure all exchanges have position metadata for BERA/USDT
                if sync_connectors:
                    position_manager.ensure_exchange_positions(sync_connectors, "BERA/USDT")
            
            # Get current timestamp
            current_timestamp = datetime.now().isoformat()
            
            # Collect all position data from database
            export_data = {
                'timestamp': current_timestamp,
                'summary': await position_manager.summarize_positions(),
                'net_positions': await position_manager.get_all_net_positions(),
                'detailed_positions': []
            }
            
            # Add detailed positions
            positions = await position_manager.get_all_positions()
            for pos in positions:
                export_data['detailed_positions'].append(pos.to_dict())
                
            return export_data
    except Exception as e:
        logger.error(f"Error exporting positions: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/exchanges/{exchange}", summary="Get positions for specific exchange")
async def get_exchange_positions(
    exchange: str,
    position_manager: PositionManager = Depends(get_position_manager)
):
    """Get all positions for a specific exchange calculated from database trades."""
    try:
        # Set up trade repository
        async for session in get_session():
            trade_repository = TradeRepository(session)
            position_manager.set_trade_repository(trade_repository)
            
            # Get positions from database
            positions = await position_manager.get_positions_by_exchange_async(exchange)
            position_data = [pos.to_dict() for pos in positions]
            
            return {
                "exchange": exchange,
                "positions": position_data,
                "count": len(position_data)
            }
    except Exception as e:
        logger.error(f"Error getting positions for exchange {exchange}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{exchange}/{symbol}", summary="Get specific position")
async def get_position(
    exchange: str,
    symbol: str,
    position_manager: PositionManager = Depends(get_position_manager)
):
    """Get a specific position by exchange and symbol calculated from database trades."""
    try:
        # Set up trade repository
        async for session in get_session():
            trade_repository = TradeRepository(session)
            position_manager.set_trade_repository(trade_repository)
            
            # Get position from database
            position = await position_manager.get_position_async(exchange, symbol)
            
            if not position or not position.is_open:
                raise HTTPException(
                    status_code=404,
                    detail=f"Position not found for {exchange} {symbol}"
                )
                
            return {
                "position": position.to_dict(),
                "exchange": exchange,
                "symbol": symbol
            }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting position for {exchange} {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{exchange}/{symbol}", summary="Reset specific position")
async def reset_position(
    exchange: str,
    symbol: str,
    position_manager: PositionManager = Depends(get_position_manager)
):
    """Reset (delete) a specific position by removing its trades from database."""
    try:
        # Set up trade repository
        async for session in get_session():
            trade_repository = TradeRepository(session)
            position_manager.set_trade_repository(trade_repository)
            
            success = await position_manager.reset_position(exchange, symbol)
            
            if not success:
                raise HTTPException(
                    status_code=404,
                    detail=f"Position not found for {exchange} {symbol}"
                )
                
            return {
                "message": f"Position reset successfully for {exchange} {symbol}",
                "exchange": exchange,
                "symbol": symbol
            }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error resetting position for {exchange} {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/", summary="Reset all positions")
async def reset_all_positions(
    confirm: bool = Query(False, description="Confirmation required"),
    position_manager: PositionManager = Depends(get_position_manager)
):
    """Reset (delete) all positions by clearing all trades from database. Requires confirmation."""
    try:
        if not confirm:
            raise HTTPException(
                status_code=400,
                detail="Reset all positions requires confirmation. Add ?confirm=true to the request."
            )
            
        # Set up trade repository
        async for session in get_session():
            trade_repository = TradeRepository(session)
            position_manager.set_trade_repository(trade_repository)
            
            await position_manager.reset_all_positions()
            
            return {
                "message": "All positions have been reset successfully",
                "timestamp": datetime.now().isoformat()
            }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error resetting all positions: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/initialize", summary="Initialize positions for all exchanges")
async def initialize_positions(
    symbol: str = Query("BERA/USDT", description="Symbol to initialize positions for"),
    position_manager: PositionManager = Depends(get_position_manager)
):
    """Initialize position metadata for all connected exchanges."""
    try:
        exchange_manager = get_exchange_manager_for_positions()
        
        if not exchange_manager:
            raise HTTPException(status_code=500, detail="Exchange manager not available")
        
        # Set up trade repository
        async for session in get_session():
            trade_repository = TradeRepository(session)
            position_manager.set_trade_repository(trade_repository)
            
            # Get connected exchange connectors
            connections = exchange_manager.get_all_connections()
            sync_connectors = {}
            for conn in connections:
                if conn.has_credentials and conn.connector and conn.status.value == "connected":
                    sync_connectors[conn.connection_id] = conn.connector
            
            if not sync_connectors:
                return {
                    "message": "No connected exchanges found",
                    "symbol": symbol,
                    "exchanges_initialized": 0
                }
            
            # Ensure all exchanges have position metadata
            position_manager.ensure_exchange_positions(sync_connectors, symbol)
            
            # Get the positions (will be calculated from database trades)
            positions = await position_manager.get_all_positions(symbol)
            
            return {
                "message": f"Position metadata initialized for {symbol}",
                "symbol": symbol,
                "exchanges_initialized": len(sync_connectors),
                "exchanges": list(sync_connectors.keys()),
                "total_positions": len(positions)
            }
        
    except Exception as e:
        logger.error(f"Error initializing positions: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 