"""
Market data API routes for trading bot system.

Provides access to:
- Real-time orderbook data from all connected exchanges
- Market information and trading pairs
- Price data and market statistics
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, HTTPException, Depends, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
import structlog

from api.services.exchange_manager import ExchangeManager


logger = structlog.get_logger(__name__)

router = APIRouter()

# Dependency injection
async def get_exchange_manager() -> ExchangeManager:
    """Get exchange manager instance."""
    # This will be overridden by dependency injection in main.py
    raise HTTPException(status_code=500, detail="Exchange manager not available")


@router.get("/orderbook/{exchange_name}/{symbol}")
async def get_orderbook(
    exchange_name: str,
    symbol: str,
    limit: int = Query(20, ge=5, le=1000, description="Number of price levels to return"),
    exchange_manager: ExchangeManager = Depends(get_exchange_manager)
) -> Dict[str, Any]:
    """
    Get orderbook for a specific symbol on a specific exchange.
    
    Args:
        exchange_name: Name of the exchange (e.g., 'binance', 'bybit')
        symbol: Trading symbol (e.g., 'BTC/USDT')
        limit: Number of price levels to return (5-1000)
        
    Returns:
        Orderbook data with bids, asks, and metadata
    """
    try:
        # Get exchange connector
        connector = await exchange_manager.get_connector(exchange_name)
        if not connector:
            raise HTTPException(
                status_code=404, 
                detail=f"Exchange '{exchange_name}' not found or not connected"
            )
        
        # Check if connector is connected
        if not connector.connected:
            raise HTTPException(
                status_code=503,
                detail=f"Exchange '{exchange_name}' is not connected"
            )
        
        # Get orderbook
        orderbook = await connector.get_orderbook(symbol, limit)
        
        if not orderbook:
            raise HTTPException(
                status_code=404,
                detail=f"Orderbook not found for {symbol} on {exchange_name}"
            )
        
        # Add metadata
        response = {
            "exchange": exchange_name,
            "symbol": symbol,
            "timestamp": orderbook.get("timestamp", int(datetime.now(timezone.utc).timestamp() * 1000)),
            "datetime": orderbook.get("datetime", datetime.now(timezone.utc).isoformat()),
            "bids": orderbook.get("bids", []),
            "asks": orderbook.get("asks", []),
            "bid_count": len(orderbook.get("bids", [])),
            "ask_count": len(orderbook.get("asks", [])),
            "spread": None,
            "mid_price": None
        }
        
        # Calculate spread and mid price if data available
        bids = response["bids"]
        asks = response["asks"]
        
        if bids and asks:
            best_bid = float(bids[0][0])  # First bid price
            best_ask = float(asks[0][0])  # First ask price
            
            response["spread"] = best_ask - best_bid
            response["mid_price"] = (best_bid + best_ask) / 2
            response["spread_bps"] = (response["spread"] / response["mid_price"]) * 10000 if response["mid_price"] > 0 else 0
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting orderbook for {symbol} on {exchange_name}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get orderbook: {str(e)}"
        )


@router.get("/orderbook/all/{symbol}")
async def get_orderbook_all_exchanges(
    symbol: str,
    limit: int = Query(20, ge=5, le=1000, description="Number of price levels to return"),
    exchange_manager: ExchangeManager = Depends(get_exchange_manager)
) -> Dict[str, Any]:
    """
    Get orderbook for a symbol across all connected exchanges.
    
    Args:
        symbol: Trading symbol (e.g., 'BTC/USDT')
        limit: Number of price levels to return
        
    Returns:
        Orderbook data from all exchanges with comparison metrics
    """
    try:
        # Get all connected exchanges
        exchanges = await exchange_manager.get_connected_exchange_names()
        
        if not exchanges:
            raise HTTPException(
                status_code=503,
                detail="No exchanges are currently connected"
            )
        
        # Fetch orderbooks from all exchanges concurrently
        tasks = []
        exchange_names = []
        
        for exchange_name in exchanges:
            connector = await exchange_manager.get_connector(exchange_name)
            if connector and connector.connected:
                tasks.append(connector.get_orderbook(symbol, limit))
                exchange_names.append(exchange_name)
        
        if not tasks:
            raise HTTPException(
                status_code=503,
                detail="No connected exchanges support the requested symbol"
            )
        
        # Execute all orderbook requests concurrently
        orderbooks = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        exchange_data = {}
        best_bids = []
        best_asks = []
        
        for i, (exchange_name, orderbook) in enumerate(zip(exchange_names, orderbooks)):
            if isinstance(orderbook, Exception):
                logger.warning(f"Failed to get orderbook from {exchange_name}: {orderbook}")
                exchange_data[exchange_name] = {
                    "error": str(orderbook),
                    "available": False
                }
                continue
            
            if not orderbook or not orderbook.get("bids") or not orderbook.get("asks"):
                exchange_data[exchange_name] = {
                    "error": "No orderbook data available",
                    "available": False
                }
                continue
            
            # Calculate metrics for this exchange
            bids = orderbook.get("bids", [])
            asks = orderbook.get("asks", [])
            
            best_bid = float(bids[0][0]) if bids else 0
            best_ask = float(asks[0][0]) if asks else 0
            
            exchange_info = {
                "available": True,
                "timestamp": orderbook.get("timestamp", int(datetime.now(timezone.utc).timestamp() * 1000)),
                "bids": bids,
                "asks": asks,
                "best_bid": best_bid,
                "best_ask": best_ask,
                "spread": best_ask - best_bid if best_bid > 0 and best_ask > 0 else None,
                "mid_price": (best_bid + best_ask) / 2 if best_bid > 0 and best_ask > 0 else None
            }
            
            if exchange_info["spread"] is not None and exchange_info["mid_price"] > 0:
                exchange_info["spread_bps"] = (exchange_info["spread"] / exchange_info["mid_price"]) * 10000
            
            exchange_data[exchange_name] = exchange_info
            
            # Collect for cross-exchange analysis
            if best_bid > 0:
                best_bids.append((exchange_name, best_bid))
            if best_ask > 0:
                best_asks.append((exchange_name, best_ask))
        
        # Cross-exchange analysis
        analysis = {
            "best_bid_exchange": None,
            "best_ask_exchange": None,
            "best_bid_price": None,
            "best_ask_price": None,
            "arbitrage_opportunities": []
        }
        
        if best_bids and best_asks:
            # Find best bid and ask across exchanges
            best_bid_info = max(best_bids, key=lambda x: x[1])
            best_ask_info = min(best_asks, key=lambda x: x[1])
            
            analysis["best_bid_exchange"] = best_bid_info[0]
            analysis["best_bid_price"] = best_bid_info[1]
            analysis["best_ask_exchange"] = best_ask_info[0]
            analysis["best_ask_price"] = best_ask_info[1]
            
            # Look for arbitrage opportunities
            for bid_exchange, bid_price in best_bids:
                for ask_exchange, ask_price in best_asks:
                    if bid_exchange != ask_exchange and bid_price > ask_price:
                        profit = bid_price - ask_price
                        profit_bps = (profit / ask_price) * 10000 if ask_price > 0 else 0
                        
                        analysis["arbitrage_opportunities"].append({
                            "buy_exchange": ask_exchange,
                            "sell_exchange": bid_exchange,
                            "buy_price": ask_price,
                            "sell_price": bid_price,
                            "profit": profit,
                            "profit_bps": profit_bps
                        })
            
            # Sort arbitrage opportunities by profit
            analysis["arbitrage_opportunities"].sort(key=lambda x: x["profit"], reverse=True)
        
        return {
            "symbol": symbol,
            "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
            "exchanges": exchange_data,
            "analysis": analysis,
            "available_exchanges": [name for name, data in exchange_data.items() if data.get("available", False)]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting orderbook for {symbol} across exchanges: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get orderbook data: {str(e)}"
        )


@router.get("/symbols")
async def get_available_symbols(
    exchange_name: Optional[str] = Query(None, description="Filter by specific exchange"),
    exchange_manager: ExchangeManager = Depends(get_exchange_manager)
) -> Dict[str, Any]:
    """
    Get available trading symbols across exchanges.
    
    Args:
        exchange_name: Optional filter by specific exchange
        
    Returns:
        Available symbols per exchange
    """
    try:
        exchanges = await exchange_manager.get_connected_exchange_names()
        
        if exchange_name:
            if exchange_name not in exchanges:
                raise HTTPException(
                    status_code=404,
                    detail=f"Exchange '{exchange_name}' not found or not connected"
                )
            exchanges = [exchange_name]
        
        symbols_data = {}
        
        for exchange in exchanges:
            connector = await exchange_manager.get_connector(exchange)
            if connector and connector.connected:
                try:
                    exchange_info = await connector.get_exchange_info()
                    symbols_data[exchange] = {
                        "symbols": exchange_info.get("symbols", []),
                        "symbol_count": len(exchange_info.get("symbols", [])),
                        "status": "connected"
                    }
                except Exception as e:
                    logger.warning(f"Failed to get symbols from {exchange}: {e}")
                    symbols_data[exchange] = {
                        "symbols": [],
                        "symbol_count": 0,
                        "status": "error",
                        "error": str(e)
                    }
            else:
                symbols_data[exchange] = {
                    "symbols": [],
                    "symbol_count": 0,
                    "status": "disconnected"
                }
        
        # Find common symbols across exchanges
        all_symbols = set()
        for data in symbols_data.values():
            if data["status"] == "connected":
                all_symbols.update(data["symbols"])
        
        common_symbols = all_symbols.copy()
        for data in symbols_data.values():
            if data["status"] == "connected":
                common_symbols = common_symbols.intersection(set(data["symbols"]))
        
        return {
            "exchanges": symbols_data,
            "common_symbols": sorted(list(common_symbols)),
            "all_symbols": sorted(list(all_symbols)),
            "total_unique_symbols": len(all_symbols),
            "common_symbol_count": len(common_symbols)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting available symbols: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get symbols: {str(e)}"
        )


@router.get("/exchanges/status")
async def get_exchanges_status(
    exchange_manager: ExchangeManager = Depends(get_exchange_manager)
) -> Dict[str, Any]:
    """
    Get status of all exchanges and their market data capabilities.
    
    Returns:
        Status information for all exchanges
    """
    try:
        # Get all exchanges (connected and configured)
        all_exchanges = await exchange_manager.get_all_exchanges()
        connected_exchanges = await exchange_manager.get_connected_exchange_names()
        
        exchange_status = {}
        
        for exchange_name in all_exchanges:
            connector = await exchange_manager.get_connector(exchange_name)
            
            if connector:
                health_info = await connector.health_check()
                exchange_status[exchange_name] = {
                    "connected": connector.connected,
                    "health": health_info,
                    "supports_orderbook": hasattr(connector, 'get_orderbook'),
                    "supports_websocket": hasattr(connector, 'websocket_manager'),
                    "market_type": getattr(connector, 'market_type', 'unknown')
                }
            else:
                exchange_status[exchange_name] = {
                    "connected": False,
                    "health": {"status": "not_configured"},
                    "supports_orderbook": False,
                    "supports_websocket": False,
                    "market_type": "unknown"
                }
        
        return {
            "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
            "total_exchanges": len(all_exchanges),
            "connected_exchanges": len(connected_exchanges),
            "exchanges": exchange_status,
            "healthy_exchanges": [
                name for name, status in exchange_status.items() 
                if status["connected"] and status["health"].get("status") == "healthy"
            ]
        }
        
    except Exception as e:
        logger.error(f"Error getting exchange status: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get exchange status: {str(e)}"
        )


# WebSocket endpoint for real-time orderbook updates
@router.websocket("/ws/orderbook/{exchange_name}/{symbol}")
async def websocket_orderbook(
    websocket: WebSocket,
    exchange_name: str,
    symbol: str,
    update_interval: int = Query(1000, ge=100, le=5000, description="Update interval in milliseconds")
):
    """
    WebSocket endpoint for real-time orderbook updates.
    
    Args:
        exchange_name: Name of the exchange
        symbol: Trading symbol
        update_interval: Update interval in milliseconds (100-5000ms)
    """
    await websocket.accept()
    
    # This would need to be implemented with the exchange manager
    # For now, return a simple message
    try:
        # Get exchange manager (would need proper dependency injection for WebSocket)
        # exchange_manager = get_exchange_manager()
        
        await websocket.send_json({
            "type": "info",
            "message": f"Real-time orderbook updates for {symbol} on {exchange_name} not yet implemented",
            "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000)
        })
        
        # Keep connection alive
        while True:
            try:
                await websocket.receive_text()
            except WebSocketDisconnect:
                break
                
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        await websocket.close()


@router.get("/health")
async def market_data_health() -> Dict[str, Any]:
    """Health check for market data service."""
    return {
        "service": "market_data",
        "status": "healthy",
        "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
        "version": "1.0.0"
    }
