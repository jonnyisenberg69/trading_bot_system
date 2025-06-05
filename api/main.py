#!/usr/bin/env python
"""
FastAPI backend for trading bot system.

Provides REST API endpoints for the frontend to manage bot instances,
monitor exchange connections, and control trading operations.
"""

import os
import sys
import asyncio
import logging
from typing import Dict, Any, List, Optional
from pathlib import Path
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import structlog
from fastapi import WebSocket

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from api.routes import bot_routes, exchange_routes, system_routes, strategy_routes, position_routes, market_data
from api.services.bot_manager import BotManager
from api.services.exchange_manager import ExchangeManager
from api.services.websocket_manager import initialize_websocket_manager, get_websocket_manager
from order_management.tracking import PositionManager
from config.settings import load_config

# Configure logging
logging.basicConfig(level=logging.INFO)
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger(__name__)

# Global services
bot_manager: Optional[BotManager] = None
exchange_manager: Optional[ExchangeManager] = None
position_manager: Optional[PositionManager] = None
websocket_manager = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan context manager."""
    global bot_manager, exchange_manager, position_manager, websocket_manager
    
    logger.info("Starting trading bot system API...")
    
    try:
        # Load configuration
        config = load_config()
        
        # Initialize services
        exchange_manager = ExchangeManager(config)
        bot_manager = BotManager(config)
        position_manager = PositionManager(data_dir="data/positions")
        
        # Initialize WebSocket manager
        websocket_manager = initialize_websocket_manager(position_manager)
        
        # Start services
        await exchange_manager.start()
        await bot_manager.start()
        await position_manager.start()
        
        # Start WebSocket periodic updates
        asyncio.create_task(websocket_manager.start_periodic_updates())
        
        logger.info("Trading bot system API started successfully")
        
        # Override dependencies using FastAPI's built-in system
        from api.routes.bot_routes import get_bot_manager
        from api.routes.exchange_routes import get_exchange_manager
        from api.routes.position_routes import get_position_manager
        from api.routes.system_routes import get_bot_manager as system_get_bot_manager
        from api.routes.system_routes import get_exchange_manager as system_get_exchange_manager
        from api.routes.strategy_routes import get_bot_manager as strategy_get_bot_manager
        from api.routes.strategy_routes import get_exchange_manager as strategy_get_exchange_manager
        from api.routes.market_data import get_exchange_manager as market_data_get_exchange_manager
        
        app.dependency_overrides[get_bot_manager] = lambda: bot_manager
        app.dependency_overrides[get_exchange_manager] = lambda: exchange_manager
        app.dependency_overrides[get_position_manager] = lambda: position_manager
        app.dependency_overrides[system_get_bot_manager] = lambda: bot_manager
        app.dependency_overrides[system_get_exchange_manager] = lambda: exchange_manager
        app.dependency_overrides[strategy_get_bot_manager] = lambda: bot_manager
        app.dependency_overrides[strategy_get_exchange_manager] = lambda: exchange_manager
        app.dependency_overrides[market_data_get_exchange_manager] = lambda: exchange_manager
        
        yield
        
    except Exception as e:
        logger.error(f"Failed to start services: {e}")
        raise
    finally:
        # Cleanup
        logger.info("Shutting down trading bot system API...")
        
        if websocket_manager:
            await websocket_manager.stop_periodic_updates()
        if bot_manager:
            await bot_manager.stop()
        if exchange_manager:
            await exchange_manager.stop()
        if position_manager:
            await position_manager.stop()
            
        logger.info("Trading bot system API shut down complete")


# Create FastAPI app
app = FastAPI(
    title="Trading Bot System API",
    description="REST API for managing trading bot instances and exchange connections",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],  # React dev server
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(bot_routes.router, prefix="/api/bots", tags=["Bots"])
app.include_router(exchange_routes.router, prefix="/api/exchanges", tags=["Exchanges"])
app.include_router(position_routes.router, prefix="/api/positions", tags=["Positions"])
app.include_router(system_routes.router, prefix="/api/system", tags=["System"])
app.include_router(strategy_routes.router, prefix="/api/strategies", tags=["Strategies"])
app.include_router(market_data.router, prefix="/api/market-data", tags=["Market Data"])

# WebSocket endpoint for real-time updates
@app.websocket("/ws/risk-monitoring")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time risk monitoring updates."""
    ws_manager = get_websocket_manager()
    await ws_manager.connect(websocket)
    
    try:
        # Keep connection alive and handle messages
        while True:
            try:
                # Wait for client messages (heartbeat, requests, etc.)
                message = await websocket.receive_text()
                # Process client messages if needed
                # For now, just log them
                logger.info(f"Received WebSocket message: {message}")
                
            except Exception as e:
                logger.error(f"Error in WebSocket communication: {e}")
                break
                
    except Exception as e:
        logger.error(f"WebSocket connection error: {e}")
    finally:
        ws_manager.disconnect(websocket)

# Root endpoint
@app.get("/", summary="API root")
async def read_root():
    """API root endpoint."""
    return {
        "message": "Trading Bot System API",
        "version": "1.0.0",
        "docs_url": "/docs",
        "health_url": "/api/system/health"
    }

# Health check endpoint
@app.get("/health", summary="Health check")
async def health_check():
    """Quick health check endpoint."""
    return {
        "status": "healthy",
        "service": "trading-bot-api"
    }

# Serve static files (frontend)
frontend_path = PROJECT_ROOT / "frontend" / "build"
if frontend_path.exists():
    app.mount("/static", StaticFiles(directory=str(frontend_path / "static")), name="static")
    
    @app.get("/{path:path}")
    async def serve_frontend(path: str):
        """Serve frontend application."""
        # For SPA routing, serve index.html for all paths that don't match API or static
        if path.startswith(("api/", "docs", "health")):
            raise HTTPException(status_code=404, detail="Not found")
            
        file_path = frontend_path / path
        if file_path.exists() and file_path.is_file():
            return FileResponse(str(file_path))
        else:
            # Serve index.html for SPA routing
            return FileResponse(str(frontend_path / "index.html"))


if __name__ == "__main__":
    import uvicorn
    
    # Development server
    uvicorn.run(
        "api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    ) 