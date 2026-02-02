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
from fastapi import WebSocket, WebSocketDisconnect

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from api.routes import bot_routes
from api.routes import exchange_routes
from api.routes import system_routes
from api.routes import strategy_routes
from api.routes import position_routes
from api.routes import market_data
from api.routes import trade_collection
from api.routes import coefficient_routes
from api.services.bot_manager import BotManager
from api.services.exchange_manager import ExchangeManager
from api.services.websocket_manager import initialize_websocket_manager, get_websocket_manager
from api.services.risk_data_service import RiskDataService
from order_management.tracking import PositionManager
from config.settings import load_config
from database.connection import init_db
from market_data_collection.startup import ensure_trade_collection_running
import redis.asyncio as redis

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
session_maker = None
redis_client: Optional[redis.Redis] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan context manager."""
    global bot_manager, exchange_manager, position_manager, websocket_manager, session_maker, redis_client
    
    logger.info("Starting trading bot system API...")
    
    try:
        # Load configuration
        config = load_config()
        
        # Initialize database
        session_maker = await init_db()
        
        # Initialize Redis
        redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
        redis_client = redis.from_url(redis_url)
        await redis_client.ping()
        logger.info("Connected to Redis")
        
        # Pass Redis client to coefficient routes
        coefficient_routes.router.redis_client = redis_client
        
        # Initialize services
        exchange_manager = ExchangeManager(config)
        bot_manager = BotManager(config)
        position_manager = PositionManager(data_dir="data/positions")
        risk_data_service = RiskDataService(session_maker)
        
        # Initialize WebSocket manager
        websocket_manager = initialize_websocket_manager(
            bot_manager=bot_manager,
            exchange_manager=exchange_manager,
            position_manager=position_manager,
            session_maker=session_maker,
            risk_data_service=risk_data_service
        )
        
        # Start services
        await exchange_manager.start()
        await bot_manager.start()
        await position_manager.start()
        
        # Start WebSocket periodic updates
        asyncio.create_task(websocket_manager.start_periodic_updates())
        
        # Ensure trade collection service is running
        trade_collection_started = await ensure_trade_collection_running()
        if trade_collection_started:
            logger.info("Trade collection service is running")
        else:
            logger.warning("Trade collection service failed to start - volume-weighted strategies may not work properly")
        
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
        if redis_client:
            await redis_client.close()
            
        logger.info("Trading bot system API shut down complete")


# Create FastAPI app
app = FastAPI(
    title="Trading Bot System API",
    description="REST API for managing trading bot instances and exchange connections",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware (fully open for local testing; no credentials)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
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
app.include_router(trade_collection.router, prefix="/api", tags=["Trade Collection"])
app.include_router(coefficient_routes.router, prefix="/api/coefficients", tags=["Coefficients"])

# WebSocket endpoint for real-time updates
@app.websocket("/ws/risk-monitoring")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time risk monitoring updates."""
    ws_manager = get_websocket_manager()
    await ws_manager.connect(websocket)
    try:
        while True:
            try:
                message = await websocket.receive_text()
                await ws_manager.handle_client_message(websocket, message)
            except WebSocketDisconnect:
                logger.info("WebSocket client disconnected normally")
                break
            except Exception as e:
                if "1012" in str(e):
                    logger.info("WebSocket closed for service restart")
                else:
                    logger.error(f"Error in WebSocket communication: {e}")
                break
    except WebSocketDisconnect:
        logger.info("WebSocket disconnected")
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
# NOTE: For local development we host the React dev server separately on port 4000.
# The production static hosting below is intentionally commented out for local testing.
# frontend_path = PROJECT_ROOT / "frontend" / "build"
# if frontend_path.exists():
#     app.mount("/static", StaticFiles(directory=str(frontend_path / "static")), name="static")
#     
#     @app.get("/{path:path}")
#     async def serve_frontend(path: str):
#         """Serve frontend application."""
#         # For SPA routing, serve index.html for all paths that don't match API or static
#         if path.startswith(("api/", "docs", "health")):
#             raise HTTPException(status_code=404, detail="Not found")
#             
#         file_path = frontend_path / path
#         if file_path.exists() and file_path.is_file():
#             return FileResponse(str(file_path))
#         else:
#             # Serve index.html for SPA routing
#             return FileResponse(str(frontend_path / "index.html") )


if __name__ == "__main__":
    import uvicorn
    
    # Development server
    uvicorn.run(
        "api.main:app",
        host="0.0.0.0",
        port=8081,
        reload=True,
        log_level="info"
    ) 
