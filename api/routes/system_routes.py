"""
System status API routes.

Provides endpoints for overall system health, status, and configuration.
"""

from typing import Dict, Any, List, Optional
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
import structlog
import platform
import psutil
import datetime
import os
import time

from api.services.bot_manager import BotManager
from api.services.exchange_manager import ExchangeManager
from api.routes.position_routes import get_position_manager
from order_management.tracking import PositionManager
from utils.trade_sync_logger import setup_trade_sync_logger, log_sync_summary

logger = structlog.get_logger(__name__)
router = APIRouter()


# Dependency injection placeholders - will be set by main.py
def get_bot_manager() -> BotManager:
    """Get bot manager dependency - will be overridden by main.py"""
    raise HTTPException(status_code=500, detail="Bot manager not available")

def get_exchange_manager() -> ExchangeManager:
    """Get exchange manager dependency - will be overridden by main.py"""
    raise HTTPException(status_code=500, detail="Exchange manager not available")


@router.get("/status", summary="Get system status")
async def get_system_status(
    bot_manager: BotManager = Depends(get_bot_manager),
    exchange_manager: ExchangeManager = Depends(get_exchange_manager)
):
    """Get overall system status including bots and exchanges."""
    try:
        # Get bot status
        all_bots = bot_manager.get_all_instances()
        running_bots = bot_manager.get_running_instances()
        
        # Get exchange status
        all_exchanges = exchange_manager.get_all_connections()
        connected_exchanges = exchange_manager.get_connected_exchanges()
        
        # System metrics
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        return {
            "system": {
                "status": "operational",
                "timestamp": datetime.datetime.now().isoformat(),
                "uptime": datetime.datetime.now().isoformat(),  # Placeholder
                "version": "1.0.0"
            },
            "bots": {
                "total": len(all_bots),
                "running": len(running_bots),
                "status": "healthy" if len(running_bots) > 0 else "idle"
            },
            "exchanges": {
                "total": len(all_exchanges),
                "connected": len(connected_exchanges),
                "status": "healthy" if len(connected_exchanges) > 0 else "disconnected"
            },
            "resources": {
                "cpu_percent": cpu_percent,
                "memory_percent": memory.percent,
                "memory_available_gb": round(memory.available / (1024**3), 2),
                "disk_percent": disk.percent,
                "disk_free_gb": round(disk.free / (1024**3), 2)
            }
        }
    except Exception as e:
        logger.error(f"Error getting system status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health", summary="Health check endpoint")
async def health_check():
    """Simple health check endpoint."""
    return {
        "status": "healthy",
        "timestamp": datetime.datetime.now().isoformat(),
        "service": "trading-bot-system"
    }


@router.get("/info", summary="Get system information")
async def get_system_info():
    """Get detailed system information."""
    try:
        return {
            "platform": {
                "system": platform.system(),
                "release": platform.release(),
                "version": platform.version(),
                "machine": platform.machine(),
                "processor": platform.processor(),
                "python_version": platform.python_version()
            },
            "hardware": {
                "cpu_count": psutil.cpu_count(),
                "memory_total_gb": round(psutil.virtual_memory().total / (1024**3), 2),
                "disk_total_gb": round(psutil.disk_usage('/').total / (1024**3), 2)
            },
            "environment": {
                "working_directory": os.getcwd(),
                "environment_variables": {
                    "PYTHONPATH": os.environ.get("PYTHONPATH", ""),
                    "PATH": os.environ.get("PATH", "")[:200] + "..."  # Truncated for security
                }
            }
        }
    except Exception as e:
        logger.error(f"Error getting system info: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/dashboard", summary="Get dashboard overview")
async def get_dashboard_overview(
    bot_manager: BotManager = Depends(get_bot_manager),
    exchange_manager: ExchangeManager = Depends(get_exchange_manager)
):
    """Get dashboard overview with key metrics."""
    try:
        # Get bot summary
        all_bots = bot_manager.get_all_instances()
        running_bots = bot_manager.get_running_instances()
        
        # Get exchange summary
        exchange_summary = await exchange_manager.get_exchange_status_summary()
        
        # Group bots by status
        bot_status_counts = {}
        for bot in all_bots:
            status = bot.status.value
            bot_status_counts[status] = bot_status_counts.get(status, 0) + 1
            
        # Group bots by strategy
        strategy_counts = {}
        for bot in all_bots:
            strategy = bot.strategy
            strategy_counts[strategy] = strategy_counts.get(strategy, 0) + 1
            
        return {
            "overview": {
                "bots": {
                    "total": len(all_bots),
                    "running": len(running_bots),
                    "status_breakdown": bot_status_counts,
                    "strategy_breakdown": strategy_counts
                },
                "exchanges": {
                    "total": exchange_summary["total_exchanges"],
                    "connected": exchange_summary["connected"],
                    "status_breakdown": {
                        "connected": exchange_summary["connected"],
                        "disconnected": exchange_summary["disconnected"],
                        "error": exchange_summary["error"],
                        "invalid_credentials": exchange_summary["invalid_credentials"],
                        "no_credentials": exchange_summary["no_credentials"]
                    }
                }
            },
            "running_bots": [bot.to_dict() for bot in running_bots],
            "connected_exchanges": exchange_summary["connections"][:5],  # Limit for dashboard
            "last_updated": datetime.datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting dashboard overview: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/metrics", summary="Get system metrics")
async def get_system_metrics():
    """Get detailed system performance metrics."""
    try:
        # CPU metrics
        cpu_percent = psutil.cpu_percent(interval=1, percpu=True)
        cpu_freq = psutil.cpu_freq()
        
        # Memory metrics
        memory = psutil.virtual_memory()
        swap = psutil.swap_memory()
        
        # Disk metrics
        disk = psutil.disk_usage('/')
        disk_io = psutil.disk_io_counters()
        
        # Network metrics
        network_io = psutil.net_io_counters()
        
        return {
            "cpu": {
                "percent_total": sum(cpu_percent) / len(cpu_percent),
                "percent_per_core": cpu_percent,
                "frequency_mhz": cpu_freq.current if cpu_freq else None,
                "core_count": len(cpu_percent)
            },
            "memory": {
                "total_gb": round(memory.total / (1024**3), 2),
                "available_gb": round(memory.available / (1024**3), 2),
                "used_gb": round(memory.used / (1024**3), 2),
                "percent": memory.percent
            },
            "swap": {
                "total_gb": round(swap.total / (1024**3), 2),
                "used_gb": round(swap.used / (1024**3), 2),
                "percent": swap.percent
            },
            "disk": {
                "total_gb": round(disk.total / (1024**3), 2),
                "used_gb": round(disk.used / (1024**3), 2),
                "free_gb": round(disk.free / (1024**3), 2),
                "percent": disk.percent,
                "read_bytes": disk_io.read_bytes if disk_io else 0,
                "write_bytes": disk_io.write_bytes if disk_io else 0
            },
            "network": {
                "bytes_sent": network_io.bytes_sent if network_io else 0,
                "bytes_recv": network_io.bytes_recv if network_io else 0,
                "packets_sent": network_io.packets_sent if network_io else 0,
                "packets_recv": network_io.packets_recv if network_io else 0
            },
            "timestamp": datetime.datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting system metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/logs", summary="Get system logs")
async def get_system_logs(
    limit: int = 100,
    level: Optional[str] = None
):
    """Get recent system logs."""
    try:
        # Placeholder - in production, this would read from actual log files
        return {
            "logs": [],
            "total_count": 0,
            "limit": limit,
            "level_filter": level,
            "message": "Log retrieval not yet implemented"
        }
    except Exception as e:
        logger.error(f"Error getting system logs: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/shutdown", summary="Shutdown system")
async def shutdown_system(
    bot_manager: BotManager = Depends(get_bot_manager),
    exchange_manager: ExchangeManager = Depends(get_exchange_manager)
):
    """Gracefully shutdown the entire system."""
    try:
        # Stop all bots
        for instance in bot_manager.get_running_instances():
            await bot_manager.stop_instance(instance.instance_id)
            
        # Stop bot manager
        await bot_manager.stop()
        
        # Stop exchange manager
        await exchange_manager.stop()
        
        return {
            "message": "System shutdown initiated",
            "timestamp": datetime.datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error shutting down system: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/accounts", summary="Get trading accounts")
async def get_accounts(
    bot_manager: BotManager = Depends(get_bot_manager),
    exchange_manager: ExchangeManager = Depends(get_exchange_manager)
):
    """Get all configured trading accounts (collections of exchange credentials)."""
    try:
        # Get all exchange connections
        connections = exchange_manager.get_all_connections()
        
        # Get all bot instances to determine active symbols
        bot_instances = bot_manager.get_all_instances()
        active_symbols = set()
        for bot in bot_instances:
            if bot.symbol:
                active_symbols.add(bot.symbol)
        
        # For now, we have one trading account called "PASSIVE_QUOTING" 
        # that includes all the configured exchanges
        account = {
            "name": "PASSIVE_QUOTING",
            "exchanges": [],
            "status": "active",
            "last_sync": None,
            "description": "Main trading account for passive quoting strategy",
            "active_symbols": list(active_symbols) if active_symbols else ["BERA/USDT"]  # Fallback
        }
        
        # Add all connected exchanges to this account
        for conn in connections:
            if conn.has_credentials:  # Only include exchanges with valid credentials
                account["exchanges"].append({
                    "connection_id": conn.connection_id,
                    "name": conn.name,
                    "exchange_type": conn.exchange_type,
                    "status": conn.status.value,
                    "connected": conn.status.value == "connected"
                })
        
        # Set account status based on exchanges
        if len(account["exchanges"]) == 0:
            account["status"] = "inactive"
        elif any(ex["connected"] for ex in account["exchanges"]):
            account["status"] = "active"
        else:
            account["status"] = "error"
        
        return {
            "accounts": [account],
            "total_count": 1
        }
    except Exception as e:
        logger.error(f"Error getting accounts: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/sync-trades", summary="Sync trades for account")
async def sync_trades(
    request: dict,
    bot_manager: BotManager = Depends(get_bot_manager),
    exchange_manager: ExchangeManager = Depends(get_exchange_manager),
    position_manager: PositionManager = Depends(get_position_manager)
):
    """Trigger trade synchronization for a specific account within time range."""
    # Set up trade sync logging
    trade_sync_logger = setup_trade_sync_logger()
    
    try:
        account = request.get("account")
        start_time = request.get("start_time")
        end_time = request.get("end_time")
        symbols = request.get("symbols")  # Can be None, we'll determine from bots
        
        if not account or not start_time or not end_time:
            raise HTTPException(
                status_code=400,
                detail="account, start_time, and end_time are required"
            )
        
        # Only accept PASSIVE_QUOTING account for now
        if account != "PASSIVE_QUOTING":
            raise HTTPException(
                status_code=404,
                detail=f"Account {account} not found. Available accounts: PASSIVE_QUOTING"
            )
        
        # Parse datetime strings
        from datetime import datetime
        try:
            start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
        except ValueError as e:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid datetime format: {e}. Use ISO format like '2025-06-04T19:00:00'"
            )
        
        # Get symbols from bot instances if not provided
        if not symbols:
            bot_instances = bot_manager.get_all_instances()
            symbols = []
            
            for bot in bot_instances:
                if bot.symbol and bot.symbol not in symbols:
                    symbols.append(bot.symbol)
                    
                # Also check if the bot has exchange-specific symbols
                if hasattr(bot, 'config') and isinstance(bot.config, dict):
                    exchange_symbols = bot.config.get('exchange_symbols', {})
                    for exchange_id, symbol in exchange_symbols.items():
                        if symbol and symbol not in symbols:
                            symbols.append(symbol)
        
        # Fallback to a default if no symbols found
        if not symbols:
            symbols = ["BERA/USDT"]  # Based on the current bot configuration
            
        logger.info(f"Determined symbols from bot instances: {symbols}")
        
        # Extract base coins for filtering (e.g., BERA from BERA/USDT)
        base_coins = []
        for symbol in symbols:
            if '/' in symbol:
                base_coin = symbol.split('/')[0]
                if base_coin not in base_coins:
                    base_coins.append(base_coin)
            else:
                # If no slash, assume it's already a base coin
                if symbol not in base_coins:
                    base_coins.append(symbol)
        
        logger.info(f"Extracted base coins for filtering: {base_coins}")
        
        # Get all connected exchanges for the PASSIVE_QUOTING account
        connections = exchange_manager.get_all_connections()
        active_connections = [
            conn for conn in connections 
            if conn.has_credentials and conn.connector and conn.status.value == "connected"
        ]
        
        if not active_connections:
            raise HTTPException(
                status_code=404,
                detail=f"No active connections found for account {account}"
            )
        
        # Use the enhanced trade sync system with pagination
        from order_management.enhanced_trade_sync import EnhancedTradeSync
        from database.repositories import TradeRepository
        from database import get_session
        
        # Get database session and repositories
        async for session in get_session():
            trade_repository = TradeRepository(session)
            break
        
        # Set up position manager with trade repository
        position_manager.set_trade_repository(trade_repository)
        
        # Create exchange connectors dict for enhanced sync
        sync_connectors = {}
        for conn in active_connections:
            sync_connectors[conn.connection_id] = conn.connector
        
        # Initialize enhanced trade sync
        enhanced_trade_sync = EnhancedTradeSync(
            exchange_connectors=sync_connectors,
            trade_repository=trade_repository,
            position_manager=position_manager
        )
        
        # Get list of all exchange IDs
        all_exchanges = [conn.connection_id for conn in active_connections]

        # Convert symbols for each exchange based on their requirements
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
        
        def get_normalized_symbol_for_aggregation(exchange_symbol: str, exchange_id: str) -> str:
            """Convert exchange-specific symbol back to normalized symbol for position aggregation."""
            
            # CRITICAL FIX: Keep spot and perp positions separate by using different normalized symbols
            # This prevents bybit_spot and bybit_perp from being treated as the same position
            
            if exchange_id == 'hyperliquid_perp':
                # BERA/USDC:USDC -> BERA/USDT-PERP (FIX: prevent BERA/USDT:USDT-PERP format)
                if '/USDC:USDC' in exchange_symbol:
                    base = exchange_symbol.split('/')[0]
                    return f"{base}/USDT-PERP"
                elif ':' in exchange_symbol:
                    # Handle any other colon-based format
                    base = exchange_symbol.split('/')[0] if '/' in exchange_symbol else exchange_symbol.split(':')[0]
                    return f"{base}/USDT-PERP"
                else:
                    # If no colon, just add -PERP suffix
                    return f"{exchange_symbol}-PERP"
                    
            elif exchange_id in ['binance_perp', 'bybit_perp']:
                # BERAUSDT -> BERA/USDT-PERP, BERUSDT -> BERA/USDT-PERP
                if 'USDT' in exchange_symbol and '/' not in exchange_symbol:
                    if exchange_symbol.endswith('USDT'):
                        base = exchange_symbol[:-4]  # Remove 'USDT'
                        return f"{base}/USDT-PERP"
                # If already has slash, add -PERP suffix (but check for existing colon issues)
                elif '/' in exchange_symbol:
                    # CRITICAL FIX: Remove any existing colon artifacts
                    clean_symbol = exchange_symbol.split(':')[0] if ':' in exchange_symbol else exchange_symbol
                    return f"{clean_symbol}-PERP"
                else:
                    return f"{exchange_symbol}-PERP"
                    
            elif exchange_id in ['binance_spot', 'bybit_spot', 'mexc_spot', 'gateio_spot', 'bitget_spot']:
                # For spot exchanges, use standard BERA/USDT format
                if '/' in exchange_symbol:
                    base, quote = exchange_symbol.split('/')[0], exchange_symbol.split('/')[1]
                    # Clean any colon artifacts from quote
                    quote_clean = quote.split(':')[0] if ':' in quote else quote
                    # Check if base coin matches our target base coins
                    if base in base_coins:
                        return f"{base}/USDT"  # Normalize to USDT for spot
                        
                # For symbols without slash, check if it matches base coins
                elif exchange_symbol.replace('USDT', '').replace('USDC', '') in base_coins:
                    base = exchange_symbol.replace('USDT', '').replace('USDC', '')
                    return f"{base}/USDT"
                    
            # Default: return as-is with exchange type suffix for safety
            if 'perp' in exchange_id or 'future' in exchange_id:
                # Clean any colon artifacts before adding -PERP
                clean_symbol = exchange_symbol.split(':')[0] if ':' in exchange_symbol else exchange_symbol
                return f"{clean_symbol}-PERP"
            else:
                # Clean any colon artifacts for spot
                clean_symbol = exchange_symbol.split(':')[0] if ':' in exchange_symbol else exchange_symbol
                return clean_symbol
        
        logger.info(f"Starting trade sync for account: {account}")
        logger.info(f"Time range: {start_dt} to {end_dt}")
        logger.info(f"Base symbols: {symbols}")
        
        sync_start_time = time.time()
        results = {}
        successful_syncs = 0
        failed_syncs = 0
        total_trades_synced = 0
        total_requests_made = 0
        
        # Sync for each exchange
        for exchange_id in all_exchanges:
            try:
                # Convert symbols for this specific exchange
                exchange_symbols = [convert_symbol_for_exchange(symbol, exchange_id) for symbol in symbols]
                
                logger.info(f"Syncing {exchange_id} with symbols: {exchange_symbols}")
                
                # Use enhanced trade sync with exchange-specific symbols
                pagination_results = await enhanced_trade_sync.sync_exchange_with_pagination(
                    exchange_name=exchange_id,
                    symbols=exchange_symbols,
                    since=start_dt
                )
                
                # Process results and update position manager
                total_trades = 0
                total_requests = 0
                for result in pagination_results:
                    total_trades += result.total_fetched
                    total_requests += result.total_requests
                    
                    # NOTE: Position manager updates are now handled in enhanced_trade_sync.py
                    # This ensures only unique (non-duplicate) trades update positions
                    # and prevents double-updating the same trade
                    
                    # Process trades for metadata only
                    for trade in result.trades:
                        # Get the original exchange-specific symbol from the trade
                        original_symbol = trade.get('symbol', exchange_symbols[0])
                        
                        # Normalize to base symbol for consistent position tracking
                        # BUT keep spot and perp separate!
                        normalized_symbol = get_normalized_symbol_for_aggregation(original_symbol, exchange_id)
                        
                        # Ensure all trades use the correct normalized symbol for position aggregation
                        trade['symbol'] = normalized_symbol
                        
                        # Add exchange-specific metadata for tracking
                        trade['exchange_symbol'] = original_symbol
                        trade['exchange_id'] = exchange_id
                        
                        # Position manager update is now handled in enhanced_trade_sync.py
                        # to ensure only unique trades are processed
                
                results[exchange_id] = {
                    'success': True,
                    'trades_synced': total_trades,
                    'requests_made': total_requests,
                    'time_taken_ms': sum(r.time_taken_ms for r in pagination_results),
                    'symbols_processed': exchange_symbols,
                    'errors': []
                }
                
                successful_syncs += 1
                total_trades_synced += total_trades
                total_requests_made += total_requests
                
                logger.info(f"✅ {exchange_id}: {total_trades} trades synced")
                
            except Exception as e:
                error_msg = f"Failed to sync {exchange_id}: {str(e)}"
                logger.error(error_msg)
                results[exchange_id] = {
                    'success': False,
                    'trades_synced': 0,
                    'requests_made': 0,
                    'time_taken_ms': 0,
                    'symbols_processed': [],
                    'errors': [error_msg]
                }
                failed_syncs += 1
        
        # Calculate total sync time
        total_sync_time_ms = int((time.time() - sync_start_time) * 1000)
        
        # Log overall sync summary
        log_sync_summary(
            trade_sync_logger,
            total_exchanges=len(all_exchanges),
            successful=successful_syncs,
            failed=failed_syncs,
            total_trades=total_trades_synced,
            total_time_ms=total_sync_time_ms
        )
        
        # CRITICAL: Ensure ALL connected exchanges have positions (even zero positions)
        # This will make hyperliquid and bitget show up with 0.0 values
        try:
            # Get all symbols that were normalized (both spot and perp variants)
            all_normalized_symbols = set()
            for symbol in symbols:
                # Add spot version
                base = symbol.split('/')[0] if '/' in symbol else symbol
                all_normalized_symbols.add(f"{base}/USDT")  # Spot
                all_normalized_symbols.add(f"{base}/USDT-PERP")  # Perp
            
            # Ensure positions exist for all exchanges and all symbol variants
            for normalized_symbol in all_normalized_symbols:
                position_manager.ensure_exchange_positions(sync_connectors, normalized_symbol)
                
            logger.info(f"Ensured positions for symbols: {list(all_normalized_symbols)}")
            
            # CRITICAL FIX: Sync positions from exchanges that support position querying
            # This will catch Hyperliquid positions that exist but weren't created by recent trades
            try:
                await position_manager.sync_positions_from_exchanges(sync_connectors)
                logger.info("Synced existing positions from exchanges")
            except Exception as e:
                logger.warning(f"Failed to sync positions from exchanges: {e}")
            
        except Exception as e:
            logger.warning(f"Failed to ensure exchange positions: {e}")
        
        # Calculate summary
        total_exchanges = len(results)
        
        # Note: Positions are automatically saved by the position manager
        # and will be available immediately for subsequent API calls
        logger.info(f"Sync completed with positions available from PostgreSQL")
        
        return {
            "account": account,
            "time_range": {
                "start": start_time,
                "end": end_time
            },
            "symbols": symbols,
            "summary": {
                "total_exchanges": total_exchanges,
                "successful_syncs": successful_syncs,
                "failed_syncs": failed_syncs,
                "total_trades_synced": total_trades_synced,
                "total_requests_made": total_requests_made,
                "success_rate": f"{(successful_syncs / total_exchanges * 100):.1f}%" if total_exchanges else "0%"
            },
            "results": results,
            "message": f"Enhanced sync completed for {account}: {total_trades_synced} trades synced across {total_exchanges} exchanges using symbols: {', '.join(symbols)}"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in enhanced trade sync: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 