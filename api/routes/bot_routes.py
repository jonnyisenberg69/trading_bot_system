"""
Bot management API routes.

Provides endpoints for creating, starting, stopping, and monitoring bot instances.
"""

from typing import Dict, Any, List, Optional
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
import structlog
from datetime import datetime

from api.services.bot_manager import BotManager

logger = structlog.get_logger(__name__)
router = APIRouter()


class CreateBotRequest(BaseModel):
    """Request model for creating a bot instance."""
    strategy: str = Field(..., description="Trading strategy name")
    symbol: str = Field(..., description="Trading symbol (e.g., BTC/USDT)")
    exchanges: List[str] = Field(..., description="List of exchanges to trade on")
    config: Optional[Dict[str, Any]] = Field(default=None, description="Additional configuration")


class UpdateCredentialsRequest(BaseModel):
    """Request model for updating bot credentials."""
    api_key: str = Field(..., description="API key")
    api_secret: str = Field(..., description="API secret")
    testnet: bool = Field(default=True, description="Use testnet")


# Dependency injection placeholder - will be set by main.py
def get_bot_manager() -> BotManager:
    """Get bot manager dependency - will be overridden by main.py"""
    raise HTTPException(status_code=500, detail="Bot manager not available")


@router.get("/", summary="Get all bot instances")
async def get_bots(bot_manager: BotManager = Depends(get_bot_manager)):
    """Get all bot instances with their current status."""
    try:
        instances = bot_manager.get_all_instances()
        return {
            "instances": [instance.to_dict() for instance in instances],
            "total_count": len(instances),
            "running_count": len(bot_manager.get_running_instances())
        }
    except Exception as e:
        logger.error(f"Error getting bot instances: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/running", summary="Get running bot instances")
async def get_running_bots(bot_manager: BotManager = Depends(get_bot_manager)):
    """Get all currently running bot instances."""
    try:
        instances = bot_manager.get_running_instances()
        return {
            "instances": [instance.to_dict() for instance in instances],
            "count": len(instances)
        }
    except Exception as e:
        logger.error(f"Error getting running bot instances: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{instance_id}", summary="Get bot instance by ID")
async def get_bot(instance_id: str, bot_manager: BotManager = Depends(get_bot_manager)):
    """Get a specific bot instance by ID."""
    try:
        instance = bot_manager.get_instance(instance_id)
        if not instance:
            raise HTTPException(status_code=404, detail=f"Bot instance {instance_id} not found")
            
        return instance.to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting bot instance {instance_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/", summary="Create new bot instance")
async def create_bot(
    request: CreateBotRequest,
    bot_manager: BotManager = Depends(get_bot_manager)
):
    """Create a new bot instance with the specified configuration."""
    try:
        instance_id = await bot_manager.create_instance(
            strategy=request.strategy,
            symbol=request.symbol,
            exchanges=request.exchanges,
            config=request.config
        )
        
        instance = bot_manager.get_instance(instance_id)
        return {
            "instance_id": instance_id,
            "message": "Bot instance created successfully",
            "instance": instance.to_dict() if instance else None
        }
    except Exception as e:
        logger.error(f"Error creating bot instance: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{instance_id}/start", summary="Start bot instance")
async def start_bot(instance_id: str, bot_manager: BotManager = Depends(get_bot_manager)):
    """Start a bot instance."""
    try:
        success = await bot_manager.start_instance(instance_id)
        if not success:
            raise HTTPException(status_code=400, detail=f"Failed to start bot instance {instance_id}")
            
        instance = bot_manager.get_instance(instance_id)
        return {
            "message": f"Bot instance {instance_id} started successfully",
            "instance": instance.to_dict() if instance else None
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error starting bot instance {instance_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{instance_id}/stop", summary="Stop bot instance")
async def stop_bot(instance_id: str, bot_manager: BotManager = Depends(get_bot_manager)):
    """Stop a bot instance."""
    try:
        success = await bot_manager.stop_instance(instance_id)
        if not success:
            raise HTTPException(status_code=400, detail=f"Failed to stop bot instance {instance_id}")
            
        instance = bot_manager.get_instance(instance_id)
        return {
            "message": f"Bot instance {instance_id} stopped successfully",
            "instance": instance.to_dict() if instance else None
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error stopping bot instance {instance_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{instance_id}", summary="Delete bot instance")
async def delete_bot(instance_id: str, bot_manager: BotManager = Depends(get_bot_manager)):
    """Delete a bot instance."""
    try:
        success = await bot_manager.delete_instance(instance_id)
        if not success:
            raise HTTPException(status_code=400, detail=f"Failed to delete bot instance {instance_id}")
            
        return {
            "message": f"Bot instance {instance_id} deleted successfully"
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting bot instance {instance_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status/summary", summary="Get bot status summary")
async def get_bot_status_summary(bot_manager: BotManager = Depends(get_bot_manager)):
    """Get summary of all bot instances and their statuses."""
    try:
        all_instances = bot_manager.get_all_instances()
        running_instances = bot_manager.get_running_instances()
        
        # Group by status
        status_counts = {}
        for instance in all_instances:
            status = instance.status.value
            status_counts[status] = status_counts.get(status, 0) + 1
            
        # Group by strategy
        strategy_counts = {}
        for instance in all_instances:
            strategy = instance.strategy
            strategy_counts[strategy] = strategy_counts.get(strategy, 0) + 1
            
        return {
            "total_instances": len(all_instances),
            "running_instances": len(running_instances),
            "status_breakdown": status_counts,
            "strategy_breakdown": strategy_counts,
            "running_details": [instance.to_dict() for instance in running_instances]
        }
    except Exception as e:
        logger.error(f"Error getting bot status summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{instance_id}/status", summary="Get detailed bot status")
async def get_bot_detailed_status(
    instance_id: str,
    bot_manager: BotManager = Depends(get_bot_manager)
):
    """Get detailed status information for a specific bot instance."""
    instance = bot_manager.get_instance(instance_id)
    if not instance:
        raise HTTPException(status_code=404, detail=f"Bot instance {instance_id} not found")
    
    status = instance.to_dict()
    
    # Add process information if available
    if instance.process:
        status["process_info"] = {
            "pid": instance.process.pid,
            "returncode": instance.process.returncode,
            "is_alive": instance.process.poll() is None
        }
    
    return {"status": "success", "data": status}


@router.get("/{instance_id}/logs", summary="Get bot logs")
async def get_bot_logs(
    instance_id: str,
    lines: int = 100,
    bot_manager: BotManager = Depends(get_bot_manager)
):
    """Get recent log entries for a specific bot instance."""
    instance = bot_manager.get_instance(instance_id)
    if not instance:
        raise HTTPException(status_code=404, detail=f"Bot instance {instance_id} not found")
    
    try:
        from pathlib import Path
        log_file = Path("logs") / "bots" / instance_id / f"{instance_id}.log"
        
        if not log_file.exists():
            return {
                "status": "success", 
                "data": {
                    "logs": [],
                    "message": "No log file found - bot may not have started yet"
                }
            }
        
        # Read last N lines from log file
        with open(log_file, 'r') as f:
            all_lines = f.readlines()
            recent_lines = all_lines[-lines:] if len(all_lines) > lines else all_lines
        
        return {
            "status": "success",
            "data": {
                "logs": [line.strip() for line in recent_lines],
                "total_lines": len(all_lines),
                "showing_lines": len(recent_lines),
                "log_file": str(log_file)
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read logs: {str(e)}")


@router.get("/{instance_id}/performance", summary="Get bot performance stats")
async def get_bot_performance(
    instance_id: str,
    bot_manager: BotManager = Depends(get_bot_manager)
):
    """Get performance statistics for a specific bot instance."""
    instance = bot_manager.get_instance(instance_id)
    if not instance:
        raise HTTPException(status_code=404, detail=f"Bot instance {instance_id} not found")
    
    # For now, return basic performance data
    # In a real implementation, this would connect to the running strategy to get live stats
    perf_data = {
        "instance_id": instance_id,
        "strategy": instance.strategy,
        "symbol": instance.symbol,
        "exchanges": instance.exchanges,
        "uptime_seconds": (
            (datetime.now() - instance.started_at).total_seconds() 
            if instance.started_at and instance.status.value == "running"
            else 0
        ),
        "status": instance.status.value,
        "last_heartbeat": instance.last_heartbeat.isoformat() if instance.last_heartbeat else None,
        "message": "Performance stats would show live trading activity when bot is actively trading"
    }
    
    return {"status": "success", "data": perf_data} 