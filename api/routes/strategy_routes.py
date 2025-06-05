"""
Strategy-specific API routes.

Provides endpoints for managing strategy configurations, especially passive quoting.
"""

import json
import os
import time
from typing import Dict, Any, List, Optional
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
import structlog

from config.settings import load_config, DEFAULT_CONFIG_PATH
from api.services.bot_manager import BotManager
from utils.symbol_utils import SymbolMapper, validate_symbol, get_base_quote, get_exchange_symbols

logger = structlog.get_logger(__name__)
router = APIRouter()


class PassiveQuotingLineConfig(BaseModel):
    """Configuration for a single passive quoting line."""
    timeout: int = Field(..., description="Timeout in seconds", ge=1)
    drift: float = Field(..., description="Drift in basis points", ge=0)
    quantity: float = Field(..., description="Order quantity", gt=0)
    quantity_randomization_factor: float = Field(
        ..., description="Quantity randomization percentage", ge=0, le=100
    )
    spread: float = Field(..., description="Spread in basis points", gt=0)
    sides: str = Field(..., description="Order sides", pattern="^(both|bid|offer)$")


class PassiveQuotingConfig(BaseModel):
    """Passive quoting strategy configuration."""
    base_coin: str = Field(..., description="Base coin symbol")
    quantity_currency: str = Field(
        ..., description="Quantity currency", pattern="^(base|quote)$"
    )
    exchanges: List[str] = Field(..., description="List of exchanges", min_items=1)
    lines: List[PassiveQuotingLineConfig] = Field(
        ..., description="Quote lines configuration", min_items=1
    )


class QuickUpdateRequest(BaseModel):
    """Request for quick bot configuration update."""
    instance_id: str = Field(..., description="Bot instance ID")
    config: PassiveQuotingConfig = Field(..., description="New configuration")


class CreatePassiveQuotingBotRequest(BaseModel):
    """Request for creating a passive quoting bot."""
    symbol: str = Field(..., description="Trading symbol (e.g., BTC/USDT)")
    config: PassiveQuotingConfig = Field(..., description="Strategy configuration")


class ValidateSymbolRequest(BaseModel):
    """Request for validating a trading symbol."""
    symbol: str = Field(..., description="Trading symbol to validate")
    exchanges: Optional[List[str]] = Field(default=None, description="List of exchanges to get specific formats for")


# Dependency injection placeholder
def get_bot_manager() -> BotManager:
    """Get bot manager dependency - will be overridden by main.py"""
    raise HTTPException(status_code=500, detail="Bot manager not available")


def get_exchange_manager():
    """Get exchange manager dependency - will be overridden by main.py"""
    raise HTTPException(status_code=500, detail="Exchange manager not available")


@router.get("/passive-quoting/config", summary="Get passive quoting default config")
async def get_passive_quoting_config(exchange_manager=Depends(get_exchange_manager)):
    """Get the default passive quoting configuration."""
    try:
        config = load_config()
        passive_config = config.get("strategies", {}).get("passive_quoting", {})
        
        if not passive_config:
            raise HTTPException(
                status_code=404, 
                detail="Passive quoting configuration not found"
            )
        
        # Get available exchanges from exchange manager instead of config
        available_exchanges = [conn.connection_id for conn in exchange_manager.get_all_connections()]
            
        return {
            "default_config": passive_config.get("default_config"),
            "presets": passive_config.get("presets", {}),
            "available_exchanges": available_exchanges,
            "available_coins": ["BTC", "ETH", "BNB", "ADA", "SOL", "DOT", "MATIC", "AVAX"]
        }
    except Exception as e:
        logger.error(f"Error getting passive quoting config: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/passive-quoting/config", summary="Update passive quoting default config")
async def update_passive_quoting_config(config: PassiveQuotingConfig):
    """Update the default passive quoting configuration."""
    try:
        # Load current config
        current_config = load_config()
        
        # Update passive quoting config
        if "strategies" not in current_config:
            current_config["strategies"] = {}
        if "passive_quoting" not in current_config["strategies"]:
            current_config["strategies"]["passive_quoting"] = {}
            
        current_config["strategies"]["passive_quoting"]["default_config"] = config.dict()
        
        # Save updated config
        with open(DEFAULT_CONFIG_PATH, 'w') as f:
            json.dump(current_config, f, indent=4)
            
        logger.info("Updated passive quoting default configuration")
        return {"message": "Configuration updated successfully"}
        
    except Exception as e:
        logger.error(f"Error updating passive quoting config: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/passive-quoting/presets", summary="Get passive quoting presets")
async def get_passive_quoting_presets():
    """Get available passive quoting configuration presets."""
    try:
        config = load_config()
        presets = config.get("strategies", {}).get("passive_quoting", {}).get("presets", {})
        
        return {"presets": presets}
    except Exception as e:
        logger.error(f"Error getting passive quoting presets: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/passive-quoting/presets/{preset_name}", summary="Save passive quoting preset")
async def save_passive_quoting_preset(preset_name: str, config: PassiveQuotingConfig):
    """Save a new passive quoting configuration preset."""
    try:
        # Load current config
        current_config = load_config()
        
        # Ensure structure exists
        if "strategies" not in current_config:
            current_config["strategies"] = {}
        if "passive_quoting" not in current_config["strategies"]:
            current_config["strategies"]["passive_quoting"] = {}
        if "presets" not in current_config["strategies"]["passive_quoting"]:
            current_config["strategies"]["passive_quoting"]["presets"] = {}
            
        # Save preset
        current_config["strategies"]["passive_quoting"]["presets"][preset_name] = config.dict()
        
        # Save updated config
        with open(DEFAULT_CONFIG_PATH, 'w') as f:
            json.dump(current_config, f, indent=4)
            
        logger.info(f"Saved passive quoting preset: {preset_name}")
        return {"message": f"Preset '{preset_name}' saved successfully"}
        
    except Exception as e:
        logger.error(f"Error saving passive quoting preset: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/passive-quoting/presets/{preset_name}", summary="Delete passive quoting preset")
async def delete_passive_quoting_preset(preset_name: str):
    """Delete a passive quoting configuration preset."""
    try:
        # Load current config
        current_config = load_config()
        
        presets = current_config.get("strategies", {}).get("passive_quoting", {}).get("presets", {})
        
        if preset_name not in presets:
            raise HTTPException(status_code=404, detail=f"Preset '{preset_name}' not found")
            
        # Remove preset
        del presets[preset_name]
        
        # Save updated config
        with open(DEFAULT_CONFIG_PATH, 'w') as f:
            json.dump(current_config, f, indent=4)
            
        logger.info(f"Deleted passive quoting preset: {preset_name}")
        return {"message": f"Preset '{preset_name}' deleted successfully"}
        
    except Exception as e:
        logger.error(f"Error deleting passive quoting preset: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/passive-quoting/create", summary="Create passive quoting bot")
async def create_passive_quoting_bot(
    request: CreatePassiveQuotingBotRequest,
    bot_manager: BotManager = Depends(get_bot_manager),
    exchange_manager=Depends(get_exchange_manager)
):
    """Create a new passive quoting bot instance."""
    try:
        # Validate trading symbol format
        if not validate_symbol(request.symbol):
            raise HTTPException(
                status_code=400,
                detail=f"Invalid trading symbol format: '{request.symbol}'. Use BASE/QUOTE format (e.g., BERA/USDT)"
            )
        
        # Normalize the symbol to standard format
        normalized_symbol = SymbolMapper.normalize_symbol(request.symbol)
        
        # Auto-populate base coin if not provided
        if not request.config.base_coin:
            base_coin, quote_coin = get_base_quote(normalized_symbol)
            if base_coin:
                request.config.base_coin = base_coin
        
        # Validate exchanges are available - use exchange manager instead of config
        available_connections = {conn.connection_id for conn in exchange_manager.get_all_connections()}
        
        for exchange in request.config.exchanges:
            if exchange not in available_connections:
                raise HTTPException(
                    status_code=400, 
                    detail=f"Exchange connection '{exchange}' is not available. Available connections: {sorted(available_connections)}"
                )
        
        # Get exchange-specific symbol formats
        exchange_symbols = get_exchange_symbols(normalized_symbol, request.config.exchanges)
        logger.info(f"Exchange-specific symbols: {exchange_symbols}")
        
        # Create bot instance with normalized symbol
        instance_id = await bot_manager.create_instance(
            strategy="passive_quoting",
            symbol=normalized_symbol,
            exchanges=request.config.exchanges,
            config={
                **request.config.dict(),
                'exchange_symbols': exchange_symbols  # Store exchange-specific symbols
            }
        )
        
        return {
            "instance_id": instance_id,
            "message": "Passive quoting bot created successfully",
            "config": request.config.dict(),
            "normalized_symbol": normalized_symbol,
            "exchange_symbols": exchange_symbols
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating passive quoting bot: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/passive-quoting/quick-update", summary="Quick config update")
async def quick_config_update(
    request: QuickUpdateRequest,
    bot_manager: BotManager = Depends(get_bot_manager)
):
    """
    Quickly update a bot's configuration.
    Stops bot, cancels orders, updates config, and restarts in under 5 seconds.
    """
    try:
        start_time = time.time()
        
        # Get bot instance
        instance = bot_manager.get_instance(request.instance_id)
        if not instance:
            raise HTTPException(
                status_code=404, 
                detail=f"Bot instance {request.instance_id} not found"
            )
        
        # Step 1: Stop bot if running
        if instance.status.value == "running":
            logger.info(f"Stopping bot {request.instance_id}")
            await bot_manager.stop_instance(request.instance_id)
        
        # Step 2: Update configuration
        logger.info(f"Updating config for bot {request.instance_id}")
        instance.config = request.config.dict()
        
        # Save updated instance
        await bot_manager._save_instance(instance)
        
        # Step 3: Start bot with new config
        logger.info(f"Starting bot {request.instance_id} with new config")
        success = await bot_manager.start_instance(request.instance_id)
        
        if not success:
            raise HTTPException(
                status_code=500, 
                detail="Failed to start bot with new configuration"
            )
        
        end_time = time.time()
        update_duration = end_time - start_time
        
        logger.info(f"Quick update completed in {update_duration:.2f} seconds")
        
        return {
            "message": "Configuration updated successfully",
            "instance_id": request.instance_id,
            "update_duration_seconds": round(update_duration, 2),
            "new_config": request.config.dict(),
            "bot_status": instance.status.value
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in quick config update: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/passive-quoting/{instance_id}/status", summary="Get bot detailed status")
async def get_passive_quoting_bot_status(
    instance_id: str,
    bot_manager: BotManager = Depends(get_bot_manager)
):
    """Get detailed status of a passive quoting bot instance."""
    try:
        instance = bot_manager.get_instance(instance_id)
        if not instance:
            raise HTTPException(
                status_code=404, 
                detail=f"Bot instance {instance_id} not found"
            )
        
        # Get detailed status if it's a passive quoting strategy
        # In production, this would interface with the actual strategy instance
        status = instance.to_dict()
        
        # Add passive quoting specific information
        if instance.strategy == "passive_quoting":
            status["strategy_details"] = {
                "type": "passive_quoting",
                "config": instance.config,
                "estimated_orders_per_line": 2,  # bid + ask
                "total_estimated_orders": len(instance.config.get("lines", [])) * 2
            }
        
        return status
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting bot status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/available-strategies", summary="Get available strategies")
async def get_available_strategies():
    """Get list of available trading strategies."""
    return {
        "strategies": [
            {
                "name": "passive_quoting",
                "display_name": "Passive Quoting",
                "description": "Places bid/ask orders around midpoint with configurable parameters",
                "features": [
                    "Multiple quote lines",
                    "Timeout-based order cancellation", 
                    "Drift-based order cancellation",
                    "Quantity randomization",
                    "Multi-exchange support"
                ]
            },
            {
                "name": "market_making",
                "display_name": "Market Making",
                "description": "Traditional market making strategy",
                "features": ["Coming soon"]
            },
            {
                "name": "arbitrage", 
                "display_name": "Arbitrage",
                "description": "Cross-exchange arbitrage opportunities",
                "features": ["Coming soon"]
            }
        ]
    }


@router.post("/validate-symbol", summary="Validate trading symbol")
async def validate_trading_symbol(request: ValidateSymbolRequest):
    """
    Validate a trading symbol and get exchange-specific formats.
    
    Args:
        request: Symbol validation request
        
    Returns:
        Validation result and exchange-specific formats
    """
    try:
        # Validate symbol format
        is_valid = validate_symbol(request.symbol)
        
        if not is_valid:
            return {
                "valid": False,
                "error": "Invalid symbol format. Use BASE/QUOTE format (e.g., BERA/USDT)",
                "normalized_symbol": None,
                "base_coin": None,
                "quote_coin": None,
                "exchange_symbols": {}
            }
        
        # Normalize symbol
        normalized_symbol = SymbolMapper.normalize_symbol(request.symbol)
        base_coin, quote_coin = get_base_quote(normalized_symbol)
        
        # Get exchange-specific formats if exchanges provided
        exchange_symbols = {}
        if request.exchanges:
            exchange_symbols = get_exchange_symbols(normalized_symbol, request.exchanges)
        
        return {
            "valid": True,
            "normalized_symbol": normalized_symbol,
            "base_coin": base_coin,
            "quote_coin": quote_coin,
            "exchange_symbols": exchange_symbols
        }
        
    except Exception as e:
        logger.error(f"Error validating symbol: {e}")
        return {
            "valid": False,
            "error": str(e),
            "normalized_symbol": None,
            "base_coin": None,
            "quote_coin": None,
            "exchange_symbols": {}
        } 