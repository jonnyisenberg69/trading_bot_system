"""
Exchange management API routes.

Provides endpoints for managing exchange connections, testing connectivity,
and updating API credentials.
"""

from typing import Dict, Any, List, Optional
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
import structlog

from api.services.exchange_manager import ExchangeManager

logger = structlog.get_logger(__name__)
router = APIRouter()


class UpdateCredentialsRequest(BaseModel):
    """Request model for updating exchange credentials."""
    api_key: Optional[str] = Field(default="", description="API key")
    api_secret: Optional[str] = Field(default="", description="API secret")
    wallet_address: Optional[str] = Field(default="", description="Wallet address (for Hyperliquid)")
    private_key: Optional[str] = Field(default="", description="Private key (for Hyperliquid)")
    passphrase: Optional[str] = Field(default="", description="Passphrase (for exchanges like Bitget)")
    testnet: bool = Field(default=True, description="Use testnet")


class BulkCredentialsRequest(BaseModel):
    """Request model for bulk credential application."""
    account_name: str = Field(..., description="Account name for tracking")
    credentials: Dict[str, UpdateCredentialsRequest] = Field(..., description="Exchange-specific credentials")
    test_connections: bool = Field(default=True, description="Test connections after applying credentials")


# Dependency injection placeholder - will be set by main.py
def get_exchange_manager() -> ExchangeManager:
    """Get exchange manager dependency - will be overridden by main.py"""
    raise HTTPException(status_code=500, detail="Exchange manager not available")


@router.get("/", summary="Get all exchange connections")
async def get_exchanges(exchange_manager: ExchangeManager = Depends(get_exchange_manager)):
    """Get all exchange connections with their current status."""
    try:
        connections = exchange_manager.get_all_connections()
        return {
            "connections": [conn.to_dict() for conn in connections],
            "total_count": len(connections),
            "connected_count": len(exchange_manager.get_connected_exchanges())
        }
    except Exception as e:
        logger.error(f"Error getting exchange connections: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status", summary="Get exchange status summary")
async def get_exchange_status(exchange_manager: ExchangeManager = Depends(get_exchange_manager)):
    """Get comprehensive status summary of all exchanges."""
    try:
        summary = await exchange_manager.get_exchange_status_summary()
        return summary
    except Exception as e:
        logger.error(f"Error getting exchange status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/supported", summary="Get supported exchanges")
async def get_supported_exchanges(exchange_manager: ExchangeManager = Depends(get_exchange_manager)):
    """Get list of all supported exchanges and their capabilities."""
    try:
        supported = exchange_manager.get_supported_exchanges()
        return {
            "supported_exchanges": supported,
            "total_count": len(supported)
        }
    except Exception as e:
        logger.error(f"Error getting supported exchanges: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/connected", summary="Get connected exchanges")
async def get_connected_exchanges(exchange_manager: ExchangeManager = Depends(get_exchange_manager)):
    """Get all currently connected exchanges."""
    try:
        # Use the sync method that returns ExchangeConnection objects
        connections = exchange_manager.get_connected_exchanges()
        return {
            "connections": [conn.to_dict() for conn in connections],
            "count": len(connections)
        }
    except Exception as e:
        logger.error(f"Error getting connected exchanges: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{connection_id}", summary="Get exchange connection by ID")
async def get_exchange(
    connection_id: str,
    exchange_manager: ExchangeManager = Depends(get_exchange_manager)
):
    """Get a specific exchange connection by ID."""
    try:
        connection = exchange_manager.get_connection(connection_id)
        if not connection:
            raise HTTPException(status_code=404, detail=f"Exchange connection {connection_id} not found")
            
        return connection.to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting exchange connection {connection_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{connection_id}/test", summary="Test exchange connection")
async def test_exchange_connection(
    connection_id: str,
    exchange_manager: ExchangeManager = Depends(get_exchange_manager)
):
    """Test connectivity for a specific exchange."""
    try:
        success = await exchange_manager.test_connection(connection_id)
        connection = exchange_manager.get_connection(connection_id)
        
        if not connection:
            raise HTTPException(status_code=404, detail=f"Exchange connection {connection_id} not found")
            
        return {
            "connection_id": connection_id,
            "test_successful": success,
            "status": connection.status.value,
            "error_message": connection.error_message,
            "connection": connection.to_dict()
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error testing exchange connection {connection_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{connection_id}/credentials", summary="Update exchange credentials")
async def update_exchange_credentials(
    connection_id: str,
    request: UpdateCredentialsRequest,
    exchange_manager: ExchangeManager = Depends(get_exchange_manager)
):
    """Update API credentials for an exchange connection."""
    try:
        success = await exchange_manager.update_credentials(
            connection_id=connection_id,
            api_key=request.api_key,
            api_secret=request.api_secret,
            wallet_address=request.wallet_address,
            private_key=request.private_key,
            passphrase=request.passphrase,
            testnet=request.testnet
        )
        
        if not success:
            raise HTTPException(
                status_code=400,
                detail=f"Failed to update credentials for {connection_id}"
            )
            
        connection = exchange_manager.get_connection(connection_id)
        return {
            "message": f"Credentials updated successfully for {connection_id}",
            "connection": connection.to_dict() if connection else None
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating credentials for {connection_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{connection_id}/credentials", summary="Delete exchange credentials")
async def delete_exchange_credentials(
    connection_id: str,
    exchange_manager: ExchangeManager = Depends(get_exchange_manager)
):
    """Delete API credentials for an exchange connection."""
    try:
        success = await exchange_manager.delete_credentials(connection_id)
        
        if not success:
            raise HTTPException(
                status_code=400,
                detail=f"Failed to delete credentials for {connection_id}"
            )
            
        connection = exchange_manager.get_connection(connection_id)
        return {
            "message": f"Credentials deleted successfully for {connection_id}",
            "connection": connection.to_dict() if connection else None
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting credentials for {connection_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/bulk-credentials", summary="Apply credentials to multiple exchanges")
async def apply_bulk_credentials(
    request: BulkCredentialsRequest,
    exchange_manager: ExchangeManager = Depends(get_exchange_manager)
):
    """Apply credentials to multiple exchanges at once."""
    try:
        results = []
        successful_count = 0
        failed_count = 0
        
        for connection_id, creds in request.credentials.items():
            try:
                # Get the connection to check if it exists
                connection = exchange_manager.get_connection(connection_id)
                if not connection:
                    results.append({
                        "connection_id": connection_id,
                        "exchange_name": connection_id,
                        "success": False,
                        "error": f"Connection {connection_id} not found"
                    })
                    failed_count += 1
                    continue
                
                # Apply credentials
                success = await exchange_manager.update_credentials(
                    connection_id=connection_id,
                    api_key=creds.api_key,
                    api_secret=creds.api_secret,
                    wallet_address=creds.wallet_address,
                    private_key=creds.private_key,
                    passphrase=creds.passphrase,
                    testnet=creds.testnet
                )
                
                if success:
                    # Test connection if requested
                    test_success = True
                    test_error = None
                    
                    if request.test_connections:
                        try:
                            test_success = await exchange_manager.test_connection(connection_id)
                            if not test_success:
                                # Get updated connection to see error
                                updated_connection = exchange_manager.get_connection(connection_id)
                                test_error = updated_connection.error_message if updated_connection else "Test failed"
                        except Exception as test_e:
                            test_success = False
                            test_error = str(test_e)
                    
                    results.append({
                        "connection_id": connection_id,
                        "exchange_name": connection.name,
                        "exchange_type": connection.exchange_type,
                        "success": True,
                        "test_success": test_success,
                        "test_error": test_error,
                        "testnet": creds.testnet
                    })
                    
                    successful_count += 1
                else:
                    results.append({
                        "connection_id": connection_id,
                        "exchange_name": connection.name,
                        "exchange_type": connection.exchange_type,
                        "success": False,
                        "error": "Failed to update credentials"
                    })
                    failed_count += 1
                    
            except Exception as e:
                logger.error(f"Error applying credentials to {connection_id}: {e}")
                connection = exchange_manager.get_connection(connection_id)
                results.append({
                    "connection_id": connection_id,
                    "exchange_name": connection.name if connection else connection_id,
                    "exchange_type": connection.exchange_type if connection else "unknown",
                    "success": False,
                    "error": str(e)
                })
                failed_count += 1
        
        return {
            "account_name": request.account_name,
            "total_exchanges": len(request.credentials),
            "successful": successful_count,
            "failed": failed_count,
            "results": results,
            "message": f"Applied credentials for account '{request.account_name}' to {successful_count}/{len(request.credentials)} exchanges"
        }
        
    except Exception as e:
        logger.error(f"Error in bulk credential application: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/test-all", summary="Test all exchange connections")
async def test_all_exchanges(exchange_manager: ExchangeManager = Depends(get_exchange_manager)):
    """Test connectivity for all exchange connections."""
    try:
        # This will trigger a test of all connections
        await exchange_manager._test_all_connections()
        
        # Return updated status
        summary = await exchange_manager.get_exchange_status_summary()
        return {
            "message": "All exchange connections tested",
            "summary": summary
        }
    except Exception as e:
        logger.error(f"Error testing all exchange connections: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{connection_id}/markets", summary="Get exchange markets")
async def get_exchange_markets(
    connection_id: str,
    limit: int = 100,
    exchange_manager: ExchangeManager = Depends(get_exchange_manager)
):
    """Get available markets for an exchange."""
    try:
        connection = exchange_manager.get_connection(connection_id)
        if not connection:
            raise HTTPException(status_code=404, detail=f"Exchange connection {connection_id} not found")
            
        if not connection.connector:
            raise HTTPException(
                status_code=400,
                detail=f"Exchange {connection_id} is not connected"
            )
            
        # Get markets from CCXT exchange within connector
        if hasattr(connection.connector, 'exchange') and hasattr(connection.connector.exchange, 'markets'):
            markets = list(connection.connector.exchange.markets.keys())
        else:
            # Fallback: try to get markets directly
            markets = []
        
        return {
            "connection_id": connection_id,
            "total_markets": len(markets),
            "markets": markets[:limit],
            "has_more": len(markets) > limit
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting markets for {connection_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{connection_id}/balance", summary="Get exchange balance")
async def get_exchange_balance(
    connection_id: str,
    exchange_manager: ExchangeManager = Depends(get_exchange_manager)
):
    """Get account balance for an exchange."""
    try:
        connection = exchange_manager.get_connection(connection_id)
        if not connection:
            raise HTTPException(status_code=404, detail=f"Exchange connection {connection_id} not found")
            
        if not connection.connector:
            raise HTTPException(
                status_code=400,
                detail=f"Exchange {connection_id} is not connected"
            )
            
        # Get balance from our connector
        balance = await connection.connector.get_balance()
        
        return {
            "connection_id": connection_id,
            "balance": balance,
            "total_currencies": len(balance) if balance else 0
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting balance for {connection_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{connection_id}/health", summary="Get exchange health")
async def get_exchange_health(
    connection_id: str,
    exchange_manager: ExchangeManager = Depends(get_exchange_manager)
):
    """Get health status for an exchange."""
    try:
        connection = exchange_manager.get_connection(connection_id)
        if not connection:
            raise HTTPException(status_code=404, detail=f"Exchange connection {connection_id} not found")
            
        if not connection.connector:
            return {
                "connection_id": connection_id,
                "healthy": False,
                "status": connection.status.value,
                "error_message": connection.error_message
            }
            
        # Check if connector is still working
        try:
            # Simple health check - try to get server time from exchange
            if hasattr(connection.connector, 'exchange'):
                server_time = await connection.connector.exchange.fetch_time()
                healthy = server_time is not None
            else:
                # Fallback: assume healthy if connected
                healthy = connection.connector.connected
        except:
            healthy = False
        
        return {
            "connection_id": connection_id,
            "healthy": healthy,
            "status": connection.status.value,
            "last_check": connection.last_check.isoformat() if connection.last_check else None
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting health for {connection_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 