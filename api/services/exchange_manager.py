"""
Exchange Manager service for managing exchange connections and status.

Handles testing exchange connections, API key validation, and monitoring
exchange health similar to Hummingbot's approach.
"""

import asyncio
import json
import os
from typing import Dict, Any, List, Optional, Set
from datetime import datetime, timedelta
from pathlib import Path
import structlog
from enum import Enum

# Add project root to path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import ccxt
from exchanges.connectors import create_exchange_connector, list_supported_exchanges
from config.settings import load_config, DEFAULT_CONFIG_PATH

logger = structlog.get_logger(__name__)


class ConnectionStatus(str, Enum):
    """Exchange connection status."""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    ERROR = "error"
    INVALID_CREDENTIALS = "invalid_credentials"
    NO_CREDENTIALS = "no_credentials"


class ExchangeConnection:
    """Represents an exchange connection."""
    
    def __init__(
        self,
        name: str,
        exchange_type: str,
        api_key: str = "",
        api_secret: str = "",
        wallet_address: str = "",
        private_key: str = "",
        passphrase: str = "",
        testnet: bool = True
    ):
        self.name = name
        self.exchange_type = exchange_type  # spot, perp, etc.
        self.api_key = api_key
        self.api_secret = api_secret
        self.wallet_address = wallet_address  # For Hyperliquid
        self.private_key = private_key  # For Hyperliquid
        self.passphrase = passphrase  # For Bitget
        self.testnet = testnet
        self.status = ConnectionStatus.DISCONNECTED
        self.connector = None
        self.last_check: Optional[datetime] = None
        self.error_message: Optional[str] = None
        self.market_count: int = 0
        self.available_symbols: List[str] = []
        
    @property
    def has_credentials(self) -> bool:
        """Check if connection has API credentials."""
        # For Hyperliquid, we need wallet address and private key
        if self.name.lower() == 'hyperliquid':
            return bool(self.wallet_address and self.private_key)
        # For standard exchanges, we need API key and secret
        return bool(self.api_key and self.api_secret)
        
    @property
    def connection_id(self) -> str:
        """Get unique connection identifier."""
        return f"{self.name}_{self.exchange_type}"
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "name": self.name,
            "exchange_type": self.exchange_type,
            "connection_id": self.connection_id,
            "status": self.status.value,
            "has_credentials": self.has_credentials,
            "testnet": self.testnet,
            "last_check": self.last_check.isoformat() if self.last_check else None,
            "error_message": self.error_message,
            "market_count": self.market_count,
            "available_symbols_count": len(self.available_symbols),
            "sample_symbols": self.available_symbols[:10] if self.available_symbols else []
        }


class ExchangeManager:
    """
    Manages exchange connections and status monitoring.
    
    Similar to Hummingbot's approach, this manager:
    - Tests exchange connectivity
    - Validates API credentials
    - Monitors exchange health
    - Provides connection status to the frontend
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.connections: Dict[str, ExchangeConnection] = {}
        self.running = False
        self.monitor_task: Optional[asyncio.Task] = None
        self.logger = logger.bind(component="ExchangeManager")
        
        # Initialize connections from config
        self._initialize_connections()
        
    def _initialize_connections(self) -> None:
        """Initialize exchange connections from configuration."""
        for exchange_config in self.config.get('exchanges', []):
            connection = ExchangeConnection(
                name=exchange_config['name'],
                exchange_type=exchange_config.get('type', 'spot'),
                api_key=exchange_config.get('api_key', ''),
                api_secret=exchange_config.get('api_secret', ''),
                wallet_address=exchange_config.get('wallet_address', ''),
                private_key=exchange_config.get('private_key', ''),
                passphrase=exchange_config.get('passphrase', ''),
                testnet=exchange_config.get('testnet', True)
            )
            
            self.connections[connection.connection_id] = connection
            
        self.logger.info(f"Initialized {len(self.connections)} exchange connections")
        
    async def start(self) -> None:
        """Start the exchange manager."""
        if self.running:
            return
            
        self.running = True
        self.logger.info("Starting exchange manager")
        
        # Initial connection test for all exchanges
        await self._test_all_connections()
        
        # Start monitoring task
        self.monitor_task = asyncio.create_task(self._monitor_connections())
        
        self.logger.info("Exchange manager started")
        
    async def stop(self) -> None:
        """Stop the exchange manager."""
        if not self.running:
            return
            
        self.running = False
        self.logger.info("Stopping exchange manager")
        
        # Stop monitoring task
        if self.monitor_task:
            self.monitor_task.cancel()
            try:
                await self.monitor_task
            except asyncio.CancelledError:
                pass
                
        # Close all connectors
        for connection in self.connections.values():
            if connection.connector:
                try:
                    # Use our connector's disconnect method instead of raw CCXT close
                    await connection.connector.disconnect()
                except Exception as e:
                    self.logger.error(f"Error closing connector for {connection.connection_id}: {e}")
                    
        self.logger.info("Exchange manager stopped")
        
    async def test_connection(self, connection_id: str) -> bool:
        """
        Test a specific exchange connection.
        
        Args:
            connection_id: Connection ID to test
            
        Returns:
            True if connection successful
        """
        if connection_id not in self.connections:
            self.logger.error(f"Connection not found: {connection_id}")
            return False
            
        connection = self.connections[connection_id]
        return await self._test_connection(connection)
        
    async def update_credentials(
        self,
        connection_id: str,
        api_key: str = "",
        api_secret: str = "",
        wallet_address: str = "",
        private_key: str = "",
        passphrase: str = "",
        testnet: bool = True
    ) -> bool:
        """
        Update credentials for an exchange connection.
        
        Args:
            connection_id: Connection ID
            api_key: API key
            api_secret: API secret
            wallet_address: Wallet address (for Hyperliquid)
            private_key: Private key (for Hyperliquid)
            passphrase: Passphrase (for exchanges like Bitget)
            testnet: Use testnet
            
        Returns:
            True if update successful
        """
        if connection_id not in self.connections:
            self.logger.error(f"Connection not found: {connection_id}")
            return False
            
        connection = self.connections[connection_id]
        
        # Close existing connector
        if connection.connector:
            try:
                await connection.connector.disconnect()
            except Exception as e:
                self.logger.error(f"Error closing existing connector: {e}")
                
        # Update credentials based on exchange type
        if connection.name.lower() == 'hyperliquid':
            # For Hyperliquid, use wallet address and private key
            connection.wallet_address = wallet_address
            connection.private_key = private_key
            # Also set api_key/secret for compatibility
            connection.api_key = wallet_address
            connection.api_secret = private_key
        else:
            # For standard exchanges
            connection.api_key = api_key
            connection.api_secret = api_secret
            
        # Set passphrase for exchanges that need it
        connection.passphrase = passphrase
        connection.testnet = testnet
        connection.connector = None
        
        # Test new connection
        success = await self._test_connection(connection)
        
        if success:
            self.logger.info(f"Updated credentials for {connection_id}")
            # Persist credentials to config file for strategy runners
            await self._persist_credentials()
        else:
            self.logger.error(f"Failed to connect with new credentials for {connection_id}")
            
        return success
        
    async def delete_credentials(self, connection_id: str) -> bool:
        """
        Delete credentials for an exchange connection.
        
        Args:
            connection_id: Connection ID
            
        Returns:
            True if deletion successful
        """
        if connection_id not in self.connections:
            self.logger.error(f"Connection not found: {connection_id}")
            return False
            
        connection = self.connections[connection_id]
        
        # Close existing connector
        if connection.connector:
            try:
                await connection.connector.disconnect()
            except Exception as e:
                self.logger.error(f"Error closing existing connector: {e}")
                
        # Clear credentials
        connection.api_key = ""
        connection.api_secret = ""
        connection.connector = None
        connection.status = ConnectionStatus.NO_CREDENTIALS
        connection.error_message = "No API credentials provided"
        connection.last_check = datetime.now()
        connection.market_count = 0
        connection.available_symbols = []
        
        self.logger.info(f"Deleted credentials for {connection_id}")
        return True
        
    def get_connection(self, connection_id: str) -> Optional[ExchangeConnection]:
        """Get exchange connection by ID."""
        return self.connections.get(connection_id)
        
    def get_all_connections(self) -> List[ExchangeConnection]:
        """Get all exchange connections."""
        return list(self.connections.values())
        
    def get_connected_exchanges(self) -> List[ExchangeConnection]:
        """Get all connected exchanges."""
        return [
            conn for conn in self.connections.values()
            if conn.status == ConnectionStatus.CONNECTED
        ]
        
    def get_exchange_connectors(self, connection_ids: List[str] = None) -> Dict[str, Any]:
        """
        Get exchange connectors for specified connection IDs.
        
        Args:
            connection_ids: List of connection IDs to get connectors for.
                          If None, returns all connected connectors.
                          
        Returns:
            Dictionary mapping connection_id to connector instance
        """
        connectors = {}
        
        if connection_ids is None:
            # Return all connected connectors
            for connection in self.connections.values():
                if (connection.status == ConnectionStatus.CONNECTED and 
                    connection.connector is not None):
                    connectors[connection.connection_id] = connection.connector
        else:
            # Return connectors for specified connection IDs
            for connection_id in connection_ids:
                connection = self.connections.get(connection_id)
                if (connection and 
                    connection.status == ConnectionStatus.CONNECTED and 
                    connection.connector is not None):
                    connectors[connection_id] = connection.connector
                else:
                    self.logger.warning(f"Connector not available for {connection_id}")
                    
        return connectors
        
    def get_supported_exchanges(self) -> Dict[str, Dict[str, bool]]:
        """Get list of supported exchanges and their capabilities."""
        return list_supported_exchanges()
        
    async def get_connector(self, exchange_name: str) -> Optional[Any]:
        """
        Get connector for a specific exchange by name.
        
        Args:
            exchange_name: Name of the exchange (e.g., 'binance', 'bybit')
            
        Returns:
            Exchange connector instance if connected, None otherwise
        """
        # Find connection by exchange name (handle both name and connection_id formats)
        for connection in self.connections.values():
            if (connection.name.lower() == exchange_name.lower() and 
                connection.status == ConnectionStatus.CONNECTED and 
                connection.connector is not None):
                return connection.connector
        return None
    
    async def get_connected_exchange_names(self) -> List[str]:
        """
        Get list of connected exchange names.
        
        Returns:
            List of exchange names that are currently connected
        """
        return [
            conn.name for conn in self.connections.values()
            if conn.status == ConnectionStatus.CONNECTED
        ]
    
    async def get_all_exchanges(self) -> List[str]:
        """
        Get list of all configured exchange names.
        
        Returns:
            List of all configured exchange names
        """
        return [conn.name for conn in self.connections.values()]
        
    async def _test_connection(self, connection: ExchangeConnection) -> bool:
        """Test a specific exchange connection."""
        self.logger.info(f"Testing connection: {connection.connection_id}")
        
        connection.status = ConnectionStatus.CONNECTING
        connection.error_message = None
        
        try:
            # Check if we have credentials
            if not connection.has_credentials:
                connection.status = ConnectionStatus.NO_CREDENTIALS
                connection.error_message = "No API credentials provided"
                self.logger.warning(f"No credentials for {connection.connection_id}")
                return False
                
            # FIXED: Use our custom connectors instead of raw CCXT instances
            # This ensures market_type configuration is properly handled
            connector = create_exchange_connector(connection.name, {
                'api_key': connection.api_key,
                'secret': connection.api_secret,
                'wallet_address': connection.wallet_address,
                'private_key': connection.private_key,
                'passphrase': connection.passphrase,
                'sandbox': connection.testnet,
                'market_type': 'future' if connection.exchange_type == 'perp' else connection.exchange_type,
                'testnet': connection.testnet
            })
            
            if not connector:
                connection.status = ConnectionStatus.ERROR
                connection.error_message = f"Failed to create connector for {connection.name}"
                return False
            
            # Test connection using our connector
            try:
                success = await connector.connect()
                if not success:
                    connection.status = ConnectionStatus.ERROR
                    connection.error_message = "Failed to connect to exchange"
                    return False
            except Exception as connect_error:
                connection.status = ConnectionStatus.ERROR
                connection.error_message = f"Connection failed: {str(connect_error)}"
                self.logger.error(f"Connection failed for {connection.connection_id}: {connect_error}")
                return False
            
            # Test private API by checking balance (requires valid authentication)
            try:
                balance = await connector.get_balance()
                
                # If we get here, the private API works - mark as connected
                connection.status = ConnectionStatus.CONNECTED
                connection.market_count = len(connector.exchange.markets) if hasattr(connector, 'exchange') and hasattr(connector.exchange, 'markets') else 0
                connection.available_symbols = list(connector.exchange.markets.keys())[:100] if hasattr(connector, 'exchange') and hasattr(connector.exchange, 'markets') else []
                connection.last_check = datetime.now()
                connection.error_message = None
                
                self.logger.info(f"Successfully connected to {connection.connection_id}: {connection.market_count} markets, private API working")
                
                # Store the connector instance (not raw CCXT)
                connection.connector = connector
                
                return True
                
            except Exception as balance_error:
                # ANY failure in private API means invalid credentials
                error_msg = str(balance_error).lower()
                
                # Check for various authentication error patterns
                auth_error_keywords = [
                    'api key', 'invalid key', 'invalid api', 'api-key format invalid',
                    'authentication', 'unauthorized', 'permission', 'forbidden',
                    'insufficient privileges', 'invalid signature', 'timestamp',
                    'invalid credentials', 'access denied', 'authentication failed',
                    'invalid nonce', 'invalid request', 'bad api key format'
                ]
                
                if any(keyword in error_msg for keyword in auth_error_keywords):
                    connection.status = ConnectionStatus.INVALID_CREDENTIALS
                    connection.error_message = "Invalid API credentials or insufficient permissions"
                    self.logger.error(f"Invalid credentials for {connection.connection_id}: {balance_error}")
                else:
                    # Other errors (network, server issues, etc.)
                    connection.status = ConnectionStatus.ERROR
                    connection.error_message = f"Private API test failed: {str(balance_error)}"
                    self.logger.error(f"Private API failed for {connection.connection_id}: {balance_error}")
                
                await connector.disconnect()
                return False
                
        except Exception as e:
            error_msg = str(e).lower()
            
            # Classify error types
            auth_error_keywords = [
                'api key', 'invalid key', 'invalid api', 'api-key format invalid',
                'authentication', 'unauthorized', 'permission', 'forbidden',
                'insufficient privileges', 'invalid signature', 'timestamp'
            ]
            
            if any(keyword in error_msg for keyword in auth_error_keywords):
                connection.status = ConnectionStatus.INVALID_CREDENTIALS
                connection.error_message = "Invalid API credentials"
            else:
                connection.status = ConnectionStatus.ERROR
                connection.error_message = str(e)
                
            self.logger.error(f"Connection test failed for {connection.connection_id}: {e}")
            
            # Clean up
            if hasattr(connection, 'connector') and connection.connector:
                try:
                    await connection.connector.disconnect()
                except:
                    pass
                connection.connector = None
                
            return False
            
        finally:
            connection.last_check = datetime.now()
    
    async def _test_all_connections(self) -> None:
        """Test all exchange connections."""
        self.logger.info("Testing all exchange connections")
        
        tasks = []
        for connection in self.connections.values():
            task = asyncio.create_task(self._test_connection(connection))
            tasks.append(task)
            
        # Wait for all tests to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        successful = sum(1 for result in results if result is True)
        self.logger.info(f"Connection test complete: {successful}/{len(tasks)} successful")
        
    async def _monitor_connections(self) -> None:
        """Monitor exchange connections for health."""
        while self.running:
            try:
                # Re-test connections every 5 minutes
                await asyncio.sleep(300)
                
                if self.running:
                    await self._test_all_connections()
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in connection monitoring: {e}")
                await asyncio.sleep(60)  # Wait before retrying
                
    async def get_exchange_status_summary(self) -> Dict[str, Any]:
        """Get summary of all exchange statuses."""
        connections = [conn.to_dict() for conn in self.connections.values()]
        
        # Group by status
        status_counts = {}
        for conn in connections:
            status = conn['status']
            status_counts[status] = status_counts.get(status, 0) + 1
            
        return {
            "total_exchanges": len(connections),
            "connected": status_counts.get(ConnectionStatus.CONNECTED.value, 0),
            "disconnected": status_counts.get(ConnectionStatus.DISCONNECTED.value, 0),
            "error": status_counts.get(ConnectionStatus.ERROR.value, 0),
            "invalid_credentials": status_counts.get(ConnectionStatus.INVALID_CREDENTIALS.value, 0),
            "no_credentials": status_counts.get(ConnectionStatus.NO_CREDENTIALS.value, 0),
            "connections": connections,
            "last_updated": datetime.now().isoformat()
        }

    async def _persist_credentials(self) -> None:
        """Persist current credentials to config file for strategy runners."""
        try:
            # Load current config
            config = load_config()
            
            # Update exchange configs with current credentials
            if 'exchanges' not in config:
                config['exchanges'] = []
            
            # Create a mapping of existing exchanges
            existing_exchanges = {
                f"{ex['name']}_{ex.get('type', 'spot')}": ex 
                for ex in config['exchanges']
            }
            
            # Update with current connection credentials
            for connection in self.connections.values():
                connection_key = connection.connection_id
                
                if connection_key in existing_exchanges:
                    # Update existing exchange config
                    exchange_config = existing_exchanges[connection_key]
                else:
                    # Create new exchange config
                    exchange_config = {
                        'name': connection.name,
                        'type': connection.exchange_type
                    }
                    config['exchanges'].append(exchange_config)
                
                # Update credentials
                exchange_config.update({
                    'api_key': connection.api_key,
                    'api_secret': connection.api_secret,
                    'wallet_address': connection.wallet_address,
                    'private_key': connection.private_key,
                    'passphrase': connection.passphrase,
                    'testnet': connection.testnet
                })
            
            # Save updated config
            with open(DEFAULT_CONFIG_PATH, 'w') as f:
                json.dump(config, f, indent=4)
                
            self.logger.info("Persisted credentials to config file")
            
        except Exception as e:
            self.logger.error(f"Failed to persist credentials: {e}") 