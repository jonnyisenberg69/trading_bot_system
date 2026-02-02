"""
WebSocket manager for real-time trading updates.

Handles real-time streaming of risk monitoring data
to connected frontend clients.
"""

import asyncio
import json
from typing import Dict, List, Set, Tuple
from datetime import datetime, timezone, timedelta
import structlog
from fastapi import WebSocket, WebSocketDisconnect
import hashlib

from api.services.trade_sync_service import trigger_trade_sync
from api.services.risk_data_service import RiskDataService
from database.models import Exchange, InventoryPriceHistory, RealizedPNL
from sqlalchemy import select, delete

logger = structlog.get_logger(__name__)


class WebSocketManager:
    """Manages WebSocket connections for real-time trading updates."""
    
    def __init__(self, bot_manager=None, exchange_manager=None, position_manager=None, session_maker=None, risk_data_service=None):
        """Initialize WebSocket manager."""
        self.active_connections: Set[WebSocket] = set()
        self.bot_manager = bot_manager
        self.exchange_manager = exchange_manager
        self.position_manager = position_manager
        self.session_maker = session_maker
        self.logger = logger.bind(component="WebSocketManager")
        self.update_interval = 5  # seconds
        self._running = False
        self._update_task = None
        # Map websocket to (start_time, end_time)
        self.connection_time_ranges: Dict[WebSocket, Tuple[datetime, datetime]] = {}
        # Track sync state per connection
        self.connection_sync_state: Dict[WebSocket, bool] = {}
        # Ensure we have a usable RiskDataService with get_summary_data
        from api.services.risk_data_service import RiskDataService as ApiRiskDataService

        if risk_data_service is None or not hasattr(risk_data_service, "get_summary_data"):
            if session_maker is None:
                raise ValueError("session_maker must be provided to create RiskDataService")
            self.logger.warning(
                "Injected risk_data_service missing or invalid; creating ApiRiskDataService as fallback",
                provided_type=str(type(risk_data_service))
            )
            self.risk_data_service = ApiRiskDataService(session_maker)
        else:
            # If the provided service is already correct, use it
            # but log its type for debugging
            self.logger.info(
                "Using injected risk_data_service",
                service_type=str(type(risk_data_service))
            )
            self.risk_data_service = risk_data_service
        # Last sent data per connection to avoid redundant updates
        self.last_sent_data: Dict[WebSocket, Dict] = {}
        # Cache of data hashes to avoid duplicate updates
        self.data_hashes: Dict[WebSocket, Dict[str, str]] = {}
        # Debounce timers for each connection
        self.debounce_timers: Dict[WebSocket, asyncio.Task] = {}
        # Rate limit for balance updates (in seconds) - increased to reduce flashing
        self.balance_update_interval = 30  # Increased from 15 to 30 seconds
        self.last_balance_update_time: Dict[WebSocket, float] = {}
        # Timestamp of last update per connection
        self.last_update_time: Dict[WebSocket, float] = {}
        # Minimum time between updates (in seconds) - increased to reduce frequency
        self.min_update_interval = 3.0  # Increased from 2.0 to 3.0 seconds
        # Cache for detailed positions to avoid duplication
        self.position_cache: Dict[WebSocket, Dict[str, Dict]] = {}
        
    async def connect(self, websocket: WebSocket):
        """Accept a new WebSocket connection."""
        await websocket.accept()
        self.active_connections.add(websocket)
        # Default: last 24h (timezone-aware UTC)
        now = datetime.now(timezone.utc)
        self.connection_time_ranges[websocket] = (now - timedelta(days=1), now)
        self.connection_sync_state[websocket] = False
        self.last_sent_data[websocket] = {}
        self.data_hashes[websocket] = {}
        self.last_balance_update_time[websocket] = 0
        self.last_update_time[websocket] = 0
        self.position_cache[websocket] = {}
        self.logger.info(f"New WebSocket connection. Total: {len(self.active_connections)}")
        
        # Send initial data
        await self.send_initial_data(websocket)
        
    def disconnect(self, websocket: WebSocket):
        """Remove a WebSocket connection."""
        self.active_connections.discard(websocket)
        self.connection_time_ranges.pop(websocket, None)
        self.connection_sync_state.pop(websocket, None)
        self.last_sent_data.pop(websocket, None)
        self.data_hashes.pop(websocket, None)
        self.last_balance_update_time.pop(websocket, None)
        self.last_update_time.pop(websocket, None)
        self.position_cache.pop(websocket, None)
        
        # Cancel any pending debounce timers
        if websocket in self.debounce_timers:
            timer = self.debounce_timers.pop(websocket)
            if timer and not timer.done():
                timer.cancel()
        
        self.logger.info(f"WebSocket disconnected. Total: {len(self.active_connections)}")
        
    def _compute_data_hash(self, data: Dict, section: str) -> str:
        """Compute a hash of the data to detect changes."""
        if section == "detailed_positions":
            # For positions, create a sorted string of position keys
            position_keys = []
            for pos in data:
                # Create a unique key for each position
                key = f"{pos.get('exchange')}_{pos.get('symbol')}_{pos.get('size')}_{pos.get('avg_price')}"
                position_keys.append(key)
            position_keys.sort()
            return hashlib.md5(json.dumps(position_keys).encode()).hexdigest()
        elif section == "balances":
            # Special handling for balances - only use significant digits to avoid small fluctuations
            balance_keys = []
            for balance in data:
                if isinstance(balance, dict) and 'total' in balance:
                    # Round balance to 4 significant digits to avoid small fluctuations
                    total = round(float(balance.get('total', 0)), 4)
                    if total > 0.0001:  # Only include non-dust balances
                        key = f"{balance.get('exchange_id')}_{balance.get('currency')}_{total}"
                        balance_keys.append(key)
            balance_keys.sort()
            return hashlib.md5(json.dumps(balance_keys).encode()).hexdigest()
        else:
            # For other data, use the JSON string
            return hashlib.md5(json.dumps(data, sort_keys=True, default=str).encode()).hexdigest()
        
    def _data_changed(self, ws: WebSocket, new_data: Dict, section: str) -> bool:
        """Check if data has changed compared to last sent data."""
        new_hash = self._compute_data_hash(new_data, section)
        old_hash = self.data_hashes.get(ws, {}).get(section)
        
        if old_hash != new_hash:
            # Update the hash
            if ws not in self.data_hashes:
                self.data_hashes[ws] = {}
            self.data_hashes[ws][section] = new_hash
            return True
        return False
    
    def _deduplicate_positions(self, positions: List[Dict]) -> List[Dict]:
        """Deduplicate positions by combining those with the same exchange and symbol."""
        position_map = {}
        
        for pos in positions:
            key = f"{pos.get('exchange')}_{pos.get('symbol')}"
            if key not in position_map:
                position_map[key] = pos
            # If duplicate, keep the most recent one
            elif pos.get('timestamp', '') > position_map[key].get('timestamp', ''):
                position_map[key] = pos
                
        return list(position_map.values())
        
    def _ensure_naive_datetime(self, dt):
        """Convert timezone-aware datetime to naive UTC datetime for database operations."""
        if dt is None:
            return None
        if dt.tzinfo is not None:
            return dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt

    async def send_initial_data(self, websocket: WebSocket):
        """Send initial position and risk data to a new connection."""
        try:
            if not self.risk_data_service:
                self.logger.error("Risk data service not available")
                return
            
            # Get time range for this connection
            start_time, end_time = self.connection_time_ranges.get(websocket, (None, None))
            
            # Ensure naive datetimes for database operations
            naive_start_time = self._ensure_naive_datetime(start_time)
            naive_end_time = self._ensure_naive_datetime(end_time)
            
            self.logger.info(f"Retrieving initial data: time range {naive_start_time} to {naive_end_time}")
            
            # Get data from risk monitoring tables with use_cache=False to get fresh data
            summary = await self.risk_data_service.get_summary_data(start_time=naive_start_time, end_time=naive_end_time)
            net_positions = await self.risk_data_service.get_net_positions(start_time=naive_start_time, end_time=naive_end_time, use_cache=False)
            detailed_positions = await self.risk_data_service.get_current_positions(start_time=naive_start_time, end_time=naive_end_time, use_cache=False)
            
            self.logger.info(f"Retrieved {len(detailed_positions)} detailed positions for initial data")
            
            # Deduplicate positions
            detailed_positions = self._deduplicate_positions(detailed_positions)
            
            # Cache positions
            self.position_cache[websocket] = {
                f"{pos.get('exchange')}_{pos.get('symbol')}": pos
                for pos in detailed_positions
            }
            
            self.logger.info(f"Position cache initialized with {len(self.position_cache[websocket])} positions")
            
            # Get realized PNL data
            realized_pnl_data = await self.risk_data_service.get_realized_pnl(start_time=naive_start_time, end_time=naive_end_time)

            initial_data = {
                "type": "initial_data",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "data": {
                    "summary": summary,
                    "net_positions": net_positions,
                    "detailed_positions": detailed_positions,
                    "risk_metrics": self.calculate_risk_metrics(summary, net_positions),
                    "realized_pnl": realized_pnl_data
                }
            }
            
            # Store as last sent data to avoid redundant updates
            self.last_sent_data[websocket] = initial_data["data"]
            
            # Initialize data hashes
            self.data_hashes[websocket] = {
                "summary": self._compute_data_hash(summary, "summary"),
                "net_positions": self._compute_data_hash(net_positions, "net_positions"),
                "detailed_positions": self._compute_data_hash(detailed_positions, "detailed_positions"),
                "realized_pnl": self._compute_data_hash(realized_pnl_data, "realized_pnl"),
                "balances": self._compute_data_hash(summary.get("balances", []), "balances")
            }
            
            # Record update time
            self.last_update_time[websocket] = asyncio.get_event_loop().time()
            
            self.logger.info(f"Sending initial data with {len(detailed_positions)} positions")
            await websocket.send_text(json.dumps(initial_data, default=str))
        except Exception as e:
            self.logger.error(f"Error sending initial data: {e}", exc_info=True)
            
    def calculate_risk_metrics(self, summary: Dict, net_positions: Dict) -> Dict:
        """Calculate risk metrics for WebSocket updates."""
        
        # Safely convert values to handle Decimal/float mixing
        long_value = float(summary.get("long_value", 0))
        short_value = float(summary.get("short_value", 0))
        net_exposure = float(summary.get("net_exposure", 0))
        
        return {
            "total_exposure": long_value + short_value,
            "net_exposure": net_exposure,
            "long_short_ratio": long_value / max(short_value, 1.0),  # Use 1.0 instead of 1
            "portfolio_diversification": len(net_positions),
            "risk_score": self.calculate_risk_score(summary, net_positions),
            "exchange_distribution": summary.get("positions_by_exchange", {}),
            "total_realized_pnl": summary.get("total_realized_pnl", 0),
            "balances": summary.get("balances", {}),
            "last_update": datetime.now(timezone.utc).isoformat()
        }
        
    def calculate_risk_score(self, summary: Dict, net_positions: Dict) -> int:
        """Calculate overall risk score."""
        score = 0
        
        # Exposure risk (convert to float for calculations)
        net_exposure = abs(float(summary.get("net_exposure", 0)))
        if net_exposure > 10000:
            score += 30
        elif net_exposure > 5000:
            score += 20
        else:
            score += 10
            
        # Concentration risk
        if net_positions:
            position_values = [abs(float(pos.get("value", 0))) for pos in net_positions.values()]
            max_position = max(position_values) if position_values else 0
            
            long_value = float(summary.get("long_value", 0))
            short_value = float(summary.get("short_value", 0))
            total_exposure = long_value + short_value
            
            concentration_ratio = max_position / max(total_exposure, 1.0)
            
            if concentration_ratio > 0.5:
                score += 40
            elif concentration_ratio > 0.3:
                score += 25
            else:
                score += 10
                
        # Exchange diversification
        exchange_count = len(summary.get("positions_by_exchange", {}))
        if exchange_count < 3:
            score += 30
        elif exchange_count < 5:
            score += 20
        else:
            score += 10
            
        return min(score, 100)
        
    async def debounced_broadcast(self, data: Dict, websocket: WebSocket):
        """Debounce updates to avoid excessive broadcasting."""
        # Cancel any existing timer for this websocket
        if websocket in self.debounce_timers:
            timer = self.debounce_timers[websocket]
            if not timer.done():
                timer.cancel()
        
        # Create a new timer with increased delay (2000ms instead of 500ms)
        self.debounce_timers[websocket] = asyncio.create_task(
            self._delayed_broadcast(data, websocket, delay=2.0)  # 2 second delay
        )
    
    async def _delayed_broadcast(self, data: Dict, websocket: WebSocket, delay: float):
        """Helper to perform a delayed broadcast."""
        await asyncio.sleep(delay)
        await self._perform_broadcast(data, websocket)
        if websocket in self.debounce_timers:
            self.debounce_timers.pop(websocket)
            
    async def _perform_broadcast(self, data: Dict, websocket: WebSocket = None):
        """Actually perform the broadcast after debouncing."""
        if not self.risk_data_service:
            self.logger.error("Risk data service not available")
            return
            
        try:
            # If websocket is provided, send only to that one, else broadcast to all
            targets = [websocket] if websocket else list(self.active_connections)
            current_time = asyncio.get_event_loop().time()
            
            for ws in targets:
                try:
                    # Check if enough time has passed since last update
                    last_update = self.last_update_time.get(ws, 0)
                    if current_time - last_update < self.min_update_interval:
                        # Skip this update if too soon after the last one
                        continue
                    
                    # Get time range for this connection
                    start_time, end_time = self.connection_time_ranges.get(ws, (None, None))
                    
                    # Ensure naive datetimes for database operations
                    naive_start_time = self._ensure_naive_datetime(start_time)
                    naive_end_time = self._ensure_naive_datetime(end_time)
                    
                    # Get updated data from risk monitoring tables
                    summary = await self.risk_data_service.get_summary_data(start_time=naive_start_time, end_time=naive_end_time)
                    net_positions = await self.risk_data_service.get_net_positions(start_time=naive_start_time, end_time=naive_end_time)
                    detailed_positions = await self.risk_data_service.get_current_positions(start_time=naive_start_time, end_time=naive_end_time)
                    
                    # Log positions count for debugging
                    self.logger.info(f"Got {len(detailed_positions)} detailed positions from database")
                    
                    # Deduplicate positions
                    detailed_positions = self._deduplicate_positions(detailed_positions)
                    
                    # Update position cache with new positions
                    position_cache = self.position_cache.get(ws, {})
                    for pos in detailed_positions:
                        key = f"{pos.get('exchange')}_{pos.get('symbol')}"
                        position_cache[key] = pos
                    
                    # Use positions from cache to ensure consistency
                    cached_positions = list(position_cache.values())
                    
                    # Only update the cache with new positions if we got some
                    if len(detailed_positions) > 0:
                        self.position_cache[ws] = position_cache
                        # Use the newest positions
                        detailed_positions = cached_positions
                    elif len(cached_positions) > 0:
                        # If no positions from database but we have cache, use the cache
                        detailed_positions = cached_positions
                        self.logger.info(f"Using {len(cached_positions)} cached positions instead of empty database result")
                    
                    realized_pnl_data = await self.risk_data_service.get_realized_pnl(start_time=naive_start_time, end_time=naive_end_time)
                    
                    # Prepare the new data
                    new_data = {
                        "summary": summary,
                        "net_positions": net_positions,
                        "detailed_positions": detailed_positions,
                        "risk_metrics": self.calculate_risk_metrics(summary, net_positions),
                        "realized_pnl": realized_pnl_data
                    }
                    
                    # Determine if we should update based on changes
                    should_update = False
                    
                    # Check if net positions changed
                    if self._data_changed(ws, new_data["net_positions"], "net_positions"):
                        should_update = True
                        self.logger.info("Broadcasting update: net positions changed")
                    
                    # Check if detailed positions changed and there are positions to send
                    if len(detailed_positions) > 0 and self._data_changed(ws, new_data["detailed_positions"], "detailed_positions"):
                        should_update = True
                        self.logger.info("Broadcasting update: detailed positions changed")
                        
                    # Check if realized PNL changed
                    if self._data_changed(ws, new_data["realized_pnl"], "realized_pnl"):
                        should_update = True
                        self.logger.info("Broadcasting update: realized PNL changed")
                        
                    # Special handling for balances - only update at most once every balance_update_interval seconds
                    last_balance_update = self.last_balance_update_time.get(ws, 0)
                    time_since_balance = current_time - last_balance_update
                    
                    if time_since_balance >= self.balance_update_interval:
                        balance_data = summary.get("balances", [])
                        if balance_data and self._data_changed(ws, balance_data, "balances"):
                            should_update = True
                            self.last_balance_update_time[ws] = current_time
                            self.logger.info("Broadcasting update: balances changed")
                    
                    # Skip update if nothing significant changed
                    if not should_update:
                        continue
                    
                    # Store this data as the last sent
                    self.last_sent_data[ws] = new_data
                    
                    # Update the last update time
                    self.last_update_time[ws] = current_time
                    
                    # Send the update
                    message = {
                        "type": "position_update",
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "data": new_data
                    }
                    await ws.send_text(json.dumps(message, default=str))
                except Exception as e:
                    self.logger.error(f"Error broadcasting to WebSocket: {e}")
                    self.disconnect(ws)
        except Exception as e:
            self.logger.error(f"Error in broadcast update: {e}", exc_info=True)
        
    async def broadcast_update(self, data: Dict, websocket: WebSocket = None):
        """Broadcast update to all connected clients with debouncing."""
        # Use debounced broadcast for the given websocket
        if websocket:
            await self.debounced_broadcast(data, websocket)
        else:
            # For all connections broadcast, directly use the perform broadcast
            # as we don't need to debounce a scheduled update
            await self._perform_broadcast(data)
        
    async def start_periodic_updates(self):
        """Start periodic position updates."""
        if self._running:
            return
        self._running = True
        self.logger.info(f"Starting periodic WebSocket updates every {self.update_interval} seconds")
        while self._running:
            try:
                if self.active_connections:
                    # Broadcast updates to all connections that are not syncing
                    for ws in list(self.active_connections):
                        if not self.connection_sync_state.get(ws, False):
                            await self.broadcast_update({}, websocket=ws)
                await asyncio.sleep(self.update_interval)
            except Exception as e:
                self.logger.error(f"Error in periodic updates: {e}")
                await asyncio.sleep(5)  # Wait before retrying
                
    async def stop_periodic_updates(self):
        """Stop periodic updates."""
        self._running = False
        if self._update_task:
            self._update_task.cancel()
            try:
                await self._update_task
            except asyncio.CancelledError:
                pass
        self.logger.info("Stopped periodic WebSocket updates")
        
    async def handle_client_message(self, websocket: WebSocket, message: str):
        try:
            msg = json.loads(message)
            if msg.get("action") == "set_time_range":
                start_time_str = msg.get("start_time")
                end_time_str = msg.get("end_time")

                if not start_time_str or not end_time_str:
                    self.logger.error("set_time_range requires start_time and end_time")
                    await websocket.send_text(json.dumps({
                        "type": "error",
                        "error": "Missing start_time or end_time"
                    }))
                    return

                # Send immediate acknowledgment that sync is starting
                await websocket.send_text(json.dumps({
                    "type": "sync_started",
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }))
                
                # Mark this connection as syncing
                self.connection_sync_state[websocket] = True
                
                # Clear position cache for this connection
                self.position_cache[websocket] = {}
                
                # Parse datetime strings first
                try:
                    # Parse the datetime strings and convert to naive UTC for database
                    start_dt = datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))
                    end_dt = datetime.fromisoformat(end_time_str.replace('Z', '+00:00'))
                    
                    # Ensure both datetimes are timezone-aware for storage
                    if start_dt.tzinfo is None:
                        start_dt = start_dt.replace(tzinfo=timezone.utc)
                    if end_dt.tzinfo is None:
                        end_dt = end_dt.replace(tzinfo=timezone.utc)
                    
                    # Convert to naive UTC for database operations
                    start_dt_naive = self._ensure_naive_datetime(start_dt)
                    end_dt_naive = self._ensure_naive_datetime(end_dt)
                    
                except Exception as e:
                    self.logger.error(f"Invalid time format: {e}")
                    await websocket.send_text(json.dumps({
                        "type": "error",
                        "error": "Invalid time format"
                    }))
                    self.connection_sync_state[websocket] = False
                    return
                
                # Clear old position data from risk monitoring tables before sync
                try:
                    from database.repositories.inventory_price_history_repository import InventoryPriceHistoryRepository
                    from database.repositories.realized_pnl_repository import RealizedPNLRepository
                    
                    async with self.session_maker() as session:
                        # Get exchange IDs to clear data for
                        if self.exchange_manager:
                            exchange_ids = []
                            connections = self.exchange_manager.get_all_connections()
                            for conn in connections:
                                if conn.has_credentials and conn.status.value == "connected":
                                    result = await session.execute(
                                        select(Exchange.id).where(Exchange.name == conn.connection_id)
                                    )
                                    exchange_id = result.scalar_one_or_none()
                                    if exchange_id:
                                        exchange_ids.append(exchange_id)
                            
                            if exchange_ids:
                                # Clear inventory price history for the time range
                                await session.execute(
                                    delete(InventoryPriceHistory).where(
                                        InventoryPriceHistory.exchange_id.in_(exchange_ids),
                                        InventoryPriceHistory.timestamp >= start_dt_naive,
                                        InventoryPriceHistory.timestamp <= end_dt_naive
                                    )
                                )
                                
                                # Clear realized PNL for the time range
                                await session.execute(
                                    delete(RealizedPNL).where(
                                        RealizedPNL.exchange_id.in_(exchange_ids),
                                        RealizedPNL.timestamp >= start_dt_naive,
                                        RealizedPNL.timestamp <= end_dt_naive
                                    )
                                )
                                
                                await session.commit()
                                self.logger.info("Cleared old risk monitoring data before sync")
                                
                except Exception as e:
                    self.logger.error(f"Failed to clear old risk monitoring data: {e}")

                try:
                    # Trigger the trade sync
                    self.logger.info(f"Triggering trade sync for time range: {start_time_str} to {end_time_str}")
                    await trigger_trade_sync(
                        account_name="Main Account",
                        start_time=start_time_str,
                        end_time=end_time_str,
                        bot_manager=self.bot_manager,
                        exchange_manager=self.exchange_manager,
                        position_manager=self.position_manager
                    )
                    
                    # Log the sync completion
                    self.logger.info(f"Trade sync completed, risk tables should be populated")
                    
                    # Add a small delay to ensure database writes are complete
                    await asyncio.sleep(0.5)
                    
                    # Update the connection's time range
                    # We use the timezone-aware versions for storage
                    self.connection_time_ranges[websocket] = (start_dt, end_dt)
                    
                    # Clear sync state
                    self.connection_sync_state[websocket] = False
                    
                    # Reset last sent data to force a fresh update
                    self.last_sent_data[websocket] = {}
                    self.data_hashes[websocket] = {}
                    
                    # Send sync complete message
                    await websocket.send_text(json.dumps({
                        "type": "sync_complete",
                        "timestamp": datetime.now(timezone.utc).isoformat()
                    }))
                    
                    # After syncing, send the updated initial data
                    await self.send_initial_data(websocket)
                    
                except Exception as sync_error:
                    self.logger.error(f"Error during trade sync: {sync_error}", exc_info=True)
                    # Clear sync state on error
                    self.connection_sync_state[websocket] = False
                    await websocket.send_text(json.dumps({
                        "type": "error",
                        "error": f"Sync failed: {str(sync_error)}"
                    }))
                    
        except Exception as e:
            self.logger.error(f"Error handling client message: {e}", exc_info=True)
            try:
                await websocket.send_text(json.dumps({
                    "type": "error",
                    "error": f"Message handling error: {str(e)}"
                }))
            except:
                pass


# Global WebSocket manager instance
_websocket_manager: WebSocketManager = None


def get_websocket_manager() -> WebSocketManager:
    """Get the global WebSocket manager instance."""
    global _websocket_manager
    if _websocket_manager is None:
        raise RuntimeError("WebSocket manager not initialized")
    return _websocket_manager


def initialize_websocket_manager(bot_manager=None, exchange_manager=None, position_manager=None, session_maker=None, risk_data_service=None) -> WebSocketManager:
    """Initialize the global WebSocket manager."""
    global _websocket_manager
    _websocket_manager = WebSocketManager(
        bot_manager=bot_manager,
        exchange_manager=exchange_manager,
        position_manager=position_manager,
        session_maker=session_maker,
        risk_data_service=risk_data_service
    )
    return _websocket_manager 
