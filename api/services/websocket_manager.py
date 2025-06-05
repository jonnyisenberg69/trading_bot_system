"""
WebSocket manager for real-time trading updates.

Handles real-time streaming of position data, trade executions,
and risk metrics to connected frontend clients.
"""

import asyncio
import json
from typing import Dict, List, Set
from datetime import datetime
import structlog
from fastapi import WebSocket, WebSocketDisconnect

from order_management.tracking import PositionManager

logger = structlog.get_logger(__name__)


class WebSocketManager:
    """Manages WebSocket connections for real-time updates."""
    
    def __init__(self, position_manager: PositionManager):
        """Initialize WebSocket manager."""
        self.active_connections: Set[WebSocket] = set()
        self.position_manager = position_manager
        self.logger = logger.bind(component="WebSocketManager")
        self.update_interval = 15  # seconds
        self._running = False
        self._update_task = None
        
    async def connect(self, websocket: WebSocket):
        """Accept a new WebSocket connection."""
        await websocket.accept()
        self.active_connections.add(websocket)
        self.logger.info(f"New WebSocket connection. Total: {len(self.active_connections)}")
        
        # Send initial data
        await self.send_initial_data(websocket)
        
    def disconnect(self, websocket: WebSocket):
        """Remove a WebSocket connection."""
        self.active_connections.discard(websocket)
        self.logger.info(f"WebSocket disconnected. Total: {len(self.active_connections)}")
        
    async def send_initial_data(self, websocket: WebSocket):
        """Send initial position and risk data to a new connection."""
        try:
            # Reload latest position data
            await self.position_manager.load_positions()
            
            # Get current data
            summary = self.position_manager.summarize_positions()
            net_positions = self.position_manager.get_all_net_positions()
            
            initial_data = {
                "type": "initial_data",
                "timestamp": datetime.now().isoformat(),
                "data": {
                    "summary": summary,
                    "net_positions": net_positions,
                    "risk_metrics": self.calculate_risk_metrics(summary, net_positions)
                }
            }
            
            await websocket.send_text(json.dumps(initial_data, default=str))
            
        except Exception as e:
            self.logger.error(f"Error sending initial data: {e}")
            
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
            "last_update": datetime.now().isoformat()
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
        
    async def broadcast_update(self, data: Dict):
        """Broadcast update to all connected clients."""
        if not self.active_connections:
            return
            
        message = {
            "type": "position_update",
            "timestamp": datetime.now().isoformat(),
            "data": data
        }
        
        message_text = json.dumps(message, default=str)
        
        # Send to all connections
        disconnected = set()
        for connection in self.active_connections:
            try:
                await connection.send_text(message_text)
            except WebSocketDisconnect:
                disconnected.add(connection)
            except Exception as e:
                self.logger.error(f"Error broadcasting to WebSocket: {e}")
                disconnected.add(connection)
                
        # Remove disconnected connections
        for connection in disconnected:
            self.disconnect(connection)
            
    async def broadcast_trade_update(self, exchange: str, trade_data: Dict):
        """Broadcast individual trade update."""
        if not self.active_connections:
            return
            
        message = {
            "type": "trade_update",
            "timestamp": datetime.now().isoformat(),
            "exchange": exchange,
            "data": trade_data
        }
        
        await self.broadcast_update(message)
        
    async def start_periodic_updates(self):
        """Start periodic position updates."""
        if self._running:
            return
            
        self._running = True
        self.logger.info(f"Starting periodic WebSocket updates every {self.update_interval} seconds")
        
        while self._running:
            try:
                if self.active_connections:
                    # Reload position data
                    await self.position_manager.load_positions()
                    
                    # Get updated data
                    summary = self.position_manager.summarize_positions()
                    net_positions = self.position_manager.get_all_net_positions()
                    
                    update_data = {
                        "summary": summary,
                        "net_positions": net_positions,
                        "risk_metrics": self.calculate_risk_metrics(summary, net_positions)
                    }
                    
                    await self.broadcast_update(update_data)
                    
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
        
    async def notify_trade_execution(self, exchange: str, trade: Dict):
        """Notify about new trade execution."""
        try:
            # Update position manager
            await self.position_manager.update_from_trade(exchange, trade)
            
            # Broadcast trade update
            await self.broadcast_trade_update(exchange, {
                "symbol": trade.get("symbol"),
                "side": trade.get("side"),
                "amount": trade.get("amount"),
                "price": trade.get("price"),
                "timestamp": trade.get("timestamp")
            })
            
            # Get updated position data for this symbol
            symbol = trade.get("symbol")
            if symbol:
                position = self.position_manager.get_position(exchange, symbol)
                net_position = self.position_manager.get_net_position(symbol)
                
                # Broadcast position update
                await self.broadcast_update({
                    "type": "position_change",
                    "exchange": exchange,
                    "symbol": symbol,
                    "position": position.to_dict() if position else None,
                    "net_position": net_position
                })
                
        except Exception as e:
            self.logger.error(f"Error processing trade notification: {e}")


# Global WebSocket manager instance
_websocket_manager: WebSocketManager = None


def get_websocket_manager() -> WebSocketManager:
    """Get the global WebSocket manager instance."""
    global _websocket_manager
    if _websocket_manager is None:
        raise RuntimeError("WebSocket manager not initialized")
    return _websocket_manager


def initialize_websocket_manager(position_manager: PositionManager) -> WebSocketManager:
    """Initialize the global WebSocket manager."""
    global _websocket_manager
    _websocket_manager = WebSocketManager(position_manager)
    return _websocket_manager 