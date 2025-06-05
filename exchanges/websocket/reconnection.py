"""
Reconnection manager for WebSocket connections.

Implements exponential backoff and connection retry logic for WebSocket connections
with configurable parameters and failure tracking.
"""

import time
import logging
from typing import Dict, Optional
from dataclasses import dataclass
import asyncio

logger = logging.getLogger(__name__)


@dataclass
class ReconnectionConfig:
    """Configuration for reconnection behavior."""
    initial_delay: float = 1.0      # Initial delay in seconds
    max_delay: float = 60.0         # Maximum delay in seconds
    multiplier: float = 2.0         # Backoff multiplier
    max_attempts: int = -1          # Maximum attempts (-1 = unlimited)
    jitter: bool = True             # Add random jitter to delays
    reset_after: float = 300.0      # Reset backoff after successful connection (seconds)
    
    # MEXC-specific settings
    mexc_initial_delay: float = 0.5  # Faster initial reconnection for MEXC
    mexc_max_delay: float = 30.0     # Lower max delay for MEXC
    mexc_reset_after: float = 60.0   # Reset backoff faster for MEXC


class ReconnectionManager:
    """
    Manages reconnection logic for WebSocket connections.
    
    Features:
    - Exponential backoff with jitter
    - Per-connection retry tracking
    - Configurable retry limits
    - Automatic backoff reset after successful connections
    """
    
    def __init__(self, config: Optional[Dict] = None):
        """
        Initialize reconnection manager.
        
        Args:
            config: Reconnection configuration dictionary
        """
        # Default configuration
        default_config = {
            'initial_delay': 1.0,
            'max_delay': 60.0,
            'multiplier': 2.0,
            'max_attempts': -1,
            'jitter': True,
            'reset_after': 300.0
        }
        
        # Update with provided config
        if config:
            default_config.update(config)
        
        self.config = ReconnectionConfig(**default_config)
        
        # Per-connection state tracking
        self.attempt_counts: Dict[str, int] = {}
        self.last_success: Dict[str, float] = {}
        self.current_delays: Dict[str, float] = {}
        
        self.logger = logging.getLogger(f"{__name__}.ReconnectionManager")
    
    def get_delay(self, connection_id: str) -> float:
        """
        Get the delay before next reconnection attempt.
        
        Args:
            connection_id: Unique identifier for the connection
            
        Returns:
            Delay in seconds before next attempt
        """
        # Check if we should reset the backoff
        self._maybe_reset_backoff(connection_id)
        
        # Increment attempt count
        if connection_id not in self.attempt_counts:
            self.attempt_counts[connection_id] = 0
        
        self.attempt_counts[connection_id] += 1
        
        # Check max attempts limit
        if (self.config.max_attempts > 0 and 
            self.attempt_counts[connection_id] > self.config.max_attempts):
            
            self.logger.error(
                f"Max reconnection attempts ({self.config.max_attempts}) "
                f"exceeded for {connection_id}"
            )
            return float('inf')  # Stop reconnecting
        
        # Use MEXC-specific settings if this is a MEXC connection
        is_mexc = 'mexc' in connection_id.lower()
        
        if is_mexc:
            initial_delay = self.config.mexc_initial_delay
            max_delay = self.config.mexc_max_delay
        else:
            initial_delay = self.config.initial_delay
            max_delay = self.config.max_delay
        
        # Calculate delay using exponential backoff
        if connection_id not in self.current_delays:
            self.current_delays[connection_id] = initial_delay
        else:
            self.current_delays[connection_id] = min(
                self.current_delays[connection_id] * self.config.multiplier,
                max_delay
            )
        
        delay = self.current_delays[connection_id]
        
        # Add jitter if enabled
        if self.config.jitter:
            import random
            jitter_factor = random.uniform(0.5, 1.5)
            delay *= jitter_factor
        
        self.logger.info(
            f"Reconnection delay for {connection_id}: {delay:.2f}s "
            f"(attempt {self.attempt_counts[connection_id]})"
            f"{' [MEXC-optimized]' if is_mexc else ''}"
        )
        
        return delay
    
    def mark_success(self, connection_id: str):
        """
        Mark a connection as successfully established.
        
        Args:
            connection_id: Unique identifier for the connection
        """
        self.last_success[connection_id] = time.time()
        
        # Reset attempt count on successful connection
        if connection_id in self.attempt_counts:
            self.logger.info(
                f"Connection {connection_id} successful after "
                f"{self.attempt_counts[connection_id]} attempts"
            )
            self.attempt_counts[connection_id] = 0
        
        # Reset delay
        if connection_id in self.current_delays:
            del self.current_delays[connection_id]
        
        self.logger.debug(f"Marked {connection_id} as successfully connected")
    
    def mark_failure(self, connection_id: str, error: Optional[Exception] = None):
        """
        Mark a connection attempt as failed.
        
        Args:
            connection_id: Unique identifier for the connection
            error: Optional error that caused the failure
        """
        if connection_id not in self.attempt_counts:
            self.attempt_counts[connection_id] = 0
        
        error_msg = f" ({error})" if error else ""
        self.logger.warning(
            f"Connection failure for {connection_id}"
            f"{error_msg}"
        )
    
    def should_reconnect(self, connection_id: str) -> bool:
        """
        Check if reconnection should be attempted.
        
        Args:
            connection_id: Unique identifier for the connection
            
        Returns:
            True if reconnection should be attempted, False otherwise
        """
        if self.config.max_attempts <= 0:
            return True  # Unlimited attempts
        
        attempt_count = self.attempt_counts.get(connection_id, 0)
        return attempt_count < self.config.max_attempts
    
    def reset_connection(self, connection_id: str):
        """
        Reset all state for a connection.
        
        Args:
            connection_id: Unique identifier for the connection
        """
        if connection_id in self.attempt_counts:
            del self.attempt_counts[connection_id]
        
        if connection_id in self.last_success:
            del self.last_success[connection_id]
        
        if connection_id in self.current_delays:
            del self.current_delays[connection_id]
        
        self.logger.info(f"Reset reconnection state for {connection_id}")
    
    def get_stats(self) -> Dict[str, Dict]:
        """
        Get reconnection statistics for all connections.
        
        Returns:
            Dictionary of connection statistics
        """
        stats = {}
        
        for connection_id in set(list(self.attempt_counts.keys()) + 
                                list(self.last_success.keys())):
            
            stats[connection_id] = {
                'attempt_count': self.attempt_counts.get(connection_id, 0),
                'current_delay': self.current_delays.get(connection_id, 0),
                'last_success': self.last_success.get(connection_id),
                'should_reconnect': self.should_reconnect(connection_id)
            }
        
        return stats
    
    def _maybe_reset_backoff(self, connection_id: str):
        """Reset backoff if enough time has passed since last success."""
        if connection_id not in self.last_success:
            return
        
        time_since_success = time.time() - self.last_success[connection_id]
        
        # Use MEXC-specific reset timing
        is_mexc = 'mexc' in connection_id.lower()
        reset_threshold = self.config.mexc_reset_after if is_mexc else self.config.reset_after
        
        if time_since_success >= reset_threshold:
            # Reset backoff after successful period
            if connection_id in self.current_delays:
                old_delay = self.current_delays[connection_id]
                del self.current_delays[connection_id]
                
                self.logger.info(
                    f"Reset backoff delay for {connection_id} "
                    f"(was {old_delay:.2f}s, stable for {time_since_success:.1f}s)"
                    f"{' [MEXC-optimized]' if is_mexc else ''}"
                )
    
    def update_config(self, config: Dict):
        """
        Update reconnection configuration.
        
        Args:
            config: New configuration values
        """
        # Update config
        for key, value in config.items():
            if hasattr(self.config, key):
                setattr(self.config, key, value)
                self.logger.info(f"Updated reconnection config: {key} = {value}")
            else:
                self.logger.warning(f"Unknown config key: {key}")
    
    def cleanup_old_connections(self, active_connections: set, max_age: float = 3600):
        """
        Clean up state for old/inactive connections.
        
        Args:
            active_connections: Set of currently active connection IDs
            max_age: Maximum age in seconds for inactive connections
        """
        current_time = time.time()
        connections_to_remove = []
        
        # Find old connections to clean up
        all_connections = set(
            list(self.attempt_counts.keys()) + 
            list(self.last_success.keys()) +
            list(self.current_delays.keys())
        )
        
        for connection_id in all_connections:
            if connection_id in active_connections:
                continue  # Skip active connections
            
            last_activity = self.last_success.get(connection_id, 0)
            if current_time - last_activity > max_age:
                connections_to_remove.append(connection_id)
        
        # Remove old connections
        for connection_id in connections_to_remove:
            self.reset_connection(connection_id)
            self.logger.debug(f"Cleaned up old connection state: {connection_id}")
        
        if connections_to_remove:
            self.logger.info(f"Cleaned up {len(connections_to_remove)} old connections")
