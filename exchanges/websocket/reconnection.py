"""WebSocket reconnection manager."""

import asyncio
import time
from typing import Optional, Callable
import logging

logger = logging.getLogger(__name__)


class ReconnectionManager:
    """Manages WebSocket reconnection logic."""
    
    def __init__(self, max_retries: int = 5, initial_delay: float = 1.0):
        self.max_retries = max_retries
        self.initial_delay = initial_delay
        self.retry_count = 0
        self.last_connection_time = 0
        
    async def should_reconnect(self) -> bool:
        """Check if reconnection should be attempted."""
        if self.retry_count >= self.max_retries:
            return False
            
        # Exponential backoff
        delay = self.initial_delay * (2 ** self.retry_count)
        time_since_last = time.time() - self.last_connection_time
        
        if time_since_last < delay:
            await asyncio.sleep(delay - time_since_last)
            
        return True
    
    def on_connection_success(self):
        """Reset retry count on successful connection."""
        self.retry_count = 0
        self.last_connection_time = time.time()
        
    def on_connection_failure(self):
        """Increment retry count on connection failure."""
        self.retry_count += 1
        logger.warning(f"Connection failed, retry {self.retry_count}/{self.max_retries}")
        
    def reset(self):
        """Reset the reconnection manager."""
        self.retry_count = 0
        self.last_connection_time = 0
