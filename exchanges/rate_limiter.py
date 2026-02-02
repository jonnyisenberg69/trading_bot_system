"""
Distributed rate limiter for exchange API requests.

Uses Redis to implement token bucket algorithm for rate limiting across
multiple bot instances and processes.
"""

import asyncio
import time
import logging
from typing import Dict, Optional, Any
from dataclasses import dataclass
import redis.asyncio as redis

logger = logging.getLogger(__name__)


@dataclass
class RateLimit:
    """Rate limit configuration for an endpoint."""
    max_requests: int
    window_seconds: int
    weight: int = 1
    
    def __post_init__(self):
        """Validate rate limit configuration."""
        if self.max_requests <= 0:
            raise ValueError("max_requests must be positive")
        if self.window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        if self.weight <= 0:
            raise ValueError("weight must be positive")


class RateLimiter:
    """
    Distributed rate limiter using Redis token bucket algorithm.
    
    Supports per-exchange, per-endpoint rate limiting with request weights.
    Thread-safe and works across multiple processes/instances.
    """
    
    def __init__(self, redis_client: redis.Redis):
        """
        Initialize rate limiter.
        
        Args:
            redis_client: Async Redis client for distributed coordination
        """
        self.redis = redis_client
        self.logger = logging.getLogger(f"{__name__}.RateLimiter")
        
        # Exchange-specific rate limits
        self.exchange_limits: Dict[str, Dict[str, RateLimit]] = {
            'binance': {
                'default': RateLimit(1200, 60),  # 1200 weight per minute
                'order': RateLimit(10, 1, weight=1),  # Order endpoints
                'query': RateLimit(20, 1, weight=1),  # Query endpoints
                'websocket': RateLimit(5, 1, weight=1)  # WebSocket management
            },
            'bybit': {
                'default': RateLimit(600, 5),  # 600 requests per 5 seconds
                'order': RateLimit(100, 5, weight=1),
                'query': RateLimit(600, 5, weight=1),
                'websocket': RateLimit(10, 1, weight=1)
            },
            'hyperliquid': {
                'default': RateLimit(200, 1),  # More conservative default
                'order': RateLimit(20, 1, weight=1),
                'query': RateLimit(200, 1, weight=1),
                'websocket': RateLimit(5, 1, weight=1)
            },
            'mexc': {
                'default': RateLimit(100, 1),  # 100 requests per second
                'order': RateLimit(20, 1, weight=1),
                'query': RateLimit(100, 1, weight=1),
                'websocket': RateLimit(5, 1, weight=1)
            },
            'gateio': {
                'default': RateLimit(900, 1),  # 900 requests per second
                'order': RateLimit(100, 1, weight=1),
                'query': RateLimit(900, 1, weight=1),
                'websocket': RateLimit(10, 1, weight=1)
            },
            'bitget': {
                'default': RateLimit(100, 1),  # 100 requests per second
                'order': RateLimit(20, 1, weight=1),
                'query': RateLimit(100, 1, weight=1),
                'websocket': RateLimit(5, 1, weight=1)
            }
        }
    
    async def check_limit(
        self, 
        exchange: str, 
        endpoint: str = 'default', 
        weight: int = 1
    ) -> bool:
        """
        Check if request is within rate limits.
        
        Args:
            exchange: Exchange name
            endpoint: API endpoint category
            weight: Request weight (for weighted rate limiting)
            
        Returns:
            True if request allowed, False if rate limited
        """
        try:
            rate_limit = self._get_rate_limit(exchange, endpoint)
            if not rate_limit:
                self.logger.warning(f"No rate limit defined for {exchange}:{endpoint}")
                return True
            
            # Calculate effective weight
            effective_weight = weight * rate_limit.weight
            
            # Redis key for this rate limit
            key = f"rate_limit:{exchange}:{endpoint}"
            
            # Use Redis Lua script for atomic token bucket operation
            allowed = await self._check_token_bucket(
                key, 
                rate_limit.max_requests,
                rate_limit.window_seconds,
                effective_weight
            )
            
            if not allowed:
                self.logger.warning(
                    f"Rate limit exceeded for {exchange}:{endpoint} "
                    f"(weight={effective_weight})"
                )
            
            return allowed
            
        except Exception as e:
            self.logger.error(f"Rate limit check failed: {e}")
            # On error, allow request to avoid blocking
            return True
    
    async def wait_for_limit(
        self, 
        exchange: str, 
        endpoint: str = 'default',
        weight: int = 1,
        max_wait: float = 60.0
    ) -> bool:
        """
        Wait until request is allowed by rate limiter.
        
        Args:
            exchange: Exchange name
            endpoint: API endpoint category
            weight: Request weight
            max_wait: Maximum time to wait in seconds
            
        Returns:
            True if allowed within max_wait, False if timed out
        """
        start_time = time.time()
        
        while time.time() - start_time < max_wait:
            if await self.check_limit(exchange, endpoint, weight):
                return True
            
            # Wait before retrying
            await asyncio.sleep(0.1)
        
        self.logger.warning(
            f"Rate limit wait timeout for {exchange}:{endpoint} "
            f"after {max_wait}s"
        )
        return False
    
    async def get_remaining_quota(
        self, 
        exchange: str, 
        endpoint: str = 'default'
    ) -> Optional[int]:
        """
        Get remaining quota for endpoint.
        
        Args:
            exchange: Exchange name
            endpoint: API endpoint category
            
        Returns:
            Remaining requests in current window, or None if unknown
        """
        try:
            rate_limit = self._get_rate_limit(exchange, endpoint)
            if not rate_limit:
                return None
            
            key = f"rate_limit:{exchange}:{endpoint}"
            
            # Get current token count
            current_tokens = await self.redis.get(f"{key}:tokens")
            if current_tokens is None:
                return rate_limit.max_requests
            
            return max(0, int(current_tokens))
            
        except Exception as e:
            self.logger.error(f"Error getting remaining quota: {e}")
            return None
    
    async def reset_limits(self, exchange: str, endpoint: Optional[str] = None):
        """
        Reset rate limits for exchange/endpoint.
        
        Args:
            exchange: Exchange name
            endpoint: Specific endpoint to reset, None for all
        """
        try:
            if endpoint:
                # Reset specific endpoint
                key = f"rate_limit:{exchange}:{endpoint}"
                await self.redis.delete(f"{key}:tokens", f"{key}:last_refill")
            else:
                # Reset all endpoints for exchange
                pattern = f"rate_limit:{exchange}:*"
                keys = await self.redis.keys(pattern)
                if keys:
                    await self.redis.delete(*keys)
            
            self.logger.info(f"Reset rate limits for {exchange}:{endpoint or 'all'}")
            
        except Exception as e:
            self.logger.error(f"Error resetting rate limits: {e}")
    
    def _get_rate_limit(self, exchange: str, endpoint: str) -> Optional[RateLimit]:
        """Get rate limit configuration for exchange/endpoint."""
        exchange_limits = self.exchange_limits.get(exchange)
        if not exchange_limits:
            return None
        
        # Try specific endpoint first, fall back to default
        return exchange_limits.get(endpoint) or exchange_limits.get('default')
    
    async def _check_token_bucket(
        self, 
        key: str, 
        max_tokens: int, 
        refill_period: int,
        requested_tokens: int
    ) -> bool:
        """
        Implement token bucket algorithm using Redis Lua script.
        
        Args:
            key: Redis key prefix
            max_tokens: Maximum tokens in bucket
            refill_period: Time in seconds to refill bucket
            requested_tokens: Number of tokens requested
            
        Returns:
            True if tokens available, False otherwise
        """
        # Lua script for atomic token bucket operation
        lua_script = """
        local tokens_key = KEYS[1] .. ':tokens'
        local last_refill_key = KEYS[1] .. ':last_refill'
        
        local max_tokens = tonumber(ARGV[1])
        local refill_period = tonumber(ARGV[2])
        local requested_tokens = tonumber(ARGV[3])
        local current_time = tonumber(ARGV[4])
        
        -- Get current state
        local current_tokens = tonumber(redis.call('GET', tokens_key)) or max_tokens
        local last_refill = tonumber(redis.call('GET', last_refill_key)) or current_time
        
        -- Calculate tokens to add based on time elapsed
        local time_elapsed = current_time - last_refill
        local tokens_to_add = math.floor(time_elapsed * max_tokens / refill_period)
        
        -- Update token count (don't exceed max)
        current_tokens = math.min(max_tokens, current_tokens + tokens_to_add)
        
        -- Check if we have enough tokens
        if current_tokens >= requested_tokens then
            -- Consume tokens
            current_tokens = current_tokens - requested_tokens
            
            -- Update Redis
            redis.call('SET', tokens_key, current_tokens)
            redis.call('SET', last_refill_key, current_time)
            redis.call('EXPIRE', tokens_key, refill_period * 2)
            redis.call('EXPIRE', last_refill_key, refill_period * 2)
            
            return 1  -- Allowed
        else
            -- Update last refill time even if request denied
            redis.call('SET', last_refill_key, current_time)
            redis.call('EXPIRE', last_refill_key, refill_period * 2)
            
            return 0  -- Denied
        end
        """
        
        try:
            current_time = time.time()
            result = await self.redis.eval(
                lua_script,
                1,  # Number of keys
                key,  # Key
                max_tokens,
                refill_period,
                requested_tokens,
                current_time
            )
            
            return bool(result)
            
        except Exception as e:
            self.logger.error(f"Token bucket check failed: {e}")
            # On error, allow request
            return True
    
    async def get_stats(self) -> Dict[str, Any]:
        """
        Get rate limiter statistics.
        
        Returns:
            Dictionary with rate limiting statistics
        """
        try:
            stats = {}
            
            for exchange in self.exchange_limits:
                exchange_stats = {}
                
                for endpoint in self.exchange_limits[exchange]:
                    remaining = await self.get_remaining_quota(exchange, endpoint)
                    rate_limit = self._get_rate_limit(exchange, endpoint)
                    
                    exchange_stats[endpoint] = {
                        'remaining': remaining,
                        'max_requests': rate_limit.max_requests if rate_limit else None,
                        'window_seconds': rate_limit.window_seconds if rate_limit else None
                    }
                
                stats[exchange] = exchange_stats
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Error getting rate limiter stats: {e}")
            return {}
    
    def add_exchange_limits(self, exchange: str, limits: Dict[str, RateLimit]):
        """
        Add or update rate limits for an exchange.
        
        Args:
            exchange: Exchange name
            limits: Dictionary of endpoint -> RateLimit
        """
        if exchange not in self.exchange_limits:
            self.exchange_limits[exchange] = {}
        
        self.exchange_limits[exchange].update(limits)
        self.logger.info(f"Updated rate limits for {exchange}: {list(limits.keys())}")
