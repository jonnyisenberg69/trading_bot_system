"""
Unit tests for the RateLimiter class.

Tests the distributed rate limiting functionality using Redis token bucket algorithm.
"""

import pytest
import asyncio
import time
from unittest.mock import AsyncMock, patch
from decimal import Decimal

from exchanges.rate_limiter import RateLimiter, RateLimit


class TestRateLimit:
    """Test RateLimit configuration class."""
    
    def test_rate_limit_creation(self):
        """Test RateLimit object creation with valid parameters."""
        rate_limit = RateLimit(max_requests=100, window_seconds=60, weight=2)
        
        assert rate_limit.max_requests == 100
        assert rate_limit.window_seconds == 60
        assert rate_limit.weight == 2
    
    def test_rate_limit_validation(self):
        """Test RateLimit validation for invalid parameters."""
        with pytest.raises(ValueError, match="max_requests must be positive"):
            RateLimit(max_requests=0, window_seconds=60)
        
        with pytest.raises(ValueError, match="window_seconds must be positive"):
            RateLimit(max_requests=100, window_seconds=0)
        
        with pytest.raises(ValueError, match="weight must be positive"):
            RateLimit(max_requests=100, window_seconds=60, weight=0)
    
    def test_rate_limit_defaults(self):
        """Test RateLimit default values."""
        rate_limit = RateLimit(max_requests=100, window_seconds=60)
        assert rate_limit.weight == 1


class TestRateLimiter:
    """Test RateLimiter functionality."""
    
    @pytest.fixture
    def rate_limiter(self, mock_redis):
        """Create RateLimiter instance for testing."""
        return RateLimiter(mock_redis)
    
    def test_initialization(self, rate_limiter):
        """Test RateLimiter initialization."""
        assert rate_limiter.redis is not None
        assert 'binance' in rate_limiter.exchange_limits
        assert 'bybit' in rate_limiter.exchange_limits
        assert 'hyperliquid' in rate_limiter.exchange_limits
    
    def test_exchange_limits_structure(self, rate_limiter):
        """Test that exchange limits are properly structured."""
        for exchange, limits in rate_limiter.exchange_limits.items():
            assert isinstance(limits, dict)
            assert 'default' in limits
            assert isinstance(limits['default'], RateLimit)
            
            for endpoint, rate_limit in limits.items():
                assert isinstance(rate_limit, RateLimit)
                assert rate_limit.max_requests > 0
                assert rate_limit.window_seconds > 0
                assert rate_limit.weight > 0
    
    @pytest.mark.asyncio
    async def test_check_limit_success(self, rate_limiter, mock_redis):
        """Test successful rate limit check."""
        # Mock Redis eval to return 1 (allowed)
        mock_redis.eval.return_value = 1
        
        result = await rate_limiter.check_limit('binance', 'default', 1)
        
        assert result is True
        mock_redis.eval.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_check_limit_exceeded(self, rate_limiter, mock_redis):
        """Test rate limit exceeded."""
        # Mock Redis eval to return 0 (denied)
        mock_redis.eval.return_value = 0
        
        result = await rate_limiter.check_limit('binance', 'default', 1)
        
        assert result is False
        mock_redis.eval.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_check_limit_unknown_exchange(self, rate_limiter, mock_redis):
        """Test rate limit check for unknown exchange."""
        # Should return True for unknown exchanges
        result = await rate_limiter.check_limit('unknown_exchange', 'default', 1)
        
        assert result is True
        # Should not call Redis for unknown exchanges
        mock_redis.eval.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_check_limit_unknown_endpoint(self, rate_limiter, mock_redis):
        """Test rate limit check for unknown endpoint (should fallback to default)."""
        mock_redis.eval.return_value = 1
        
        result = await rate_limiter.check_limit('binance', 'unknown_endpoint', 1)
        
        assert result is True
        mock_redis.eval.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_check_limit_with_weight(self, rate_limiter, mock_redis):
        """Test rate limit check with custom weight."""
        mock_redis.eval.return_value = 1
        
        result = await rate_limiter.check_limit('binance', 'order', 5)
        
        assert result is True
        
        # Check that the Lua script was called with correct weight
        call_args = mock_redis.eval.call_args
        assert call_args[0][2] == 'rate_limit:binance:order'  # Redis key
        # Weight should be 5 * rate_limit.weight (which is 1 for order endpoint)
        assert int(float(call_args[0][5])) == 5  # requested_tokens in Lua script args (ARGV[3])
    
    @pytest.mark.asyncio
    async def test_check_limit_redis_error(self, rate_limiter, mock_redis):
        """Test rate limit check when Redis fails."""
        # Mock Redis to raise an exception
        mock_redis.eval.side_effect = Exception("Redis connection failed")
        
        result = await rate_limiter.check_limit('binance', 'default', 1)
        
        # Should return True on error to avoid blocking
        assert result is True
    
    @pytest.mark.asyncio
    async def test_wait_for_limit_success(self, rate_limiter, mock_redis):
        """Test waiting for rate limit when quota becomes available."""
        # Mock Redis to return denied first, then allowed
        mock_redis.eval.side_effect = [0, 1]
        
        with patch('asyncio.sleep') as mock_sleep:
            result = await rate_limiter.wait_for_limit('binance', 'default', 1, max_wait=1.0)
        
        assert result is True
        mock_sleep.assert_called_once_with(0.1)
    
    @pytest.mark.asyncio
    async def test_wait_for_limit_timeout(self, rate_limiter, mock_redis):
        """Test waiting for rate limit with timeout."""
        # Mock Redis to always return denied
        mock_redis.eval.return_value = 0
        
        # Use actual time for timeout test
        start_time = time.time()
        result = await rate_limiter.wait_for_limit('binance', 'default', 1, max_wait=0.2)
        elapsed_time = time.time() - start_time
        
        assert result is False
        assert elapsed_time >= 0.2
        assert elapsed_time < 0.5  # Should not wait much longer than max_wait
    
    @pytest.mark.asyncio
    async def test_get_remaining_quota(self, rate_limiter, mock_redis):
        """Test getting remaining quota."""
        mock_redis.get.return_value = b'50'  # 50 tokens remaining
        
        result = await rate_limiter.get_remaining_quota('binance', 'default')
        
        assert result == 50
        mock_redis.get.assert_called_once_with('rate_limit:binance:default:tokens')
    
    @pytest.mark.asyncio
    async def test_get_remaining_quota_no_data(self, rate_limiter, mock_redis):
        """Test getting remaining quota when no data exists."""
        mock_redis.get.return_value = None
        
        result = await rate_limiter.get_remaining_quota('binance', 'default')
        
        # Should return max_requests when no data exists
        assert result == 1200  # binance default limit
    
    @pytest.mark.asyncio
    async def test_get_remaining_quota_unknown_exchange(self, rate_limiter, mock_redis):
        """Test getting remaining quota for unknown exchange."""
        result = await rate_limiter.get_remaining_quota('unknown', 'default')
        
        assert result is None
        mock_redis.get.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_reset_limits_specific_endpoint(self, rate_limiter, mock_redis):
        """Test resetting limits for specific endpoint."""
        await rate_limiter.reset_limits('binance', 'order')
        
        # Should delete both tokens and last_refill keys
        expected_keys = [
            'rate_limit:binance:order:tokens',
            'rate_limit:binance:order:last_refill'
        ]
        mock_redis.delete.assert_called_once_with(*expected_keys)
    
    @pytest.mark.asyncio
    async def test_reset_limits_all_endpoints(self, rate_limiter, mock_redis):
        """Test resetting limits for all endpoints of an exchange."""
        mock_redis.keys.return_value = [
            b'rate_limit:binance:default:tokens',
            b'rate_limit:binance:order:tokens',
            b'rate_limit:binance:query:tokens'
        ]
        
        await rate_limiter.reset_limits('binance')
        
        mock_redis.keys.assert_called_once_with('rate_limit:binance:*')
        mock_redis.delete.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_token_bucket_lua_script(self, rate_limiter, mock_redis):
        """Test that the token bucket Lua script is called with correct parameters."""
        mock_redis.eval.return_value = 1
        
        await rate_limiter.check_limit('binance', 'default', 3)
        
        # Verify Lua script call
        call_args = mock_redis.eval.call_args
        lua_script = call_args[0][0]
        
        # Check that it's a Lua script (contains Lua keywords)
        assert 'local' in lua_script
        assert 'redis.call' in lua_script
        assert 'tokens_key' in lua_script
        
        # Check parameters
        assert call_args[0][1] == 1  # Number of keys
        assert call_args[0][2] == 'rate_limit:binance:default'  # Key
        assert int(call_args[0][3]) == 1200  # max_tokens
        assert int(call_args[0][4]) == 60    # refill_period
        assert int(call_args[0][5]) == 3     # requested_tokens
        # Last parameter is current_time (timestamp)
    
    @pytest.mark.asyncio
    async def test_get_stats(self, rate_limiter, mock_redis):
        """Test getting rate limiter statistics."""
        # Mock Redis responses for different exchanges
        mock_redis.get.side_effect = lambda key: {
            'rate_limit:binance:default:tokens': b'800',
            'rate_limit:binance:order:tokens': b'5',
            'rate_limit:bybit:default:tokens': b'400'
        }.get(key)
        
        stats = await rate_limiter.get_stats()
        
        assert 'binance' in stats
        assert 'bybit' in stats
        assert 'default' in stats['binance']
        assert 'order' in stats['binance']
        
        # Check specific values
        assert stats['binance']['default']['remaining'] == 800
        assert stats['binance']['default']['max_requests'] == 1200
        assert stats['binance']['order']['remaining'] == 5
    
    def test_add_exchange_limits(self, rate_limiter):
        """Test adding new exchange limits."""
        new_limits = {
            'custom_endpoint': RateLimit(50, 10, 2)
        }
        
        rate_limiter.add_exchange_limits('new_exchange', new_limits)
        
        assert 'new_exchange' in rate_limiter.exchange_limits
        assert 'custom_endpoint' in rate_limiter.exchange_limits['new_exchange']
        
        limit = rate_limiter.exchange_limits['new_exchange']['custom_endpoint']
        assert limit.max_requests == 50
        assert limit.window_seconds == 10
        assert limit.weight == 2
    
    def test_get_rate_limit_existing(self, rate_limiter):
        """Test getting rate limit for existing exchange/endpoint."""
        rate_limit = rate_limiter._get_rate_limit('binance', 'default')
        
        assert rate_limit is not None
        assert isinstance(rate_limit, RateLimit)
        assert rate_limit.max_requests == 1200
        assert rate_limit.window_seconds == 60
    
    def test_get_rate_limit_fallback_to_default(self, rate_limiter):
        """Test getting rate limit falls back to default for unknown endpoint."""
        rate_limit = rate_limiter._get_rate_limit('binance', 'unknown_endpoint')
        
        assert rate_limit is not None
        assert rate_limit.max_requests == 1200  # Should be binance default
    
    def test_get_rate_limit_unknown_exchange(self, rate_limiter):
        """Test getting rate limit for unknown exchange."""
        rate_limit = rate_limiter._get_rate_limit('unknown_exchange', 'default')
        
        assert rate_limit is None


class TestRateLimiterIntegration:
    """Integration tests for RateLimiter with realistic scenarios."""
    
    @pytest.mark.asyncio
    async def test_concurrent_requests(self, rate_limiter, mock_redis):
        """Test rate limiter with concurrent requests."""
        # Mock Redis to allow first 5 requests, then deny
        mock_redis.eval.side_effect = [1, 1, 1, 1, 1, 0, 0, 0]
        
        # Create multiple concurrent requests
        tasks = []
        for i in range(8):
            task = asyncio.create_task(
                rate_limiter.check_limit('binance', 'order', 1)
            )
            tasks.append(task)
        
        results = await asyncio.gather(*tasks)
        
        # First 5 should be allowed, last 3 denied
        assert sum(results) == 5
        assert results[:5] == [True] * 5
        assert results[5:] == [False] * 3
    
    @pytest.mark.asyncio
    async def test_different_exchanges_independent(self, rate_limiter, mock_redis):
        """Test that different exchanges have independent rate limits."""
        mock_redis.eval.return_value = 1
        
        # Make requests to different exchanges
        binance_result = await rate_limiter.check_limit('binance', 'default', 1)
        bybit_result = await rate_limiter.check_limit('bybit', 'default', 1)
        
        assert binance_result is True
        assert bybit_result is True
        
        # Should have called Redis eval twice with different keys
        assert mock_redis.eval.call_count == 2
        
        call_keys = [call[0][2] for call in mock_redis.eval.call_args_list]
        assert 'rate_limit:binance:default' in call_keys
        assert 'rate_limit:bybit:default' in call_keys
    
    @pytest.mark.asyncio
    async def test_weighted_requests(self, rate_limiter, mock_redis):
        """Test rate limiting with weighted requests."""
        mock_redis.eval.return_value = 1
        
        # Make weighted request
        result = await rate_limiter.check_limit('binance', 'default', 10)
        
        assert result is True
        
        # Check that the effective weight was calculated correctly
        call_args = mock_redis.eval.call_args
        # Weight should be 10 * 1 (default weight) = 10
        assert int(call_args[0][5]) == 10
