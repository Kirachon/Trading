"""
Redis-based rate limiter for exchange API calls.
"""
import redis.asyncio as redis
import asyncio
import time
import logging
from typing import Optional

logger = logging.getLogger(__name__)

class RateLimiter:
    """Token bucket rate limiter using Redis."""
    
    def __init__(self, redis_url: str, exchange_id: str, max_requests: int, window_seconds: int):
        """
        Initialize rate limiter.
        
        Args:
            redis_url: Redis connection URL
            exchange_id: Exchange identifier
            max_requests: Maximum requests per window
            window_seconds: Time window in seconds
        """
        self.redis = redis.from_url(redis_url)
        self.key = f"rate_limit:{exchange_id}"
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        
    async def acquire(self, timeout: Optional[float] = None) -> bool:
        """
        Acquire a rate limit token, blocking if necessary.
        
        Args:
            timeout: Maximum time to wait for token (None = no timeout)
            
        Returns:
            True if token acquired, False if timeout
        """
        start_time = time.time()
        
        while True:
            try:
                # Use Redis pipeline for atomic operations
                async with self.redis.pipeline() as pipe:
                    pipe.multi()
                    pipe.incr(self.key)
                    pipe.expire(self.key, self.window_seconds)
                    result = await pipe.execute()
                
                count = result[0]
                
                if count <= self.max_requests:
                    return True
                
                # Check timeout
                if timeout and (time.time() - start_time) >= timeout:
                    return False
                
                # Calculate sleep time
                sleep_time = self.window_seconds / self.max_requests
                await asyncio.sleep(sleep_time)
                
            except Exception as e:
                logger.error(f"Rate limiter error: {e}")
                # Fallback: allow request but log error
                return True
    
    async def get_remaining(self) -> int:
        """Get remaining requests in current window."""
        try:
            count = await self.redis.get(self.key)
            if count is None:
                return self.max_requests
            return max(0, self.max_requests - int(count))
        except Exception as e:
            logger.error(f"Error getting remaining requests: {e}")
            return self.max_requests
    
    async def reset(self):
        """Reset the rate limiter."""
        try:
            await self.redis.delete(self.key)
        except Exception as e:
            logger.error(f"Error resetting rate limiter: {e}")
    
    async def close(self):
        """Close Redis connection."""
        try:
            await self.redis.close()
        except Exception as e:
            logger.error(f"Error closing Redis connection: {e}")

class ExchangeRateLimiters:
    """Manages rate limiters for different exchanges."""
    
    def __init__(self, redis_url: str):
        self.redis_url = redis_url
        self.limiters = {}
        
        # Exchange-specific rate limits (requests per minute)
        self.limits = {
            'binance': {'requests': 1200, 'window': 60},
            'kucoin': {'requests': 1800, 'window': 60},
            'coinbase': {'requests': 600, 'window': 60},
        }
    
    def get_limiter(self, exchange_id: str) -> RateLimiter:
        """Get rate limiter for exchange."""
        if exchange_id not in self.limiters:
            limits = self.limits.get(exchange_id, {'requests': 600, 'window': 60})
            self.limiters[exchange_id] = RateLimiter(
                self.redis_url,
                exchange_id,
                limits['requests'],
                limits['window']
            )
        return self.limiters[exchange_id]
    
    async def close_all(self):
        """Close all rate limiters."""
        for limiter in self.limiters.values():
            await limiter.close()
        self.limiters.clear()
