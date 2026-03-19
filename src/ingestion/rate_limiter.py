"""
Token Bucket Rate Limiter для API goszakup.gov.kz.
"""
import asyncio
import time


class RateLimiter:
    """Token bucket rate limiter."""

    def __init__(self, requests_per_second: float = 5.0):
        self.rate = requests_per_second
        self.max_tokens = requests_per_second * 2
        self.tokens = self.max_tokens
        self.last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self):
        """Wait until a token is available."""
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self.last_refill
            self.tokens = min(self.max_tokens, self.tokens + elapsed * self.rate)
            self.last_refill = now

            if self.tokens < 1:
                wait_time = (1 - self.tokens) / self.rate
                await asyncio.sleep(wait_time)
                self.tokens = 0
            else:
                self.tokens -= 1
