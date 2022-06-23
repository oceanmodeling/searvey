import random
import time
from typing import Type

import limits


def wait(wait_time: float = 0.1, jitter: bool = True) -> None:
    """Wait for `wait_time` + some random `jitter`"""
    if jitter:
        # Set jitter to be <= 1% of wait_time
        jitter_time = random.random() / (1 / wait_time * 100)
    else:
        jitter_time = 0
    time.sleep(wait_time + jitter_time)


class RateLimit:
    def __init__(
        self,
        rate_limit: limits.RateLimitItem = limits.parse("5/second"),
        storage: Type[limits.storage.Storage] = limits.storage.MemoryStorage,
        strategy: Type[limits.strategies.RateLimiter] = limits.strategies.MovingWindowRateLimiter,
    ) -> None:
        self.rate_limit = rate_limit
        self.storage = storage()
        self.strategy = strategy(self.storage)

    def reached(self, identifier: str, cost: int = 1) -> bool:
        return not self.strategy.hit(
            self.rate_limit,
            identifier,
        )
