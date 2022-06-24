import time

import limits

from searvey.rate_limit import RateLimit
from searvey.rate_limit import wait


def test_RateLimit() -> None:
    # Define a function with a rate limit of N calls / second and call it N+1 times
    # Then check that the duration of all the calls was ≰ 1 second
    limit = 5
    repetitions = limit + 1
    rate_limit = RateLimit(rate_limit=limits.parse(f"{limit}/second"))

    def return_one():
        while rate_limit.reached("identifier"):
            wait()
        return 1

    t1 = time.time()
    total = sum(return_one() for _ in range(repetitions))
    t2 = time.time()

    assert t2 - t1 > 1
    assert total == repetitions
