"""
Rate Limiter Service — Phase 1: in-memory token bucket.

Goal of this phase: get the token-bucket MATH right in isolation, with no
Redis and no network. Single process, state in a dict. This is the standalone,
cleaned-up version of the bucket you built inside the crawler.

The one question this service answers:
    "Is this client allowed to make a request right now?"

Run the demo:
    python limiter_phase1.py

Run the tests:
    python test_phase1.py
"""

import time
from dataclasses import dataclass, field


@dataclass
class Bucket:
    """The state of one client's bucket: how many tokens, and when last refilled."""
    tokens: float
    last_refill: float       # epoch seconds of the last refill


class TokenBucketLimiter:
    """In-memory token-bucket rate limiter.

    rate  = tokens added per second (the sustained allowed rate)
    burst = bucket capacity (the largest momentary burst allowed)

    A request "spends" one token. If the bucket has < 1 token, the request is
    denied and we report how long until a token is available.
    """

    def __init__(self, rate: float, burst: float):
        if rate <= 0 or burst <= 0:
            raise ValueError("rate and burst must be positive")
        self.rate = rate
        self.burst = burst
        self._buckets: dict[str, Bucket] = {}

    def _now(self) -> float:
        # Wrapped so tests can substitute a fake clock instead of real time.
        return time.monotonic()

    def _refill(self, b: Bucket, now: float) -> None:
        """Add tokens for the time elapsed since the last refill, capped at burst."""
        elapsed = max(0.0, now - b.last_refill)
        b.tokens = min(self.burst, b.tokens + elapsed * self.rate)
        b.last_refill = now

    def allow(self, client_id: str) -> tuple[bool, float]:
        """Try to spend one token for `client_id`.

        Returns (allowed, retry_after_seconds).
        retry_after is 0.0 when allowed; otherwise the wait until 1 token exists.
        """
        now = self._now()
        b = self._buckets.get(client_id)
        if b is None:                      # first time we see this client
            b = Bucket(tokens=self.burst, last_refill=now)  # start full
            self._buckets[client_id] = b

        self._refill(b, now)

        if b.tokens >= 1.0:
            b.tokens -= 1.0
            return True, 0.0

        # not enough: how long until we accrue the shortfall (here, up to 1 token)?
        shortfall = 1.0 - b.tokens
        retry_after = shortfall / self.rate
        return False, retry_after

    def remaining(self, client_id: str) -> int:
        """Whole tokens currently available for a client (after a refill)."""
        b = self._buckets.get(client_id)
        if b is None:
            return int(self.burst)
        self._refill(b, self._now())
        return int(b.tokens)


def _demo() -> None:
    """Fire 8 quick requests at a 2/sec bucket with capacity 5, then wait and retry."""
    limiter = TokenBucketLimiter(rate=2.0, burst=5.0)
    client = "user-1"

    print("Bucket: rate=2/sec, burst=5.  Firing 8 rapid requests:\n")
    for i in range(1, 9):
        allowed, retry = limiter.allow(client)
        status = "ALLOW" if allowed else f"DENY (retry in {retry:.2f}s)"
        print(f"  request {i}: {status}   (remaining ~{limiter.remaining(client)})")

    print("\nSleeping 1.5s to let tokens refill (~3 tokens at 2/sec)...\n")
    time.sleep(1.5)

    for i in range(9, 13):
        allowed, retry = limiter.allow(client)
        status = "ALLOW" if allowed else f"DENY (retry in {retry:.2f}s)"
        print(f"  request {i}: {status}   (remaining ~{limiter.remaining(client)})")


if __name__ == "__main__":
    _demo()
