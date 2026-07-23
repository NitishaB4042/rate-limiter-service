"""
Rate Limiter Service — Phase 2: distributed token bucket (Redis + atomic Lua).

What changed from Phase 1:
  - Bucket state moves from an in-process dict into Redis, so MANY processes
    (API servers) share one consistent limit per client.
  - The refill-and-take logic moves into a single atomic Lua script that Redis
    runs start-to-finish without interruption. This is what makes the limiter
    correct under concurrency: two simultaneous requests can never both take
    the last token.

The same TokenBucket idea as the crawler's Phase 3, but here it IS the service.

Run the demo (needs Redis; set REDIS_URL or use localhost):
    python limiter_phase2.py

Run the tests:
    python test_phase2.py
"""

import os
import time
import asyncio

import redis.asyncio as aioredis


def redis_url() -> str:
    return os.environ.get("REDIS_URL", "redis://localhost:6379")


NS = os.environ.get("RL_NS", "rl")          # namespace prefix for all our keys


# ---------------------------------------------------------------------------
# The atomic token-bucket script.
#
# State per client: a Redis HASH with fields {tokens, ts}.
#   tokens = fractional tokens currently available
#   ts     = last refill time (ms)
#
# In ONE uninterrupted step the script:
#   1. reads the client's current tokens + timestamp (new client starts full)
#   2. refills by elapsed time, capped at burst
#   3. if >= 1 token: take one, return allowed=1
#      else: return allowed=0 and the ms until one token is ready
#   4. saves the new state, and sets an idle expiry so stale clients vanish
# ---------------------------------------------------------------------------
TOKEN_BUCKET_LUA = """
local key    = KEYS[1]
local rate   = tonumber(ARGV[1])     -- tokens per second
local burst  = tonumber(ARGV[2])     -- bucket capacity
local now_ms = tonumber(ARGV[3])     -- current time in ms
local cost   = tonumber(ARGV[4])     -- tokens this request costs (usually 1)

local data = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts     = tonumber(data[2])

if tokens == nil then
  tokens = burst                     -- brand-new client starts with a full bucket
  ts = now_ms
end

-- refill based on elapsed time
local elapsed = math.max(0, now_ms - ts) / 1000.0
tokens = math.min(burst, tokens + elapsed * rate)
ts = now_ms

local allowed = 0
local retry_ms = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
else
  retry_ms = math.ceil((cost - tokens) / rate * 1000)
end

redis.call('HMSET', key, 'tokens', tokens, 'ts', ts)
-- auto-expire idle clients so we don't keep state forever for one-off callers.
-- keep it alive at least as long as a full refill would take.
local ttl = math.ceil(burst / rate) + 60
redis.call('EXPIRE', key, ttl)

-- return tokens left as an integer (floor) for the X-RateLimit-Remaining header
return {allowed, retry_ms, math.floor(tokens)}
"""


class DistributedTokenBucket:
    """Token-bucket rate limiter backed by Redis + an atomic Lua script."""

    def __init__(self, r: aioredis.Redis, rate: float, burst: float):
        if rate <= 0 or burst <= 0:
            raise ValueError("rate and burst must be positive")
        self.r = r
        self.rate = rate
        self.burst = burst
        self._sha = None                       # cached id of the uploaded script

    async def _ensure_loaded(self):
        if self._sha is None:
            self._sha = await self.r.script_load(TOKEN_BUCKET_LUA)

    def _key(self, client_id: str) -> str:
        return f"{NS}:bucket:{client_id}"

    async def allow(self, client_id: str, cost: float = 1.0):
        """Try to spend `cost` tokens for client_id.

        Returns (allowed: bool, retry_after: float seconds, remaining: int).
        """
        await self._ensure_loaded()
        now_ms = int(time.time() * 1000)
        try:
            allowed, retry_ms, remaining = await self.r.evalsha(
                self._sha, 1, self._key(client_id),
                self.rate, self.burst, now_ms, cost)
        except aioredis.ResponseError:
            # script cache was flushed (e.g. Redis restart/failover) -> reload once
            self._sha = None
            await self._ensure_loaded()
            allowed, retry_ms, remaining = await self.r.evalsha(
                self._sha, 1, self._key(client_id),
                self.rate, self.burst, now_ms, cost)
        return bool(allowed), int(retry_ms) / 1000.0, int(remaining)

    async def reset(self, client_id: str):
        await self.r.delete(self._key(client_id))


async def _demo():
    r = aioredis.from_url(redis_url(), decode_responses=True)
    limiter = DistributedTokenBucket(r, rate=2.0, burst=5.0)
    client = "user-1"
    await limiter.reset(client)

    print("Bucket: rate=2/sec, burst=5.  Firing 8 rapid requests:\n")
    for i in range(1, 9):
        allowed, retry, remaining = await limiter.allow(client)
        status = "ALLOW" if allowed else f"DENY (retry in {retry:.2f}s)"
        print(f"  request {i}: {status}   (remaining {remaining})")

    print("\nSleeping 1.5s to let tokens refill (~3 at 2/sec)...\n")
    await asyncio.sleep(1.5)

    for i in range(9, 13):
        allowed, retry, remaining = await limiter.allow(client)
        status = "ALLOW" if allowed else f"DENY (retry in {retry:.2f}s)"
        print(f"  request {i}: {status}   (remaining {remaining})")

    await limiter.reset(client)
    await r.aclose()


if __name__ == "__main__":
    asyncio.run(_demo())
