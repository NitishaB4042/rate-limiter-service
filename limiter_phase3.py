"""
Rate Limiter Service — Phase 3: four algorithms, one interface.

The crawler only needed a token bucket. A standalone service should offer the
classic algorithms and let you pick per use-case. All four share the same
interface and are each implemented as a single ATOMIC Lua script:

  - TokenBucket          : smooth rate + controlled burst
  - FixedWindow          : simplest; suffers the "boundary burst" (2x at edges)
  - SlidingWindowLog     : exact; memory grows with request volume
  - SlidingWindowCounter : cheap approximation of the log; the usual prod pick

Common interface:
    allowed, retry_after, remaining = await limiter.allow(client_id)

Run the demo:   python limiter_phase3.py
Run the tests:  python test_phase3.py
"""

import os
import time
import asyncio

import redis.asyncio as aioredis


def redis_url() -> str:
    return os.environ.get("REDIS_URL", "redis://localhost:6379")

NS = os.environ.get("RL_NS", "rl")


# ===========================================================================
# Base class: handles script loading + the evalsha-with-reload dance.
# ===========================================================================
class BaseLimiter:
    LUA = ""                       # each subclass sets its own script
    NAME = "base"

    def __init__(self, r: aioredis.Redis, limit: float, window: float):
        # limit  = how many requests are allowed per `window` seconds
        # window = the time window in seconds
        if limit <= 0 or window <= 0:
            raise ValueError("limit and window must be positive")
        self.r = r
        self.limit = limit
        self.window = window
        self._sha = None

    def _key(self, client_id: str) -> str:
        return f"{NS}:{self.NAME}:{client_id}"

    async def _ensure_loaded(self):
        if self._sha is None:
            self._sha = await self.r.script_load(self.LUA)

    async def _run(self, key, *args):
        await self._ensure_loaded()
        try:
            return await self.r.evalsha(self._sha, 1, key, *args)
        except aioredis.ResponseError:
            self._sha = None
            await self._ensure_loaded()
            return await self.r.evalsha(self._sha, 1, key, *args)

    async def allow(self, client_id: str):
        raise NotImplementedError

    async def reset(self, client_id: str):
        await self.r.delete(self._key(client_id))


# ===========================================================================
# 1. TOKEN BUCKET  — tokens refill at limit/window per second, cap = limit.
#    Strength: allows a controlled burst (up to `limit`) then steady rate.
# ===========================================================================
TOKEN_BUCKET_LUA = """
local key = KEYS[1]
local rate   = tonumber(ARGV[1])      -- tokens per second
local burst  = tonumber(ARGV[2])      -- capacity
local now_ms = tonumber(ARGV[3])
local data = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts = tonumber(data[2])
if tokens == nil then tokens = burst; ts = now_ms end
local elapsed = math.max(0, now_ms - ts) / 1000.0
tokens = math.min(burst, tokens + elapsed * rate)
ts = now_ms
local allowed = 0
local retry_ms = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
else
  retry_ms = math.ceil((1 - tokens) / rate * 1000)
end
redis.call('HMSET', key, 'tokens', tokens, 'ts', ts)
redis.call('EXPIRE', key, math.ceil(burst / rate) + 60)
return {allowed, retry_ms, math.floor(tokens)}
"""

class TokenBucket(BaseLimiter):
    NAME = "tb"
    LUA = TOKEN_BUCKET_LUA
    async def allow(self, client_id):
        rate = self.limit / self.window      # tokens per second
        now_ms = int(time.time() * 1000)
        allowed, retry_ms, remaining = await self._run(
            self._key(client_id), rate, self.limit, now_ms)
        return bool(allowed), int(retry_ms) / 1000.0, int(remaining)


# ===========================================================================
# 2. FIXED WINDOW  — one counter per clock-window; resets at the boundary.
#    Flaw: a client can send `limit` near the end of one window and `limit`
#    again at the start of the next -> up to 2x limit across the boundary.
# ===========================================================================
FIXED_WINDOW_LUA = """
local key    = KEYS[1]
local limit  = tonumber(ARGV[1])
local window = tonumber(ARGV[2])      -- seconds
local now_ms = tonumber(ARGV[3])
-- which window are we in? floor(now / window). Key is namespaced per window.
local win = math.floor(now_ms / (window * 1000))
local wkey = key .. ':' .. win
local count = tonumber(redis.call('GET', wkey) or '0')
local allowed = 0
local retry_ms = 0
if count < limit then
  redis.call('INCR', wkey)
  redis.call('PEXPIRE', wkey, math.ceil(window * 1000))
  allowed = 1
  count = count + 1
else
  -- denied until this window ends
  local win_end = (win + 1) * window * 1000
  retry_ms = win_end - now_ms
end
return {allowed, retry_ms, math.max(0, limit - count)}
"""

class FixedWindow(BaseLimiter):
    NAME = "fw"
    LUA = FIXED_WINDOW_LUA
    async def allow(self, client_id):
        now_ms = int(time.time() * 1000)
        allowed, retry_ms, remaining = await self._run(
            self._key(client_id), self.limit, self.window, now_ms)
        return bool(allowed), int(retry_ms) / 1000.0, int(remaining)


# ===========================================================================
# 3. SLIDING WINDOW LOG  — store a timestamp per request in a sorted set.
#    Allow if the count within the last `window` seconds is < limit.
#    Strength: exact, no boundary burst.  Cost: one stored entry per request.
# ===========================================================================
SLIDING_LOG_LUA = """
local key    = KEYS[1]
local limit  = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local now_ms = tonumber(ARGV[3])
local cutoff = now_ms - window * 1000
-- drop timestamps older than the window
redis.call('ZREMRANGEBYSCORE', key, '-inf', cutoff)
local count = redis.call('ZCARD', key)
local allowed = 0
local retry_ms = 0
if count < limit then
  -- member must be unique; use now_ms plus a tiny random suffix
  redis.call('ZADD', key, now_ms, now_ms .. ':' .. math.random(1,1000000))
  allowed = 1
  count = count + 1
else
  -- retry when the OLDEST request in the window falls out
  local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
  if oldest[2] then
    retry_ms = math.ceil(tonumber(oldest[2]) + window * 1000 - now_ms)
  end
end
redis.call('PEXPIRE', key, math.ceil(window * 1000))
return {allowed, retry_ms, math.max(0, limit - count)}
"""

class SlidingWindowLog(BaseLimiter):
    NAME = "swl"
    LUA = SLIDING_LOG_LUA
    async def allow(self, client_id):
        now_ms = int(time.time() * 1000)
        allowed, retry_ms, remaining = await self._run(
            self._key(client_id), self.limit, self.window, now_ms)
        return bool(allowed), int(retry_ms) / 1000.0, int(remaining)


# ===========================================================================
# 4. SLIDING WINDOW COUNTER  — approximate the log with two window counts.
#    estimate = current_count + previous_count * (overlap fraction)
#    Strength: fixes the boundary burst with fixed, tiny memory.
# ===========================================================================
SLIDING_COUNTER_LUA = """
local key    = KEYS[1]
local limit  = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local now_ms = tonumber(ARGV[3])
local win_ms = window * 1000
local cur_win = math.floor(now_ms / win_ms)
local prev_win = cur_win - 1
local cur_key  = key .. ':' .. cur_win
local prev_key = key .. ':' .. prev_win
local cur  = tonumber(redis.call('GET', cur_key) or '0')
local prev = tonumber(redis.call('GET', prev_key) or '0')
-- how far we are into the current window (0.0 at start, ->1.0 at end)
local into = (now_ms % win_ms) / win_ms
-- weight the previous window by the part still overlapping the sliding window
local estimate = prev * (1 - into) + cur
local allowed = 0
local retry_ms = 0
if estimate < limit then
  redis.call('INCR', cur_key)
  redis.call('PEXPIRE', cur_key, math.ceil(win_ms * 2))
  allowed = 1
  estimate = estimate + 1
else
  retry_ms = math.ceil((1 - into) * win_ms)   -- rough: until prev weight drops
end
return {allowed, retry_ms, math.max(0, math.floor(limit - estimate))}
"""

class SlidingWindowCounter(BaseLimiter):
    NAME = "swc"
    LUA = SLIDING_COUNTER_LUA
    async def allow(self, client_id):
        now_ms = int(time.time() * 1000)
        allowed, retry_ms, remaining = await self._run(
            self._key(client_id), self.limit, self.window, now_ms)
        return bool(allowed), int(retry_ms) / 1000.0, int(remaining)


# --- registry so callers can pick by name ---
ALGORITHMS = {
    "token_bucket": TokenBucket,
    "fixed_window": FixedWindow,
    "sliding_log": SlidingWindowLog,
    "sliding_counter": SlidingWindowCounter,
}

def make_limiter(name, r, limit, window):
    if name not in ALGORITHMS:
        raise ValueError(f"unknown algorithm: {name}")
    return ALGORITHMS[name](r, limit, window)


async def _demo():
    r = aioredis.from_url(redis_url(), decode_responses=True)
    print("Each algorithm: limit=5 per 2s window. Firing 8 rapid requests.\n")
    for name in ALGORITHMS:
        lim = make_limiter(name, r, limit=5, window=2)
        await lim.reset("u")
        verdicts = []
        for _ in range(8):
            allowed, _, _ = await lim.allow("u")
            verdicts.append("A" if allowed else ".")
        await lim.reset("u")
        print(f"  {name:18} {' '.join(verdicts)}   (A=allowed, .=denied)")
    await r.aclose()


if __name__ == "__main__":
    asyncio.run(_demo())
