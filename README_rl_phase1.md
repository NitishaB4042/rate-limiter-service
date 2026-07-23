# Rate Limiter Service — Phase 1

In-memory token bucket. Single process, no Redis, no network. The goal of this
phase is to get the token-bucket **math** right in isolation, with fast,
deterministic tests. This is the standalone, cleaned-up version of the limiter
you built inside the crawler.

## Files
- `limiter_phase1.py` — the token bucket + a runnable demo
- `test_rl_phase1.py` — tests (no Redis, no real waiting)

## The idea
A client gets a "bucket" of tokens. Each request spends one token. The bucket
refills at a steady **rate** (tokens/sec) up to a **burst** capacity. If the
bucket is empty, the request is denied and we report how long to wait.

- `rate`  = sustained allowed requests per second
- `burst` = largest momentary spike allowed (the bucket's capacity)

```python
limiter = TokenBucketLimiter(rate=2.0, burst=5.0)
allowed, retry_after = limiter.allow("user-1")
```

## Run the demo
```bash
python limiter_phase1.py
```
It fires 8 rapid requests at a 2/sec bucket (capacity 5): the first 5 are
allowed (the burst), the next 3 are denied with a 0.5s retry. After waiting
1.5s (~3 tokens refill), 3 more are allowed.

## Run the tests
```bash
python test_rl_phase1.py
```
Tests use a **fake clock** so time can be advanced instantly — no real sleeping,
fully deterministic. They cover: full-burst allowance, correct retry math,
refill over time, the refill cap, per-client independence, the remaining count,
and rejecting bad config.

## Design notes (for the interview)
- **Lazy refill**: we don't run a background timer adding tokens. Instead, on
  each request we compute how many tokens *would* have accrued since the last
  check and add them (capped at burst). Simpler and exact.
- **`time.monotonic()`** is used (not `time.time()`) because it never goes
  backwards if the system clock is adjusted — important for correct elapsed-time
  math.
- **`_now()` is wrapped** so tests can inject a fake clock. Designing for
  testability up front is a good habit to show.

## What's deliberately NOT here yet
- Shared state across processes → **Phase 2** (move to Redis + atomic Lua)
- The "exactly K of N concurrent" atomicity proof → **Phase 2**
- Other algorithms (fixed/sliding window) → **Phase 3**
- HTTP API → **Phase 4**
- Algorithm comparison benchmark → **Phase 5**

> Note: this in-memory version is correct for ONE process only. Two processes
> would each keep their own buckets and enforce the limit twice. Fixing that is
> exactly what Phase 2 does by moving state into shared Redis.
