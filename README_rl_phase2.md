# Rate Limiter Service — Phase 2

Distributed token bucket: state in **Redis**, the decision in an **atomic Lua
script**. Now many API servers share one consistent limit per client, and the
check is correct under heavy concurrency.

## Files
- `limiter_phase2.py` — the distributed limiter + a demo
- `test_rl_phase2.py` — tests (need a running Redis)

## What changed from Phase 1
| Phase 1 | Phase 2 |
|---|---|
| bucket state in a Python dict | bucket state in Redis (one HASH per client) |
| correct for ONE process only | correct across MANY processes / API servers |
| refill+take in Python | refill+take in one **atomic Lua script** |
| no concurrency guarantee | proven "exactly K of N" under concurrent load |

## Why the Lua script
The decision is read-modify-write: read the token count, decide, write the new
count. If two requests do that as separate steps, both can read "1 left," both
decide "ok," and both proceed — 2 requests for a budget of 1. Redis runs a Lua
script **start to finish without interruption**, so the whole decision is one
atomic step and the race is impossible. The script also auto-expires idle
clients so we don't keep state forever for one-off callers.

## Setup
```bash
pip install redis
export REDIS_URL="rediss://default:<password>@<host>.upstash.io:6379"  # or local
```
(Upstash/Redis Cloud free tier works on a tablet — no install.)

## Run the demo
```bash
python limiter_phase2.py
```
Same behavior as Phase 1 (5 allowed, 3 denied, refill, 3 more) — but now the
state lives in Redis, so the limit holds across processes.

## Run the tests
```bash
python test_rl_phase2.py
```
Headline tests:
- **exactly_k_of_n** — 50 concurrent requests on a capacity-10 bucket grant
  **exactly 10**. This is the proof the Lua script is race-free.
- **two_servers_share_one_limit** — two separate limiter objects (simulating
  two API servers) pointed at the same Redis enforce **one combined** limit
  (4 of 20), proving the limit is global, not per-server.

## API
```python
limiter = DistributedTokenBucket(r, rate=2.0, burst=5.0)
allowed, retry_after, remaining = await limiter.allow("user-1")
```
`remaining` is the whole tokens left — it feeds the `X-RateLimit-Remaining`
header in Phase 4.

## Redis keys
- `rl:bucket:{client_id}` — HASH `{tokens, ts}`, the per-client bucket
  (auto-expires after an idle period)

Change `RL_NS` to run isolated limiters side by side.

## Interview notes
- **Atomicity**: the entire refill-and-take is one Lua script; concurrent
  callers are serialized by Redis, so the limit is never overshot.
- **State in Redis, not the server**: that's what makes the limit consistent
  no matter how many API servers you run — and lets you add servers freely.
- **Scaling the limiter itself**: the bottleneck becomes Redis; shard clients
  across Redis instances by hashing the client_id so no single Redis is hot.

## Still deferred
- Other algorithms (fixed window, sliding window log/counter) → **Phase 3**
- HTTP API with 429s and rate-limit headers → **Phase 4**
- Algorithm-comparison benchmark → **Phase 5**
