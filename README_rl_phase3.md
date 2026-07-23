# Rate Limiter Service — Phase 3

Four classic rate-limiting algorithms behind one common interface, each
implemented as a single **atomic Lua script**. Swap algorithms with one
parameter. This is where the standalone service goes beyond the crawler.

## Files
- `limiter_phase3.py` — the four algorithms + a registry + a demo
- `test_rl_phase3.py` — tests (need a running Redis)

## Common interface
```python
lim = make_limiter("sliding_counter", r, limit=100, window=60)  # 100 req / 60s
allowed, retry_after, remaining = await lim.allow("user-1")
```
`limit` = requests allowed per `window` seconds.

## The four algorithms

| Algorithm | Strength | Flaw / cost | Use when |
|---|---|---|---|
| **token_bucket** | smooth rate + controlled burst | burst can briefly exceed steady rate | you want to permit short bursts |
| **fixed_window** | simplest, tiny memory | **boundary burst**: up to 2x at window edges | rough limiting, simplicity matters |
| **sliding_log** | exact, no boundary burst | memory grows with request volume (one entry each) | exactness matters, volume is modest |
| **sliding_counter** | near-exact, fixed tiny memory | a small approximation | the usual production default |

### How each works (briefly)
- **Token bucket** — tokens refill at `limit/window` per second up to a cap of
  `limit`; each request spends one.
- **Fixed window** — count requests in the current clock-window; reset at the
  boundary. Simple, but a client can spend the full limit at the end of one
  window and again at the start of the next.
- **Sliding window log** — store a timestamp per request in a sorted set; allow
  if the count within the last `window` seconds is under the limit. Exact, but
  one stored entry per request.
- **Sliding window counter** — estimate = current-window count + previous-window
  count × (fraction of the previous window still overlapping). Fixes the
  boundary burst with two small counters instead of a full log.

## The headline result: the boundary burst
The test `test_boundary_burst_behavior` sends the full limit just before a
window edge and the full limit just after. Measured (limit=5):

```
  fixed_window     allowed 10   <- the flaw: 2x the limit across the edge
  sliding_log      allowed 5    <- exact
  sliding_counter  allowed 6    <- cheap approximation, nearly as good
  token_bucket     allowed 6    <- smoothed by refill
```
This contrast is the centerpiece of the project and the most common
rate-limiter interview topic. Phase 5 turns it into a chart.

## Run
```bash
pip install redis
export REDIS_URL="rediss://...upstash.io:6379"   # or local
python limiter_phase3.py        # demo: all four on the same traffic
python test_rl_phase3.py        # tests, incl. the boundary-burst proof
```

## Interview notes
- **All four are atomic** — each Lua script does its whole read-decide-write in
  one uninterrupted step, so all four pass the "exactly K of N concurrent" test.
- **Picking one**: token bucket for controlled bursts; sliding counter as the
  balanced default; sliding log when you need exactness and volume is low; fixed
  window only when simplicity outweighs the boundary flaw.
- **Memory**: log is O(requests) per client; the other three are O(1) per client.

## Still deferred
- HTTP API with 429s + rate-limit headers → **Phase 4**
- Algorithm-comparison benchmark chart → **Phase 5**
