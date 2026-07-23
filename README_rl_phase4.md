# Rate Limiter Service — Phase 4

A real HTTP service (FastAPI) wrapping the four algorithms. Other systems call
it over HTTP to ask "is this client allowed?" and get back proper status codes
and the standard rate-limit headers. This is the piece that makes it a
**service**, not just a library.

## Files
- `service.py` — the FastAPI app
- `demo_client.py` — a small client that hammers the service (and is the Phase 5
  traffic generator)
- `test_rl_phase4.py` — tests (need a running Redis)

## Endpoints
- `GET  /health` → `{"status":"ok"}` (checks Redis too)
- `GET  /algorithms` → the supported algorithm names
- `POST /check` → **200** allowed / **429** denied
  - body: `{client_id, limit, window, algorithm?, cost?}`
  - response headers: `X-RateLimit-Limit`, `X-RateLimit-Remaining`, and
    `Retry-After` (on 429)
- `POST /reset` → `{client_id, algorithm}` clears a client's state (for testing)

## Why these specifics
- **429 Too Many Requests** is the universal "you've been rate-limited" status.
- **X-RateLimit-Remaining / Retry-After** are the de-facto standard headers
  real APIs (GitHub, Twitter, Stripe) send, so clients know how many calls are
  left and how long to back off.
- **Pydantic validation**: `limit`/`window` must be > 0, enforced automatically
  (bad input → 422 with no code of ours).

## Run
```bash
pip install fastapi "uvicorn[standard]" httpx redis
export REDIS_URL="rediss://...upstash.io:6379"   # or local

uvicorn service:app --reload --port 8000
```
Then, in another terminal/cell:
```bash
python demo_client.py
# or call it directly:
curl -X POST localhost:8000/check -H 'content-type: application/json' \
     -d '{"client_id":"user-1","limit":5,"window":2,"algorithm":"token_bucket"}'
```

A denied response looks like:
```
HTTP/1.1 429 Too Many Requests
X-RateLimit-Limit: 3
X-RateLimit-Remaining: 0
Retry-After: 1.5
{"allowed": false, "retry_after": 1.5, "remaining": 0}
```

## Test
```bash
python test_rl_phase4.py
```
Tests drive the real app in-process via httpx's ASGI transport (no separate
server needed) and cover: health, allow-then-429, the rate-limit headers,
unknown-algorithm 400, bad-input 422, and the algorithms list.

## Notes for Hugging Face Spaces
- Add a `requirements.txt`: fastapi, uvicorn, redis, httpx.
- Set `REDIS_URL` as a Space secret (your Upstash address).
- Start command: `uvicorn service:app --host 0.0.0.0 --port 7860`.

## Design notes (interview)
- **Limiter caching**: one limiter object per (algorithm, limit, window) is
  cached, so we don't re-upload the Lua script on every request.
- **Connection lifecycle**: the Redis connection opens on startup and closes on
  shutdown via FastAPI's `lifespan`, rather than per-request.
- **Stateless API, shared Redis**: run many copies of this server behind a load
  balancer; they all enforce one consistent limit because the state is in Redis.

## Still deferred
- The algorithm-comparison benchmark chart → **Phase 5** (the finale)
