"""
Rate Limiter Service — Phase 4: HTTP API (FastAPI).

Turns the library from Phase 3 into a real SERVICE other systems call over
HTTP. A caller asks "is client X allowed?" and gets back allowed/denied plus
the standard rate-limit headers that real APIs use.

Endpoints:
  GET  /health                      -> {"status": "ok"}
  POST /check                       -> 200 allowed / 429 denied
       body: {client_id, limit, window, algorithm?, cost?}
       headers on response: X-RateLimit-Limit, X-RateLimit-Remaining,
                            Retry-After (only when denied)
  POST /reset  {client_id, algorithm} -> clears a client's state (for testing)

Run the server:
    uvicorn service:app --reload --port 8000
  (or: python service.py)

Then call it:
    curl -X POST localhost:8000/check \
         -H 'content-type: application/json' \
         -d '{"client_id":"user-1","limit":5,"window":2}'
"""

import os
from contextlib import asynccontextmanager

import redis.asyncio as aioredis
from fastapi import FastAPI, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

import limiter_phase3 as lim  # reuse the four algorithms from Phase 3


def redis_url() -> str:
    return os.environ.get("REDIS_URL", "redis://localhost:6379")


# A small cache of limiter objects, keyed by (algorithm, limit, window), so we
# don't rebuild (and re-upload the Lua script) on every request.
_limiters: dict = {}
_redis: aioredis.Redis | None = None


def get_limiter(algorithm: str, limit: float, window: float):
    key = (algorithm, limit, window)
    if key not in _limiters:
        _limiters[key] = lim.make_limiter(algorithm, _redis, limit, window)
    return _limiters[key]


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Open the Redis connection when the server starts, close it on shutdown.
    global _redis
    _redis = aioredis.from_url(redis_url(), decode_responses=True)
    yield
    await _redis.aclose()


app = FastAPI(title="Rate Limiter Service", lifespan=lifespan)


# ---- request/response shapes (validated automatically by FastAPI) ----
class CheckRequest(BaseModel):
    client_id: str
    limit: float = Field(gt=0, description="requests allowed per window")
    window: float = Field(gt=0, description="window length in seconds")
    algorithm: str = "token_bucket"
    cost: float = Field(default=1.0, gt=0, description="tokens this request costs")


class ResetRequest(BaseModel):
    client_id: str
    algorithm: str = "token_bucket"


@app.get("/health")
async def health():
    try:
        await _redis.ping()
        return {"status": "ok"}
    except Exception as e:
        return JSONResponse(status_code=503, content={"status": "redis_unreachable", "detail": str(e)})


@app.get("/algorithms")
async def algorithms():
    """List the supported algorithm names."""
    return {"algorithms": list(lim.ALGORITHMS.keys())}


@app.post("/check")
async def check(req: CheckRequest, response: Response):
    if req.algorithm not in lim.ALGORITHMS:
        return JSONResponse(status_code=400,
                            content={"error": f"unknown algorithm '{req.algorithm}'",
                                     "supported": list(lim.ALGORITHMS.keys())})

    limiter = get_limiter(req.algorithm, req.limit, req.window)

    # token bucket supports a per-request cost; others ignore it
    if req.algorithm == "token_bucket" and req.cost != 1.0:
        # token bucket's allow() takes only client_id in Phase 3's interface,
        # so for costs != 1 we fall back to calling allow() cost times.
        allowed_all = True
        last = (True, 0.0, 0)
        for _ in range(int(req.cost)):
            last = await limiter.allow(req.client_id)
            if not last[0]:
                allowed_all = False
                break
        allowed, retry_after, remaining = (allowed_all, last[1], last[2])
    else:
        allowed, retry_after, remaining = await limiter.allow(req.client_id)

    # standard rate-limit headers every good limiter returns
    response.headers["X-RateLimit-Limit"] = str(int(req.limit))
    response.headers["X-RateLimit-Remaining"] = str(max(0, remaining))

    if allowed:
        return {"allowed": True, "remaining": max(0, remaining)}

    # 429 = "Too Many Requests" — the universal rate-limited signal
    response.headers["Retry-After"] = str(round(retry_after, 2))
    return JSONResponse(
        status_code=429,
        content={"allowed": False, "retry_after": round(retry_after, 2),
                 "remaining": 0},
        headers={
            "X-RateLimit-Limit": str(int(req.limit)),
            "X-RateLimit-Remaining": "0",
            "Retry-After": str(round(retry_after, 2)),
        },
    )


@app.post("/reset")
async def reset(req: ResetRequest):
    if req.algorithm not in lim.ALGORITHMS:
        return JSONResponse(status_code=400, content={"error": "unknown algorithm"})
    limiter = get_limiter(req.algorithm, 1, 1)   # limit/window irrelevant for reset
    await limiter.reset(req.client_id)
    return {"reset": req.client_id, "algorithm": req.algorithm}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
