"""
Tests for Rate Limiter Phase 4 — the HTTP service. Needs a running Redis.
Uses httpx ASGITransport to call the real FastAPI app in-process (no server).
Run: python test_rl_phase4.py
"""
import asyncio
import httpx
import service


async def client():
    # drive the real app directly through ASGI; lifespan opens/closes Redis
    transport = httpx.ASGITransport(app=service.app)
    return httpx.AsyncClient(transport=transport, base_url="http://test")


async def with_app(coro):
    # run the app's lifespan (so _redis is opened) around a test body
    async with service.app.router.lifespan_context(service.app):
        async with await client() as c:
            await coro(c)


def test_health():
    async def body(c):
        r = await c.get("/health")
        assert r.status_code == 200, r.status_code
        assert r.json()["status"] == "ok"
    asyncio.run(with_app(body))
    print("  health: PASS")


def test_allows_then_429():
    async def body(c):
        await c.post("/reset", json={"client_id": "h1", "algorithm": "fixed_window"})
        # limit 3 per big window -> first 3 ok, 4th is 429
        codes = []
        for _ in range(4):
            r = await c.post("/check", json={"client_id": "h1", "limit": 3,
                                             "window": 100, "algorithm": "fixed_window"})
            codes.append(r.status_code)
        assert codes == [200, 200, 200, 429], codes
        await c.post("/reset", json={"client_id": "h1", "algorithm": "fixed_window"})
    asyncio.run(with_app(body))
    print("  allows_then_429 (200,200,200,429): PASS")


def test_headers_present():
    async def body(c):
        await c.post("/reset", json={"client_id": "h2", "algorithm": "fixed_window"})
        r1 = await c.post("/check", json={"client_id": "h2", "limit": 2,
                                          "window": 100, "algorithm": "fixed_window"})
        assert r1.headers["X-RateLimit-Limit"] == "2"
        assert r1.headers["X-RateLimit-Remaining"] == "1"   # 2 - 1
        # exhaust, then check 429 carries Retry-After
        await c.post("/check", json={"client_id": "h2", "limit": 2, "window": 100,
                                     "algorithm": "fixed_window"})
        r3 = await c.post("/check", json={"client_id": "h2", "limit": 2, "window": 100,
                                          "algorithm": "fixed_window"})
        assert r3.status_code == 429
        assert "Retry-After" in r3.headers
        assert r3.headers["X-RateLimit-Remaining"] == "0"
        await c.post("/reset", json={"client_id": "h2", "algorithm": "fixed_window"})
    asyncio.run(with_app(body))
    print("  headers_present (limit/remaining/retry-after): PASS")


def test_unknown_algorithm_400():
    async def body(c):
        r = await c.post("/check", json={"client_id": "x", "limit": 5, "window": 1,
                                         "algorithm": "nope"})
        assert r.status_code == 400, r.status_code
        assert "supported" in r.json()
    asyncio.run(with_app(body))
    print("  unknown_algorithm_400: PASS")


def test_bad_input_422():
    async def body(c):
        # limit must be > 0; FastAPI/pydantic rejects with 422 automatically
        r = await c.post("/check", json={"client_id": "x", "limit": 0, "window": 1})
        assert r.status_code == 422, r.status_code
    asyncio.run(with_app(body))
    print("  bad_input_422 (validation): PASS")


def test_algorithms_listed():
    async def body(c):
        r = await c.get("/algorithms")
        assert r.status_code == 200
        names = r.json()["algorithms"]
        assert "token_bucket" in names and "sliding_counter" in names
    asyncio.run(with_app(body))
    print("  algorithms_listed: PASS")


if __name__ == "__main__":
    print("Running Rate Limiter Phase 4 tests (needs Redis):")
    test_health()
    test_allows_then_429()
    test_headers_present()
    test_unknown_algorithm_400()
    test_bad_input_422()
    test_algorithms_listed()
    print("All tests passed.")
