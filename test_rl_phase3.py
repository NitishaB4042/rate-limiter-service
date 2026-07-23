"""
Tests for Rate Limiter Phase 3 — four algorithms. Needs a running Redis.
Run: python test_rl_phase3.py
"""
import asyncio
import time
import limiter_phase3 as m


def test_all_algorithms_atomic():
    """Every algorithm: 50 concurrent requests on limit 10 -> exactly 10 allowed."""
    async def go():
        r = m.aioredis.from_url(m.redis_url(), decode_responses=True)
        for name in m.ALGORITHMS:
            lim = m.make_limiter(name, r, limit=10, window=5)
            await lim.reset("atom")
            res = await asyncio.gather(*[lim.allow("atom") for _ in range(50)])
            allowed = sum(1 for a, _, _ in res if a)
            assert allowed == 10, (name, allowed)
            await lim.reset("atom")
        await r.aclose()
    asyncio.run(go())
    print("  all_algorithms_atomic (10 of 50 each): PASS")


def test_all_allow_then_deny():
    """Baseline: limit=5 -> first 5 allowed, rest denied, for every algorithm."""
    async def go():
        r = m.aioredis.from_url(m.redis_url(), decode_responses=True)
        for name in m.ALGORITHMS:
            lim = m.make_limiter(name, r, limit=5, window=10)
            await lim.reset("u")
            verdicts = []
            for _ in range(8):
                ok, _, _ = await lim.allow("u")
                verdicts.append(ok)
            assert verdicts[:5] == [True]*5, (name, verdicts)
            assert verdicts[5:] == [False]*3, (name, verdicts)
            await lim.reset("u")
        await r.aclose()
    asyncio.run(go())
    print("  all_allow_then_deny (5 then deny): PASS")


async def _burst(lim, n):
    c = 0
    for _ in range(n):
        ok, _, _ = await lim.allow("b")
        if ok:
            c += 1
    return c


async def _boundary(r, name, limit=5, window=1.0):
    lim = m.make_limiter(name, r, limit=limit, window=window)
    await lim.reset("b")
    win_ms = window * 1000
    while True:
        into = ((time.time() * 1000) % win_ms) / win_ms
        if 0.80 <= into <= 0.90:
            break
        await asyncio.sleep(0.005)
    a1 = await _burst(lim, limit)
    await asyncio.sleep(window * 0.25)
    a2 = await _burst(lim, limit)
    await lim.reset("b")
    return a1 + a2


def test_boundary_burst_behavior():
    """The headline difference: fixed window allows ~2x at a window edge;
    sliding variants hold near the limit."""
    async def go():
        r = m.aioredis.from_url(m.redis_url(), decode_responses=True)
        fixed = await _boundary(r, "fixed_window")
        log = await _boundary(r, "sliding_log")
        counter = await _boundary(r, "sliding_counter")
        # fixed window lets through clearly more than the limit (the flaw)
        assert fixed >= 9, fixed
        # sliding log is exact: never more than the limit across the span
        assert log <= 5, log
        # sliding counter approximates the log: much better than fixed window
        assert counter <= 7, counter
        assert counter < fixed, (counter, fixed)
        await r.aclose()
    asyncio.run(go())
    print("  boundary_burst_behavior (fixed~10, log~5, counter~6): PASS")


def test_unknown_algorithm_rejected():
    async def go():
        r = m.aioredis.from_url(m.redis_url(), decode_responses=True)
        try:
            m.make_limiter("nonsense", r, 5, 1)
            assert False, "should have raised"
        except ValueError:
            pass
        await r.aclose()
    asyncio.run(go())
    print("  unknown_algorithm_rejected: PASS")


if __name__ == "__main__":
    print("Running Rate Limiter Phase 3 tests (needs Redis):")
    test_all_algorithms_atomic()
    test_all_allow_then_deny()
    test_boundary_burst_behavior()
    test_unknown_algorithm_rejected()
    print("All tests passed.")
