"""
Tests for Rate Limiter Phase 2 — requires a running Redis (REDIS_URL or localhost).
Run: python test_rl_phase2.py
"""
import asyncio
import limiter_phase2 as m


def test_exactly_k_of_n():
    """50 concurrent requests on a capacity-10 bucket -> exactly 10 allowed."""
    async def go():
        r = m.aioredis.from_url(m.redis_url(), decode_responses=True)
        lim = m.DistributedTokenBucket(r, rate=1, burst=10)
        await lim.reset("atom")
        results = await asyncio.gather(*[lim.allow("atom") for _ in range(50)])
        allowed = sum(1 for a, _, _ in results if a)
        assert allowed == 10, allowed
        await lim.reset("atom")
        await r.aclose()
    asyncio.run(go())
    print("  exactly_k_of_n (10 of 50): PASS")


def test_two_servers_share_one_limit():
    """Two separate limiter objects (two API servers) enforce ONE combined limit."""
    async def go():
        r = m.aioredis.from_url(m.redis_url(), decode_responses=True)
        a = m.DistributedTokenBucket(r, rate=1, burst=4)
        b = m.DistributedTokenBucket(r, rate=1, burst=4)
        await a.reset("shared")
        res = await asyncio.gather(*([a.allow("shared") for _ in range(10)] +
                                     [b.allow("shared") for _ in range(10)]))
        allowed = sum(1 for x, _, _ in res if x)
        assert allowed == 4, allowed
        await a.reset("shared")
        await r.aclose()
    asyncio.run(go())
    print("  two_servers_share_one_limit (4 of 20): PASS")


def test_retry_after_is_correct():
    async def go():
        r = m.aioredis.from_url(m.redis_url(), decode_responses=True)
        lim = m.DistributedTokenBucket(r, rate=2, burst=1)   # 2 tokens/sec
        await lim.reset("u")
        assert (await lim.allow("u"))[0] is True             # spend the one token
        allowed, retry, _ = await lim.allow("u")             # denied
        assert allowed is False
        assert abs(retry - 0.5) < 0.05, retry                # ~0.5s at 2/sec
        await lim.reset("u")
        await r.aclose()
    asyncio.run(go())
    print("  retry_after_is_correct (~0.5s): PASS")


def test_refill_after_wait():
    async def go():
        r = m.aioredis.from_url(m.redis_url(), decode_responses=True)
        lim = m.DistributedTokenBucket(r, rate=10, burst=3)  # fast refill for a quick test
        await lim.reset("u")
        for _ in range(3):
            await lim.allow("u")                             # drain
        assert (await lim.allow("u"))[0] is False
        await asyncio.sleep(0.35)                            # ~3.5 tokens at 10/sec
        allowed = [(await lim.allow("u"))[0] for _ in range(3)]
        assert allowed[0] is True and allowed[1] is True, allowed
        await lim.reset("u")
        await r.aclose()
    asyncio.run(go())
    print("  refill_after_wait: PASS")


def test_clients_independent():
    async def go():
        r = m.aioredis.from_url(m.redis_url(), decode_responses=True)
        lim = m.DistributedTokenBucket(r, rate=1, burst=2)
        await lim.reset("alice"); await lim.reset("bob")
        assert (await lim.allow("alice"))[0] is True
        assert (await lim.allow("alice"))[0] is True
        assert (await lim.allow("alice"))[0] is False        # alice drained
        assert (await lim.allow("bob"))[0] is True           # bob unaffected
        await lim.reset("alice"); await lim.reset("bob")
        await r.aclose()
    asyncio.run(go())
    print("  clients_independent: PASS")


def test_remaining_reported():
    async def go():
        r = m.aioredis.from_url(m.redis_url(), decode_responses=True)
        lim = m.DistributedTokenBucket(r, rate=1, burst=5)
        await lim.reset("u")
        _, _, rem = await lim.allow("u")
        assert rem == 4, rem                                 # 5 - 1 spent
        await lim.reset("u")
        await r.aclose()
    asyncio.run(go())
    print("  remaining_reported: PASS")


if __name__ == "__main__":
    print("Running Rate Limiter Phase 2 tests (needs Redis):")
    test_exactly_k_of_n()
    test_two_servers_share_one_limit()
    test_retry_after_is_correct()
    test_refill_after_wait()
    test_clients_independent()
    test_remaining_reported()
    print("All tests passed.")
