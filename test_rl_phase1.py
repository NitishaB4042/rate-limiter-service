"""
Tests for Rate Limiter Phase 1 — no Redis, no network, no real waiting.

We inject a FAKE CLOCK so we can advance time instantly and deterministically.
Run: python test_phase1.py
"""

from limiter_phase1 import TokenBucketLimiter


class FakeClock:
    """A clock we control by hand, so tests don't depend on real time."""
    def __init__(self, start: float = 1000.0):
        self.t = start

    def now(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


def make_limiter(rate, burst):
    """Build a limiter whose _now() reads our fake clock."""
    clock = FakeClock()
    lim = TokenBucketLimiter(rate=rate, burst=burst)
    lim._now = clock.now          # swap in the fake clock
    return lim, clock


def test_fresh_bucket_allows_full_burst():
    lim, clock = make_limiter(rate=2.0, burst=5.0)
    allowed = [lim.allow("u")[0] for _ in range(5)]
    assert allowed == [True] * 5, allowed       # capacity 5 -> 5 allowed
    assert lim.allow("u")[0] is False           # 6th denied
    print("  fresh_bucket_allows_full_burst: PASS")


def test_deny_reports_correct_retry():
    lim, clock = make_limiter(rate=2.0, burst=1.0)
    assert lim.allow("u")[0] is True            # spend the only token
    allowed, retry = lim.allow("u")             # now empty
    assert allowed is False
    # at 2 tokens/sec, one token takes 0.5s
    assert abs(retry - 0.5) < 1e-6, retry
    print("  deny_reports_correct_retry: PASS")


def test_refill_over_time():
    lim, clock = make_limiter(rate=2.0, burst=5.0)
    for _ in range(5):                          # drain the bucket
        lim.allow("u")
    assert lim.allow("u")[0] is False           # empty now
    clock.advance(1.0)                          # 1 second -> +2 tokens
    assert lim.allow("u")[0] is True
    assert lim.allow("u")[0] is True
    assert lim.allow("u")[0] is False           # only 2 had refilled
    print("  refill_over_time: PASS")


def test_refill_capped_at_burst():
    lim, clock = make_limiter(rate=2.0, burst=5.0)
    for _ in range(5):                          # drain
        lim.allow("u")
    clock.advance(100.0)                        # huge wait
    # tokens must cap at burst (5), not 200
    allowed = [lim.allow("u")[0] for _ in range(6)]
    assert allowed == [True]*5 + [False], allowed
    print("  refill_capped_at_burst: PASS")


def test_clients_are_independent():
    lim, clock = make_limiter(rate=1.0, burst=2.0)
    assert lim.allow("alice")[0] is True
    assert lim.allow("alice")[0] is True
    assert lim.allow("alice")[0] is False       # alice exhausted
    # bob has his own full bucket
    assert lim.allow("bob")[0] is True
    assert lim.allow("bob")[0] is True
    print("  clients_are_independent: PASS")


def test_remaining_reports_tokens():
    lim, clock = make_limiter(rate=1.0, burst=3.0)
    assert lim.remaining("u") == 3              # fresh
    lim.allow("u")
    assert lim.remaining("u") == 2
    print("  remaining_reports_tokens: PASS")


def test_rejects_bad_config():
    for rate, burst in [(0, 5), (5, 0), (-1, 5)]:
        try:
            TokenBucketLimiter(rate=rate, burst=burst)
            assert False, "should have raised"
        except ValueError:
            pass
    print("  rejects_bad_config: PASS")


if __name__ == "__main__":
    print("Running Rate Limiter Phase 1 tests:")
    test_fresh_bucket_allows_full_burst()
    test_deny_reports_correct_retry()
    test_refill_over_time()
    test_refill_capped_at_burst()
    test_clients_are_independent()
    test_remaining_reports_tokens()
    test_rejects_bad_config()
    print("All tests passed.")
