# Rate Limiter Service — Phase 5 (Final)

The benchmark and comparison charts. This phase adds no new limiting ability —
it **visualizes the tradeoffs** between the four algorithms, which is the most
persuasive thing you can show for this project.

## Files
- `benchmark_rl.py` — runs two experiments, writes two charts + JSON
- `rl_boundary.png` — the headline chart (boundary burst)
- `rl_acceptance.png` — acceptance under steady overload
- `requirements_rl.txt` — install list for the whole service

## Run
```bash
pip install -r requirements_rl.txt
export REDIS_URL="rediss://...upstash.io:6379"   # or local
python benchmark_rl.py
```

## Experiment 1 — the boundary burst (the headline)
Fire a burst of `limit` requests just before a window edge, then `limit` more
just after. A correct ~1-second sliding limit should allow ~10 total; a flawed
one allows ~20. Measured (limit = 10 per 1s):

```
  token_bucket     total 12     (refill smooths it)
  fixed_window     total 20     <-- 2x boundary burst (the flaw)
  sliding_log      total 10     (exact)
  sliding_counter  total 11     (cheap approximation, nearly exact)
```

`rl_boundary.png` shows fixed_window's total bar shooting to 20 while the
others hug the intended-limit line. **This single chart is the centerpiece of
the project** — it makes a design tradeoff visible instead of abstract.

## Experiment 2 — acceptance under steady overload
Offer 60 requests/sec at a 10/sec limit and measure the fraction accepted. All
four converge near the ideal (~17%), because under *steady* load they behave
similarly. The point: the algorithms differ at **boundaries and bursts**, not
under smooth traffic — which is exactly why you pick based on burst behavior.

## How to read the comparison (interview)
- **Fixed window** is simplest but allows up to 2x the limit across a window
  edge — the boundary burst, shown live in `rl_boundary.png`.
- **Sliding window log** eliminates it exactly, at the cost of one stored
  timestamp per request (memory grows with traffic).
- **Sliding window counter** approximates the log with two small counters —
  nearly as accurate, fixed memory. The usual production default.
- **Token bucket** smooths bursts via refill and is ideal when you want to
  permit a controlled burst above the steady rate.

> Honesty note (like the crawler's benchmark): under steady load the algorithms
> look the same; the interesting differences only appear at edges. Showing both
> charts — "same under steady load, very different at boundaries" — is a more
> credible story than a single cherry-picked result.

## The complete service, recapped
| Phase | Adds | Key idea |
|-------|------|----------|
| 1 | in-memory token bucket | the refill-and-take math |
| 2 | Redis + atomic Lua | correct under concurrency (exactly K of N) |
| 3 | four algorithms | the accuracy/memory tradeoff |
| 4 | FastAPI HTTP service | 429s + standard rate-limit headers |
| 5 | benchmark + charts | the boundary-burst comparison |
