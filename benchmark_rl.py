"""
Rate Limiter Service — Phase 5: benchmark & comparison chart.

Fires controlled traffic patterns at each algorithm and charts how they behave
DIFFERENTLY. Unlike a raw speed number, this visualizes a design TRADEOFF \u2014
the headline being the "boundary burst": fixed window lets ~2x the limit through
at a window edge, while the sliding variants hold near the limit.

Run (needs Redis; set REDIS_URL or use localhost):
    python benchmark_rl.py

Outputs:
    rl_boundary.png        \u2014 allowed-per-instant across a window boundary
    rl_acceptance.png      \u2014 acceptance rate under steady overload
    rl_benchmark_results.json
"""

import os
import time
import json
import asyncio

import redis.asyncio as aioredis
import limiter_phase3 as m

ALGOS = ["token_bucket", "fixed_window", "sliding_log", "sliding_counter"]
COLORS = {
    "token_bucket": "#2E75B6",
    "fixed_window": "#C0392B",
    "sliding_log": "#375623",
    "sliding_counter": "#B9770E",
}


def redis_url():
    return os.environ.get("REDIS_URL", "redis://localhost:6379")


# ---------------------------------------------------------------------------
# Experiment 1 — the boundary burst (the classic demonstration).
# Send a BURST of `limit` requests just before a window edge, then another
# BURST of `limit` just after. Plot how many were allowed in each burst.
# Fixed window allows BOTH bursts in full (2x); the sliding variants don't.
# ---------------------------------------------------------------------------
async def boundary_bursts(r, algo, limit=10, window=1.0):
    """Return (allowed_before, allowed_after) for two edge-straddling bursts."""
    lim = m.make_limiter(algo, r, limit=limit, window=window)
    await lim.reset("b")
    win_ms = window * 1000

    # wait until we're ~85% into a window (just before the edge)
    while True:
        into = ((time.time() * 1000) % win_ms) / win_ms
        if 0.82 <= into <= 0.90:
            break
        await asyncio.sleep(0.002)

    # burst 1: fire `limit` requests as fast as possible, just before the edge
    before = 0
    for _ in range(limit):
        ok, _, _ = await lim.allow("b")
        before += 1 if ok else 0

    # cross the boundary into the next window
    await asyncio.sleep(window * 0.25)

    # burst 2: fire `limit` more, just after the edge
    after = 0
    for _ in range(limit):
        ok, _, _ = await lim.allow("b")
        after += 1 if ok else 0

    await lim.reset("b")
    return before, after


# ---------------------------------------------------------------------------
# Experiment 2 — acceptance rate under steady overload.
# Send well above the limit at a constant rate; measure the fraction allowed.
# A correct limiter should accept ~ limit/window of the offered load.
# ---------------------------------------------------------------------------
async def acceptance_under_overload(r, algo, limit=10, window=1.0,
                                    duration=3.0, rps=60):
    lim = m.make_limiter(algo, r, limit=limit, window=window)
    await lim.reset("o")
    interval = 1.0 / rps
    sent = 0
    allowed = 0
    n = int(duration * rps)
    for _ in range(n):
        ok, _, _ = await lim.allow("o")
        sent += 1
        allowed += 1 if ok else 0
        await asyncio.sleep(interval)
    await lim.reset("o")
    # ideal acceptance fraction = capacity per second / offered per second
    ideal = (limit / window) / rps
    return allowed, sent, allowed / sent, ideal


async def main():
    r = aioredis.from_url(redis_url(), decode_responses=True)
    results = {}

    # ---- Experiment 1: boundary bursts ----
    print("Experiment 1: boundary burst (limit=10 per 1s)")
    print("  Fire 10 just before the window edge, 10 just after.")
    print("  A correct ~1s sliding limit should allow ~10 total; fixed window allows ~20.\n")
    bursts = {}
    for algo in ALGOS:
        before, after = await boundary_bursts(r, algo)
        bursts[algo] = (before, after)
        total = before + after
        flag = "  <-- 2x boundary burst!" if total >= 18 else ""
        print(f"  {algo:18} before={before:2d}  after={after:2d}  total={total:2d}{flag}")
        results.setdefault(algo, {})["boundary_before"] = before
        results[algo]["boundary_after"] = after
        results[algo]["boundary_total"] = total

    # ---- Experiment 2: acceptance under overload ----
    print("\nExperiment 2: acceptance under steady overload (offered 60 rps, limit 10/s)\n")
    for algo in ALGOS:
        allowed, sent, frac, ideal = await acceptance_under_overload(r, algo)
        print(f"  {algo:18} accepted {allowed}/{sent} = {frac*100:.0f}%  (ideal ~{ideal*100:.0f}%)")
        results[algo]["acceptance"] = round(frac, 3)
        results[algo]["ideal_acceptance"] = round(ideal, 3)

    await r.aclose()

    # ---- write results ----
    with open("rl_benchmark_results.json", "w") as f:
        json.dump(results, f, indent=2)
    print("\nwrote rl_benchmark_results.json")

    # ---- chart 1: boundary timeline ----
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        import numpy as np
        fig, ax = plt.subplots(figsize=(8.2, 4.8))
        x = np.arange(len(ALGOS))
        w = 0.27
        before_vals = [bursts[a][0] for a in ALGOS]
        after_vals = [bursts[a][1] for a in ALGOS]
        total_vals = [bursts[a][0] + bursts[a][1] for a in ALGOS]
        ax.bar(x - w, before_vals, w, label="burst before edge", color="#9DC3E6")
        ax.bar(x,     after_vals,  w, label="burst after edge",  color="#F4B183")
        ax.bar(x + w, total_vals,  w, label="total across edge", color="#C0392B", alpha=0.9)
        ax.axhline(10, ls="--", color="#375623", linewidth=1.6,
                   label="intended limit (10)")
        ax.set_xticks(x)
        ax.set_xticklabels(ALGOS, rotation=12)
        ax.set_ylabel("requests allowed")
        ax.set_title("Boundary burst: two bursts of 10 straddling a window edge\n"
                     "Fixed window allows ~20 (2x); sliding variants hold near 10")
        ax.legend(fontsize=9)
        for xi, tv in zip(x, total_vals):
            ax.text(xi + w, tv + 0.3, str(tv), ha="center", fontsize=10, fontweight="bold")
        fig.tight_layout()
        fig.savefig("rl_boundary.png", dpi=130)
        print("wrote rl_boundary.png")

        # ---- chart 2: acceptance bar chart ----
        fig2, ax2 = plt.subplots(figsize=(8, 4.4))
        accs = [results[a]["acceptance"] * 100 for a in ALGOS]
        bars = ax2.bar(ALGOS, accs, color=[COLORS[a] for a in ALGOS], alpha=0.85)
        ideal_pct = results[ALGOS[0]]["ideal_acceptance"] * 100
        ax2.axhline(ideal_pct, ls="--", color="#7F7F7F",
                    label=f"ideal ~{ideal_pct:.0f}%")
        ax2.set_ylabel("requests accepted (%)")
        ax2.set_title("Acceptance under steady overload (offered 60 rps, limit 10/s)")
        ax2.legend()
        for b, v in zip(bars, accs):
            ax2.text(b.get_x() + b.get_width()/2, v + 1, f"{v:.0f}%",
                     ha="center", fontsize=10)
        fig2.tight_layout()
        fig2.savefig("rl_acceptance.png", dpi=130)
        print("wrote rl_acceptance.png")
    except ImportError:
        print("(matplotlib not installed \u2014 skipped charts)")


if __name__ == "__main__":
    asyncio.run(main())
