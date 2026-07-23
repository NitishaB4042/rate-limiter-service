"""
A tiny demo client for the Rate Limiter Service (Phase 4).
Hammers /check and prints which calls were allowed vs 429'd.
Also serves as the traffic generator reused in the Phase 5 benchmark.

Start the server first:  uvicorn service:app --port 8000
Then run:                python demo_client.py
"""
import asyncio
import httpx

BASE = "http://localhost:8000"


async def main():
    async with httpx.AsyncClient(base_url=BASE) as c:
        algo = "sliding_counter"
        await c.post("/reset", json={"client_id": "demo", "algorithm": algo})
        print(f"Algorithm={algo}, limit=5 per 3s. Sending 8 rapid requests:\n")
        for i in range(1, 9):
            r = await c.post("/check", json={"client_id": "demo", "limit": 5,
                                             "window": 3, "algorithm": algo})
            remaining = r.headers.get("X-RateLimit-Remaining", "?")
            if r.status_code == 200:
                print(f"  request {i}: 200 ALLOW   (remaining {remaining})")
            else:
                ra = r.headers.get("Retry-After", "?")
                print(f"  request {i}: 429 DENY    (retry after {ra}s)")


if __name__ == "__main__":
    asyncio.run(main())
