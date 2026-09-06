"""Phase 2 load generator.

Two experiments:
  scan  — fixed request rate, growing driver count. Exposes the full-table scan.
  ramp  — fixed driver count, growing request rate. Exposes saturation.

Open-loop by design: requests are scheduled at a fixed rate and latency is
measured from the SCHEDULED time, not the send time. A closed-loop generator
(send, wait, send) stops issuing load exactly when the server slows down, which
hides queueing delay — that is coordinated omission, and it makes a saturated
system look healthy.

Run:  docker compose up -d
      uv run uvicorn main:app --port 8100 --log-level warning
      uv run python bench.py seed --drivers 10000
      uv run python bench.py scan
      uv run python bench.py ramp
"""

import argparse
import asyncio
import random
import time

import httpx
from sqlalchemy import delete, select, text, update

from cache import cache
from db import Session, init_db
from models import Driver, Ride, Rider
from services.geo_index import AVAILABLE_DRIVERS_KEY

BASE = "http://127.0.0.1:8100"
CITY = (12.85, 13.10, 77.45, 77.75)  # Bengaluru bounding box


def pct(values, p):
    if not values:
        return float("nan")
    s = sorted(values)
    return s[min(int(len(s) * p / 100), len(s) - 1)] * 1000  # ms


async def seed(n_drivers):
    await init_db()
    async with Session() as s:
        await s.execute(delete(Ride))
        await s.execute(delete(Driver))
        await s.execute(delete(Rider))
        s.add(Rider(id=1, name="bench", phone_number="6000000000"))
        lo_lat, hi_lat, lo_lng, hi_lng = CITY
        s.add_all([
            Driver(
                id=i, name=f"d{i}", phone_number=f"6{i:09d}",
                car_number=f"KA01AA{i:04d}", car_model="Swift", status="available",
                lat=random.uniform(lo_lat, hi_lat), lng=random.uniform(lo_lng, hi_lng),
            )
            for i in range(1, n_drivers + 1)
        ])
        await s.commit()
        _COORDS.clear()
        # Explicit ids do not advance the sequence, so the next ORM insert
        # would collide on the primary key. Push both sequences past the
        # seeded range.
        await s.execute(text("select setval('drivers_id_seq', :v)"), {"v": n_drivers + 1})
        await s.execute(text("select setval('riders_id_seq', 2)"))
        await s.commit()
    await _geo_sync(n_drivers, reset=True)
    print(f"seeded {n_drivers} drivers + 1 rider (postgres + redis)")


async def _geo_sync(n, reset=False):
    """Mirror drivers 1..n into the Redis geo index.

    Without this the benchmark searches an empty index and reports a flat
    line that measures nothing.
    """
    if reset:
        await cache.delete(AVAILABLE_DRIVERS_KEY)
    async with Session() as s:
        rows = (await s.execute(
            select(Driver.id, Driver.lng, Driver.lat).where(Driver.id <= n)
        )).all()
    flat = []
    for did, lng, lat in rows:
        flat += [lng, lat, did]
    if flat:
        await cache.geoadd(AVAILABLE_DRIVERS_KEY, flat)


_COORDS: dict[int, tuple[float, float]] = {}


async def _coords():
    """Driver positions, read once. Re-querying every top-up would measure
    the harness instead of the server."""
    global _COORDS
    if not _COORDS:
        async with Session() as s:
            rows = (await s.execute(select(Driver.id, Driver.lat, Driver.lng))).all()
        _COORDS = {r[0]: (r[1], r[2]) for r in rows}
    return _COORDS


async def set_available(n):
    """Expose exactly n drivers to the matcher; park the rest offline.

    Must touch Redis as well as Postgres — matching reads the geo index now,
    so a Postgres-only top-up benchmarks an empty index.
    """
    async with Session() as s:
        await s.execute(update(Driver).values(status="offline"))
        await s.execute(
            update(Driver).where(Driver.id <= n).values(status="available")
        )
        await s.commit()

    coords = await _coords()
    members = []
    for i in range(1, n + 1):
        lat, lng = coords[i]
        members.extend([lng, lat, i])
    pipe = cache.pipeline()
    pipe.delete(AVAILABLE_DRIVERS_KEY)
    if members:
        pipe.geoadd(AVAILABLE_DRIVERS_KEY, members)
    await pipe.execute()
    await _geo_sync(n, reset=True)


async def keep_supply(n, stop):
    """Matched drivers flip to 'assigned' and leave the candidate set. Top the
    pool back up so we measure the scan over a constant n, not a draining one."""
    while not stop.is_set():
        async with Session() as s:
            await s.execute(update(Driver).where(Driver.id <= n).values(status="available"))
            await s.commit()
        # no DEL here: that would leave the index momentarily empty mid-run
        await _geo_sync(n)
        await asyncio.sleep(1.0)


async def open_loop(client, rps, duration):
    """Fire ride requests at a fixed rate regardless of server health."""
    lo_lat, hi_lat, lo_lng, hi_lng = CITY
    samples, errors = [], 0
    t_zero = time.perf_counter()

    async def one(scheduled):
        nonlocal errors
        drift = scheduled - (time.perf_counter() - t_zero)
        if drift > 0:
            await asyncio.sleep(drift)
        try:
            r = await client.post("/rides", json={
                "rider_id": 1,
                "pickup_lat": random.uniform(lo_lat, hi_lat),
                "pickup_lng": random.uniform(lo_lng, hi_lng),
                "dest_lat": random.uniform(lo_lat, hi_lat),
                "dest_lng": random.uniform(lo_lng, hi_lng),
            })
            if r.status_code >= 400:
                errors += 1
        except Exception:
            errors += 1
        # measured from the scheduled instant: includes time spent queued
        samples.append(time.perf_counter() - t_zero - scheduled)

    interval = 1.0 / rps
    tasks = [asyncio.create_task(one(i * interval))
             for i in range(int(duration * rps))]
    await asyncio.gather(*tasks)
    return samples, errors


async def stage(label, n_drivers, rps, seconds):
    await set_available(n_drivers)
    stop = asyncio.Event()
    topper = asyncio.create_task(keep_supply(n_drivers, stop))
    limits = httpx.Limits(max_connections=2000, max_keepalive_connections=2000)
    async with httpx.AsyncClient(base_url=BASE, limits=limits, timeout=30.0) as c:
        samples, errors = await open_loop(c, rps, seconds)
    stop.set()
    await topper
    n = len(samples)
    print(f"{label:>10} {rps:>6} {n:>7} {pct(samples,50):>9.1f} "
          f"{pct(samples,95):>9.1f} {pct(samples,99):>9.1f} "
          f"{errors:>7} {n/seconds:>9.1f}")
    return pct(samples, 99)


async def run(mode):
    async with httpx.AsyncClient(base_url=BASE, timeout=5.0) as c:
        try:
            await c.get("/health")
        except Exception:
            raise SystemExit(f"no server at {BASE} — start uvicorn first")

    hdr = f"{'drivers':>10} {'rps':>6} {'sent':>7} {'p50 ms':>9} {'p95 ms':>9} {'p99 ms':>9} {'errors':>7} {'thru/s':>9}"
    print(hdr)
    print("-" * len(hdr))
    if mode == "scan":
        for n in (100, 500, 1000, 2500, 5000, 10000):
            await stage(str(n), n, rps=50, seconds=8)
    else:
        for rps in (10, 25, 50, 100, 200, 400):
            await stage("2000", 2000, rps=rps, seconds=8)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["seed", "scan", "ramp"])
    ap.add_argument("--drivers", type=int, default=10000)
    a = ap.parse_args()
    asyncio.run(seed(a.drivers) if a.cmd == "seed" else run(a.cmd))
