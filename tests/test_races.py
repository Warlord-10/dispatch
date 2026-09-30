"""Concurrent state transitions: the hard invariants under real contention.
Run: docker compose up -d && uv run python tests/test_races.py

Starts its own API as 4 uvicorn processes, so concurrent requests land in
separate processes with separate connection pools, as in production. Every
invariant is checked in Postgres, not inferred from HTTP status codes.

Scenarios 3 and 4 are races with a narrow window: against read-then-write code
they failed 3/100 and 100/100. TRIALS stays at 100 so the rare one still shows.
"""

import pathlib
import sys

# tests live in tests/, the app lives one level up
ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import asyncio
import os
import subprocess
import uuid
from collections import Counter

import httpx
from sqlalchemy import func, select, update

from db import Session
from enums import DriverStatus, RideStatus
from messaging.connection import rabbitmq
from models import Driver, Ride
from services import offers

PORT = 8765
API = f"http://127.0.0.1:{PORT}"
TRIALS = 100
PICKUP = (-45.0, 170.0)  # far from other suites' drivers

_base = uuid.uuid4().int % 10**6
_n = 0
made_drivers: list[int] = []


def phone() -> str:
    global _n
    _n += 1
    return f"9{_base:06d}{_n:03d}"


async def new_rider(c) -> int:
    return (await c.post("/riders", json={"name": "R", "phone_number": phone()})).json()["id"]


async def new_driver(c) -> int:
    r = await c.post("/drivers", json={"name": "D", "phone_number": phone(),
                                       "car_number": "KA01RR0001", "car_model": "Swift"})
    d = r.json()["id"]
    await c.patch(f"/drivers/{d}/status", json={"status": "available"})
    made_drivers.append(d)
    return d


async def new_ride(c, rider: int) -> str:
    r = await c.post("/rides", json={"rider_id": rider, "pickup_lat": PICKUP[0], "pickup_lng": PICKUP[1],
                                     "dest_lat": PICKUP[0] - 0.1, "dest_lng": PICKUP[1] + 0.1})
    assert r.status_code == 202, r.text
    return r.json()["id"]


async def ride_row(ride_id: str) -> Ride:
    async with Session() as s:
        return await s.get(Ride, uuid.UUID(ride_id))


async def driver_status(driver_id: int) -> str:
    async with Session() as s:
        return (await s.get(Driver, driver_id)).status


async def scenarios(c) -> None:
    rider = await new_rider(c)

    # 1 ------------------------------------------------------------------
    drivers = [await new_driver(c) for _ in range(50)]
    ride = await new_ride(c, rider)
    await offers.record_offers(ride, drivers)
    rs = await asyncio.gather(*[c.post(f"/rides/{ride}/accept", json={"driver_id": d}) for d in drivers])
    codes = Counter(r.status_code for r in rs)
    row = await ride_row(ride)
    statuses = Counter([await driver_status(d) for d in drivers])
    assert codes == {200: 1, 409: 49}, f"expected one winner and 49 'taken': {dict(codes)}"
    assert row.status == RideStatus.ASSIGNED and row.driver_id in drivers
    assert statuses == {DriverStatus.ASSIGNED: 1, DriverStatus.AVAILABLE: 49}, dict(statuses)
    print("1. 50 drivers accept one ride at once -> 1 winner, 49 x 409, no loser left assigned")

    # 2 ------------------------------------------------------------------
    driver = await new_driver(c)
    rides = [await new_ride(c, rider) for _ in range(50)]
    for r in rides:
        await offers.record_offers(r, [driver])
    rs = await asyncio.gather(*[c.post(f"/rides/{r}/accept", json={"driver_id": driver}) for r in rides])
    codes = Counter(r.status_code for r in rs)
    async with Session() as s:
        held = await s.scalar(select(func.count()).where(
            Ride.driver_id == driver, Ride.status == RideStatus.ASSIGNED))
        pending = await s.scalar(select(func.count()).where(
            Ride.id.in_([uuid.UUID(r) for r in rides]), Ride.status == RideStatus.PENDING))
    assert codes == {200: 1, 409: 49}, dict(codes)
    assert held == 1 and pending == 49, (held, pending)
    print("2. one driver accepts 50 rides at once -> holds exactly 1, other 49 still pending")

    # 3 ------------------------------------------------------------------
    stranded = 0
    for _ in range(TRIALS):
        d = await new_driver(c)
        r = await new_ride(c, rider)
        await offers.record_offers(r, [d])
        await asyncio.gather(c.post(f"/rides/{r}/accept", json={"driver_id": d}),
                             c.post(f"/rides/{r}/cancel"))
        row = await ride_row(r)
        assert row.status == RideStatus.CANCELLED, f"cancel must always win in the end: {row.status}"
        if await driver_status(d) != DriverStatus.AVAILABLE:
            stranded += 1
    assert stranded == 0, f"{stranded}/{TRIALS} drivers stranded 'assigned' on a cancelled ride"
    print(f"3. cancel vs accept x{TRIALS} -> ride always cancelled, driver always freed")

    # 4 ------------------------------------------------------------------
    both = Counter()
    for _ in range(TRIALS):
        d = await new_driver(c)
        r = await new_ride(c, rider)
        await offers.record_offers(r, [d])
        assert (await c.post(f"/rides/{r}/accept", json={"driver_id": d})).status_code == 200
        com, can = await asyncio.gather(c.post(f"/rides/{r}/complete"), c.post(f"/rides/{r}/cancel"))
        codes = sorted([com.status_code, can.status_code])
        row = await ride_row(r)
        both[(tuple(codes), row.status)] += 1
        assert codes == [200, 409], f"both transitions were acknowledged: complete {com.status_code}, cancel {can.status_code}"
        winner = RideStatus.COMPLETED if com.status_code == 200 else RideStatus.CANCELLED
        assert row.status == winner, f"stored {row.status}, but the 200 went to {winner}"
        assert await driver_status(d) == DriverStatus.AVAILABLE
    print(f"4. complete vs cancel x{TRIALS} -> exactly one 200, stored status matches it: "
          + ", ".join(f"{v} {s}" for (_, s), v in both.items()))


async def cleanup() -> None:
    async with Session() as s:
        await s.execute(update(Driver).where(Driver.id.in_(made_drivers)).values(status=DriverStatus.OFFLINE))
        await s.execute(update(Ride).where(Ride.driver_id.in_(made_drivers), Ride.status == RideStatus.ASSIGNED)
                        .values(status=RideStatus.CANCELLED))
        await s.commit()
    await rabbitmq.connect()
    ch = await rabbitmq.publisher_channel()
    await (await ch.declare_queue("ride.requested", passive=True)).purge()  # nobody consumed them
    await rabbitmq.close()


async def main() -> None:
    api = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "main:app", "--workers", "4",
         "--port", str(PORT), "--log-level", "warning"],
        cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    try:
        async with httpx.AsyncClient(base_url=API, timeout=30,
                                     limits=httpx.Limits(max_connections=200)) as c:
            for _ in range(60):
                try:
                    if (await c.get("/health")).status_code == 200:
                        break
                except httpx.TransportError:
                    await asyncio.sleep(0.5)
            else:
                raise RuntimeError("API did not start")
            await asyncio.sleep(2)  # let all 4 workers finish their startup
            await scenarios(c)
    finally:
        api.terminate()
        api.wait()
        await cleanup()
    print("races ok")


if __name__ == "__main__":
    asyncio.run(main())
