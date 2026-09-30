"""Driver ping / presence / sweeper. Run: uv run python tests/test_driver_index.py"""

import pathlib
import sys

# tests live in tests/, the app lives one level up
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

import asyncio
import time

from enums import DriverStatus
from redis_client import redis_client
from redis_keys import DRIVERS_AVAILABLE_GEO, DRIVERS_STATUS, driver_position
from services import geo_index as g

BLR = (12.9352, 77.6245)


async def reset():
    keys = await redis_client.keys("dispatch:*")
    if keys:
        await redis_client.delete(*keys)


async def main():
    now = time.time()
    await reset()

    # a driver with no recorded status is tracked but not dispatchable
    assert await g.driver_ping(1, *BLR, now=now) is False
    assert await redis_client.exists(driver_position(1)) == 1, "position still recorded"
    assert await redis_client.zcard(DRIVERS_AVAILABLE_GEO) == 0
    print("1. unknown status -> position+liveness recorded, not dispatchable")

    # going available uses the position from DRIVERS_GEO, not from Postgres
    await g.set_status(1, DriverStatus.AVAILABLE)
    assert await redis_client.zcard(DRIVERS_AVAILABLE_GEO) == 1
    print("2. set_status(available) sourced position from DRIVERS_GEO (no PG columns)")

    assert await g.driver_ping(1, *BLR, now=now) is True
    assert await g.search_drivers_in_radius(*BLR, now=now) == ["1"]
    print("3. ping now dispatchable, search finds them")

    # assigned drivers leave the dispatchable set but keep being tracked
    await g.set_status(1, DriverStatus.ASSIGNED)
    assert await g.search_drivers_in_radius(*BLR, now=now) == []
    assert await g.driver_ping(1, *BLR, now=now) is False
    assert await redis_client.exists(driver_position(1)) == 1, "position key keeps them"
    print("4. assigned -> out of dispatch, still in the global position set")

    await g.set_status(1, DriverStatus.AVAILABLE)

    # read-time staleness: no sweeper involved
    stale_now = now + 31
    assert await g.search_drivers_in_radius(*BLR, now=stale_now) == []
    assert await g.is_online(1, now=stale_now) is False
    assert await g.is_online(1, now=now) is True
    print("5. stale ping filtered at read time, sweeper not involved")

    # THE DEADLOCK CASE: sweeper drops them, then they come back
    removed = await g.sweep(now=stale_now)
    assert removed == 1
    assert await redis_client.zcard(DRIVERS_AVAILABLE_GEO) == 0
    assert await redis_client.hget(DRIVERS_STATUS, "1") == DriverStatus.AVAILABLE, \
        "status must survive the sweep or the driver can never rejoin"
    assert await g.driver_ping(1, *BLR, now=stale_now) is True, "swept driver must rejoin on ping"
    assert await g.search_drivers_in_radius(*BLR, now=stale_now) == ["1"]
    print("6. swept driver rejoins on next ping (status survived the sweep)")

    # a driver who ends their shift is different from one who lost signal
    await g.set_status(1, DriverStatus.OFFLINE)
    assert await g.is_online(1, now=stale_now) is True, "still pinging = still online"
    assert await g.search_drivers_in_radius(*BLR, now=stale_now) == []
    print("7. status=offline vs stale ping are tracked separately")

    # nearest-first across several drivers
    await reset()
    later = time.time()
    for i, lat in ((10, 12.9352), (11, 12.9450), (12, 12.9550)):
        await g.set_status(i, DriverStatus.AVAILABLE)
        await g.driver_ping(i, lat, 77.6245, now=later)
    assert await g.search_drivers_in_radius(*BLR, now=later) == ["10", "11", "12"]
    print("8. nearest-first ordering preserved")

    # sweep only removes the stale one
    await g.driver_ping(11, 12.9450, 77.6245, now=later + 40)
    removed = await g.sweep(now=later + 40)
    assert removed == 2, f"expected 10 and 12 swept, got {removed}"
    assert await g.search_drivers_in_radius(*BLR, now=later + 40) == ["11"]
    print("9. sweep removes only stale members, leaves the fresh one")

    # round 2 must reach past the drivers round 1 already asked
    await reset()
    for i in range(25):
        await g.set_status(100 + i, DriverStatus.AVAILABLE)
        await g.driver_ping(100 + i, BLR[0] + i * 0.0005, BLR[1], now=later)
    round1 = await g.search_drivers_in_radius(*BLR, count=20, now=later)
    assert round1 == [str(100 + i) for i in range(20)], round1
    round2 = await g.search_drivers_in_radius(*BLR, count=20, exclude=set(round1), now=later)
    assert round2 == [str(100 + i) for i in range(20, 25)], round2
    print("10. exclusion over-fetches: round 2 reaches drivers #21-25")

    await reset()
    print("driver index ok")


if __name__ == "__main__":
    asyncio.run(main())
