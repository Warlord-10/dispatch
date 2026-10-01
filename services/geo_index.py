"""The Redis-side driver index. Nothing outside this module talks to Redis
about drivers.

Three structures:

  driver:{id}:position   STRING "lng,lat,epoch", native TTL  — position + liveness
  drivers:status         HASH   id -> DriverStatus           — eligibility
  drivers:available:geo  GEO    dispatchable positions       — the search index

Position and liveness live in one self-expiring key: the key existing means we
know where the driver was, the embedded epoch means they are online right now.
Only the available geo set needs sweeping, because geo members never expire.
"""

import asyncio
import logging
import time

from config import (
    CANDIDATE_COUNT,
    POSITION_TTL_S,
    RADIUS_LADDER_KM,
    STALE_AFTER_S,
    SWEEP_INTERVAL_S,
)
from enums import DriverStatus
from redis_client import redis_client
from redis_keys import DRIVERS_AVAILABLE_GEO, DRIVERS_STATUS, driver_position

log = logging.getLogger(__name__)


# --------------------------------------------------------------------------
# position (+ liveness, via the embedded timestamp)
# --------------------------------------------------------------------------

def _encode(lat: float, lng: float, epoch: float) -> str:
    return f"{lng:.6f},{lat:.6f},{epoch:.0f}"


def _decode(raw: str | None) -> tuple[float, float, float] | None:
    """-> (lng, lat, epoch), or None if absent/corrupt."""
    if not raw:
        return None
    try:
        lng, lat, epoch = raw.split(",")
        return float(lng), float(lat), float(epoch)
    except ValueError:
        return None


async def update_driver_pos(
    driver_id: int, lat: float, lng: float, now: float | None = None
) -> None:
    """Write the driver's position. Native TTL reclaims it — no sweeper."""
    now = time.time() if now is None else now
    await redis_client.set(
        driver_position(driver_id), _encode(lat, lng, now), ex=POSITION_TTL_S
    )


async def get_driver_pos(driver_id: int) -> tuple[float, float, float] | None:
    return _decode(await redis_client.get(driver_position(driver_id)))


async def get_driver_positions(driver_ids) -> dict[str, tuple[float, float, float]]:
    """Bulk read for the dispatch path: one MGET, not N round trips."""
    ids = [str(d) for d in driver_ids]
    if not ids:
        return {}
    raws = await redis_client.mget([driver_position(d) for d in ids])
    out = {}
    for driver_id, raw in zip(ids, raws):
        decoded = _decode(raw)
        if decoded is not None:
            out[driver_id] = decoded
    return out


async def is_online(driver_id: int, now: float | None = None) -> bool:
    now = time.time() if now is None else now
    pos = await get_driver_pos(driver_id)
    return pos is not None and pos[2] >= now - STALE_AFTER_S


# --------------------------------------------------------------------------
# eligibility
# --------------------------------------------------------------------------

async def get_status(driver_id: int) -> str | None:
    return await redis_client.hget(DRIVERS_STATUS, str(driver_id))


async def set_status(driver_id: int, status: str) -> None:
    """Record eligibility and move the driver in or out of the dispatchable set."""
    await redis_client.hset(DRIVERS_STATUS, str(driver_id), str(status))
    if status == DriverStatus.AVAILABLE:
        await add_available_driver(driver_id)
    else:
        await remove_available_driver(driver_id)


async def load_statuses(rows) -> int:
    """Rebuild eligibility from Postgres. Redis keeps no durable copy, so without
    this a Redis restart leaves the whole fleet undispatchable."""
    rows = list(rows)
    if not rows:
        return 0
    await redis_client.hset(
        DRIVERS_STATUS, mapping={str(i): str(s) for i, s in rows}
    )
    return len(rows)


# --------------------------------------------------------------------------
# the dispatchable set
# --------------------------------------------------------------------------

async def add_available_driver(driver_id: int) -> bool:
    """Put a driver into the dispatchable set, using their last known position.

    Returns False when no position is on record — a brand new driver, or one
    offline longer than POSITION_TTL_S. Their next ping adds them, because the
    ping is gated on the status hash, not on membership of this set.
    """
    pos = await get_driver_pos(driver_id)
    if pos is None:
        return False
    lng, lat, _ = pos
    await redis_client.geoadd(DRIVERS_AVAILABLE_GEO, (lng, lat, driver_id))
    return True


async def remove_available_driver(driver_id: int) -> None:
    """Assigned, or gone offline. A geo set is a zset, so ZREM works."""
    await redis_client.zrem(DRIVERS_AVAILABLE_GEO, driver_id)


async def driver_ping(
    driver_id: int, lat: float, lng: float, now: float | None = None
) -> bool:
    """Record position; also refresh the dispatch index if currently available.

    Gated on the status hash, never on membership of the available set —
    gating on membership means a driver removed by the sweeper can never rejoin.
    """
    now = time.time() if now is None else now
    await update_driver_pos(driver_id, lat, lng, now=now)

    if await get_status(driver_id) != DriverStatus.AVAILABLE:
        return False
    await redis_client.geoadd(DRIVERS_AVAILABLE_GEO, (lng, lat, driver_id))
    return True


# --------------------------------------------------------------------------
# search
# --------------------------------------------------------------------------

async def search_drivers_in_radius(
    lat: float,
    lng: float,
    ladder: tuple[float, ...] = RADIUS_LADDER_KM,
    count: int = CANDIDATE_COUNT,
    exclude: set[str] | None = None,
    now: float | None = None,
) -> list[str]:
    """Nearest dispatchable, live drivers — closest first."""
    now = time.time() if now is None else now
    exclude = exclude or set()
    cutoff = now - STALE_AFTER_S

    for radius in ladder:
        found = await redis_client.geosearch(
            DRIVERS_AVAILABLE_GEO,
            longitude=lng,
            latitude=lat,
            radius=radius,
            unit="km",
            sort="ASC",
            # Over-fetch by the exclusion size: COUNT is applied before we
            # filter, so asking for `count` would return the same nearest
            # drivers every round and never reach the next ones out.
            count=count + len(exclude),
        )
        candidates = [d for d in found if d not in exclude]
        # Read-time freshness check. Correctness lives here, not in the sweeper:
        # it holds even if the sweeper is dead or behind.
        positions = await get_driver_positions(candidates)
        fresh = [
            d for d in candidates
            if d in positions and positions[d][2] >= cutoff
        ]
        if fresh:
            return fresh[:count]
    return []


# --------------------------------------------------------------------------
# sweeper (memory hygiene only — never correctness)
# --------------------------------------------------------------------------

async def sweep(now: float | None = None) -> int:
    """Drop members of the available set whose position key has expired or gone
    stale. Position and liveness need no sweeping; only geo members do.

    O(N) over the available set rather than the O(log N + k) a scored zset gave
    us — affordable precisely because correctness does not depend on it, so this
    can run once a minute instead of every few seconds.
    """
    now = time.time() if now is None else now
    cutoff = now - STALE_AFTER_S
    members = await redis_client.zrange(DRIVERS_AVAILABLE_GEO, 0, -1)
    if not members:
        return 0

    positions = await get_driver_positions(members)
    dead = [
        m for m in members
        if m not in positions or positions[m][2] < cutoff
    ]
    if dead:
        await redis_client.zrem(DRIVERS_AVAILABLE_GEO, *dead)
    return len(dead)


async def sweeper_loop() -> None:
    """ZREM is idempotent, so every replica can run this with no leader election."""
    while True:
        try:
            await sweep()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # a dead sweeper must not take the app down
            log.exception("sweeper pass failed, retrying in %ds", SWEEP_INTERVAL_S)
        await asyncio.sleep(SWEEP_INTERVAL_S)

