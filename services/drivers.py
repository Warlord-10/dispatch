"""Driver state transitions.

Dual write: Postgres owns eligibility, Redis indexes it. They are not one
transaction and cannot be — if the Redis write fails the driver is briefly
missing from dispatch. Phase 8 (transactional outbox) is the proper fix.
"""

import time

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from config import STALE_AFTER_S
from enums import DriverStatus
from models import Driver
from schemas.drivers import DriverPing, DriverPresence, DriverStatusUpdate
from services import geo_index


class DriverNotFound(Exception):
    pass


async def get_driver(session: AsyncSession, driver_id: int) -> Driver:
    driver = await session.get(Driver, driver_id)
    if driver is None:
        raise DriverNotFound(f"driver {driver_id} not found")
    return driver


async def apply_status(
    session: AsyncSession, driver_id: int, status: DriverStatus
) -> Driver:
    """Postgres first (durable), then Redis (fast). Never the other way round:
    eligibility is the one piece of driver state nothing regenerates."""
    driver = await get_driver(session, driver_id)
    driver.status = status

    await session.commit()
    await session.refresh(driver)

    await geo_index.set_status(driver_id, status)
    return driver


async def set_driver_status(
    session: AsyncSession, driver_id: int, data: DriverStatusUpdate
) -> Driver:
    return await apply_status(session, driver_id, data.status)


async def assign_driver(session: AsyncSession, driver_id: int, ride_id=None) -> Driver:
    return await apply_status(session, driver_id, DriverStatus.ASSIGNED)


async def free_driver(session: AsyncSession, driver_id: int) -> Driver:
    return await apply_status(session, driver_id, DriverStatus.AVAILABLE)


async def record_ping(driver_id: int, data: DriverPing) -> DriverPresence:
    """The hot path: Redis only, no Postgres read or write."""
    now = time.time()
    dispatchable = await geo_index.driver_ping(driver_id, data.lat, data.lng, now=now)
    status = await geo_index.get_status(driver_id)
    return DriverPresence(
        driver_id=driver_id,
        status=status,
        online=True,
        dispatchable=dispatchable,
        lat=data.lat,
        lng=data.lng,
        last_seen_epoch=now,
        seconds_since_ping=0.0,
    )


async def get_presence(driver_id: int) -> DriverPresence:
    now = time.time()
    status = await geo_index.get_status(driver_id)
    pos = await geo_index.get_driver_pos(driver_id)
    online = pos is not None and pos[2] >= now - STALE_AFTER_S
    return DriverPresence(
        driver_id=driver_id,
        status=status,
        online=online,
        dispatchable=online and status == DriverStatus.AVAILABLE,
        lat=None if pos is None else pos[1],
        lng=None if pos is None else pos[0],
        last_seen_epoch=None if pos is None else pos[2],
        seconds_since_ping=None if pos is None else round(now - pos[2], 1),
    )


async def rebuild_status_index(session: AsyncSession) -> int:
    """Reload eligibility from Postgres into Redis at startup."""
    rows = (await session.execute(select(Driver.id, Driver.status))).all()
    return await geo_index.load_statuses([(r[0], r[1]) for r in rows])
