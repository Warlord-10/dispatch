"""Driver state transitions.

Dual write: Postgres is the source of truth, Redis is the index over it.
They are not one transaction and cannot be — if the Redis write fails the
driver is briefly missing from dispatch and the next ping heals it.
Phase 8 (transactional outbox) is where this gets fixed properly.
"""

from sqlalchemy.ext.asyncio import AsyncSession

from models import Driver
from schemas.drivers import DriverStatusUpdate
from services import geo_index


class DriverNotFound(Exception):
    pass


async def get_driver(session: AsyncSession, driver_id: int) -> Driver:
    driver = await session.get(Driver, driver_id)
    if driver is None:
        raise DriverNotFound(f"driver {driver_id} not found")
    return driver


async def set_driver_status(session: AsyncSession, driver_id: int, data: DriverStatusUpdate) -> Driver:
    driver = await get_driver(session, driver_id)

    driver.status = data.status
    if data.lat is not None and data.lng is not None:
        driver.lat = data.lat
        driver.lng = data.lng

    await session.commit()
    await session.refresh(driver)

    if driver.status == "available":
        await geo_index.add_driver(driver.id, driver.lat, driver.lng)
    else:
        await geo_index.remove_driver(driver.id)

    return driver


async def assign_driver(session: AsyncSession, driver_id: int, ride_id) -> Driver:
    driver = await get_driver(session, driver_id)
    driver.status = "assigned"

    await session.commit()
    await session.refresh(driver)

    await geo_index.remove_driver(driver_id)
    return driver


async def free_driver(session: AsyncSession, driver_id: int) -> Driver:
    driver = await get_driver(session, driver_id)
    driver.status = "available"

    await session.commit()
    await session.refresh(driver)

    await geo_index.add_driver(driver_id, driver.lat, driver.lng)
    return driver
