"""Driver state transitions.

ponytail: set_driver_status is a demo shim. Phase 1's real endpoint is a
location ping (POST /drivers/{id}/location) that writes lat/lng and flips
offline -> available implicitly. That ping is what generates 12.5k writes/s
in Phase 2 and moves to Redis in Phase 4.
"""

from sqlalchemy.ext.asyncio import AsyncSession

from models import Driver
from schemas.drivers import DriverStatusUpdate


class DriverNotFound(Exception):
    pass


async def set_driver_status(
    session: AsyncSession, driver_id: int, data: DriverStatusUpdate
) -> Driver:
    driver = await session.get(Driver, driver_id)
    if driver is None:
        raise DriverNotFound(f"driver {driver_id} not found")

    driver.status = data.status
    if data.lat is not None and data.lng is not None:
        driver.lat = data.lat
        driver.lng = data.lng

    await session.commit()
    await session.refresh(driver)
    return driver
