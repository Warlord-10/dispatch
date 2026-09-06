import uuid
from datetime import datetime, timezone

from sqlalchemy.ext.asyncio import AsyncSession

from models import Ride
from services.drivers import assign_driver
from services.geo_index import search_drivers_in_radius


class NoDriverFound(Exception):
    """Raised when no available driver is nearby."""


async def match_ride(session: AsyncSession, ride_id: uuid.UUID | int) -> Ride:
    """Assign the nearest dispatchable driver. Redis does the ranking now.

    The haversine/equirectangular SQL this used to run is gone: the geo index
    returns candidates already sorted by true distance.
    """
    ride = await session.get(Ride, ride_id)
    if ride is None:
        raise ValueError(f"ride {ride_id} not found")

    drivers = await search_drivers_in_radius(ride.pickup_lat, ride.pickup_lng)
    if not drivers:
        raise NoDriverFound("no available driver found")

    # geosearch returns member names as strings
    driver_id = int(drivers[0])

    ride.driver_id = driver_id
    ride.status = "assigned"
    ride.assigned_at = datetime.now(timezone.utc)

    # same session, so this commit persists the ride changes too
    await assign_driver(session, driver_id, ride.id)

    await session.refresh(ride)
    return ride
