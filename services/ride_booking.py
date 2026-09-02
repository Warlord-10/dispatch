import uuid
from datetime import datetime, timezone

from sqlalchemy.ext.asyncio import AsyncSession

from models import Driver, Ride
from schemas.rides import RideRequest
from services.dispatch_engine import NoDriverFound, match_ride

TERMINAL = ("completed", "cancelled")


class RideNotFound(Exception):
    pass


class InvalidRideState(Exception):
    """The ride is not in a state where this transition is legal."""


async def get_ride(session: AsyncSession, ride_id: uuid.UUID | int) -> Ride:
    ride = await session.get(Ride, ride_id)
    if ride is None:
        raise RideNotFound(f"ride {ride_id} not found")
    return ride


async def _free_driver(session: AsyncSession, ride: Ride) -> None:
    """Return the driver to the pool. No-op for a ride nobody was assigned to."""
    if ride.driver_id is None:
        return
    driver = await session.get(Driver, ride.driver_id)
    if driver is not None:
        driver.status = "available"


async def book_ride(session: AsyncSession, data: RideRequest, rider_id: int) -> Ride:
    ride = Ride(
        rider_id=rider_id,
        pickup_lng=data.pickup_lng,
        pickup_lat=data.pickup_lat,
        dest_lng=data.dest_lng,
        dest_lat=data.dest_lat,
    )
    session.add(ride)
    await session.commit()
    await session.refresh(ride)

    try:
        await match_ride(session, ride.id)
    except NoDriverFound:
        # The ride is already committed, so raising here would report failure
        # for a row that exists. Leave it 'pending' and let the caller poll —
        # Phase 5 is where a worker retries it properly.
        pass

    await session.refresh(ride)
    return ride


async def accept_ride(session: AsyncSession, ride_id: uuid.UUID | int, driver_id: int) -> Ride:
    # ponytail: unused in Phase 1's auto-assign flow. Kept for the offer/accept
    # model, where this becomes a compare-and-set against status='pending'.
    ride = await get_ride(session, ride_id)
    if ride.status in TERMINAL:
        raise InvalidRideState(f"ride is already {ride.status}")

    ride.driver_id = driver_id
    ride.status = "assigned"
    ride.assigned_at = datetime.now(timezone.utc)
    await session.commit()
    await session.refresh(ride)
    return ride


async def complete_ride(session: AsyncSession, ride_id: uuid.UUID | int) -> Ride:
    ride = await get_ride(session, ride_id)
    if ride.status != "assigned":
        raise InvalidRideState(f"cannot complete a ride that is {ride.status}")

    ride.status = "completed"
    await _free_driver(session, ride)
    await session.commit()
    await session.refresh(ride)
    return ride


async def cancel_ride(session: AsyncSession, ride_id: uuid.UUID | int) -> Ride:
    ride = await get_ride(session, ride_id)
    if ride.status in TERMINAL:
        raise InvalidRideState(f"ride is already {ride.status}")

    ride.status = "cancelled"
    await _free_driver(session, ride)
    await session.commit()
    await session.refresh(ride)
    return ride
