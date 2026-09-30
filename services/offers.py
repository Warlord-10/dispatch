"""Ride offers: who has been approached, and the accept/reject transitions.

The approached-set lives in Redis with a TTL rather than an offers table. It is
only needed while a ride is being matched, and a durable table can come when you
want offer history (acceptance rates, driver scoring).
"""

import time
from datetime import datetime, timezone

from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from config import APPROACHED_TTL_S, OFFER_TTL_S
from enums import DriverStatus, RideStatus
from models import Driver, Ride
from redis_client import redis_client
from redis_keys import ride_offers
from services import geo_index


class RideAlreadyTaken(Exception):
    """Another driver won the race for this ride."""


class DriverUnavailable(Exception):
    """This driver is not in a state to take a ride."""


class NotOffered(Exception):
    """This driver was never offered this ride."""


class OfferExpired(Exception):
    """The driver answered after the accept window closed."""


async def record_offers(ride_id, driver_ids, now: float | None = None) -> float:
    """Mark drivers approached. Returns the offer timestamp, so the caller puts
    the same instant on the offer message that the accept check will use."""
    now = time.time() if now is None else now
    if not driver_ids:
        return now

    key = ride_offers(ride_id)
    pipe = redis_client.pipeline(transaction=True)
    pipe.hset(key, mapping={str(d): now for d in driver_ids})
    pipe.expire(key, APPROACHED_TTL_S)
    await pipe.execute()
    return now


async def already_offered(ride_id) -> set[str]:
    return set(await redis_client.hkeys(ride_offers(ride_id)))


async def check_offer(ride_id, driver_id: int, now: float | None = None) -> None:
    """Raise unless this driver holds a live offer for this ride.

    `now` is the server's clock on receipt, never a timestamp from the phone:
    the client controls that value and could simply claim it was early.
    """
    now = time.time() if now is None else now
    offered_at = await redis_client.hget(ride_offers(ride_id), str(driver_id))
    if offered_at is None:
        raise NotOffered(f"ride {ride_id} was not offered to driver {driver_id}")
    if now - float(offered_at) > OFFER_TTL_S:
        raise OfferExpired(f"offer for ride {ride_id} expired")


async def clear_offers(ride_id) -> None:
    await redis_client.delete(ride_offers(ride_id))



async def accept_ride_offer(
    session: AsyncSession, ride_id, driver_id: int
) -> Ride:
    """Driver taps accept. Two compare-and-sets inside one transaction.

    Broadcasting to 20 drivers makes simultaneous accepts routine, not rare, so
    neither check can be a read-then-write:

      1. claim the DRIVER  (available -> assigned) — the scarce resource
      2. claim the RIDE    (pending   -> assigned)

    Zero rows on either means somebody else got there first, and the rollback
    releases whatever was already claimed. Guarding only the ride would let one
    driver accept two rides; guarding only the driver would let two drivers take
    one ride.
    """
    await check_offer(ride_id, driver_id)
    now = datetime.now(timezone.utc)

    claimed_driver = await session.execute(
        update(Driver)
        .where(Driver.id == driver_id, Driver.status == DriverStatus.AVAILABLE)
        .values(status=DriverStatus.ASSIGNED)
        .returning(Driver.id)
    )
    if claimed_driver.first() is None:
        await session.rollback()
        raise DriverUnavailable(f"driver {driver_id} is not available")

    claimed_ride = await session.execute(
        update(Ride)
        .where(Ride.id == ride_id, Ride.status == RideStatus.PENDING)
        .values(
            status=RideStatus.ASSIGNED, driver_id=driver_id, assigned_at=now
        )
        .returning(Ride.id)
    )
    if claimed_ride.first() is None:
        await session.rollback()  # also releases the driver claim above
        raise RideAlreadyTaken(f"ride {ride_id} is no longer pending")

    await session.commit()

    await geo_index.set_status(driver_id, DriverStatus.ASSIGNED)
    await clear_offers(ride_id)

    return await session.get(Ride, ride_id)


async def reject_ride_offer(session: AsyncSession, ride_id, driver_id: int) -> None:
    """Driver declines.

    Nothing to write: the driver entered the approached hash when the offer was
    sent, so later rounds already skip them, and a silent timeout ends the same
    way. The check only stops drivers rejecting rides they were never offered;
    a late reject is harmless, so the window is not enforced here.
    """
    # ponytail: a no-op beyond validation. Record it when rejects should start
    # the next round early, or feed acceptance-rate analytics.
    if not await redis_client.hexists(ride_offers(ride_id), str(driver_id)):
        raise NotOffered(f"ride {ride_id} was not offered to driver {driver_id}")
