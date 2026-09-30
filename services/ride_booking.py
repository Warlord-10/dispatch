import hashlib
import uuid
from datetime import datetime, timezone

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from enums import RideStatus
from models import Ride
from schemas.rides import RideRequest
from messaging.ride_queue import RideQueuePayload, ride_queue
from services.drivers import free_driver
from services.notifications import push_ride_update


class RideNotFound(Exception):
    pass


class InvalidRideState(Exception):
    """The ride is not in a state where this transition is legal."""


class IdempotencyKeyReused(Exception):
    """Same Idempotency-Key, different request body: a client bug, not a retry."""


async def get_ride(session: AsyncSession, ride_id: uuid.UUID | int) -> Ride:
    ride = await session.get(Ride, ride_id)
    if ride is None:
        raise RideNotFound(f"ride {ride_id} not found")
    return ride

async def _publish(ride: Ride) -> None:
    # Dual write: the ride is committed, then published. A crash in between
    # leaves a pending ride nobody will match — Phase 8's transactional outbox
    # is the fix. Swallowing the error keeps the ride row rather than 500ing.
    try:
        await ride_queue.publish(
            RideQueuePayload(
                ride_id=ride.id,
                rider_id=ride.rider_id,
                pickup_lat=ride.pickup_lat,
                pickup_lng=ride.pickup_lng,
                dest_lat=ride.dest_lat,
                dest_lng=ride.dest_lng,
                round_number=0,
            )
        )
    except Exception as exc:
        print(f"publish failed for ride {ride.id}: {exc!r}")


async def book_ride(
    session: AsyncSession,
    data: RideRequest,
    rider_id: int,
    idempotency_key: uuid.UUID | None = None,
) -> Ride:
    request_hash = hashlib.sha256(data.model_dump_json().encode()).hexdigest()
    ride = Ride(
        rider_id=rider_id,
        pickup_lng=data.pickup_lng,
        pickup_lat=data.pickup_lat,
        dest_lng=data.dest_lng,
        dest_lat=data.dest_lat,
        idempotency_key=idempotency_key,
        request_hash=request_hash,
    )
    # Insert first and let the unique constraint decide, rather than
    # SELECT-then-INSERT: two concurrent retries would both see "no ride yet",
    # both insert, and the loser would 500. Same reasoning as registration.
    session.add(ride)
    try:
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        constraint = getattr(getattr(exc.orig, "__cause__", None), "constraint_name", "") or ""
        if constraint != "rides_idempotency_key":
            raise
        return await _replay(session, rider_id, idempotency_key, request_hash)

    await session.refresh(ride)
    await _publish(ride)
    return ride


async def _replay(
    session: AsyncSession, rider_id: int, idempotency_key: uuid.UUID, request_hash: str
) -> Ride:
    """A retry: hand back the original ride instead of creating another."""
    existing = (
        await session.execute(
            select(Ride).where(
                Ride.rider_id == rider_id, Ride.idempotency_key == idempotency_key
            )
        )
    ).scalar_one()
    if existing.request_hash != request_hash:
        raise IdempotencyKeyReused(
            f"Idempotency-Key {idempotency_key} was already used for a different ride request"
        )
    if existing.status == RideStatus.PENDING:
        # Harmless (the worker drops duplicates) and it rescues a ride whose
        # original publish failed.
        await _publish(existing)
    return existing


async def _transition(
    session: AsyncSession, ride_id, allowed_from: tuple[RideStatus, ...], to: RideStatus, **values
) -> Ride:
    """Move a ride to `to` only if it is still in one of `allowed_from`, and
    free its driver in the same transaction.

    A compare-and-set, not read-then-write. With read-then-write, complete and
    cancel both read 'assigned', both pass their check and both write: measured,
    100/100 concurrent pairs returned 200 to both. And a cancel that read the
    ride before an accept committed wrote 'cancelled' over it without freeing
    the driver it never saw: 3/100 left a driver stranded as 'assigned'.

    The UPDATE takes the row lock and re-checks the status under it, so exactly
    one transition wins. RETURNING driver_id reads the row as it is at that
    moment, so a driver assigned a millisecond earlier is seen and freed.
    """
    result = await session.execute(
        update(Ride)
        .where(Ride.id == ride_id, Ride.status.in_(allowed_from))
        .values(status=to, **values)
        .returning(Ride.driver_id)
    )
    row = result.first()
    if row is None:
        await session.rollback()
        ride = await get_ride(session, ride_id)  # raises RideNotFound
        raise InvalidRideState(f"cannot move a {ride.status} ride to {to}")

    # free_driver commits, which commits the ride update with it: both or neither.
    if row.driver_id is not None:
        await free_driver(session, row.driver_id)
    else:
        await session.commit()

    ride = await session.get(Ride, ride_id, populate_existing=True)
    # Rider and driver both hear it: a rider's cancel is news to the driver
    # driving to them, and a completion is news to the rider.
    await push_ride_update(ride)
    return ride


async def complete_ride(session: AsyncSession, ride_id: uuid.UUID | int) -> Ride:
    return await _transition(
        session, ride_id, (RideStatus.ASSIGNED,), RideStatus.COMPLETED,
        completed_at=datetime.now(timezone.utc),
    )


async def cancel_ride(session: AsyncSession, ride_id: uuid.UUID | int) -> Ride:
    return await _transition(
        session, ride_id, (RideStatus.PENDING, RideStatus.ASSIGNED), RideStatus.CANCELLED,
        cancelled_at=datetime.now(timezone.utc),
    )


async def timed_out_ride(session: AsyncSession, ride_id: uuid.UUID | int) -> bool:
    """pending -> timed_out, only if still pending. Returns whether it happened.

    A compare-and-set, not read-then-write: the timeout lands at the same moment
    the last round's accept window closes, so a driver accepting at 14.9s races
    it. An unconditional write would stamp timed_out over an assigned ride and
    strand the driver. Zero rows means the accept won; that is success, not error.
    """
    result = await session.execute(
        update(Ride)
        .where(Ride.id == ride_id, Ride.status == RideStatus.PENDING)
        .values(status=RideStatus.TIMED_OUT)
        .returning(Ride.id)
    )
    timed_out = result.first() is not None
    await session.commit()
    if timed_out:
        await push_ride_update(await session.get(Ride, ride_id, populate_existing=True))
    return timed_out
