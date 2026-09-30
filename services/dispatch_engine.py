"""Fan-out: turn a pending ride into offers to nearby drivers.

Runs inside the matcher worker, not in an HTTP request — which is why nothing
here imports FastAPI and every function takes an explicit session.
"""

from sqlalchemy.ext.asyncio import AsyncSession

from config import MAX_OFFER_ROUNDS
from enums import RideStatus
from models import Ride
from services import offers
from services.geo_index import search_drivers_in_radius
from messaging.ride_queue import RideQueuePayload
from services.ride_booking import timed_out_ride


# Outcomes of a round, not failures. The worker catches all three and acks;
# only an unexpected error may escape to the DLQ.
class RideNotPending(Exception):
    """Accepted or cancelled since the last round: stop matching it."""


class NoDriverAvailable(Exception):
    """Nobody new within the widest radius this round: try the next round."""


class RideTimedOut(Exception):
    """Out of rounds: the ride is now timed_out."""


async def dispatch_ride(session: AsyncSession, payload: RideQueuePayload) -> list[str]:
    """Decide one round for a ride: the nearest available drivers not yet
    approached, closest first. Raises one of the outcomes above otherwise.

    Safe to run twice on the same ride: at-least-once delivery means this WILL
    be re-run after a worker crash.
    """
    ride = await session.get(Ride, payload.ride_id)
    if ride is None:
        raise ValueError(f"ride {payload.ride_id} not found")

    if ride.status != RideStatus.PENDING:
        raise RideNotPending(f"ride {ride.id} is {ride.status}")

    if payload.round_number > MAX_OFFER_ROUNDS:
        if await timed_out_ride(session, ride.id):
            raise RideTimedOut(f"ride {ride.id} timed out after {MAX_OFFER_ROUNDS} rounds")
        # The compare-and-set lost: a driver accepted in the last instant.
        raise RideNotPending(f"ride {ride.id} was taken just before timing out")

    approached = await offers.already_offered(ride.id)
    drivers = await search_drivers_in_radius(
        ride.pickup_lat, ride.pickup_lng, exclude=approached
    )
    if not drivers:
        raise NoDriverAvailable(f"no new drivers for ride {ride.id} in round {payload.round_number}")
    return drivers
