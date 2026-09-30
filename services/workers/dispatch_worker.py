"""Matcher worker. Run: uv run python -m services.workers.dispatch_worker

Consumes ride requests and fans them out to nearby drivers. A separate process
from the API on purpose: matching must not compete with request serving, and
you scale the two independently.
"""

import asyncio
from datetime import datetime, timezone

from config import OFFER_TTL_S
from db import Session
from messaging.connection import rabbitmq
from messaging.ride_queue import RideQueuePayload, ride_queue, ride_retry_queue
from services.dispatch_engine import (
    NoDriverAvailable,
    RideNotPending,
    RideTimedOut,
    dispatch_ride,
)
from services.offers import record_offers
from messaging.offer_queue import OfferQueuePayload, offer_queue


async def handle_ride_requested(payload: RideQueuePayload) -> None:
    payload.round_number += 1

    async with Session() as session:
        try:
            driver_ids = await dispatch_ride(session, payload)
        except (RideNotPending, RideTimedOut) as outcome:
            # The loop ends here. Return (ack), never raise: raising means
            # "this message failed" and would park a normal outcome in the DLQ.
            print(f"[matcher] {outcome}")
            return
        except NoDriverAvailable:
            driver_ids = []  # no offers this round, but still schedule the next

    # Session closed before publishing: holding a pooled Postgres connection
    # through N publishes starves the other in-flight rides (pool 10, prefetch 20).
    if driver_ids:
        offered_at = await record_offers(payload.ride_id, driver_ids)
        expires_at = datetime.fromtimestamp(offered_at + OFFER_TTL_S, timezone.utc)

        for driver_id in driver_ids:
            offer = OfferQueuePayload(
                ride_id=payload.ride_id,
                rider_id=payload.rider_id,
                driver_id=int(driver_id),
                pickup_lat=payload.pickup_lat,
                pickup_lng=payload.pickup_lng,
                dest_lat=payload.dest_lat,
                dest_lng=payload.dest_lng,
                expires_at=expires_at,
            )
            await offer_queue.publish(offer)

    # Unconditional: an empty round still schedules the next one, and a ride
    # taken during the next 15s is caught by the None check on its way back.
    await ride_retry_queue.publish(payload)

    print(f"[matcher] ride={payload.ride_id} round {payload.round_number}: "
          f"offered to {len(driver_ids)} driver(s)")


async def main() -> None:
    await rabbitmq.connect()
    await ride_queue.subscribe(handle_ride_requested)
    print(f"[matcher] consuming {ride_queue.name}")
    try:
        await asyncio.Future()  # run until cancelled
    finally:
        await ride_queue.close()
        await rabbitmq.close()


if __name__ == "__main__":
    asyncio.run(main())
