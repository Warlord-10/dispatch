"""Matcher worker. Run: uv run python -m services.workers.dispatch_worker

Consumes ride requests and fans them out to nearby drivers. A separate process
from the API on purpose: matching must not compete with request serving, and
you scale the two independently.
"""

import asyncio
import logging
import time
from datetime import datetime, timezone

from aio_pika.exceptions import PublishError

from config import OFFER_TTL_S
from log_setup import setup_logging
from db import Session
from messaging.connection import rabbitmq
from messaging.ride_queue import RideQueuePayload, ride_queue, ride_retry_queue
from services.dispatch_engine import (
    NoDriverAvailable,
    RideNotPending,
    RideTimedOut,
    dispatch_ride,
)
from enums import Recipient
from messaging.push_queue import PushMessage, push_queue
from schemas.offers import RideOffer
from services.connections import gateways_for
from services.offers import record_offers

log = logging.getLogger(__name__)


async def handle_ride_requested(payload: RideQueuePayload) -> None:
    payload.round_number += 1

    async with Session() as session:
        try:
            driver_ids = await dispatch_ride(session, payload)
        except (RideNotPending, RideTimedOut) as outcome:
            # The loop ends here. Return (ack), never raise: raising means
            # "this message failed" and would park a normal outcome in the DLQ.
            log.info("%s", outcome)
            return
        except NoDriverAvailable:
            driver_ids = []  # no offers this round, but still schedule the next

    # Session closed before publishing: holding a pooled Postgres connection
    # through N publishes starves the other in-flight rides (pool 10, prefetch 20).
    offered: list[str] = []
    if driver_ids:
        offered_at = time.time()
        expires_at = datetime.fromtimestamp(offered_at + OFFER_TTL_S, timezone.utc)
        gateways = await gateways_for(Recipient.DRIVER, driver_ids)

        for driver_id in driver_ids:
            gateway = gateways.get(driver_id)
            if gateway is None:
                # No live socket: they cannot see an offer, so don't spend them.
                # They stay eligible for the next round, when they may be back.
                continue
            offer = RideOffer(
                ride_id=payload.ride_id,
                rider_id=payload.rider_id,
                driver_id=int(driver_id),
                pickup_lat=payload.pickup_lat,
                pickup_lng=payload.pickup_lng,
                dest_lat=payload.dest_lat,
                dest_lng=payload.dest_lng,
                expires_at=expires_at,
            )
            msg = PushMessage(
                recipient=Recipient.DRIVER,
                recipient_id=int(driver_id),
                body={"type": "ride_offer", **offer.model_dump(mode="json")},
            )
            try:
                await push_queue.publish(gateway, msg)
            except PublishError:
                continue  # that gateway died; its exclusive queue went with it
            offered.append(driver_id)

        # Recorded after publishing, and only the offers that reached a gateway:
        # a driver recorded but never offered would be excluded for nothing.
        # The reverse risk, a driver accepting before being recorded, needs a
        # human tap inside the milliseconds between these two lines.
        await record_offers(payload.ride_id, offered, now=offered_at)

    # Unconditional: an empty round still schedules the next one, and a ride
    # taken during the next 15s is caught on its way back.
    await ride_retry_queue.publish(payload)

    # The ride id is already in ctx=, so the message only says what happened.
    log.info("round %d: offered to %d of %d nearby driver(s)",
             payload.round_number, len(offered), len(driver_ids))


async def main() -> None:
    setup_logging("worker")
    await rabbitmq.connect()
    await ride_queue.subscribe(handle_ride_requested)
    log.info("consuming %s", ride_queue.name)
    try:
        await asyncio.Future()  # run until cancelled
    finally:
        await ride_queue.close()
        await rabbitmq.close()


if __name__ == "__main__":
    asyncio.run(main())
