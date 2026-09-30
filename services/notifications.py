"""Tell the people on a ride that it changed.

Best effort by design. It runs after the change is committed, so it must never
fail the request that made the change: the rider's cancel succeeded whether or
not their phone hears about it. A missed push is repaired by the client's resync
on reconnect (GET /rides/{id}, GET /drivers/{id}/session). A crash between the
commit and the push loses it outright; that is the dual write the Phase 8 outbox
closes.
"""

from aio_pika.exceptions import PublishError

from enums import Recipient
from messaging.push_queue import PushMessage, push_queue
from models import Ride
from schemas.rides import RideOut
from services.connections import gateways_for


async def push(recipient: Recipient, recipient_id: int, body: dict) -> bool:
    """Deliver to whichever gateway holds their socket. False if none does."""
    gateway = (await gateways_for(recipient, [recipient_id])).get(str(recipient_id))
    if gateway is None:
        return False
    try:
        await push_queue.publish(
            gateway, PushMessage(recipient=recipient, recipient_id=recipient_id, body=body)
        )
    except PublishError:
        return False  # that gateway died; its queue went with it
    return True


async def push_ride_update(ride: Ride) -> None:
    """The whole ride, not a delta: the client replaces its copy, so an update
    that arrives late or twice still leaves it correct."""
    body = {"type": "ride_update", "ride": RideOut.model_validate(ride).model_dump(mode="json")}
    targets = [(Recipient.RIDER, ride.rider_id)]
    if ride.driver_id is not None:
        targets.append((Recipient.DRIVER, ride.driver_id))
    for recipient, recipient_id in targets:
        try:
            await push(recipient, recipient_id, body)
        except Exception as exc:  # Redis or broker trouble: log it, never fail the caller
            print(f"push failed: ride {ride.id} to {recipient} {recipient_id}: {exc!r}")
