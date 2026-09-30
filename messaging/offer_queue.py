import uuid
from datetime import datetime

import aio_pika
from aio_pika.abc import AbstractRobustChannel, AbstractRobustQueue
from pydantic import BaseModel

from config import OFFER_TTL_S
from messaging.connection import rabbitmq


class OfferQueuePayload(BaseModel):
    ride_id: uuid.UUID
    rider_id: int
    driver_id: int
    # Same instant the accept check measures from (the approached hash), so the
    # countdown on the phone and the server's cutoff cannot disagree.
    expires_at: datetime
    pickup_lat: float
    pickup_lng: float
    dest_lat: float
    dest_lng: float


class OfferQueue:
    name = "ride.offers"

    def __init__(self):
        self._declared = False
        self._channel: AbstractRobustChannel | None = None

    async def declare(self, channel: AbstractRobustChannel) -> AbstractRobustQueue:
        """Idempotent. The arguments must match the live queue exactly, or the
        broker closes the channel with PRECONDITION_FAILED."""
        # No DLQ on purpose: an offer past its accept window is worthless, so
        # there is nothing to replay. Expired or failed offers are just dropped.
        return await channel.declare_queue(
            self.name,
            durable=True,
            arguments={"x-message-ttl": OFFER_TTL_S * 1000},
        )

    async def publish(self, payload: OfferQueuePayload) -> None:
        channel = await rabbitmq.publisher_channel()
        # The default exchange drops messages for a queue that does not exist,
        # so the publisher cannot rely on the worker having declared it.
        if not self._declared:
            await self.declare(channel)
            self._declared = True

        message = aio_pika.Message(
            body=payload.model_dump_json().encode(),
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,  # survives a broker restart
        )
        # mandatory: an unroutable message raises instead of vanishing
        await channel.default_exchange.publish(
            message, routing_key=self.name, mandatory=True
        )


offer_queue = OfferQueue()
