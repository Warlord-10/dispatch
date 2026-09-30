import uuid
from collections.abc import Awaitable, Callable

import aio_pika
from aio_pika.abc import (
    AbstractIncomingMessage,
    AbstractRobustChannel,
    AbstractRobustQueue,
)
from pydantic import BaseModel

from config import OFFER_TTL_S, PREFETCH
from messaging.connection import rabbitmq


class RideQueuePayload(BaseModel):
    ride_id: uuid.UUID
    rider_id: int
    pickup_lat: float
    pickup_lng: float
    dest_lat: float
    dest_lng: float
    round_number: int


class RideQueue:
    name = "ride.requested"
    dlx = "dispatch.dlx"
    dlq = "ride.requested.dlq"

    def __init__(self):
        self._declared = False
        self._channel: AbstractRobustChannel | None = None

    async def declare(self, channel: AbstractRobustChannel) -> AbstractRobustQueue:
        """Idempotent. The arguments must match the live queue exactly, or the
        broker closes the channel with PRECONDITION_FAILED."""
        dlx = await channel.declare_exchange(
            self.dlx, aio_pika.ExchangeType.DIRECT, durable=True
        )
        dlq = await channel.declare_queue(self.dlq, durable=True)
        await dlq.bind(dlx, routing_key=self.name)
        return await channel.declare_queue(
            self.name,
            durable=True,
            arguments={
                "x-dead-letter-exchange": self.dlx,
                "x-dead-letter-routing-key": self.name,
            },
        )

    async def publish(self, payload: RideQueuePayload) -> None:
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

    async def subscribe(
        self,
        handler: Callable[[RideQueuePayload], Awaitable[None]],
        prefetch: int = PREFETCH,
    ) -> None:
        self._channel = await rabbitmq.new_channel(prefetch)
        queue = await self.declare(self._channel)

        async def on_message(message: AbstractIncomingMessage) -> None:
            # requeue=False: a bad payload or a failing handler goes to the DLQ
            # instead of looping. Validation runs inside, so garbage is rejected
            # before the handler touches the database.
            async with message.process(requeue=False):
                await handler(RideQueuePayload.model_validate_json(message.body))

        await queue.consume(on_message)

    async def close(self) -> None:
        if self._channel is not None:
            await self._channel.close()
            self._channel = None


class RideRetryQueue:
    """The 15s wait between offer rounds, done by the broker.

    Nobody consumes this queue. A message sits here for OFFER_TTL_S, expires,
    and is dead-lettered through the default exchange straight back into
    ride.requested, where the worker runs the next round. A consumer here would
    take messages immediately and skip the wait, so there is no subscribe().
    """

    name = "ride.requested.retry"

    def __init__(self):
        self._declared = False

    async def declare(self, channel: AbstractRobustChannel) -> AbstractRobustQueue:
        """Idempotent. The arguments must match the live queue exactly, or the
        broker closes the channel with PRECONDITION_FAILED."""
        return await channel.declare_queue(
            self.name,
            durable=True,
            arguments={
                # One TTL for the whole queue, not per message: RabbitMQ only
                # expires from the head, so mixed per-message TTLs would let a
                # long one hold back short ones behind it.
                "x-message-ttl": OFFER_TTL_S * 1000,
                "x-dead-letter-exchange": "",  # default exchange: routes by queue name
                "x-dead-letter-routing-key": RideQueue.name,
            },
        )

    async def publish(self, payload: RideQueuePayload) -> None:
        channel = await rabbitmq.publisher_channel()
        if not self._declared:
            await self.declare(channel)
            self._declared = True

        message = aio_pika.Message(
            body=payload.model_dump_json().encode(),
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        )
        await channel.default_exchange.publish(
            message, routing_key=self.name, mandatory=True
        )


ride_queue = RideQueue()
ride_retry_queue = RideRetryQueue()
