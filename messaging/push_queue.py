"""Messages for phones, routed to the one gateway process holding the socket.

A direct exchange. Each gateway binds its own queue with its gateway id as the
routing key; publishers look the id up in Redis (services.connections) and
publish with it, so no gateway receives, or forwards, another gateway's traffic.

Each gateway's queue is exclusive: the broker deletes it the moment that
gateway's connection dies. A publish to a dead gateway then has no queue to land
in and, being mandatory, raises PublishError at once. Messages queued for a dead
gateway are lost with it. That is acceptable for everything sent here: offers are
worthless after OFFER_TTL_S, and a ride update missed is repaired by the client's
resync on reconnect.
"""

from collections.abc import Awaitable, Callable

import aio_pika
from aio_pika.abc import AbstractIncomingMessage, AbstractRobustChannel, AbstractRobustExchange
from pydantic import BaseModel

from config import OFFER_TTL_S, PREFETCH
from enums import Recipient
from messaging.connection import rabbitmq


class PushMessage(BaseModel):
    recipient: Recipient
    recipient_id: int
    body: dict  # sent to the socket verbatim; always carries a "type"


class PushQueue:
    exchange_name = "dispatch.push"

    def __init__(self):
        self._exchange: AbstractRobustExchange | None = None
        self._channel: AbstractRobustChannel | None = None

    async def _declare_exchange(self, channel: AbstractRobustChannel) -> AbstractRobustExchange:
        return await channel.declare_exchange(
            self.exchange_name, aio_pika.ExchangeType.DIRECT, durable=True
        )

    async def publish(self, gateway_id: str, msg: PushMessage) -> None:
        """Raises aio_pika.exceptions.PublishError if no live gateway has that id."""
        if self._exchange is None:
            self._exchange = await self._declare_exchange(await rabbitmq.publisher_channel())
        # Not persistent: the queue dies with its gateway, so there is nothing
        # a broker restart could usefully restore.
        message = aio_pika.Message(body=msg.model_dump_json().encode(), content_type="application/json")
        await self._exchange.publish(message, routing_key=gateway_id, mandatory=True)

    async def subscribe(
        self,
        gateway_id: str,
        handler: Callable[[PushMessage], Awaitable[None]],
        prefetch: int = PREFETCH,
    ) -> None:
        self._channel = await rabbitmq.new_channel(prefetch)
        exchange = await self._declare_exchange(self._channel)
        queue = await self._channel.declare_queue(
            f"gateway.{gateway_id}",
            exclusive=True,
            # Anything still queued after this long is stale: an offer past its
            # accept window, or a ride update the client will have resynced past.
            arguments={"x-message-ttl": OFFER_TTL_S * 1000},
        )
        await queue.bind(exchange, routing_key=gateway_id)

        async def on_message(message: AbstractIncomingMessage) -> None:
            async with message.process(requeue=False):
                await handler(PushMessage.model_validate_json(message.body))

        await queue.consume(on_message)

    async def close(self) -> None:
        if self._channel is not None:
            await self._channel.close()
            self._channel = None


push_queue = PushQueue()
