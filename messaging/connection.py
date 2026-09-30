# messaging/connection.py
import aio_pika
from aio_pika.abc import AbstractRobustChannel, AbstractRobustConnection

from config import PREFETCH, RABBITMQ_URL


class RabbitMQ:
    """One connection per process. Hands out channels and knows nothing about queues."""

    def __init__(self, url: str = RABBITMQ_URL):
        self._url = url
        self._connection: AbstractRobustConnection | None = None
        self._publisher: AbstractRobustChannel | None = None

    async def connect(self) -> None:
        if self._connection is not None:
            return
        self._connection = await aio_pika.connect_robust(self._url)
        # on_return_raises: a mandatory publish nobody can route raises
        # instead of being logged and forgotten.
        self._publisher = await self._connection.channel(
            publisher_confirms=True, on_return_raises=True)

    def _require(self) -> AbstractRobustConnection:
        if self._connection is None:
            raise RuntimeError("RabbitMQ.connect() was not called")
        return self._connection

    async def publisher_channel(self) -> AbstractRobustChannel:
        """Shared by all publishers. Confirms on, so publish() waits for the broker's ack."""
        self._require()
        return self._publisher

    async def new_channel(self, prefetch: int = PREFETCH) -> AbstractRobustChannel:
        """One per consumer: a channel error then kills only that consumer."""
        channel = await self._require().channel()
        await channel.set_qos(prefetch_count=prefetch)
        return channel

    async def close(self) -> None:
        if self._connection is not None:
            await self._connection.close()
        self._connection = None
        self._publisher = None


rabbitmq = RabbitMQ()