"""Which gateway process holds a driver's or rider's socket.

Each gateway only knows its own sockets, so anything that wants to push to a
phone looks the owner up here and publishes to that gateway's queue.
"""

from config import GATEWAY_CONN_TTL_S
from enums import Recipient
from redis_client import redis_client
from redis_keys import DRIVER_WEBSOCKET_CONN, RIDER_WEBSOCKET_CONN

_KEYS = {Recipient.DRIVER: DRIVER_WEBSOCKET_CONN, Recipient.RIDER: RIDER_WEBSOCKET_CONN}

# Delete only if the entry is still ours. Someone who reconnected to another
# gateway has already overwritten it; the old gateway's cleanup must not erase
# the new mapping. Check-and-delete in one script, so nothing runs in between.
_UNREGISTER = redis_client.register_script("""
if redis.call('HGET', KEYS[1], ARGV[1]) == ARGV[2] then
    return redis.call('HDEL', KEYS[1], ARGV[1])
end
return 0
""")


async def register(recipient: Recipient, recipient_id: int, gateway_id: str) -> None:
    """Called on connect and on every socket ping. Last writer wins: whichever
    gateway they are actively pinging through is where they are."""
    key = _KEYS[recipient]
    pipe = redis_client.pipeline(transaction=True)
    pipe.hset(key, str(recipient_id), gateway_id)
    pipe.hexpire(key, GATEWAY_CONN_TTL_S, str(recipient_id))
    await pipe.execute()


async def unregister(recipient: Recipient, recipient_id: int, gateway_id: str) -> None:
    await _UNREGISTER(keys=[_KEYS[recipient]], args=[str(recipient_id), gateway_id])


async def gateways_for(recipient: Recipient, ids) -> dict[str, str]:
    """id -> gateway id, for those currently connected anywhere."""
    ids = [str(i) for i in ids]
    if not ids:
        return {}
    found = await redis_client.hmget(_KEYS[recipient], ids)
    return {i: g for i, g in zip(ids, found) if g}
