"""The Redis connection.

Named redis_client, not cache: Redis holds the only copy of live driver
location. "Cache" would imply it is safe to drop, and it is not.
"""

import redis.asyncio as redis

from config import REDIS_URL

# Async client: the sync one blocks the event loop on every command, stalling
# every other in-flight request on this worker.
redis_client = redis.from_url(REDIS_URL, decode_responses=True)
