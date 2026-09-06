import os

import redis.asyncio as redis
from dotenv import load_dotenv

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6380")

# Async client: the sync one blocks the event loop on every command, which
# stalls every other in-flight request on this worker.
cache = redis.from_url(REDIS_URL, decode_responses=True)
