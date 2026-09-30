"""Environment-backed settings and the knobs you will tune under load.

Every value below gets adjusted while reading a benchmark. Hunting them across
four modules mid-run is miserable, so they live together.
"""

import os

from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql+asyncpg://dispatch:dispatch@localhost:5433/dispatch"
)
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6380")
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5673/")

# --- matching ---
RADIUS_LADDER_KM = (3.0, 5.0, 8.0)   # escalation rungs, widest last
CANDIDATE_COUNT = 20                 # over-fetch: some will be stale or lose the race

# --- driver liveness ---
PING_INTERVAL_S = 10                 # expected client cadence
STALE_AFTER_S = 30                   # 3 missed pings -> not dispatchable
POSITION_TTL_S = 3600                # how long a last-known position is kept.
                                     # Deliberately >> STALE_AFTER_S: 30s decides
                                     # dispatchability, this decides whether we
                                     # still know where they were.
SWEEP_INTERVAL_S = 60                # only trims the available geo set now

# --- messaging ---
PREFETCH = 20                        # unacked messages per consumer; without this
                                     # one worker grabs the whole queue

# --- offers ---
OFFER_TTL_S = 15                     # how long a driver has to accept
MAX_OFFER_ROUNDS = 3                 # then the ride is timed_out
APPROACHED_TTL_S = 15 * 60           # approached hash self-cleans; must outlive
                                     # OFFER_TTL_S * MAX_OFFER_ROUNDS (45s)
