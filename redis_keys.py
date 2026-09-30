"""The Redis keyspace, in one place.

Redis has no schema and no namespacing: a mistyped key returns empty rather
than raising, and two modules picking the same name for different structures
fail as WRONGTYPE at best. Declaring every key here makes both visible.

Each entry notes its type and how it is reclaimed — a key with neither a TTL
nor a sweeper is a memory leak on a timer.
"""

NS = "dispatch"

# GEO  — the dispatchable subset; search reads only this. Sweeper + explicit removal.
DRIVERS_AVAILABLE_GEO = f"{NS}:drivers:available:geo"

# HASH — driver_id -> DriverStatus. Written only on status transitions, which are
# rare, so it cannot drift by volume. NEVER swept: it is what lets a driver who
# lost signal become dispatchable again the moment they ping. Rebuilt from
# Postgres at startup.
DRIVERS_STATUS = f"{NS}:drivers:status"


# HASH — driver_id -> epoch the offer was sent, for one ride. Keys exclude the
# driver from later rounds; values bound the accept window. Reclaimed by TTL
# only, never deleted when the ride ends: late answers must still find their
# entry, so a losing driver hears 409 "taken", not 403 "never offered".
def ride_offers(ride_id) -> str:
    return f"{NS}:ride:{ride_id}:offered"


# STRING — "lng,lat,epoch" for one driver. Reclaimed by native TTL, so there is
# no sweeper for position or liveness. The key's existence means "we know where
# they were"; the embedded epoch means "they are online right now" — two
# different questions, and a TTL can only answer one of them.
def driver_position(driver_id) -> str:
    return f"{NS}:driver:{driver_id}:position"


# HASH — driver_id -> gateway id holding their socket. Per-field TTL (HEXPIRE,
# GATEWAY_CONN_TTL_S) refreshed on every socket ping; removed on disconnect,
# but only by the gateway that owns the entry. A gateway killed without
# cleanup stops refreshing, so its entries expire by themselves.
DRIVER_WEBSOCKET_CONN = f"{NS}:driver:websocket:connection"

# HASH — rider_id -> gateway id. Same lifecycle as DRIVER_WEBSOCKET_CONN.
RIDER_WEBSOCKET_CONN = f"{NS}:rider:websocket:connection"
