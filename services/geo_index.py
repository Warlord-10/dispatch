"""The Redis-side driver index. Nothing outside this module talks to Redis.

The index holds exactly the dispatchable drivers, so a search result needs no
status filtering. Losing it on a Redis restart is acceptable: it is an index
over data Postgres owns, and driver pings rebuild it.
"""

from cache import cache

AVAILABLE_DRIVERS_KEY = "drivers:available:geo"

# Escalation ladder, widest rung last. A loop with a cap, never recursion:
# "expand until I find someone" has no base case in a city with few drivers.
RADIUS_LADDER_KM = (3.0, 5.0, 8.0)


async def add_driver(driver_id: int, lat: float, lng: float) -> None:
    """Put a driver into the dispatchable set."""
    await cache.geoadd(AVAILABLE_DRIVERS_KEY, (lng, lat, driver_id))


async def remove_driver(driver_id: int) -> None:
    """Take a driver out — assigned, or gone offline. A geo set is a zset."""
    await cache.zrem(AVAILABLE_DRIVERS_KEY, driver_id)


async def driver_ping(driver_id: int, lat: float, lng: float) -> bool:
    """Refresh a driver's position, but only if they are already dispatchable.

    Membership is a ZSCORE lookup. Searching a zero-metre radius never matches,
    so it would silently drop every ping.
    """
    if await cache.zscore(AVAILABLE_DRIVERS_KEY, driver_id) is None:
        return False
    await add_driver(driver_id, lat, lng)
    return True


async def search_drivers_in_radius(
    lat: float, lng: float, ladder: tuple[float, ...] = RADIUS_LADDER_KM, count: int = 20
) -> list[str]:
    """Nearest dispatchable drivers, closest first. Empty if the widest rung finds none."""
    for radius in ladder:
        drivers = await cache.geosearch(
            AVAILABLE_DRIVERS_KEY,
            longitude=lng,
            latitude=lat,
            radius=radius,
            unit="km",
            sort="ASC",
            count=count
        )
        if drivers:
            return drivers
    return []
