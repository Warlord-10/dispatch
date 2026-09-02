import uuid
from datetime import datetime, timezone

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from models import Driver, Ride


# Earth radius in kilometers (mean radius)
EARTH_RADIUS_KM = 6371.0088


class NoDriverFound(Exception):
    """Raised when no available driver is nearby."""
    pass


async def match_ride(session: AsyncSession, ride_id: uuid.UUID | int) -> Ride:
    """Finds the closest available driver to the ride's pickup location and assigns them.
    
    Uses the equirectangular approximation formula:
        φ₁, φ₂ = latitudes in radians
        λ₁, λ₂ = longitudes in radians
        Δφ = φ₂ − φ₁
        Δλ = λ₂ − λ₁
        x = Δλ · cos((φ₁ + φ₂) / 2)
        y = Δφ
        d = R · √(x² + y²)
    """
    ride = await session.get(Ride, ride_id)
    if ride is None:
        raise ValueError("Ride not found")

    # Pickup coordinates (φ₁, λ₁) in radians
    phi1 = func.radians(ride.pickup_lat)
    lambda1 = func.radians(ride.pickup_lng)

    # Driver coordinates (φ₂, λ₂) in radians
    phi2 = func.radians(Driver.lat)
    lambda2 = func.radians(Driver.lng)

    delta_phi = phi2 - phi1
    delta_lambda = lambda2 - lambda1
    mean_phi = (phi1 + phi2) / 2.0

    x = delta_lambda * func.cos(mean_phi)
    y = delta_phi
    distance = EARTH_RADIUS_KM * func.sqrt(x * x + y * y)

    # Query the closest driver who is available and has coordinates
    stmt = (
        select(Driver)
        .where(
            Driver.status == "available",
            Driver.lat.is_not(None),
            Driver.lng.is_not(None),
        )
        .order_by(distance)
        .limit(1)
    )

    result = await session.execute(stmt)
    driver = result.scalars().first()

    if driver is None:
        raise NoDriverFound("No available driver found")

    # Assign driver to ride
    ride.driver_id = driver.id
    ride.status = "assigned"
    ride.assigned_at = datetime.now(timezone.utc)
    driver.status = "assigned"

    await session.commit()
    await session.refresh(ride)
    return ride