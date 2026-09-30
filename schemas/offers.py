import uuid
from datetime import datetime

from pydantic import BaseModel


class RideOffer(BaseModel):
    """What a driver's app is shown when a ride is offered to them."""

    ride_id: uuid.UUID
    rider_id: int
    driver_id: int
    # Same instant the accept check measures from (the approached hash), so the
    # countdown on the phone and the server's cutoff cannot disagree.
    expires_at: datetime
    pickup_lat: float
    pickup_lng: float
    dest_lat: float
    dest_lng: float
