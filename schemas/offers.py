import uuid

from pydantic import BaseModel, ConfigDict


class RideOffer(BaseModel):
    """What a driver is shown when a ride is offered to them."""

    model_config = ConfigDict(frozen=True)

    ride_id: uuid.UUID
    pickup_lat: float
    pickup_lng: float
    dest_lat: float
    dest_lng: float
    expires_in_s: int