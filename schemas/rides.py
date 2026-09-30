import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict

from enums import RideStatus
from schemas.geo import Latitude, Longitude


class RideRequest(BaseModel):
    """What the matcher needs: where from, where to."""

    pickup_lng: Longitude
    pickup_lat: Latitude
    dest_lng: Longitude
    dest_lat: Latitude


class RideCreate(RideRequest):
    # ponytail: rider_id in the body until there is auth. Phase 1 has no
    # identity layer, so anyone can book as anyone.
    rider_id: int


class RideOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    rider_id: int
    driver_id: int | None
    status: RideStatus
    pickup_lng: float
    pickup_lat: float
    dest_lng: float
    dest_lat: float
    requested_at: datetime
    assigned_at: datetime | None


class RideAction(BaseModel):
    """Who is acting. ponytail: driver_id in the body until there is auth."""

    driver_id: int
