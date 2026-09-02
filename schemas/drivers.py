from typing import Literal

from pydantic import BaseModel, model_validator

from schemas.geo import Latitude, Longitude


class DriverStatusUpdate(BaseModel):
    """Demo stand-in for the real location ping endpoint (Phase 1)."""

    status: Literal["offline", "available", "assigned"]
    lat: Latitude | None = None
    lng: Longitude | None = None

    @model_validator(mode="after")
    def location_required_when_available(self):
        # A driver with no coordinates is invisible to match_ride, so going
        # 'available' without them would look like a silent no-op.
        if self.status == "available" and (self.lat is None or self.lng is None):
            raise ValueError("lat and lng are required to go available")
        return self
