from pydantic import BaseModel

from enums import DriverStatus
from schemas.geo import Latitude, Longitude
from schemas.registration import DriverOut
from schemas.rides import RideOut


class DriverStatusUpdate(BaseModel):
    """Eligibility only. Position arrives via the location ping, which is the
    sole writer of driver coordinates now that Postgres no longer stores them."""

    status: DriverStatus


class DriverPing(BaseModel):
    lat: Latitude
    lng: Longitude


class DriverPresence(BaseModel):
    driver_id: int
    status: DriverStatus | None      # None = never seen by the index
    online: bool                     # pinged inside the staleness window
    dispatchable: bool               # available AND online
    lat: float | None = None
    lng: float | None = None
    last_seen_epoch: float | None = None
    seconds_since_ping: float | None = None


class DriverSession(BaseModel):
    """Everything a driver app needs to redraw itself after (re)connecting.

    The socket only carries changes; a change sent while the socket was down is
    gone. So on every connect the app fetches this first, then applies deltas.
    """

    driver: DriverOut               # status here is Postgres, the source of truth
    presence: DriverPresence        # what the dispatch index currently believes
    current_ride: RideOut | None    # the one assigned ride, if any
