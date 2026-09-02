from typing import Annotated

from pydantic import Field

# Coordinates arrive from clients, so bound them here — the trust boundary.
# Unbounded floats sail through and produce silently wrong distances.
Latitude = Annotated[float, Field(ge=-90, le=90)]
Longitude = Annotated[float, Field(ge=-180, le=180)]
