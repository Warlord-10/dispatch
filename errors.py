"""The base for expected failures. Imports nothing, so every layer can use it.

A DispatchError is a request the rules refuse: a ride already taken, an offer
expired, a phone number in use. Not a bug. Each subclass maps to one HTTP status
in main.py, the only place that knows about HTTP, and every one reaches the
client with a stable machine-readable `code` next to the human `detail`:

  {"detail": "offer for ride 7f3a… expired 4s ago", "code": "offer_expired", "request_id": "…"}

Clients branch on `code`, never on the wording of `detail`, so the wording can
be improved without breaking an app.
"""

import re


class DispatchError(Exception):
    @property
    def code(self) -> str:
        """RideAlreadyTaken -> ride_already_taken. Derived, so it cannot drift."""
        return re.sub(r"(?<!^)(?=[A-Z])", "_", type(self).__name__).lower()
