"""Domain vocabulary. Imports nothing, so every layer can depend on it.

StrEnum (not `class X(str, Enum)`): members are real strings, so they compare
equal to what Postgres returns and render cleanly in f-strings. The older form
leaks "RideStatus.ASSIGNED" into string interpolation.
"""

from enum import StrEnum


class DriverStatus(StrEnum):
    OFFLINE = "offline"
    AVAILABLE = "available"
    ASSIGNED = "assigned"


class RideStatus(StrEnum):
    PENDING = "pending"
    ASSIGNED = "assigned"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed_out"

    @classmethod
    def terminal(cls) -> tuple["RideStatus", ...]:
        """States no transition may leave."""
        return (cls.COMPLETED, cls.CANCELLED, cls.TIMED_OUT)


class Recipient(StrEnum):
    """Whose socket. One gateway holds both kinds."""

    DRIVER = "driver"
    RIDER = "rider"


def sql_values(enum_cls) -> str:
    """Render an enum as a SQL tuple, so CHECK constraints cannot drift from code."""
    return str(tuple(m.value for m in enum_cls))
