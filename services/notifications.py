"""How a ride offer reaches a driver's device.

Deliberately abstract. RabbitMQ distributes work among *our* workers; it does
not reach phones. The transport is WebSocket or push, and that is the next step
— so the dispatch engine depends on this interface and nothing else.

The offer rows in Redis are the source of truth for "who was approached".
A notification is a nudge on top; if it is lost, the driver's next poll still
sees the offer.
"""

from abc import ABC, abstractmethod

from schemas.offers import RideOffer


class DriverNotifier(ABC):
    @abstractmethod
    async def send_ride_offer(self, driver_id: int, offer: RideOffer) -> None:
        """Push one offer to one driver."""

    @abstractmethod
    async def withdraw_ride_offer(self, driver_id: int, ride_id) -> None:
        """Tell a driver the offer is gone — someone else took it, or it expired."""


class LoggingNotifier(DriverNotifier):
    """Placeholder until the WebSocket layer exists. Makes the fan-out observable
    and lets the whole engine be tested without a transport."""

    def __init__(self):
        self.sent: list[tuple[int, RideOffer]] = []
        self.withdrawn: list[tuple[int, str]] = []

    async def send_ride_offer(self, driver_id: int, offer: RideOffer) -> None:
        self.sent.append((driver_id, offer))
        print(f"[offer] ride={offer.ride_id} -> driver={driver_id}")

    async def withdraw_ride_offer(self, driver_id: int, ride_id) -> None:
        self.withdrawn.append((driver_id, str(ride_id)))
        print(f"[withdraw] ride={ride_id} -> driver={driver_id}")


_notifier: DriverNotifier = LoggingNotifier()


def get_notifier() -> DriverNotifier:
    return _notifier


def set_notifier(notifier: DriverNotifier) -> None:
    """Swap in the WebSocket implementation when it exists, or a fake in tests."""
    global _notifier
    _notifier = notifier
