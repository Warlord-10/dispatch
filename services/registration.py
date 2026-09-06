"""Registration for riders and drivers.

Knows nothing about HTTP — no FastAPI imports, no HTTPException. The router
translates AlreadyRegistered into a 409. Same reason the matcher worker in
Phase 5 will be able to call into services/ without an HTTP request.
"""

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from models import Driver, Rider
from schemas.registration import DriverRegister, RiderRegister


class AlreadyRegistered(Exception):
    """Phone number is already taken on that role."""

    def __init__(self, phone_number: str):
        self.phone_number = phone_number
        super().__init__(f"{phone_number} is already registered")


async def _insert(session: AsyncSession, row: Driver | Rider, phone_number: str):
    # Insert and let the unique index reject duplicates, rather than
    # SELECT-then-INSERT: two concurrent registrations would both see "free"
    # and both insert. The constraint is the only check without a race.
    session.add(row)
    try:
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        # Only a duplicate phone means "already registered". Mapping every
        # IntegrityError here hides unrelated violations (a primary-key
        # collision reports itself as a taken phone number) and sends you
        # debugging the wrong constraint.
        constraint = getattr(getattr(exc.orig, "__cause__", None), "constraint_name", "") or ""
        if "phone_number" in constraint:
            raise AlreadyRegistered(phone_number) from exc
        raise
    await session.refresh(row)
    return row


async def register_rider(session: AsyncSession, data: RiderRegister) -> Rider:
    return await _insert(
        session,
        Rider(name=data.name, phone_number=data.phone_number),
        data.phone_number,
    )


async def register_driver(session: AsyncSession, data: DriverRegister) -> Driver:
    return await _insert(
        session,
        Driver(
            name=data.name,
            phone_number=data.phone_number,
            car_number=data.car_number,
            car_model=data.car_model,
            status="offline",
        ),
        data.phone_number,
    )