from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from db import get_session
from schemas.drivers import DriverPing, DriverPresence, DriverSession, DriverStatusUpdate
from schemas.registration import DriverOut
from services.drivers import get_driver_session, get_presence, record_ping, set_driver_status

router = APIRouter(prefix="/drivers", tags=["drivers"])


@router.patch("/{driver_id}/status", response_model=DriverOut)
async def update_status(
    driver_id: int,
    data: DriverStatusUpdate,
    session: AsyncSession = Depends(get_session),
):
    """Eligibility change. Hits Postgres — rare, so that is fine."""
    return await set_driver_status(session, driver_id, data)


@router.post("/{driver_id}/location", response_model=DriverPresence)
async def location_ping(driver_id: int, data: DriverPing):
    """The hot path. No database session at all, by design."""
    return await record_ping(driver_id, data)


@router.get("/{driver_id}/presence", response_model=DriverPresence)
async def presence(driver_id: int):
    """Is this driver online, and are they dispatchable right now?"""
    return await get_presence(driver_id)


@router.get("/{driver_id}/session", response_model=DriverSession)
async def driver_session(driver_id: int, session: AsyncSession = Depends(get_session)):
    """Call on every (re)connect, before trusting the socket: current status,
    presence, and the assigned ride. The socket then carries only changes."""
    return await get_driver_session(session, driver_id)
