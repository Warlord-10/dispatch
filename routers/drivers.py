from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from db import get_session
from schemas.drivers import DriverStatusUpdate
from schemas.registration import DriverOut
from services.drivers import set_driver_status

router = APIRouter(prefix="/drivers", tags=["drivers"])


@router.patch("/{driver_id}/status", response_model=DriverOut)
async def update_status(
    driver_id: int,
    data: DriverStatusUpdate,
    session: AsyncSession = Depends(get_session),
):
    return await set_driver_status(session, driver_id, data)
