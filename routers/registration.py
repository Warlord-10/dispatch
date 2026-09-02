from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession

from db import get_session
from schemas.registration import DriverOut, DriverRegister, RiderOut, RiderRegister
from services.registration import register_driver, register_rider

router = APIRouter(tags=["registration"])


@router.post("/riders", response_model=RiderOut, status_code=status.HTTP_201_CREATED)
async def create_rider(data: RiderRegister, session: AsyncSession = Depends(get_session)):
    return await register_rider(session, data)


@router.post("/drivers", response_model=DriverOut, status_code=status.HTTP_201_CREATED)
async def create_driver(data: DriverRegister, session: AsyncSession = Depends(get_session)):
    return await register_driver(session, data)
