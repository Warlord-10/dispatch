import uuid

from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession

from db import get_session
from schemas.rides import RideCreate, RideOut
from services.ride_booking import (
    get_ride as fetch_ride,
    book_ride,
    cancel_ride,
    complete_ride,
)

router = APIRouter(prefix="/rides", tags=["rides"])


@router.post("", response_model=RideOut, status_code=status.HTTP_201_CREATED)
async def create_ride(data: RideCreate, session: AsyncSession = Depends(get_session)):
    """Returns the ride whether or not a driver was found.

    status='assigned' means matched, status='pending' means no supply nearby.
    A 201 either way: the ride row exists, so reporting an error would be a lie.
    """
    return await book_ride(session, data, data.rider_id)


@router.get("/{ride_id}", response_model=RideOut)
async def get_ride(ride_id: uuid.UUID, session: AsyncSession = Depends(get_session)):
    return await fetch_ride(session, ride_id)


@router.post("/{ride_id}/complete", response_model=RideOut)
async def finish_ride(ride_id: uuid.UUID, session: AsyncSession = Depends(get_session)):
    return await complete_ride(session, ride_id)


@router.post("/{ride_id}/cancel", response_model=RideOut)
async def abort_ride(ride_id: uuid.UUID, session: AsyncSession = Depends(get_session)):
    return await cancel_ride(session, ride_id)
