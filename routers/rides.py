import uuid

from fastapi import APIRouter, Depends, Header, status
from sqlalchemy.ext.asyncio import AsyncSession

from db import get_session
from schemas.rides import RideAction, RideCreate, RideOut
from services.offers import accept_ride_offer, reject_ride_offer
from services.ride_booking import (
    get_ride as fetch_ride,
    book_ride,
    cancel_ride,
    complete_ride,
)

router = APIRouter(prefix="/rides", tags=["rides"])


@router.post("", response_model=RideOut, status_code=status.HTTP_202_ACCEPTED)
async def create_ride(
    data: RideCreate,
    session: AsyncSession = Depends(get_session),
    # Typed as UUID so a malformed key is a 422 before any of our code runs.
    idempotency_key: uuid.UUID | None = Header(default=None),
):
    """Accepted for matching, not matched. Always returns status='pending'.

    202, not 201: the ride row exists, but the thing the rider asked for (a
    driver) happens later in the worker. Poll GET /rides/{id} for the outcome.

    Send the same Idempotency-Key on every retry of one booking: a repeat
    returns the original ride instead of creating a second one.
    """
    return await book_ride(session, data, data.rider_id, idempotency_key)


@router.get("/{ride_id}", response_model=RideOut)
async def get_ride(ride_id: uuid.UUID, session: AsyncSession = Depends(get_session)):
    return await fetch_ride(session, ride_id)


@router.post("/{ride_id}/complete", response_model=RideOut)
async def finish_ride(ride_id: uuid.UUID, session: AsyncSession = Depends(get_session)):
    return await complete_ride(session, ride_id)


@router.post("/{ride_id}/cancel", response_model=RideOut)
async def abort_ride(ride_id: uuid.UUID, session: AsyncSession = Depends(get_session)):
    return await cancel_ride(session, ride_id)


@router.post("/{ride_id}/accept", response_model=RideOut)
async def accept(
    ride_id: uuid.UUID,
    data: RideAction,
    session: AsyncSession = Depends(get_session),
):
    """Driver accepts an offer. First one wins; the rest get 409."""
    return await accept_ride_offer(session, ride_id, data.driver_id)


@router.post("/{ride_id}/reject", status_code=status.HTTP_204_NO_CONTENT)
async def reject(
    ride_id: uuid.UUID,
    data: RideAction,
    session: AsyncSession = Depends(get_session),
):
    """Driver declines. The ride stays pending for everyone else offered it."""
    await reject_ride_offer(session, ride_id, data.driver_id)
