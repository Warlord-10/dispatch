import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from db import Session, init_db
from messaging.connection import rabbitmq
from routers import drivers, registration, rides
from services.drivers import DriverNotFound, rebuild_status_index
from services.geo_index import sweeper_loop
from services.offers import DriverUnavailable, NotOffered, OfferExpired, RideAlreadyTaken
from services.registration import AlreadyRegistered
from services.ride_booking import IdempotencyKeyReused, InvalidRideState, RideNotFound


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()

    # Redis holds no durable copy of eligibility: rebuild it from Postgres so a
    # Redis restart does not leave the whole fleet undispatchable.
    async with Session() as session:
        loaded = await rebuild_status_index(session)
    print(f"status index rebuilt for {loaded} drivers")

    try:
        await rabbitmq.connect()
    except Exception as exc:
        # The API still serves reads and registrations without a broker;
        # rides just will not be matched.
        print(f"rabbitmq unavailable: {exc!r}")

    sweeper = asyncio.create_task(sweeper_loop())
    try:
        yield
    finally:
        sweeper.cancel()
        await rabbitmq.close()


app = FastAPI(title="Dispatch", lifespan=lifespan)

app.include_router(registration.router)
app.include_router(drivers.router)
app.include_router(rides.router)


# Domain exceptions map to HTTP here, in one place. This is the only layer
# allowed to know about status codes — services stay HTTP-free so the Phase 5
# matcher worker can call them without a request.
_STATUS = {
    AlreadyRegistered: 409,
    InvalidRideState: 409,
    IdempotencyKeyReused: 422,
    RideAlreadyTaken: 409,
    DriverUnavailable: 409,
    OfferExpired: 409,
    NotOffered: 403,
    RideNotFound: 404,
    DriverNotFound: 404,
}

for exc_type, code in _STATUS.items():
    @app.exception_handler(exc_type)
    async def _handle(request: Request, exc: Exception, code: int = code):
        return JSONResponse(status_code=code, content={"detail": str(exc)})


@app.get("/health")
def health_check():
    return {"message": "OK"}
