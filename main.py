from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from db import init_db
from routers import drivers, registration, rides
from services.drivers import DriverNotFound
from services.registration import AlreadyRegistered
from services.ride_booking import InvalidRideState, RideNotFound


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield


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
