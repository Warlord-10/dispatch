import asyncio
import logging
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from log_setup import log_context, setup_logging

setup_logging("api")  # before the imports below create their loggers' records

from db import Session, init_db  # noqa: E402
from errors import DispatchError  # noqa: E402
from messaging.connection import rabbitmq  # noqa: E402
from routers import drivers, registration, rides  # noqa: E402
from services.drivers import DriverNotFound, rebuild_status_index  # noqa: E402
from services.geo_index import sweeper_loop  # noqa: E402
from services.offers import DriverUnavailable, NotOffered, OfferExpired, RideAlreadyTaken  # noqa: E402
from services.registration import AlreadyRegistered  # noqa: E402
from services.ride_booking import IdempotencyKeyReused, InvalidRideState, RideNotFound  # noqa: E402

log = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()

    # Redis holds no durable copy of eligibility: rebuild it from Postgres so a
    # Redis restart does not leave the whole fleet undispatchable.
    async with Session() as session:
        loaded = await rebuild_status_index(session)
    log.info("status index rebuilt for %d drivers", loaded)

    try:
        await rabbitmq.connect()
    except Exception as exc:
        # The API still serves reads and registrations without a broker;
        # rides just will not be matched. Loud, because that is an outage.
        log.error("rabbitmq unavailable, rides will not be matched: %r", exc)

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
# allowed to know about status codes — services stay HTTP-free so the matcher
# worker can call them without a request.
_STATUS: dict[type[DispatchError], int] = {
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


def _error(status: int, detail: str, code: str) -> JSONResponse:
    """The one error shape: `code` for machines, `detail` for people, and the
    request id that finds the matching log lines."""
    rid = log_context.get()
    return JSONResponse(
        status_code=status,
        content={"detail": detail, "code": code, "request_id": rid},
        headers={"X-Request-ID": rid},
    )


@app.exception_handler(DispatchError)
async def _domain_error(request: Request, exc: DispatchError):
    status = _STATUS.get(type(exc))
    if status is None:
        # A new DispatchError nobody mapped: still the client's problem, but
        # flag it so the table above gets its entry.
        log.warning("no HTTP status mapped for %s, answering 400", type(exc).__name__)
        status = 400
    # INFO, not WARNING: refusals are the rules working. 49 of 50 racing
    # accepts end here by design.
    log.info("%s %s -> %d %s: %s", request.method, request.url.path, status, exc.code, exc)
    return _error(status, str(exc), exc.code)


@app.middleware("http")
async def _request_context(request: Request, call_next):
    """Tag everything this request logs with one id, return it to the client,
    and turn an unexpected exception into a JSON 500 that points at the log."""
    # Honour an id from a proxy or the client, so one id spans the whole hop.
    rid = request.headers.get("X-Request-ID") or uuid.uuid4().hex[:12]
    token = log_context.set(rid)
    try:
        response = await call_next(request)
    except Exception:
        log.exception("%s %s -> 500 unhandled", request.method, request.url.path)
        return _error(500, "internal error, quote the request_id when reporting it", "internal_error")
    finally:
        log_context.reset(token)
    response.headers["X-Request-ID"] = rid
    return response


@app.get("/health")
def health_check():
    return {"message": "OK"}
