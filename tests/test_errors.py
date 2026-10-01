"""Error responses, request ids, and failures that must be logged, not lost.
Run: docker compose up -d && uv run python tests/test_errors.py
"""

import pathlib
import sys

# tests live in tests/, the app lives one level up
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

import asyncio
import logging
import uuid

import aio_pika
import httpx
from asgi_lifespan import LifespanManager

import routers.drivers
from main import app
from messaging.connection import rabbitmq
from messaging.ride_queue import RideQueuePayload, ride_queue
from services import offers
from services.workers.dispatch_worker import handle_ride_requested


class Capture(logging.Handler):
    """Collect records, with the ctx the app's filter would have attached."""

    def __init__(self):
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record):
        self.records.append(record)

    def find(self, text: str) -> list[logging.LogRecord]:
        return [r for r in self.records if text in r.getMessage()]


async def main():
    cap = Capture()
    root = logging.getLogger()
    cap.addFilter(root.handlers[0].filters[0])  # the filter that sets ctx=
    root.addHandler(cap)

    async with LifespanManager(app):
        t = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=t, base_url="http://t") as c:
            u = uuid.uuid4().int % 10**8
            rider = (await c.post("/riders", json={"name": "R", "phone_number": f"9{u:08d}0"})).json()["id"]
            d = (await c.post("/drivers", json={"name": "D", "phone_number": f"9{u:08d}1",
                                                "car_number": "KA01ER0001", "car_model": "Swift"})).json()["id"]

            # 1. every refusal carries a stable code and the request id
            r = await c.post("/riders", json={"name": "R", "phone_number": f"9{u:08d}0"})
            body = r.json()
            assert r.status_code == 409 and body["code"] == "already_registered", body
            assert body["request_id"] == r.headers["X-Request-ID"], (body, r.headers)
            assert "is already registered" in body["detail"]
            r = await c.get(f"/rides/{uuid.uuid4()}")
            assert r.status_code == 404 and r.json()["code"] == "ride_not_found", r.text
            print("1. refusals: stable code + human detail + request id (409 already_registered, 404 ride_not_found)")

            # 2. codes tell apart what the status alone cannot: three different 409s and a 403
            ride = (await c.post("/rides", json={"rider_id": rider, "pickup_lat": 1.0, "pickup_lng": 1.0,
                                                 "dest_lat": 1.1, "dest_lng": 1.1})).json()["id"]
            r = await c.post(f"/rides/{ride}/accept", json={"driver_id": d})
            assert (r.status_code, r.json()["code"]) == (403, "not_offered"), r.text
            await offers.record_offers(ride, [d], now=0)  # offered in 1970: long expired
            r = await c.post(f"/rides/{ride}/accept", json={"driver_id": d})
            assert (r.status_code, r.json()["code"]) == (409, "offer_expired"), r.text
            assert "expired" in r.json()["detail"] and "15s" in r.json()["detail"], r.text
            await offers.record_offers(ride, [d])
            r = await c.post(f"/rides/{ride}/accept", json={"driver_id": d})  # driver is offline
            assert (r.status_code, r.json()["code"]) == (409, "driver_unavailable"), r.text
            await c.post(f"/rides/{ride}/cancel")
            r = await c.post(f"/rides/{ride}/cancel")
            assert (r.status_code, r.json()["code"]) == (409, "invalid_ride_state"), r.text
            assert "from cancelled to cancelled" in r.json()["detail"], r.text
            print("2. not_offered / offer_expired / driver_unavailable / invalid_ride_state are distinguishable")

            # 3. a client-supplied request id is honoured end to end
            r = await c.get(f"/rides/{uuid.uuid4()}", headers={"X-Request-ID": "trace-me-123"})
            assert r.headers["X-Request-ID"] == "trace-me-123" and r.json()["request_id"] == "trace-me-123"
            assert any(rec.ctx == "trace-me-123" for rec in cap.find("ride_not_found")), \
                "the log line should carry the client's request id"
            print("3. X-Request-ID from the client is echoed and appears in the log line")

            # 4. a bug becomes a JSON 500 that points at its log line, not a bare text 500
            original = routers.drivers.get_presence

            async def boom(driver_id):
                raise RuntimeError("simulated bug")

            routers.drivers.get_presence = boom
            try:
                r = await c.get(f"/drivers/{d}/presence")
            finally:
                routers.drivers.get_presence = original
            body = r.json()
            assert r.status_code == 500 and body["code"] == "internal_error", r.text
            assert "simulated bug" not in r.text, "internals must not leak to the client"
            logged = cap.find("-> 500 unhandled")
            assert logged and logged[-1].ctx == body["request_id"] and logged[-1].exc_info, \
                "the 500 must be logged with its traceback under the same request id"
            print("4. unhandled exception -> JSON 500, no internals leaked, traceback logged under the request id")

        # 5/6. queue failures dead-letter AND leave one log line naming the ride
        ch = await rabbitmq.publisher_channel()
        await (await ride_queue.declare(ch)).purge()
        dlq = await ch.declare_queue(ride_queue.dlq, durable=True)
        await dlq.purge()
        await ride_queue.subscribe(handle_ride_requested)

        await ch.default_exchange.publish(aio_pika.Message(b'{"ride_id": "nope"}'), routing_key=ride_queue.name)
        ghost = uuid.uuid4()
        await ride_queue.publish(RideQueuePayload(ride_id=ghost, rider_id=1, pickup_lat=1, pickup_lng=1,
                                                  dest_lat=1, dest_lng=1, round_number=0))
        await asyncio.sleep(1)
        await ride_queue.close()

        dead = []
        while (m := await dlq.get(no_ack=True, fail=False)) is not None:
            dead.append(m.body)
        assert len(dead) == 2, dead
        malformed = cap.find("malformed message on ride.requested")
        assert len(malformed) == 1 and not malformed[0].exc_info, "one line, no traceback for garbage"
        failed = cap.find("failed on ride.requested, dead-lettered")
        assert len(failed) == 1 and failed[0].ctx == f"ride={ghost}" and failed[0].exc_info, \
            "a real failure is logged once, with its ride id and traceback"
        assert "RideNotFound" in logging.Formatter().formatException(failed[0].exc_info)
        print("5. malformed message -> DLQ, logged once without a traceback")
        print("6. ride that does not exist -> DLQ, logged once as RideNotFound with ctx=ride=<id>")

    root.removeHandler(cap)
    print("errors ok")


if __name__ == "__main__":
    asyncio.run(main())
