"""Book -> RabbitMQ -> matcher -> fan-out -> accept/reject.
Run: docker compose up -d && uv run python tests/test_dispatch_flow.py
"""

import pathlib
import sys

# tests live in tests/, the app lives one level up
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

import asyncio
import json
import uuid

import httpx
from asgi_lifespan import LifespanManager
from sqlalchemy import update

from db import Session
from enums import DriverStatus, RideStatus
from main import app
from messaging.connection import rabbitmq
from messaging.ride_queue import RideQueuePayload, ride_queue
from models import Driver
from redis_client import redis_client
from redis_keys import DRIVERS_AVAILABLE_GEO, DRIVERS_STATUS, ride_offers
from services import geo_index, offers
from services.dispatch_engine import NoDriverAvailable, RideNotPending, RideTimedOut, dispatch_ride
from services.ride_booking import timed_out_ride

BLR = (12.9352, 77.6245)


async def reset():
    async with Session() as s:
        await s.execute(update(Driver).values(status=DriverStatus.OFFLINE))
        await s.commit()
    keys = await redis_client.keys("dispatch:*")
    if keys:
        await redis_client.delete(*keys)
    # Other suites publish rides nobody consumes; drain_queue would pick those
    # up instead of ours.
    q = await ride_queue.declare(await rabbitmq.publisher_channel())
    await q.purge()


async def drain_queue():
    """Pull one message off the work queue, standing in for the worker process."""
    q = await ride_queue.declare(await rabbitmq.publisher_channel())
    msg = await q.get(no_ack=True, fail=False)
    return None if msg is None else json.loads(msg.body.decode())


async def main():
    async with LifespanManager(app):
        await reset()
        t = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=t, base_url="http://t") as c:
            u = uuid.uuid4().int % 10**8
            r = await c.post("/riders", json={"name": "R", "phone_number": f"9{u:08d}0"})
            rider = r.json()

            dids = []
            for n in (1, 2, 3):
                r = await c.post("/drivers", json={
                    "name": f"D{n}", "phone_number": f"9{u:08d}{n}",
                    "car_number": f"KA01AA000{n}", "car_model": "Swift"})
                assert r.status_code == 201, r.text
                dids.append(r.json()["id"])

            # available + a ping puts them in the dispatch index
            for i, did in enumerate(dids):
                await c.patch(f"/drivers/{did}/status", json={"status": "available"})
                await c.post(f"/drivers/{did}/location",
                             json={"lat": BLR[0] + i * 0.004, "lng": BLR[1]})
            assert await redis_client.zcard(DRIVERS_AVAILABLE_GEO) == 3
            print("1. three drivers available and pinging")

            # booking publishes to RabbitMQ; the ride itself stays pending
            body = {"rider_id": rider["id"], "pickup_lat": BLR[0], "pickup_lng": BLR[1],
                    "dest_lat": 12.97, "dest_lng": 77.64}
            r = await c.post("/rides", json=body)
            assert r.status_code == 202, r.text
            ride = r.json()
            assert ride["status"] == "pending"

            queued = await drain_queue()
            assert queued is not None, "nothing was published to RabbitMQ"
            assert queued["ride_id"] == ride["id"], queued
            print("2. ride published to RabbitMQ (UUID serialised correctly)")

            # the worker's job, run inline
            payload = RideQueuePayload.model_validate(queued)
            async with Session() as s:
                approached = await dispatch_ride(s, payload)
            await offers.record_offers(ride["id"], approached)
            assert sorted(int(d) for d in approached) == sorted(dids), approached
            print(f"3. matched {len(approached)} drivers, nearest first: {approached}")

            # rerunning must not re-offer — at-least-once delivery guarantees this happens
            async with Session() as s:
                try:
                    await dispatch_ride(s, payload)
                    raise AssertionError("redelivery re-offered the ride")
                except NoDriverAvailable:
                    pass
            print("4. redelivery is idempotent (approached hash excludes them)")

            # only drivers holding the offer may answer it
            r = await c.post(f"/rides/{ride['id']}/accept", json={"driver_id": 999_999_999})
            assert r.status_code == 403, r.text
            r = await c.post(f"/rides/{ride['id']}/reject", json={"driver_id": 999_999_999})
            assert r.status_code == 403, r.text
            print("4b. a driver never offered the ride gets 403 on accept and reject")

            # the accept window is enforced by the server clock
            key = ride_offers(ride["id"])
            real = await redis_client.hget(key, str(dids[0]))
            await redis_client.hset(key, str(dids[0]), float(real) - 16)
            r = await c.post(f"/rides/{ride['id']}/accept", json={"driver_id": dids[0]})
            assert r.status_code == 409 and "expired" in r.text, r.text
            await redis_client.hset(key, str(dids[0]), real)
            print("4c. accept after 15s -> 409 expired")

            # a reject leaves the ride pending for the others
            r = await c.post(f"/rides/{ride['id']}/reject", json={"driver_id": dids[0]})
            assert r.status_code == 204, r.text
            r = await c.get(f"/rides/{ride['id']}")
            assert r.json()["status"] == "pending"
            print("5. reject leaves the ride pending")

            # THE RACE: all three accept at once, exactly one may win
            results = await asyncio.gather(*[
                c.post(f"/rides/{ride['id']}/accept", json={"driver_id": d})
                for d in dids
            ], return_exceptions=True)
            codes = sorted(r.status_code for r in results)
            assert codes == [200, 409, 409], codes
            winner = [r for r in results if r.status_code == 200][0].json()
            assert winner["status"] == "assigned" and winner["driver_id"] in dids
            print(f"6. 3 simultaneous accepts -> {codes}, driver {winner['driver_id']} won")

            # the winner left the dispatch index; the losers are still available
            assert await geo_index.get_status(winner["driver_id"]) == DriverStatus.ASSIGNED
            assert str(winner["driver_id"]) not in await redis_client.zrange(
                DRIVERS_AVAILABLE_GEO, 0, -1)
            assert await redis_client.zcard(DRIVERS_AVAILABLE_GEO) == 2
            print("7. winner removed from the dispatch index, losers untouched")

            # what a reconnecting driver sees
            r = await c.get(f"/drivers/{winner['driver_id']}/session")
            assert r.status_code == 200, r.text
            sess = r.json()
            assert sess["driver"]["status"] == "assigned"
            assert sess["current_ride"]["id"] == ride["id"], sess
            assert sess["presence"]["online"] is True
            loser = next(d for d in dids if d != winner["driver_id"])
            assert (await c.get(f"/drivers/{loser}/session")).json()["current_ride"] is None
            assert (await c.get("/drivers/999999999/session")).status_code == 404
            print("7b. session: winner gets their assigned ride back, losers none, unknown driver 404")

            # completing frees them, position recovered from their own key
            r = await c.post(f"/rides/{ride['id']}/complete")
            assert r.status_code == 200 and r.json()["status"] == "completed", r.text
            assert await redis_client.zcard(DRIVERS_AVAILABLE_GEO) == 3
            print("8. completion re-added the driver using their position key")
            sess = (await c.get(f"/drivers/{winner['driver_id']}/session")).json()
            assert sess["current_ride"] is None and sess["driver"]["status"] == "available", sess
            print("8b. session after completion: no current ride, available")

            # a taken ride stops the loop, even when its last round comes due
            async with Session() as s:
                for rnd in (2, 4):
                    try:
                        await dispatch_ride(s, payload.model_copy(update={"round_number": rnd}))
                        raise AssertionError("a completed ride was dispatched again")
                    except RideNotPending:
                        pass
            r = await c.get(f"/rides/{ride['id']}")
            assert r.json()["status"] == RideStatus.COMPLETED, r.text
            print("9. taken ride -> RideNotPending in any round, status untouched")

            # no supply -> an empty round keeps it pending; only the cap times it out
            await reset()
            r = await c.post("/rides", json=body)
            empty_ride = r.json()
            empty = RideQueuePayload.model_validate(await drain_queue())
            async with Session() as s:
                try:
                    await dispatch_ride(s, empty.model_copy(update={"round_number": 1}))
                    raise AssertionError("expected NoDriverAvailable")
                except NoDriverAvailable:
                    pass
            r = await c.get(f"/rides/{empty_ride['id']}")
            assert r.json()["status"] == RideStatus.PENDING, r.text
            async with Session() as s:
                try:
                    await dispatch_ride(s, empty.model_copy(update={"round_number": 4}))
                    raise AssertionError("expected RideTimedOut")
                except RideTimedOut:
                    pass
            r = await c.get(f"/rides/{empty_ride['id']}")
            assert r.json()["status"] == RideStatus.TIMED_OUT, r.text
            async with Session() as s:
                assert await timed_out_ride(s, empty_ride["id"]) is False, "timed out twice"
            print("10. empty round -> NoDriverAvailable, still pending; round 4 -> RideTimedOut, once")

        await reset()
    print("dispatch flow ok")


if __name__ == "__main__":
    asyncio.run(main())
