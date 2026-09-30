"""End-to-end over HTTP. Run: uv run python tests/test_phase1.py

Covers registration, eligibility, the location ping, presence and ride
creation. Assignment is NOT asserted here: book_ride publishes to RabbitMQ
now, so a ride stays 'pending' until the matcher worker exists.
"""

import pathlib
import sys

# tests live in tests/, the app lives one level up
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

import asyncio
import uuid

import httpx
from asgi_lifespan import LifespanManager
from sqlalchemy import update

from db import Session
from main import app
from models import Driver
from redis_client import redis_client


BLR = {"lat": 12.9352, "lng": 77.6245}


async def reset_supply():
    """Own the starting state: a driver left available by a previous run would
    otherwise satisfy assertions that expect no supply."""
    async with Session() as s:
        await s.execute(update(Driver).values(status="offline"))
        await s.commit()
    keys = await redis_client.keys("dispatch:*")
    if keys:
        await redis_client.delete(*keys)


async def main():
    async with LifespanManager(app):
        await reset_supply()
        t = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=t, base_url="http://t") as c:
            uniq = uuid.uuid4().int % 10**8

            r = await c.post("/riders", json={"name": "Rhea", "phone_number": f"9{uniq:08d}1"})
            assert r.status_code == 201, r.text
            rider = r.json()

            r = await c.post("/drivers", json={
                "name": "Sam", "phone_number": f"9{uniq:08d}2",
                "car_number": "ka-01 ab 1234", "car_model": "Swift"})
            assert r.status_code == 201, r.text
            driver = r.json()
            assert driver["status"] == "offline"
            assert driver["car_number"] == "KA01AB1234"
            did = driver["id"]

            # duplicate phone -> 409 from the unique index
            r = await c.post("/riders", json={"name": "Dup", "phone_number": f"+919{uniq:08d}1"})
            assert r.status_code == 409, r.text

            # a driver who has never pinged is neither online nor dispatchable
            r = await c.get(f"/drivers/{did}/presence")
            assert r.status_code == 200 and r.json()["online"] is False, r.text

            # eligibility carries no coordinates any more
            r = await c.patch(f"/drivers/{did}/status", json={"status": "available"})
            assert r.status_code == 200 and r.json()["status"] == "available", r.text

            # ...but without a ping there is still no position, so no dispatch
            r = await c.get(f"/drivers/{did}/presence")
            assert r.json()["dispatchable"] is False, r.text

            # the ping is what makes them dispatchable
            r = await c.post(f"/drivers/{did}/location", json=BLR)
            assert r.status_code == 200, r.text
            assert r.json()["dispatchable"] is True, r.text

            r = await c.get(f"/drivers/{did}/presence")
            body = r.json()
            assert body["online"] is True and body["dispatchable"] is True, body
            assert body["seconds_since_ping"] < 5, body

            # bad coordinates rejected at the trust boundary
            for bad in ({"lat": 91.0, "lng": 77.6}, {"lat": 12.9, "lng": 181.0}):
                r = await c.post(f"/drivers/{did}/location", json=bad)
                assert r.status_code == 422, r.text

            ride_body = {"rider_id": rider["id"], "pickup_lat": 12.93, "pickup_lng": 77.62,
                         "dest_lat": 12.97, "dest_lng": 77.64}
            r = await c.post("/rides", json=ride_body)
            assert r.status_code == 202, r.text
            ride = r.json()
            assert ride["status"] == "pending", "async matching: no worker yet"
            assert ride["driver_id"] is None

            r = await c.post("/rides", json={**ride_body, "pickup_lat": 91.0})
            assert r.status_code == 422, r.text

            # a pending ride can be cancelled; completing it cannot
            r = await c.post(f"/rides/{ride['id']}/complete")
            assert r.status_code == 409, r.text
            r = await c.post(f"/rides/{ride['id']}/cancel")
            assert r.status_code == 200 and r.json()["status"] == "cancelled", r.text
            r = await c.post(f"/rides/{ride['id']}/cancel")
            assert r.status_code == 409, r.text

            r = await c.post(f"/rides/{uuid.uuid4()}/cancel")
            assert r.status_code == 404, r.text

            # going offline removes them from dispatch but they are still online
            r = await c.patch(f"/drivers/{did}/status", json={"status": "offline"})
            assert r.status_code == 200
            r = await c.get(f"/drivers/{did}/presence")
            assert r.json() == {**r.json(), "online": True, "dispatchable": False}

    print("phase 1 end-to-end ok")


if __name__ == "__main__":
    asyncio.run(main())
