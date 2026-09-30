"""Idempotent ride booking. Run: uv run python tests/test_idempotency.py"""

import pathlib
import sys

# tests live in tests/, the app lives one level up
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

import asyncio
import uuid

import httpx
from asgi_lifespan import LifespanManager
from sqlalchemy import func, select

from db import Session
from main import app
from messaging.connection import rabbitmq
from messaging.ride_queue import ride_queue
from models import Ride

BODY = {"pickup_lat": 12.93, "pickup_lng": 77.62, "dest_lat": 12.97, "dest_lng": 77.64}


async def rides_for(rider_id: int) -> int:
    async with Session() as s:
        return await s.scalar(select(func.count()).where(Ride.rider_id == rider_id))


async def main():
    async with LifespanManager(app):
        t = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=t, base_url="http://t") as c:
            u = uuid.uuid4().int % 10**8
            a = (await c.post("/riders", json={"name": "A", "phone_number": f"7{u:08d}1"})).json()
            b = (await c.post("/riders", json={"name": "B", "phone_number": f"7{u:08d}2"})).json()
            body = {**BODY, "rider_id": a["id"]}

            key = str(uuid.uuid4())
            r1 = await c.post("/rides", json=body, headers={"Idempotency-Key": key})
            r2 = await c.post("/rides", json=body, headers={"Idempotency-Key": key})
            assert r1.status_code == r2.status_code == 202, (r1.text, r2.text)
            assert r1.json()["id"] == r2.json()["id"]
            assert await rides_for(a["id"]) == 1
            print("1. retry with the same key returns the original ride, one row")

            r = await c.post("/rides", json={**body, "pickup_lat": 13.0}, headers={"Idempotency-Key": key})
            assert r.status_code == 422, r.text
            assert await rides_for(a["id"]) == 1
            print("2. same key, different body -> 422, nothing created")

            # the double-submit race: all arrive before any has committed
            key = str(uuid.uuid4())
            rs = await asyncio.gather(*[
                c.post("/rides", json=body, headers={"Idempotency-Key": key}) for _ in range(5)
            ])
            assert all(r.status_code == 202 for r in rs), [r.text for r in rs]
            assert len({r.json()["id"] for r in rs}) == 1, [r.json()["id"] for r in rs]
            assert await rides_for(a["id"]) == 2
            print("3. 5 concurrent requests with one key -> one ride, all 202")

            r = await c.post("/rides", json={**BODY, "rider_id": b["id"]}, headers={"Idempotency-Key": key})
            assert r.status_code == 202 and r.json()["rider_id"] == b["id"], r.text
            assert r.json()["id"] != rs[0].json()["id"], "rider B read rider A's ride"
            print("4. keys are scoped per rider: B reusing A's key gets their own ride")

            await c.post("/rides", json=body)
            await c.post("/rides", json=body)
            assert await rides_for(a["id"]) == 4
            print("5. no header -> every request is a new ride")

            r = await c.post("/rides", json=body, headers={"Idempotency-Key": "not-a-uuid"})
            assert r.status_code == 422, r.text
            print("6. malformed key -> 422")

        # nobody consumes during the test; don't leave rides for the next suite
        q = await ride_queue.declare(await rabbitmq.publisher_channel())
        await q.purge()
    print("idempotency ok")


if __name__ == "__main__":
    asyncio.run(main())
