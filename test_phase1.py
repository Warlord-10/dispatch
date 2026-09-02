"""End-to-end Phase 1 over HTTP. Run: uv run python test_phase1.py"""

import asyncio
import uuid

import httpx
from asgi_lifespan import LifespanManager
from sqlalchemy import update

from db import Session
from main import app
from models import Driver


async def reset_supply():
    """Own the starting state: a driver left 'available' by a previous run
    would get matched and break the no-supply assertion below."""
    async with Session() as s:
        await s.execute(update(Driver).values(status="offline"))
        await s.commit()


async def main():
    async with LifespanManager(app):
        await reset_supply()
        t = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=t, base_url="http://t") as c:
            uniq = uuid.uuid4().int % 10000

            r = await c.post("/riders", json={"name": "Rhea", "phone_number": f"98{uniq:04d}0001"})
            assert r.status_code == 201, r.text
            rider = r.json()

            r = await c.post("/drivers", json={
                "name": "Sam", "phone_number": f"98{uniq:04d}0002",
                "car_number": "ka-01 ab 1234", "car_model": "Swift"})
            assert r.status_code == 201, r.text
            driver = r.json()
            assert driver["status"] == "offline"
            assert driver["car_number"] == "KA01AB1234"

            # duplicate phone -> 409 from the unique index
            r = await c.post("/riders", json={"name": "Dup", "phone_number": f"+9198{uniq:04d}0001"})
            assert r.status_code == 409, r.text

            # bad coordinates rejected at the trust boundary
            r = await c.post("/rides", json={"rider_id": rider["id"], "pickup_lat": 91.0,
                "pickup_lng": 77.6, "dest_lat": 12.9, "dest_lng": 77.6})
            assert r.status_code == 422, r.text

            # no driver online yet -> ride created, left pending, still 201
            body = {"rider_id": rider["id"], "pickup_lat": 12.93, "pickup_lng": 77.62,
                    "dest_lat": 12.97, "dest_lng": 77.64}
            r = await c.post("/rides", json=body)
            assert r.status_code == 201 and r.json()["status"] == "pending", r.text
            await c.post(f"/rides/{r.json()['id']}/cancel")

            # going available without coordinates is rejected
            r = await c.patch(f"/drivers/{driver['id']}/status", json={"status": "available"})
            assert r.status_code == 422, r.text

            r = await c.patch(f"/drivers/{driver['id']}/status",
                              json={"status": "available", "lat": 12.9352, "lng": 77.6245})
            assert r.status_code == 200 and r.json()["status"] == "available", r.text

            # now it matches
            r = await c.post("/rides", json=body)
            ride = r.json()
            assert ride["status"] == "assigned", r.text
            assert ride["driver_id"] == driver["id"]
            assert ride["assigned_at"] is not None

            # completing frees the driver back into the pool
            r = await c.post(f"/rides/{ride['id']}/complete")
            assert r.status_code == 200 and r.json()["status"] == "completed", r.text
            r = await c.post("/rides", json=body)
            assert r.json()["status"] == "assigned", "driver was not returned to the pool"
            ride2 = r.json()

            # cancelling frees the driver too
            r = await c.post(f"/rides/{ride2['id']}/cancel")
            assert r.status_code == 200 and r.json()["status"] == "cancelled", r.text
            r = await c.post("/rides", json=body)
            assert r.json()["status"] == "assigned", "cancel did not free the driver"
            await c.post(f"/rides/{r.json()['id']}/cancel")

            # illegal transitions
            r = await c.post(f"/rides/{ride['id']}/complete")
            assert r.status_code == 409, r.text
            r = await c.post(f"/rides/{uuid.uuid4()}/cancel")
            assert r.status_code == 404, r.text

    print("phase 1 end-to-end ok")


if __name__ == "__main__":
    asyncio.run(main())
