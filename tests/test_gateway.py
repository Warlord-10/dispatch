"""Sockets across gateways: offers, location pings, ride updates, a gateway dying.
Run: docker compose up -d && uv run python tests/test_gateway.py

Starts two real gateway processes. The worker's handler runs inline, standing
in for the worker process, exactly as it would consume a ride message.
"""

import pathlib
import sys

# tests live in tests/, the app lives one level up
ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import asyncio
import json
import os
import subprocess
import uuid

import httpx
import websockets
from asgi_lifespan import LifespanManager
from sqlalchemy import update

from db import Session
from enums import DriverStatus, Recipient, RideStatus
from main import app
from messaging.connection import rabbitmq
from messaging.ride_queue import RideQueuePayload, ride_queue, ride_retry_queue
from models import Driver, Ride
from services import offers
from services.connections import gateways_for
from services.ride_booking import timed_out_ride
from services.workers.dispatch_worker import handle_ride_requested

PICKUP = (-41.2865, 174.7762)  # far from other suites' drivers
GW = {"gw-test-1": 8801, "gw-test-2": 8802}


def start_gateway(gw_id: str) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "gateway:app", "--port", str(GW[gw_id]), "--log-level", "warning"],
        cwd=ROOT, env={**os.environ, "GATEWAY_ID": gw_id},
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )


async def connect(gw_id: str, kind: str, id_: int):
    """Open a socket and wait until the gateway has registered it."""
    for _ in range(60):
        try:
            ws = await websockets.connect(f"ws://127.0.0.1:{GW[gw_id]}/ws/{kind}/{id_}")
            break
        except OSError:
            await asyncio.sleep(0.25)  # gateway still starting
    await ws.send(json.dumps({"type": "ping"}))
    assert json.loads(await asyncio.wait_for(ws.recv(), 3)) == {"type": "pong"}
    return ws


async def recv(ws, type_: str) -> dict:
    """Next message of this type, skipping others (e.g. an offer arriving
    while we wait for a ride update)."""
    while True:
        msg = json.loads(await asyncio.wait_for(ws.recv(), 3))
        if msg["type"] == type_:
            return msg


async def ask(ws, msg: dict) -> dict:
    await ws.send(json.dumps(msg))
    return json.loads(await asyncio.wait_for(ws.recv(), 3))


async def next_ride_message() -> RideQueuePayload:
    q = await ride_queue.declare(await rabbitmq.publisher_channel())
    for _ in range(20):
        msg = await q.get(no_ack=True, fail=False)
        if msg is not None:
            return RideQueuePayload.model_validate_json(msg.body)
        await asyncio.sleep(0.1)
    raise AssertionError("ride was never published")


async def purge() -> None:
    ch = await rabbitmq.publisher_channel()
    for q in (ride_queue, ride_retry_queue):
        await (await q.declare(ch)).purge()


async def main():
    gateways = {gw: start_gateway(gw) for gw in GW}
    drivers: list[int] = []
    try:
        async with LifespanManager(app):
            await purge()
            t = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=t, base_url="http://t") as c:
                u = uuid.uuid4().int % 10**8
                rider = (await c.post("/riders", json={"name": "R", "phone_number": f"9{u:08d}0"})).json()["id"]
                for n in (1, 2):
                    d = (await c.post("/drivers", json={"name": f"D{n}", "phone_number": f"9{u:08d}{n}",
                                                        "car_number": f"KA01GW000{n}", "car_model": "Swift"})).json()["id"]
                    await c.patch(f"/drivers/{d}/status", json={"status": "available"})
                    drivers.append(d)
                a, b = drivers

                async def book() -> str:
                    r = await c.post("/rides", json={"rider_id": rider, "pickup_lat": PICKUP[0], "pickup_lng": PICKUP[1],
                                                     "dest_lat": PICKUP[0] + 0.05, "dest_lng": PICKUP[1] + 0.05})
                    ride_id = r.json()["id"]
                    await handle_ride_requested(await next_ride_message())  # the worker's job
                    return ride_id

                ws_a = await connect("gw-test-1", "drivers", a)
                ws_b = await connect("gw-test-2", "drivers", b)
                ws_r = await connect("gw-test-2", "riders", rider)
                assert await gateways_for(Recipient.DRIVER, [a, b]) == {str(a): "gw-test-1", str(b): "gw-test-2"}
                assert await gateways_for(Recipient.RIDER, [rider]) == {str(rider): "gw-test-2"}
                print("1. drivers and rider registered against the gateway holding their socket")

                # location over the socket: the drivers were never pinged over HTTP
                for ws, n in ((ws_a, 1), (ws_b, 2)):
                    pong = await ask(ws, {"type": "ping", "lat": PICKUP[0] + n * 0.001, "lng": PICKUP[1]})
                    assert pong == {"type": "pong", "dispatchable": True}, pong
                p = (await c.get(f"/drivers/{a}/presence")).json()
                assert p["online"] and abs(p["lat"] - (PICKUP[0] + 0.001)) < 1e-6, p
                bad = await ask(ws_a, {"type": "ping", "lat": 999, "lng": PICKUP[1]})
                assert bad["type"] == "error", bad
                assert (await ask(ws_a, {"type": "nonsense"}))["type"] == "error"
                await ws_a.send("not json")
                assert json.loads(await asyncio.wait_for(ws_a.recv(), 3))["type"] == "error"
                assert (await ask(ws_a, {"type": "ping"})) == {"type": "pong"}, "socket should survive bad input"
                print("2. position pings over the socket reach the index; bad input gets an error, socket survives")

                ride = await book()
                got_a, got_b = await recv(ws_a, "ride_offer"), await recv(ws_b, "ride_offer")
                assert (got_a["driver_id"], got_b["driver_id"]) == (a, b)
                assert got_a["ride_id"] == got_b["ride_id"] == ride
                assert set(await offers.already_offered(ride)) == {str(a), str(b)}
                print("3. one round, two gateways: each offer reached only its own driver's socket")

                # the Phase 7 done-when: the rider's socket is on gw-2, the accept
                # runs in the API process, the driver is on gw-1
                r = await c.post(f"/rides/{ride}/accept", json={"driver_id": a})
                assert r.status_code == 200, r.text
                upd = await recv(ws_r, "ride_update")
                assert upd["ride"]["id"] == ride and upd["ride"]["status"] == "assigned"
                assert upd["ride"]["driver_id"] == a
                assert (await recv(ws_a, "ride_update"))["ride"]["status"] == "assigned"
                print("4. accept via the API reaches the rider on another gateway, and the driver")

                await c.post(f"/rides/{ride}/cancel")
                assert (await recv(ws_r, "ride_update"))["ride"]["status"] == "cancelled"
                assert (await recv(ws_a, "ride_update"))["ride"]["status"] == "cancelled", \
                    "the driver must hear the rider cancelled"
                print("5. rider cancels: rider and the assigned driver both get the update")

                # kill -9 gateway 2: no cleanup runs, so B's Redis entry survives
                gateways["gw-test-2"].kill()
                gateways["gw-test-2"].wait()
                await asyncio.sleep(0.5)  # broker notices the dead connection, drops its queue
                assert await gateways_for(Recipient.DRIVER, [b]) == {str(b): "gw-test-2"}, "entry should outlive the kill"
                ride2 = await book()
                assert (await recv(ws_a, "ride_offer"))["ride_id"] == ride2
                assert set(await offers.already_offered(ride2)) == {str(a)}, \
                    "driver on the dead gateway was recorded as offered"
                print("6. gateway killed: its driver is skipped at publish time and not spent; the other still gets it")

                # the rider comes back on the surviving gateway; the worker's timeout reaches them
                ws_r = await connect("gw-test-1", "riders", rider)
                async with Session() as s:
                    assert await timed_out_ride(s, uuid.UUID(ride2)) is True
                assert (await recv(ws_r, "ride_update"))["ride"]["status"] == "timed_out"
                print("7. rider reconnects elsewhere; a timeout from the worker path reaches them")

                # reconnect on the same gateway: the old socket is closed, the entry stays
                ws_a2 = await connect("gw-test-1", "drivers", a)
                await ws_a.wait_closed()
                assert ws_a.close_code == 4000, ws_a.close_code
                await asyncio.sleep(0.2)
                assert await gateways_for(Recipient.DRIVER, [a]) == {str(a): "gw-test-1"}, \
                    "old socket's cleanup removed the new connection's entry"
                print("8. reconnect replaces the old socket; its cleanup leaves the new entry alone")

                await ws_a2.close()
                await ws_r.close()
                await asyncio.sleep(0.3)
                assert await gateways_for(Recipient.DRIVER, [a]) == {}
                assert await gateways_for(Recipient.RIDER, [rider]) == {}
                print("9. disconnect removes the entry")
            await purge()
    finally:
        for p in gateways.values():
            if p.poll() is None:
                p.terminate()
                p.wait()
        async with Session() as s:
            await s.execute(update(Driver).where(Driver.id.in_(drivers)).values(status=DriverStatus.OFFLINE))
            await s.execute(update(Ride).where(Ride.driver_id.in_(drivers), Ride.status == RideStatus.ASSIGNED)
                            .values(status=RideStatus.CANCELLED))
            await s.commit()
    print("gateway ok")


if __name__ == "__main__":
    asyncio.run(main())
