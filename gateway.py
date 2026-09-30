"""WebSocket gateway: holds driver and rider sockets, pushes messages down them.

Run one:      GATEWAY_ID=gw-1 uv run uvicorn gateway:app --port 8001
Run several:  uv run uvicorn gateway:app --port 8001 --workers 4
              (leave GATEWAY_ID unset: each process must get its own id, and
              two processes sharing one would fight over one exclusive queue)

A separate app from the API on purpose. Sockets are long-lived and stateful,
requests are short and stateless; they scale and fail differently.

What travels over the socket, and what stays on HTTP:
  socket, client -> server   ping, with the driver's position (the hot path)
  socket, server -> client   ride_offer (drivers), ride_update (both)
  HTTP                       every command that needs an answer: book, accept,
                             reject, cancel, complete, go online/offline, and
                             the resync on connect

Protocol:
  driver sends  {"type": "ping", "lat": 12.93, "lng": 77.62}   every PING_INTERVAL_S
  rider sends   {"type": "ping"}
  server sends  {"type": "pong", ...}  or  {"type": "error", "detail": ...}

The ping keeps the socket reachable (it refreshes the Redis entry naming this
gateway), so a client that stops pinging stops receiving pushes after
GATEWAY_CONN_TTL_S. For drivers it also records the position, exactly like
POST /drivers/{id}/location, which stays as the fallback for when the socket is
unavailable (an app in the background, typically). On every connect the client
should fetch its state over HTTP first; the socket carries only what changes.
"""

import json
import os
import uuid
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from pydantic import ValidationError

from enums import Recipient
from messaging.connection import rabbitmq
from messaging.push_queue import PushMessage, push_queue
from schemas.drivers import DriverPing
from services import connections as registry
from services.drivers import record_ping

GATEWAY_ID = ""  # set at startup, never at import: see the module docstring

# The sockets held by THIS process. Another gateway has its own dict and cannot
# see these, which is exactly why Redis records who is connected where.
connections: dict[tuple[Recipient, int], WebSocket] = {}


async def push(recipient: Recipient, recipient_id: int, body: dict) -> bool:
    """Send to a socket held by this process. False if not here or gone."""
    ws = connections.get((recipient, recipient_id))
    if ws is None:
        return False
    try:
        await ws.send_json(body)
        return True
    except Exception:  # closed between the lookup and the send
        connections.pop((recipient, recipient_id), None)
        return False


async def deliver(msg: PushMessage) -> None:
    if not await push(msg.recipient, msg.recipient_id, msg.body):
        # Disconnected, or moved to another gateway, after the publisher looked
        # them up. Lost; their resync on reconnect covers it.
        print(f"[{GATEWAY_ID}] {msg.recipient} {msg.recipient_id} not connected here, "
              f"{msg.body.get('type')} dropped")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global GATEWAY_ID
    GATEWAY_ID = os.getenv("GATEWAY_ID") or f"gw-{uuid.uuid4().hex[:8]}"
    await rabbitmq.connect()
    await push_queue.subscribe(GATEWAY_ID, deliver)
    print(f"[{GATEWAY_ID}] ready")
    try:
        yield
    finally:
        await push_queue.close()
        await rabbitmq.close()


app = FastAPI(title="Dispatch gateway", lifespan=lifespan)


PingHandler = Callable[[WebSocket, int, dict], Awaitable[None]]


async def _serve(ws: WebSocket, recipient: Recipient, recipient_id: int, on_ping: PingHandler) -> None:
    """One socket's whole life: register, answer pings, clean up."""
    await ws.accept()
    key = (recipient, recipient_id)
    old = connections.get(key)
    connections[key] = ws  # a reconnect replaces the old socket
    if old is not None:
        try:
            await old.close(code=4000)  # the same person twice: keep the newest
        except Exception:
            pass
    await registry.register(recipient, recipient_id, GATEWAY_ID)
    try:
        while True:
            # Parsed by hand, not receive_json(): a malformed frame from a phone
            # gets an error reply, it does not tear down the connection.
            raw = await ws.receive_text()  # raises WebSocketDisconnect on close
            try:
                msg = json.loads(raw)
            except ValueError:
                msg = None
            if not isinstance(msg, dict):
                await ws.send_json({"type": "error", "detail": "expected a JSON object"})
                continue
            if msg.get("type") == "ping":
                await registry.register(recipient, recipient_id, GATEWAY_ID)  # refresh the TTL
                await on_ping(ws, recipient_id, msg)
            else:
                await ws.send_json({"type": "error", "detail": f"unknown type {msg.get('type')!r}"})
    except WebSocketDisconnect:
        pass
    finally:
        # A reconnect has already registered a newer socket; this cleanup
        # belongs to the old one and must not remove it.
        if connections.get(key) is ws:
            del connections[key]
            await registry.unregister(recipient, recipient_id, GATEWAY_ID)


async def _driver_ping(ws: WebSocket, driver_id: int, msg: dict) -> None:
    if "lat" not in msg and "lng" not in msg:
        await ws.send_json({"type": "pong"})  # keepalive only
        return
    try:
        # Untrusted input from a phone: the same validation as the HTTP route.
        ping = DriverPing(lat=msg.get("lat"), lng=msg.get("lng"))
    except ValidationError as exc:
        await ws.send_json({"type": "error", "detail": exc.errors(include_url=False, include_context=False)})
        return
    presence = await record_ping(driver_id, ping)
    await ws.send_json({"type": "pong", "dispatchable": presence.dispatchable})


async def _rider_ping(ws: WebSocket, rider_id: int, msg: dict) -> None:
    await ws.send_json({"type": "pong"})


# ponytail: ids straight from the path, no auth — anyone can connect as anyone
# and read their pushes, or move a driver's position. Authenticate the socket
# once there is auth.
@app.websocket("/ws/drivers/{driver_id}")
async def driver_socket(ws: WebSocket, driver_id: int):
    await _serve(ws, Recipient.DRIVER, driver_id, _driver_ping)


@app.websocket("/ws/riders/{rider_id}")
async def rider_socket(ws: WebSocket, rider_id: int):
    await _serve(ws, Recipient.RIDER, rider_id, _rider_ping)


@app.get("/health")
def health():
    return {"gateway": GATEWAY_ID, "connections": len(connections)}
