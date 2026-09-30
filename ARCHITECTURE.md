# Dispatch — Architecture and Progress Log

Dispatch is a real-time supply–demand matching engine, the core of a ride-hailing
or delivery app. A rider asks for a ride, the system finds the nearest available
drivers, offers the ride to them, and exactly one of them gets it.

The project is built to learn system design by doing it: each phase of
[ROADMAP.md](ROADMAP.md) starts from something that works, breaks it under load or
failure, measures why, and replaces the broken part. This document records what
the system looks like now, why it looks that way, and the problems found along
the way.

**Status:** Phases 1–6 complete. Phase 7's done-when is met: offers, location pings and ride updates travel over WebSockets across multiple gateways.

---

## 1. The system at a glance

```
                        ┌──────────────────────────────────────┐
  rider / driver apps   │  API  (FastAPI, uvicorn)             │
  ─── HTTP ──────────►  │  routers → services → models         │
                        └───┬──────────────┬───────────────┬───┘
                            │              │               │
                  durable   │    hot path  │     publish   │
                            ▼              ▼               ▼
                    ┌────────────┐  ┌────────────┐  ┌──────────────────────┐
                    │ Postgres   │  │ Redis      │  │ RabbitMQ             │
                    │ riders     │  │ positions  │  │ ride.requested       │
                    │ drivers    │  │ status     │  │ ride.requested.retry │
                    │ rides      │  │ geo index  │  │ ride.offers          │
                    └─────▲──────┘  │ approached │  └──────────┬───────────┘
                          │         └─────▲──────┘             │ consume
                          │               │                    ▼
                          │         ┌─────┴────────────────────────────┐
                          └─────────┤ Dispatch worker(s)               │
                                    │ one process each, 20 rides       │
                                    │ in flight per process            │
                                    └──────────────────────────────────┘
```

| Component | What it does | Runs as |
|---|---|---|
| **API** | Registration, driver status and pings, booking, accept/reject/complete/cancel | `uvicorn main:app` |
| **Dispatch worker** | Consumes ride requests, runs matching rounds, publishes offers, schedules the next round | `python -m services.workers.dispatch_worker`, one OS process per copy |
| **WebSocket gateway** | Holds driver and rider sockets; takes drivers' location pings; pushes offers and ride updates routed to it | `uvicorn gateway:app`, one or more processes, each with its own gateway id |
| **Postgres 16** | Source of truth for anything that must survive a restart | Docker, port **5433** |
| **Redis 8** | Live driver state: position, eligibility, the geo index, who has been offered a ride | Docker, port **6380** |
| **RabbitMQ 4** | Hands ride requests to workers; holds the timer between rounds; carries offers to drivers | Docker, port **5673** (UI on 15673) |

Ports are shifted from the defaults because other stacks on this machine already
use 5432, 6379 and 5672.

---

## 2. Where each piece of data lives, and why

The central design idea: **choose a consistency model per kind of data, not one
for the whole system.** Some data must never be lost; some changes ten times a
minute and is worthless after thirty seconds. Putting both in the same store
makes one of them wrong.

| Data | Store | Why there | If the store dies |
|---|---|---|---|
| Riders, drivers, rides, assignments | **Postgres** | Must survive restarts; needs constraints and transactions | Outage (it is the source of truth) |
| Driver position | **Redis** | Written on every ping (~every 10 s per driver); only the latest value matters | Positions rebuild within one ping interval |
| Driver status (eligibility) | **Redis**, copied from Postgres | Read on every ping and every search; must be fast | Rebuilt from Postgres at startup |
| Geo index of available drivers | **Redis** | `GEOSEARCH` answers "nearest N within R km" in one command | Rebuilt as drivers ping |
| Who was offered a ride, and when | **Redis** | Only needed during the 45 s matching window | A few drivers may be offered a ride twice; no ride is corrupted |
| Pending work and round timers | **RabbitMQ** | Durable hand-off between processes, redelivery, delays | Messages are persistent; the broker restores them |

### 2.1 Postgres schema

Three tables, kept deliberately small. Columns are added only when a phase needs them.

- **`riders`** — `id`, `name`, `phone_number` (unique), `created_at`.
- **`drivers`** — `id`, `name`, `phone_number` (unique), `status`, `car_number`,
  `car_model`, `created_at`. `status` is `offline | available | assigned`,
  enforced by a CHECK constraint generated from the Python enum, so code and
  database cannot drift. **No location columns**: see Phase 4.
- **`rides`** — `id` (UUID), `rider_id`, `driver_id`, `status`, pickup and
  destination coordinates, timestamps, `idempotency_key`, `request_hash`.
  - `status` is `pending | assigned | completed | cancelled | timed_out`, also a CHECK constraint.
  - A partial index on `status = 'pending'`, the only status the matcher looks at.
  - `UNIQUE (rider_id, idempotency_key)`, the storage-level guarantee behind idempotent booking.
  - `UNIQUE (driver_id) WHERE status = 'assigned'`: **one driver, at most one
    assigned ride**, enforced by Postgres itself. The code already guarantees it
    (§3.5); the index catches the next bug that doesn't.

Ride ids are UUIDs because they are handed to clients and will be sharded in
Phase 9. Changing a primary key's type later is the one migration worth avoiding.

### 2.2 Redis keyspace

Every key is declared in [redis_keys.py](redis_keys.py). Redis has no schema, so a
typo in a key name silently returns nothing; one file keeps the whole keyspace
visible. Each key states how it is cleaned up, because a key with neither a TTL
nor a sweeper is a slow memory leak.

| Key | Type | Holds | Cleanup |
|---|---|---|---|
| `dispatch:driver:{id}:position` | STRING | `"lng,lat,epoch"` | TTL 1 hour |
| `dispatch:drivers:status` | HASH | `driver_id → status` | Never expires; rebuilt from Postgres on startup |
| `dispatch:drivers:available:geo` | GEO | Positions of *available* drivers only | Removed on status change; a sweeper trims stale members |
| `dispatch:ride:{id}:offered` | HASH | `driver_id → epoch the offer was sent` | TTL 15 min only, never deleted early (§3.5) |
| `dispatch:driver:websocket:connection` | HASH | `driver_id → gateway id` holding their socket | Per-field TTL 30 s, refreshed on every socket ping; removed on disconnect by the owning gateway only |
| `dispatch:rider:websocket:connection` | HASH | `rider_id → gateway id` | Same as above |

**Three separate questions about a driver.** Early designs mixed these up, which
caused most of the Phase 4 confusion:

1. **Eligibility**: *may* they get a ride? (status hash; changes only on explicit actions)
2. **Liveness**: are they online *right now*? (the epoch inside the position; stale after 30 s)
3. **Position**: *where* were they last? (the position key; kept for an hour)

A driver who loses signal for a minute is still `available` (eligibility) and we
still know where they were (position), but they are not dispatchable (liveness).
The moment they ping again they are back in the geo index. No state is lost and
no "reconnect" logic is needed.

Why the position TTL is an hour and not 30 seconds: the key's *existence* answers
"do we know where they were", and the *epoch inside it* answers "are they online".
A 30-second TTL would throw away the position we need to put a driver back into
the geo index when they finish a ride.

### 2.3 RabbitMQ topology

| Queue | Consumer | Settings | Purpose |
|---|---|---|---|
| `ride.requested` | dispatch worker | dead-letters to `ride.requested.dlq` | One message = one matching round for one ride |
| `ride.requested.dlq` | a human | — | Messages that genuinely failed (bugs, outages) |
| `ride.requested.retry` | **nobody, on purpose** | TTL 15 s, dead-letters into `ride.requested` | The wait between rounds, done by the broker |
| `dispatch.push` *(direct exchange)* | — | — | Routes every message for a phone by gateway id |
| `gateway.{gateway_id}` | that one gateway | **exclusive**, TTL 15 s, no DLQ | One queue per gateway process, carrying offers and ride updates for the sockets it holds; the broker deletes it when the gateway dies |

One connection per process. The API and the worker share one publisher channel
with publisher confirms on (every publish waits for the broker's ack), and each
consumer gets its own channel, so a channel error takes down only that consumer.
Publishes use `mandatory=True`, so a message nobody can route raises an error
instead of vanishing.

---

## 3. How the main flows work

### 3.1 Driver ping (the hot path)

The highest-volume message by far. It arrives two ways, both calling the same
`record_ping()`:
- **over the driver's socket**, `{"type": "ping", "lat": …, "lng": …}` every 10 s,
  the normal path. The same message also keeps the socket reachable (§3.4b);
- **`POST /drivers/{id}/location`**, the fallback when the socket is unavailable,
  typically an app in the background, where phones suspend sockets.

Both validate the coordinates with the same `DriverPing` schema. At the gateway,
a malformed ping gets an `error` frame and the socket stays open.

```
SET dispatch:driver:{id}:position "lng,lat,now" EX 3600
if status hash says "available":
    GEOADD dispatch:drivers:available:geo lng lat id
```

No Postgres, no session, no transaction. Two Redis commands.

The check is on the **status hash**, never on "is the driver already in the geo
set". The sweeper removes silent drivers from the geo set; if the ping only
re-added drivers already in the set, a swept driver could never get back in.

### 3.2 Driver status changes

`PATCH /drivers/{id}/status`, and internally on accept, complete and cancel.

Postgres is written first, then the Redis status hash. Going `available` puts the
driver into the geo index **using their stored position key**, so a driver
finishing a ride is dispatchable immediately, without waiting for their next ping.
This was the problem that made per-driver position keys necessary (see Phase 4).

### 3.3 Booking a ride

`POST /rides`, with an optional `Idempotency-Key: <uuid>` header.

1. Insert the ride as `pending` with the key and a SHA-256 of the request body.
2. If the insert fails on `rides_idempotency_key`, this is a retry: return the
   original ride (same key, different body → **422**).
3. Publish a `ride.requested` message with `round_number=0`.
4. Return **202 Accepted**: the ride exists, but the driver comes later. The
   client polls `GET /rides/{id}`.

The insert happens first and the unique constraint decides, rather than
"check whether the key exists, then insert": two retries arriving together would
both see "not found" and both insert.

### 3.4 The matching loop

```
ride.requested ──► worker: round_number += 1
     ▲               dispatch_ride(...)
     │                 ├─ RideNotPending     → ack, stop     (accepted or cancelled)
     │                 ├─ RideTimedOut       → ack, stop     (round 4: ran out of rounds)
     │                 ├─ NoDriverAvailable  → no offers this round
     │                 └─ [driver ids]       → record in the approached hash,
     │                                         publish one offer per driver
     │               publish to ride.requested.retry
     │                         │
     └───── 15 s TTL ◄─────────┘   (the broker moves it back)
```

- **Rounds.** Rounds 1–3 search and offer. The message that arrives after round 3
  times the ride out: 3 × 15 s = a 45 s matching window.
- **Radius.** Each round searches the 3 → 5 → 8 km ladder and stops at the first
  ring that has a fresh driver not yet approached. Widening happens by itself once
  the nearby drivers have all been asked.
- **Exclusion.** Drivers enter the approached hash *when the offer is sent*, so
  rejecting and ignoring end the same way: that driver is not asked again.
- **Outcomes vs failures.** `dispatch_ride` raises named outcomes and the worker
  catches all three. Only an unexpected error escapes the handler, and escaping
  means "failed" → DLQ. A timeout is a normal result and must never land in the DLQ.
- **Concurrency.** Each worker processes up to 20 rides at once (prefetch 20) as
  asyncio tasks on one thread. Scale by adding worker *processes*.

### 3.4b Getting a message to the right phone

A socket lives in exactly one gateway process, and each process only knows its
own sockets. So delivery needs routing, not load balancing:

```
driver connects to gw-1  → HSET  driver:websocket:connection  7 → gw-1   (+ HEXPIRE 30 s)
driver pings every 10 s  → same write again, refreshing the 30 s
anyone pushing           → HGET/HMGET the recipient → publish to dispatch.push
                           with routing key "gw-1" → only gw-1's queue gets it
gw-1                     → push down the socket named in the message
```

**What travels on the socket, and what stays on HTTP.** The socket carries only
what can be lost and replaced a moment later: location pings from drivers, and
pushes to phones. Every command that needs an answer stays on HTTP: booking,
accept, reject, cancel, complete, going online or offline, and the resync.

| Message | Direction | Sent when |
|---|---|---|
| `ping` (+ `lat`/`lng` for drivers) | phone → gateway | every 10 s |
| `ride_offer` | gateway → driver | the worker offers them a ride |
| `ride_update` | gateway → rider and assigned driver | accept, cancel, complete, time out |

- **`ride_update` carries the whole ride**, not a delta. The client replaces its
  copy, so an update arriving late or twice still leaves it correct.
- **The driver hears a rider's cancel**, which matters most: they may be driving
  to the pickup.
- **Pushes are best effort.** They run after the change is committed and can
  never fail the request. A missed one is repaired by the resync on reconnect:
  `GET /rides/{id}` for riders, `GET /drivers/{id}/session` for drivers (their
  status, presence and current ride).
- **Offers are not withdrawn** when a ride is taken. An offer lives 15 s; a losing
  driver who taps accept gets 409, and the card expires on its own.

- **Gateway ids** are generated at startup (`gw-3f2a9c1e`), never a PID: PIDs
  repeat across machines and get reused. With `uvicorn --workers N`, each process
  runs its own startup and gets its own id and its own queue.
- **A driver with no entry is not offered the ride** and not recorded as
  approached, so a disconnected driver doesn't use up a slot and stays eligible
  for the next round.
- **Only offers that reached a gateway are recorded**, after publishing. The
  earlier order (record, then publish) could exclude a driver who never saw the offer.
- **A gateway that dies** without cleanup (`kill -9`) leaves its Redis entries
  behind, but its exclusive queue disappears with its connection. The worker's
  publish to it is `mandatory`, so it raises `PublishError` immediately and the
  driver is skipped. The stale entries expire within 30 s.
- **Reconnects:** a second socket for the same driver replaces the first (closed
  with code 4000). Removing an entry is a Lua check-and-delete that only removes
  it if it still names this gateway, so an old socket's cleanup can never erase a
  newer connection.

### 3.5 Accepting and rejecting an offer

`POST /rides/{id}/accept` runs, in order:

1. **Was this driver offered this ride?** `HGET` on the approached hash. No → **403**.
2. **Is the offer still open?** Server clock minus the offer time > 15 s → **409 expired**.
   The server's clock, never a timestamp from the phone, because the client controls that.
3. **Two conditional updates in one transaction:**
   ```sql
   UPDATE drivers SET status='assigned' WHERE id=$d AND status='available'
   UPDATE rides   SET status='assigned', driver_id=$d WHERE id=$r AND status='pending'
   ```
   Zero rows on either → roll back → **409**. Guarding only the ride would let one
   driver take two rides; guarding only the driver would let two drivers take one ride.

Reject only checks the driver was offered the ride. There is nothing to write:
they were excluded when the offer went out.

The approached hash is **not** deleted when the ride is taken, cancelled or times
out; its TTL cleans it up. Deleting it made every driver who answered after the
winner fail step 1, so they heard 403 "never offered" instead of 409 "taken".

### 3.6 Completing, cancelling and timing out

Every transition that ends a ride is a single conditional update. None of them
reads the status first and decides in Python:

| Transition | Allowed from | Statement |
|---|---|---|
| Complete | `assigned` | `UPDATE rides SET status='completed' WHERE id=$r AND status='assigned' RETURNING driver_id` |
| Cancel | `pending`, `assigned` | `UPDATE rides SET status='cancelled' WHERE id=$r AND status IN (...) RETURNING driver_id` |
| Time out | `pending` | `UPDATE rides SET status='timed_out' WHERE id=$r AND status='pending'` |

- **Zero rows** means something else got there first. Complete and cancel then
  return 409; the timeout just does nothing.
- **`RETURNING driver_id`** reads the driver *at the moment of the update, under
  the row lock*. A driver who accepted a millisecond before a cancel is seen and
  freed. The ride change and the driver's release commit in one transaction.
- **No deadlock with accept**, although accept locks driver → ride and cancel locks
  ride → driver: a cancel only touches the driver row after the accept has
  committed and released it.
- Complete and cancel share one helper, `_transition()` in `services/ride_booking.py`.

---

## 4. Code layout

```
main.py                 app, startup (rebuild status index, connect broker, sweeper),
                        the one place domain exceptions become HTTP status codes
gateway.py              the WebSocket gateway app: driver and rider sockets, pings, pushes
config.py               every tunable number, in one file
enums.py                DriverStatus, RideStatus; imports nothing, so every layer can use it
redis_keys.py           the whole Redis keyspace
routers/                HTTP only: parse, call a service, return
schemas/                Pydantic request/response models
services/               business logic; no FastAPI imports, so the worker can call it
    geo_index.py        the only module that talks to Redis about drivers
    drivers.py          status transitions (Postgres, then Redis)
    offers.py           approached hash, accept/reject
    dispatch_engine.py  one matching round and its outcomes
    ride_booking.py     book (idempotent), complete, cancel, time out
    connections.py      who is connected to which gateway (Redis)
    notifications.py    push a ride update to its rider and driver
    workers/dispatch_worker.py
messaging/
    connection.py       one connection per process, hands out channels
    ride_queue.py       ride.requested + ride.requested.retry
    push_queue.py       dispatch.push exchange + one exclusive queue per gateway
models/                 SQLAlchemy tables
tests/                  one runnable script per area; each prints numbered steps
bench.py                open-loop load generator
```

The rule that shaped the layout: **services never know they are behind HTTP.**
Status codes live only in `main.py`. That is why the worker, a separate process
with no requests, can call the same `dispatch_ride`, `timed_out_ride` and
`record_offers` as the API.

---

## 5. Progress log, phase by phase

### Phase 1 — The naive monolith

**Goal:** the simplest thing that works end to end.

**Built:** FastAPI + async SQLAlchemy + Postgres. Driver location stored as
columns on `drivers`. Booking searched every driver by distance *inside the HTTP
request*, then assigned one.

**Design choices made here:**
- **FastAPI over Django.** The system is I/O-bound (database, Redis, broker), and
  FastAPI is async end to end. Django's ORM is still largely synchronous, which
  would block the event loop, the exact problem async exists to avoid.
- **Distance.** Haversine is exact, but the equirectangular approximation
  (`x = Δλ·cos(φ̄)`, `y = Δφ`, `d = R·√(x²+y²)`) is cheaper and accurate at city
  scale. Measured: its error grows with *latitude span*, not distance (3.61% for
  two points 60° apart at 60°N, 5.15% Delhi→London), so it is fine for "nearest
  driver within 8 km" and wrong for intercity distances.
- **String enums** (`StrEnum`) for statuses. A plain `Enum` compared unequal to the
  string stored in the database, so `status in TERMINAL` was always False.

### Phase 2 — Break it

**Goal:** find where Phase 1 falls over, with numbers.

**Built:** [bench.py](bench.py), an *open-loop* load generator. It fires requests on
a schedule and measures latency from each request's **scheduled** time, not its
send time. A closed loop (send, wait, send) slows down when the server slows
down and hides the problem, a mistake called *coordinated omission*.

**Found:**

| Drivers | p50 at 50 rps | p50 at 250 rps |
|---|---|---|
| 100 | ~17 ms | 4.2 ms |
| 1,000 | ~17 ms | 43.2 ms |
| 10,000 | ~17 ms | 173.4 ms |

- At low load the full scan is invisible. Near capacity it dominates: **41×** slower
  from fleet size alone.
- The saturation knee was between 350 and 400 rps: p50 went from 9.2 ms to **11,171 ms**.
- `EXPLAIN` showed why: `Seq Scan rows=10000` plus a top-N sort on every booking.
- **Unplanned finding:** the `drivers` table grew to **66 MB for 10,000 rows**
  (1,648 kB after `VACUUM FULL`). Every location ping was an `UPDATE`, and Postgres
  keeps old row versions until vacuum. High-frequency writes do not belong in Postgres.

### Phase 3 — A real spatial index

**Goal:** make search cost independent of fleet size.

**Considered:** geohash, H3 hexagons, S2 cells, Redis GEO. Hex grids need
neighbour-cell lookups to avoid missing a driver just across a cell edge.
Redis `GEOSEARCH` already handles radius search, sorting and limits in one command.

**Built:** a Redis geo index of available drivers, searched with a radius ladder
(3 → 5 → 8 km), with staleness checked when reading.

**Found:** at 250 rps, p50 went from 4.2 / 43.2 / 173.4 ms to **3.5 / 4.9 / 6.2 ms**
for 100 / 1k / 10k drivers. The fleet-size penalty dropped from 41× to **1.8×**, with zero errors.

### Phase 4 — Split the hot path from the source of truth

**Goal:** location pings stop touching Postgres entirely.

**Built:**
- Location columns removed from `drivers`. A driver's position only matters live.
- Per-driver position keys (`STRING "lng,lat,epoch"`, TTL 1 hour).
- A status hash in Redis, written only on real status changes, rebuilt from
  Postgres at startup.
- A sweeper that trims stale members from the geo set. It exists for memory
  hygiene only: correctness comes from the freshness check at read time, so it
  holds even when the sweeper is dead.

**Questions settled with measurements:**
- *"Won't one geo key become a hot key?"* `GEOADD` stayed flat at ~0.17 ms from
  10k to 500k members, and pipelined writes reached **90,055/s** against a need of
  about 5,000/s. Redis is single-threaded, so a "hot key" only matters in Redis
  Cluster, a Phase 9 concern.
- *"Per-driver keys for search instead?"* **43× slower** (279.94 ms vs 6.44 ms), with
  no memory saving. The single geo index stays for search; per-driver keys hold positions.
- *STRING or HASH for positions?* 20 candidates via one `MGET`: **0.274 ms**. Via 20
  pipelined `HGETALL`: 0.552 ms. STRING is 2× faster and smaller (64 B vs 79 B).

### Phase 5 — Asynchronous matching

**Goal:** booking returns immediately; matching happens in a separate worker that
survives crashes.

**Built:** everything in sections 2.3 and 3.3–3.6. The queue classes, the dispatch
worker, the retry loop driven by the broker's TTL, per-driver offers, the
accept-window check, and idempotent booking.

**Why RabbitMQ over Redis for queues:** acknowledgements, redelivery, dead-letter
queues and delayed delivery come built in. Redis Streams can do most of this, but
the retry, the DLQ and the delay have to be built by hand.

**Why not Celery:** it is synchronous at heart. This stack is async throughout
(asyncpg, `redis.asyncio`, aio_pika), and Celery would reduce each worker to one
ride at a time. It would also take over the queue topology, which is the part
this phase exists to design.

**Why processes, not threads:** each worker is one process, one thread and one
event loop running 20 rides at once. Threads add nothing here: the work waits on
I/O, which one loop already overlaps, and Python's GIL stops threads from running
Python in parallel anyway. Separate processes fail independently. A killed worker
takes only its own rides, and RabbitMQ redelivers them.

**Measured:**
- Publish → handler start: p50 **3.5 ms**, p99 8.4 ms. Queued does not mean delayed.
- 40 messages with a 1-second handler: **2.0 s** total, 20 running at once. Prefetch is
  each worker's concurrency.
- The loop against the real worker: rounds exactly 15 s apart (22:18:18 → :33 → :48 →
  22:19:03 timed out), and an accepted ride stops at the next round.

**Done-when, verified — `kill -9` a worker mid-match:**

| Killed | Result | What made it safe |
|---|---|---|
| After consuming, before offers | Message redelivered; round 1 ran once on the new worker | RabbitMQ returns unacked messages |
| After offers, before the retry publish | Redelivered; **0 new offers** (2 total, not 4); offer times untouched; loop continued | The approached hash |
| While waiting in the retry queue | The TTL fired with no worker alive; round 2 ran as soon as one came back | The broker holds the timer |

No message reached the DLQ in any scenario.

### Phase 6 — The double-booking race

**Goal:** prove that one driver can never get two rides and one ride can never get
two drivers, under real concurrency rather than in theory.

**How it was tested:** [tests/test_races.py](tests/test_races.py) starts the API as
**4 uvicorn processes**, so concurrent requests land in separate processes with
separate connection pools. It fires requests at the same instant and checks every
invariant **in Postgres**, not from the HTTP status codes.

**First run, against the existing code:**

| Scenario | Result |
|---|---|
| 50 drivers accept one ride | ✅ 1 winner. ⚠️ But the losers got 39 × 403 "never offered" and 10 × 409 |
| One driver accepts 50 rides | ✅ Holds exactly 1 |
| Rider cancels while a driver accepts, ×100 | ❌ **3/100 drivers stuck `assigned`** on a cancelled ride, never dispatchable again |
| Complete and cancel at once, ×100 | ❌ **100/100 got 200 twice**; every trip stored as `cancelled` |

So the accept, already a conditional update since Phase 5, held. The transitions
written as read-then-write did not:
- **Cancel vs accept** fails only 3% of the time, because the window is a few
  milliseconds. That is exactly how such bugs survive into production.
- **Complete vs cancel** fails every time the two requests overlap: both read
  `assigned`, both pass the check, both write.

Under Postgres's default `READ COMMITTED` isolation, nothing stops two
transactions reading the same row, deciding, and both writing. The second
`UPDATE ... WHERE id = $r` simply waits for the row lock and then overwrites,
because nothing in its `WHERE` clause says the row changed.

**Built:**
- `complete_ride` and `cancel_ride` rewritten as conditional updates (§3.6).
- The approached hash is no longer deleted on terminal transitions (the 403s).
- A partial unique index: one assigned ride per driver, enforced by the database.

**After:** all four scenarios pass. 49 × 409 for the losers; 0/100 drivers
stranded; complete vs cancel gives exactly one 200 every time, and the stored
status matches it.

**The approaches from the roadmap, and why the database's own conditional update won:**
1. *Read-then-write*: broken, as measured above.
2. *`SELECT ... FOR UPDATE`*: correct, but holds locks across round trips, and
   inconsistent lock order between two code paths produces deadlocks.
3. *Conditional `UPDATE ... WHERE status = ...`*: correct in one round trip, with
   no lock held while the application thinks. **Used everywhere.**
4. *A Redis lock*: the wrong tool. The lock can expire while its holder is paused
   (GC, a slow query) and still believes it holds it. Postgres never checks a
   fencing token, so the database has to be the real guard anyway.

**Unplanned finding — the bug was already in the data.** The unique index could
not be created at first: **20,679 `assigned` rides from the Phase 2 load test**
broke it. 4,528 drivers held several assigned rides each, up to 42 at once.
That was the Phase 1 read-then-write matcher double-booking under 350 rps, and
nobody had noticed because nothing checked. The database was dropped and rebuilt
from the models, so every constraint now comes from code.


### Phase 7 — Real-time delivery to phones

**Goal:** stop phones polling. Offers and ride changes reach the phone the moment
they happen, even with several gateway processes, and a dropped connection
recovers on its own.

**The core problem:** a socket lives in one process. With two gateways, the
worker (a third process) and the API (a fourth) have no way to reach it
directly. The fix is a registry, `driver/rider → gateway id` in Redis, plus one
exclusive queue per gateway, routed by that id (§3.4b).

**Decisions:**
- **Gateway ids are generated at startup**, not taken from the PID or process
  name: PIDs repeat across machines and are reused, and the id has to be
  something RabbitMQ can route to.
- **Liveness from the socket ping**: a 30 s per-field TTL on each registry entry,
  refreshed by every ping, so a gateway killed without cleanup leaves nothing
  permanent behind. An exclusive queue makes publishes to a dead gateway fail
  immediately instead of waiting 30 s.
- **Location pings moved onto the socket** (the highest-volume message), with the
  HTTP endpoint kept as a fallback for backgrounded apps. Both paths share one
  validator and one function.
- **Commands stayed on HTTP**: they need status codes, idempotency and retries,
  and the race tests from Phase 6 cover them there.
- **Offers are not withdrawn** when a ride is taken: 15 s is too short to be
  worth a message, and a losing accept already gets a clean 409.
- **Only reachable drivers are offered a ride, and only offers that reached a
  gateway are recorded.** This also closed the Phase 5 gap where a crash could
  mark a driver as approached without them ever seeing the offer.

**Verified** in [tests/test_gateway.py](tests/test_gateway.py), with two real gateway processes:
- each offer reaches only its own driver's socket;
- an accept made through the API reaches a rider connected to a *different*
  gateway (the roadmap's done-when);
- the assigned driver hears a rider's cancel;
- `kill -9` on a gateway: its drivers are skipped at once and stay eligible;
- a reconnect replaces the old socket without its cleanup erasing the new entry;
- bad input over the socket gets an error frame and the connection survives.

---

## 6. Problems we hit, and what fixed them

Grouped by lesson rather than date. Each one either happened or was caught
before it could.

### Concurrency and races

| Problem | Fix |
|---|---|
| Two drivers could accept one ride, or one driver two rides | Conditional updates on **both** rows in one transaction (§3.5) |
| Any driver could accept any ride just by knowing its id | Accept checks the approached hash first (403) |
| The timeout could overwrite an accept that landed in the same instant, leaving the ride `timed_out` and its driver `assigned` | `timed_out_ride` became a conditional update; zero rows = the accept won |
| Check-then-insert idempotency: two concurrent retries both insert, and the loser gets a 500 | Insert first; the unique constraint decides; the loser reads back the winner's ride |
| Global unique idempotency key: rider B could read rider A's ride with A's key | Uniqueness scoped to `(rider_id, idempotency_key)` |
| Cancel racing an accept left the driver `assigned` on a cancelled ride (3/100) | `cancel_ride` as a conditional update that frees the driver it finds under the lock |
| Complete and cancel both succeeded on the same ride (100/100) | Both through one conditional `_transition()`; the second gets 409 |
| Losing accepts got 403 "never offered" because the winner deleted the approached hash | Never delete it early; the TTL cleans up |
| The Phase 1 matcher had double-booked 4,528 drivers during load tests, unnoticed | A partial unique index, so the database refuses it whatever the code does |

### Messaging

| Problem | Fix |
|---|---|
| `json.dumps` rejected UUIDs, so every publish failed and rides sat pending forever | Typed Pydantic payloads with `model_dump_json()` |
| `publish_message(str, dict)`: a typo in either argument silently discarded the message | One class per queue; typed `publish(payload)`; `mandatory=True` |
| A redelivered message timed out a ride that had three live offers ("no drivers" treated the same as "no *new* drivers") | Exclusion-aware rounds; only the round cap times a ride out |
| The retry queue re-declared `ride.requested` without its arguments, which closes the shared publisher channel (`PRECONDITION_FAILED`), and it had no TTL | Retry queue declares only itself: TTL + dead-letter into `ride.requested` |
| Round counting off by one: only two offer rounds ran | Booking publishes round 0; the worker increments, then checks the cap |
| `None` vs `[]` meant "stop" vs "try again"; one careless `if not x` would merge them | Named outcome exceptions (`RideNotPending`, `NoDriverAvailable`, `RideTimedOut`), all caught in the worker |
| Raising the timeout out of the handler sent every timed-out ride to the DLQ | The rule: the DLQ is for failures, never outcomes |
| Expired offers would pile up in an offer DLQ; a dead offer is never worth replaying | `ride.offers` has a TTL and no DLQ |
| The worker held a Postgres connection through every publish (pool 10, prefetch 20) | Close the session before publishing |

### Search

| Problem | Fix |
|---|---|
| `GEOSEARCH COUNT 20` ran *before* excluding approached drivers, so round 2 got the same 20 back and never reached driver #21 | Request `count + len(exclude)`, return at most `count` |
| The first version recursed without a bound: 99 calls out to 201 km | A fixed radius ladder |
| Gating the ping on "is the driver in the geo set" meant a swept driver could never return | Gate on the status hash |
| A driver finishing a ride had no position to re-enter the geo index until their next ping | Per-driver position keys with a long TTL |

### Infrastructure and tooling

| Problem | Fix |
|---|---|
| `create_all` never alters existing tables. New columns silently didn't exist, three times | Manual `ALTER TABLE`, then the database was dropped and rebuilt from the models in Phase 6. **Alembic is overdue** |
| `bench.py seed` inserted ids 1–10000 without moving the id sequence, so every later insert collided | `setval` on both sequences after seeding |
| Registration reported *any* integrity error as "phone already registered", which hid the collision above | Check the constraint name; re-raise anything else |
| Sync Redis client blocking the event loop; `host='redis'` unreachable from the host; inverted compose port mapping | `redis.asyncio`; `localhost:6380`; `6380:6379` |
| Port collisions with other local stacks | Postgres 5433, Redis 6380, RabbitMQ 5673 |
| Tests interfering with each other through leftover drivers, phone numbers and queued messages | Each suite resets its own Redis keys, drivers and queues |
| Worker output didn't appear (Python buffers `print` when output isn't a terminal) | `PYTHONUNBUFFERED=1` |
| aio_pika's robust channel caches queue objects, so repeated passive declares returned stale message counts during the kill test | Count on a fresh channel each time |

---

## 7. Known gaps

Deliberate, with the phase that addresses each.

- **Dual writes (Phase 8).** Booking commits the ride, then publishes. A crash in
  between leaves a pending ride no worker will see. Similarly, the worker records
  approached drivers before publishing their offers: a crash between the two skips
  those drivers for that ride. The transactional outbox fixes both.
- **Phase 7, what is left.** The rider cannot yet see the assigned driver moving:
  the driver's location ping would also need forwarding to the rider while a
  ride is assigned. And the pushes are a dual write (commit, then publish),
  covered by the resync until the Phase 8 outbox.
- **Sockets are unauthenticated.** Anyone can connect as any driver id and read
  their offers.
- **One active ride per rider** is not enforced. Two sessions with two different
  idempotency keys can book two rides. The fix is a partial unique index on
  `rides(rider_id) WHERE status IN ('pending','assigned')`.
- **No schema migrations.** Alembic, before the next schema change. The
  database has been rebuilt from scratch rather than migrated so far.
- **`bench.py` is out of date.** Its `seed` still writes `lat`/`lng` columns on
  `drivers`, which Phase 4 removed, so it cannot run. It needs to seed the Redis
  position keys and status hash instead.
- **Drivers can set their own status to anything.** `PATCH /drivers/{id}/status`
  accepts any value, including `available` in the middle of a ride. Once there
  is auth, only `offline ↔ available` should be driver-settable.
- **`services/notifications.py`** (the `DriverNotifier` interface) is no longer used
  by anything since offers moved to `ride.offers`. Delete it, or reuse it as the
  gateway's delivery interface in Phase 7.

---

## 8. Running it

```bash
docker compose up -d                                   # Postgres, Redis, RabbitMQ
uv run uvicorn main:app --reload                       # API
uv run python -m services.workers.dispatch_worker      # one worker; run N times for N
uv run uvicorn gateway:app --port 8001                 # WebSocket gateway
```

Tests are plain scripts, each printing numbered steps:

```bash
uv run python tests/test_dispatch_flow.py
uv run python tests/test_gateway.py      # starts two gateways on :8801/:8802
uv run python tests/test_driver_index.py
uv run python tests/test_idempotency.py
uv run python tests/test_phase1.py
uv run python tests/test_races.py        # starts its own 4-process API on :8765
uv run python tests/test_registration.py
```
