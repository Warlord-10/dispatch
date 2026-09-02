# Dispatch — a real-time geo matching engine

Matching riders to nearby drivers under a latency SLO, built by breaking the naive
version on purpose and fixing it with the concept that was invented for that break.

**Rule of the project: never add a layer before you have measured the pain it removes.**
Every phase has a *Break it* step. If you skip that step you are collecting
buzzwords, not learning system design.

---

## Phase 0 — Requirements, SLOs, envelope math

No code. One page in `DESIGN.md`.

- **Functional:** driver goes online / pings location; rider requests ride at a
  pickup point; system assigns exactly one nearby driver; both sides see status
  in real time; either side can cancel; ride completes.
- **Non-functional (pick numbers and commit to them):**
  - p99 request→assignment ≤ 2s
  - a driver is never concurrently assigned to two rides — **hard invariant**
  - driver location staleness ≤ 5s
  - target scale: 50k online drivers in a city, 300 ride requests/s peak
- **Envelope math:** 50k drivers × 1 ping / 4s = **12.5k writes/s** vs 300 reads/s.
  Write:read is ~40:1 and the writes are worthless 4 seconds later. Write that
  ratio down — it dictates almost every decision from Phase 3 onward.

**Concepts:** functional vs non-functional requirements, SLI/SLO/error budget,
capacity estimation, identifying the dominant flow before designing.

**Done when:** you can state, in one sentence each, the hard invariant and the
single number that will break your first design.

---

## Phase 1 — The deliberately naive monolith

One FastAPI process, one Postgres. Matching happens **inside** the HTTP handler.

- `drivers(id, status, lng, lat, updated_at)`, `rides(id, rider_id, pickup, status, driver_id)`
- `POST /rides` → `SELECT ... FROM drivers WHERE status='available' ORDER BY <distance> LIMIT 1`
  → update both rows → return the assignment.

Use the naive distance expression on purpose. No index that helps it.

**Concepts:** schema design, transactions, synchronous request path, connection pooling.

**Done when:** it works end to end for ~10 drivers and you can articulate why the
`ORDER BY distance` is a full scan.

---

## Phase 2 — Break it (the most important phase)

Write `sim.py`: N drivers pinging on a jittered interval, M riders requesting.
Ramp until it falls over. Record p50/p95/p99 and error rate at each step.

You are looking for three distinct failures, and you should be able to name which
is which from the graph shape alone:

1. Latency climbing linearly with driver count → the full scan.
2. Latency hitting a cliff, then timeouts → pool exhaustion / queueing.
3. Throughput flat while latency rises → you are saturated, not slow.

**Concepts:** load testing, percentiles (why the mean lies), Little's Law
(`concurrency = throughput × latency`), queueing delay vs service time,
coordinated omission, saturation vs latency.

**Done when:** you have a table of QPS → p99 and you can predict the p99 at
2× load before running it.

---

## Phase 3 — A real spatial index

Fix failure #1. Two credible options; implement one, understand both:

- **Redis GEO** (`GEOADD` / `GEOSEARCH`) — a geohash-scored sorted set. Ephemeral,
  extremely fast, in-memory.
- **PostGIS** `geography` + GiST index — durable, joinable with the rest of your data.

The subtle part, and the thing interviewers probe: geohash cells are rectangles,
so the true nearest neighbour can sit in an **adjacent cell**. A single-cell lookup
is a *recall bug*, not a performance detail. Understand how ring/neighbour search
fixes it, and where H3 (hex, uniform neighbours) and S2 (quadtree on a sphere)
differ from geohash.

**Concepts:** spatial indexing, geohash/S2/H3, space-filling curves, why a B-tree
on `(lat, lng)` cannot answer "nearest", precision vs recall in cell queries,
index selectivity.

**Done when:** driver-count no longer moves p99, and you have a test proving the
nearest driver is found when they sit just across a cell boundary.

---

## Phase 4 — Split the hot path from the source of truth

Fix the 40:1 ratio from Phase 0. Location pings stop touching Postgres entirely —
they land in Redis. Postgres keeps only what must survive a restart: drivers,
rides, assignments.

Staleness becomes a *design tool*: a driver whose last ping is older than N seconds
is simply not dispatchable. That is a deliberate consistency trade, so write down
why losing the whole location index on a Redis restart is acceptable.

**Concepts:** hot path vs durable path, choosing a consistency model **per data
class** rather than per system, write amplification, TTL/staleness as correctness,
cache vs system-of-record, blast radius of a dependency loss.

**Done when:** Postgres write QPS drops ~40×, and killing Redis degrades matching
without corrupting any ride.

---

## Phase 5 — Asynchronous matching

`POST /rides` writes `status='pending'`, enqueues, and returns **202** immediately.
A separate matcher worker consumes. Start with Redis Streams + consumer groups
(**not** Kafka — you have not earned Kafka yet, and Streams teaches the same primitives).

Build the unglamorous parts, they are the actual content of this phase:

- idempotency key on ride creation, so a client retry doesn't create two rides
- at-least-once delivery ⇒ your consumer **must** be idempotent
- `XPENDING` / `XCLAIM` to recover messages from a worker that died mid-flight
- a retry policy with exponential backoff **and jitter**, and a terminal
  `unmatched` state after K attempts (this is your dead-letter path)

**Concepts:** async decoupling, queue vs sync RPC, at-least-once vs at-most-once vs
effectively-once, consumer groups, visibility timeout, DLQ, backpressure, the fact
that a queue converts a latency problem into a durability problem.

**Done when:** you `kill -9` a matcher mid-assignment and no ride is lost or
double-processed.

---

## Phase 6 — The double-booking race (the crown jewel)

Run 8 matcher workers. Point many simultaneous ride requests at a region with one
available driver. You will violate your hard invariant. **Reproduce it first** —
a race you have not seen fail is a race you do not understand.

Then walk the ladder deliberately and measure each rung:

1. read-then-write — broken, and know *why* `READ COMMITTED` permits it
2. `SELECT ... FOR UPDATE` — pessimistic; correct, but watch throughput and deadlocks
3. `UPDATE drivers SET ... WHERE id=$1 AND status='available'` returning rowcount —
   optimistic; correct, one round trip, no lock held. On 0 rows, try the next candidate.
4. a Redis distributed lock — deliberately the *wrong* tool here, and you should be
   able to say why (fencing tokens, clock assumptions, the Redlock argument)

**Concepts:** lost update, isolation levels, optimistic vs pessimistic concurrency,
compare-and-set, idempotency, deadlocks, why "just add a lock" is usually a
database feature you already had.

**Done when:** a test fires 50 concurrent assignment attempts at one driver and
asserts **exactly one** wins — and it fails if you revert to rung 1.

---

## Phase 7 — Real-time status to clients

WebSocket for ride status. Then run **two** API replicas and watch it break: the
rider's socket lives on replica A, the matcher's update is published from replica B,
and the rider hears nothing.

Fix with Redis pub/sub fanout. Then handle reconnect: a socket that drops must not
lose the transition it missed — so the client resyncs from `GET /rides/{id}` on
connect, and the socket carries only deltas.

**Concepts:** stateful connections behind a load balancer, sticky sessions and why
they are a trap, pub/sub fanout, presence, at-most-once push + read-your-writes
recovery, long-poll/SSE/WebSocket trade-offs.

**Done when:** with 2+ replicas, the rider sees the assignment regardless of which
replica holds the socket, and a forced disconnect self-heals.

---

### ⛳ Weekend finish line is here — Phases 0–7.

That is a genuinely distributed system with a proven concurrency invariant and a
measured before/after at every layer. Everything below is the following weekends.

---

## Phase 8 — The dual-write problem and the transactional outbox

You assign the driver in Postgres, then push the notification. The process dies
between the two. Now the ride is assigned and nobody knows.

Fix: write the event into an `outbox` table **in the same transaction** as the
assignment; a relay publishes it and marks it sent. Consumers are idempotent, so
duplicate publishes are harmless.

Then add cancellation as a **saga**: ride cancelled → free the driver → re-add to
the geo index → notify. Each step compensating and retryable.

**Concepts:** dual-write problem, transactional outbox, CDC / logical replication,
"exactly-once" as at-least-once + idempotent consumer, sagas and compensating
transactions, two-phase commit and why you're avoiding it.

---

## Phase 9 — Sharding and hot partitions

Partition by city, then by geohash prefix. Then simulate a stadium emptying: one
cell takes 60% of traffic.

**Concepts:** partition key selection, consistent hashing, virtual nodes, hot-shard
mitigation (split, salt, jitter), cross-shard queries, rebalancing without downtime,
why the obvious key is often the hot one.

## Phase 10 — Rate limiting and fairness

Per-rider request limits, per-driver ping limits, per-tenant quotas. Implement the
counter atomically (Redis Lua) — a read-modify-write rate limiter is Phase 6 again.

**Concepts:** token bucket vs sliding window vs leaky bucket, atomicity in the
limiter itself, 429 + `Retry-After`, accuracy vs coordination cost in a distributed
limiter, admission control as a load-shedding tool.

## Phase 11 — Caching, read replicas, CQRS

Add read-heavy endpoints (city supply heatmap, ETA lookups) and serve them from a
separate read path.

**Concepts:** cache-aside vs write-through, TTL, cache stampede / thundering herd
and single-flight, probabilistic early expiration, read replicas and replica lag,
materialized views, CQRS, eventual consistency that a user can actually notice.

## Phase 12 — Surge pricing as stream processing

Demand/supply per cell over sliding windows.

**Concepts:** windowing (tumbling / sliding / session), watermarks, late and
out-of-order events, stateful aggregation, checkpointing.

## Phase 13 — Observability

Structured logs, RED + USE metrics, Prometheus/Grafana, OpenTelemetry traces that
survive the **queue hop** (propagate trace context inside the message — this is the
part everyone gets wrong).

**Concepts:** metrics vs logs vs traces, cardinality explosions, trace context
propagation across async boundaries, SLO burn-rate alerting, alert on symptoms.

## Phase 14 — Failure injection

Kill Redis. Kill Postgres' primary. Add 500ms of latency. Partition the matcher
from the queue. Fill the disk.

**Concepts:** graceful degradation, circuit breakers, timeouts and budgets, retry
storms and why jitter is not optional, bulkheads, CAP as an operational question
("what do we serve when Redis is gone?"), chaos engineering, runbooks.

## Phase 15 — Where AI actually earns its place (optional)

Only after the system is sound. Each of these needs a latency budget and a fallback
for when the model is slow or wrong:

- **Demand forecasting per cell** → driver repositioning nudges. Offline training,
  online inference, feature store, model staleness.
- **ETA prediction** replacing a naive haversine estimate. Measure whether it
  actually beats the baseline — most "AI features" don't.
- **NL ops console:** "why were 40 rides unmatched in Koramangala at 9pm?" over
  your own metrics and traces. Teaches tool-calling agents, grounding, and the
  guardrails around letting a model query production.

**Concepts:** AI as a component with an SLO, offline vs online inference, graceful
fallback, evaluation against a baseline, cost per inference in the hot path.

---

## Stack, and what to deliberately *not* use yet

| Need | Use | Not yet, and why |
|---|---|---|
| API + WebSocket | FastAPI + uvicorn | — |
| Source of truth | Postgres (asyncpg) | Not Mongo — you want transactions in Phase 6 |
| Spatial index | Redis GEO or PostGIS | Not Elasticsearch — wrong tool, big ops cost |
| Queue | Redis Streams | Not Kafka until Phase 9 makes partitions real |
| Workers | plain asyncio loop | Not Celery — it hides the delivery semantics you're learning |
| Local topology | docker compose | Not Kubernetes — it teaches you YAML, not design |
| Metrics | Prometheus + Grafana | — |

Every one of those "not yet" entries is a real system-design answer: the cost of a
component is not its features, it's the failure modes and operational surface it adds.

## How to work through this

1. Keep a `DESIGN.md` and append to it every phase: what broke, the numbers before
   and after, and what you chose *not* to do. This document is the actual output of
   the project — the code is just the evidence.
2. One measurement per phase. A phase without a before/after number did not happen.
3. Each phase leaves exactly one runnable check behind. Phase 6's concurrency test
   is the one that matters most; it should fail loudly if anyone reverts the fix.
