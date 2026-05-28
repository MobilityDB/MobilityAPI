# MobilityAPI tier comparison — Go vs Python, same MobilityDB

Both tiers run **identical SQL** (`asMFJSON` + jsonb assembly, `tgeompointFromMFJSON`
on writes) against the **same database** (`mfapi_demo`: 500 Danish-AIS vessel
trajectories as the `ships` collection, MobilityDB 1.4 on PostgreSQL 17). The only
variable is the HTTP/runtime tier.

- **Go**: `net/http` + `pgxpool` (16 connections), single static binary, no cgo.
- **Python baseline**: threaded `http.server` + `psycopg2` `ThreadedConnectionPool` (16) — a controlled proxy for the Python tier, same SQL/DB.
- **Load**: `hey`, warm cache, localhost.

## Throughput and reliability

| Endpoint | Concurrency | Go req/s | Go success | Python req/s | Python success |
|---|---:|---:|---:|---:|---:|
| `/health` (no DB — pure tier) | 200 | **85,950** | 30000/30000 | 1,337 | 30000/30000 |
| `/collections` (light DB) | 200 | **30,709** | 20000/20000 | 649 | 14748/20000 |
| `…/items/1` (one trajectory) | 100 | **2,434** | 5000/5000 | 547 | 3623/5000 |
| `…/items?limit=10` (ten full trajectories) | 50 | 90 | 3000/3000 | 162* | 1531/3000 |

\* Python's higher number on the heavy endpoint is load-shedding, not throughput: it completed only 51 % of requests; the remainder errored. Go completed 100 %.

## Reading the result

- **Tier-bound endpoints** (little/no per-request DB work): Go is **~47–64×** faster, because the cost is per-request runtime overhead, which the compiled, no-GIL, goroutine-per-request model absorbs and the Python tier does not.
- **Heavy DB-bound endpoint** (serializing full trajectories): raw throughput is limited by `pool × asMFJSON` and converges; the difference is **reliability** — Go serves every request at the DB-bound ceiling, while the Python tier **sheds ~half the load** at c=50 and ~26 % at c=200.
- **The DB is the shared bottleneck** for heavy serialization; the tier language decides how the front end behaves under concurrency.

## Caveats

This Python baseline is threaded `http.server` (same family as the current single-threaded server, which would be lower still). A FastAPI/uvicorn/asyncpg or gunicorn-multi-worker deployment would raise the Python ceiling, but the structural gap on tier-bound paths and the load-shedding behaviour under concurrency are properties of the runtime model, not of this particular server. A run against the production PyMEOS server (point `run_compare.sh` at its host:port) refines the exact figures.
