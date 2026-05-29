MobilityAPI
===========

[![License: PostgreSQL](https://img.shields.io/badge/License-PostgreSQL-blue.svg)](https://www.postgresql.org/about/licence/)
[![Go 1.25+](https://img.shields.io/badge/go-1.25+-00ADD8.svg)](https://go.dev/)
[![OGC API – Moving Features](https://img.shields.io/badge/OGC%20API-Moving%20Features-green.svg)](https://docs.ogc.org/is/22-003r3/22-003r3.html)

The reference implementation of the [OGC API – Moving Features Standard](https://docs.ogc.org/is/22-003r3/22-003r3.html) over [MobilityDB](https://github.com/MobilityDB/MobilityDB) — a thin, compiled HTTP tier for clients that don't speak SQL or the PostgreSQL wire protocol.

## What it is

MobilityAPI exposes MobilityDB collections of moving features over plain HTTP — for browser apps, mobile clients, microservices, and ETL / lakehouse pipelines. It is a thin tier: every temporal computation and (de)serialization runs inside MobilityDB itself (`asMFJSON`, `atTime`, `tgeompointFromMFJSON`, `appendInstant`). The server holds no MEOS — no cgo, no embedded library — so it stays small and stateless, and the database is the single source of truth.

- **Streaming responses** — a FeatureCollection is written row by row from a server-side cursor, so memory is bounded regardless of result size.
- **Keyset pagination** — `WHERE id > :after` with OGC `next` links, no `OFFSET`, constant cost through large collections.
- **Index-using filters** — `bbox` and `datetime` push to the MobilityDB GiST index; `subtrajectory` clips with `atTime`.
- **Lakehouse export** — a streaming `GET …/export` feed (NDJSON, or `?format=parquet` with the trajectory WKB plus a bbox/time sidecar) that DuckDB / MobilityDuck / Spark can ingest directly.

This Go server is the reference (production) implementation. A PyMEOS-based Python implementation is also available at [MobilityAPI-Python](https://github.com/MobilityDB/MobilityAPI-Python).

## Architecture

```
HTTP client  ──▶  MobilityAPI (Go · net/http + pgxpool)  ──▶  MobilityDB / PostgreSQL
```

The tier translates between OGC API – Moving Features requests/responses and MobilityDB SQL; all geometry and temporal logic lives in the database.

## Endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/` · `/api` · `/conformance` | Landing page, OpenAPI document, conformance declaration |
| GET | `/collections` · `/collections/{cid}` | Collections and collection metadata (spatial + temporal extent) |
| GET | `/collections/{cid}/items` | Moving features, streamed and keyset-paged, with `bbox` / `datetime` / `subtrajectory` filters |
| GET | `/collections/{cid}/items/{fid}` | A moving feature as a Feature |
| POST | `/collections/{cid}/items` | Create a moving feature from a MovingFeatureJSON body |
| GET | `/collections/{cid}/items/{fid}/tgsequence` | The temporal geometry (MF-JSON) |
| GET | `/collections/{cid}/items/{fid}/tproperties` · `/.../{pname}` | Derived temporal properties: `velocity` · `distance` · `heading` |
| GET | `/collections/{cid}/export` | Lakehouse feed: NDJSON, or `?format=parquet` |
| PUT / DELETE | `/collections/{cid}/items/{fid}` | Replace or delete a feature |
| POST | `/collections/{cid}/items/{fid}/tgsequence` | Append a temporally-disjoint sub-trajectory |

`GET /export`, `PUT`, and `DELETE` are opt-in extensions beyond the OGC conformance classes declared at `GET /conformance`.

## Prerequisites

- Go 1.25 or later
- PostgreSQL with the [MobilityDB](https://github.com/MobilityDB/MobilityDB) extension (and PostGIS)

## Build and run

```bash
go build -o mfapi .
MFAPI_DSN="postgres:///mydb?host=/var/run/postgresql&port=5432&user=me" ./mfapi
```

The server listens on `:8088` by default. Configuration is by environment variable:

| Variable | Default | Meaning |
|---|---|---|
| `MFAPI_DSN` | `postgres:///mfapi_demo?host=/tmp&port=5432&user=esteban` | MobilityDB connection string |
| `MFAPI_PORT` | `8088` | Listen port |
| `MFAPI_MAXCONNS` | `16` | Connection-pool size |
| `MFAPI_DEFAULT_LIMIT` / `MFAPI_MAX_LIMIT` | `100` / `10000` | Page size defaults and ceiling |
| `MFAPI_EXPORT_LIMIT` | `0` (unbounded) | Rows per export stream |
| `MFAPI_PARQUET_ROWGROUP` | `1024` | Rows per Parquet row group (bounds export memory) |

## Test

```bash
go test ./...
```

## Repository layout

- `main.go` — the server: routing, OGC request/response shaping, streaming and export.
- `bench/` — a comparison harness and results.
- `tutorial/` — a notebook walkthrough of the endpoints against the canonical AIS dataset.

## Where MobilityAPI fits

MobilityAPI is the HTTP / OGC layer of the MEOS ecosystem:

- **MEOS** (canonical C library) — the underlying type system and computations.
- **MobilityDB** · **MobilityDuck** · **MobilitySpark** — peer SQL surfaces over MEOS.
- **Language bindings** — [PyMEOS](https://github.com/MobilityDB/PyMEOS), [JMEOS](https://github.com/MobilityDB/JMEOS), [meos-rs](https://github.com/MobilityDB/meos-rs), [GoMEOS](https://github.com/MobilityDB/GoMEOS), [MEOS.NET](https://github.com/MobilityDB/MEOS.NET), [MEOS.js](https://github.com/MobilityDB/MEOS.js).

A longer overview is at [libmeos.org](https://libmeos.org/).

## License

MobilityAPI is released under [The PostgreSQL License](https://www.postgresql.org/about/licence/).
