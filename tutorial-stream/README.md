# MobilityAPI — MF Stream tutorial (continuous queries)

`tutorial.ipynb` walks through the streaming half of MobilityAPI: the
[OGC API – Moving Features – Part 4 (Stream Extension)](https://www.opengis.net/spec/ogcapi-movingfeatures-4/1.0)
continuous-query endpoints, served by the same Go tier over MobilityDB/MEOS. A
continuous query applies a **lifted** scalar operation (`ln`, `exp`, `×`, `+`, …)
to a streaming `tfloat` and delivers the transformed instants over Server-Sent
Events — the streaming counterpart of applying a float operation to a scalar.

The notebook is plain HTTP + SSE, so it is **engine-agnostic**: it runs on the
in-process `meos-local` engine, and the same notebook drives the Flink, Kafka
Streams, and Spark Structured Streaming engines once selected. Those plug into
the `StreamEngine` seam exactly as PostgreSQL, DuckDB, and Spark plug into the
request–response `Backend` seam.

## Selecting the engine

The engine is chosen where the tier starts, not in the notebook:

- default — the in-process `meos-local` engine (`-tags meos`, libmeos linked).
- `MFAPI_STREAM_ENGINE=flink` — run each continuous query as a Flink DataStream
  job; the tier itself needs no MEOS. See [`flink/README.md`](flink/README.md) for
  the bridge job and its configuration (`MFAPI_FLINK_CMD`, `MFAPI_FLINK_LIBPATH`).

The Kafka Streams engine (`MFAPI_STREAM_ENGINE=kafka`) plugs into the same seam
through the same line-protocol contract; see [`kafka/README.md`](kafka/README.md)
for its bridge job and configuration (`MFAPI_KAFKA_CMD`, `MFAPI_KAFKA_LIBPATH`). A
Spark Structured Streaming engine plugs in the same way.

## Prerequisites

- The Go tier built **with the streaming engine** and reachable on
  `http://localhost:8088`:

  ```
  CGO_ENABLED=1 go build -tags meos -o mfapi .
  MFAPI_DSN=<dsn> LD_LIBRARY_PATH=/usr/local/lib ./mfapi
  ```

  The default (cgo-free) build serves every other endpoint but reports the
  streaming engine as not built (`501`). Point the notebook at another host with
  the `MFAPI_HOST` environment variable.

- A Jupyter kernel. The first cell installs `requests`.

The notebook is self-contained: it creates its own `stream_demo` collection, one
moving feature, and a `speed` temporal float, then registers and streams a
continuous transform. No external dataset is required.

## Endpoints exercised

| Method | Path | Meaning |
|---|---|---|
| `POST` | `…/tproperties/{name}/queries` | register a continuous transform (`operation`) or a windowed aggregate (`aggregation` + `window`) |
| `GET` | `…/tproperties/{name}/queries` | list registered queries |
| `GET` | `…/tproperties/{name}/queries/{queryId}` | query status (`cquery` link object) |
| `GET` | `…/tproperties/{name}/queries/{queryId}/stream` | result stream (Server-Sent Events) |
| `DELETE` | `…/tproperties/{name}/queries/{queryId}` | stop the query |
| `POST` | `…/tproperties/{name}/ingest` | push a live record (`{datetime, value}`) to the live queries on the property |
| `POST` | `…/tgsequence/queries` | register a position (geometry) query |
| `GET` | `…/tgsequence/queries/{queryId}/stream` | the moving feature's positions (SSE) |

A query registered with `"live": true` runs over the pushed stream instead of a
replay of stored values: a producer `POST`s records to `…/ingest` and every live
query on the property processes them in real time — the broker-agnostic form of
the EDR Pub/Sub source (a Kafka/MQTT binding feeds the same hub).

The geometry query streams the moving feature's position as `{datetime,
coordinates}` events — the feed an animated map consumes: a browser subscribes to
the SSE stream and renders the moving point (e.g. a DeckGL `TripsLayer` over a
MapLibre basemap, with [MEOS.js](https://github.com/MobilityDB/MEOS.js) handling
the temporal values client-side), interpolating between the streamed vertices to
animate smooth motion. A property query (speed → knots, or a windowed aggregate)
overlays the live value.
