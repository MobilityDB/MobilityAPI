# OGC API – Moving Features – Part 4 (Stream Extension) — conformance

This tier implements the **Continuous Query** requirements class of OGC API –
Moving Features – Part 4. The class is declared at `GET /conformance`:

```
http://www.opengis.net/spec/ogcapi-movingfeatures-4/1.0/conf/cquery
```

## Requirements

| Requirement | Status | Where |
|---|---|---|
| **Req 1** `cquery_link` — the `cquery` link object carries `rel`, `queryId`, `href`, `channel`, `status`, `type` | **met** | `cqueryLink` (`stream.go`); asserted by `TestCqueryLinkShape` |
| **Req 2** `window` — a query carries a `window`; the result carries `[windowStart, windowEnd]` | **met** | `postQuery` window parsing; the meos engine emits the bounds; `TestWindowResultShape` |
| **Req 3** `window_tumbling` — fixed-duration, non-overlapping windows | **met** | `runAggregate` TUMBLING (`stream_engine_meos.go`) |
| **Req 4** `window_hopping` — fixed-duration, overlapping windows | **met** | `runHopping` (`stream_engine_meos.go`); `TestMeosHoppingWindow` |
| **Req 5** `window_count` — windows over a record count | **met** | `runAggregate` COUNT; `TestMeosWindowAggregate` |

## Aggregations

`TemporalProperty` `TReal` — **met**: `COUNT, SUM, AVG, MIN, MAX`, computed
through MEOS (`temporal_num_instants`, `tfloat_min_value`/`tfloat_max_value`,
`tnumber_avg_value`, and the value sum).

`TText` (`COUNT, COUNT_DISTINCT`), `TBoolean` (`ANY, ALL, …`), and the
`TemporalPrimitiveGeometry` operations of Table 7 (`LENGTH, AREA, TO_TRAJ, …`)
are **not yet** exposed.

## Lifecycle and delivery

- Lifecycle states `running`, `stopped`, `failed` are reported by the `cquery`
  `status` (`registered` is transient — a query enters `running` on submission).
- Results are delivered over **Server-Sent Events**, one of the channel bindings
  Part 4 allows; the Kafka and WebSocket result bindings are not yet exposed.
- Live ingestion: a query with `"live": true` sources pushed records. Producers
  push over HTTP (`…/ingest`) or **MQTT** (`MFAPI_MQTT_BROKER`, topic
  `mfapi/<cid>/<fid>/<pname>`) — the EDR Pub/Sub source over a real broker. A
  Kafka source feeds the same hub the same way.

## Extensions beyond Part 4

Lifted scalar transforms over a streaming `tfloat` (`ln`, `exp`, `×`, `+`, …), a
geometry position stream (`…/tgsequence/queries`) for an animated map, and the
Flink streaming engine behind the same `StreamEngine` seam are additions, not
Part 4 requirements.
