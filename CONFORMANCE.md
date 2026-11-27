# The Stream proposal for OGC API – Moving Features — requirements

The requirements of the **Continuous Query** of the Stream proposal for OGC API –
Moving Features (titled "Part 4: Stream Extension"), keyed by the number and
identifier the proposal gives them, each with the code that meets it and the test
that reads it. `ats_part4_test.go` holds the suite to this table. OGC holds no
draft of a Stream part, so the service claims no conformance class for it: the
continuous queries are MobilityAPI extensions, which the API definition marks
`x-mobilityapi-extension`.

What the service declares and answers under every standard it implements, and
the extensions it serves, is stated by the specification [MobilityAPI and the OGC
API Standards](https://github.com/MobilityDB/MobilityDB/blob/master/doc/specs/mobilityapi-ogc-api.md).

## Requirements

| Requirement | Status | Where |
|---|---|---|
| **Req 1** `cquery_link` — the `cquery` link object carries `rel`, `queryId`, `href`, `channel`, `status`, `type` | **met** | `cqueryLink` (`stream.go`); asserted by `TestCqueryLinkShape` |
| **Req 2** `window` — a query carries a `window`; the result carries `[windowStart, windowEnd]`, `[start, end)` for a time window and `[start, end]` for a count window | **met** | `postQuery` window parsing; `emitWindow` (`stream_engine_meos.go`) writes the bounds of the window; `TestMeosWindowBounds` |
| **Req 3** `window_tumbling` — fixed-duration, non-overlapping windows | **met** | `runAggregate` TUMBLING (`stream_engine_meos.go`); `TestMeosTumblingWindow` |
| **Req 4** `window_hopping` — fixed-duration, overlapping windows | **met** | `runHopping` (`stream_engine_meos.go`); `TestMeosHoppingWindow` |
| **Req 5** `window_count` — windows over a record count | **met** | `runAggregate` COUNT (`stream_engine_meos.go`); `TestMeosWindowAggregate` |
