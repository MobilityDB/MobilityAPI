# OGC API – Moving Features – Part 4 (Stream Extension) — requirements

The requirements of the **Continuous Query** class of OGC API – Moving Features –
Part 4, `http://www.opengis.net/spec/ogcapi-movingfeatures-4/1.0/conf/cquery`,
keyed by the number and identifier of the draft this service implements, each with
the code that meets it and the test that reads it. `ats_part4_test.go` holds the
suite to this table.

What the service declares and answers under Part 4 and every other standard it
implements is stated by the specification [MobilityAPI and the OGC API
Standards](https://github.com/MobilityDB/MobilityDB/blob/master/doc/specs/mobilityapi-ogc-api.md).

## Requirements

| Requirement | Status | Where |
|---|---|---|
| **Req 1** `cquery_link` — the `cquery` link object carries `rel`, `queryId`, `href`, `channel`, `status`, `type` | **met** | `cqueryLink` (`stream.go`); asserted by `TestCqueryLinkShape` |
| **Req 2** `window` — a query carries a `window`; the result carries `[windowStart, windowEnd]`, `[start, end)` for a time window and `[start, end]` for a count window | **met** | `postQuery` window parsing; `emitWindow` (`stream_engine_meos.go`) writes the bounds of the window; `TestMeosWindowBounds` |
| **Req 3** `window_tumbling` — fixed-duration, non-overlapping windows | **met** | `runAggregate` TUMBLING (`stream_engine_meos.go`); `TestMeosTumblingWindow` |
| **Req 4** `window_hopping` — fixed-duration, overlapping windows | **met** | `runHopping` (`stream_engine_meos.go`); `TestMeosHoppingWindow` |
| **Req 5** `window_count` — windows over a record count | **met** | `runAggregate` COUNT (`stream_engine_meos.go`); `TestMeosWindowAggregate` |
