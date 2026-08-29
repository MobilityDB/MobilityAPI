# Sample documents

Every file here is the body of one request through the service's own routing
table, written by the service itself:

    MFAPI_DSN=... mfapi -emit samples

so a sample states what the code answers rather than what its author believed
it answers. Regenerating rewrites the whole set.

These were read from the conformance fixture
(`tutorial/setup/load_conformance.sql`), whose collection is `conformance`, whose
first feature is `1` and whose first temporal property is `anchored`. Pointed at
another database the emitter names that database's own resources instead: it
takes the first collection the service lists, that collection's first feature
and that feature's first temporal property.

A status other than 200 is a sample too. The tier answers 501 for an
acceleration under linear interpolation, because a velocity that is constant on
every segment has no acceleration the standard's motion model can carry, and
saying so is the honest answer rather than a zero.

⛔ The `timeStamp` of the TemporalProperties document is the moment the
document was written, which the standard defines it to be, so that one line
differs between two regenerations of an unchanged service.

| file | status | request | resource |
|---|---|---|---|
| [`landing-page.json`](landing-page.json) | 200 | `GET /` | the landing page |
| [`api-definition.json`](api-definition.json) | 200 | `GET /api` | the API definition |
| [`conformance-declaration.json`](conformance-declaration.json) | 200 | `GET /conformance` | the conformance declaration |
| [`collections.json`](collections.json) | 200 | `GET /collections` | the Collections document |
| [`collection.json`](collection.json) | 200 | `GET /collections/conformance` | a Collection document |
| [`moving-feature-collection.json`](moving-feature-collection.json) | 200 | `GET /collections/conformance/items` | a MovingFeatureCollection document |
| [`moving-feature.json`](moving-feature.json) | 200 | `GET /collections/conformance/items/1` | a MovingFeature document |
| [`temporal-geometry-sequence.json`](temporal-geometry-sequence.json) | 200 | `GET /collections/conformance/items/1/tgsequence` | a TemporalGeometrySequence document |
| [`temporal-geometry-distance.json`](temporal-geometry-distance.json) | 200 | `GET /collections/conformance/items/1/tgsequence/1/distance` | the distance a temporal primitive geometry covers |
| [`temporal-geometry-velocity.json`](temporal-geometry-velocity.json) | 200 | `GET /collections/conformance/items/1/tgsequence/1/velocity` | the velocity along a temporal primitive geometry |
| [`temporal-geometry-acceleration.json`](temporal-geometry-acceleration.json) | 501 | `GET /collections/conformance/items/1/tgsequence/1/acceleration` | the acceleration along a temporal primitive geometry |
| [`temporal-properties.json`](temporal-properties.json) | 200 | `GET /collections/conformance/items/1/tproperties` | a TemporalProperties document |
| [`temporal-property.json`](temporal-property.json) | 200 | `GET /collections/conformance/items/1/tproperties/anchored` | a TemporalProperty document |
