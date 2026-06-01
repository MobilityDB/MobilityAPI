# MobilityAPI, the mf-api reference, and the OGC standard

MobilityAPI implements [OGC API – Moving Features – Part 1: Core](https://docs.ogc.org/is/22-003r3/22-003r3.html)
over MobilityDB. This note compares its surface to the standard and to the
reference implementation [`aistairc/mf-api`](https://github.com/aistairc/mf-api),
a Python server built on pygeoapi and MobilityDB.

## Stacks

| | MobilityAPI | aistairc/mf-api |
|---|---|---|
| Language | Go (`net/http`, `pgxpool`) | Python (Flask via pygeoapi) |
| Temporal engine | MobilityDB (all temporal work in SQL; the tier holds no MEOS) | MobilityDB + PyMEOS / SQLAlchemy / GeoAlchemy2 |
| Geometry | PostGIS | PostGIS |
| Response model | streamed, keyset-paged FeatureCollection | in-memory FeatureCollection |

## Endpoints

Each row is one OGC API – Moving Features resource. The standard and the mf-api
reference share the same route set, since mf-api is the reference implementation
of Part 1: Core.

| Method · Path | OGC Part 1 | mf-api | MobilityAPI |
|---|:--:|:--:|:--:|
| `GET /` | ● | ● | ● |
| `GET /api` | ● | ● | ● |
| `GET /conformance` | ● | ● | ● |
| `GET /collections` | ● | ● | ● |
| `POST /collections` | ● | ● | ● |
| `GET /collections/{c}` | ● | ● | ● |
| `PUT /collections/{c}` | ● | ● | ● |
| `DELETE /collections/{c}` | ● | ● | ● |
| `GET /collections/{c}/items` | ● | ● | ● |
| `POST /collections/{c}/items` | ● | ● | ● |
| `GET /collections/{c}/items/{f}` | ● | ● | ● |
| `DELETE /collections/{c}/items/{f}` | ● | ● | ● |
| `GET …/{f}/tgsequence` | ● | ● | ● |
| `POST …/{f}/tgsequence` | ● | ● | ● |
| `DELETE …/tgsequence/{tg}` | ● | ● | ● |
| `GET …/tgsequence/{tg}/distance` | ● | ● | ● |
| `GET …/tgsequence/{tg}/velocity` | ● | ● | ● |
| `GET …/tgsequence/{tg}/acceleration` | ● | ● | 501 ² |
| `GET …/{f}/tproperties` | ● | ● | ● |
| `POST …/{f}/tproperties` | ● | ● | ● |
| `GET …/tproperties/{name}` | ● | ● | ● |
| `POST …/tproperties/{name}` | ● | ● | ● |
| `DELETE …/tproperties/{name}` | ● | ● | ● |

Beyond the standard, MobilityAPI adds `GET /health`, `GET …/{f}` exposing the
single-feature GeoJSON geometry, `PUT …/items/{f}`, and a lakehouse bulk feed
`GET …/{c}/export` (NDJSON, or `?format=parquet`). These sit outside `conformsTo`.

The `{tGeometryId}` is the **1-based index of a member sequence** within the
feature's `tgeompoint` sequence set (`sequenceN` / `numSequences`): the temporal
geometry sequence is enumerated as gap-separated temporal primitive geometries,
each with its own id, so `GET`, the derived queries and `DELETE` all address a
specific member. `DELETE …/tgsequence/{tg}` removes that member (409 when it is
the feature's only one).

² Acceleration returns 501. With linearly interpolated position the speed is
piecewise-constant, so its derivative is zero within each segment and undefined
at the vertices; the value is not approximated.

## Temporal properties

The standard distinguishes two families that the route tree keeps separate:

- **TemporalGeometryQuery** — `…/tgsequence/{tg}/distance|velocity|acceleration`
  — kinematics **derived** from the geometry. MobilityAPI serves distance and
  velocity from the exact MobilityDB functions `cumulativeLength` and `speed`,
  reporting each segment's own interpolation; acceleration is 501 (note ²).
- **TemporalProperty** — `…/tproperties` — **user-supplied**, time-varying
  non-spatial attributes (fuel, temperature, load). MobilityAPI stores each as a
  native MobilityDB temporal value (`tfloat`, `tint`, `ttext`, `tbool`) keyed by
  collection, feature and name; `POST` parses the value with the type-specific
  `*FromMFJSON`, appending merges new values and rejects a temporal overlap with
  409, and `GET` serves an OGC `temporalProperty` via `asMFJSON`, honouring the
  `datetime` and `leaf` selectors.

## Conformance

```
http://www.opengis.net/spec/ogcapi-movingfeatures-1/1.0/conf/common
http://www.opengis.net/spec/ogcapi-movingfeatures-1/1.0/conf/mf-collection
http://www.opengis.net/spec/ogcapi-movingfeatures-1/1.0/conf/movingfeatures
http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/core
http://www.opengis.net/spec/ogcapi-common-2/1.0/conf/collections
```
