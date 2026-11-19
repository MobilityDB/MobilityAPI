# The normative OGC API — Moving Features Part 1 schemas

`ogcapi-movingfeatures-1.bundled.json` is the OpenAPI 3.0.3 document OGC publishes for
*OGC API — Moving Features — Part 1: Core* (OGC 22-003r3), vendored byte for byte:

    URL     https://schemas.opengis.net/ogcapi/movingfeatures/part1/1.0/openapi/ogcapi-movingfeatures-1.bundled.json
    bytes   108910
    sha256  7e8e0a0e68c936dd59ca40ed50b63eb3121ef96082307438e54bb01362276866

It is the bundled form, so every `$ref` resolves inside the one file and validation needs no
network. `components.schemas` holds the 36 schemas the standard defines, and
`ats_schema_test.go` validates the documents this tier emits against them.

The copy is never edited. To confirm it still matches what OGC publishes:

    MFAPI_SCHEMA_FRESHNESS=1 go test -run TestATSSchemaBundleMatchesOGC -v .

That test fetches the URL above and compares; it skips without the variable so the suite
stays offline.

## Why the bundle rather than the individual schema files

The same directory publishes `schemas/*.yaml` one file per schema, each `$ref`-ing its
siblings by relative path. Reading those needs a resolver that walks the directory and a
YAML parser; the bundle carries identical content with the references already resolved, and
is what a validator can load directly.

## The one translation the validator applies

OpenAPI 3.0 spells nullability as `nullable: true` beside a `type`, where a JSON Schema
draft spells the same thing as a type union. A JSON Schema validator does not know that keyword
and would silently ignore it, rejecting a null the standard admits. The validator therefore
rewrites `{"type": "T", "nullable": true}` to `{"type": ["T", "null"]}` before compiling,
and asserts it made exactly the 16 rewrites this document calls for — a translation that
silently stopped applying would make the run permissive without saying so.

Nothing else is rewritten. Measured on this document: 0 schemas put a sibling keyword beside
a `$ref` (where OpenAPI ignores the sibling and JSON Schema 2020-12 applies it), and neither
`exclusiveMinimum` nor `exclusiveMaximum` appears in the boolean form OpenAPI uses, so the
two dialects agree on every other keyword present.

`format` is asserted rather than treated as an annotation. JSON Schema leaves that to the
validator, and the assertion is what the standard's own `motionCurve` needs to admit the five
interpolation values it names: at the default reading its two `oneOf` branches both match a
plain string, so every named value matches both and is rejected while an arbitrary string
matches one and passes. `TestATSSchemaMotionCurveIsInverted` measures both readings and the
correction proposed for them.

## What this validation does not cover

A temporal GEOMETRY's `datetimes` are `{"type": "string"}` with no format, where a temporal
PROPERTY's are `{"type": "string", "format": "date-time"}`. Validating a
TemporalGeometrySequence therefore says nothing about the shape of the instants inside it —
any string passes. The standard is not silent on the point in its prose: it states that the
syntax of a date-time is RFC 3339 section 5.6 and that a server SHALL interpret it so, which
makes the geometry schema weaker than the standard it belongs to.
`TestATSSchemaDatetimeFormatIsAsymmetric` pins the asymmetry, so the gap is stated rather than
mistaken for coverage.

## The standard's own examples, measured against the standard's own schemas

Every example this document publishes sits in a media-type object beside the schema it
illustrates, so the pairing is the document's own. Of the 16 examples paired with a named
schema:

| reading of `format` | examples failing their own schema |
|---|---|
| asserted | **10 of 16** |
| an annotation — JSON Schema's default | **13 of 16** |

Both are asserted by `TestATSSchemaPublishedExamples`, because the difference between them is
the inverted `oneOf` above acting on the standard's own material: at the default reading the
URI branch of `motionCurve` reduces to "any string", both branches match a named value, and
`oneOf` rejects the vocabulary the standard itself defines. A seventeenth example is attached
to an inline schema and is not compared. The per-example reasons are in the test's log.

⛔ **TWO RESPONSES DECLARE A SCHEMA AND PUBLISH NO EXAMPLE** — `LandingPage` and `Conformance`.
They are not passes: a row with nothing to validate cannot be one, and counting it among the
examples that satisfy their schema makes a tally read better than the material it is taken
over. The test reports them separately for that reason.

The failures concentrate in two cells the schemas type in a way no document can meet:
`temporalPrimitiveValue.values` is a single scalar beside a `datetimes` array of `minItems: 2`,
and `temporalProperty` carries that value object. They are defects in the standard, not in the
examples — the examples are what a reader would write.

## The resolved schema

The wiki page [Implementing OGC API — Moving Features in
MobilityDB](https://github.com/MobilityDB/MobilityDB/wiki/Implementing-OGC-API-Moving-Features-in-MobilityDB)
offers the Standards Working Group candidate resolutions of the standard. Resolutions 1 to 5 edit
the normative schemas, and `ats_resolved_schema_test.go` implements them: it reads this copy,
applies each resolution in memory at the sites it names (each site asserted to hold the published
text first) and writes nothing back.

| Resolution | Edit | Sites |
|---|---|---|
| 1 | the URI branch of each `oneOf` takes the absolute-URI pattern `^[A-Za-z][A-Za-z0-9+.-]*:` | 6 |
| 2 | `temporalPrimitiveValue.values` is an array mirroring `datetimes` | 1 |
| 3 | the three-character branch of a `form` takes the UN/CEFACT common-code pattern `^[A-Z0-9]{3}$` | 2 |
| 4 | a property's value interpolation is the named schema `valueInterpolation`, referred to everywhere | 4 |
| 5 | the `datetimes` of a temporal geometry, a temporal primitive value and parametric values take `format: date-time` and the RFC 3339 pattern | 3 |

Measured against the resolved schema, under either reading of `format`:

- every document the service writes (`samples/`) is valid;
- **5 of 16** published examples fail, the five whose example is wrong against a right schema
  (a flat `extent.spatial.bbox`; a `trs` whose `properties` is a string), against 10 or 13 of 16
  published;
- no document that satisfies the published schema fails the resolved one.

The resolutions do narrow what the schema admits, and the test states each narrowing as a case: a
scalar `values` (2), a three-character unit that is no common code such as `deg` (3), and an
instant that is no RFC 3339 date-time (5).
