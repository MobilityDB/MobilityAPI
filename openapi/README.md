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
