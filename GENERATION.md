# Generation

MobilityAPI is a tier over MEOS, so what it knows about MEOS is generated from
MEOS rather than kept by hand. The generator lives in `tools/codegen`, its
output is committed, and CI regenerates it against MobilityDB master and
refuses a difference.

## What is generated

`catalog_gen.go` is the table of MEOS temporal types: for each one, its name,
its base type, its bounding box, the token `asMFJSON` writes for it, whether it
is spatial, whether it is a number, and whether MEOS interpolates it linearly.

Run it against any MobilityDB checkout:

```bash
go run ./tools/codegen -mobilitydb <checkout> -o catalog_gen.go
```

## Where the facts come from

The generator reads the MEOS sources that define them, in the checkout it is
pointed at:

| source | what is read |
| --- | --- |
| `meos/src/temporal/meos_catalog.c` | the `[T_X] = "x"` type names; each temporal type's `temptype_basetype` and `type_bboxtype`; and the predicates `temporal_type`, `tnumber_type`, `tspatial_type` and `temptype_supports_linear` |
| `meos/src/temporal/type_out.c` | `temptype_as_mfjson_sb`, the switch that writes each type's MF-JSON type token |

A type MEOS carries that the MF-JSON switch does not name has no MF-JSON form,
and the empty token in the table says so. That is how `tdouble2`, `tdouble3`
and `tdouble4`, which exist for temporal aggregation, stay off every surface
without a list here naming them.

The sources are read by anchoring on a definition and counting braces, never by
matching a pattern across a span, and string literals are stepped over so the
`{` inside `{\"type\":\"MovingFloat\",` is read as text rather than as
structure.

## What the tier adds, and what holds it complete

Two things are the standard's rather than MEOS's, so they are stated in
`main.go` next to the code that uses them:

- `ogcScalar` maps a base type to the OGC temporal-property token Part 1
  defines for it, and to the interpolation this tier assumes when a request
  body names none. Part 1 defines four tokens and no more, so this is keyed by
  base type: every temporal type MEOS builds over one of those bases is carried
  automatically, and one over a fifth base is a question for the standard.
- `tPropAlias` holds the further spellings a client may send for a type.

`catalog_test.go` is what keeps the two halves in step. It asserts that every
non-spatial type MEOS writes MF-JSON for, over a base the standard names,
resolves through `tPropType` with the MEOS type and the MF-JSON token the
generated table carries; that no spatial type resolves, since a spatial value
is a moving feature's geometry rather than a temporal property; and that the
store's value columns are one per scalar type. A temporal type MEOS adds over
one of the standard's bases therefore arrives as a failing test rather than as
a request the tier rejects at run time.

## The catalog this could read instead

`MobilityDB/MEOS-API` publishes `meos-idl.json`, the machine-readable catalog
every language binding generates from, and reading it here rather than the C
sources would put this tier on the same footing as the bindings. Two of the
facts above are not in that catalog today: the MF-JSON type token has no field,
and `typeRelations.byBase` holds one temporal type per base, so it carries
`trgeometry` for the base `pose` and drops `tpose`, which shares that base.
Both are single-source facts in `meos_catalog.c` and `type_out.c`, which is
what the generator reads until the catalog states them.
