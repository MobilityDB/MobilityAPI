# Generation

MobilityAPI is a tier over MEOS, so what it knows about MEOS is generated from
the MEOS-API catalog rather than kept by hand. The generator lives in
`tools/codegen`, its output is committed, and CI derives the catalog from
MobilityDB master, regenerates against it, and refuses a difference.

## What is generated

`catalog_gen.go` is the table of MEOS temporal types: for each one, its name,
its base type, its bounding box, the token `asMFJSON` writes for it, whether it
is spatial, whether it is a number, and whether MEOS interpolates it linearly.

Run it against a catalog:

```bash
go run ./tools/codegen -catalog <meos-idl.json> -o catalog_gen.go
```

## Where the facts come from

`meos-idl.json` is the machine-readable description of the MEOS C library that
[MEOS-API](https://github.com/MobilityDB/MEOS-API) derives from the MEOS
headers and sources, and that every binding in the ecosystem generates from.
This tier reads one registry of it, `temporalTypes`, which MEOS-API in turn
reads from:

| source | what is read |
| --- | --- |
| `meos/src/temporal/meos_catalog.c` | the `[T_X] = "x"` type names; each temporal type's `temptype_basetype` and `type_bboxtype`; and the predicates `temporal_type`, `tnumber_type`, `tspatial_type` and `temptype_supports_linear` |
| `meos/src/temporal/type_out.c` | `temptype_as_mfjson_sb`, the switch that writes each type's MF-JSON type token |

A type MEOS carries that the MF-JSON switch does not name has no MF-JSON form,
and the empty token in the table says so. That is how `tdouble2`, `tdouble3`
and `tdouble4`, which exist for temporal aggregation, stay off every surface
without a list here naming them.

The catalog is a derived artifact of one MobilityDB commit, so it is never
committed. CI derives it with
`MobilityDB/MEOS-API/.github/actions/provision-meos@master`, the action every
catalog-consuming binding uses, and passes the path it reports to the
generator.

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

## Why the catalog and not the C

Reading MEOS's C directly would work, and it is what this generator did before
the catalog stated the MF-JSON type token and named every temporal type a base
carries. It is the wrong place to read from: it gives this tier a parse of
someone else's C to keep working, and it puts the tier on a different footing
from PyMEOS, JMEOS and the rest, which all project the catalog. One source of
truth, read one way, is the whole point of the catalog existing.
