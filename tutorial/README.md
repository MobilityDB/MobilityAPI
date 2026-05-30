# MobilityAPI (Go tier) — tutorial

`tutorial.ipynb` walks through the OGC API – Moving Features endpoints served by the
Go MobilityAPI tier over MobilityDB, using a day of Danish AIS data. It is plain HTTP
(`requests`) against the server — the tier holds no MEOS, so nothing else runs in the
notebook.

## Prerequisites

- The Go tier reachable on `http://localhost:8088` with the `ships` collection loaded
  (the `mfapi_demo` database). Build and run it per the repository README
  (`go build -o mfapi . && MFAPI_DSN=<dsn> ./mfapi`). Point the notebook elsewhere with
  the `MFAPI_HOST` environment variable.
- A Jupyter kernel. The notebook's first cell installs its Python packages
  (`requests numpy matplotlib pyproj pillow`), so a bare kernel works.

The notebook is a plain-HTTP client — its second cell runs a preflight that checks the
tier is up and the `ships` collection is present, printing what to start if not, rather
than failing deep in a later cell. The basemap is fetched as OpenStreetMap XYZ tiles
(cached under `/tmp/mfapi_tiles`), so geopandas/contextily are not required.

## Run

```
jupyter notebook tutorial/tutorial.ipynb
```

## What it covers

Collections (with `crs`/`extent`/`links`), streamed and keyset-paged items, a single
feature with the full standard fields (`crs`/`trs`/`bbox`/`time`/`geometry`), the `bbox`
and `subtrajectory`/`datetime` filters, the temporal-geometry sequence, the derived
measures (`velocity`/`distance`, and `acceleration` returning `501` because it is not
derivable for piecewise-linear motion), the temporal properties with the `leaf` selector,
the write lifecycle (`POST`/`PUT`/`DELETE`, append via `merge`), and the NDJSON/Parquet
lakehouse export.
