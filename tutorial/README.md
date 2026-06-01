# MobilityAPI (Go tier) — tutorial

`tutorial.ipynb` walks through the OGC API – Moving Features endpoints served by the
Go MobilityAPI tier over MobilityDB, using a day of Danish AIS data. It is plain HTTP
(`requests`) against the server — the tier holds no MEOS, so nothing else runs in the
notebook.

## Prerequisites

- The Go tier running on `http://localhost:8088` with the `ships` collection loaded
  (the `mfapi_demo` database). Start it with `./mfapi-go` (see the repo README).
- Python packages: `requests matplotlib numpy pyproj Pillow`.

Build the `mfapi_demo` database from a day of [Danish Maritime Authority AIS
data](https://web.ais.dk/aisdata/) with `setup/load_ships.sql`:

```
createdb mfapi_demo
psql -d mfapi_demo -v data_csv_path=/path/to/aisdk-2026-02-26.csv \
     -f tutorial/setup/load_ships.sql
```

The basemap is fetched as OpenStreetMap XYZ tiles (cached under `/tmp/mfapi_tiles`), so
geopandas/contextily are not required.

## Run

```
pip install requests matplotlib numpy pyproj Pillow jupyter
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
