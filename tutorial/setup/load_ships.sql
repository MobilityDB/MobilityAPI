-- ============================================================================
-- MobilityAPI tutorial — load the `ships` collection
--
-- Builds the database the Go tier (and the tutorial notebook) expect: the
-- `collections` registry the tier reads, plus a `ships` table of one MobilityDB
-- tgeompoint trajectory per vessel, assembled from a day of Danish AIS point
-- reports.
--
-- Each vessel becomes one moving feature whose trajectory is a tgeompoint
-- split at temporal gaps with tgeompointSeqSetGaps (a gap longer than `gap`
-- starts a new sequence within the trajectory). Coordinates are stored in
-- EPSG:25832 (ETRS89 / UTM 32N, metres), the collection's CRS.
--
-- Run against a database with the MobilityDB extension, passing the path to the
-- AIS CSV (the `aisdk-<date>.csv` open dataset from the Danish Maritime
-- Authority, https://web.ais.dk/aisdata/):
--
--   createdb mfapi_demo
--   psql -d mfapi_demo -v data_csv_path=/path/to/aisdk-2026-02-26.csv \
--        -f tutorial/setup/load_ships.sql
--
-- The CSV is read with server-side COPY, so the path must be reachable by the
-- PostgreSQL server and the role must be a superuser or hold pg_read_server_files
-- (the canonical MobilityDB AIS-loading idiom). `gap` is the run-splitting
-- threshold; tune it to taste.
-- ============================================================================

SET DATESTYLE = 'ISO, DMY';                 -- AIS timestamps are DD/MM/YYYY
\set gap '30 minutes'
CREATE EXTENSION IF NOT EXISTS MobilityDB CASCADE;

-- --- Collection registry the Go tier reads (SELECT id, crs FROM collections) -
CREATE TABLE IF NOT EXISTS collections (
  id          text PRIMARY KEY,
  title       text,
  description  text,
  item_type   text,
  crs         integer
);
INSERT INTO collections (id, title, description, item_type, crs)
VALUES ('ships', 'ships',
        'Danish AIS vessel trajectories (MobilityDB tgeompoint)',
        'movingfeature', 25832)
ON CONFLICT (id) DO UPDATE
  SET title = EXCLUDED.title, description = EXCLUDED.description,
      item_type = EXCLUDED.item_type, crs = EXCLUDED.crs;

-- --- 1. Stage the raw AIS point reports ------------------------------------
DROP TABLE IF EXISTS AISInput;
CREATE TABLE AISInput (
  T timestamp, TypeOfMobile text, MMSI bigint, Latitude float, Longitude float,
  NavigationalStatus text, ROT float, SOG float, COG float, Heading integer,
  IMO text, Callsign text, Name text, ShipType text, CargoType text,
  Width float, Length float, TypeOfPositionFixingDevice text, Draught float,
  Destination text, ETA text, DataSourceType text,
  SizeA float, SizeB float, SizeC float, SizeD float
);
COPY AISInput FROM :'data_csv_path' WITH (FORMAT csv, HEADER true);

-- --- 2. Clean placeholders and project to the collection CRS ---------------
ALTER TABLE AISInput ADD COLUMN Geom geometry(Point, 25832);
UPDATE AISInput
SET Name = NULLIF(Name, 'Unknown'),
    Geom = ST_Transform(ST_SetSRID(ST_MakePoint(Longitude, Latitude), 4326), 25832)
WHERE Latitude  BETWEEN 40.18 AND 84.73
  AND Longitude BETWEEN -16.1 AND 32.88
  AND SOG IS NOT NULL AND COG IS NOT NULL;

-- --- 3. One report per (vessel, instant) -----------------------------------
DROP TABLE IF EXISTS AISClean;
CREATE TABLE AISClean AS
SELECT DISTINCT ON (MMSI, T) MMSI, T, Name, Geom
FROM AISInput
WHERE Geom IS NOT NULL
ORDER BY MMSI, T;

-- --- 4. Assemble one gap-split trajectory per vessel ----------------------
DROP TABLE IF EXISTS ships;
CREATE TABLE ships AS
SELECT row_number() OVER (ORDER BY mmsi)::int AS id, mmsi, name, trip
FROM (
  SELECT MMSI AS mmsi,
         MIN(Name) FILTER (WHERE Name IS NOT NULL) AS name,
         tgeompointSeqSetGaps(array_agg(tgeompoint(Geom, T) ORDER BY T),
                              maxt := (:'gap')::interval) AS trip
  FROM AISClean
  GROUP BY MMSI
  HAVING count(*) > 1) tracks
WHERE numInstants(trip) > 1;                 -- a trajectory needs ≥ 2 instants

-- --- 5. Drop implausible trajectories (zero-length or > 1500 km) ------------
DELETE FROM ships WHERE length(trip) = 0 OR length(trip) >= 1500000;

ALTER TABLE ships ADD PRIMARY KEY (id);
CREATE INDEX ships_trip_gist ON ships USING gist (trip);   -- bbox/datetime filters

DROP TABLE AISInput, AISClean;
ANALYZE ships;

SELECT count(*) AS trajectories, count(DISTINCT mmsi) AS vessels FROM ships;
