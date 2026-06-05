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
SET TIME ZONE 'UTC';                         -- AIS times are UTC; store them as UTC
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
SELECT DISTINCT ON (MMSI, T) MMSI, T, Name, Geom, SOG, ShipType
FROM AISInput
WHERE Geom IS NOT NULL
ORDER BY MMSI, T;

-- --- 4. Assemble one gap-split trajectory per vessel ----------------------
-- Ids run from the most-travelled vessel down, so the lowest ids (the default
-- selection of both tutorials) are vessels that actually move.
DROP TABLE IF EXISTS ships;
CREATE TABLE ships AS
SELECT row_number() OVER (ORDER BY length(trip) DESC, mmsi)::int AS id, mmsi, name, ship_type, trip
FROM (
  SELECT MMSI AS mmsi,
         MIN(Name) FILTER (WHERE Name IS NOT NULL) AS name,
         COALESCE(MIN(ShipType) FILTER (WHERE ShipType IS NOT NULL AND ShipType <> 'Undefined'), 'Other') AS ship_type,
         tgeompointSeqSetGaps(array_agg(tgeompoint(Geom, T) ORDER BY T),
                              maxt := (:'gap')::interval) AS trip
  FROM AISClean
  GROUP BY MMSI
  HAVING count(*) > 1) tracks
WHERE numInstants(trip) > 1                   -- a trajectory needs ≥ 2 instants
  AND length(trip) > 0                        -- drop stationary noise
  AND length(trip) < 1500000;                 -- drop implausible (> 1500 km) tracks

ALTER TABLE ships ADD PRIMARY KEY (id);
CREATE INDEX ships_trip_gist ON ships USING gist (trip);   -- bbox/datetime filters

-- --- 5. Speed-over-ground (AIS SOG) as a temporal property per vessel --------
-- The tier serves this at /…/tproperties/speed; the streaming tutorial charts
-- its windowed average. Built while the AIS staging tables are still present.
CREATE TABLE IF NOT EXISTS mf_tproperty (
  cid text NOT NULL, fid bigint NOT NULL, name text NOT NULL,
  ptype text NOT NULL, uom text, description text,
  vfloat tfloat, vint tint, vtext ttext, vbool tbool,
  PRIMARY KEY (cid, fid, name)
);
-- Cleared first so reloading rebuilds against the current ids (the ships table
-- is reassigned ids on every load).
DELETE FROM mf_tproperty WHERE cid = 'ships' AND name = 'speed';
INSERT INTO mf_tproperty (cid, fid, name, ptype, uom, description, vfloat)
SELECT 'ships', s.id, 'speed', 'TReal', 'kn', 'Speed over ground (AIS SOG)',
       tfloatSeqSetGaps(array_agg(tfloat(c.SOG, c.T) ORDER BY c.T),
                        maxt := (:'gap')::interval)
FROM AISClean c JOIN ships s ON s.mmsi = c.MMSI
WHERE c.SOG IS NOT NULL AND c.SOG < 102.3   -- 102.3 = SOG "not available"
GROUP BY s.id
HAVING count(*) > 1;

DROP TABLE AISInput, AISClean;
ANALYZE ships;

-- --- 6. Demo temporal properties for the first feature ----------------------
-- User-supplied, time-varying attributes the tier serves at /…/tproperties,
-- stored as native MobilityDB temporal values in the same mf_tproperty table
-- the Go tier writes. Each is a linear tfloat over the feature's own window, so
-- the values track the most-travelled vessel (feature 1).
INSERT INTO mf_tproperty (cid, fid, name, ptype, uom, description, vfloat)
SELECT 'ships', 1, p.name, 'TReal', p.uom, p.description,
       ('[' || p.v0 || '@' || startTimestamp(s.trip) || ', '
            || p.v1 || '@' || endTimestamp(s.trip)   || ']')::tfloat
FROM ships s,
     (VALUES ('fuel',       'L',   'Fuel remaining over the voyage', 22.78, 41.5),
             ('cargo_temp', 'Cel', 'Reefer cargo temperature',        4.2,   4.0),
             ('load',       't',   'Cargo load',                    1200.0, 1180.0))
       AS p(name, uom, description, v0, v1)
WHERE s.id = 1
ON CONFLICT (cid, fid, name) DO UPDATE
  SET vfloat = EXCLUDED.vfloat, uom = EXCLUDED.uom,
      description = EXCLUDED.description;

SELECT count(*) AS trajectories, count(DISTINCT mmsi) AS vessels FROM ships;
