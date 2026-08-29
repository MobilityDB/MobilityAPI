-- The conformance fixture: the smallest database the OGC API - Moving Features
-- Part 1 abstract tests can run against.
--
-- WHY IT IS NOT load_ships.sql. That loader builds the tutorial's collection
-- from an aisdk-<date>.csv of the Danish Maritime Authority, gigabytes fetched
-- over the network and reshaped into whatever vessels that day happened to
-- carry. Conformance needs the opposite: a fixed, tiny, offline corpus whose
-- every value is written here, so a run answers the same thing on any machine
-- and a failure names a defect rather than a dataset. Annex A tests structure
-- and lifecycle, never volume.
--
-- WHAT IT HOLDS, and why each piece exists:
--   * two moving features, so a Collections listing has more than one member
--     and a delete can remove one while leaving the collection non-empty;
--   * a trajectory of several instants per feature, so numSequences,
--     sequenceN, cumulativeLength and speed all have something to answer and
--     the derived-query tests are not degenerate;
--   * one feature carrying a temporal property of each scalar type the
--     standard predefines a token for and MobilityDB stores, so the
--     TemporalProperties document exercises the type check rather than one
--     lucky case;
--   * a second collection, so a test that deletes one cannot silently pass by
--     operating on the only one present.
--
-- SRID 25832 (ETRS89 / UTM 32N) matches the tutorial collection, so the same
-- tier configuration serves both and the crs assertions are meaningful in a
-- projected system rather than degenerate in degrees.
--
--   psql -d <db> -f tutorial/setup/load_conformance.sql

CREATE EXTENSION IF NOT EXISTS MobilityDB CASCADE;

-- --- The collection registry the tier reads (SELECT id, crs FROM collections)
CREATE TABLE IF NOT EXISTS collections (
  id          text PRIMARY KEY,
  title       text,
  description text,
  item_type   text,
  crs         integer
);

INSERT INTO collections (id, title, description, item_type, crs) VALUES
  ('conformance', 'conformance',
   'A fixed two-feature corpus for the OGC API - Moving Features Part 1 abstract tests',
   'movingfeature', 25832),
  ('conformance_alt', 'conformance_alt',
   'A second collection, so a delete cannot pass by acting on the only one',
   'movingfeature', 25832)
ON CONFLICT (id) DO UPDATE
  SET title = EXCLUDED.title, description = EXCLUDED.description,
      item_type = EXCLUDED.item_type, crs = EXCLUDED.crs;

-- --- The moving features ----------------------------------------------------
-- The tier reads a feature table named for its collection, carrying id and a
-- tgeompoint named trip. Coordinates are metres in UTM 32N around Aarhus, and
-- every timestamp is a literal, so no assertion depends on the clock.
DROP TABLE IF EXISTS conformance;
CREATE TABLE conformance (
  id    integer PRIMARY KEY,
  mmsi  bigint,
  name  text,
  trip  tgeompoint
);

INSERT INTO conformance (id, mmsi, name, trip) VALUES
  (1, 219000001, 'Alpha', tgeompoint
     '[SRID=25832;POINT(575000 6220000)@2026-01-01 08:00:00+00,
       SRID=25832;POINT(576000 6220500)@2026-01-01 08:10:00+00,
       SRID=25832;POINT(577500 6221000)@2026-01-01 08:25:00+00,
       SRID=25832;POINT(579000 6222000)@2026-01-01 08:45:00+00]'),
  (2, 219000002, 'Bravo', tgeompoint
     '[SRID=25832;POINT(574000 6219000)@2026-01-01 09:00:00+00,
       SRID=25832;POINT(574800 6219400)@2026-01-01 09:12:00+00,
       SRID=25832;POINT(575600 6220100)@2026-01-01 09:30:00+00]');

CREATE INDEX IF NOT EXISTS conformance_trip_gist ON conformance USING gist (trip);

DROP TABLE IF EXISTS conformance_alt;
CREATE TABLE conformance_alt (LIKE conformance INCLUDING ALL);
INSERT INTO conformance_alt (id, mmsi, name, trip) VALUES
  (1, 219000003, 'Charlie', tgeompoint
     '[SRID=25832;POINT(570000 6215000)@2026-01-01 10:00:00+00,
       SRID=25832;POINT(571000 6215500)@2026-01-01 10:20:00+00]');

-- --- Temporal properties ----------------------------------------------------
-- One of each scalar type the standard predefines, so the TemporalProperties
-- document exercises TReal, TInteger, TText and TBoolean rather than one of
-- them. TImage has no MobilityDB storage and is absent by construction.
CREATE TABLE IF NOT EXISTS mf_tproperty (
  cid text NOT NULL, fid bigint NOT NULL, name text NOT NULL,
  ptype text NOT NULL, uom text, description text,
  vfloat tfloat, vint tint, vtext ttext, vbool tbool,
  PRIMARY KEY (cid, fid, name)
);

DELETE FROM mf_tproperty WHERE cid IN ('conformance', 'conformance_alt');

INSERT INTO mf_tproperty (cid, fid, name, ptype, uom, description, vfloat) VALUES
  ('conformance', 1, 'speed', 'TReal', 'km/h', 'Speed over ground', tfloat
     '[12.5@2026-01-01 08:00:00+00, 14.0@2026-01-01 08:10:00+00,
       11.25@2026-01-01 08:25:00+00, 13.75@2026-01-01 08:45:00+00]');

INSERT INTO mf_tproperty (cid, fid, name, ptype, uom, description, vint) VALUES
  ('conformance', 1, 'heading', 'TInteger', 'deg', 'Course over ground', tint
     '[45@2026-01-01 08:00:00+00, 50@2026-01-01 08:10:00+00,
       40@2026-01-01 08:25:00+00]');

INSERT INTO mf_tproperty (cid, fid, name, ptype, uom, description, vtext) VALUES
  ('conformance', 1, 'status', 'TText', '', 'Navigational status', ttext
     '[under way@2026-01-01 08:00:00+00, moored@2026-01-01 08:45:00+00]');

INSERT INTO mf_tproperty (cid, fid, name, ptype, uom, description, vbool) VALUES
  ('conformance', 1, 'anchored', 'TBoolean', '', 'Whether the vessel is at anchor', tbool
     '[false@2026-01-01 08:00:00+00, true@2026-01-01 08:45:00+00]');

-- A property on the second feature, so a delete on feature 1 cannot empty the
-- table and pass a later listing by accident.
INSERT INTO mf_tproperty (cid, fid, name, ptype, uom, description, vfloat) VALUES
  ('conformance', 2, 'speed', 'TReal', 'km/h', 'Speed over ground', tfloat
     '[9.0@2026-01-01 09:00:00+00, 10.5@2026-01-01 09:30:00+00]');
