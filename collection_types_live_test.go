package main

// Mirrors #TestATSLiveFeatureLifecycle of ats_live_test.go over every kind of
// temporal geometry a collection can hold, against the same fixture database.

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

// typedGeometries are one temporal geometry of each kind, the MF-JSON
// MobilityDB's asMFJSON writes for a literal of its regression tests.
var typedGeometries = []struct{ typ, mfjson string }{
	{"th3index", `{"type":"MovingH3Index","values":["8001fffffffffff","801ffffffffffff"],"datetimes":["2001-01-01T00:00:00+00:00","2001-01-02T00:00:00+00:00"],"lower_inc":true,"upper_inc":true,"interpolation":"Step"}`},
	{"tquadbin", `{"type":"MovingQuadbin","values":["480fffffffffffff","48427fffffffffff"],"datetimes":["2001-01-01T08:00:00+00:00","2001-01-01T08:05:00+00:00"],"lower_inc":true,"upper_inc":true,"interpolation":"Step"}`},
	{"ts2cell", `{"type":"MovingS2Cell","values":["47c3c3","54b5c9"],"datetimes":["2001-01-01T00:00:00+00:00","2001-01-02T00:00:00+00:00"],"lower_inc":true,"upper_inc":true,"interpolation":"Step"}`},
	{"tcbuffer", `{"type":"MovingCircularBuffer","values":[{"point":[1,1],"radius":0.2},{"point":[1,1],"radius":0.4}],"datetimes":["2001-01-01T00:00:00+00:00","2001-01-02T00:00:00+00:00"],"lower_inc":true,"upper_inc":true,"interpolation":"Linear"}`},
	{"trgeometry", `{"type":"MovingRigidGeometry","geometry":{"type":"Polygon","coordinates":[[[1,1],[2,2],[3,1],[1,1]]]},"values":[{"position":{"lat":1,"lon":1},"yaw":0.3},{"position":{"lat":2,"lon":2},"yaw":0.5}],"datetimes":["2001-01-01T00:00:00+00:00","2001-01-02T00:00:00+00:00"],"lower_inc":true,"upper_inc":true,"interpolation":"Linear"}`},
	{"tpose", `{"type":"MovingPose","values":[{"position":{"lat":0,"lon":0},"yaw":0},{"position":{"lat":20,"lon":10},"yaw":1}],"datetimes":["2026-01-01T00:00:00+00:00","2026-01-02T00:00:00+00:00"],"lower_inc":true,"upper_inc":true,"interpolation":"Linear"}`},
	{"tposechain", `{"type":"MovingPoseChain","values":[[{"position":{"lat":0,"lon":0},"yaw":0}],[{"position":{"lat":1,"lon":1},"yaw":0}]],"datetimes":["2001-01-01T00:00:00+00:00","2001-01-02T00:00:00+00:00"],"lower_inc":true,"upper_inc":true,"interpolation":"Linear"}`},
	{"tgeompoint", `{"type":"MovingPoint","coordinates":[[1,1],[2,2]],"datetimes":["2001-01-01T08:00:00+00:00","2001-01-01T08:05:00+00:00"],"lower_inc":true,"upper_inc":true,"interpolation":"Linear"}`},
	{"tgeogpoint", `{"type":"MovingPoint","coordinates":[[1,1],[2,2]],"datetimes":["2001-01-01T08:00:00+00:00","2001-01-01T08:05:00+00:00"],"lower_inc":true,"upper_inc":true,"interpolation":"Linear"}`},
	{"tgeometry", `{"type":"MovingGeometry","values":[{"type":"LineString","coordinates":[[0,0],[2,0]]},{"type":"LineString","coordinates":[[0,0],[3,0]]}],"datetimes":["2001-01-01T00:00:00+00:00","2001-01-02T00:00:00+00:00"],"lower_inc":true,"upper_inc":true,"interpolation":"Step"}`},
	{"tgeography", `{"type":"MovingGeometry","values":[{"type":"LineString","coordinates":[[0,0],[2,0]]},{"type":"LineString","coordinates":[[0,0],[3,0]]}],"datetimes":["2001-01-01T00:00:00+00:00","2001-01-02T00:00:00+00:00"],"lower_inc":true,"upper_inc":true,"interpolation":"Step"}`},
	{"tnpoint", `{"type":"MovingNetworkPoint","values":[{"route":1,"position":0.2},{"route":1,"position":0.4}],"datetimes":["2001-01-01T00:00:00+00:00","2001-01-02T00:00:00+00:00"],"lower_inc":true,"upper_inc":true,"interpolation":"Linear"}`},
	{"tpcpoint", `{"type":"MovingPCPoint","values":[{"pcid":41,"pt":[1,1,1]},{"pcid":41,"pt":[2,2,2]}],"datetimes":["2024-01-01T00:00:00+00:00","2024-01-02T00:00:00+00:00"],"lower_inc":true,"upper_inc":true,"interpolation":"Step"}`},
	{"tpcpatch", `{"type":"MovingPCPatch","values":[{"pcid":41,"npoints":2,"bounds":[1,2,1,2],"pts":[[1,1,1],[2,2,2]]},{"pcid":41,"npoints":1,"bounds":[3,3,3,3],"pts":[[3,3,3]]}],"datetimes":["2024-01-01T00:00:00+00:00","2024-01-02T00:00:00+00:00"],"lower_inc":true,"upper_inc":true,"interpolation":"Step"}`},
}

// pcSchema41 is the point cloud schema of MobilityDB's regression tests
// (mobilitydb/test/pointcloud/sql/load_pointcloud.sql), registered in WGS 84 under
// pcid 41, which the point cloud samples above read their dimensions from.
const pcSchema41 = `INSERT INTO pointcloud_formats (pcid, srid, schema) VALUES (41, 4326,
'<?xml version="1.0" encoding="UTF-8"?>
<pc:PointCloudSchema xmlns:pc="http://pointcloud.org/schemas/PC/1.1"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <pc:dimension><pc:position>1</pc:position><pc:size>4</pc:size><pc:name>X</pc:name>
    <pc:interpretation>int32_t</pc:interpretation><pc:scale>0.01</pc:scale></pc:dimension>
  <pc:dimension><pc:position>2</pc:position><pc:size>4</pc:size><pc:name>Y</pc:name>
    <pc:interpretation>int32_t</pc:interpretation><pc:scale>0.01</pc:scale></pc:dimension>
  <pc:dimension><pc:position>3</pc:position><pc:size>4</pc:size><pc:name>Z</pc:name>
    <pc:interpretation>int32_t</pc:interpretation><pc:scale>0.01</pc:scale></pc:dimension>
  <pc:metadata><Metadata name="srid">4326</Metadata></pc:metadata>
</pc:PointCloudSchema>') ON CONFLICT (pcid) DO NOTHING`

// typedPrerequisites registers what the samples read besides MF-JSON: the point
// cloud schema of pcid 41, and the network a network point's route 1 lies on,
// public.ways as MobilityDB's npoint tests lay it out. Each is removed again only
// where this test registered it, so a deployment's own schema or network stays.
func typedPrerequisites(t *testing.T) func() {
	t.Helper()
	ctx := context.Background()
	var undo []string
	n, err := db.Exec(ctx, pcSchema41)
	if err != nil {
		t.Fatalf("registering the point cloud schema: %v", err)
	}
	if n > 0 {
		undo = append(undo, "DELETE FROM pointcloud_formats WHERE pcid = 41")
	}
	var ways *string
	if err := db.QueryRow(ctx, "SELECT CAST(to_regclass('public.ways') AS text)").Scan(&ways); err != nil {
		t.Fatalf("looking for public.ways: %v", err)
	}
	if ways == nil {
		if _, err := db.Exec(ctx, "CREATE TABLE public.ways (gid bigint PRIMARY KEY, the_geom geometry, length double precision)"); err != nil {
			t.Fatalf("creating public.ways: %v", err)
		}
		undo = append(undo, "DROP TABLE public.ways")
		if _, err := db.Exec(ctx, "INSERT INTO public.ways VALUES (1, ST_GeomFromText('LINESTRING(0 0,1 1)', 4326), sqrt(2))"); err != nil {
			t.Fatalf("adding route 1: %v", err)
		}
	}
	return func() {
		for _, sql := range undo {
			db.Exec(ctx, sql)
		}
	}
}

// A collection of each kind takes a moving feature of that kind, answers it back,
// states its type, and answers a query MobilityDB defines over the type while
// refusing, with its reason, one it does not.
func TestLiveCollectionOfEveryGeometryType(t *testing.T) {
	mux, done := atsLiveMux(t)
	defer done()
	defer typedPrerequisites(t)()
	if len(typedGeometries) != len(geometryTypes) {
		t.Errorf("the samples cover %d types, a collection can hold %d: %s", len(typedGeometries), len(geometryTypes), geometryTypeNames())
	}
	for _, g := range typedGeometries {
		t.Run(g.typ, func(t *testing.T) {
			cid := "typed_" + g.typ
			atsDo(t, mux, "DELETE", "/collections/"+cid, "")
			defer atsDo(t, mux, "DELETE", "/collections/"+cid, "")
			rec := atsDo(t, mux, "POST", "/collections",
				`{"id":"`+cid+`","title":"`+g.typ+`","temporalGeometryType":"`+g.typ+`","crs":4326}`)
			if rec.Code != 201 {
				t.Fatalf("POST /collections of %s = %d (%s)", g.typ, rec.Code, rec.Body.String())
			}
			rec = atsDo(t, mux, "GET", "/collections/"+cid, "")
			var col map[string]any
			if err := json.Unmarshal(rec.Body.Bytes(), &col); err != nil || col["temporalGeometryType"] != g.typ {
				t.Fatalf("GET /collections/%s states temporalGeometryType %v, want %s (%s)", cid, col["temporalGeometryType"], g.typ, rec.Body.String())
			}
			items := "/collections/" + cid + "/items"
			rec = atsDo(t, mux, "POST", items, `{"properties":{"name":"one"},"temporalGeometry":`+g.mfjson+`}`)
			if !oneOf(rec.Code, 201, 202) {
				t.Fatalf("POST %s = %d (%s)", items, rec.Code, rec.Body.String())
			}
			fid := atsCreatedID(t, rec)
			if rec := atsDo(t, mux, "GET", items+"/"+fid, ""); rec.Code != 200 {
				t.Fatalf("the feature reads %d (%s)", rec.Code, rec.Body.String())
			}
			rec = atsDo(t, mux, "GET", items+"/"+fid+"/tgsequence", "")
			if rec.Code != 200 || !strings.Contains(rec.Body.String(), temporalTypeByName[g.typ].MFJSON) {
				t.Fatalf("tgsequence reads %d without its token %s (%s)", rec.Code, temporalTypeByName[g.typ].MFJSON, rec.Body.String())
			}
			rec = atsDo(t, mux, "GET", items+"/"+fid+"/tgsequence/1/velocity", "")
			if want := map[bool]int{true: 200, false: 400}[typeHas(g.typ, "speed")]; rec.Code != want {
				t.Errorf("velocity of a %s collection = %d, want %d (%s)", g.typ, rec.Code, want, rec.Body.String())
			}
		})
	}
}

// A type no collection can hold is refused when the collection is created, and
// a filter MobilityDB states no function for over the type is refused rather
// than answered by the box test alone.
func TestLiveCollectionTypeRefusals(t *testing.T) {
	mux, done := atsLiveMux(t)
	defer done()
	rec := atsDo(t, mux, "POST", "/collections", `{"id":"typed_float","temporalGeometryType":"tfloat"}`)
	if rec.Code != 400 || !strings.Contains(rec.Body.String(), "th3index") {
		t.Errorf("a tfloat collection = %d, want 400 naming the types accepted (%s)", rec.Code, rec.Body.String())
	}
	const cid = "typed_bbox"
	atsDo(t, mux, "DELETE", "/collections/"+cid, "")
	defer atsDo(t, mux, "DELETE", "/collections/"+cid, "")
	if rec := atsDo(t, mux, "POST", "/collections", `{"id":"`+cid+`","temporalGeometryType":"th3index","crs":4326}`); rec.Code != 201 {
		t.Fatalf("POST /collections = %d (%s)", rec.Code, rec.Body.String())
	}
	rec = atsDo(t, mux, "GET", "/collections/"+cid+"/items?bbox=0,0,1,1", "")
	if rec.Code != 400 || !strings.Contains(rec.Body.String(), "eIntersects") {
		t.Errorf("bbox over a th3index collection = %d, want 400 naming eIntersects (%s)", rec.Code, rec.Body.String())
	}
}
