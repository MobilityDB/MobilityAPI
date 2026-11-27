package main

// Mirrors #TestLiveBboxOverGeographies of geographic_bbox_live_test.go for a
// collection of temporal H3 and S2 cells, which MobilityDB relates to a
// geography alone (eIntersects(th3index, geography)), so the region of an EDR
// query is read as a geography on the spheroid (#geographic). Each cell is the
// one holding a point at a resolution, computed by the database, so the test
// states no cell index of its own.

import (
	"net/url"
	"reflect"
	"testing"
)

func TestLiveEDRQueriesOverCells(t *testing.T) {
	mux, done := atsLiveMux(t)
	defer done()
	for _, c := range []struct{ typ, mfjson, cell string }{
		// The H3 cell of resolution 5 holding the point, as latLngToCell gives
		// it, and the S2 cell of level 10, as the conversion of a temporal point
		// ts2cell(tgeompoint, 10) gives it (605_ts2cell_spatialfuncs)
		{"th3index", "MovingH3Index", "CAST(latLngToCell(ST_SetSRID(ST_Point($1, $2), 4326), 5) AS text)"},
		{"ts2cell", "MovingS2Cell", "CAST(startValue(ts2cell(tgeompoint(ST_SetSRID(ST_Point($1, $2), 4326), " +
			"timestamptz '2001-01-01'), 10)) AS text)"},
	} {
		cell := func(lon, lat float64) string {
			t.Helper()
			var s string
			if err := db.QueryRow(t.Context(), "SELECT "+c.cell, lon, lat).Scan(&s); err != nil {
				t.Fatalf("%s: the cell of (%v, %v): %v", c.typ, lon, lat, err)
			}
			return s
		}
		cid := "edr_cells_" + c.typ
		atsDo(t, mux, "DELETE", "/collections/"+cid, "")
		defer atsDo(t, mux, "DELETE", "/collections/"+cid, "")
		if rec := atsDo(t, mux, "POST", "/collections",
			`{"id":"`+cid+`","temporalGeometryType":"`+c.typ+`","crs":4326}`); rec.Code != 201 {
			t.Fatalf("POST /collections of %s = %d (%s)", c.typ, rec.Code, rec.Body.String())
		}
		var ids []string
		for i, at := range [][2]float64{{4.35, 50.85}, {-70.65, -33.45}} {
			v := cell(at[0], at[1])
			rec := atsDo(t, mux, "POST", "/collections/"+cid+"/items",
				`{"id":`+itoa(i+1)+`,"properties":{},"temporalGeometry":{"type":"`+c.mfjson+`",`+
					`"values":["`+v+`","`+v+`"],"datetimes":["2001-01-01T00:00:00+00:00",`+
					`"2001-01-02T00:00:00+00:00"],"lower_inc":true,"upper_inc":true,"interpolation":"Step"}}`)
			if !oneOf(rec.Code, 201, 202) {
				t.Fatalf("POST a %s feature = %d (%s)", c.typ, rec.Code, rec.Body.String())
			}
			ids = append(ids, atsCreatedID(t, rec))
		}
		brussels := ids[0]
		for _, q := range []struct {
			name, path string
			want       []string
		}{
			{"an area about Brussels", "area?coords=" +
				url.QueryEscape("POLYGON((4.3 50.8,4.4 50.8,4.4 50.9,4.3 50.9,4.3 50.8))"), []string{brussels}},
			{"an area far from both", "area?coords=" +
				url.QueryEscape("POLYGON((10 10,11 10,11 11,10 11,10 10))"), []string{}},
			{"a path across Brussels", "trajectory?coords=" +
				url.QueryEscape("LINESTRING(4.0 50.85,4.7 50.85)"), []string{brussels}},
			{"an area about Brussels before the cells", "area?coords=" +
				url.QueryEscape("POLYGON((4.3 50.8,4.4 50.8,4.4 50.9,4.3 50.9,4.3 50.8))") +
				"&datetime=../2000-12-31T00:00:00Z", []string{}},
		} {
			rec := atsDo(t, mux, "GET", "/collections/"+cid+"/"+q.path, "")
			if got := geographicIDs(t, rec.Code, rec.Body.String()); !reflect.DeepEqual(got, q.want) {
				t.Errorf("%s, %s: %v, want %v", c.typ, q.name, got, q.want)
			}
		}
		// MobilityDB states no eOverlaps over a cell, so a box is refused, as
		// over any geography it cannot test a box against
		for _, path := range []string{"/cube?z=0&bbox=4,50,5,51", "/items?bbox=4,50,5,51"} {
			if rec := atsDo(t, mux, "GET", "/collections/"+cid+path, ""); rec.Code != 400 {
				t.Errorf("%s: %s answered %d, want 400: %s", c.typ, path, rec.Code, rec.Body.String())
			}
		}
	}
}
