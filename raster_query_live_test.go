package main

// Mirrors #TestLiveRasterEDRQueries of edr_live_test.go for the raster query of
// a moving feature, over the same series: two tiles over (0, 0)-(4, 2) in
// January valued 1 and 2, one over (0, 0)-(2, 2) in February valued 5.

import (
	"encoding/json"
	"strings"
	"testing"
)

// rasterQueryValues reads the values and the first instant a raster query
// answers, a value repeated by the next one counted once.
func rasterQueryValues(t *testing.T, body string) (values []float64, first string) {
	t.Helper()
	var doc struct {
		Type          string `json:"type"`
		ValueSequence []struct {
			Datetimes []string  `json:"datetimes"`
			Values    []float64 `json:"values"`
		} `json:"valueSequence"`
	}
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatalf("the answer is no temporal property: %v: %s", err, body)
	}
	if doc.Type != "TReal" {
		t.Errorf("the answer is of type %q, want TReal: %s", doc.Type, body)
	}
	for _, s := range doc.ValueSequence {
		if first == "" && len(s.Datetimes) > 0 {
			first = s.Datetimes[0]
		}
		for _, v := range s.Values {
			if len(values) == 0 || values[len(values)-1] != v {
				values = append(values, v)
			}
		}
	}
	return values, first
}

func TestLiveRasterQueryOfAMovingFeature(t *testing.T) {
	cleanup := coverageFixture(t)
	defer cleanup()
	mux, done := atsLiveMux(t)
	defer done()
	if rec := atsDo(t, mux, "POST", "/collections",
		`{"id":"cov","coverage":{"table":"cov_series","validtime":"validtime","raster":"rast"}}`); rec.Code != 201 {
		t.Fatalf("registering the series answered %d: %s", rec.Code, rec.Body.String())
	}
	atsDo(t, mux, "DELETE", "/collections/rships", "")
	defer atsDo(t, mux, "DELETE", "/collections/rships", "")
	if rec := atsDo(t, mux, "POST", "/collections",
		`{"id":"rships","temporalGeometryType":"tgeompoint","crs":4326}`); rec.Code != 201 {
		t.Fatalf("creating the ships answered %d: %s", rec.Code, rec.Body.String())
	}
	items := "/collections/rships/items/"
	ship := func(id, mfjson string) string {
		rec := atsDo(t, mux, "POST", "/collections/rships/items", `{"id":`+id+`,"properties":{},"temporalGeometry":`+mfjson+`}`)
		if !oneOf(rec.Code, 201, 202) {
			t.Fatalf("adding a ship answered %d: %s", rec.Code, rec.Body.String())
		}
		return atsCreatedID(t, rec)
	}
	// Ship a sails along y = 1 from x = 0.5 to x = 3.5 over ten January days,
	// leaving the tile valued 1 for the one valued 2 at x = 2, on the 15th.
	a := ship("1", `{"type":"MovingPoint","coordinates":[[0.5,1],[3.5,1]],"datetimes":["2024-01-10T00:00:00+00:00","2024-01-20T00:00:00+00:00"],"lower_inc":true,"upper_inc":true,"interpolation":"Linear"}`)
	// Ship b lies at (1, 1) from January 30 to February 2, across the two periods.
	b := ship("2", `{"type":"MovingPoint","coordinates":[[1,1],[1,1]],"datetimes":["2024-01-30T00:00:00+00:00","2024-02-02T00:00:00+00:00"],"lower_inc":true,"upper_inc":true,"interpolation":"Linear"}`)

	cases := []struct {
		name, path string
		code       int
		values     []float64
		first      string
	}{
		{"a crosses from the tile valued 1 to the one valued 2",
			a + "/tgsequence/1/raster?collection=cov", 200, []float64{1, 2}, "2024-01-10"},
		{"the band is named",
			a + "/tgsequence/1/raster?collection=cov&band=1", 200, []float64{1, 2}, "2024-01-10"},
		{"b reads January's raster and then February's",
			b + "/tgsequence/1/raster?collection=cov", 200, []float64{1, 5}, "2024-01-30"},
		{"a value range keeps the times a reads a value inside it",
			a + "/tgsequence/1/raster?collection=cov&valueRange=1.5,10", 200, []float64{2}, "2024-01-15"},
		{"a value range no value falls inside answers no value",
			a + "/tgsequence/1/raster?collection=cov&valueRange=10,20", 200, nil, ""},
		{"the raster collection is named", a + "/tgsequence/1/raster", 400, nil, ""},
		{"an unknown raster collection", a + "/tgsequence/1/raster?collection=nowhere", 404, nil, ""},
		{"a collection of moving features is no raster collection",
			a + "/tgsequence/1/raster?collection=rships", 400, nil, ""},
		{"a band is a positive integer", a + "/tgsequence/1/raster?collection=cov&band=0", 400, nil, ""},
		{"a value range has two bounds", a + "/tgsequence/1/raster?collection=cov&valueRange=1", 400, nil, ""},
		{"a value range is ordered", a + "/tgsequence/1/raster?collection=cov&valueRange=5,1", 400, nil, ""},
		{"a temporal geometry the feature lacks", a + "/tgsequence/2/raster?collection=cov", 404, nil, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := atsDo(t, mux, "GET", items+c.path, "")
			if rec.Code != c.code {
				t.Fatalf("GET %s = %d, want %d: %s", c.path, rec.Code, c.code, rec.Body.String())
			}
			if c.code != 200 {
				return
			}
			values, first := rasterQueryValues(t, rec.Body.String())
			if len(values) != len(c.values) {
				t.Fatalf("GET %s reads %v, want %v", c.path, values, c.values)
			}
			for i := range values {
				if values[i] != c.values[i] {
					t.Fatalf("GET %s reads %v, want %v", c.path, values, c.values)
				}
			}
			if !strings.HasPrefix(first, c.first) {
				t.Errorf("GET %s starts at %q, want %s", c.path, first, c.first)
			}
		})
	}

	// A collection whose type MobilityDB declares no rasterValue over is refused
	// with the reason the catalog gives.
	atsDo(t, mux, "DELETE", "/collections/rgships", "")
	defer atsDo(t, mux, "DELETE", "/collections/rgships", "")
	if rec := atsDo(t, mux, "POST", "/collections",
		`{"id":"rgships","temporalGeometryType":"tgeogpoint","crs":4326}`); rec.Code != 201 {
		t.Fatalf("creating the geodetic ships answered %d: %s", rec.Code, rec.Body.String())
	}
	if rec := atsDo(t, mux, "POST", "/collections/rgships/items",
		`{"id":1,"properties":{},"temporalGeometry":{"type":"MovingPoint","coordinates":[[0.5,1],[3.5,1]],"datetimes":["2024-01-10T00:00:00+00:00","2024-01-20T00:00:00+00:00"],"lower_inc":true,"upper_inc":true,"interpolation":"Linear"}}`); !oneOf(rec.Code, 201, 202) {
		t.Fatalf("adding a geodetic ship answered %d: %s", rec.Code, rec.Body.String())
	}
	if rec := atsDo(t, mux, "GET", "/collections/rgships/items/1/tgsequence/1/raster?collection=cov", ""); rec.Code != 400 ||
		!strings.Contains(rec.Body.String(), "states no rasterValue") {
		t.Errorf("the raster query of a tgeogpoint answered %d: %s", rec.Code, rec.Body.String())
	}

	// The derived queries of a feature are unchanged by the raster query.
	if rec := atsDo(t, mux, "GET", items+a+"/tgsequence/1/velocity", ""); rec.Code != 200 {
		t.Errorf("velocity answered %d: %s", rec.Code, rec.Body.String())
	}
}
