package main

// Mirrors #TestLiveRasterCollection of coverage_live_test.go for the EDR
// queries, over the same series: two tiles over (0, 0)-(4, 2) in January
// valued 1 and 2, one over (0, 0)-(2, 2) in February valued 5.

import (
	"encoding/json"
	"strings"
	"testing"
)

// edrCoverage reads what a CoverageJSON answer states: its domain type, the
// size of its x axis (a Grid's) and its values, the parameter value's for a
// Point, PointSeries or Trajectory and band1's for a Grid.
func edrCoverage(t *testing.T, body string) (domain string, nx int, values []float64) {
	t.Helper()
	var c struct {
		Domain struct {
			DomainType string `json:"domainType"`
			Axes       struct {
				X struct {
					Num    int       `json:"num"`
					Values []float64 `json:"values"`
				} `json:"x"`
			} `json:"axes"`
		} `json:"domain"`
		Ranges map[string]struct {
			Values []*float64 `json:"values"`
		} `json:"ranges"`
	}
	if err := json.Unmarshal([]byte(body), &c); err != nil {
		t.Fatalf("the answer is no CoverageJSON: %v: %s", err, body)
	}
	nx = c.Domain.Axes.X.Num
	if nx == 0 {
		nx = len(c.Domain.Axes.X.Values)
	}
	for _, k := range []string{"value", "band1"} {
		if r, ok := c.Ranges[k]; ok {
			for _, v := range r.Values {
				if v != nil {
					values = append(values, *v)
				}
			}
		}
	}
	return c.Domain.DomainType, nx, values
}

func TestLiveRasterEDRQueries(t *testing.T) {
	cleanup := coverageFixture(t)
	defer cleanup()
	mux, done := atsLiveMux(t)
	defer done()
	if rec := atsDo(t, mux, "POST", "/collections",
		`{"id":"cov","coverage":{"table":"cov_series","validtime":"validtime","raster":"rast"}}`); rec.Code != 201 {
		t.Fatalf("registering the series answered %d: %s", rec.Code, rec.Body.String())
	}

	// The description states the six queries, the corridor and the radius with
	// their units, and the distance units /req/edr/rc-collection-info I
	// requires of a description linking a radius.
	rec := atsDo(t, mux, "GET", "/collections/cov", "")
	var col struct {
		DistanceUnits []string `json:"distanceunits"`
		DataQueries   map[string]struct {
			Link struct {
				Href, Rel string
				Variables map[string]any
			}
		} `json:"data_queries"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &col); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{"position", "radius", "area", "cube", "trajectory", "corridor"} {
		l := col.DataQueries[q].Link
		if l.Rel != "data" || l.Href != "/collections/cov/"+q || l.Variables["query_type"] != q {
			t.Errorf("data_queries.%s is %+v", q, l)
		}
	}
	if col.DataQueries["corridor"].Link.Variables["width_units"] == nil {
		t.Errorf("the corridor states no width_units: %+v", col.DataQueries["corridor"])
	}
	if col.DataQueries["radius"].Link.Variables["within_units"] == nil || len(col.DistanceUnits) == 0 {
		t.Errorf("the radius states no within_units or the description no distanceunits: %+v %v",
			col.DataQueries["radius"], col.DistanceUnits)
	}

	type want struct {
		domain string
		nx     int
		first  float64
		last   float64
	}
	for _, c := range []struct {
		path string
		want want
	}{
		// A point at an instant of January reads the first tile.
		{"/collections/cov/position?coords=POINT(0.5%201)&datetime=2024-01-15T00:00:00Z",
			want{"Point", 1, 1, 1}},
		// Over January and February: 1, then February's 5.
		{"/collections/cov/position?coords=POINT(0.5%201)&datetime=2024-01-15T00:00:00Z/2024-02-15T00:00:00Z",
			want{"PointSeries", 1, 1, 5}},
		// A path across January's two tiles, 2024-01-10 to 2024-01-11.
		{"/collections/cov/trajectory?coords=LINESTRINGM(0.5%201%201704844800,3.5%201%201704931200)",
			want{"Trajectory", 0, 1, 2}},
		// The first tile of January.
		{"/collections/cov/area?coords=POLYGON((0%200,2%200,2%202,0%202,0%200))&datetime=2024-01-15T00:00:00Z",
			want{"Grid", 2, 1, 1}},
		// A 200 km corridor along the first tile of February, wider than its
		// one-degree pixels, whose centres clip keeps
		{"/collections/cov/corridor?coords=LINESTRING(0.5%201,1.5%201)&corridor-width=200&width-units=km" +
			"&corridor-height=1&height-units=m&datetime=2024-02-15T00:00:00Z", want{"Grid", 2, 5, 5}},
		// A 40 km radius about the centre of the second tile's first pixel in
		// January, a circle inside that one-degree pixel, keeps that pixel's
		// value alone, on a grid of two pixels each way: the mask rounds the
		// extent of the circle outward to whole pixels, as ST_Clip's does
		{"/collections/cov/radius?coords=POINT(2.5%201.5)&within=40&within-units=km" +
			"&datetime=2024-01-15T00:00:00Z", want{"Grid", 2, 2, 2}},
		// The cube of the second tile in January, z read and not applied.
		{"/collections/cov/cube?bbox=2,0,4,2&z=0/100&datetime=2024-01-15T00:00:00Z",
			want{"Grid", 2, 2, 2}},
	} {
		rec := coverageGet(t, c.path, "")
		if rec.Code != 200 || rec.Header().Get("Content-Type") != mediaCovJSON {
			t.Errorf("%s answered %d: %s", c.path, rec.Code, rec.Body.String())
			continue
		}
		domain, nx, values := edrCoverage(t, rec.Body.String())
		if domain != c.want.domain || (c.want.nx > 0 && nx != c.want.nx) || len(values) == 0 ||
			values[0] != c.want.first || values[len(values)-1] != c.want.last {
			t.Errorf("%s answered %s, x %d, values %v; want %+v", c.path, domain, nx, values, c.want)
		}
	}

	// The refusals the standard states.
	for _, path := range []string{
		"/collections/cov/position",
		"/collections/cov/position?coords=POLYGON((0%200,1%200,1%201,0%200))",
		"/collections/cov/trajectory?coords=LINESTRING(0.5%201,1.5%201)",
		"/collections/cov/position?coords=POINT(0.5%201)&f=GeoJSON",
		"/collections/cov/corridor?coords=LINESTRING(0.5%201,1.5%201)&width-units=km&corridor-height=1&height-units=m",
		"/collections/cov/corridor?coords=LINESTRING(0.5%201,1.5%201)&corridor-width=1&width-units=mi" +
			"&corridor-height=1&height-units=m",
		"/collections/cov/position?coords=nonsense",
		"/collections/cov/radius?coords=POINT(2.5%201.5)&within-units=km",
		"/collections/cov/radius?coords=POINT(2.5%201.5)&within=60",
		"/collections/cov/radius?coords=POINT(2.5%201.5)&within=60&within-units=mi",
		"/collections/cov/radius?coords=LINESTRING(0.5%201,1.5%201)&within=60&within-units=km",
		"/collections/cov/cube?bbox=2,0,4,2",
		"/collections/cov/cube?z=0/100",
		"/collections/cov/cube?bbox=2,0,4&z=0/100",
	} {
		if rec := coverageGet(t, path, ""); rec.Code != 400 {
			t.Errorf("%s answered %d, want 400: %s", path, rec.Code, rec.Body.String())
		}
	}
	rec = atsDo(t, mux, "GET", "/conformance", "")
	if !strings.Contains(rec.Body.String(), edrConformance) {
		t.Errorf("the conformance declaration does not name %s", edrConformance)
	}

	// The instances are the two periods, January and February, in time order,
	// each described as a collection is, with the extent of its period alone
	// and its data links under its own path; the description /instances/{iid}
	// writes is the one /instances lists.
	rec = atsDo(t, mux, "GET", "/collections/cov/instances", "")
	var insts struct {
		Links     []struct{ Rel, Href, Type string }
		Instances []struct {
			ID     string
			Links  []struct{ Rel, Href, Type string }
			Extent struct {
				Spatial  struct{ Bbox [][]float64 }
				Temporal struct{ Interval [][]string }
			}
		}
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &insts); err != nil || rec.Code != 200 {
		t.Fatalf("listing the instances answered %d: %s", rec.Code, rec.Body.String())
	}
	if len(insts.Instances) != 2 || !strings.HasPrefix(insts.Instances[0].ID, "2024-01-01") ||
		!strings.HasPrefix(insts.Instances[1].ID, "2024-02-01") {
		t.Fatalf("the instances are %+v, want January and February", insts.Instances)
	}
	jan, feb := insts.Instances[0], insts.Instances[1]
	if b := jan.Extent.Spatial.Bbox; len(b) != 1 || b[0][2] != 4 {
		t.Errorf("January's extent is %v, want its two tiles, to x 4", b)
	}
	if b := feb.Extent.Spatial.Bbox; len(b) != 1 || b[0][2] != 2 {
		t.Errorf("February's extent is %v, want its one tile, to x 2", b)
	}
	if iv := feb.Extent.Temporal.Interval; len(iv) != 1 || !strings.HasPrefix(iv[0][0], "2024-02-01") ||
		!strings.HasPrefix(iv[0][1], "2024-03-01") {
		t.Errorf("February's interval is %v", iv)
	}
	febPath := "/collections/cov/instances/" + feb.ID
	data := 0
	for _, l := range feb.Links {
		if l.Rel == "data" && strings.HasPrefix(l.Href, febPath+"/") {
			data++
		}
	}
	if data != len(edrQueries) {
		t.Errorf("February's links hold %d data links under %s, want %d: %+v", data, febPath,
			len(edrQueries), feb.Links)
	}
	var one, listed map[string]any
	rec = atsDo(t, mux, "GET", febPath, "")
	if err := json.Unmarshal(rec.Body.Bytes(), &one); err != nil || rec.Code != 200 {
		t.Fatalf("describing %s answered %d: %s", febPath, rec.Code, rec.Body.String())
	}
	var all struct{ Instances []map[string]any }
	json.Unmarshal(atsDo(t, mux, "GET", "/collections/cov/instances", "").Body.Bytes(), &all)
	listed = all.Instances[1]
	for _, k := range []string{"id", "title", "extent", "links", "data_queries"} {
		a, _ := json.Marshal(one[k])
		b, _ := json.Marshal(listed[k])
		if string(a) != string(b) {
			t.Errorf("%s states %s as %s, /instances as %s", febPath, k, a, b)
		}
	}

	// A query under an instance reads its period alone: a position with no
	// datetime reads January's value under January and February's under
	// February, and an area with no datetime clips January's raster under
	// January where the collection's own area reads the period that ends last.
	for _, c := range []struct {
		path  string
		first float64
	}{
		{"/collections/cov/instances/" + jan.ID + "/position?coords=POINT(0.5%201)", 1},
		{febPath + "/position?coords=POINT(0.5%201)", 5},
		{"/collections/cov/instances/" + jan.ID + "/area?coords=POLYGON((0%200,2%200,2%202,0%202,0%200))", 1},
		{"/collections/cov/area?coords=POLYGON((0%200,2%200,2%202,0%202,0%200))", 5},
	} {
		rec := coverageGet(t, strings.ReplaceAll(c.path, "+", "%2B"), "")
		if rec.Code != 200 {
			t.Errorf("%s answered %d: %s", c.path, rec.Code, rec.Body.String())
			continue
		}
		if _, _, values := edrCoverage(t, rec.Body.String()); len(values) == 0 || values[0] != c.first {
			t.Errorf("%s reads %v, want %v first", c.path, values, c.first)
		}
	}
	for path, want := range map[string]int{
		"/collections/cov/instances/2023-01-01T00:00:00Z":                                404,
		"/collections/cov/instances/2023-01-01T00:00:00Z/position?coords=POINT(0.5%201)": 404,
		// A datetime outside the instance's period meets no period of it
		febPath + "/area?coords=POLYGON((0%200,2%200,2%202,0%202,0%200))&datetime=2024-01-15T00:00:00Z": 404,
		"/collections/nocov/instances": 404,
	} {
		if rec := coverageGet(t, strings.ReplaceAll(path, "+", "%2B"), ""); rec.Code != want {
			t.Errorf("%s answered %d, want %d: %s", path, rec.Code, want, rec.Body.String())
		}
	}
}
