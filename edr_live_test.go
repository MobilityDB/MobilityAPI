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

	// The description states the four queries, the corridor with its units.
	rec := atsDo(t, mux, "GET", "/collections/cov", "")
	var col struct {
		DataQueries map[string]struct {
			Link struct {
				Href, Rel string
				Variables map[string]any
			}
		} `json:"data_queries"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &col); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{"position", "area", "trajectory", "corridor"} {
		l := col.DataQueries[q].Link
		if l.Rel != "data" || l.Href != "/collections/cov/"+q || l.Variables["query_type"] != q {
			t.Errorf("data_queries.%s is %+v", q, l)
		}
	}
	if col.DataQueries["corridor"].Link.Variables["width_units"] == nil {
		t.Errorf("the corridor states no width_units: %+v", col.DataQueries["corridor"])
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
	} {
		if rec := coverageGet(t, path, ""); rec.Code != 400 {
			t.Errorf("%s answered %d, want 400: %s", path, rec.Code, rec.Body.String())
		}
	}
	rec = atsDo(t, mux, "GET", "/conformance", "")
	if !strings.Contains(rec.Body.String(), edrConformance) {
		t.Errorf("the conformance declaration does not name %s", edrConformance)
	}
}
