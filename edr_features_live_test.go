package main

// Mirrors #TestLiveRasterEDRQueries of edr_live_test.go for the EDR queries
// over a collection of moving features, over the conformance fixture: Alpha
// from (575000, 6220000) to (579000, 6222000) between 08:00 and 08:45, Bravo
// from (574000, 6219000) to (575600, 6220100) between 09:00 and 09:30, on
// 2026-01-01, in UTM 32N. Each geometry of a query is stated in UTM 32N and
// carried into CRS84 by the database, so the test reads the positions the
// fixture states.

import (
	"encoding/json"
	"net/url"
	"reflect"
	"strings"
	"testing"
)

// edrWKT answers the WKT in CRS84 of the geometry wkt states in UTM 32N.
func edrWKT(t *testing.T, wkt string) string {
	t.Helper()
	var out string
	if err := db.QueryRow(t.Context(), "SELECT ST_AsText(ST_Transform(ST_GeomFromText($1, 25832), 4326), 9)",
		wkt).Scan(&out); err != nil {
		t.Fatal(err)
	}
	return out
}

// edrBBox answers the bbox in CRS84 of the rectangle (x0, y0)-(x1, y1) in UTM 32N.
func edrBBox(t *testing.T, x0, y0, x1, y1 string) string {
	t.Helper()
	var xmin, ymin, xmax, ymax string
	if err := db.QueryRow(t.Context(), "SELECT CAST(ST_XMin(e) AS text), CAST(ST_YMin(e) AS text), "+
		"CAST(ST_XMax(e) AS text), CAST(ST_YMax(e) AS text) FROM (SELECT ST_Transform(ST_MakeEnvelope("+
		x0+", "+y0+", "+x1+", "+y1+", 25832), 4326) e) s").Scan(&xmin, &ymin, &xmax, &ymax); err != nil {
		t.Fatal(err)
	}
	return xmin + "," + ymin + "," + xmax + "," + ymax
}

// edrFeatureIDs answers the ids of the features of a FeatureCollection, in order.
func edrFeatureIDs(t *testing.T, body string) []string {
	t.Helper()
	var fc struct {
		Type     string `json:"type"`
		Features []struct {
			ID string `json:"id"`
		} `json:"features"`
	}
	if err := json.Unmarshal([]byte(body), &fc); err != nil || fc.Type != "FeatureCollection" {
		t.Fatalf("the answer is no FeatureCollection: %v: %s", err, body)
	}
	ids := []string{}
	for _, f := range fc.Features {
		ids = append(ids, f.ID)
	}
	return ids
}

func TestLiveFeatureEDRQueries(t *testing.T) {
	mux, done := atsLiveMux(t)
	defer done()
	get := func(path string) (int, string, string) {
		rec := atsDo(t, mux, "GET", path, "")
		return rec.Code, rec.Header().Get("Content-Type"), rec.Body.String()
	}

	// The description is what /req/edr/rc-collection-info requires, alike at
	// /collections/{cid} and in /collections.
	var one map[string]any
	code, _, body := get("/collections/conformance")
	if code != 200 || json.Unmarshal([]byte(body), &one) != nil {
		t.Fatalf("/collections/conformance answered %d: %s", code, body)
	}
	var all struct {
		Collections []map[string]any `json:"collections"`
	}
	if _, _, body := get("/collections"); json.Unmarshal([]byte(body), &all) != nil {
		t.Fatalf("/collections is no JSON: %s", body)
	}
	listed := false
	for _, c := range all.Collections {
		if c["id"] == "conformance" {
			listed = reflect.DeepEqual(c, one)
		}
	}
	if !listed {
		t.Errorf("/collections does not list the description /collections/conformance answers")
	}
	for _, k := range []string{"id", "links", "extent", "crs", "data_queries", "parameter_names",
		"output_formats", "distanceunits", "keywords"} {
		if one[k] == nil {
			t.Errorf("the description has no %s: %s", k, body)
		}
	}
	queries, _ := one["data_queries"].(map[string]any)
	for _, q := range []string{"area", "radius", "cube", "trajectory"} {
		entry, _ := queries[q].(map[string]any)
		l, _ := entry["link"].(map[string]any)
		if l["rel"] != "data" || l["href"] != "/collections/conformance/"+q || l["type"] == nil {
			t.Errorf("data_queries.%s is %v", q, queries[q])
		}
	}
	data := 0
	links, _ := one["links"].([]any)
	for _, l := range links {
		l, _ := l.(map[string]any)
		if l["rel"] == nil || l["type"] == nil {
			t.Errorf("the link %v has no rel or no type", l)
		}
		if l["rel"] == "data" {
			data++
		}
	}
	if data != 4 {
		t.Errorf("the description has %d data links, want one per query, 4", data)
	}

	near := edrWKT(t, "POINT(575600 6220100)") // Bravo's last position
	for _, c := range []struct {
		name, path string
		want       []string
	}{
		{"an area about Alpha's last position", "/collections/conformance/area?coords=" +
			url.QueryEscape(edrWKT(t, "POLYGON((578500 6221500,579500 6221500,579500 6222500,578500 6222500,578500 6221500))")),
			[]string{"1"}},
		{"the same area once Alpha has stopped", "/collections/conformance/area?coords=" +
			url.QueryEscape(edrWKT(t, "POLYGON((578500 6221500,579500 6221500,579500 6222500,578500 6222500,578500 6221500))")) +
			"&datetime=2026-01-01T09:00:00Z/2026-01-01T09:30:00Z", []string{}},
		{"an area about both", "/collections/conformance/area?coords=" +
			url.QueryEscape(edrWKT(t, "POLYGON((573000 6218000,580000 6218000,580000 6223000,573000 6223000,573000 6218000))")),
			[]string{"1", "2"}},
		{"an area about both at an instant of Bravo's", "/collections/conformance/area?coords=" +
			url.QueryEscape(edrWKT(t, "POLYGON((573000 6218000,580000 6218000,580000 6223000,573000 6223000,573000 6218000))")) +
			"&datetime=2026-01-01T09:10:00Z", []string{"2"}},
		// Alpha passes 178.9 m from Bravo's last position, at (575520, 6220260)
		// on its first segment, so a radius of 170 m holds Bravo alone and one of
		// 190 m both, where Alpha's positions themselves are all farther than 400 m
		{"170 m about Bravo's last position", "/collections/conformance/radius?coords=" +
			url.QueryEscape(near) + "&within=170&within-units=m", []string{"2"}},
		{"190 m about it", "/collections/conformance/radius?coords=" + url.QueryEscape(near) +
			"&within=0.19&within-units=km", []string{"1", "2"}},
		{"190 m about it before Bravo sets out", "/collections/conformance/radius?coords=" + url.QueryEscape(near) +
			"&within=190&within-units=m&datetime=../2026-01-01T08:50:00Z", []string{"1"}},
		{"190 m about it before either sets out", "/collections/conformance/radius?coords=" + url.QueryEscape(near) +
			"&within=190&within-units=m&datetime=../2025-12-31T00:00:00Z", []string{}},
		{"190 m about it once Alpha has stopped", "/collections/conformance/radius?coords=" + url.QueryEscape(near) +
			"&within=190&within-units=m&datetime=2026-01-01T08:50:00Z/..", []string{"2"}},
		{"a cube about Bravo's first position", "/collections/conformance/cube?bbox=" +
			edrBBox(t, "573900", "6218900", "574100", "6219100") + "&z=0/10", []string{"2"}},
		// A path across Alpha's second segment, east of every position of Bravo's
		{"a path across Alpha's", "/collections/conformance/trajectory?coords=" +
			url.QueryEscape(edrWKT(t, "LINESTRING(577000 6219000,577000 6223000)")), []string{"1"}},
	} {
		code, media, body := get(c.path)
		if code != 200 || media != "application/json" {
			t.Errorf("%s: %s answered %d %s: %s", c.name, c.path, code, media, body)
			continue
		}
		if got := edrFeatureIDs(t, body); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: %s answered the features %v, want %v", c.name, c.path, got, c.want)
		}
	}

	// A page of one feature links the next, which holds the other
	both := "/collections/conformance/area?coords=" +
		url.QueryEscape(edrWKT(t, "POLYGON((573000 6218000,580000 6218000,580000 6223000,573000 6223000,573000 6218000))"))
	_, _, body = get(both + "&limit=1")
	var page struct {
		Links []struct{ Rel, Href string } `json:"links"`
	}
	if json.Unmarshal([]byte(body), &page) != nil || len(page.Links) != 1 || page.Links[0].Rel != "next" {
		t.Fatalf("a page of one links no next page: %s", body)
	}
	if got := edrFeatureIDs(t, body); !reflect.DeepEqual(got, []string{"1"}) {
		t.Errorf("the first page holds %v, want [1]", got)
	}
	if _, _, next := get(page.Links[0].Href); !reflect.DeepEqual(edrFeatureIDs(t, next), []string{"2"}) {
		t.Errorf("the next page %s holds %v, want [2]", page.Links[0].Href, edrFeatureIDs(t, next))
	}

	// In GeoJSON each feature is the path it traverses, in WGS 84
	code, media, body := get(both + "&f=GeoJSON")
	var geo struct {
		Features []struct {
			Geometry struct {
				Type        string      `json:"type"`
				Coordinates [][]float64 `json:"coordinates"`
			} `json:"geometry"`
			TemporalGeometry any `json:"temporalGeometry"`
		} `json:"features"`
	}
	if code != 200 || media != "application/geo+json" || json.Unmarshal([]byte(body), &geo) != nil ||
		len(geo.Features) != 2 {
		t.Fatalf("f=GeoJSON answered %d %s: %s", code, media, body)
	}
	for _, f := range geo.Features {
		c := f.Geometry.Coordinates
		if f.Geometry.Type != "LineString" || f.TemporalGeometry != nil || len(c) == 0 ||
			c[0][0] < 9 || c[0][0] > 11 || c[0][1] < 55 || c[0][1] > 57 {
			t.Errorf("a GeoJSON feature is %+v, want its path in longitude and latitude", f)
		}
	}

	// What each query refuses
	for _, path := range []string{
		"/collections/conformance/area",
		"/collections/conformance/area?coords=" + url.QueryEscape(near),
		"/collections/conformance/area?coords=" + url.QueryEscape(near) + "&f=CoverageJSON",
		"/collections/conformance/radius?coords=" + url.QueryEscape(near) + "&within=100",
		"/collections/conformance/radius?coords=" + url.QueryEscape(near) + "&within=100&within-units=mi",
		"/collections/conformance/radius?coords=" + url.QueryEscape(near) + "&within=-1&within-units=m",
		"/collections/conformance/radius?coords=" + url.QueryEscape(near) + "&within=100&within-units=m" +
			"&parameter-name=velocity",
		"/collections/conformance/cube?bbox=" + edrBBox(t, "573900", "6218900", "574100", "6219100"),
		"/collections/conformance/trajectory?coords=" + url.QueryEscape("LINESTRINGM(10 56 0,10.1 56 60)"),
		// An interval whose end precedes its start, which MobilityDB refuses
		"/collections/conformance/area?coords=" + url.QueryEscape(edrWKT(t,
			"POLYGON((573000 6218000,580000 6218000,580000 6223000,573000 6223000,573000 6218000))")) +
			"&datetime=2026-01-02T00:00:00Z/2026-01-01T00:00:00Z",
	} {
		if code, _, body := get(path); code != 400 {
			t.Errorf("%s answered %d, want 400: %s", path, code, body)
		}
	}
	for _, path := range []string{
		"/collections/conformance/position?coords=" + url.QueryEscape(near),
		"/collections/conformance/instances/1/area?coords=" + url.QueryEscape(near),
		"/collections/none/area?coords=" + url.QueryEscape(near),
	} {
		if code, _, body := get(path); code != 404 {
			t.Errorf("%s answered %d, want 404: %s", path, code, body)
		}
	}

	// A collection of geographies is related to the query's geometry on the
	// spheroid: a moving point from (1, 1) to (2, 2) meets an area about (1.5,
	// 1.5) and comes within 120 km, not 100 km, of (3, 2), 111 km east of its last
	// position
	const geog = "edr_geog"
	atsDo(t, mux, "DELETE", "/collections/"+geog, "")
	defer atsDo(t, mux, "DELETE", "/collections/"+geog, "")
	if rec := atsDo(t, mux, "POST", "/collections",
		`{"id":"`+geog+`","temporalGeometryType":"tgeogpoint","crs":4326}`); rec.Code != 201 {
		t.Fatalf("POST /collections of tgeogpoint = %d (%s)", rec.Code, rec.Body.String())
	}
	if rec := atsDo(t, mux, "POST", "/collections/"+geog+"/items", `{"properties":{},"temporalGeometry":`+
		`{"type":"MovingPoint","coordinates":[[1,1],[2,2]],"datetimes":["2001-01-01T00:00:00+00:00",`+
		`"2001-01-02T00:00:00+00:00"],"lower_inc":true,"upper_inc":true,"interpolation":"Linear"}}`); !oneOf(rec.Code, 201, 202) {
		t.Fatalf("POST a tgeogpoint feature = %d (%s)", rec.Code, rec.Body.String())
	}
	for _, c := range []struct {
		path string
		n    int
	}{
		{"/collections/" + geog + "/area?coords=" + url.QueryEscape("POLYGON((1.4 1.4,1.6 1.4,1.6 1.6,1.4 1.6,1.4 1.4))"), 1},
		{"/collections/" + geog + "/area?coords=" + url.QueryEscape("POLYGON((5 5,6 5,6 6,5 6,5 5))"), 0},
		{"/collections/" + geog + "/cube?bbox=1.4,1.4,1.6,1.6&z=0", 1},
		{"/collections/" + geog + "/radius?coords=" + url.QueryEscape("POINT(3 2)") + "&within=120&within-units=km", 1},
		{"/collections/" + geog + "/radius?coords=" + url.QueryEscape("POINT(3 2)") + "&within=100&within-units=km", 0},
	} {
		code, _, body := get(c.path)
		if code != 200 || len(edrFeatureIDs(t, body)) != c.n {
			t.Errorf("%s answered %d with %d features, want %d: %s", c.path, code, len(edrFeatureIDs(t, body)), c.n, body)
		}
	}

	_, _, body = get("/conformance")
	for _, c := range []string{edrCollectionsConformance, edrQueriesConformance} {
		if !strings.Contains(body, `"`+c+`"`) {
			t.Errorf("the conformance declaration does not name %s", c)
		}
	}
}
