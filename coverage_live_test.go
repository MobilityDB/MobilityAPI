package main

// Mirrors #TestATSLiveCollectionLifecycle of ats_live_test.go for a raster
// collection, against the same fixture database: a raster series is registered
// over a table the test creates, described, and read as coverages, and removing
// the collection leaves the table.

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
)

// coverageSeries is a raster series of two periods in EPSG:4326: two tiles of
// 2 x 2 one-degree pixels side by side over (0, 0)-(4, 2) in January, valued 1
// and 2, and one tile over (0, 0)-(2, 2) in February, valued 5.
const coverageSeries = `CREATE TABLE cov_series (validtime tstzspan, rast raster);
INSERT INTO cov_series VALUES
  ('[2024-01-01, 2024-02-01)', ST_AddBand(ST_MakeEmptyRaster(2, 2, 0, 2, 1, -1, 0, 0, 4326), '8BUI'::text, 1, 0)),
  ('[2024-01-01, 2024-02-01)', ST_AddBand(ST_MakeEmptyRaster(2, 2, 2, 2, 1, -1, 0, 0, 4326), '8BUI'::text, 2, 0)),
  ('[2024-02-01, 2024-03-01)', ST_AddBand(ST_MakeEmptyRaster(2, 2, 0, 2, 1, -1, 0, 0, 4326), '8BUI'::text, 5, 0))`

// coverageFixture creates the series and enables the GDAL drivers for the
// database, which PostGIS keeps disabled until postgis.gdal_enabled_drivers
// names them, and answers the function undoing both. It runs on a connection of
// its own before the tier's pool opens, so every connection of the pool reads
// the setting.
func coverageFixture(t *testing.T) func() {
	t.Helper()
	ctx := context.Background()
	mux, done := atsLiveMux(t)
	_ = mux
	stmts := []string{
		"DROP TABLE IF EXISTS cov_series",
		coverageSeries,
		`DO $$ BEGIN EXECUTE format('ALTER DATABASE %I SET postgis.gdal_enabled_drivers = %L',
		   current_database(), 'ENABLE_ALL'); END $$`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(ctx, s); err != nil {
			done()
			t.Fatalf("%s: %v", s, err)
		}
	}
	done()
	return func() {
		_, undo := atsLiveMux(t)
		db.Exec(ctx, "DELETE FROM collections WHERE id = 'cov'")
		db.Exec(ctx, "DROP TABLE IF EXISTS cov_series")
		db.Exec(ctx, `DO $$ BEGIN EXECUTE format('ALTER DATABASE %I RESET postgis.gdal_enabled_drivers',
		   current_database()); END $$`)
		undo()
	}
}

func coverageGet(t *testing.T, path, accept string) *httptest.ResponseRecorder {
	t.Helper()
	mux, done := atsLiveMux(t)
	defer done()
	r := httptest.NewRequest("GET", path, nil)
	if accept != "" {
		r.Header.Set("Accept", accept)
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, r)
	return rec
}

func TestLiveRasterCollection(t *testing.T) {
	cleanup := coverageFixture(t)
	defer cleanup()
	mux, done := atsLiveMux(t)
	defer done()

	// Registration checks the table's columns and creates nothing.
	for _, bad := range []string{
		`{"id":"cov","coverage":{"table":"cov_series","validtime":"rast","raster":"rast"}}`,
		`{"id":"cov","coverage":{"table":"cov_series","validtime":"validtime","raster":"rast","rasterType":"tgeompoint"}}`,
		`{"id":"cov","coverage":{"table":"cov_series","validtime":"validtime","raster":"rast; DROP"}}`,
	} {
		if rec := atsDo(t, mux, "POST", "/collections", bad); rec.Code != 400 {
			t.Errorf("registering %s answered %d, want 400: %s", bad, rec.Code, rec.Body.String())
		}
	}
	rec := atsDo(t, mux, "POST", "/collections",
		`{"id":"cov","title":"A series","coverage":{"table":"cov_series","validtime":"validtime","raster":"rast"}}`)
	if rec.Code != 201 {
		t.Fatalf("registering the series answered %d: %s", rec.Code, rec.Body.String())
	}

	// The description links the coverage and the schema, and states the extent.
	rec = atsDo(t, mux, "GET", "/collections/cov", "")
	var col struct {
		ItemType string `json:"itemType"`
		Links    []struct{ Rel, Href, Type string }
		Extent   struct {
			Spatial  struct{ Bbox [][]float64 }
			Temporal struct{ Interval [][]string }
		}
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &col); err != nil || rec.Code != 200 {
		t.Fatalf("describing the collection answered %d: %s", rec.Code, rec.Body.String())
	}
	rels := map[string]bool{}
	for _, l := range col.Links {
		rels[l.Rel+" "+l.Type] = true
	}
	for _, want := range []string{relCoverage + " " + mediaGeoTIFF, relCoverage + " " + mediaCovJSON,
		relSchema + " " + mediaSchema} {
		if !rels[want] {
			t.Errorf("the description has no link %s: %+v", want, col.Links)
		}
	}
	if b := col.Extent.Spatial.Bbox; len(b) != 1 || b[0][0] != 0 || b[0][1] != 0 || b[0][2] != 4 || b[0][3] != 2 {
		t.Errorf("the spatial extent is %v, want [[0 0 4 2]]", b)
	}
	if iv := col.Extent.Temporal.Interval; len(iv) != 1 || !strings.HasPrefix(iv[0][0], "2024-01-01") ||
		!strings.HasPrefix(iv[0][1], "2024-03-01") {
		t.Errorf("the temporal extent is %v, want 2024-01-01 to 2024-03-01", iv)
	}

	// The description carries what /req/edr/rc-collection-info and
	// collection.yaml of EDR 1.1 require: every link with its rel and type, a
	// data link per query, the output formats, and the band as a Parameter
	// keyed as parameter-name reads it. enc compares decoded members as their
	// JSON encodings.
	enc := func(v any) string { b, _ := json.Marshal(v); return string(b) }
	var edr struct {
		Links []struct {
			Rel, Href, Type string
		}
		OutputFormats  []string                  `json:"output_formats"`
		Keywords       []string                  `json:"keywords"`
		ParameterNames map[string]map[string]any `json:"parameter_names"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &edr); err != nil {
		t.Fatalf("reading the description: %v", err)
	}
	data := map[string]bool{}
	for _, l := range edr.Links {
		if l.Type == "" {
			t.Errorf("the link %s %s has no type", l.Rel, l.Href)
		}
		if l.Rel == "data" && l.Type == mediaCovJSON {
			data[l.Href] = true
		}
	}
	for _, q := range edrQueries {
		if !data["/collections/cov/"+q.name] {
			t.Errorf("the description has no data link to the %s query: %+v", q.name, edr.Links)
		}
	}
	if len(edr.OutputFormats) != 1 || edr.OutputFormats[0] != edrFormat {
		t.Errorf("output_formats is %v, want [%s]", edr.OutputFormats, edrFormat)
	}
	if len(edr.Keywords) == 0 {
		t.Errorf("the description has no keywords")
	}
	if p := edr.ParameterNames["band1"]; len(edr.ParameterNames) != 1 || p["type"] != "Parameter" ||
		p["id"] != "band1" || p["data-type"] != "integer" ||
		!strings.Contains(enc(p["observedProperty"]), `"label":{"en":"Band 1"}`) {
		t.Errorf("parameter_names is %v, want band1 an integer Parameter labelled Band 1", edr.ParameterNames)
	}

	// /collections lists the collection with the id, title, description and
	// extent of its own description, as /req/collections/src-md-success C
	// requires.
	rec = atsDo(t, mux, "GET", "/collections", "")
	var list struct{ Collections []map[string]any }
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil || rec.Code != 200 {
		t.Fatalf("listing the collections answered %d: %s", rec.Code, rec.Body.String())
	}
	var own map[string]any
	if err := json.Unmarshal(atsDo(t, mux, "GET", "/collections/cov", "").Body.Bytes(), &own); err != nil {
		t.Fatalf("reading the description: %v", err)
	}
	found := false
	for _, c := range list.Collections {
		if c["id"] != "cov" {
			continue
		}
		found = true
		for _, k := range []string{"id", "title", "description", "extent", "parameter_names", "output_formats"} {
			if enc(c[k]) != enc(own[k]) {
				t.Errorf("/collections states %s as %s, /collections/cov as %s", k, enc(c[k]), enc(own[k]))
			}
		}
	}
	if !found {
		t.Errorf("/collections does not list cov: %s", rec.Body.String())
	}

	// The schema lists the one band.
	rec = coverageGet(t, "/collections/cov/schema", "")
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"band1"`) ||
		!strings.Contains(rec.Body.String(), `"integer"`) {
		t.Errorf("the schema answered %d: %s", rec.Code, rec.Body.String())
	}

	// No datetime answers the latest period, as GeoTIFF.
	rec = coverageGet(t, "/collections/cov/coverage", "")
	if rec.Code != 200 || rec.Header().Get("Content-Type") != mediaGeoTIFF ||
		!strings.HasPrefix(rec.Header().Get("Content-Datetime"), "2024-02-01") {
		t.Fatalf("the latest coverage answered %d, %q, %q: %s", rec.Code, rec.Header().Get("Content-Type"),
			rec.Header().Get("Content-Datetime"), rec.Body.String())
	}
	if b := rec.Body.Bytes(); len(b) < 4 || !(string(b[:2]) == "II" || string(b[:2]) == "MM") {
		t.Errorf("the GeoTIFF does not open as a TIFF: % x", b[:4])
	}

	// A datetime inside January answers the two tiles of January joined, as
	// CoverageJSON: a grid four pixels wide.
	rec = coverageGet(t, "/collections/cov/coverage?datetime=2024-01-15T00:00:00Z", mediaCovJSON)
	var cov struct {
		Domain struct {
			Axes struct{ X, Y struct{ Num int } }
		}
	}
	if rec.Code != 200 || rec.Header().Get("Content-Type") != mediaCovJSON {
		t.Fatalf("the January coverage answered %d: %s", rec.Code, rec.Body.String())
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &cov); err != nil || cov.Domain.Axes.X.Num != 4 ||
		cov.Domain.Axes.Y.Num != 2 {
		t.Errorf("the January coverage is not a 4 x 2 grid: %s", rec.Body.String())
	}

	// A bbox over the first tile answers that tile's bounds.
	rec = coverageGet(t, "/collections/cov/coverage?datetime=2024-01-15T00:00:00Z&bbox=0,0,2,2", mediaCovJSON)
	if rec.Code != 200 || rec.Header().Get("Content-Bbox") != "0,0,2,2" {
		t.Errorf("the clipped coverage answered %d, Content-Bbox %q: %s", rec.Code,
			rec.Header().Get("Content-Bbox"), rec.Body.String())
	}
	rec = coverageGet(t, "/collections/cov/coverage?datetime=2024-01-15T00:00:00Z&subset=Lon(0:2),Lat(0:2)", mediaCovJSON)
	if rec.Code != 200 || rec.Header().Get("Content-Bbox") != "0,0,2,2" {
		t.Errorf("the subset coverage answered %d, Content-Bbox %q", rec.Code, rec.Header().Get("Content-Bbox"))
	}

	// The refusals the draft states.
	for _, c := range []struct {
		path, accept string
		code         int
	}{
		{"/collections/cov/coverage?datetime=2024-01-01T00:00:00Z/2024-03-01T00:00:00Z", "", 400},
		{"/collections/cov/coverage?bbox=0,0,2,2&subset=Lat(0:1)", "", 400},
		{"/collections/cov/coverage?datetime=2024-01-15T00:00:00Z&subset=time(\"2024-01-15T00:00:00Z\")", "", 400},
		{"/collections/cov/coverage?properties=band2", "", 400},
		{"/collections/cov/coverage?properties=band1&exclude-properties=band1", "", 400},
		{"/collections/cov/coverage", "application/geo+json", 406},
		{"/collections/cov/coverage?datetime=2023-01-01T00:00:00Z", "", 404},
		{"/collections/nosuch/coverage", "", 404},
	} {
		if rec := coverageGet(t, c.path, c.accept); rec.Code != c.code {
			t.Errorf("%s (Accept %q) answered %d, want %d: %s", c.path, c.accept, rec.Code, c.code,
				rec.Body.String())
		}
	}
	rec = coverageGet(t, "/collections/cov/coverage?datetime=2024-01-01T00:00:00Z/2024-03-01T00:00:00Z", "")
	if !strings.Contains(rec.Body.String(), "2 periods") {
		t.Errorf("a datetime meeting both periods does not name them: %s", rec.Body.String())
	}

	// The conformance declaration names the classes.
	rec = atsDo(t, mux, "GET", "/conformance", "")
	for _, c := range coverageConformance {
		if !strings.Contains(rec.Body.String(), c) {
			t.Errorf("the conformance declaration does not name %s", c)
		}
	}

	// Removing the collection removes its registration and leaves the table.
	if rec := atsDo(t, mux, "DELETE", "/collections/cov", ""); rec.Code != 200 && rec.Code != 204 {
		t.Fatalf("removing the collection answered %d: %s", rec.Code, rec.Body.String())
	}
	var n int
	if err := db.QueryRow(context.Background(), "SELECT COUNT(*) FROM cov_series").Scan(&n); err != nil || n != 3 {
		t.Errorf("removing the collection touched its table: %d rows, %v", n, err)
	}
}
