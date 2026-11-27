package main

// Mirrors #TestLiveFeatureEDRQueries of edr_features_live_test.go for the bbox
// over a collection of geographies, which /items and the EDR cube read as a
// longitude and latitude box on the spheroid (#lonLatBoxTest).

import (
	"encoding/json"
	"reflect"
	"testing"
)

// geographicIDs answers the ids of the features a FeatureCollection holds, in
// order, failing the test where the answer is no FeatureCollection.
func geographicIDs(t *testing.T, code int, body string) []string {
	t.Helper()
	var fc struct {
		Type     string `json:"type"`
		Features []struct {
			ID string `json:"id"`
		} `json:"features"`
	}
	if code != 200 || json.Unmarshal([]byte(body), &fc) != nil || fc.Type != "FeatureCollection" {
		t.Fatalf("answered %d, no FeatureCollection: %s", code, body)
	}
	ids := []string{}
	for _, f := range fc.Features {
		ids = append(ids, f.ID)
	}
	return ids
}

func TestLiveBboxOverGeographies(t *testing.T) {
	mux, done := atsLiveMux(t)
	defer done()
	post := func(cid, typ string, features ...string) []string {
		t.Helper()
		atsDo(t, mux, "DELETE", "/collections/"+cid, "")
		if rec := atsDo(t, mux, "POST", "/collections",
			`{"id":"`+cid+`","temporalGeometryType":"`+typ+`","crs":4326}`); rec.Code != 201 {
			t.Fatalf("POST /collections of %s = %d (%s)", typ, rec.Code, rec.Body.String())
		}
		var ids []string
		for i, f := range features {
			rec := atsDo(t, mux, "POST", "/collections/"+cid+"/items",
				`{"id":`+itoa(i+1)+`,"properties":{},"temporalGeometry":`+f+`}`)
			if !oneOf(rec.Code, 201, 202) {
				t.Fatalf("POST a %s feature = %d (%s)", typ, rec.Code, rec.Body.String())
			}
			ids = append(ids, atsCreatedID(t, rec))
		}
		return ids
	}
	const span = `"datetimes":["2001-01-01T00:00:00+00:00","2001-01-02T00:00:00+00:00"],` +
		`"lower_inc":true,"upper_inc":true,"interpolation":"Linear"}`

	// The arc from (-170, 70) to (170, 70) follows the geodesic across the
	// antimeridian, which rises to 70.28 near longitude 180; the point stays at
	// (0, 70.25), where the arc would pass were it a straight line in longitude
	// and latitude
	const points = "geo_bbox_points"
	defer atsDo(t, mux, "DELETE", "/collections/"+points, "")
	ids := post(points, "tgeogpoint",
		`{"type":"MovingPoint","coordinates":[[-170,70],[170,70]],`+span,
		`{"type":"MovingPoint","coordinates":[[0,70.25],[0,70.25]],`+span)
	arc, still := ids[0], ids[1]
	for _, c := range []struct {
		name, bbox string
		want       []string
	}{
		{"the summit of the arc", "175,70.2,180,70.3", []string{arc}},
		{"the line in longitude and latitude, which the geodesic does not take", "-5,69.9,5,70.1", []string{}},
		{"under the arc, within its bounding box", "178,70.0,180,70.1", []string{}},
		{"across the antimeridian, minLon above maxLon", "179,70.2,-179,70.3", []string{arc}},
		{"the same latitudes away from the antimeridian", "-179,70.2,179,70.3", []string{arc, still}},
	} {
		rec := atsDo(t, mux, "GET", "/collections/"+points+"/items?bbox="+c.bbox, "")
		if got := geographicIDs(t, rec.Code, rec.Body.String()); !reflect.DeepEqual(got, c.want) {
			t.Errorf("/items, %s (bbox %s): %v, want %v", c.name, c.bbox, got, c.want)
		}
		rec = atsDo(t, mux, "GET", "/collections/"+points+"/cube?z=0&bbox="+c.bbox, "")
		if got := geographicIDs(t, rec.Code, rec.Body.String()); !reflect.DeepEqual(got, c.want) {
			t.Errorf("/cube, %s (bbox %s): %v, want %v", c.name, c.bbox, got, c.want)
		}
	}

	// A geography of lines meets the box where its line does, not where its
	// bounding box does
	const lines = "geo_bbox_lines"
	defer atsDo(t, mux, "DELETE", "/collections/"+lines, "")
	line := post(lines, "tgeography", `{"type":"MovingGeometry","values":[{"type":"LineString",`+
		`"coordinates":[[1,1],[3,3]]},{"type":"LineString","coordinates":[[1,1],[3,3]]}],`+
		`"datetimes":["2001-01-01T00:00:00+00:00","2001-01-02T00:00:00+00:00"],"lower_inc":true,`+
		`"upper_inc":true,"interpolation":"Step"}`)[0]
	for _, c := range []struct {
		bbox string
		want []string
	}{
		{"1.9,1.9,2.1,2.1", []string{line}},
		{"2.5,1,3,1.5", []string{}},
	} {
		rec := atsDo(t, mux, "GET", "/collections/"+lines+"/items?bbox="+c.bbox, "")
		if got := geographicIDs(t, rec.Code, rec.Body.String()); !reflect.DeepEqual(got, c.want) {
			t.Errorf("/items over a tgeography, bbox %s: %v, want %v", c.bbox, got, c.want)
		}
	}

	// What is refused is answered as refused, and not as a page of no feature
	for _, path := range []string{
		"/collections/" + points + "/items?bbox=10,71,20,70",
		"/collections/" + points + "/cube?z=0&bbox=10,71,20,70",
		"/collections/" + points + "/items?datetime=2001-01-02T00:00:00Z/2001-01-01T00:00:00Z",
	} {
		if rec := atsDo(t, mux, "GET", path, ""); rec.Code != 400 {
			t.Errorf("%s answered %d, want 400: %s", path, rec.Code, rec.Body.String())
		}
	}
}
