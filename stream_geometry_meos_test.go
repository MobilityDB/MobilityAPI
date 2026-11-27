//go:build meos

package main

// Mirrors #TestMeosWindowAggregate of stream_meos_test.go for the window
// aggregations of a geometry continuous query (#geometryAggregations), over
// the conformance fixture: Alpha's four positions in UTM 32N, at 08:00, 08:10,
// 08:25 and 08:45, whose segments measure 1118.034, 1581.139 and 1802.776 m.

import (
	"encoding/json"
	"math"
	"net/http"
	"strings"
	"testing"
	"time"
)

// geometryWindowEvents registers a geometry query with body on the feature
// path and answers its events keyed by window start, read until every one of
// want windows is seen; the stream loops, so none is missed.
func geometryWindowEvents(t *testing.T, path, body string, want int) map[string]Event {
	t.Helper()
	mux, done := atsLiveMux(t)
	defer done()
	rec := atsDo(t, mux, "POST", path, body)
	if rec.Code != 201 {
		t.Fatalf("POST %s %s = %d (%s)", path, body, rec.Code, rec.Body.String())
	}
	var link struct {
		QueryID string `json:"queryId"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &link); err != nil {
		t.Fatal(err)
	}
	cq := streamReg.get(link.QueryID)
	defer func() { cq.stop(); streamReg.remove(cq.id) }()
	ch := cq.subscribe()
	defer cq.unsubscribe(ch)
	out := map[string]Event{}
	deadline := time.After(20 * time.Second)
	for len(out) < want {
		select {
		case ev, ok := <-ch:
			if !ok {
				t.Fatalf("the query ended after %d of %d windows (status %s)", len(out), want, cq.status())
			}
			start, _ := ev["windowStart"].(string)
			out[start] = ev
		case <-deadline:
			t.Fatalf("%d of %d windows read in 20 s", len(out), want)
		}
	}
	return out
}

func num(t *testing.T, ev Event) float64 {
	t.Helper()
	switch v := ev["value"].(type) {
	case float64:
		return v
	case int:
		return float64(v)
	}
	t.Fatalf("the value of %v is no number", ev)
	return 0
}

func TestLiveGeometryWindowAggregates(t *testing.T) {
	const alpha = "/collections/conformance/items/1/tgsequence/queries"
	const close = 0.001

	// COUNT windows of two instants: 08:00-08:10 and 08:25-08:45
	evs := geometryWindowEvents(t, alpha,
		`{"aggregation":"LENGTH","window":{"type":"COUNT","size":2},"intervalMs":20}`, 2)
	for start, want := range map[string]float64{"2026-01-01T08:00:00Z": 1118.034, "2026-01-01T08:25:00Z": 1802.776} {
		if ev, ok := evs[start]; !ok || math.Abs(num(t, ev)-want) > close || ev["count"] != 2 {
			t.Errorf("LENGTH of the COUNT window at %s = %v, want %.3f over 2 instants", start, ev, want)
		}
	}

	// TUMBLING windows of 30 minutes restrict the trajectory at 08:30, a quarter
	// of the way along its last segment: 1118.034 + 1581.139 + 450.694 before it,
	// 1352.082 after it
	evs = geometryWindowEvents(t, alpha,
		`{"aggregation":"LENGTH","window":{"type":"TUMBLING","size":30,"unit":"MINUTES"},"intervalMs":20}`, 2)
	for start, want := range map[string][2]float64{
		"2026-01-01T08:00:00Z": {3149.867, 3}, "2026-01-01T08:30:00Z": {1352.082, 1}} {
		if ev, ok := evs[start]; !ok || math.Abs(num(t, ev)-want[0]) > close || ev["count"] != int(want[1]) {
			t.Errorf("LENGTH of the TUMBLING window at %s = %v, want %.3f over %v instants", start, ev, want[0], want[1])
		}
	}

	// INTERSECT, a region about Alpha's last position, met in the second window alone
	var region string
	_, done := atsLiveMux(t)
	err := db.QueryRow(t.Context(), "SELECT ST_AsText(ST_Transform(ST_MakeEnvelope(578500, 6221500, "+
		"579500, 6222500, 25832), 4326))").Scan(&region)
	done()
	if err != nil {
		t.Fatal(err)
	}
	evs = geometryWindowEvents(t, alpha, `{"aggregation":"INTERSECT","region":"`+region+
		`","window":{"type":"TUMBLING","size":30,"unit":"MINUTES"},"intervalMs":20}`, 2)
	if evs["2026-01-01T08:00:00Z"]["value"] != false || evs["2026-01-01T08:30:00Z"]["value"] != true {
		t.Errorf("INTERSECT = %v, want false then true", evs)
	}

	// TO_TRAJ, the path of the window in WGS 84
	evs = geometryWindowEvents(t, alpha,
		`{"aggregation":"TO_TRAJ","window":{"type":"COUNT","size":2},"intervalMs":20}`, 2)
	var path struct {
		Type        string      `json:"type"`
		Coordinates [][]float64 `json:"coordinates"`
	}
	raw, _ := json.Marshal(evs["2026-01-01T08:00:00Z"]["value"])
	if json.Unmarshal(raw, &path) != nil || path.Type != "LineString" || len(path.Coordinates) != 2 ||
		path.Coordinates[0][0] < 9 || path.Coordinates[0][0] > 11 {
		t.Errorf("TO_TRAJ = %s, want a LineString of two positions in longitude and latitude", raw)
	}
}

// Lines and polygons measure what they cover during the window, each overlap
// counted once: two overlapping 2 x 2 squares cover 7, two lines 0-2 and 1-3
// cover 3. Their space-time volume is the integral of their area over the
// window's time, in seconds.
func TestLiveGeometryWindowShapes(t *testing.T) {
	// withMux runs f against a live mux, released when f returns, so that
	// #geometryWindowEvents opens its own in between
	withMux := func(f func(mux *http.ServeMux)) {
		mux, done := atsLiveMux(t)
		defer done()
		f(mux)
	}
	post := func(mux *http.ServeMux, cid, values string) {
		t.Helper()
		atsDo(t, mux, "DELETE", "/collections/"+cid, "")
		if rec := atsDo(t, mux, "POST", "/collections",
			`{"id":"`+cid+`","temporalGeometryType":"tgeometry","crs":3857}`); rec.Code != 201 {
			t.Fatalf("POST /collections %s = %d (%s)", cid, rec.Code, rec.Body.String())
		}
		if rec := atsDo(t, mux, "POST", "/collections/"+cid+"/items", `{"id":1,"properties":{},`+
			`"temporalGeometry":{"type":"MovingGeometry","values":`+values+`,"datetimes":`+
			`["2001-01-01T00:00:00+00:00","2001-01-01T00:10:00+00:00"],"lower_inc":true,"upper_inc":true,`+
			`"interpolation":"Step"}}`); !oneOf(rec.Code, 201, 202) {
			t.Fatalf("POST a feature of %s = %d (%s)", cid, rec.Code, rec.Body.String())
		}
	}
	withMux(func(mux *http.ServeMux) {
		post(mux, "geo_win_polys", `[{"type":"Polygon","coordinates":[[[0,0],[2,0],[2,2],[0,2],[0,0]]]},`+
			`{"type":"Polygon","coordinates":[[[1,1],[3,1],[3,3],[1,3],[1,1]]]}]`)
		post(mux, "geo_win_lines", `[{"type":"LineString","coordinates":[[0,0],[2,0]]},`+
			`{"type":"LineString","coordinates":[[1,0],[3,0]]}]`)
	})
	defer withMux(func(mux *http.ServeMux) {
		atsDo(t, mux, "DELETE", "/collections/geo_win_polys", "")
		atsDo(t, mux, "DELETE", "/collections/geo_win_lines", "")
	})
	for _, c := range []struct {
		cid, agg string
		want     float64
	}{
		{"geo_win_polys", "AREA", 7},
		{"geo_win_lines", "LENGTH", 3},
		// The first square is held for the 600 seconds up to the second, which
		// holds no time: 4 x 600 square units times seconds
		{"geo_win_polys", "VOLUME", 2400},
		{"geo_win_lines", "VOLUME", 0},
	} {
		evs := geometryWindowEvents(t, "/collections/"+c.cid+"/items/1/tgsequence/queries",
			`{"aggregation":"`+c.agg+`","window":{"type":"COUNT","size":2},"intervalMs":20}`, 1)
		if ev := evs["2001-01-01T00:00:00Z"]; ev == nil || math.Abs(num(t, ev)-c.want) > 1e-9 {
			t.Errorf("%s of %s = %v, want %v", c.agg, c.cid, ev, c.want)
		}
	}

	// What is refused, and why
	withMux(func(mux *http.ServeMux) {
		for _, c := range []struct{ path, body, says string }{
			{"/collections/conformance/items/1/tgsequence/queries",
				`{"aggregation":"AREA","window":{"type":"COUNT","size":2}}`, "not defined"},
			{"/collections/conformance/items/1/tgsequence/queries",
				`{"aggregation":"VOLUME","window":{"type":"COUNT","size":2}}`, "no area"},
			{"/collections/conformance/items/1/tgsequence/queries",
				`{"aggregation":"INTERSECT","window":{"type":"COUNT","size":2}}`, "needs a region"},
			{"/collections/conformance/items/1/tgsequence/queries",
				`{"aggregation":"INTERSECT","region":"POLYGON((","window":{"type":"COUNT","size":2}}`, "no WKT"},
			{"/collections/conformance/items/1/tgsequence/queries",
				`{"aggregation":"LENGTH","window":{"type":"WEEKLY","size":2}}`, "window type"},
		} {
			rec := atsDo(t, mux, "POST", c.path, c.body)
			if rec.Code != 400 || !strings.Contains(rec.Body.String(), c.says) {
				t.Errorf("POST %s %s = %d (%s), want 400 saying %q", c.path, c.body, rec.Code,
					rec.Body.String(), c.says)
			}
		}
	})
}
