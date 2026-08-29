// The Annex A tests that need data: the lifecycles a `-success` test checks a
// status code for, run against a real backend through the tier's own routing
// table.
//
// They are skipped unless MFAPI_DSN names a database carrying the conformance
// fixture (tutorial/setup/load_conformance.sql), so the default `go test ./...`
// stays offline and the same file is what a CI job with a service container
// runs. Skipping is reported per abstract test, never silently.
//
// ⛔ EVERY LIFECYCLE RESTORES WHAT IT CHANGED. A conformance run is not allowed
// to leave the fixture altered, or the next test reads a database shaped by the
// previous one and a failure stops meaning what it says. Each test creates its
// own subject and removes it, and none touches the two features the read-side
// tests assert against.
package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// atsLiveMux opens the fixture database and returns the tier's routing table,
// or skips when no DSN is configured.
func atsLiveMux(t *testing.T) (*http.ServeMux, func()) {
	t.Helper()
	dsn := os.Getenv("MFAPI_DSN")
	if dsn == "" {
		t.Skip("needs the conformance fixture: set MFAPI_DSN to a database loaded " +
			"with tutorial/setup/load_conformance.sql")
	}
	b, err := openBackend(dsn)
	if err != nil {
		t.Fatalf("opening %s: %v", dsn, err)
	}
	prev := db
	db = b
	return newMux(), func() { db = prev; b.Close() }
}

func atsDo(t *testing.T, mux *http.ServeMux, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var r *http.Request
	if body == "" {
		r = httptest.NewRequest(method, path, nil)
	} else {
		r = httptest.NewRequest(method, path, strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, r)
	return rec
}

// oneOf reports whether the status is among those the abstract test admits.
func oneOf(code int, want ...int) bool {
	for _, w := range want {
		if code == w {
			return true
		}
	}
	return false
}

// /conf/mf-collection/collections-get-success and collection-get-success,
// against the fixture rather than a scripted backend.
func TestATSLiveCollections(t *testing.T) {
	mux, done := atsLiveMux(t)
	defer done()
	if rec := atsDo(t, mux, "GET", "/collections", ""); rec.Code != 200 {
		t.Errorf("GET /collections = %d, want 200", rec.Code)
	}
	if rec := atsDo(t, mux, "GET", "/collections/conformance", ""); rec.Code != 200 {
		t.Errorf("GET /collections/conformance = %d, want 200", rec.Code)
	}
}

// /conf/movingfeatures/features-get-success and mf-get-success.
func TestATSLiveFeatures(t *testing.T) {
	mux, done := atsLiveMux(t)
	defer done()
	if rec := atsDo(t, mux, "GET", "/collections/conformance/items", ""); rec.Code != 200 {
		t.Errorf("GET items = %d, want 200 (%s)", rec.Code, rec.Body.String())
	}
	if rec := atsDo(t, mux, "GET", "/collections/conformance/items/1", ""); rec.Code != 200 {
		t.Errorf("GET items/1 = %d, want 200 (%s)", rec.Code, rec.Body.String())
	}
}

// /conf/movingfeatures/tgsequence-get-success and the three tpgeometry
// queries, against real trajectories.
func TestATSLiveTemporalGeometry(t *testing.T) {
	mux, done := atsLiveMux(t)
	defer done()
	if rec := atsDo(t, mux, "GET", "/collections/conformance/items/1/tgsequence", ""); rec.Code != 200 {
		t.Errorf("GET tgsequence = %d, want 200 (%s)", rec.Code, rec.Body.String())
	}
	for _, q := range []string{"distance", "velocity"} {
		if rec := atsDo(t, mux, "GET", "/collections/conformance/items/1/tgsequence/1/"+q, ""); rec.Code != 200 {
			t.Errorf("GET %s = %d, want 200 (%s)", q, rec.Code, rec.Body.String())
		}
	}
	// Acceleration is not derivable under Linear interpolation; the tier says so.
	if rec := atsDo(t, mux, "GET", "/collections/conformance/items/1/tgsequence/1/acceleration", ""); rec.Code != 501 {
		t.Errorf("GET acceleration = %d, want 501", rec.Code)
	}
}

// /conf/movingfeatures/tproperties-get-success and tproperty-get-success.
func TestATSLiveTemporalProperties(t *testing.T) {
	mux, done := atsLiveMux(t)
	defer done()
	if rec := atsDo(t, mux, "GET", "/collections/conformance/items/1/tproperties", ""); rec.Code != 200 {
		t.Errorf("GET tproperties = %d, want 200 (%s)", rec.Code, rec.Body.String())
	}
	for _, p := range []string{"speed", "heading", "status", "anchored"} {
		if rec := atsDo(t, mux, "GET", "/collections/conformance/items/1/tproperties/"+p, ""); rec.Code != 200 {
			t.Errorf("GET tproperties/%s = %d, want 200 (%s)", p, rec.Code, rec.Body.String())
		}
	}
}

// /conf/mf-collection/collections-post-success, collections-put-success and
// collections-delete-success: the collection lifecycle, on a collection this
// test creates so the fixture is untouched.
func TestATSLiveCollectionLifecycle(t *testing.T) {
	mux, done := atsLiveMux(t)
	defer done()
	const cid = "ats_lifecycle"
	defer atsDo(t, mux, "DELETE", "/collections/"+cid, "")

	rec := atsDo(t, mux, "POST", "/collections",
		`{"id":"`+cid+`","title":"lifecycle","description":"created by the conformance suite","itemType":"movingfeature","crs":25832}`)
	if !oneOf(rec.Code, 201, 202) {
		t.Fatalf("POST /collections = %d, want 201 or 202 (%s)", rec.Code, rec.Body.String())
	}
	rec = atsDo(t, mux, "PUT", "/collections/"+cid,
		`{"title":"lifecycle renamed","description":"replaced by the conformance suite","itemType":"movingfeature","crs":25832}`)
	if !oneOf(rec.Code, 200, 202, 204) {
		t.Errorf("PUT /collections/%s = %d, want 200, 202 or 204 (%s)", cid, rec.Code, rec.Body.String())
	}
	rec = atsDo(t, mux, "DELETE", "/collections/"+cid, "")
	if !oneOf(rec.Code, 200, 202, 204) {
		t.Errorf("DELETE /collections/%s = %d, want 200, 202 or 204 (%s)", cid, rec.Code, rec.Body.String())
	}
	if rec := atsDo(t, mux, "GET", "/collections/"+cid, ""); rec.Code != 404 {
		t.Errorf("the deleted collection answers %d, want 404", rec.Code)
	}
}

// /conf/movingfeatures/tproperty-post-success and tproperty-delete-success: a
// property this test adds to the fixture's first feature and removes again.
func TestATSLiveTemporalPropertyLifecycle(t *testing.T) {
	mux, done := atsLiveMux(t)
	defer done()
	const base = "/collections/conformance/items/1/tproperties"
	const name = "ats_probe"
	defer atsDo(t, mux, "DELETE", base+"/"+name, "")

	rec := atsDo(t, mux, "POST", base,
		`[{"name":"`+name+`","type":"TReal","form":"m","description":"added by the conformance suite",`+
			`"datetimes":["2026-01-01T08:00:00+00","2026-01-01T08:10:00+00"],"values":[1.0,2.0],"interpolation":"Linear"}]`)
	if !oneOf(rec.Code, 200, 201, 202) {
		t.Fatalf("POST %s = %d, want 201 or 202 (%s)", base, rec.Code, rec.Body.String())
	}
	if rec := atsDo(t, mux, "GET", base+"/"+name, ""); rec.Code != 200 {
		t.Errorf("the created property reads %d, want 200 (%s)", rec.Code, rec.Body.String())
	}
	rec = atsDo(t, mux, "DELETE", base+"/"+name, "")
	if !oneOf(rec.Code, 200, 202, 204) {
		t.Errorf("DELETE %s/%s = %d, want 200, 202 or 204 (%s)", base, name, rec.Code, rec.Body.String())
	}
	if rec := atsDo(t, mux, "GET", base+"/"+name, ""); rec.Code != 404 {
		t.Errorf("the deleted property answers %d, want 404", rec.Code)
	}
}

// The fixture is unchanged by the run: the two features and their four
// properties are still what the read-side tests assert against.
func TestATSLiveFixtureIsRestored(t *testing.T) {
	mux, done := atsLiveMux(t)
	defer done()
	if rec := atsDo(t, mux, "GET", "/collections/conformance/items/1/tproperties", ""); rec.Code != 200 {
		t.Fatalf("tproperties = %d, want 200", rec.Code)
	} else if n := strings.Count(rec.Body.String(), `"name"`); n != 4 {
		t.Errorf("the fixture carries %d properties, want the original 4", n)
	}
	if rec := atsDo(t, mux, "GET", "/collections/conformance/items/2", ""); rec.Code != 200 {
		t.Errorf("the second feature reads %d, want 200", rec.Code)
	}
}
