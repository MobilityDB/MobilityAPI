// A temporal property's unit of measure, on the way in and on the way out.
//
// /components/schemas/temporalProperty gives `form` two branches and nothing else: an
// absolute URI, or a string of exactly three characters. That excludes the symbol of
// the metre, of the second, and `km/h` — most unit symbols in ordinary use — so a unit
// reaches a conformant document as a register URI or not at all.
//
// The check runs at the point of storage rather than only at the point of publication,
// because a unit stored here that no conformant document can carry would be published
// unconformantly or silently dropped, and a caller writing it can still fix it.
package main

import (
	"net/http/httptest"
	"strings"
	"testing"
)

// The predicate is the schema's two branches and nothing more.
func TestATSFormIsWhatTheSchemaAdmits(t *testing.T) {
	for _, c := range []struct {
		uom  string
		want bool
		why  string
	}{
		{"", true, "no unit at all: the member is optional and is omitted"},
		{"deg", true, "exactly three characters"},
		{"m/s", true, "exactly three characters"},
		{uomURI + "m", true, "an absolute URI"},
		{uomURI + "km_h-1", true, "an absolute URI"},
		{"m", false, "the metre's own symbol is one character"},
		{"s", false, "the second's own symbol is one character"},
		{"km/h", false, "four characters and not a URI"},
		{"m/s^2", false, "five characters and not a URI"},
		{"/def/uom/UCUM/0/m", false, "a relative reference is not an absolute URI"},
	} {
		if got := conformantForm(c.uom); got != c.want {
			t.Errorf("conformantForm(%q) = %v, want %v — %s", c.uom, got, c.want, c.why)
		}
	}
}

// A unit no conformant document can carry is refused where the caller can still fix it.
func TestATSFormRefusedOnWrite(t *testing.T) {
	f := atsCollectionsBackend()
	f.answers = append(f.answers,
		fakeAnswer{match: "SELECT 1 FROM", rows: [][]any{{1}}},
		fakeAnswer{match: "to_regclass('mf_tproperty')", rows: [][]any{{"mf_tproperty"}}},
	)
	withBackend(f, func() {
		body := `[{"name":"speed","type":"TReal","form":"km/h",` +
			`"datetimes":["2026-01-01T08:00:00+00","2026-01-01T08:10:00+00"],` +
			`"values":[1.0,2.0],"interpolation":"Linear"}]`
		r := httptest.NewRequest("POST", "/collections/ships/items/1/tproperties", strings.NewReader(body))
		r.SetPathValue("cid", "ships")
		r.SetPathValue("fid", "1")
		r.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		postTProperties(rec, r)
		if rec.Code != 400 {
			t.Fatalf("a unit the schema cannot express is stored with status %d, want 400 (%s)",
				rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), "km/h") {
			t.Errorf("the refusal does not name the unit it refuses: %s", rec.Body.String())
		}
	})
}
