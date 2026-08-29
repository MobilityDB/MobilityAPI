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
	"encoding/json"
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

// The interpolation a client may write is the set the standard names, and a name
// outside it is refused rather than carried to MobilityDB to fail there.
//
// ⛔ THE TWO REFUSALS ARE DIFFERENT ANSWERS. A name the standard admits and this
// tier does not carry is 501, because the request is understood and unserved; a
// name the standard does not admit is 400, because it is not a request at all.
// `Stepwise` is the second kind: it is the older MF-JSON encoding extension's
// word for the step function and Part 1 does not name it.
func TestATSInterpolationIsTheStandardsSet(t *testing.T) {
	for _, c := range []struct {
		in   string
		mdb  string
		code int
	}{
		{"Linear", "Linear", 0},
		{"Step", "Step", 0},
		{"Discrete", "Discrete", 0},
		{"Quadratic", "", 501},
		{"Cubic", "", 501},
		{"Regression", "", 501},
		{"Stepwise", "", 400},
		{"stepwise", "", 400},
		{"Bogus", "", 400},
	} {
		t.Run(c.in, func(t *testing.T) {
			got, err := mdbInterp(c.in)
			if c.code == 0 {
				if err != nil {
					t.Fatalf("%q is a name the standard admits: %v", c.in, err)
				}
				if got != c.mdb {
					t.Errorf("%q resolves to %q, want %q", c.in, got, c.mdb)
				}
				return
			}
			if err == nil {
				t.Fatalf("%q resolves to %q, want a refusal", c.in, got)
			}
			if s := errStatus(err, 400); s != c.code {
				t.Errorf("%q is refused with %d, want %d (%v)", c.in, s, c.code, err)
			}
		})
	}
}

// The refusal reaches a client through the body it writes, not only through the
// resolver, so a temporal property naming an interpolation the standard does not
// carry answers 501 rather than a MobilityDB parse error.
func TestATSInterpolationRefusalReachesTheClient(t *testing.T) {
	for _, c := range []struct {
		in   string
		code int
	}{{"Regression", 501}, {"Stepwise", 400}} {
		t.Run(c.in, func(t *testing.T) {
			body := `{"datetimes":["2026-01-01T00:00:00Z","2026-01-01T00:10:00Z"],` +
				`"values":[1,2],"interpolation":"` + c.in + `"}`
			var m map[string]any
			if err := json.Unmarshal([]byte(body), &m); err != nil {
				t.Fatal(err)
			}
			_, err := tPropMFJSON("MovingFloat", "Linear", m)
			if err == nil {
				t.Fatalf("%q is carried into the MF-JSON rather than refused", c.in)
			}
			if s := errStatus(err, 400); s != c.code {
				t.Errorf("%q reaches the client as %d, want %d (%v)", c.in, s, c.code, err)
			}
		})
	}
}
