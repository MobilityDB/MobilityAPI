// The three TemporalPrimitiveGeometry queries of Annex A, and the evidence
// behind two proposed corrections to it.
//
// ⛔ ANNEX A SENDS ALL THREE TO THE SAME ENDPOINT. tpgeometry-query-distance,
// -velocity and -acceleration each say "Issue an HTTP GET request to the URL
// {root}/collections/{collectionId}/items/{mFeatureId}/tgsequence/{tGeometryId}
// /distance", so the velocity and acceleration tests never exercise the
// resource they name. The three are distinct quantities off one trajectory --
// cumulativeLength, speed, and the derivative of speed -- and the tests below
// show them answering separately, which is what a corrected Annex A would ask
// for.
//
// ⛔ AND ACCELERATION IS NOT A TYPO ALONE: Annex A wants a TReal
// time-to-acceleration curve, while the standard's own Linear interpolation
// makes position piecewise-linear, so speed is piecewise-CONSTANT and its
// derivative is zero inside every segment and undefined at each vertex. There
// is no curve to return. The tier answers 501 and says so, which is the
// position this file records: a corrected Annex A either scopes the
// acceleration test to interpolations where the quantity exists, or admits 501
// for those where it does not.
package main

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
)

// atsQueryBackend answers what tgSequenceQuery reads, with an MF-JSON moving
// float standing in for the derived curve MobilityDB computes.
func atsQueryBackend() *fakeBackend {
	const mffloat = `{"type":"MovingFloat","datetimes":["2026-01-01T00:00:00+00","2026-01-01T00:10:00+00"],` +
		`"values":[0,1234.5],"interpolation":"Linear"}`
	f := atsCollectionsBackend()
	f.answers = append(f.answers,
		fakeAnswer{match: "SELECT numSequences(", rows: [][]any{{1}}},
		fakeAnswer{match: "SELECT asMFJSON(", rows: [][]any{{mffloat}}},
	)
	return f
}

func atsQuery(t *testing.T, qtype string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest("GET", "/collections/ships/items/1/tgsequence/1/"+qtype, nil)
	req.SetPathValue("cid", "ships")
	req.SetPathValue("fid", "1")
	req.SetPathValue("tgid", "1")
	req.SetPathValue("qtype", qtype)
	rec := httptest.NewRecorder()
	tgSequenceQuery(rec, req)
	return rec
}

// /conf/movingfeatures/tpgeometry-query-distance and -velocity: each is served
// at ITS OWN endpoint and returns the TReal the abstract test requires.
func TestATSTPGeometryQueryDistanceAndVelocity(t *testing.T) {
	withBackend(atsQueryBackend(), func() {
		for _, q := range []string{"distance", "velocity"} {
			t.Run(q, func(t *testing.T) {
				rec := atsQuery(t, q)
				if rec.Code != 200 {
					t.Fatalf("%s: status = %d, want 200 (body %s)", q, rec.Code, rec.Body.String())
				}
				var doc struct {
					Name string `json:"name"`
					Type string `json:"type"`
					Form string `json:"form"`
				}
				if err := json.Unmarshal(rec.Body.Bytes(), &doc); err != nil {
					t.Fatal(err)
				}
				if doc.Type != "TReal" {
					t.Errorf("%s: type = %q, want TReal", q, doc.Type)
				}
				if doc.Name != q {
					t.Errorf("%s: name = %q, want the query it answers", q, doc.Name)
				}
			})
		}
	})
}

// The two queries answer DIFFERENT quantities, which is what makes Annex A's
// shared `/distance` URL a defect rather than a harmless duplication: velocity
// carries m/s and distance carries m, off the same trajectory.
func TestATSTPGeometryQueriesAreDistinct(t *testing.T) {
	if tProps["velocity"].expr == tProps["distance"].expr {
		t.Fatalf("velocity and distance derive from the same expression %q", tProps["velocity"].expr)
	}
	if tProps["velocity"].uom == tProps["distance"].uom {
		t.Errorf("velocity and distance report the same unit %q", tProps["velocity"].uom)
	}
	withBackend(atsQueryBackend(), func() {
		var forms []string
		for _, q := range []string{"distance", "velocity"} {
			var doc struct {
				Form string `json:"form"`
			}
			if err := json.Unmarshal(atsQuery(t, q).Body.Bytes(), &doc); err != nil {
				t.Fatal(err)
			}
			forms = append(forms, doc.Form)
		}
		if forms[0] == forms[1] {
			t.Errorf("both queries report the unit %q, so one endpoint cannot stand for both", forms[0])
		}
	})
}

// /conf/movingfeatures/tpgeometry-query-acceleration: under the Linear
// interpolation the standard defines, acceleration has no curve to return, and
// the tier says so rather than inventing zeros. This test pins the position
// offered to OGC; it changes only if the standard scopes the query.
func TestATSTPGeometryQueryAccelerationNotDerivable(t *testing.T) {
	withBackend(atsQueryBackend(), func() {
		rec := atsQuery(t, "acceleration")
		if rec.Code != 501 {
			t.Fatalf("acceleration: status = %d, want 501 (not implemented for this motion model)", rec.Code)
		}
		var e struct {
			Description string `json:"description"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &e); err != nil {
			t.Fatal(err)
		}
		// The reason is the substance of the report: a bare 501 would say
		// "unfinished", and this one says "not derivable, and here is why".
		for _, want := range []string{"derivab", "Step", "vertices"} {
			if !strings.Contains(e.Description, want) {
				t.Errorf("the 501 does not explain %q: %s", want, e.Description)
			}
		}
	})
}

// An unknown query type is not silently treated as one of the three.
func TestATSTPGeometryQueryUnknownType(t *testing.T) {
	withBackend(atsQueryBackend(), func() {
		if rec := atsQuery(t, "jerk"); rec.Code != 404 {
			t.Errorf("an unknown query type answers %d, want 404", rec.Code)
		}
	})
}
