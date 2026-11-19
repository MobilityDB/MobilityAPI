// The three abstract tests of Annex A as resolution 6 of the wiki page
// "Implementing OGC API — Moving Features in MobilityDB" corrects them, as
// executable rows beside the published ones in atsPart1.
//
// A published row is read as the standard writes it; a corrected row is read as
// the candidate resolution offered to the Standards Working Group writes it, and
// is discharged by the answer that corrected test method states. Each corrected
// row keeps the identifier of the abstract test it corrects, so the two readings
// of one test sit side by side and a correction never stands for a test Annex A
// does not hold.
//
// ⛔ A CORRECTED ROW IS EVIDENCE FOR THE RESOLUTION, NOT A CHANGE TO THE CLAIM.
// Conformance is still judged by atsPart1; these rows show the corrected methods
// can be run against an implementation and what it answers to them.
package main

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// atsCorrection is one abstract test of Annex A as resolution 6 corrects it.
type atsCorrection struct {
	id           string // the identifier of the abstract test it corrects, a row of atsPart1
	method, path string // the request the corrected test method issues, with sample identifiers
	purpose      string // the corrected test purpose, abridged
	dischargedBy string // the test that reads the answer it states
}

// atsPart1Corrected holds the corrections of resolution 6: A.20 validated against
// the schema its own Table A.7 cites, and A.26 and A.27 issuing the request for
// the resource each is named for, A.27 under each interpolation it names.
var atsPart1Corrected = []atsCorrection{
	{"/conf/movingfeatures/tgsequence-get-success", "GET", "/collections/ships/items/1/tgsequence",
		"type is TemporalGeometrySequence and geometrySequence holds TemporalPrimitiveGeometry items",
		"TestATSTGSequenceGetSuccess"},
	{"/conf/movingfeatures/tpgeometry-query-velocity", "GET", "/collections/ships/items/1/tgsequence/1/velocity",
		"a Velocity query at /velocity returns a TReal time-to-velocity curve",
		"TestATSTPGeometryQueryDistanceAndVelocity"},
	{"/conf/movingfeatures/tpgeometry-query-acceleration", "GET", "/collections/ships/items/1/tgsequence/1/acceleration",
		"an Acceleration query at /acceleration returns a TReal curve under an interpolation where one exists, " +
			"and states why none exists under every other",
		"TestATSCorrectedAccelerationByInterpolation"},
}

// Every corrected row corrects an abstract test Annex A holds, once.
func TestATSCorrectedRowsCorrectAnnexA(t *testing.T) {
	inAnnexA := map[string]bool{}
	for _, a := range atsPart1 {
		inAnnexA[a.id] = true
	}
	seen := map[string]bool{}
	for _, c := range atsPart1Corrected {
		if !inAnnexA[c.id] {
			t.Errorf("%s corrects no abstract test of Annex A", c.id)
		}
		if seen[c.id] {
			t.Errorf("%s is corrected twice", c.id)
		}
		seen[c.id] = true
	}
	if len(atsPart1Corrected) != 3 {
		t.Errorf("resolution 6 corrects 3 abstract tests (A.20, A.26, A.27); the registry holds %d",
			len(atsPart1Corrected))
	}
}

// The request each corrected method issues is one the service routes, and the
// routing pattern names the resource the test is named for rather than standing
// in for it: the velocity and acceleration requests reach the query route with
// their own query type, never /distance.
func TestATSCorrectedRoutes(t *testing.T) {
	mux := newMux()
	for _, c := range atsPart1Corrected {
		req := httptest.NewRequest(c.method, "http://mfapi"+c.path, nil)
		_, pattern := mux.Handler(req)
		if pattern == "" {
			t.Errorf("%s: %s %s is not routed", c.id, c.method, c.path)
			continue
		}
		if strings.HasSuffix(c.path, "/distance") {
			t.Errorf("%s: the corrected method still requests /distance", c.id)
		}
	}
}

// Every corrected row names a test that a test file declares, as
// #TestATSEveryAnsweredRowIsDischarged holds every published row stating a response.
func TestATSCorrectedRowsAreDischarged(t *testing.T) {
	files, err := filepath.Glob("*_test.go")
	if err != nil {
		t.Fatal(err)
	}
	var src []byte
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		src = append(src, b...)
	}
	for _, c := range atsPart1Corrected {
		if c.dischargedBy == "" {
			t.Errorf("%s names no test that reads its answer", c.id)
			continue
		}
		if !bytes.Contains(src, []byte("func "+c.dischargedBy+"(")) {
			t.Errorf("%s names %s, which no test file declares", c.id, c.dischargedBy)
		}
	}
}

// The corrected A.27, under each interpolation MobilityDB carries. None of them
// makes acceleration a curve, so each answers 501 with the reason for that
// interpolation; the standard's Quadratic and Cubic, under which the quantity is
// a curve, are no interpolation MEOS names, so no geometry the service holds can
// reach the 200 branch, and accelerationNotDerivable covering every name MEOS
// carries is what this test pins.
func TestATSCorrectedAccelerationByInterpolation(t *testing.T) {
	for _, c := range []struct {
		interp string
		says   []string
	}{
		{"Linear", []string{"derivab", "Step", "vertices"}},
		{"Step", []string{"derivab", "constant between its instants", "jumps"}},
		{"Discrete", []string{"derivab", "at its instants alone"}},
		{"None", []string{"derivab", "single instant"}},
	} {
		t.Run(c.interp, func(t *testing.T) {
			withBackend(atsQueryBackendInterp(c.interp), func() {
				rec := atsQuery(t, "acceleration")
				if rec.Code != 501 {
					t.Fatalf("status = %d, want 501 (body %s)", rec.Code, rec.Body.String())
				}
				var e struct {
					Description string `json:"description"`
				}
				if err := json.Unmarshal(rec.Body.Bytes(), &e); err != nil {
					t.Fatal(err)
				}
				for _, want := range c.says {
					if !strings.Contains(e.Description, want) {
						t.Errorf("the 501 for a %s geometry does not say %q: %s", c.interp, want, e.Description)
					}
				}
			})
		})
	}
	if len(accelerationNotDerivable) != 4 {
		t.Errorf("accelerationNotDerivable states %d interpolations; MEOS names 4 (None, Discrete, Step, Linear)",
			len(accelerationNotDerivable))
	}
}

// An interpolation MEOS does not name is an error of the service, not a 501
// stating a reason nobody wrote.
func TestATSCorrectedAccelerationUnknownInterpolation(t *testing.T) {
	withBackend(atsQueryBackendInterp("Cubic"), func() {
		if rec := atsQuery(t, "acceleration"); rec.Code != 500 {
			t.Errorf("an interpolation MEOS does not name answers %d, want 500", rec.Code)
		}
	})
}

// The acceleration of a geometry the service does not hold is 404, as for the
// other queries: the 501 states why an existing geometry has no acceleration, and
// answering it for a feature that does not exist would state that of nothing.
func TestATSCorrectedAccelerationOfNothingIsNotFound(t *testing.T) {
	f := atsCollectionsBackend()
	f.answers = append(f.answers, fakeAnswer{match: "SELECT numSequences(", rows: [][]any{{1}}})
	withBackend(f, func() {
		req := httptest.NewRequest("GET", "/collections/ships/items/1/tgsequence/7/acceleration", nil)
		req.SetPathValue("cid", "ships")
		req.SetPathValue("fid", "1")
		req.SetPathValue("tgid", "7")
		req.SetPathValue("qtype", "acceleration")
		rec := httptest.NewRecorder()
		tgSequenceQuery(rec, req)
		if rec.Code != 404 {
			t.Errorf("the acceleration of temporal geometry #7 of a feature holding 1 answers %d, want 404 (%s)",
				rec.Code, rec.Body.String())
		}
	})
}
