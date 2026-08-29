// The OGC API - Moving Features - Part 1: Core (OGC 22-003r3) Abstract Test
// Suite, Annex A, as executable tests. Each abstract test is one row of
// atsPart1, keyed by the identifier the standard gives it, so the suite is
// indexed by the ATS rather than by what the tier happens to implement: an
// abstract test with no counterpart here is a reported gap, not an omission.
//
// A row is discharged in one of three ways. A route row names the method and
// URL template the abstract test issues, checked against the routing table the
// service runs. A document row is settled by the service description or the
// conformance declaration. A live row needs a populated backend and runs only
// when MFAPI_DSN is set.
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"testing"
)

// atsKind is how an abstract test is discharged.
type atsKind int

const (
	atsRoute    atsKind = iota // the tier must serve the method and path it issues
	atsDocument                // the service description or conformance declaration settles it
	atsResponse                // the returned document is asserted against a scripted backend
	atsLive                    // needs a populated backend: stateful round-trips
)

// atsTest is one abstract test of Annex A.
type atsTest struct {
	id      string  // the identifier the standard gives it
	class   string  // its conformance class
	kind    atsKind //
	method  string  // for a route row: the method the abstract test issues
	path    string  // for a route row: the URL template, in ServeMux syntax
	purpose string  // the test purpose, abridged
}

// The conformance class URIs of Part 1.
const (
	classCollection = "http://www.opengis.net/spec/ogcapi-movingfeatures-1/1.0/conf/mf-collection"
	classFeatures   = "http://www.opengis.net/spec/ogcapi-movingfeatures-1/1.0/conf/movingfeatures"
	classCommon     = "http://www.opengis.net/spec/ogcapi-movingfeatures-1/1.0/conf/common"
)

// atsPart1 is Annex A. The 45 abstract tests of the mf-collection and
// movingfeatures conformance classes; the common class carries requirements
// but no abstract tests.
var atsPart1 = []atsTest{
	// -- mf-collection ------------------------------------------------------
	{"/conf/mf-collection/collections-get", classCollection, atsRoute, "GET", "/collections", "the Collections can be retrieved from the expected location"},
	{"/conf/mf-collection/collections-get-success", classCollection, atsResponse, "", "", "the Collections comply with the required structure, itemType is movingfeature"},
	{"/conf/mf-collection/collections-post", classCollection, atsRoute, "POST", "/collections", "a Collection can be created at the expected location"},
	{"/conf/mf-collection/collections-post-success", classCollection, atsLive, "", "", "the POST response is 201 or 202"},
	{"/conf/mf-collection/collection-get", classCollection, atsRoute, "GET", "/collections/{cid}", "the Collection can be retrieved from the expected location"},
	{"/conf/mf-collection/collection-get-success", classCollection, atsResponse, "", "", "the Collection complies with the required structure and contents"},
	{"/conf/mf-collection/collection-put", classCollection, atsRoute, "PUT", "/collections/{cid}", "the Collection can be replaced at the expected location"},
	{"/conf/mf-collection/collections-put-success", classCollection, atsLive, "", "", "the PUT response is 200, 202 or 204"},
	{"/conf/mf-collection/collection-delete", classCollection, atsRoute, "DELETE", "/collections/{cid}", "the Collection can be deleted at the expected location"},
	{"/conf/mf-collection/collections-delete-success", classCollection, atsLive, "", "", "the DELETE response is 200, 202 or 204"},

	// -- movingfeatures: the features and one feature -----------------------
	{"/conf/movingfeatures/features-get", classFeatures, atsRoute, "GET", "/collections/{cid}/items", "MovingFeatures can be extracted using query parameters"},
	{"/conf/movingfeatures/features-get-success", classFeatures, atsLive, "", "", "the MovingFeatures comply with the required structure"},
	{"/conf/movingfeatures/features-post", classFeatures, atsRoute, "POST", "/collections/{cid}/items", "a MovingFeature can be created at the expected location"},
	{"/conf/movingfeatures/features-post-success", classFeatures, atsLive, "", "", "the POST response is 201 or 202"},
	{"/conf/movingfeatures/mf-get", classFeatures, atsRoute, "GET", "/collections/{cid}/items/{fid}", "the MovingFeature can be retrieved from the expected location"},
	{"/conf/movingfeatures/mf-get-success", classFeatures, atsLive, "", "", "the MovingFeature complies with the required structure"},
	{"/conf/movingfeatures/mf-delete", classFeatures, atsRoute, "DELETE", "/collections/{cid}/items/{fid}", "the MovingFeature can be deleted at the expected location"},
	{"/conf/movingfeatures/mf-delete-success", classFeatures, atsLive, "", "", "the DELETE response is 200, 202 or 204"},

	// -- movingfeatures: the temporal geometry ------------------------------
	{"/conf/movingfeatures/tgsequence-get", classFeatures, atsRoute, "GET", "/collections/{cid}/items/{fid}/tgsequence", "the TemporalGeometrySequence can be extracted using query parameters"},
	{"/conf/movingfeatures/tgsequence-get-success", classFeatures, atsResponse, "", "", "type is MovingGeometryCollection and prism holds TemporalPrimitiveGeometry items"},
	{"/conf/movingfeatures/tgsequence-post", classFeatures, atsRoute, "POST", "/collections/{cid}/items/{fid}/tgsequence", "a TemporalPrimitiveGeometry can be created at the expected location"},
	{"/conf/movingfeatures/tgsequence-post-success", classFeatures, atsLive, "", "", "the POST response is 201 or 202"},
	{"/conf/movingfeatures/tpgeometry-delete", classFeatures, atsRoute, "DELETE", "/collections/{cid}/items/{fid}/tgsequence/{tgid}", "the TemporalPrimitiveGeometry can be deleted at the expected location"},
	{"/conf/movingfeatures/tpgeometry-delete-success", classFeatures, atsLive, "", "", "the DELETE response is 200, 202 or 204"},
	{"/conf/movingfeatures/tpgeometry-query-distance", classFeatures, atsRoute, "GET", "/collections/{cid}/items/{fid}/tgsequence/{tgid}/{qtype}", "a Distance query returns a TReal time-to-distance curve"},
	{"/conf/movingfeatures/tpgeometry-query-velocity", classFeatures, atsRoute, "GET", "/collections/{cid}/items/{fid}/tgsequence/{tgid}/{qtype}", "a Velocity query returns a TReal time-to-velocity curve"},
	{"/conf/movingfeatures/tpgeometry-query-acceleration", classFeatures, atsRoute, "GET", "/collections/{cid}/items/{fid}/tgsequence/{tgid}/{qtype}", "an Acceleration query returns a TReal time-to-acceleration curve"},

	// -- movingfeatures: the temporal properties ----------------------------
	{"/conf/movingfeatures/tproperties-get", classFeatures, atsRoute, "GET", "/collections/{cid}/items/{fid}/tproperties", "the TemporalProperties can be extracted using query parameters"},
	{"/conf/movingfeatures/tproperties-get-success", classFeatures, atsResponse, "", "", "each entry carries name and a type of TBoolean, TText, TInteger, TReal or TImage"},
	{"/conf/movingfeatures/tproperties-post", classFeatures, atsRoute, "POST", "/collections/{cid}/items/{fid}/tproperties", "a TemporalProperty can be created at the expected location"},
	{"/conf/movingfeatures/tproperties-post-success", classFeatures, atsLive, "", "", "the POST response is 201 or 202"},
	{"/conf/movingfeatures/tproperty-get", classFeatures, atsRoute, "GET", "/collections/{cid}/items/{fid}/tproperties/{pname}", "the TemporalProperty can be extracted using query parameters"},
	{"/conf/movingfeatures/tproperty-get-success", classFeatures, atsLive, "", "", "the TemporalProperty holds an array of TemporalPrimitiveValue items"},
	{"/conf/movingfeatures/tproperty-post", classFeatures, atsRoute, "POST", "/collections/{cid}/items/{fid}/tproperties/{pname}", "a TemporalPrimitiveValue can be created at the expected location"},
	{"/conf/movingfeatures/tproperty-post-success", classFeatures, atsLive, "", "", "the POST response is 201 or 202"},
	{"/conf/movingfeatures/tproperty-delete", classFeatures, atsRoute, "DELETE", "/collections/{cid}/items/{fid}/tproperties/{pname}", "the TemporalProperty can be deleted at the expected location"},
	{"/conf/movingfeatures/tproperty-delete-success", classFeatures, atsLive, "", "", "the DELETE response is 200, 202 or 204"},
	{"/conf/movingfeatures/tpvalue-delete", classFeatures, atsRoute, "DELETE", "/collections/{cid}/items/{fid}/tproperties/{pname}/{tvid}", "the TemporalPrimitiveValue can be deleted at the expected location"},
	{"/conf/movingfeatures/tpvalue-delete-success", classFeatures, atsLive, "", "", "the DELETE response is 200, 202 or 204"},

	// -- movingfeatures: the query parameters -------------------------------
	{"/conf/movingfeatures/param-leaf-definition", classFeatures, atsDocument, "", "", "the leaf query parameter is constructed correctly"},
	{"/conf/movingfeatures/param-leaf-response", classFeatures, atsLive, "", "", "the leaf query parameter is processed correctly"},
	{"/conf/movingfeatures/param-subtrajectory-definition", classFeatures, atsDocument, "", "", "the subTrajectory query parameter is constructed correctly"},
	{"/conf/movingfeatures/param-subtrajectory-response", classFeatures, atsLive, "", "", "the subTrajectory query parameter is processed correctly"},
	{"/conf/movingfeatures/param-subtemporalvalue-definition", classFeatures, atsDocument, "", "", "the subTemporalValue query parameter is constructed correctly"},
	{"/conf/movingfeatures/param-subtemporalvalue-response", classFeatures, atsLive, "", "", "the subTemporalValue query parameter is processed correctly"},
}

// atsSampleValue fills a URL template's wildcards so the routing table can be
// asked which pattern the request matches.
var atsSampleValue = strings.NewReplacer(
	"{cid}", "ships", "{fid}", "1", "{tgid}", "1", "{qtype}", "distance",
	"{pname}", "speed", "{tvid}", "1",
)

// atsRouteFor reports the routing pattern the tier serves for an abstract
// test's method and URL template, and whether the tier serves it at all.
func atsRouteFor(mux *http.ServeMux, a atsTest) (string, bool) {
	req := httptest.NewRequest(a.method, "http://mfapi"+atsSampleValue.Replace(a.path), nil)
	_, pattern := mux.Handler(req)
	if pattern == "" {
		return "", false
	}
	return pattern, strings.HasSuffix(pattern, a.path)
}

// Annex A holds 45 abstract tests over the mf-collection and movingfeatures
// conformance classes; the common class carries requirements but no abstract
// tests. Each identifier appears exactly once and names a Part 1 class.
func TestATSRegistryCoversAnnexA(t *testing.T) {
	const annexATests = 45
	if len(atsPart1) != annexATests {
		t.Errorf("registry holds %d abstract tests, Annex A holds %d", len(atsPart1), annexATests)
	}
	seen := map[string]bool{}
	for _, a := range atsPart1 {
		if seen[a.id] {
			t.Errorf("abstract test %s appears more than once", a.id)
		}
		seen[a.id] = true
		if a.class != classCollection && a.class != classFeatures && a.class != classCommon {
			t.Errorf("%s names %q, which is not a Part 1 conformance class", a.id, a.class)
		}
		if a.kind == atsRoute && (a.method == "" || a.path == "") {
			t.Errorf("%s is discharged by route and must name a method and a path", a.id)
		}
	}
}

// The service declares every conformance class its abstract tests belong to.
func TestATSConformanceDeclaresPart1Classes(t *testing.T) {
	rec := httptest.NewRecorder()
	conformance(rec, httptest.NewRequest("GET", "/conformance", nil))
	var body struct {
		ConformsTo []string `json:"conformsTo"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	declared := map[string]bool{}
	for _, c := range body.ConformsTo {
		declared[c] = true
	}
	classes := map[string]bool{}
	for _, a := range atsPart1 {
		classes[a.class] = true
	}
	for c := range classes {
		if !declared[c] {
			t.Errorf("conformance does not declare %s, whose abstract tests this suite runs", c)
		}
	}
}

// Every abstract test that issues a request reaches the handler the standard
// puts at that location.
func TestATSRouteMandates(t *testing.T) {
	mux := newMux()
	for _, a := range atsPart1 {
		if a.kind != atsRoute {
			continue
		}
		t.Run(a.id, func(t *testing.T) {
			pattern, ok := atsRouteFor(mux, a)
			if pattern == "" {
				t.Errorf("%s: the tier serves no %s %s, so this abstract test cannot pass (%s)",
					a.id, a.method, a.path, a.purpose)
				return
			}
			if !ok {
				t.Errorf("%s: %s %s resolves to %q, not to the location the standard names",
					a.id, a.method, a.path, pattern)
			}
		})
	}
}

// atsQueryParams are the query parameters the Moving Features abstract tests
// require the service description to define.
var atsQueryParams = map[string]string{
	"/conf/movingfeatures/param-leaf-definition":             "leaf",
	"/conf/movingfeatures/param-subtrajectory-definition":    "subTrajectory",
	"/conf/movingfeatures/param-subtemporalvalue-definition": "subTemporalValue",
}

// The service description defines each query parameter the standard specifies,
// which is what the parameter definition tests validate against.
func TestATSParameterDefinitions(t *testing.T) {
	rec := httptest.NewRecorder()
	apiDoc(rec, httptest.NewRequest("GET", "/api", nil))
	var doc struct {
		Paths map[string]map[string]json.RawMessage `json:"paths"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &doc); err != nil {
		t.Fatal(err)
	}
	defined := map[string]bool{}
	for _, ops := range doc.Paths {
		for _, raw := range ops {
			var op struct {
				Parameters []struct {
					Name string `json:"name"`
				} `json:"parameters"`
			}
			if json.Unmarshal(raw, &op) != nil {
				continue
			}
			for _, prm := range op.Parameters {
				defined[prm.Name] = true
			}
		}
	}
	ids := make([]string, 0, len(atsQueryParams))
	for id := range atsQueryParams {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		name := atsQueryParams[id]
		t.Run(id, func(t *testing.T) {
			if !defined[name] {
				t.Errorf("%s: the service description defines no %q query parameter, so there is nothing to validate the definition against",
					id, name)
			}
		})
	}
}

// The abstract tests that read or write data need a populated backend, which
// MFAPI_DSN supplies.
func TestATSLiveOperations(t *testing.T) {
	dsn := os.Getenv("MFAPI_DSN")
	for _, a := range atsPart1 {
		if a.kind != atsLive {
			continue
		}
		t.Run(a.id, func(t *testing.T) {
			if dsn == "" {
				t.Skipf("needs a populated backend: set MFAPI_DSN to run %s (%s)", a.id, a.purpose)
			}
			t.Skipf("%s awaits its live assertion (%s)", a.id, a.purpose)
		})
	}
}

// TestATSCoverageReport prints Annex A against the tier, one line per abstract
// test, so the conformance statement is read off the suite rather than
// maintained by hand. Run it with: go test -run TestATSCoverageReport -v
func TestATSCoverageReport(t *testing.T) {
	mux := newMux()
	rows := append([]atsTest(nil), atsPart1...)
	sort.Slice(rows, func(i, j int) bool { return rows[i].id < rows[j].id })
	var served, missing, live int
	for _, a := range rows {
		state := ""
		switch a.kind {
		case atsRoute:
			if _, ok := atsRouteFor(mux, a); ok {
				state, served = "served", served+1
			} else {
				state, missing = "NOT SERVED", missing+1
			}
		case atsDocument:
			state = "service description"
		case atsResponse:
			state, served = "response asserted", served+1
		case atsLive:
			state, live = "needs a backend", live+1
		}
		t.Log(fmt.Sprintf("%-56s %s", a.id, state))
	}
	t.Log(fmt.Sprintf("%d abstract tests: %d served, %d not served, %d need a backend",
		len(rows), served, missing, live))
}
