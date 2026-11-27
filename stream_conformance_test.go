package main

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
)

// The service declares no conformance class for its continuous queries: they
// follow the Stream proposal for Moving Features, which OGC holds no draft of,
// and are MobilityAPI extensions.
func TestConformanceDeclaresNoStreamClass(t *testing.T) {
	rec := httptest.NewRecorder()
	conformance(rec, httptest.NewRequest("GET", "/conformance", nil))
	var body struct {
		ConformsTo []string `json:"conformsTo"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.ConformsTo) == 0 {
		t.Fatal("the conformance declaration lists no class, so this test would assert nothing")
	}
	for _, c := range body.ConformsTo {
		if strings.Contains(c, "ogcapi-movingfeatures-4") {
			t.Errorf("the conformance declaration lists %s, a class of a Stream part OGC holds no draft of", c)
		}
	}
}

// The API definition documents the continuous-query paths.
func TestAPIDeclaresStreamingPaths(t *testing.T) {
	rec := httptest.NewRecorder()
	apiDoc(rec, httptest.NewRequest("GET", "/api", nil))
	var doc struct {
		Paths map[string]any `json:"paths"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &doc); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{
		"/collections/{cid}/items/{fid}/tproperties/{pname}/queries",
		"/collections/{cid}/items/{fid}/tproperties/{pname}/ingest",
		"/collections/{cid}/items/{fid}/tgsequence/queries",
	} {
		if _, ok := doc.Paths[p]; !ok {
			t.Errorf("API definition is missing the streaming path %q", p)
		}
	}
}

// The cquery link object carries the properties Requirement 1 mandates.
func TestCqueryLinkShape(t *testing.T) {
	cq := &contQuery{id: "q1", spec: QuerySpec{CID: "ships", FID: 7, Pname: "speed", Op: "mul"}}
	link := cqueryLink(httptest.NewRequest("GET", "http://host/x", nil), cq)
	for _, k := range []string{"rel", "queryId", "href", "channel", "status", "type"} {
		if _, ok := link[k]; !ok {
			t.Errorf("cquery link object missing required property %q", k)
		}
	}
	if link["rel"] != "cquery" {
		t.Errorf("rel = %v, want cquery", link["rel"])
	}
	if link["queryId"] != "q1" {
		t.Errorf("queryId = %v, want q1", link["queryId"])
	}
	if link["status"] != "running" {
		t.Errorf("status = %v, want running", link["status"])
	}
}

// A geometry query's cquery link is scoped to the tgsequence resource.
func TestCqueryLinkGeometryScope(t *testing.T) {
	cq := &contQuery{id: "q2", spec: QuerySpec{Kind: "geometry", CID: "ships", FID: 7}}
	link := cqueryLink(httptest.NewRequest("GET", "http://host/x", nil), cq)
	ch, _ := link["channel"].(string)
	if !strings.HasPrefix(ch, "/collections/ships/items/7/tgsequence/queries") {
		t.Errorf("geometry channel = %q, want a tgsequence/queries path", ch)
	}
}
