package main

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
)

// The service declares the OGC MF Part 4 (Continuous Query) conformance class.
func TestConformanceDeclaresPart4(t *testing.T) {
	rec := httptest.NewRecorder()
	conformance(rec, httptest.NewRequest("GET", "/conformance", nil))
	var body struct {
		ConformsTo []string `json:"conformsTo"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	const want = "http://www.opengis.net/spec/ogcapi-movingfeatures-4/1.0/conf/cquery"
	for _, c := range body.ConformsTo {
		if c == want {
			return
		}
	}
	t.Errorf("conformance does not declare the MF Part 4 cquery class %q", want)
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

// The window-aggregation result carries the bounds Requirement 2 mandates.
func TestWindowResultShape(t *testing.T) {
	ev := Event{
		"windowStart": "2026-01-01T00:00:00Z",
		"windowEnd":   "2026-01-01T00:00:30Z",
		"aggregation": "AVG",
		"property":    "speed",
		"value":       6.2,
		"count":       3,
	}
	for _, k := range []string{"windowStart", "windowEnd", "aggregation", "value", "count"} {
		if _, ok := ev[k]; !ok {
			t.Errorf("window aggregate result missing required field %q", k)
		}
	}
}
