// The abstract tests of the class landing-page of OGC API – Common – Part 1: Core
// (OGC 19-072), as rows beside those of Moving Features Part 1 (#atsDischargedBy)
// and Part 4 (#atsPart4): the landing page, the API definition and the
// conformance declaration, each requested through the routing table the service
// runs (#newMux) and read against the schema the abstract test names.
//
// The landing page and the conformance declaration are validated against the
// landingPage and confClasses schemas of the vendored Moving Features document,
// which state the constraints of Common's landingPage.yaml, confClasses.yaml and
// link.yaml: the same required members of the same types. The API definition is
// validated against the OpenAPI 3.0 schema, vendored in openapi/ beside them.
package main

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// oasSchemaPath is the vendored JSON Schema of OpenAPI 3.0 documents.
const oasSchemaPath = "openapi/oas-3.0-schema.json"

// atsCommonLandingPage is the class landing-page, its abstract tests in the
// order Common Part 1 gives them, each with the test that reads what it states.
var atsCommonLandingPage = []struct{ id, dischargedBy string }{
	{"/conf/landing-page/root-op", "TestCommonLandingPage"},
	{"/conf/landing-page/root-success", "TestCommonLandingPage"},
	{"/conf/landing-page/api-definition-op", "TestCommonAPIDefinition"},
	{"/conf/landing-page/api-definition-success", "TestCommonAPIDefinition"},
	{"/conf/landing-page/conformance-op", "TestCommonConformanceDeclaration"},
	{"/conf/landing-page/conformance-success", "TestCommonConformanceDeclaration"},
}

// commonGet requests path through the routing table the service runs.
func commonGet(path string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	newMux().ServeHTTP(rec, httptest.NewRequest("GET", path, nil))
	return rec
}

// root-op, root-success: the landing page answers 200, is a landingPage, and
// links the API definition (service-desc) and the conformance declaration under
// the relation type Common Part 1 requires, beside the one Features Part 1 does.
func TestCommonLandingPage(t *testing.T) {
	rec := commonGet("/")
	validate(t, ogcSchemas(t)("landingPage"), rec, "the landing page")
	var page struct {
		Links []struct{ Href, Rel string } `json:"links"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	links := map[string]string{}
	for _, l := range page.Links {
		links[l.Rel] = l.Href
	}
	for rel, href := range map[string]string{
		"service-desc": "/api",
		relConformance: "/conformance",
		"conformance":  "/conformance",
		"data":         "/collections",
	} {
		if links[rel] != href {
			t.Errorf("the landing page links %q to %q, want %q", rel, links[rel], href)
		}
	}
}

// api-definition-op, api-definition-success: the API definition the landing page
// links answers 200 and is an OpenAPI 3.0 document.
func TestCommonAPIDefinition(t *testing.T) {
	f, err := os.Open(oasSchemaPath)
	if err != nil {
		t.Fatalf("the vendored OpenAPI schema is missing: %v", err)
	}
	defer f.Close()
	doc, err := jsonschema.UnmarshalJSON(f)
	if err != nil {
		t.Fatal(err)
	}
	c := jsonschema.NewCompiler()
	if err := c.AddResource("oas.json", doc); err != nil {
		t.Fatal(err)
	}
	schema, err := c.Compile("oas.json")
	if err != nil {
		t.Fatal(err)
	}
	validate(t, schema, commonGet("/api"), "the API definition")

	// The schema refuses what is no OpenAPI 3.0 document, so the validation
	// above asserts something.
	for _, bad := range []string{`{"openapi":"3.0.3","paths":{}}`, `{"openapi":"2.0","info":{"title":"x","version":"1"},"paths":{}}`} {
		v, err := jsonschema.UnmarshalJSON(bytes.NewReader([]byte(bad)))
		if err != nil {
			t.Fatal(err)
		}
		if schema.Validate(v) == nil {
			t.Errorf("the OpenAPI schema admits %s", bad)
		}
	}
}

// conformance-op, conformance-success: the conformance declaration answers 200,
// is a confClasses, and lists the classes of Common Part 1 the service implements.
func TestCommonConformanceDeclaration(t *testing.T) {
	validate(t, ogcSchemas(t)("confClasses"), commonGet("/conformance"), "the conformance declaration")
	declared := declaredClasses(t)
	for _, c := range []string{confCommonCore, confCommonLanding} {
		if !declared[c] {
			t.Errorf("the conformance declaration does not list %s", c)
		}
	}
}

// Every row names a test a test file declares, as #TestATSPart4RowsAreDischarged
// holds the rows of Part 4, and the class is the one the routes are marked with.
func TestCommonLandingPageRowsAreDischarged(t *testing.T) {
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
	for _, r := range atsCommonLandingPage {
		if !bytes.Contains(src, []byte("func "+r.dischargedBy+"(")) {
			t.Errorf("%s names %s, which no test file declares", r.id, r.dischargedBy)
		}
	}
	for _, r := range routeTable() {
		if r.path == "/" || r.path == "/api" || r.path == "/conformance" {
			if r.class != confCommonLanding {
				t.Errorf("GET %s is marked %s, want %s", r.path, r.class, confCommonLanding)
			}
		}
	}
}
