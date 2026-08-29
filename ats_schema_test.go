// Every document a `-success` abstract test reads is validated against the schema
// OGC publishes for it, in `openapi/ogcapi-movingfeatures-1.bundled.json`.
//
// The Annex A prose says what a response must carry; the schema says it in a form a
// machine checks. A suite that only asserts the fields it thought to name passes a
// document missing everything it forgot, so the schema is what makes the conformance
// claim mean something. It runs in the same job as every other test — no network, no
// database — because the bundled document resolves all of its own references.
//
// ⛔ THE SCHEMAS ARE NEVER EDITED TO MAKE A TEST PASS. Where the tier and the schema
// disagree the tier is wrong, except where the standard disagrees with ITSELF, which
// ats_response_test.go records and this file then confirms against the schema.
package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

const ogcBundlePath = "openapi/ogcapi-movingfeatures-1.bundled.json"

// ogcBundleURL is where OGC publishes the document vendored at ogcBundlePath.
const ogcBundleURL = "https://schemas.opengis.net/ogcapi/movingfeatures/part1/1.0/openapi/" +
	"ogcapi-movingfeatures-1.bundled.json"

// nullableRewrites is how many `nullable: true` sites the vendored document carries.
// Asserting the count is what keeps the rewrite from silently becoming a no-op: a
// translation that stops applying makes the validator MORE permissive, which no failing
// test would report.
const nullableRewrites = 16

// openAPINullable rewrites OpenAPI 3.0's `nullable: true` into the type union a JSON
// Schema validator reads, and counts what it rewrote. Nothing else about the document
// is touched — see openapi/README.md for why no other translation is needed.
func openAPINullable(n any, count *int) any {
	switch v := n.(type) {
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, sub := range v {
			out[k] = openAPINullable(sub, count)
		}
		if nullable, ok := out["nullable"].(bool); ok {
			delete(out, "nullable")
			if nullable {
				*count++
				switch t := out["type"].(type) {
				case string:
					out["type"] = []any{t, "null"}
				case []any:
					out["type"] = append(t, "null")
				}
			}
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, sub := range v {
			out[i] = openAPINullable(sub, count)
		}
		return out
	}
	return n
}

// ogcSchemas compiles the vendored document once and answers a validator per schema name.
func ogcSchemas(t *testing.T) func(string) *jsonschema.Schema {
	t.Helper()
	f, err := os.Open(ogcBundlePath)
	if err != nil {
		t.Fatalf("the vendored OGC schemas are missing: %v", err)
	}
	defer f.Close()
	doc, err := jsonschema.UnmarshalJSON(f)
	if err != nil {
		t.Fatalf("reading %s: %v", ogcBundlePath, err)
	}
	var rewritten int
	doc = openAPINullable(doc, &rewritten)
	if rewritten != nullableRewrites {
		t.Fatalf("the nullable rewrite fired %d times, want %d: the vendored document moved, "+
			"so re-read openapi/README.md before changing this count", rewritten, nullableRewrites)
	}

	c := jsonschema.NewCompiler()
	// `format` is an annotation by default in JSON Schema, and asserting it is what the
	// standard's own motionCurve schema needs to admit the five interpolation values it
	// itself names — TestATSSchemaMotionCurveIsInverted measures both readings.
	c.AssertFormat()
	if err := c.AddResource("ogc.json", doc); err != nil {
		t.Fatalf("loading the OGC document: %v", err)
	}
	return func(name string) *jsonschema.Schema {
		s, err := c.Compile("ogc.json#/components/schemas/" + name)
		if err != nil {
			t.Fatalf("no schema %q in the OGC document: %v", name, err)
		}
		return s
	}
}

// validate asserts the recorded response is the schema's kind of document.
func validate(t *testing.T, schema *jsonschema.Schema, rec *httptest.ResponseRecorder, what string) {
	t.Helper()
	if rec.Code != 200 {
		t.Fatalf("%s: status = %d, want 200 (%s)", what, rec.Code, rec.Body.String())
	}
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(rec.Body.Bytes()))
	if err != nil {
		t.Fatalf("%s: the response is not JSON: %v", what, err)
	}
	if err := schema.Validate(doc); err != nil {
		t.Errorf("%s does not satisfy its OGC schema:\n%v", what, err)
	}
}

func req(method, path string, values map[string]string) *http.Request {
	r := httptest.NewRequest(method, path, nil)
	for k, v := range values {
		r.SetPathValue(k, v)
	}
	return r
}

// The documents that need no backend at all.
func TestATSSchemaServiceDocuments(t *testing.T) {
	schema := ogcSchemas(t)
	for _, c := range []struct {
		what    string
		name    string
		path    string
		handler http.HandlerFunc
	}{
		{"the landing page", "landingPage", "/", landing},
		{"the conformance declaration", "confClasses", "/conformance", conformance},
	} {
		t.Run(c.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			c.handler(rec, httptest.NewRequest("GET", c.path, nil))
			validate(t, schema(c.name), rec, c.what)
		})
	}
}

// /conf/mf-collection/collections-get-success and collection-get-success, asserted
// against the schemas rather than against the fields this suite happened to name.
func TestATSSchemaCollectionDocuments(t *testing.T) {
	schema := ogcSchemas(t)
	withBackend(atsCollectionsBackend(), func() {
		rec := httptest.NewRecorder()
		listCollections(rec, httptest.NewRequest("GET", "/collections", nil))
		validate(t, schema("collections"), rec, "the Collections document")

		rec = httptest.NewRecorder()
		getCollection(rec, req("GET", "/collections/ships", map[string]string{"cid": "ships"}))
		validate(t, schema("collection"), rec, "the Collection document")
	})
}

// /conf/movingfeatures/tproperties-get-success.
func TestATSSchemaTemporalPropertiesDocument(t *testing.T) {
	schema := ogcSchemas(t)
	f := atsCollectionsBackend()
	f.answers = append(f.answers,
		fakeAnswer{match: "SELECT 1 FROM", rows: [][]any{{1}}},
		fakeAnswer{match: "to_regclass('mf_tproperty')", rows: [][]any{{"mf_tproperty"}}},
		fakeAnswer{match: "FROM mf_tproperty WHERE", rows: [][]any{
			{"speed", "TReal", uomURI + "km_h-1", "speed over ground"},
			{"label", "TText", "", "vessel label"},
		}},
	)
	withBackend(f, func() {
		rec := httptest.NewRecorder()
		listTProperties(rec, req("GET", "/collections/ships/items/1/tproperties",
			map[string]string{"cid": "ships", "fid": "1"}))
		validate(t, schema("temporalProperties"), rec, "the TemporalProperties document")
	})
}

// /conf/movingfeatures/tgsequence-get-success — and the schema is what settles the
// contradiction TestATSTGSequenceTypeContradiction records: `type` is the enum's single
// value, so a document satisfying Annex A's prose would fail here.
func TestATSSchemaTemporalGeometrySequenceDocument(t *testing.T) {
	const mfjson = `{"type":"MovingPoint","crs":{"type":"Name","properties":{"name":"urn:ogc:def:crs:EPSG::4326"}},` +
		`"coordinates":[[4.35,50.85],[4.36,50.86]],"datetimes":["2026-01-01T00:00:00+00","2026-01-01T00:10:00+00"],` +
		`"interpolation":"Linear"}`
	schema := ogcSchemas(t)
	f := atsCollectionsBackend()
	f.answers = append(f.answers,
		fakeAnswer{match: "SELECT 1 FROM", rows: [][]any{{1}}},
		fakeAnswer{match: "SELECT numSequences(", rows: [][]any{{1}}},
		fakeAnswer{match: "SELECT asMFJSON(", rows: [][]any{{mfjson}}},
	)
	withBackend(f, func() {
		rec := httptest.NewRecorder()
		tgSequence(rec, req("GET", "/collections/ships/items/1/tgsequence",
			map[string]string{"cid": "ships", "fid": "1"}))
		validate(t, schema("temporalGeometrySequence"), rec, "the TemporalGeometrySequence document")
	})
}

// The schema is the authority the tier follows where Annex A's prose contradicts it, so
// the enum it declares is asserted directly: a reader of this suite finds the standard's
// own artifact rather than a constant somebody chose.
func TestATSSchemaDeclaresTGSequenceType(t *testing.T) {
	f, err := os.Open(ogcBundlePath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var doc struct {
		Components struct {
			Schemas map[string]struct {
				Required   []string `json:"required"`
				Properties map[string]struct {
					Enum []string `json:"enum"`
				} `json:"properties"`
			} `json:"schemas"`
		} `json:"components"`
	}
	if err := json.NewDecoder(f).Decode(&doc); err != nil {
		t.Fatal(err)
	}
	s, ok := doc.Components.Schemas["temporalGeometrySequence"]
	if !ok {
		t.Fatal("the OGC document declares no temporalGeometrySequence schema")
	}
	if got := s.Properties["type"].Enum; len(got) != 1 || got[0] != "TemporalGeometrySequence" {
		t.Errorf("type enum = %v, want [TemporalGeometrySequence] — the schema moved, and the "+
			"contradiction ats_response_test.go records needs re-reading", got)
	}
	var hasSequence bool
	for _, r := range s.Required {
		if r == "geometrySequence" {
			hasSequence = true
		}
	}
	if !hasSequence {
		t.Errorf("required = %v, want geometrySequence among them", s.Required)
	}
}

// ⛔ THE motionCurve SCHEMA IS INVERTED UNDER JSON SCHEMA'S OWN DEFAULT, and this test
// is the measurement rather than an argument. /components/schemas/motionCurve is
//
//	oneOf: [ {string, enum: [Discrete, Step, Linear, Quadratic, Cubic]},
//	         {string, format: uri} ]
//
// `oneOf` admits a value matching EXACTLY ONE branch. JSON Schema makes `format` an
// annotation unless a validator opts in, so branch 1 reduces to "any string": every one
// of the five interpolations the standard names matches BOTH branches and is rejected,
// while an arbitrary string matches branch 1 alone and is accepted. A validator run at
// the specification's default settings therefore rejects exactly the values the standard
// defines and accepts the ones it does not.
//
// The fix this measures — keeping `oneOf` and giving the URI branch an absolute-URI
// pattern, so the two branches stop overlapping — answers correctly whichever way a
// validator treats `format`. It is offered to OGC; the schema itself is not edited here.
func TestATSSchemaMotionCurveIsInverted(t *testing.T) {
	const published = `{"oneOf":[
	 {"type":"string","enum":["Discrete","Step","Linear","Quadratic","Cubic"],"default":"Linear"},
	 {"type":"string","format":"uri"}]}`
	const proposed = `{"oneOf":[
	 {"type":"string","enum":["Discrete","Step","Linear","Quadratic","Cubic"],"default":"Linear"},
	 {"type":"string","format":"uri","pattern":"^[A-Za-z][A-Za-z0-9+.-]*:"}]}`

	compile := func(src string, assertFormat bool) *jsonschema.Schema {
		d, err := jsonschema.UnmarshalJSON(bytes.NewReader([]byte(src)))
		if err != nil {
			t.Fatal(err)
		}
		c := jsonschema.NewCompiler()
		if assertFormat {
			c.AssertFormat()
		}
		if err := c.AddResource("m.json", d); err != nil {
			t.Fatal(err)
		}
		s, err := c.Compile("m.json")
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	accepts := func(s *jsonschema.Schema, v string) bool { return s.Validate(any(v)) == nil }

	named := []string{"Discrete", "Step", "Linear", "Quadratic", "Cubic"}
	const aURI, notACurve = "http://example.org/curve", "Bogus"

	// The finding: at the specification's default the published schema is inverted.
	pub := compile(published, false)
	for _, v := range named {
		if accepts(pub, v) {
			t.Errorf("the published motionCurve accepts %q at default format handling; "+
				"this test records that it does not, so the schema has been corrected "+
				"and openapi/README.md needs re-reading", v)
		}
	}
	if !accepts(pub, notACurve) {
		t.Errorf("the published motionCurve rejects %q at default format handling; "+
			"this test records that it accepts it", notACurve)
	}

	// The fix answers the same under both readings, which is what makes it a fix rather
	// than a second way to be right by accident.
	for _, assert := range []bool{false, true} {
		s := compile(proposed, assert)
		for _, v := range append(append([]string{}, named...), aURI) {
			if !accepts(s, v) {
				t.Errorf("the proposed motionCurve rejects %q (assertFormat=%v)", v, assert)
			}
		}
		if accepts(s, notACurve) {
			t.Errorf("the proposed motionCurve accepts %q (assertFormat=%v)", notACurve, assert)
		}
	}
}

// ⛔ WHAT THIS VALIDATION CANNOT SEE, STATED RATHER THAN LEFT IMPLICIT. A temporal
// GEOMETRY's datetimes are `{"type": "string"}` with no format, while a temporal
// PROPERTY's are `{"type": "string", "format": "date-time"}`. The same concept carries
// two different constraints, so validating a TemporalGeometrySequence says nothing
// about the shape of the instants inside it — any string passes.
//
// That matters because the standard's own normative text is not silent on the point:
// it says the syntax of a date-time is RFC 3339 section 5.6, and that a server SHALL
// interpret it so. The geometry schema under-constrains against its own standard.
//
// This test pins the asymmetry so a reader of the suite finds the gap rather than
// mistaking a passing document for a checked one, and so a corrected schema surfaces
// here as a failure to simplify.
func TestATSSchemaDatetimeFormatIsAsymmetric(t *testing.T) {
	f, err := os.Open(ogcBundlePath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var doc struct {
		Components struct {
			Schemas map[string]struct {
				Properties map[string]struct {
					Items struct {
						Type   string `json:"type"`
						Format string `json:"format"`
					} `json:"items"`
				} `json:"properties"`
			} `json:"schemas"`
		} `json:"components"`
	}
	if err := json.NewDecoder(f).Decode(&doc); err != nil {
		t.Fatal(err)
	}
	geom := doc.Components.Schemas["temporalPrimitiveGeometry"].Properties["datetimes"].Items
	value := doc.Components.Schemas["temporalPrimitiveValue"].Properties["datetimes"].Items
	if value.Format != "date-time" {
		t.Errorf("temporalPrimitiveValue.datetimes items format = %q, want date-time", value.Format)
	}
	if geom.Format != "" {
		t.Errorf("temporalPrimitiveGeometry.datetimes items now carry format %q — the asymmetry "+
			"this test records is gone, so the note above it and openapi/README.md need re-reading",
			geom.Format)
	}
	if geom.Type != "string" {
		t.Errorf("temporalPrimitiveGeometry.datetimes items type = %q, want string", geom.Type)
	}
}

// The vendored copy is what OGC publishes. It needs the network, so it runs only when
// asked for; openapi/README.md carries the URL and the checksum it is pinned at.
func TestATSSchemaBundleMatchesOGC(t *testing.T) {
	if os.Getenv("MFAPI_SCHEMA_FRESHNESS") == "" {
		t.Skip("set MFAPI_SCHEMA_FRESHNESS=1 to fetch " + ogcBundleURL + " and compare")
	}
	resp, err := http.Get(ogcBundleURL)
	if err != nil {
		t.Fatalf("fetching the published schemas: %v", err)
	}
	defer resp.Body.Close()
	published, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	vendored, err := os.ReadFile(ogcBundlePath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(vendored, published) {
		t.Errorf("%s differs from %s: %d bytes vendored, %d published. Re-vendor it unchanged "+
			"and re-read what moved; never edit the copy.",
			ogcBundlePath, ogcBundleURL, len(vendored), len(published))
	}
}
