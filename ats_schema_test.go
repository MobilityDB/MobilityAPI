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
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
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

// ogcSchemas compiles the vendored document once and answers a validator per schema
// name, with `format` asserted — the reading under which the standard's own named
// values are admissible.
func ogcSchemas(t *testing.T) func(string) *jsonschema.Schema {
	return ogcSchemasFormat(t, true)
}

// ogcSchemasFormat compiles the vendored document under either reading of `format`.
// The two are not a preference: JSON Schema makes `format` an annotation by default
// and an assertion by opt-in, and issue 4's inverted `oneOf` means the standard's own
// interpolation values are admissible under one reading and not the other.
func ogcSchemasFormat(t *testing.T, assertFormat bool) func(string) *jsonschema.Schema {
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
	if assertFormat {
		c.AssertFormat()
	}
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

// /conf/movingfeatures/tproperty-get-success — the `type` a TemporalProperty
// document names is one the standard defines.
//
// ⛔ THE ADMITTED SET IS READ OUT OF THE VENDORED DOCUMENT, never written here: a
// constant chosen in this file would agree with whatever the tier does and assert
// nothing. The schema states the enum once, at
// components/schemas/temporalProperty/properties/type.
//
// ⛔ THE WHOLE DOCUMENT CANNOT BE VALIDATED THE WAY ITS SIBLINGS ARE, and the
// reason is the standard's, not the tier's: `temporalPrimitiveValue` declares
// `datetimes` an array of `minItems: 2` beside `values` as a single scalar
// (`oneOf` number/string/boolean), so no document carrying a value per instant
// satisfies it. This asserts the cells that are satisfiable, of which the type
// token is one.
func TestATSSchemaTemporalPropertyType(t *testing.T) {
	f, err := os.Open(ogcBundlePath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var doc struct {
		Components struct {
			Schemas struct {
				TemporalProperty struct {
					Properties struct {
						Type struct {
							Enum []string `json:"enum"`
						} `json:"type"`
					} `json:"properties"`
				} `json:"temporalProperty"`
			} `json:"schemas"`
		} `json:"components"`
	}
	if err := json.NewDecoder(f).Decode(&doc); err != nil {
		t.Fatalf("reading %s: %v", ogcBundlePath, err)
	}
	admitted := doc.Components.Schemas.TemporalProperty.Properties.Type.Enum
	if len(admitted) == 0 {
		t.Fatal("the vendored document declares no type enum, so this test would assert nothing")
	}
	in := func(s string) bool {
		for _, a := range admitted {
			if a == s {
				return true
			}
		}
		return false
	}
	// Every spelling the tier accepts, so a token is checked per stored type rather
	// than for the one type a single case would happen to cover.
	for _, stored := range []string{
		"TReal", "tfloat", "measure", "number",
		"TInteger", "tint", "integer", "int",
		"TText", "tstring", "text", "string",
		"TBoolean", "tbool", "boolean", "bool",
	} {
		tt, ok := tPropType(stored)
		if !ok {
			t.Errorf("the tier does not resolve the stored type %q", stored)
			continue
		}
		if !in(tt.ogc) {
			t.Errorf("a %q property is written as type %q, which the standard does not define; it admits %v",
				stored, tt.ogc, admitted)
		}
	}
}

// The interpolation the tier writes is one the standard names.
//
// ⛔ BOTH ADMITTED SETS ARE READ OUT OF THE VENDORED DOCUMENT. Part 1 constrains
// an interpolation in exactly two places and they are not the same list:
// `motionCurve` (what a temporal geometry takes, through a $ref that a walk over
// `temporalPrimitiveGeometry` alone does not see) and `temporalPrimitiveValue`.
// The step function is `Step` in both, and the word `Stepwise` — the older
// MF-JSON encoding extension's spelling — appears nowhere in the document.
func TestATSSchemaInterpolationToken(t *testing.T) {
	raw, err := os.ReadFile(ogcBundlePath)
	if err != nil {
		t.Fatal(err)
	}
	if n := bytes.Count(raw, []byte(`"Stepwise"`)); n != 0 {
		t.Errorf(`the vendored document names "Stepwise" %d times; the tier writes "Step" because it named it 0`, n)
	}
	var doc struct {
		Components struct {
			Schemas struct {
				MotionCurve struct {
					OneOf []struct {
						Enum []string `json:"enum"`
					} `json:"oneOf"`
				} `json:"motionCurve"`
				TemporalPrimitiveValue struct {
					Properties struct {
						Interpolation struct {
							Enum []string `json:"enum"`
						} `json:"interpolation"`
					} `json:"properties"`
				} `json:"temporalPrimitiveValue"`
			} `json:"schemas"`
		} `json:"components"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("reading %s: %v", ogcBundlePath, err)
	}
	var curve []string
	for _, b := range doc.Components.Schemas.MotionCurve.OneOf {
		curve = append(curve, b.Enum...)
	}
	value := doc.Components.Schemas.TemporalPrimitiveValue.Properties.Interpolation.Enum
	if len(curve) == 0 || len(value) == 0 {
		t.Fatal("the vendored document declares no interpolation enum, so this test would assert nothing")
	}
	in := func(set []string, s string) bool {
		for _, a := range set {
			if a == s {
				return true
			}
		}
		return false
	}
	// What the tier writes: MobilityDB's own token, carried through ogcify.
	const geometry = `{"type":"MovingPoint","interpolation":"Step"}`
	const valueSeq = `{"interpolation":"Step","values":[1,2]}`
	for _, c := range []struct {
		what, doc string
		admitted  []string
	}{
		{"a temporal geometry", geometry, curve},
		{"a temporal property value", valueSeq, value},
	} {
		var m map[string]any
		if err := json.Unmarshal([]byte(ogcify(c.doc)), &m); err != nil {
			t.Fatalf("%s: ogcify produced no JSON: %v", c.what, err)
		}
		got, _ := m["interpolation"].(string)
		if !in(c.admitted, got) {
			t.Errorf("%s is written with interpolation %q, which the standard does not name; it admits %v",
				c.what, got, c.admitted)
		}
	}
}

// publishedExampleFailures is what the standard's own examples score against the
// standard's own schemas, under each reading of `format`. Both are asserted rather
// than reported so that a change in the vendored document is noticed rather than
// absorbed, and so that the DIFFERENCE between them stays visible: it is issue 4's
// inverted `oneOf` acting on the standard's own material.
const (
	publishedExampleFailures        = 10 // `format` asserted
	publishedExampleFailuresDefault = 13 // `format` as an annotation, JSON Schema's default
)

// The standard's examples, measured against the standard's schemas.
//
// Every example the document publishes sits in a media-type object beside the
// schema it illustrates, so the pairing is the document's own and nothing here
// chooses it. An example that does not satisfy the schema it is attached to is a
// defect in the standard, not in this tier, and the count is what makes that
// claim checkable instead of quotable.
func TestATSSchemaPublishedExamples(t *testing.T) {
	raw, err := os.ReadFile(ogcBundlePath)
	if err != nil {
		t.Fatal(err)
	}
	var doc any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	type pair struct {
		where, name string
		example     any
	}
	var pairs []pair
	var inline int
	var walk func(o any, p string)
	walk = func(o any, p string) {
		switch v := o.(type) {
		case map[string]any:
			if s, ok := v["schema"].(map[string]any); ok {
				if ex, has := v["example"]; has {
					if ref, ok := s["$ref"].(string); ok {
						n := ref[strings.LastIndex(ref, "/")+1:]
						pairs = append(pairs, pair{p, n, ex})
					} else {
						inline++
					}
				}
			}
			for k, sub := range v {
				walk(sub, p+"/"+k)
			}
		case []any:
			for i, sub := range v {
				walk(sub, fmt.Sprintf("%s[%d]", p, i))
			}
		}
	}
	walk(doc, "")
	if len(pairs) == 0 {
		t.Fatal("the vendored document pairs no example with a named schema, so this test would assert nothing")
	}

	// Both readings, because issue 4 makes the count depend on the setting: with
	// `format` an annotation the URI branch of `motionCurve` reduces to "any string",
	// both branches then match a named value, and `oneOf` rejects the standard's own
	// vocabulary. The difference between the two counts IS that defect, acting on the
	// standard's own material.
	for _, reading := range []struct {
		what   string
		assert bool
		want   int
	}{
		{"`format` asserted", true, publishedExampleFailures},
		{"`format` as an annotation (JSON Schema's default)", false, publishedExampleFailuresDefault},
	} {
		schema := ogcSchemasFormat(t, reading.assert)
		var failed int
		for _, pr := range pairs {
			// jsonschema validates the value shape it unmarshals itself, so the example
			// is round-tripped through the same reader the rest of the suite uses.
			b, err := json.Marshal(pr.example)
			if err != nil {
				t.Fatalf("%s: re-encoding the example: %v", pr.where, err)
			}
			val, err := jsonschema.UnmarshalJSON(bytes.NewReader(b))
			if err != nil {
				t.Fatalf("%s: the published example is not JSON: %v", pr.where, err)
			}
			if err := schema(pr.name).Validate(val); err != nil {
				failed++
				if reading.assert {
					t.Logf("the %s example does not satisfy the %s schema it is published under:\n  %v",
						pr.where, pr.name, firstLines(err.Error(), 4))
				}
			}
		}
		t.Logf("with %s: %d of %d published examples fail the schema they are attached to",
			reading.what, failed, len(pairs))
		if failed != reading.want {
			t.Errorf("with %s, %d of %d published examples fail their own schema, want %d: the vendored "+
				"document moved, so re-read openapi/README.md before changing this count",
				reading.what, failed, len(pairs), reading.want)
		}
	}
	t.Logf("%d further example is attached to an inline schema and is not compared; "+
		"%d responses declare a schema and publish no example at all", inline, exampleless(t))
}

// exampleless counts the responses that declare a schema and publish no example.
// ⛔ THEY ARE NOT PASSES. A row with nothing to validate cannot be one, and counting
// it among the examples that satisfy their schema is what makes a tally read better
// than the material it is taken over.
func exampleless(t *testing.T) int {
	t.Helper()
	raw, err := os.ReadFile(ogcBundlePath)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Components struct {
			Responses map[string]struct {
				Content map[string]map[string]any `json:"content"`
			} `json:"responses"`
		} `json:"components"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	var n int
	for _, r := range doc.Components.Responses {
		for _, mt := range r.Content {
			_, hasSchema := mt["schema"]
			_, hasExample := mt["example"]
			if hasSchema && !hasExample {
				n++
			}
		}
	}
	return n
}

// firstLines keeps a validation error readable in a log.
func firstLines(s string, n int) string {
	parts := strings.SplitN(s, "\n", n+1)
	if len(parts) > n {
		parts = append(parts[:n], "  …")
	}
	return strings.Join(parts, "\n")
}
