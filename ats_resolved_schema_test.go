// The resolved schema: the OGC API – Moving Features Part 1 document with candidate
// resolutions 1 to 5 of the wiki page "Implementing OGC API — Moving Features in
// MobilityDB" applied, and every document this tier writes and every example OGC
// publishes validated against it.
//
// A resolution is offered to the Standards Working Group with its implementation as
// evidence, never as text alone. This file is that implementation for the five edits
// of the normative schemas: it shows each one can be written as a schema a validator
// compiles, that it answers the cases it exists for, and what it does to the
// documents that exist today.
//
// It reads the vendored document as ats_schema_test.go does (#ogcSchemasFormat,
// #openAPINullable) and the published examples through #publishedExamples, and adds
// only the resolutions.
//
// ⛔ THE VENDORED COPY IS NEVER EDITED. resolvedOGC reads it and builds the resolved
// document in memory, each resolution an edit at the sites the wiki names, and each
// site asserted to hold the published text before it is edited: a vendored document
// that moved fails here instead of being resolved into something nobody proposed.
package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// absoluteURIPattern is the pattern resolution 1 gives the URI branch of a `oneOf`,
// so that it stops overlapping the branch of named values. It is the pattern
// #TestATSSchemaMotionCurveIsInverted measures on motionCurve alone.
const absoluteURIPattern = "^[A-Za-z][A-Za-z0-9+.-]*:"

// unCEFACTCodePattern is the pattern resolution 3 gives the three-character branch of
// a `form`: a UN/CEFACT Recommendation 20 common code, as the standard's own examples
// write their units (`KMH`, `MTR`, `MQS`). It holds no `:`, so it cannot overlap the
// URI branch of resolution 1 whichever way a validator treats `format`.
const unCEFACTCodePattern = "^[A-Z0-9]{3}$"

// rfc3339Pattern is the pattern resolution 5 gives the instants of a temporal value:
// the date-time of RFC 3339 section 5.6, which the standard names as their syntax. It
// states in a pattern what `format: date-time` states only to a validator that
// asserts `format`, so the instants are constrained under either reading.
const rfc3339Pattern = `^[0-9]{4}-[0-9]{2}-[0-9]{2}[Tt][0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?([Zz]|[+-][0-9]{2}:[0-9]{2})$`

// valueInterpolationSchema is the name resolution 4 gives the one schema of a
// property's value interpolation, which the published document spells inline.
const valueInterpolationSchema = "valueInterpolation"

// resolutionSites is how many places each resolution edits. Asserting the counts is
// what keeps a resolution from silently ceasing to apply, as #nullableRewrites does
// for the nullable rewrite.
var resolutionSites = map[int]int{
	1: 6, // the URI branch of motionCurve, of the three inline interpolations and of the two forms
	2: 1, // temporalPrimitiveValue.values
	3: 2, // temporalProperty.form and parametricValues.*.form
	4: 4, // the three inline interpolations and temporalPrimitiveValue.interpolation
	5: 3, // the datetimes of temporalPrimitiveGeometry, temporalPrimitiveValue and parametricValues
}

// at walks a decoded JSON document along a path of object keys and array indexes.
func at(t *testing.T, doc any, path ...any) any {
	t.Helper()
	cur := doc
	for i, step := range path {
		switch s := step.(type) {
		case string:
			m, ok := cur.(map[string]any)
			if !ok {
				t.Fatalf("resolving %v: %v is not an object", path, path[:i])
			}
			if cur, ok = m[s]; !ok {
				t.Fatalf("resolving %v: no member %q at %v", path, s, path[:i])
			}
		case int:
			a, ok := cur.([]any)
			if !ok || s >= len(a) {
				t.Fatalf("resolving %v: no item %d at %v", path, s, path[:i])
			}
			cur = a[s]
		}
	}
	return cur
}

// object answers the object at path, failing when there is none.
func object(t *testing.T, doc any, path ...any) map[string]any {
	t.Helper()
	m, ok := at(t, doc, path...).(map[string]any)
	if !ok {
		t.Fatalf("resolving %v: not an object", path)
	}
	return m
}

// decode reads a JSON text the way the validator does.
func decode(t *testing.T, src string) any {
	t.Helper()
	v, err := jsonschema.UnmarshalJSON(strings.NewReader(src))
	if err != nil {
		t.Fatalf("%s is not JSON: %v", src, err)
	}
	return v
}

// expect asserts a site holds the published text before a resolution edits it.
func expect(t *testing.T, r int, got any, want string, path ...any) {
	t.Helper()
	if !reflect.DeepEqual(got, decode(t, want)) {
		b, _ := json.Marshal(got)
		t.Fatalf("resolution %d: %v holds %s, want the published %s: the vendored document moved, "+
			"so re-read the wiki page before resolving it", r, path, b, want)
	}
}

const (
	publishedURIBranch      = `{"type":"string","format":"uri"}`
	resolvedURIBranch       = `{"type":"string","format":"uri","pattern":"^[A-Za-z][A-Za-z0-9+.-]*:"}`
	publishedThreeCharForm  = `{"type":"string","minLength":3,"maxLength":3}`
	publishedValueEnum      = `{"type":"string","enum":["Discrete","Step","Linear","Regression"],"default":"Linear"}`
	publishedPrimitiveValue = `{"type":"string","enum":["Discrete","Step","Linear","Regression"]}`
)

// publishedOGC decodes the vendored document as the validator reads it, with the
// nullable rewrite of #ogcSchemasFormat applied and nothing else. Each call answers a
// fresh tree, so a resolution edits nothing another reader holds.
func publishedOGC(t *testing.T) any {
	t.Helper()
	f, err := os.Open(ogcBundlePath)
	if err != nil {
		t.Fatalf("the vendored OGC schemas are missing: %v", err)
	}
	defer f.Close()
	raw, err := jsonschema.UnmarshalJSON(f)
	if err != nil {
		t.Fatalf("reading %s: %v", ogcBundlePath, err)
	}
	var rewritten int
	doc := openAPINullable(raw, &rewritten)
	if rewritten != nullableRewrites {
		t.Fatalf("the nullable rewrite fired %d times, want %d", rewritten, nullableRewrites)
	}
	return doc
}

// resolvedOGC reads the vendored document and answers it with resolutions 1 to 5
// applied, and how many sites each resolution edited.
func resolvedOGC(t *testing.T) (any, map[int]int) {
	t.Helper()
	doc := publishedOGC(t)
	schemas := object(t, doc, "components", "schemas")
	edits := map[int]int{}

	pv := func(i int) []any {
		return []any{"components", "schemas", "parametricValues", "additionalProperties", "oneOf", i, "properties"}
	}
	// Resolution 1: the URI branch of each of the six `oneOf` takes an absolute-URI
	// pattern, so that a named value matches the enum alone, an absolute URI the URI
	// branch alone, and an arbitrary string neither.
	for _, site := range [][]any{
		{"components", "schemas", "motionCurve", "oneOf", 1},
		append(pv(0), "interpolation", "oneOf", 1),
		append(pv(1), "interpolation", "oneOf", 1),
		append(pv(2), "interpolation", "oneOf", 1),
		append(pv(0), "form", "oneOf", 1),
		{"components", "schemas", "temporalProperty", "properties", "form", "oneOf", 0},
	} {
		branch := object(t, doc, site...)
		expect(t, 1, branch, publishedURIBranch, site...)
		branch["pattern"] = absoluteURIPattern
		edits[1]++
	}

	// Resolution 2: a value per instant, `values` an array mirroring `datetimes` with
	// the published scalar `oneOf` as its items.
	tpv := object(t, doc, "components", "schemas", "temporalPrimitiveValue", "properties")
	expect(t, 2, tpv["values"],
		`{"oneOf":[{"type":"number"},{"type":"string"},{"type":"boolean"}]}`,
		"temporalPrimitiveValue", "values")
	tpv["values"] = map[string]any{
		"type":     "array",
		"minItems": decode(t, "2"),
		"items":    tpv["values"],
	}
	edits[2]++

	// Resolution 3: a unit is a register URI or a UN/CEFACT common code. The branch of
	// three characters stays, since the standard's own examples write their units in
	// it, and takes the pattern of a common code, so that it stops overlapping the URI
	// branch and stops admitting any three characters.
	for _, site := range [][]any{
		append(pv(0), "form", "oneOf", 0),
		{"components", "schemas", "temporalProperty", "properties", "form", "oneOf", 1},
	} {
		branch := object(t, doc, site...)
		expect(t, 3, branch, publishedThreeCharForm, site...)
		branch["pattern"] = unCEFACTCodePattern
		edits[3]++
	}

	// Resolution 4: a property's value interpolation is spelled once, as a named
	// schema, and every site that spells it refers to it. The three inline copies are
	// identical once resolution 1 has applied, which is asserted, so hoisting them
	// changes no answer; temporalPrimitiveValue.interpolation, the fourth site, takes
	// the same schema and so gains the URI branch the inline copies carry.
	inline := `{"oneOf":[` + publishedValueEnum + `,` + resolvedURIBranch + `]}`
	ref := map[string]any{"$ref": "#/components/schemas/" + valueInterpolationSchema}
	for i := 0; i < 3; i++ {
		props := object(t, doc, pv(i)...)
		expect(t, 4, props["interpolation"], inline, "parametricValues", i, "interpolation")
		props["interpolation"] = ref
		edits[4]++
	}
	expect(t, 4, tpv["interpolation"], publishedPrimitiveValue, "temporalPrimitiveValue", "interpolation")
	tpv["interpolation"] = ref
	edits[4]++
	if _, taken := schemas[valueInterpolationSchema]; taken {
		t.Fatalf("resolution 4: the published document already names a schema %q", valueInterpolationSchema)
	}
	schemas[valueInterpolationSchema] = decode(t, inline)

	// Resolution 5: the instants of every temporal value are RFC 3339 date-times, stated
	// as `format: date-time` and as the pattern of section 5.6, so that a validator
	// that reads `format` as an annotation constrains them too. A temporal geometry's
	// instants, published as any string, take both; those of a primitive value and of
	// parametric values, published with the format alone, take the pattern. The other
	// date-times of the document (a `timeStamp`, the temporal extent, the `leaf`
	// parameter) are no instants of a temporal value and are not edited.
	for _, c := range []struct {
		schema, published string
	}{
		{"temporalPrimitiveGeometry", `{"type":"string"}`},
		{"temporalPrimitiveValue", `{"type":"string","format":"date-time"}`},
		{"parametricValues", `{"type":"string","format":"date-time"}`},
	} {
		items := object(t, doc, "components", "schemas", c.schema, "properties", "datetimes", "items")
		expect(t, 5, items, c.published, c.schema, "datetimes", "items")
		items["format"] = "date-time"
		items["pattern"] = rfc3339Pattern
		edits[5]++
	}

	return doc, edits
}

// compileOGC compiles a decoded document under a reading of `format` and answers a
// validator per JSON pointer into it; #ogcSchemasFormat does the same for the
// vendored document by schema name.
func compileOGC(t *testing.T, doc any, assertFormat bool) func(string) *jsonschema.Schema {
	t.Helper()
	c := jsonschema.NewCompiler()
	if assertFormat {
		c.AssertFormat()
	}
	if err := c.AddResource("ogc.json", doc); err != nil {
		t.Fatalf("loading the document: %v", err)
	}
	return func(pointer string) *jsonschema.Schema {
		s, err := c.Compile("ogc.json#" + pointer)
		if err != nil {
			t.Fatalf("no schema at %s: %v", pointer, err)
		}
		return s
	}
}

func schemaPointer(name string) string { return "/components/schemas/" + name }

// readings are the two readings of `format` JSON Schema allows, as
// #TestATSSchemaPublishedExamples measures them.
var readings = []struct {
	what   string
	assert bool
}{
	{"`format` asserted", true},
	{"`format` as an annotation", false},
}

// Every resolution edits exactly the sites the wiki names, and the resolved document
// compiles under either reading.
func TestATSResolvedSchemaEdits(t *testing.T) {
	doc, edits := resolvedOGC(t)
	if !reflect.DeepEqual(edits, resolutionSites) {
		t.Errorf("the resolutions edited %v sites, want %v", edits, resolutionSites)
	}
	for _, r := range readings {
		schema := compileOGC(t, doc, r.assert)
		for _, name := range []string{"motionCurve", valueInterpolationSchema, "temporalPrimitiveValue",
			"temporalPrimitiveGeometry", "temporalProperty", "parametricValues", "temporalGeometrySequence"} {
			schema(schemaPointer(name))
		}
	}
}

// A case of a resolution: a document at a schema site, and whether the published and
// the resolved schemas admit it. A case where the two differ is what the resolution
// changes; a case where they agree pins that it changes nothing else.
type resolutionCase struct {
	resolution int
	pointer    string
	doc        string
	published  bool // under `format` asserted
	resolved   bool // under either reading
	// publishedDefault is the published verdict under the annotation reading, where it
	// differs from the asserted one.
	publishedDefault *bool
}

func verdict(b bool) *bool { return &b }

// The cases each resolution exists for, and the documents it narrows.
//
// ⛔ THREE RESOLUTIONS NARROW WHAT THE PUBLISHED SCHEMA ADMITS. A scalar `values`
// (resolution 2), a three-character unit that is no UN/CEFACT common code such as
// `deg` (resolution 3) and a temporal geometry's instant that is no RFC 3339
// date-time (resolution 5) are admitted today and refused once resolved. Each is a
// document the standard's own text does not admit (a value per instant; RFC 3339) or
// one the resolution means to refuse (a unit neither from a register nor a common
// code), so the narrowing is the resolution; it is stated here as cases rather than
// claimed away. "Every document conformant today stays conformant"
// holds over the documents that exist, the tier's and the standard's examples
// (TestATSResolvedSchemaKeepsEveryDocument), not over every document a schema admits.
func TestATSResolvedSchemaCases(t *testing.T) {
	const tp = "/components/schemas/temporalProperty/properties/form"
	const pvForm = "/components/schemas/parametricValues/additionalProperties/oneOf/0/properties/form"
	const pvInterp = "/components/schemas/parametricValues/additionalProperties/oneOf/0/properties/interpolation"
	const tpv = "/components/schemas/temporalPrimitiveValue"
	const tpg = "/components/schemas/temporalPrimitiveGeometry/properties/datetimes"
	const tpvTimes = "/components/schemas/temporalPrimitiveValue/properties/datetimes"
	const pvTimes = "/components/schemas/parametricValues/properties/datetimes"
	mc := schemaPointer("motionCurve")
	value := func(values, interp string) string {
		return `{"datetimes":["2026-01-01T08:00:00Z","2026-01-01T08:10:00Z"],"values":` + values +
			`,"interpolation":"` + interp + `"}`
	}

	var cases []resolutionCase
	// Resolution 1, at a motion curve and at a value interpolation: the named values
	// and an absolute URI are admitted and an arbitrary string is not, under either
	// reading. Published, the annotation reading inverts both.
	for _, p := range []string{mc, pvInterp} {
		named := []string{"Discrete", "Step", "Linear"}
		if p == mc {
			named = append(named, "Quadratic", "Cubic")
		} else {
			named = append(named, "Regression")
		}
		for _, v := range named {
			cases = append(cases, resolutionCase{1, p, `"` + v + `"`, true, true, verdict(false)})
		}
		cases = append(cases,
			resolutionCase{1, p, `"http://example.org/curve"`, true, true, nil},
			resolutionCase{1, p, `"Bogus"`, false, false, verdict(true)})
	}
	// Resolution 2: a value per instant is admitted, and a scalar beside two instants
	// is not.
	cases = append(cases,
		resolutionCase{2, tpv, value(`[0.0, 20.0]`, "Linear"), false, true, nil},
		resolutionCase{2, tpv, value(`[true, false]`, "Step"), false, true, nil},
		resolutionCase{2, tpv, value(`20.0`, "Linear"), true, false, nil})
	// Resolution 3: a register URI and a UN/CEFACT common code are admitted under
	// either reading, the codes the standard's own examples write among them; `deg`,
	// three characters and no common code, is refused, as are the symbols the published
	// schema never admitted. Published, the annotation reading inverts the branches as
	// it does motionCurve's: a common code matches both and is refused, and any string
	// longer than three characters matches the URI branch alone and is admitted.
	for _, p := range []string{tp, pvForm} {
		cases = append(cases,
			resolutionCase{3, p, `"http://www.opengis.net/def/uom/UCUM/0/m"`, true, true, nil},
			resolutionCase{3, p, `"http://www.opengis.net/def/uom/UCUM/0/km_h-1"`, true, true, nil},
			resolutionCase{3, p, `"MTR"`, true, true, verdict(false)},
			resolutionCase{3, p, `"KMH"`, true, true, verdict(false)},
			resolutionCase{3, p, `"MQS"`, true, true, verdict(false)},
			resolutionCase{3, p, `"deg"`, true, false, verdict(false)},
			resolutionCase{3, p, `"m"`, false, false, verdict(true)},
			resolutionCase{3, p, `"km/h"`, false, false, verdict(true)})
	}
	// Resolution 4: temporalPrimitiveValue.interpolation reads the named schema, so it
	// admits a URI as the inline copies do, and its vocabulary is the published one.
	cases = append(cases,
		resolutionCase{4, tpv, value(`[0.0, 20.0]`, "http://example.org/curve"), false, true, nil},
		resolutionCase{4, tpv, value(`[0.0, 20.0]`, "Regression"), false, true, nil},
		resolutionCase{4, tpv, value(`[0.0, 20.0]`, "Cubic"), false, false, nil})
	// Resolution 5: the instants of a temporal geometry and of a temporal value are
	// RFC 3339 under either reading; PostgreSQL's text rendering of a timestamptz is
	// not. Published, a geometry's instants are any string, and a value's are
	// constrained only when `format` is asserted.
	const rfc = `["2026-01-01T08:00:00Z","2026-01-01T08:10:00.5+01:00"]`
	const pgText = `["2026-01-01 08:00:00+00","2026-01-01 08:10:00+00"]`
	const words = `["yesterday","today"]`
	cases = append(cases,
		resolutionCase{5, tpg, rfc, true, true, nil},
		resolutionCase{5, tpg, pgText, true, false, nil},
		resolutionCase{5, tpg, words, true, false, nil})
	for _, p := range []string{tpvTimes, pvTimes} {
		cases = append(cases,
			resolutionCase{5, p, rfc, true, true, nil},
			resolutionCase{5, p, pgText, false, false, verdict(true)},
			resolutionCase{5, p, words, false, false, verdict(true)})
	}

	resolved, _ := resolvedOGC(t)
	pub := publishedOGC(t)
	narrowed := map[int]int{}
	for _, r := range readings {
		pubSchema, resSchema := compileOGC(t, pub, r.assert), compileOGC(t, resolved, r.assert)
		for _, c := range cases {
			v := decode(t, c.doc)
			wantPub := c.published
			if !r.assert && c.publishedDefault != nil {
				wantPub = *c.publishedDefault
			}
			if got := pubSchema(c.pointer).Validate(v) == nil; got != wantPub {
				t.Errorf("resolution %d, %s, published schema with %s: admits %s = %v, want %v",
					c.resolution, c.pointer, r.what, c.doc, got, wantPub)
			}
			if got := resSchema(c.pointer).Validate(v) == nil; got != c.resolved {
				t.Errorf("resolution %d, %s, resolved schema with %s: admits %s = %v, want %v",
					c.resolution, c.pointer, r.what, c.doc, got, c.resolved)
			}
			if r.assert && c.published && !c.resolved {
				narrowed[c.resolution]++
			}
		}
	}
	t.Logf("cases the published schema admits and the resolved one refuses, by resolution: %v", narrowed)
}

// sampleSchemas pairs each document the tier writes, as committed under samples/,
// with the schema the bundle attaches to that response. The API definition is an
// OpenAPI document, which no schema of the standard describes.
var sampleSchemas = map[string]string{
	"landing-page.json":                   "landingPage",
	"conformance-declaration.json":        "confClasses",
	"collections.json":                    "collections",
	"collection.json":                     "collection",
	"moving-feature-collection.json":      "movingFeatures",
	"moving-feature.json":                 "movingFeature",
	"temporal-geometry-sequence.json":     "temporalGeometrySequence",
	"temporal-geometry-distance.json":     "temporalProperty",
	"temporal-geometry-velocity.json":     "temporalProperty",
	"temporal-geometry-acceleration.json": "exception",
	"temporal-properties.json":            "temporalProperties",
	"temporal-property.json":              "temporalProperty",
}

// corpusDoc is a document that exists today: one the tier writes, or one OGC publishes.
type corpusDoc struct {
	what, schema string
	doc          any
}

// corpus answers every document the tier writes and every example the bundle pairs
// with a named schema (#publishedExamples).
func corpus(t *testing.T) (samples, examples []corpusDoc) {
	t.Helper()
	files, err := filepath.Glob("samples/*.json")
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, f := range files {
		b := filepath.Base(f)
		if b == "api-definition.json" {
			continue
		}
		name, ok := sampleSchemas[b]
		if !ok {
			t.Errorf("samples/%s pairs with no schema of the standard: add it to sampleSchemas", b)
			continue
		}
		seen[b] = true
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		samples = append(samples, corpusDoc{"samples/" + b, name, decode(t, string(raw))})
	}
	for b := range sampleSchemas {
		if !seen[b] {
			t.Errorf("samples/%s is paired with a schema and missing", b)
		}
	}
	pairs, _ := publishedExamples(t)
	for _, p := range pairs {
		b, err := json.Marshal(p.example)
		if err != nil {
			t.Fatal(err)
		}
		examples = append(examples, corpusDoc{"the example at " + p.where, p.name, decode(t, string(b))})
	}
	sort.Slice(examples, func(i, j int) bool { return examples[i].what < examples[j].what })
	return samples, examples
}

// resolvedExampleFailures is how many of the standard's own examples fail the schema
// they are attached to once resolved, under either reading of `format`: the five the
// wiki finds wrong against a right schema (a flat `extent.spatial.bbox` in Collection
// and Collections; a `trs` whose `properties` is a string in MovingFeature,
// MovingFeatures and TemporalGeometrySequence). Published, the count is
// #publishedExampleFailures or #publishedExampleFailuresDefault according to the
// reading; resolved, it is the same under both, which is resolutions 1 and 2 acting
// on the standard's own material.
const resolvedExampleFailures = 5

// Every document the tier writes satisfies the resolved schema under either reading,
// and the standard's own examples fail it only where the example is wrong.
func TestATSResolvedSchemaDocuments(t *testing.T) {
	samples, examples := corpus(t)
	resolved, _ := resolvedOGC(t)
	for _, r := range readings {
		schema := compileOGC(t, resolved, r.assert)
		for _, d := range samples {
			if err := schema(schemaPointer(d.schema)).Validate(d.doc); err != nil {
				t.Errorf("%s does not satisfy the resolved %s schema with %s:\n%v",
					d.what, d.schema, r.what, firstLines(err.Error(), 6))
			}
		}
		var failed []string
		for _, d := range examples {
			if err := schema(schemaPointer(d.schema)).Validate(d.doc); err != nil {
				failed = append(failed, d.schema)
				t.Logf("with %s, %s fails the resolved %s schema:\n  %v",
					r.what, d.what, d.schema, firstLines(err.Error(), 3))
			}
		}
		sort.Strings(failed)
		t.Logf("with %s: %d of %d published examples fail the resolved schema", r.what, len(failed), len(examples))
		if len(failed) != resolvedExampleFailures {
			t.Errorf("with %s, %d of %d published examples fail the resolved schema, want %d: %v",
				r.what, len(failed), len(examples), resolvedExampleFailures, failed)
		}
		for _, f := range failed {
			switch f {
			case "collection", "collections", "movingFeature", "movingFeatures", "temporalGeometrySequence":
			default:
				t.Errorf("with %s, a published %s example fails the resolved schema, though the wiki "+
					"finds the examples of that schema right", r.what, f)
			}
		}
	}
}

// Every document that exists today and satisfies the published schema satisfies the
// resolved one: no resolution takes conformance from a document the tier writes or
// the standard publishes. Measured under each reading of `format`; a document the
// resolutions make conformant is logged, never one they make non-conformant.
func TestATSResolvedSchemaKeepsEveryDocument(t *testing.T) {
	samples, examples := corpus(t)
	resolved, _ := resolvedOGC(t)
	pub := publishedOGC(t)
	for _, r := range readings {
		pubSchema, resSchema := compileOGC(t, pub, r.assert), compileOGC(t, resolved, r.assert)
		var gained, kept int
		for _, d := range append(append([]corpusDoc{}, samples...), examples...) {
			p := schemaPointer(d.schema)
			before, after := pubSchema(p).Validate(d.doc) == nil, resSchema(p).Validate(d.doc) == nil
			switch {
			case before && !after:
				t.Errorf("with %s, %s satisfies the published %s schema and not the resolved one",
					r.what, d.what, d.schema)
			case before:
				kept++
			case after:
				gained++
				t.Logf("with %s, %s satisfies the resolved %s schema and not the published one",
					r.what, d.what, d.schema)
			}
		}
		t.Logf("with %s: %d documents conformant before and after, %d conformant only once resolved",
			r.what, kept, gained)
	}
}

// Resolving reads the vendored document and writes nothing: the bytes on disk are
// still what OGC publishes, as #TestATSSchemaBundleMatchesOGC compares them.
func TestATSResolvedSchemaLeavesTheVendoredCopy(t *testing.T) {
	before, err := os.ReadFile(ogcBundlePath)
	if err != nil {
		t.Fatal(err)
	}
	resolvedOGC(t)
	after, err := os.ReadFile(ogcBundlePath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatalf("resolving %s changed it on disk", ogcBundlePath)
	}
}
