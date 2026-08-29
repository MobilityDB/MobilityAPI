// The sample documents: one per resource the tier serves, written by the tier
// itself.
//
// A sample assembled by hand states what its author believed the service
// answers. These state what it answers, because every one is the body of a real
// request through the service's own routing table. Regenerating rewrites the
// whole set, so a sample that has drifted from the code cannot survive one.
//
// The emitter names no collection, feature or property of its own. It reads the
// collections the service lists and takes the first, then that collection's
// features and takes the first, then that feature's temporal properties — the
// walk a client makes through the documents' own contents. So it answers for
// whatever database it is pointed at, and against
// tutorial/setup/load_conformance.sql it writes the conformance sample set.
//
//	MFAPI_DSN=... mfapi -emit samples
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// sample is one resource of the tier: the request that reads it, the OGC
// resource the answer is an instance of, and what the tier answered.
type sample struct {
	file string // basename the document is written under
	path string // the request the tier answers
	what string // the resource this document is an instance of
	code int
	body []byte
}

// firstMember reads the first element of a named array out of a document and
// returns one of its fields as text. It is how the emitter finds a collection,
// a feature and a temporal property without naming any of them.
func firstMember(body []byte, array, field string) (string, error) {
	var doc map[string]any
	if err := json.Unmarshal(body, &doc); err != nil {
		return "", fmt.Errorf("the %s document is not JSON: %w", array, err)
	}
	members, ok := doc[array].([]any)
	if !ok || len(members) == 0 {
		return "", fmt.Errorf("the document carries no %s to sample", array)
	}
	m, ok := members[0].(map[string]any)
	if !ok {
		return "", fmt.Errorf("the first member of %s is not an object", array)
	}
	switch v := m[field].(type) {
	case string:
		return v, nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case nil:
		return "", fmt.Errorf("the first member of %s carries no %s", array, field)
	default:
		return fmt.Sprint(v), nil
	}
}

// emitSamples writes one document per resource into dir, plus an index naming
// each one and the request that produced it.
func emitSamples(dir string) error {
	mux := newMux()
	read := func(path string) (int, []byte) {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		return rec.Code, rec.Body.Bytes()
	}
	// A resource the walk needs in order to reach the next one has to answer, or
	// the set below it would be silently missing rather than wrong.
	require := func(path string) ([]byte, error) {
		code, body := read(path)
		if code != http.StatusOK {
			return nil, fmt.Errorf("GET %s answered %d, so the walk cannot continue: %s",
				path, code, strings.TrimSpace(string(body)))
		}
		return body, nil
	}

	cols, err := require("/collections")
	if err != nil {
		return err
	}
	cid, err := firstMember(cols, "collections", "id")
	if err != nil {
		return err
	}
	collection := "/collections/" + cid

	items, err := require(collection + "/items")
	if err != nil {
		return err
	}
	fid, err := firstMember(items, "features", "id")
	if err != nil {
		return err
	}
	feature := collection + "/items/" + fid

	props, err := require(feature + "/tproperties")
	if err != nil {
		return err
	}
	pname, err := firstMember(props, "temporalProperties", "name")
	if err != nil {
		return err
	}

	// A feature's temporal primitive geometries are addressed by their 1-based
	// position, which is what sequenceN takes, so the first one is 1.
	const firstSequence = "1"
	tgseq := feature + "/tgsequence/" + firstSequence

	samples := []sample{
		{file: "landing-page", path: "/", what: "the landing page"},
		{file: "api-definition", path: "/api", what: "the API definition"},
		{file: "conformance-declaration", path: "/conformance", what: "the conformance declaration"},
		{file: "collections", path: "/collections", what: "the Collections document"},
		{file: "collection", path: collection, what: "a Collection document"},
		{file: "moving-feature-collection", path: collection + "/items", what: "a MovingFeatureCollection document"},
		{file: "moving-feature", path: feature, what: "a MovingFeature document"},
		{file: "temporal-geometry-sequence", path: feature + "/tgsequence", what: "a TemporalGeometrySequence document"},
		{file: "temporal-geometry-distance", path: tgseq + "/distance", what: "the distance a temporal primitive geometry covers"},
		{file: "temporal-geometry-velocity", path: tgseq + "/velocity", what: "the velocity along a temporal primitive geometry"},
		{file: "temporal-geometry-acceleration", path: tgseq + "/acceleration", what: "the acceleration along a temporal primitive geometry"},
		{file: "temporal-properties", path: feature + "/tproperties", what: "a TemporalProperties document"},
		{file: "temporal-property", path: feature + "/tproperties/" + pname, what: "a TemporalProperty document"},
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	for i := range samples {
		s := &samples[i]
		s.code, s.body = read(s.path)
		name, out := s.file+".json", &bytes.Buffer{}
		if json.Valid(s.body) {
			if err := json.Indent(out, s.body, "", "  "); err != nil {
				return fmt.Errorf("indenting %s: %w", s.path, err)
			}
			out.WriteByte('\n')
		} else {
			// A response the service does not write as JSON is kept verbatim: the
			// sample is the answer, not a rendering of it.
			name = s.file + ".txt"
			out.Write(s.body)
		}
		if err := os.WriteFile(filepath.Join(dir, name), out.Bytes(), 0o644); err != nil {
			return err
		}
	}
	return writeSampleIndex(dir, cid, fid, pname, samples)
}

// writeSampleIndex writes the README naming every sample, the request that
// produced it and the status the tier answered with.
func writeSampleIndex(dir, cid, fid, pname string, samples []sample) error {
	var b strings.Builder
	b.WriteString(`# Sample documents

Every file here is the body of one request through the service's own routing
table, written by the service itself:

    MFAPI_DSN=... mfapi -emit samples

so a sample states what the code answers rather than what its author believed
it answers. Regenerating rewrites the whole set.

These were read from the conformance fixture
(` + "`tutorial/setup/load_conformance.sql`" + `), whose collection is ` + "`" + cid + "`" + `, whose
first feature is ` + "`" + fid + "`" + ` and whose first temporal property is ` + "`" + pname + "`" + `. Pointed at
another database the emitter names that database's own resources instead: it
takes the first collection the service lists, that collection's first feature
and that feature's first temporal property.

A status other than 200 is a sample too. The tier answers 501 for an
acceleration under linear interpolation, because a velocity that is constant on
every segment has no acceleration the standard's motion model can carry, and
saying so is the honest answer rather than a zero.

⛔ The ` + "`timeStamp`" + ` of the TemporalProperties document is the moment the
document was written, which the standard defines it to be, so that one line
differs between two regenerations of an unchanged service.

| file | status | request | resource |
|---|---|---|---|
`)
	for _, s := range samples {
		name := s.file + ".json"
		if !json.Valid(s.body) {
			name = s.file + ".txt"
		}
		fmt.Fprintf(&b, "| [`%s`](%s) | %d | `GET %s` | %s |\n", name, name, s.code, s.path, s.what)
	}
	return os.WriteFile(filepath.Join(dir, "README.md"), []byte(b.String()), 0o644)
}
