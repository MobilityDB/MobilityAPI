package main

// Mirrors #TestCatalogIsPopulated of catalog_test.go: it holds the generated
// table to the code that reads it rather than to a list of names.

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// tripCall matches a chain of SQL calls whose innermost argument is the trip
// column, or the clipped expression the handlers splice in its place:
// asMFJSON(transform(tsample(trip, ...)), numSequences("+expr+").
var tripCall = regexp.MustCompile(`((?:[A-Za-z_][A-Za-z0-9_]*\(\s*)+)(?:trip\b|"\s*\+\s*expr\b)`)

var callName = regexp.MustCompile(`([A-Za-z_][A-Za-z0-9_]*)\(`)

// Every SQL function the tier applies to a feature's temporal geometry must be
// one featureFunctions lists, since each TemporalType's Funcs, which decides
// what a collection of that type can answer, is read from that list.
func TestEveryFunctionOverTripIsAFeatureFunction(t *testing.T) {
	listed := map[string]bool{}
	for _, f := range featureFunctions {
		listed[strings.ToLower(f)] = true
	}
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	called := map[string][]string{}
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") || strings.HasSuffix(file, "_gen.go") {
			continue
		}
		src, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		for _, chain := range tripCall.FindAllStringSubmatch(string(src), -1) {
			for _, m := range callName.FindAllStringSubmatch(chain[1], -1) {
				name := m[1]
				// The reader of a write, tgeompointFromMFJSON and its siblings,
				// is the type's own and recorded per type apart from this list.
				if strings.HasSuffix(name, "FromMFJSON") {
					continue
				}
				called[name] = append(called[name], file)
			}
		}
	}
	if len(called) == 0 {
		t.Fatal("found no SQL call over trip: the pattern no longer matches how the tier writes its SQL")
	}
	var missing []string
	for name, where := range called {
		if !listed[strings.ToLower(name)] {
			missing = append(missing, name+" ("+strings.Join(where, ", ")+")")
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("SQL functions applied to trip that featureFunctions does not list, so no "+
			"TemporalType records whether MobilityDB declares them: %s; add them to "+
			"tools/codegen/main.go and regenerate", strings.Join(missing, "; "))
	}
}
