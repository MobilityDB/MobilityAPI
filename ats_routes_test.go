// Every route the service serves is a requirement of a conformance class it
// declares, or a MobilityAPI extension the API definition names as one.
//
// The routing table and the API definition are both read from #routeTable, and
// this file holds the three to each other: a route marked with a class the
// service does not declare, a route the API definition does not describe, an
// operation described that no route serves, and a route an abstract test of
// Annex A issues that is marked as an extension, each fail here.
package main

import (
	"encoding/json"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
)

// declaredClasses reads the conformance declaration the service writes.
func declaredClasses(t *testing.T) map[string]bool {
	t.Helper()
	rec := httptest.NewRecorder()
	conformance(rec, httptest.NewRequest("GET", "/conformance", nil))
	var doc struct {
		ConformsTo []string `json:"conformsTo"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &doc); err != nil {
		t.Fatal(err)
	}
	out := map[string]bool{}
	for _, c := range doc.ConformsTo {
		out[c] = true
	}
	return out
}

// apiOperations reads the API definition the service writes: each operation,
// keyed "METHOD path", with its marking.
func apiOperations(t *testing.T) map[string]map[string]any {
	t.Helper()
	rec := httptest.NewRecorder()
	apiDoc(rec, httptest.NewRequest("GET", "/api", nil))
	var doc struct {
		Paths map[string]map[string]map[string]any `json:"paths"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &doc); err != nil {
		t.Fatal(err)
	}
	out := map[string]map[string]any{}
	for p, item := range doc.Paths {
		for m, op := range item {
			out[strings.ToUpper(m)+" "+p] = op
		}
	}
	return out
}

// Every route is marked with a class the service declares, or as an extension;
// the API definition describes every route with that marking and describes no
// operation that no route serves.
func TestATSEveryRouteIsAClassOrAnExtension(t *testing.T) {
	declared := declaredClasses(t)
	ops := apiOperations(t)
	served := map[string]bool{}
	byClass := map[string]int{}
	for _, r := range routeTable() {
		key := r.method + " " + r.path
		if served[key] {
			t.Errorf("%s is routed twice", key)
		}
		served[key] = true
		if r.class != extension && !declared[r.class] {
			t.Errorf("%s is marked with %s, which the conformance declaration does not list", key, r.class)
		}
		op, ok := ops[key]
		if !ok {
			t.Errorf("%s is served and the API definition does not describe it", key)
			continue
		}
		class, _ := op[xConformanceClass].(string)
		ext, _ := op[xExtension].(bool)
		switch {
		case r.class == extension && (!ext || class != ""):
			t.Errorf("%s is an extension and the API definition marks it %s=%v %s=%q",
				key, xExtension, ext, xConformanceClass, class)
		case r.class != extension && (ext || class != r.class):
			t.Errorf("%s is a requirement of %s and the API definition marks it %s=%v %s=%q",
				key, r.class, xExtension, ext, xConformanceClass, class)
		}
		if r.class == extension {
			byClass["extension"]++
		} else {
			byClass[r.class]++
		}
	}
	for key := range ops {
		if !served[key] {
			t.Errorf("the API definition describes %s, which no route serves", key)
		}
	}
	if len(served) == 0 {
		t.Fatal("the route table is empty, so this test would assert nothing")
	}
	var lines []string
	for c, n := range byClass {
		lines = append(lines, c+": "+itoa(n))
	}
	sort.Strings(lines)
	t.Logf("%d routes: %s", len(served), strings.Join(lines, "; "))
}

// A route an abstract test of Annex A issues is a requirement of the class the
// test belongs to, never an extension: the marking agrees with the standard's
// own conformance instrument.
func TestATSAnnexARoutesAreClassRoutes(t *testing.T) {
	classOf := map[string]string{}
	for _, r := range routeTable() {
		classOf[r.method+" "+r.path] = r.class
	}
	var n int
	for _, a := range atsPart1 {
		if a.kind != atsRoute {
			continue
		}
		n++
		class, ok := classOf[a.method+" "+a.path]
		if !ok {
			t.Errorf("%s issues %s %s, which the route table does not hold", a.id, a.method, a.path)
			continue
		}
		if class != a.class {
			t.Errorf("%s issues %s %s, marked %q; the abstract test belongs to %s",
				a.id, a.method, a.path, class, a.class)
		}
	}
	if n == 0 {
		t.Fatal("Annex A holds no route row, so this test would assert nothing")
	}
}
