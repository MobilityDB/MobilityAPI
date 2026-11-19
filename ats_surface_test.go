// The surface of the service, route by route: for every operation #routeTable
// holds, the conformance class it is a requirement of or that it is an
// extension, and the functions MobilityDB declares that its handler applies.
//
// A binding states its surface by its object layer; MobilityAPI has none, so it
// states its surface here, in api/surface.json, which this file writes and
// holds to the code. The functions of a route are read from the source: every
// SQL call in a string literal its handler reaches, through the package
// functions it calls, kept to the names the generated tables state MobilityDB
// declares (#temporalTypes, #rasterTypes, #boxFunctions, #mathOps). A route
// whose handler reaches a branch applies every function of that branch, so the
// list is what the route can apply. A name the handler builds at run time from
// the generated tables, the MF-JSON reader <type>FromMFJSON of a collection's
// type, is the type's and is read from #temporalTypes, not from this list.
//
// Regenerate after a change to a route or a handler:
//
//	go test -run TestSurfaceIsWhatTheServiceApplies -update-surface .
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
)

const surfacePath = "api/surface.json"

var updateSurface = flag.Bool("update-surface", false, "write "+surfacePath+" from the code")

// surfaceRoute is one operation of the surface.
type surfaceRoute struct {
	Method    string   `json:"method"`
	Path      string   `json:"path"`
	Class     string   `json:"conformanceClass,omitempty"`
	Extension bool     `json:"extension,omitempty"`
	Functions []string `json:"functions"`
}

// declaredFunctions answers the functions the generated tables state MobilityDB
// declares, keyed by their lower-case name, valued by the catalog's spelling.
func declaredFunctions() map[string]string {
	out := map[string]string{}
	add := func(n string) { out[strings.ToLower(n)] = n }
	for _, t := range temporalTypes {
		for _, f := range t.Funcs {
			add(f)
		}
	}
	for _, t := range rasterTypes {
		for _, f := range t.Funcs {
			add(f)
		}
	}
	for _, fs := range boxFunctions {
		for _, f := range fs {
			add(f)
		}
	}
	for _, m := range mathOps {
		add(m.SQLName)
	}
	return out
}

// packageSource parses the non-test Go files of the package and answers each
// top-level function and each package variable by name: a handler takes SQL from
// its own literals and from tables such as tProps, whose expressions it applies.
func packageSource(t *testing.T) map[string]ast.Node {
	t.Helper()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	out := map[string]ast.Node{}
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, f, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range file.Decls {
			switch v := d.(type) {
			case *ast.FuncDecl:
				if v.Recv == nil {
					out[v.Name.Name] = v
				}
			case *ast.GenDecl:
				if v.Tok != token.VAR {
					continue
				}
				for _, spec := range v.Specs {
					vs := spec.(*ast.ValueSpec)
					for _, n := range vs.Names {
						out[n.Name] = vs
					}
				}
			}
		}
	}
	return out
}

// sqlCall matches a call in SQL text: a name followed by its opening parenthesis.
var sqlCall = regexp.MustCompile(`([A-Za-z_][A-Za-z0-9_]*)\(`)

// routingFunctions are the functions that route requests rather than answer them.
var routingFunctions = map[string]bool{"routeTable": true, "newMux": true}

// handlerName answers the function a handler is, or is a closure of. Go names a
// closure after every function enclosing it, the ones inlined into its caller
// among them (routeTable.edrQuery.func1 for the closure edrQuery returns, inlined
// into routeTable), so the handler is the innermost named function.
func handlerName(h any) string {
	full := runtime.FuncForPC(reflect.ValueOf(h).Pointer()).Name()
	parts := strings.Split(full[strings.LastIndex(full, "/")+1:], ".")[1:]
	for len(parts) > 1 && closureName.MatchString(parts[len(parts)-1]) {
		parts = parts[:len(parts)-1]
	}
	return parts[len(parts)-1]
}

// closureName matches the name Go gives an anonymous function, func1, func2, ...
var closureName = regexp.MustCompile(`^func\d+$`)

// reachedFunctions answers the declared functions the SQL literals reachable from
// the function root apply, following the package functions and variables it
// names.
func reachedFunctions(decls map[string]ast.Node, declared map[string]string, root string) []string {
	seen := map[string]bool{}
	found := map[string]bool{}
	var visit func(name string)
	visit = func(name string) {
		fd, ok := decls[name]
		if !ok || seen[name] {
			return
		}
		seen[name] = true
		ast.Inspect(fd, func(n ast.Node) bool {
			switch v := n.(type) {
			case *ast.BasicLit:
				if v.Kind == token.STRING {
					s, err := strconv.Unquote(v.Value)
					if err != nil {
						return true
					}
					for _, m := range sqlCall.FindAllStringSubmatch(s, -1) {
						if spelled, ok := declared[strings.ToLower(m[1])]; ok {
							found[spelled] = true
						}
					}
				}
			case *ast.Ident:
				// A package function or variable named here. The
				// route table and the router name every handler and apply none, so
				// the walk does not enter them: the API definition reads the table
				// to mark its operations and applies nothing the routes apply.
				if _, ok := decls[v.Name]; ok && !routingFunctions[v.Name] {
					visit(v.Name)
				}
			}
			return true
		})
	}
	visit(root)
	out := make([]string, 0, len(found))
	for f := range found {
		out = append(out, f)
	}
	sort.Strings(out)
	return out
}

// surface builds the surface of the service from the route table and the source.
func surface(t *testing.T) []surfaceRoute {
	t.Helper()
	decls := packageSource(t)
	declared := declaredFunctions()
	var out []surfaceRoute
	for _, r := range routeTable() {
		root := handlerName(r.handler)
		if _, ok := decls[root]; !ok || routingFunctions[root] {
			t.Fatalf("%s %s: its handler resolves to %s, which is no handler of the package source",
				r.method, r.path, root)
		}
		s := surfaceRoute{Method: r.method, Path: r.path, Class: r.class, Extension: r.class == extension,
			Functions: reachedFunctions(decls, declared, root)}
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Path != out[j].Path {
			return out[i].Path < out[j].Path
		}
		return out[i].Method < out[j].Method
	})
	return out
}

// The committed surface is what the code applies; -update-surface rewrites it.
func TestSurfaceIsWhatTheServiceApplies(t *testing.T) {
	b, err := json.MarshalIndent(surface(t), "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	b = append(b, '\n')
	if *updateSurface {
		if err := os.WriteFile(surfacePath, b, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	committed, err := os.ReadFile(surfacePath)
	if err != nil {
		t.Fatalf("%v: write it with go test -run TestSurfaceIsWhatTheServiceApplies -update-surface .", err)
	}
	if !bytes.Equal(committed, b) {
		t.Errorf("%s is not what the code applies; rewrite it with "+
			"go test -run TestSurfaceIsWhatTheServiceApplies -update-surface .", surfacePath)
	}
}

// The reading finds what a handler is known to apply: the TemporalGeometrySequence
// is written by asMFJSON over sequenceN, the coverage by the raster writers, and
// the health check applies nothing.
func TestSurfaceReadsKnownHandlers(t *testing.T) {
	byKey := map[string][]string{}
	for _, s := range surface(t) {
		byKey[s.Method+" "+s.Path] = s.Functions
	}
	has := func(key, fn string) bool {
		for _, f := range byKey[key] {
			if f == fn {
				return true
			}
		}
		return false
	}
	for _, c := range []struct{ key, fn string }{
		{"GET /collections/{cid}/items/{fid}/tgsequence", "asMFJSON"},
		{"GET /collections/{cid}/items/{fid}/tgsequence", "sequenceN"},
		{"GET /collections/{cid}/items/{fid}/tgsequence/{tgid}/{qtype}", "interp"},
		{"GET /collections/{cid}/items/{fid}/tgsequence/{tgid}/{qtype}", "cumulativeLength"},
		{"GET /collections/{cid}/items/{fid}/tgsequence/{tgid}/{qtype}", "speed"},
		{"GET /collections/{cid}/coverage", "rasterUnion"},
		{"GET /collections/{cid}/position", "asCoverageJSON"},
	} {
		if !has(c.key, c.fn) {
			t.Errorf("%s does not list %s, which its handler applies: %v", c.key, c.fn, byKey[c.key])
		}
	}
	if fs := byKey["GET /health"]; len(fs) != 0 {
		t.Errorf("GET /health lists %v, though it applies no function", fs)
	}
}
