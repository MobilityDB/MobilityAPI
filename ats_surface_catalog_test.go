// The surface of the service read against what MobilityDB declares.
//
// api/surface.json states, route by route, the functions each handler applies
// (#TestSurfaceIsWhatTheServiceApplies). This file reads them against the
// generated tables: every function a route applies is one MobilityDB states over
// a type the route serves, and, per temporal type, it reports which raster and
// input and output functions the routes reach and which routes a collection of
// that type answers in full.
//
// A route serves the types of the collections it reads. A Coverages or EDR route
// reads a raster collection, a collection-level route (/collections,
// /collections/{cid}) registers and describes a collection of either kind, and
// every other route reads a moving feature's temporal value. Each also serves
// the box and span types (#boxFunctions), the bounds a query is given and the
// extent a collection states. MobilityAPI is measured here rather than by the
// binding matrices of MobilityDB, which read an object layer a server has none of.
package main

import (
	"encoding/json"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// routeServes answers the kinds of collection a route reads: raster, temporal,
// or both.
func routeServes(s surfaceRoute) (temporal, raster bool) {
	if s.Path == "/collections" || s.Path == "/collections/{cid}" {
		return true, true
	}
	// The EDR queries a collection of moving features answers as well
	for _, q := range edrFeatureQueries {
		if s.Path == "/collections/{cid}/"+q.name {
			return true, true
		}
	}
	if strings.HasPrefix(s.Class, coverageConfBase) || s.Class == edrConformance {
		return false, true
	}
	return true, false
}

// functionsOver answers the functions the generated tables state over the
// temporal types, over the raster types and over the box and span types.
func functionsOver() (temporal, raster, box map[string]bool) {
	temporal, raster, box = map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, t := range temporalTypes {
		for _, f := range t.Funcs {
			temporal[f] = true
		}
	}
	for _, t := range rasterTypes {
		for _, f := range t.Funcs {
			raster[f] = true
		}
	}
	for b, fs := range boxFunctions {
		box[b] = true // the box's own constructor, stbox(trip)
		for _, f := range fs {
			box[f] = true
		}
	}
	return temporal, raster, box
}

// committedSurface reads api/surface.json.
func committedSurface(t *testing.T) []surfaceRoute {
	t.Helper()
	b, err := os.ReadFile(surfacePath)
	if err != nil {
		t.Fatal(err)
	}
	var out []surfaceRoute
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if len(out) == 0 {
		t.Fatalf("%s holds no route, so this test would assert nothing", surfacePath)
	}
	return out
}

// misplaced answers the functions a route applies that MobilityDB states over no
// type the route serves, reading the types as #routeServes and #functionsOver do.
func misplaced(s surfaceRoute) []string {
	temporal, raster, box := functionsOver()
	servesTemporal, servesRaster := routeServes(s)
	var out []string
	for _, f := range s.Functions {
		if !box[f] && !(servesTemporal && temporal[f]) && !(servesRaster && raster[f]) {
			out = append(out, f)
		}
	}
	return out
}

// Every function a route applies is one MobilityDB states over a type the route
// serves, over the surface #TestSurfaceIsWhatTheServiceApplies holds to the code.
func TestSurfaceFunctionsAreStatedOverTheServedTypes(t *testing.T) {
	var checked int
	for _, s := range committedSurface(t) {
		checked += len(s.Functions)
		if m := misplaced(s); len(m) > 0 {
			t.Errorf("%s %s applies %v, which MobilityDB states over no type the route serves", s.Method, s.Path, m)
		}
	}
	if checked == 0 {
		t.Fatal("the surface applies no function, so this test would assert nothing")
	}
}

// The check refuses what it exists to refuse, as #TestATSSchemaMotionCurveIsInverted
// pairs a check with the cases it must refuse: a raster writer on a
// moving-feature route, a trajectory function on a raster route, and nothing a
// collection-level route applies to a collection of either kind.
func TestSurfaceMisplacedFunctionsAreFound(t *testing.T) {
	for _, c := range []struct {
		s    surfaceRoute
		want []string
	}{
		{surfaceRoute{Path: "/collections/{cid}/items", Class: confMovingFeatures,
			Functions: []string{"asMFJSON", "asTIFF"}}, []string{"asTIFF"}},
		{surfaceRoute{Path: "/collections/{cid}/coverage", Class: coverageConfBase + "core",
			Functions: []string{"asTIFF", "cumulativeLength"}}, []string{"cumulativeLength"}},
		{surfaceRoute{Path: "/collections", Class: confMFCollection,
			Functions: []string{"SRID", "setSRID", "stbox"}}, nil},
	} {
		if got := misplaced(c.s); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s applying %v: misplaced %v, want %v", c.s.Path, c.s.Functions, got, c.want)
		}
	}
}

// isIOFunction reports a function that writes or reads a value in an external
// format: a writer as<Format> or a reader <type>From<Format>.
func isIOFunction(f string) bool {
	return strings.HasPrefix(f, "as") || strings.Contains(f, "From")
}

// Per temporal type: the raster and input and output functions the moving-feature
// routes reach that MobilityDB states over the type, and the routes a collection
// of the type answers in full, every function they apply being stated over it.
// It is a report: a route a type does not answer in full refuses that collection
// by name (#typeHas), which is what the service states.
func TestSurfacePerCollectionType(t *testing.T) {
	_, raster, box := functionsOver()
	surface := committedSurface(t)
	for _, tt := range temporalTypes {
		if !tt.Spatial {
			continue
		}
		has := map[string]bool{}
		for _, f := range tt.Funcs {
			has[f] = true
		}
		boxHas := map[string]bool{tt.Box: true}
		for _, f := range boxFunctions[tt.Box] {
			boxHas[f] = true
		}
		var io, rasterReached []string
		seen := map[string]bool{}
		var full, partial int
		for _, s := range surface {
			if servesTemporal, _ := routeServes(s); !servesTemporal || len(s.Functions) == 0 {
				continue
			}
			answered := true
			for _, f := range s.Functions {
				answered = answered && (has[f] || boxHas[f])
				if seen[f] || !has[f] {
					continue
				}
				seen[f] = true
				if isIOFunction(f) {
					io = append(io, f)
				}
				if raster[f] && !box[f] {
					rasterReached = append(rasterReached, f)
				}
			}
			if answered {
				full++
			} else {
				partial++
			}
		}
		sort.Strings(io)
		sort.Strings(rasterReached)
		t.Logf("%-12s routes answered in full %2d, in part %2d; input and output %v; raster %v",
			tt.Name, full, partial, io, rasterReached)
	}
}
