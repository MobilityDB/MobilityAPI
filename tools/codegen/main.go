// Command codegen writes catalog_gen.go, the tier's table of MEOS temporal
// types, from the MEOS-API catalog.
//
// meos-idl.json is the machine-readable description of the MEOS C library that
// MobilityDB/MEOS-API derives from the MEOS headers and sources, and that every
// binding in the ecosystem generates from. Its `temporalTypes` registry states,
// per Temporal<T>, the facts this tier needs: the base type, the bounding box,
// the MF-JSON type token asMFJSON writes, and whether the type is spatial, a
// number, and linearly interpolated. Reading them here is what makes this tier
// a projection of the same catalog as PyMEOS, JMEOS and the rest, rather than a
// reader of MEOS's C with a parse of its own to keep working.
//
// Usage:
//
//	go run ./tools/codegen -catalog <meos-idl.json> -o catalog_gen.go
//
// The catalog is derived, never committed: CI generates it from a MobilityDB
// commit with MobilityDB/MEOS-API's provision-meos action, regenerates this
// file against it and refuses a difference, so a type MEOS gains reaches this
// tier as a failing check rather than as silence.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"go/format"
	"os"
	"sort"
	"strings"
)

// catalog is the part of meos-idl.json this generator reads.
type catalog struct {
	TemporalTypes map[string]temporalType `json:"temporalTypes"`
	Functions     []function              `json:"functions"`
	Aggregates    []sqlSignature          `json:"aggregates"`
	Compositions  []sqlSignature          `json:"compositions"`
	Structs       []cStruct               `json:"structs"`
}

// function is the part of a catalog function entry the tables read: the C
// symbol, the SQL name that is the cross-engine invariant, the Doxygen group and
// category that place it, the family and API level MEOS gives it, the
// aggregates it supports, and the parameter C types.
type function struct {
	Name          string         `json:"name"`
	Group         string         `json:"group"`
	Category      string         `json:"category"`
	Family        string         `json:"family"`
	API           string         `json:"api"`
	SQLName       string         `json:"sqlfn"`
	SQLAgg        []string       `json:"sqlAgg"`
	Params        []param        `json:"params"`
	Returns       ctype          `json:"returnType"`
	SQLSignatures []sqlSignature `json:"sqlSignatures"`
}

// sqlSignature is one SQL signature a C function backs: its argument and result
// types, where it differs from the function's sqlfn the SQL name it carries, and
// the support role it plays for a type (input, output, receive, send). An
// aggregate states the same, with the MEOS function its transition calls.
type sqlSignature struct {
	Args        []string    `json:"args"`
	Ret         string      `json:"ret"`
	SQLName     string      `json:"sqlName"`
	TypeSupport string      `json:"typeSupport"`
	Transition  *transition `json:"transition"`
}

// transition is the MEOS function an aggregate's transition calls.
type transition struct {
	Meos string `json:"meos"`
}

// cStruct is one C struct of the catalog: its family, and the functions that
// read and write its text form.
type cStruct struct {
	Name          string        `json:"name"`
	Family        string        `json:"family"`
	Serialization serialization `json:"serialization"`
}

// serialization names the functions that read (In) and write (Out) a struct's
// text form.
type serialization struct {
	In  string `json:"in"`
	Out string `json:"out"`
}

// featureFunctions are the SQL functions the tier applies to a feature's
// temporal geometry (the trip column). Which of them MobilityDB declares over a
// temporal type decides what a collection of that type can answer, so the
// generator records it per type, and trip_functions_test.go holds this list to
// the functions the tier's SQL calls.
var featureFunctions = []string{
	"appendInstant", "asBinary", "asMFJSON", "atTime", "cumulativeLength",
	"deleteTime", "eDwithin", "eIntersects", "eOverlaps", "extent", "getTime", "interp", "merge", "numInstants",
	"numSequences", "rasterTileValue", "rasterValue", "sequenceN", "setSRID",
	"speed", "tgeogpoint", "timeSpan", "trajectory", "transform", "tsample",
}

// typeFunctions answers, per temporal type, the featureFunctions MobilityDB
// declares with that type as first argument, and the type's MF-JSON reader
// <type>FromMFJSON where it has one. Like #mathOperations it selects from the
// catalog by the functions' own metadata alone, here their SQL signatures; a
// signature carries its own sqlName where it differs from the function's sqlfn.
func typeFunctions(fns []function, aggs, comps []sqlSignature, types map[string]temporalType) map[string][]string {
	wanted := map[string]bool{}
	for _, n := range featureFunctions {
		wanted[n] = true
	}
	// The box of a feature's temporal geometry is built by the constructor its
	// type's bounding box names (stbox(trip), tpcbox(trip) for a point cloud).
	for _, t := range types {
		if t.Box != "" {
			wanted[t.Box] = true
		}
	}
	have := map[string]map[string]bool{}
	for name, t := range types {
		have[name] = map[string]bool{}
		// The bounding box of a feature's temporal geometry is read the same
		// way, the collection's extent transforming it where its type can be.
		if t.Box != "" {
			have[t.Box] = map[string]bool{}
		}
	}
	for _, f := range fns {
		for _, s := range f.SQLSignatures {
			name := s.SQLName
			if name == "" {
				name = f.SQLName
			}
			if name == "" || len(s.Args) == 0 {
				continue
			}
			if t, ok := have[s.Args[0]]; ok && wanted[name] {
				t[name] = true
			}
			if s.Args[0] == "text" && strings.HasSuffix(name, "FromMFJSON") {
				if t, ok := have[strings.TrimSuffix(name, "FromMFJSON")]; ok {
					t[name] = true
				}
			}
		}
	}
	// An aggregate (extent) states its SQL name and arguments in the catalog's
	// aggregates registry, as a scalar signature does.
	for _, a := range aggs {
		if len(a.Args) == 0 {
			continue
		}
		if t, ok := have[a.Args[0]]; ok && wanted[a.SQLName] {
			t[a.SQLName] = true
		}
	}
	// A composition (eIntersects(tnpoint, geometry), the relationship of the
	// temporal point a tnpoint casts to) is a SQL function MobilityDB declares
	// with no C symbol of its own, which the catalog states in its compositions
	// registry by its SQL name and arguments, as an aggregate is stated.
	for _, c := range comps {
		if len(c.Args) == 0 {
			continue
		}
		if t, ok := have[c.Args[0]]; ok && wanted[c.SQLName] {
			t[c.SQLName] = true
		}
	}
	out := map[string][]string{}
	for tname, t := range have {
		for n := range t {
			out[tname] = append(out[tname], n)
		}
		sort.Strings(out[tname])
	}
	return out
}

// rasterFamily is the MEOS family whose types a raster collection holds.
const rasterFamily = "RASTER"

// rasterFunctions answers, per raster type, the SQL functions MobilityDB
// declares over it, which are what a raster collection can answer. It is the
// raster counterpart of #typeFunctions, reading the same SQL signatures and
// aggregates and crediting a signature's own sqlName where it carries one.
//
// The raster types are the SQL types of the structs of the family: a struct
// states the function reading its text form, and that function's SQL signature
// answers the type, so `raster` and `raquet` are read from the catalog rather
// than listed here. A struct whose reader states no SQL signature is no type a
// raster collection holds: BandStats is read and written as the summarystats
// record PostGIS defines, which a function answers and no column is, the set
// tools/raster_matrix.py names as RASTER_TYPES.
//
// The functions are those MobilityDB/MobilityDB tools/raster_matrix.py counts
// as the raster surface, by the same rule: the public functions of the family
// carrying a SQL name, less the support functions of a type (a signature with a
// typeSupport role, raquet_in) and of an aggregate (sqlAgg, rasterUnionCombine),
// and the aggregates whose transition is a function of the family, under the
// aggregate's own name. A function belongs to every raster type one of its
// signatures names, as an argument or as its result, an array of the type
// included, so that rasterValue(tgeompoint, raster) is a function of `raster`.
func rasterFunctions(c catalog) (map[string][]string, error) {
	byName := map[string]*function{}
	family := map[string]bool{}
	for i := range c.Functions {
		f := &c.Functions[i]
		byName[f.Name] = f
		if f.Family == rasterFamily {
			family[f.Name] = true
		}
	}
	have := map[string]map[string]bool{}
	for _, s := range c.Structs {
		if s.Family != rasterFamily || s.Serialization.In == "" {
			continue
		}
		in, ok := byName[s.Serialization.In]
		if !ok {
			return nil, fmt.Errorf("the struct %s reads its text form with %s, which the "+
				"catalog does not declare", s.Name, s.Serialization.In)
		}
		if len(in.SQLSignatures) == 0 || in.SQLSignatures[0].Ret == "" {
			continue
		}
		have[in.SQLSignatures[0].Ret] = map[string]bool{}
	}
	// add credits name to every raster type a signature names.
	add := func(name string, sig sqlSignature) {
		for _, t := range append(append([]string{}, sig.Args...), sig.Ret) {
			if fs, ok := have[strings.ToLower(strings.TrimSuffix(strings.TrimSpace(t), "[]"))]; ok {
				fs[name] = true
			}
		}
	}
	for _, f := range c.Functions {
		if f.Family != rasterFamily || f.API != "public" || f.SQLName == "" || len(f.SQLAgg) > 0 {
			continue
		}
		support := false
		for _, s := range f.SQLSignatures {
			support = support || s.TypeSupport != ""
		}
		if support {
			continue
		}
		for _, s := range f.SQLSignatures {
			name := s.SQLName
			if name == "" {
				name = f.SQLName
			}
			add(name, s)
		}
	}
	for _, a := range c.Aggregates {
		if a.Transition != nil && family[a.Transition.Meos] {
			add(a.SQLName, a)
		}
	}
	out := map[string][]string{}
	for typ, fs := range have {
		out[typ] = []string{}
		for n := range fs {
			out[typ] = append(out[typ], n)
		}
		sort.Strings(out[typ])
	}
	return out, nil
}

// param is one parameter of a catalog function: its C spelling as the header
// writes it, and its canonical spelling, which is the C spelling with every
// typedef the catalog states resolved.
type param struct {
	Name      string `json:"name"`
	CType     string `json:"cType"`
	Canonical string `json:"canonical"`
}

type ctype struct {
	C string `json:"c"`
}

// temporalType is one entry of the catalog's temporalTypes registry. MFJSON is
// absent for a type asMFJSON does not write, so an empty token is what says a
// type has no MF-JSON form.
type temporalType struct {
	Base    string `json:"base"`
	Box     string `json:"bbox"`
	MFJSON  string `json:"mfjson"`
	Spatial bool   `json:"spatial"`
	Number  bool   `json:"number"`
	Linear  bool   `json:"linear"`
}

func main() {
	path := flag.String("catalog", "meos-idl.json", "the MEOS-API catalog")
	out := flag.String("o", "catalog_gen.go", "file to write")
	outMeos := flag.String("o-meos", "mathops_meos_gen.go", "file to write the cgo dispatch to")
	flag.Parse()

	raw, err := os.ReadFile(*path)
	if err != nil {
		fail(err)
	}
	var c catalog
	if err := json.Unmarshal(raw, &c); err != nil {
		fail(fmt.Errorf("%s: %w", *path, err))
	}
	if len(c.TemporalTypes) == 0 {
		fail(fmt.Errorf("%s carries no temporalTypes registry: it is older than the "+
			"MEOS-API that states one, or it is not a MEOS-API catalog", *path))
	}

	ops, err := mathOperations(c.Functions)
	if err != nil {
		fail(fmt.Errorf("%s: %w", *path, err))
	}
	if len(ops) == 0 {
		fail(fmt.Errorf("%s names no temporal math operation: the groups this reads, "+
			"meos_temporal_math and meos_temporal_transf, are not where they were", *path))
	}

	raster, err := rasterFunctions(c)
	if err != nil {
		fail(fmt.Errorf("%s: %w", *path, err))
	}
	if len(raster) == 0 {
		fail(fmt.Errorf("%s names no raster type: no struct of the family %s states "+
			"the function reading its text form", *path, rasterFamily))
	}

	src, err := render(c.TemporalTypes, typeFunctions(c.Functions, c.Aggregates, c.Compositions, c.TemporalTypes), ops, raster)
	if err != nil {
		fail(err)
	}
	if err := os.WriteFile(*out, src, 0o644); err != nil {
		fail(err)
	}

	dispatch, called, err := renderDispatch(ops)
	if err != nil {
		fail(err)
	}
	if err := os.WriteFile(*outMeos, dispatch, 0o644); err != nil {
		fail(err)
	}
	fmt.Fprintf(os.Stderr, "codegen: %d temporal types, %d raster types and %d math operations "+
		"written to %s, %d of them callable from %s\n", len(c.TemporalTypes), len(raster), len(ops),
		*out, called, *outMeos)
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "codegen:", err)
	os.Exit(1)
}

// mathGroups are the two Doxygen groups MEOS puts a temporal number's value
// transformations in.
var mathGroups = map[string]bool{
	"meos_temporal_math":   true,
	"meos_temporal_transf": true,
}

// mathPrefixes are the type classes whose functions accept a tfloat, which is
// the value a stream record carries. A MEOS function's leading token names the
// class it is generic over, so tfloat_, tnumber_ and temporal_ all admit one
// while tint_ and tbigint_ do not.
var mathPrefixes = map[string]bool{
	"tfloat":   true,
	"tnumber":  true,
	"temporal": true,
}

// mathOperation is one MEOS function over a temporal number that the tier can
// apply to a stream record.
type mathOperation struct {
	SQLName      string // the SQL name, which is the same on every engine
	Symbol       string // the MEOS C symbol
	Arg          string // the C type of its one further parameter, empty when it takes none
	ArgCanonical string // the canonical spelling of that type, empty when it takes none
}

// mathOperations selects, from the catalog, the functions that take a temporal
// number and answer one. The selection is the function's own metadata and
// nothing else:
//
//   - the Doxygen group places it among the value transformations;
//   - the category is transformation, which is what separates a value
//     transformation from a conversion (temporal_as_tinstant) and from an
//     accessor (tnumber_delta_value);
//   - it takes a Temporal and answers a Temporal;
//   - its leading name token names a class that admits a tfloat, or it is the
//     arithmetic form <op>_tfloat_float;
//   - it takes at most one further parameter, and that parameter is not itself
//     a Temporal, since a stream record carries one value and not two.
//
// The type of that further parameter is read by its canonical spelling, and it
// must be one argConversions fills or one argUnfillable states a query value
// cannot fill: any other type is an error, so a type the catalog gives a
// parameter reaches this tier as a failing generator rather than as a missing
// operation.
func mathOperations(fns []function) ([]mathOperation, error) {
	var out []mathOperation
	for _, f := range fns {
		if !mathGroups[f.Group] || f.Category != "transformation" || f.SQLName == "" {
			continue
		}
		if !strings.Contains(f.Returns.C, "Temporal *") {
			continue
		}
		if len(f.Params) == 0 || !strings.Contains(f.Params[0].CType, "Temporal *") {
			continue
		}
		rest := f.Params[1:]
		if len(rest) > 1 || (len(rest) == 1 && strings.Contains(rest[0].CType, "Temporal *")) {
			continue
		}
		if !mathPrefixes[strings.SplitN(f.Name, "_", 2)[0]] && !strings.HasSuffix(f.Name, "_tfloat_float") {
			continue
		}
		op := mathOperation{SQLName: f.SQLName, Symbol: f.Name}
		if len(rest) == 1 {
			op.Arg = rest[0].CType
			op.ArgCanonical = rest[0].Canonical
			if op.ArgCanonical == "" {
				return nil, fmt.Errorf("%s: the parameter %s carries no canonical spelling, "+
					"so its type cannot be read from the catalog", f.Name, rest[0].Name)
			}
			if _, ok := argConversions[op.ArgCanonical]; !ok {
				if _, ok := argUnfillable[op.ArgCanonical]; !ok {
					return nil, fmt.Errorf("%s: the parameter %s has the type %q, which is "+
						"neither filled from a query value (argConversions) nor stated as one "+
						"a query value cannot fill (argUnfillable)", f.Name, rest[0].Name, op.ArgCanonical)
				}
			}
		}
		out = append(out, op)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].SQLName != out[j].SQLName {
			return out[i].SQLName < out[j].SQLName
		}
		return out[i].Symbol < out[j].Symbol
	})
	return out, nil
}

// argConversions turn the one float64 a continuous query carries into the C
// value a MEOS function's further parameter takes, keyed by the canonical
// spelling of the parameter's type, so a typedef the catalog resolves to one of
// these types is filled as that type.
var argConversions = map[string]string{
	"":       "",
	"double": "C.double(arg)",
	"int":    "C.int(arg)",
	"bool":   "arg != 0",
}

// argUnfillable are the canonical parameter types the one float64 of a query
// cannot fill, each with the reason. An operation taking one of them gets no
// case and reaches the caller as one the dispatch does not serve. A type in
// neither this map nor argConversions stops the generator.
var argUnfillable = map[string]string{
	"const Interval *": "an interval is a duration, not a single number",
	"interpType":       "an interpolation is an enumeration, not a number",
}

// renderDispatch writes the cgo call for each operation, which is what keeps
// the C symbol behind an operation out of hand-written code: the symbol comes
// from the catalog, and a MEOS rename becomes a compile error here rather than
// a wrong call. It answers the source and how many operations it serves.
func renderDispatch(ops []mathOperation) ([]byte, int, error) {
	var b bytes.Buffer
	b.WriteString(`//go:build meos

// Code generated by tools/codegen from the MEOS-API catalog. DO NOT EDIT.
//
// Regenerate with:
//
//	go run ./tools/codegen -catalog <meos-idl.json> -o-meos mathops_meos_gen.go
//
// One case per operation the catalog states over a temporal number, calling the
// MEOS symbol the catalog names. An operation whose further parameter has a type
// the single value a query carries cannot fill, an interval or an
// interpolation, gets no case.

package main

/*
#cgo pkg-config: meos
#include <meos.h>
*/
import "C"

// callMathOp applies the operation named by its SQL name to a temporal number,
// filling any further parameter from arg. It answers false when the operation
// is not one this dispatch serves, which the caller reports as an unknown
// operation rather than as a failed call.
func callMathOp(sqlName string, temp *C.Temporal, arg float64) (*C.Temporal, bool) {
	switch sqlName {
`)
	called := 0
	for _, op := range ops {
		conv, ok := argConversions[op.ArgCanonical]
		if !ok {
			continue
		}
		called++
		fmt.Fprintf(&b, "\tcase %q:\n", op.SQLName)
		if conv == "" {
			fmt.Fprintf(&b, "\t\treturn C.%s(temp), true\n", op.Symbol)
		} else {
			fmt.Fprintf(&b, "\t\treturn C.%s(temp, %s), true\n", op.Symbol, conv)
		}
	}
	b.WriteString(`	}
	return nil, false
}
`)
	src, err := format.Source(b.Bytes())
	return src, called, err
}

// render writes catalog_gen.go, the tables the tier reads, as #renderDispatch
// writes the cgo dispatch: the temporal types with their functions, the boxes,
// the math operations, and the raster types with their functions.
func render(types map[string]temporalType, funcs map[string][]string, ops []mathOperation,
	raster map[string][]string) ([]byte, error) {
	names := make([]string, 0, len(types))
	for name := range types {
		names = append(names, name)
	}
	sort.Strings(names)

	var b bytes.Buffer
	b.WriteString(`// Code generated by tools/codegen from the MEOS-API catalog. DO NOT EDIT.
//
// Regenerate with:
//
//	go run ./tools/codegen -catalog <meos-idl.json> -o catalog_gen.go
//
// The source is meos-idl.json#/temporalTypes, which MobilityDB/MEOS-API derives
// from meos/src/temporal/meos_catalog.c (the type names, each temporal type's
// base and bounding box, and the predicates temporal_type, tnumber_type,
// tspatial_type and temptype_supports_linear) and meos/src/temporal/type_out.c
// (temptype_as_mfjson_sb, the MF-JSON type token).

package main

// TemporalType is one MEOS temporal type, as MEOS's own catalog describes it.
// MFJSON is empty for a type asMFJSON does not write, so an empty token is what
// says a type has no MF-JSON form. Funcs are the SQL functions the tier applies
// to a feature's temporal geometry that MobilityDB declares over the type, and
// its MF-JSON reader <type>FromMFJSON, read from the catalog's SQL signatures.
type TemporalType struct {
	Name    string
	Base    string
	Box     string
	MFJSON  string
	Spatial bool
	Number  bool
	Linear  bool
	Funcs   []string
}

// temporalTypes is every temporal type MEOS declares, ordered by name.
var temporalTypes = []TemporalType{
`)
	for _, name := range names {
		t := types[name]
		fmt.Fprintf(&b, "\t{Name: %q, Base: %q, Box: %q, MFJSON: %q, Spatial: %t, Number: %t, Linear: %t, Funcs: %#v},\n",
			name, t.Base, t.Box, t.MFJSON, t.Spatial, t.Number, t.Linear, funcs[name])
	}
	boxes := map[string]bool{}
	for _, t := range types {
		if t.Box != "" {
			boxes[t.Box] = true
		}
	}
	boxNames := make([]string, 0, len(boxes))
	for n := range boxes {
		boxNames = append(boxNames, n)
	}
	sort.Strings(boxNames)
	b.WriteString("}\n\n// boxFunctions are, per bounding box type of a temporal type, the\n" +
		"// featureFunctions MobilityDB declares over that box.\n" +
		"var boxFunctions = map[string][]string{\n")
	for _, n := range boxNames {
		fmt.Fprintf(&b, "\t%q: %#v,\n", n, funcs[n])
	}
	b.WriteString("}\n\n// featureFunctions are the SQL functions the tier applies to a feature's\n" +
		"// temporal geometry, the set each TemporalType's Funcs is read from.\n" +
		"var featureFunctions = ")
	fmt.Fprintf(&b, "%#v\n", featureFunctions)
	b.WriteString(`
// temporalTypeByName indexes temporalTypes by the MEOS type name.
var temporalTypeByName = func() map[string]*TemporalType {
	m := make(map[string]*TemporalType, len(temporalTypes))
	for i := range temporalTypes {
		m[temporalTypes[i].Name] = &temporalTypes[i]
	}
	return m
}()

// MathOp is one MEOS function that takes a temporal number and answers one.
// SQLName is the name the operation carries on every engine, Symbol the MEOS C
// symbol behind it, and Arg the C type of its one further parameter, empty when
// it takes none.
type MathOp struct {
	SQLName string
	Symbol  string
	Arg     string
}

// mathOps is every such function the catalog states, ordered by SQL name.
var mathOps = []MathOp{
`)
	for _, op := range ops {
		fmt.Fprintf(&b, "\t{SQLName: %q, Symbol: %q, Arg: %q},\n", op.SQLName, op.Symbol, op.Arg)
	}
	b.WriteString(`}

// mathOpBySQLName indexes mathOps by the SQL name. A name several functions
// carry keeps them all, so a caller resolves it rather than reading whichever
// entry a map happened to hold.
var mathOpBySQLName = func() map[string][]*MathOp {
	m := map[string][]*MathOp{}
	for i := range mathOps {
		m[mathOps[i].SQLName] = append(m[mathOps[i].SQLName], &mathOps[i])
	}
	return m
}()

// RasterType is one raster type of MEOS, the SQL type of a struct of the family
// RASTER. Funcs are the SQL functions MobilityDB declares over it, as an argument
// or as their result, which are what a raster collection of the type answers:
// the public functions of the family carrying a SQL name, less the support
// functions of a type and of an aggregate, and the family's aggregates.
type RasterType struct {
	Name  string
	Funcs []string
}

// rasterTypes is every raster type MEOS declares, ordered by name.
var rasterTypes = []RasterType{
`)
	rnames := make([]string, 0, len(raster))
	for n := range raster {
		rnames = append(rnames, n)
	}
	sort.Strings(rnames)
	for _, n := range rnames {
		fmt.Fprintf(&b, "\t{Name: %q, Funcs: %#v},\n", n, raster[n])
	}
	b.WriteString(`}

// rasterTypeByName indexes rasterTypes by the SQL type name.
var rasterTypeByName = func() map[string]*RasterType {
	m := make(map[string]*RasterType, len(rasterTypes))
	for i := range rasterTypes {
		m[rasterTypes[i].Name] = &rasterTypes[i]
	}
	return m
}()
`)
	return format.Source(b.Bytes())
}
