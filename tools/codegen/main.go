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
}

// function is the part of a catalog function entry the math-operation table
// reads: the C symbol, the SQL name that is the cross-engine invariant, the
// Doxygen group and category that place it, and the parameter C types.
type function struct {
	Name     string  `json:"name"`
	Group    string  `json:"group"`
	Category string  `json:"category"`
	SQLName  string  `json:"sqlfn"`
	Params   []param `json:"params"`
	Returns  ctype   `json:"returnType"`
}

type param struct {
	Name  string `json:"name"`
	CType string `json:"cType"`
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

	ops := mathOperations(c.Functions)
	if len(ops) == 0 {
		fail(fmt.Errorf("%s names no temporal math operation: the groups this reads, "+
			"meos_temporal_math and meos_temporal_transf, are not where they were", *path))
	}

	src, err := render(c.TemporalTypes, ops)
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
	fmt.Fprintf(os.Stderr, "codegen: %d temporal types and %d math operations written to %s, "+
		"%d of them callable from %s\n", len(c.TemporalTypes), len(ops), *out, called, *outMeos)
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
	SQLName string // the SQL name, which is the same on every engine
	Symbol  string // the MEOS C symbol
	Arg     string // the C type of its one further parameter, empty when it takes none
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
func mathOperations(fns []function) []mathOperation {
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
		arg := ""
		if len(rest) == 1 {
			arg = rest[0].CType
		}
		out = append(out, mathOperation{SQLName: f.SQLName, Symbol: f.Name, Arg: arg})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].SQLName != out[j].SQLName {
			return out[i].SQLName < out[j].SQLName
		}
		return out[i].Symbol < out[j].Symbol
	})
	return out
}

// argConversions turn the one float64 a continuous query carries into the C
// value a MEOS function's further parameter takes. A parameter type absent from
// this map cannot be filled from a query, so the operation carrying it gets no
// case and reaches the caller as one the dispatch does not serve.
var argConversions = map[string]string{
	"":       "",
	"double": "C.double(arg)",
	"int":    "C.int(arg)",
	"bool":   "arg != 0",
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
// MEOS symbol the catalog names. An operation whose further parameter cannot be
// filled from the single value a query carries gets no case.

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
		conv, ok := argConversions[op.Arg]
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

func render(types map[string]temporalType, ops []mathOperation) ([]byte, error) {
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
// says a type has no MF-JSON form.
type TemporalType struct {
	Name    string
	Base    string
	Box     string
	MFJSON  string
	Spatial bool
	Number  bool
	Linear  bool
}

// temporalTypes is every temporal type MEOS declares, ordered by name.
var temporalTypes = []TemporalType{
`)
	for _, name := range names {
		t := types[name]
		fmt.Fprintf(&b, "\t{Name: %q, Base: %q, Box: %q, MFJSON: %q, Spatial: %t, Number: %t, Linear: %t},\n",
			name, t.Base, t.Box, t.MFJSON, t.Spatial, t.Number, t.Linear)
	}
	b.WriteString(`}

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
`)
	return format.Source(b.Bytes())
}
