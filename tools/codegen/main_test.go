package main

// These test the generator's reading of parameter types by calling
// #mathOperations and #renderDispatch directly, beside the checks of the
// generated table in the tier's own catalog_test.go
// (#TestTheCatalogStatesMoreThanTheTierLifts), which read its output.

import (
	"strings"
	"testing"
)

// mathFunction is a catalog entry that passes every selection rule of
// #mathOperations, with one further parameter of the given C and canonical
// spellings.
func mathFunction(name, cType, canonical string) function {
	return function{
		Name:     name,
		Group:    "meos_temporal_math",
		Category: "transformation",
		SQLName:  name,
		Params: []param{
			{Name: "temp", CType: "const Temporal *", Canonical: "const Temporal *"},
			{Name: "arg", CType: cType, Canonical: canonical},
		},
		Returns: ctype{C: "Temporal *"},
	}
}

// A parameter whose C spelling is a typedef is filled by the type the catalog
// resolves it to, so #renderDispatch serves it.
func TestAParameterIsFilledByItsCanonicalType(t *testing.T) {
	ops, err := mathOperations([]function{mathFunction("tfloat_scale", "int32", "int")})
	if err != nil {
		t.Fatal(err)
	}
	if len(ops) != 1 || ops[0].Arg != "int32" || ops[0].ArgCanonical != "int" {
		t.Fatalf("got %+v, want one operation with Arg int32 and ArgCanonical int", ops)
	}
	src, called, err := renderDispatch(ops)
	if err != nil {
		t.Fatal(err)
	}
	if called != 1 || !strings.Contains(string(src), "C.tfloat_scale(temp, C.int(arg))") {
		t.Fatalf("the dispatch serves %d operations, want the int32 parameter filled as an int:\n%s", called, src)
	}
}

// A parameter of a type a query value cannot fill keeps its operation in the
// table and out of the dispatch #renderDispatch writes.
func TestAnUnfillableParameterGetsNoCase(t *testing.T) {
	ops, err := mathOperations([]function{mathFunction("tfloat_shift", "const Interval *", "const Interval *")})
	if err != nil {
		t.Fatal(err)
	}
	if len(ops) != 1 {
		t.Fatalf("got %d operations, want 1", len(ops))
	}
	src, called, err := renderDispatch(ops)
	if err != nil {
		t.Fatal(err)
	}
	if called != 0 || strings.Contains(string(src), "tfloat_shift") {
		t.Fatalf("the dispatch serves %d operations, want none:\n%s", called, src)
	}
}

// A parameter type #mathOperations does not know, or one the catalog gives no
// canonical spelling, stops the generator and names the function.
func TestAnUnknownParameterTypeStopsTheGenerator(t *testing.T) {
	for _, tc := range []struct{ cType, canonical, want string }{
		{"const Span *", "const Span *", `"const Span *"`},
		{"int32", "", "no canonical spelling"},
	} {
		_, err := mathOperations([]function{mathFunction("tfloat_odd", tc.cType, tc.canonical)})
		if err == nil || !strings.Contains(err.Error(), "tfloat_odd") || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("parameter %q canonical %q: got error %v, want one naming tfloat_odd and %s",
				tc.cType, tc.canonical, err, tc.want)
		}
	}
}
