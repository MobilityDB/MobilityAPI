package main

import (
	"sort"
	"strings"
	"testing"
)

// The generated table is what the tier knows about MEOS's temporal types, so
// these assert the shape the rest of the code relies on rather than a list of
// names, which would be the copy the generator exists to remove.

func TestCatalogIsPopulated(t *testing.T) {
	if len(temporalTypes) < 10 {
		t.Fatalf("the generated catalog holds %d temporal types, which is too few to have been read from MEOS; regenerate with go run ./tools/codegen", len(temporalTypes))
	}
	for _, tt := range temporalTypes {
		if tt.Name == "" || tt.Base == "" {
			t.Errorf("a catalog row carries no name or no base type: %+v", tt)
		}
		if got := temporalTypeByName[tt.Name]; got == nil || got.Name != tt.Name {
			t.Errorf("temporalTypeByName does not index %q", tt.Name)
		}
	}
}

// A temporal property is a non-spatial value, so every non-spatial type MEOS
// writes MF-JSON for and gives a base the standard has a token for must be one
// the store can carry. A type MEOS adds over such a base arrives here as a
// failure rather than as a request the tier rejects at runtime.
func TestEveryScalarTypeTheStandardNamesIsCarried(t *testing.T) {
	for _, tt := range temporalTypes {
		if tt.Spatial || tt.MFJSON == "" {
			continue
		}
		if _, named := ogcScalar[tt.Base]; !named {
			continue
		}
		got, ok := tPropType(tt.Name)
		if !ok {
			t.Errorf("%s is a scalar temporal type over base %s, which OGC Part 1 names, and tPropType does not resolve it", tt.Name, tt.Base)
			continue
		}
		if got.cast != tt.Name {
			t.Errorf("tPropType(%q).cast = %q, want %q", tt.Name, got.cast, tt.Name)
		}
		if got.mf != tt.MFJSON {
			t.Errorf("tPropType(%q).mf = %q, want the token MEOS writes, %q", tt.Name, got.mf, tt.MFJSON)
		}
	}
}

// A spatial type is a moving feature's geometry, never a temporal property, so
// the store must not offer to hold one.
func TestNoSpatialTypeIsATemporalProperty(t *testing.T) {
	for _, tt := range temporalTypes {
		if !tt.Spatial {
			continue
		}
		if _, ok := tPropType(tt.Name); ok {
			t.Errorf("tPropType resolves %q, which is a spatial type and belongs under tgsequence", tt.Name)
		}
	}
}

// The tokens a client may send resolve, in the case the standard writes them
// and in lower case, and an unknown one does not.
func TestTPropTypeResolvesTheTokensClientsSend(t *testing.T) {
	for _, tok := range []string{"", "TReal", "treal", "tfloat", "measure", "TInteger", "tint", "integer", "TText", "ttext", "string", "TBoolean", "tbool", "boolean"} {
		if _, ok := tPropType(tok); !ok {
			t.Errorf("tPropType(%q) does not resolve", tok)
		}
	}
	for _, tok := range []string{"tgeompoint", "tpose", "nonsense", "tjsonb"} {
		if _, ok := tPropType(tok); ok {
			t.Errorf("tPropType(%q) resolves, and it names no OGC temporal property", tok)
		}
	}
}

// Two MEOS types carry the OGC token TInteger, tint over int4 and tbigint over
// int8, so the token alone does not name a type. Resolving it by whichever the
// map yields last picks a different type from run to run, and a stored property
// is then read out of a column its table may not have.
func TestATokenSeveralTypesCarryResolvesToOne(t *testing.T) {
	carriers := map[string][]string{}
	for name, tt := range scalarTemporalTypes {
		carriers[tt.ogc] = append(carriers[tt.ogc], name)
	}
	for token, names := range carriers {
		got, ok := tPropType(token)
		if !ok {
			t.Errorf("the OGC token %q resolves to no type, and %v carry it", token, names)
			continue
		}
		if got.ogc != token {
			t.Errorf("tPropType(%q) resolves to a type whose token is %q", token, got.ogc)
		}
		if len(names) > 1 {
			want := ogcCanonical[token]
			if want == "" {
				t.Errorf("%v all carry the token %q and ogcCanonical names none of them", names, token)
			} else if got.cast != want {
				t.Errorf("tPropType(%q).cast = %q, want the canonical %q", token, got.cast, want)
			}
		}
	}
	// The resolution the conformance fixture depends on: a TInteger property is
	// stored in vint, which is the column a store created before tbigint has.
	got, ok := tPropType("TInteger")
	if !ok || got.cast != "tint" || got.col != "vint" {
		t.Errorf("tPropType(\"TInteger\") = %+v, want tint in vint", got)
	}
}

// The OGC token and the default interpolation are what the stored documents
// carry, so they are asserted by value: a change to either rewrites what the
// service answers for properties already in a store.
func TestScalarBindingsAreTheStandardsOwn(t *testing.T) {
	want := map[string][2]string{
		"tbool":   {"TBoolean", "Step"},
		"tint":    {"TInteger", "Step"},
		"tbigint": {"TInteger", "Step"},
		"tfloat":  {"TReal", "Linear"},
		"ttext":   {"TText", "Discrete"},
	}
	for name, w := range want {
		got, ok := tPropType(name)
		if !ok {
			t.Errorf("%s does not resolve", name)
			continue
		}
		if got.ogc != w[0] || got.defInterp != w[1] {
			t.Errorf("tPropType(%q) = ogc %q interp %q, want %q and %q", name, got.ogc, got.defInterp, w[0], w[1])
		}
	}
}

// The store's value columns are one per scalar type, named for it, and the DDL
// fragment names each with its type.
func TestValueColumnsAreOnePerScalarType(t *testing.T) {
	cols := tPropValueColumnNames()
	if len(cols) != len(scalarTemporalTypes) {
		t.Fatalf("%d value columns for %d scalar types", len(cols), len(scalarTemporalTypes))
	}
	var names []string
	for _, c := range cols {
		names = append(names, c.name)
		if c.name != "v"+strings.TrimPrefix(c.typ, "t") {
			t.Errorf("column %q does not name its type %q", c.name, c.typ)
		}
		if !strings.Contains(tPropValueColumns(), c.name+" "+c.typ) {
			t.Errorf("the DDL fragment does not declare %s %s", c.name, c.typ)
		}
	}
	if !sort.StringsAreSorted(names) {
		t.Errorf("the value columns are not in a stable order: %v", names)
	}
}
