package main

import (
	"encoding/json"
	"testing"
)

// tPropType resolves OGC / MobilityDB type tokens to the scalar temporal type
// carried in storage, defaulting an empty token to TReal.
func TestTPropType(t *testing.T) {
	cases := map[string]tType{
		"":         {"MovingFloat", "vfloat", "tfloat", "TReal", "Linear"},
		"TReal":    {"MovingFloat", "vfloat", "tfloat", "TReal", "Linear"},
		"measure":  {"MovingFloat", "vfloat", "tfloat", "TReal", "Linear"},
		"TInt":     {"MovingInteger", "vint", "tint", "TInt", "Step"},
		"integer":  {"MovingInteger", "vint", "tint", "TInt", "Step"},
		"TText":    {"MovingText", "vtext", "ttext", "TText", "Discrete"},
		"string":   {"MovingText", "vtext", "ttext", "TText", "Discrete"},
		"TBool":    {"MovingBoolean", "vbool", "tbool", "TBool", "Step"},
		"BOOLEAN ": {"MovingBoolean", "vbool", "tbool", "TBool", "Step"},
	}
	for in, want := range cases {
		got, ok := tPropType(in)
		if !ok || got != want {
			t.Errorf("tPropType(%q) = %+v,%v want %+v,true", in, got, ok, want)
		}
	}
	if _, ok := tPropType("tgeompoint"); ok {
		t.Error("tPropType accepted a non-scalar type")
	}
}

// tPropMFJSON renders a MobilityDB MF-JSON document from an OGC temporal
// property body, mapping interpolation and folding a valueSequence into a
// sequence set when it holds more than one segment.
func TestTPropMFJSON(t *testing.T) {
	parse := func(s string) map[string]any {
		var m map[string]any
		if err := json.Unmarshal([]byte(s), &m); err != nil {
			t.Fatalf("bad json: %v", err)
		}
		return m
	}
	unmarshal := func(s string) map[string]any {
		var m map[string]any
		json.Unmarshal([]byte(s), &m)
		return m
	}

	// flat form, OGC "Stepwise" maps to MobilityDB "Step"
	out, err := tPropMFJSON("MovingFloat", "Linear",
		parse(`{"datetimes":["2026-01-01T00:00:00Z"],"values":[1],"interpolation":"Stepwise"}`))
	if err != nil {
		t.Fatal(err)
	}
	if m := unmarshal(out); m["type"] != "MovingFloat" || m["interpolation"] != "Step" {
		t.Errorf("flat form wrong: %s", out)
	}

	// absent interpolation falls back to the type default
	out, _ = tPropMFJSON("MovingText", "Discrete", parse(`{"datetimes":["2026-01-01T00:00:00Z"],"values":["x"]}`))
	if m := unmarshal(out); m["interpolation"] != "Discrete" {
		t.Errorf("default interpolation not applied: %s", out)
	}

	// a single-segment valueSequence collapses to the flat form
	out, _ = tPropMFJSON("MovingFloat", "Linear",
		parse(`{"valueSequence":[{"datetimes":["2026-01-01T00:00:00Z"],"values":[1],"interpolation":"Linear"}]}`))
	if m := unmarshal(out); m["sequences"] != nil || m["datetimes"] == nil {
		t.Errorf("single valueSequence should be flat: %s", out)
	}

	// a multi-segment valueSequence becomes a sequence set
	out, _ = tPropMFJSON("MovingFloat", "Linear", parse(`{"valueSequence":[
	  {"datetimes":["2026-01-01T00:00:00Z"],"values":[1]},
	  {"datetimes":["2026-01-02T00:00:00Z"],"values":[2]}]}`))
	if m := unmarshal(out); m["sequences"] == nil {
		t.Errorf("multi valueSequence should be a sequence set: %s", out)
	}

	// missing datetimes/values is an error
	if _, err := tPropMFJSON("MovingFloat", "Linear", parse(`{"interpolation":"Linear"}`)); err == nil {
		t.Error("expected an error for a body without datetimes/values")
	}
}
