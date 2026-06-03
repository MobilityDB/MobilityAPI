package main

import "testing"

func TestRewriteSparkSQL(t *testing.T) {
	in := "SELECT id, CAST(Tmin(b) AS text), Xmin(b), asMFJSON(g) FROM s"
	got := rewriteSparkSQL(in)
	want := "SELECT id, CAST(stboxTmin(b) AS text), stboxXmin(b), temporalAsMfjson(g) FROM s"
	if got != want {
		t.Errorf("rewriteSparkSQL\n got %q\nwant %q", got, want)
	}
	// identity for functions that match the Spark idiom already
	id := "SELECT sequenceN(trip, 1), eIntersects(trip, env), speed(trip)"
	if rewriteSparkSQL(id) != id {
		t.Errorf("identity functions were rewritten: %q", rewriteSparkSQL(id))
	}
}

func TestInlineParams(t *testing.T) {
	got := inlineParams("WHERE id=$1 AND name=$2 AND k=$10", []any{int64(7), "O'Hara", 1, 2, 3, 4, 5, 6, 7, 42})
	want := "WHERE id=7 AND name='O''Hara' AND k=42"
	if got != want {
		t.Errorf("inlineParams\n got %q\nwant %q", got, want)
	}
}
