//go:build meos

package main

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"
)

// Submit applies each lifted operation to a tfloat instant through MEOS on a
// per-query worker thread and returns the exact pointwise result.
func TestMeosTransform(t *testing.T) {
	e, err := defaultStreamEngine()
	if err != nil {
		t.Fatal(err)
	}
	const ts = "2026-01-01T00:00:00+00"
	cases := []struct {
		op   string
		arg  float64
		in   float64
		want float64
	}{
		{"ln", 0, math.E, 1},
		{"exp", 0, 0, 1},
		{"log10", 0, 1000, 3},
		{"abs", 0, -5, 5},
		{"ceil", 0, 2.1, 3},
		{"floor", 0, 2.9, 2},
		{"mul", 2, 3, 6},
		{"add", 10, 5, 15},
		{"sub", 4, 10, 6},
		{"div", 4, 10, 2.5},
		{"degrees", 0, math.Pi, 180},
		{"radians", 0, 180, math.Pi},
		{"sin", 0, math.Pi / 2, 1},
		{"cos", 0, 0, 1},
		{"tan", 0, math.Pi / 4, 1},
	}
	// Every lifted operation is measured here, and that is what makes this an
	// equivalence check rather than a sample: the dispatch it exercises names
	// the MEOS symbol per operation, so an operation nobody measures could call
	// the wrong one and still pass.
	covered := map[string]bool{}
	for _, c := range cases {
		covered[c.op] = true
	}
	for op := range liftedOps {
		if !covered[op] {
			t.Errorf("the lifted operation %q is not measured here, so nothing checks which MEOS function it calls", op)
		}
	}

	for _, c := range cases {
		ctx, cancel := context.WithCancel(context.Background())
		src := make(chan Instant, 1)
		h, err := e.Submit(ctx, QuerySpec{Op: c.op, Arg: c.arg}, src)
		if err != nil {
			t.Errorf("%s: submit: %v", c.op, err)
			cancel()
			continue
		}
		src <- Instant{T: ts, V: c.in}
		select {
		case got := <-h.Results():
			if math.Abs(got["value"].(float64)-c.want) > 1e-9 {
				t.Errorf("%s(%v, arg=%v) = %v, want %v", c.op, c.in, c.arg, got["value"], c.want)
			}
			if got["datetime"] != ts {
				t.Errorf("%s: timestamp changed %q -> %q", c.op, ts, got["datetime"])
			}
		case <-time.After(5 * time.Second):
			t.Errorf("%s: no result", c.op)
		}
		cancel()
	}
}

// A COUNT window aggregates its records' values through MEOS.
func TestMeosWindowAggregate(t *testing.T) {
	e, _ := defaultStreamEngine()
	cases := []struct {
		agg  string
		want float64
	}{
		{"COUNT", 3}, {"SUM", 12}, {"AVG", 4}, {"MIN", 2}, {"MAX", 6},
	}
	for _, c := range cases {
		ctx, cancel := context.WithCancel(context.Background())
		src := make(chan Instant, 3)
		h, err := e.Submit(ctx, QuerySpec{Agg: c.agg, Window: Window{Type: "COUNT", Size: 3}}, src)
		if err != nil {
			t.Errorf("%s: submit: %v", c.agg, err)
			cancel()
			continue
		}
		for _, v := range []float64{2, 4, 6} {
			src <- Instant{T: "2026-01-01T00:00:00+00", V: v}
		}
		select {
		case got := <-h.Results():
			if math.Abs(got["value"].(float64)-c.want) > 1e-9 {
				t.Errorf("%s over [2,4,6] = %v, want %v", c.agg, got["value"], c.want)
			}
			if got["count"].(int) != 3 {
				t.Errorf("%s: count = %v, want 3", c.agg, got["count"])
			}
		case <-time.After(5 * time.Second):
			t.Errorf("%s: no window result", c.agg)
		}
		cancel()
	}
}

// Text and boolean aggregations over a COUNT window, through MEOS.
func TestMeosTextBoolAggregate(t *testing.T) {
	e, _ := defaultStreamEngine()

	// TText COUNT_DISTINCT over ["a","b","a"] = 2
	ctx, cancel := context.WithCancel(context.Background())
	src := make(chan Instant, 3)
	h, err := e.Submit(ctx, QuerySpec{Ptype: "TText", Agg: "COUNT_DISTINCT", Window: Window{Type: "COUNT", Size: 3}}, src)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{"a", "b", "a"} {
		src <- Instant{T: "2026-01-01T00:00:00Z", S: s}
	}
	if got := (<-h.Results())["value"]; got != 2.0 {
		t.Errorf("COUNT_DISTINCT = %v, want 2", got)
	}
	cancel()

	// TBool ANY / ALL / COUNT_TRUE over [t, f, f]
	for _, c := range []struct {
		agg  string
		want any
	}{{"ANY", true}, {"ALL", false}, {"COUNT_TRUE", 1.0}} {
		ctx, cancel := context.WithCancel(context.Background())
		src := make(chan Instant, 3)
		h, err := e.Submit(ctx, QuerySpec{Ptype: "TBoolean", Agg: c.agg, Window: Window{Type: "COUNT", Size: 3}}, src)
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range []string{"t", "f", "f"} {
			src <- Instant{T: "2026-01-01T00:00:00Z", S: s}
		}
		if got := (<-h.Results())["value"]; got != c.want {
			t.Errorf("%s = %v, want %v", c.agg, got, c.want)
		}
		cancel()
	}
}

// A HOPPING window emits one aggregate per hop over the last span — overlapping.
func TestMeosHoppingWindow(t *testing.T) {
	e, _ := defaultStreamEngine()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src := make(chan Instant, 8)
	h, err := e.Submit(ctx, QuerySpec{
		Agg:    "AVG",
		Window: Window{Type: "HOPPING", Size: 3, Unit: "SECONDS", Hop: 1, HopUnit: "SECONDS"},
	}, src)
	if err != nil {
		t.Fatal(err)
	}
	for i, v := range []float64{10, 20, 30, 40, 50} {
		src <- Instant{T: fmt.Sprintf("2026-01-01T00:00:0%dZ", i), V: v}
	}
	// Five records one second apart yield four hop boundaries; the fourth window
	// covers [20, 30, 40] → AVG 30 over 3 records.
	var last Event
	for i := 0; i < 4; i++ {
		select {
		case last = <-h.Results():
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d window results", i)
		}
	}
	if last["count"].(int) != 3 || math.Abs(last["value"].(float64)-30) > 1e-9 {
		t.Errorf("4th hopping window = value %v count %v, want 30 / 3", last["value"], last["count"])
	}
}

// Concurrent queries each run on their own MEOS-initialised thread.
func TestMeosConcurrentQueries(t *testing.T) {
	e, _ := defaultStreamEngine()
	const n = 8
	done := make(chan float64, n)
	for i := 0; i < n; i++ {
		go func(k int) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			src := make(chan Instant, 1)
			h, err := e.Submit(ctx, QuerySpec{Op: "mul", Arg: float64(k)}, src)
			if err != nil {
				done <- math.NaN()
				return
			}
			src <- Instant{T: "2026-01-01T00:00:00+00", V: 10}
			select {
			case got := <-h.Results():
				done <- got["value"].(float64)
			case <-time.After(5 * time.Second):
				done <- math.NaN()
			}
		}(i)
	}
	sum := 0.0
	for i := 0; i < n; i++ {
		v := <-done
		if math.IsNaN(v) {
			t.Fatal("a concurrent query produced no result")
		}
		sum += v
	}
	// each query computes 10*k for k in 0..n-1 → 10 * sum(0..n-1)
	want := 10.0 * float64(n*(n-1)/2)
	if math.Abs(sum-want) > 1e-9 {
		t.Errorf("concurrent results sum = %v, want %v", sum, want)
	}
}

// windowResults submits an aggregation over the given window, feeds it one record
// per (second, value) pair from 2026-01-01T00:00:00Z, and answers the first n
// window results.
func windowResults(t *testing.T, w Window, recs [][2]float64, n int) []Event {
	t.Helper()
	e, _ := defaultStreamEngine()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	src := make(chan Instant, len(recs))
	h, err := e.Submit(ctx, QuerySpec{Agg: "AVG", Window: w}, src)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range recs {
		src <- Instant{T: fmt.Sprintf("2026-01-01T00:00:%02dZ", int(r[0])), V: r[1]}
	}
	var out []Event
	for len(out) < n {
		select {
		case ev := <-h.Results():
			out = append(out, ev)
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d of %d window results", len(out), n)
		}
	}
	return out
}

// Requirement 2: a window result carries the bounds of its window. A time window
// is [start, end), the span it covers whatever records fall in it; a COUNT window
// is [start, end], the times of its first and last record.
func TestMeosWindowBounds(t *testing.T) {
	for _, c := range []struct {
		what       string
		w          Window
		recs       [][2]float64
		start, end string
	}{
		// Records at 1 s and 2 s of the tumbling window [0 s, 3 s), closed by the record at 4 s.
		{"TUMBLING", Window{Type: "TUMBLING", Size: 3, Unit: "SECONDS"},
			[][2]float64{{1, 10}, {2, 20}, {4, 40}}, "2026-01-01T00:00:00Z", "2026-01-01T00:00:03Z"},
		// The first hop boundary after a record at 0 s is 1 s: the window [-2 s, 1 s).
		{"HOPPING", Window{Type: "HOPPING", Size: 3, Unit: "SECONDS", Hop: 1, HopUnit: "SECONDS"},
			[][2]float64{{0, 10}, {1, 20}}, "2025-12-31T23:59:58Z", "2026-01-01T00:00:01Z"},
		{"COUNT", Window{Type: "COUNT", Size: 2},
			[][2]float64{{1, 10}, {5, 20}}, "2026-01-01T00:00:01Z", "2026-01-01T00:00:05Z"},
	} {
		t.Run(c.what, func(t *testing.T) {
			ev := windowResults(t, c.w, c.recs, 1)[0]
			if ev["windowStart"] != c.start || ev["windowEnd"] != c.end {
				t.Errorf("window = [%v, %v], want [%s, %s]", ev["windowStart"], ev["windowEnd"], c.start, c.end)
			}
		})
	}
}

// Requirement 3: a TUMBLING window is fixed-duration and non-overlapping, so
// each record falls in exactly one window and consecutive windows meet.
func TestMeosTumblingWindow(t *testing.T) {
	// Windows of 2 s over records at 0..5 s: [0,2) {10,20}, [2,4) {30,40}, closed
	// by the records at 2 s and 4 s.
	evs := windowResults(t, Window{Type: "TUMBLING", Size: 2, Unit: "SECONDS"},
		[][2]float64{{0, 10}, {1, 20}, {2, 30}, {3, 40}, {4, 50}}, 2)
	for i, want := range []struct {
		start, end string
		avg        float64
	}{
		{"2026-01-01T00:00:00Z", "2026-01-01T00:00:02Z", 15},
		{"2026-01-01T00:00:02Z", "2026-01-01T00:00:04Z", 35},
	} {
		ev := evs[i]
		if ev["windowStart"] != want.start || ev["windowEnd"] != want.end {
			t.Errorf("window %d = [%v, %v], want [%s, %s]", i, ev["windowStart"], ev["windowEnd"], want.start, want.end)
		}
		if ev["count"].(int) != 2 || math.Abs(ev["value"].(float64)-want.avg) > 1e-9 {
			t.Errorf("window %d = AVG %v over %v records, want %v over 2", i, ev["value"], ev["count"], want.avg)
		}
	}
	if evs[0]["windowEnd"] != evs[1]["windowStart"] {
		t.Errorf("consecutive tumbling windows do not meet: %v then %v", evs[0]["windowEnd"], evs[1]["windowStart"])
	}
}
