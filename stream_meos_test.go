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
