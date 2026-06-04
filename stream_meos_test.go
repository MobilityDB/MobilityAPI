//go:build meos

package main

import (
	"context"
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
			if math.Abs(got.V-c.want) > 1e-9 {
				t.Errorf("%s(%v, arg=%v) = %v, want %v", c.op, c.in, c.arg, got.V, c.want)
			}
			if got.T != ts {
				t.Errorf("%s: timestamp changed %q -> %q", c.op, ts, got.T)
			}
		case <-time.After(5 * time.Second):
			t.Errorf("%s: no result", c.op)
		}
		cancel()
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
				done <- got.V
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
