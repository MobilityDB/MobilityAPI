package main

import (
	"context"
	"testing"
	"time"
)

// fakeEngine is a pure-Go test double for the StreamEngine seam: it transforms
// each record by adding spec.Arg, so the control plane can be exercised without
// cgo, MEOS, or a database.
type fakeEngine struct{}

func (fakeEngine) Name() string { return "fake" }

func (fakeEngine) Submit(ctx context.Context, spec QuerySpec, source <-chan Instant) (QueryHandle, error) {
	h := &fakeHandle{results: make(chan Event, 16), status: "running"}
	go func() {
		defer close(h.results)
		for {
			select {
			case <-ctx.Done():
				return
			case in, ok := <-source:
				if !ok {
					return
				}
				ev := Event{"datetime": in.T, "value": in.V + spec.Arg, "property": spec.Pname, "operation": spec.Op}
				select {
				case h.results <- ev:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return h, nil
}

type fakeHandle struct {
	results chan Event
	status  string
}

func (h *fakeHandle) Results() <-chan Event { return h.results }
func (h *fakeHandle) Status() string        { return h.status }
func (h *fakeHandle) Stop() error           { return nil }

// liftedOps marks the operations needing a scalar operand; the rest are unary.
// TestEngineForRouting measures per-query engine switching: a single tier routes
// distinct engine names to distinct engines, fails loudly when a cluster engine
// is unconfigured, rejects unknown names, and caches each engine.
func TestEngineForRouting(t *testing.T) {
	t.Setenv("MFAPI_FLINK_CMD", "/bin/echo flink-bridge")
	t.Setenv("MFAPI_KAFKA_CMD", "/bin/echo kafka-bridge")

	fl, err := engineFor("flink")
	if err != nil || fl.Name() != "flink" {
		t.Fatalf("engineFor(flink) = %v, %v; want a flink engine", fl, err)
	}
	ka, err := engineFor("kafka")
	if err != nil || ka.Name() != "kafka" {
		t.Fatalf("engineFor(kafka) = %v, %v; want a kafka engine", ka, err)
	}
	if fl == ka {
		t.Fatal("flink and kafka resolved to the same engine instance")
	}
	if again, _ := engineFor("flink"); again != fl {
		t.Fatal("engineFor(flink) is not cached (returned a new instance)")
	}
	if _, err := engineFor("bogus"); err == nil {
		t.Fatal("engineFor(bogus) should reject an unknown engine name")
	}
}

func TestEngineForFailsLoudWhenUnconfigured(t *testing.T) {
	delete(engCache, "kafka")
	t.Setenv("MFAPI_KAFKA_CMD", "")
	if _, err := engineFor("kafka"); err == nil {
		t.Fatal("engineFor(kafka) without MFAPI_KAFKA_CMD should fail loudly, not fall back")
	}
}

func TestLiftedOpsCatalogue(t *testing.T) {
	for _, op := range []string{"ln", "exp", "abs", "degrees"} {
		if info, ok := liftedOps[op]; !ok || info.needsArg {
			t.Errorf("%s should be a known unary op", op)
		}
	}
	for _, op := range []string{"add", "sub", "mul", "div"} {
		if info, ok := liftedOps[op]; !ok || !info.needsArg {
			t.Errorf("%s should be a known scalar-arg op", op)
		}
	}
	if _, ok := liftedOps["sin"]; ok {
		t.Error("sin is not in MEOS yet and must not be advertised")
	}
}

// parseInstantsMFJSON reads instants from both the flat and the sequence-set
// MF-JSON forms.
func TestParseInstantsMFJSON(t *testing.T) {
	flat := `{"type":"MovingFloat","datetimes":["2026-01-01T00:00:00Z","2026-01-02T00:00:00Z"],"values":[1.5,2.5]}`
	got, err := parseInstantsMFJSON([]byte(flat))
	if err != nil || len(got) != 2 || got[0].V != 1.5 || got[1].V != 2.5 {
		t.Fatalf("flat parse = %+v, %v", got, err)
	}
	seqset := `{"type":"MovingFloat","sequences":[
	  {"datetimes":["2026-01-01T00:00:00Z"],"values":[10]},
	  {"datetimes":["2026-01-02T00:00:00Z"],"values":[20]}]}`
	got, err = parseInstantsMFJSON([]byte(seqset))
	if err != nil || len(got) != 2 || got[0].V != 10 || got[1].V != 20 {
		t.Fatalf("sequence-set parse = %+v, %v", got, err)
	}
	if _, err := parseInstantsMFJSON([]byte(`{"type":"MovingFloat"}`)); err == nil {
		t.Error("expected an error for a document with no instants")
	}
}

// submit runs a query on the engine and fans its results out to subscribers;
// stop ends it and closes the subscriber channels.
func TestRegistrySubmitBroadcastStop(t *testing.T) {
	src := make(chan Instant, 4)
	ctx, cancel := context.WithCancel(context.Background())
	spec := QuerySpec{CID: "ships", FID: 1, Pname: "speed", Op: "add", Arg: 1}
	cq, err := streamReg.submit(ctx, cancel, fakeEngine{}, spec, src)
	if err != nil {
		t.Fatal(err)
	}
	defer streamReg.remove(cq.id)

	sub := cq.subscribe()
	src <- Instant{T: "2026-01-01T00:00:00Z", V: 10}
	select {
	case got := <-sub:
		if got["value"] != 11.0 {
			t.Fatalf("transform: want 11 got %v", got["value"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no event delivered")
	}

	if cq.status() != "running" {
		t.Errorf("status = %q, want running", cq.status())
	}
	cq.stop()
	select {
	case _, ok := <-sub:
		for ok {
			_, ok = <-sub
		}
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber not closed after stop")
	}
	if cq.status() != "stopped" {
		t.Errorf("status after stop = %q, want stopped", cq.status())
	}
}

// a subscriber that arrives after the query has stopped gets a closed channel.
func TestSubscribeAfterStop(t *testing.T) {
	src := make(chan Instant)
	ctx, cancel := context.WithCancel(context.Background())
	cq, err := streamReg.submit(ctx, cancel, fakeEngine{}, QuerySpec{CID: "c", FID: 2, Pname: "p", Op: "abs"}, src)
	if err != nil {
		t.Fatal(err)
	}
	defer streamReg.remove(cq.id)
	cq.stop()
	// allow the pump to observe the closed result channel
	deadline := time.Now().Add(2 * time.Second)
	for cq.status() != "stopped" && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	sub := cq.subscribe()
	if _, ok := <-sub; ok {
		t.Error("subscriber after stop should receive a closed channel")
	}
}
