package main

// Mirrors #TestRegistrySubmitBroadcastStop of stream_test.go over the
// WebSocket channel (#websocketQuery): a query on the fake engine, read
// through a real HTTP server, so no database and no MEOS is needed.

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestWebSocketDeliversResultsAndClosesWithTheQuery(t *testing.T) {
	src := make(chan Instant, 4)
	ctx, cancel := context.WithCancel(context.Background())
	spec := QuerySpec{CID: "ships", FID: 1, Pname: "speed", Op: "add", Arg: 1}
	cq, err := streamReg.submit(ctx, cancel, fakeEngine{}, spec, src)
	if err != nil {
		t.Fatal(err)
	}
	defer streamReg.remove(cq.id)
	srv := httptest.NewServer(newMux())
	defer srv.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http") + cq.basePath() + "/" + cq.id + "/ws"
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dialling %s: %v", url, err)
	}
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	// The subscription is taken when the connection opens; the records sent
	// after it arrive in order, each one JSON message
	deadline := time.Now().Add(2 * time.Second)
	subscribed := func() bool {
		cq.mu.Lock()
		defer cq.mu.Unlock()
		return len(cq.subs) > 0
	}
	for !subscribed() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	src <- Instant{T: "2026-01-01T00:00:00Z", V: 10}
	src <- Instant{T: "2026-01-01T00:00:01Z", V: 20}
	for _, want := range []float64{11, 21} {
		kind, msg, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("reading the result %v: %v", want, err)
		}
		var ev map[string]any
		if kind != websocket.TextMessage || json.Unmarshal(msg, &ev) != nil || ev["value"] != want {
			t.Fatalf("message %d %s, want the JSON result %v", kind, msg, want)
		}
	}

	// The query stops and the connection closes normally, saying so
	cq.stop()
	_, _, err = conn.ReadMessage()
	var ce *websocket.CloseError
	if !errorsAs(err, &ce) || ce.Code != websocket.CloseNormalClosure ||
		!strings.Contains(ce.Text, "stopped") {
		t.Fatalf("after the query stops the connection reads %v, want a normal closure saying it stopped", err)
	}
}

func TestWebSocketUnknownQuery(t *testing.T) {
	srv := httptest.NewServer(newMux())
	defer srv.Close()
	url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/collections/c/items/1/tgsequence/queries/none/ws"
	_, resp, err := websocket.DefaultDialer.Dial(url, nil)
	if err == nil || resp == nil || resp.StatusCode != 404 {
		t.Fatalf("dialling an unknown query answered %v, %v; want 404", resp, err)
	}
}

// errorsAs is errors.As for the close error the reader returns.
func errorsAs(err error, target **websocket.CloseError) bool {
	ce, ok := err.(*websocket.CloseError)
	if ok {
		*target = ce
	}
	return ok
}
