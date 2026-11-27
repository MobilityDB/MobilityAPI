//go:build meos

package main

// Mirrors #TestWebSocketDeliversResultsAndClosesWithTheQuery of
// stream_websocket_test.go for the Server-Sent Events of #streamQuery, over the
// whole HTTP cycle of a continuous query on a temporal property of the
// conformance fixture: the speed of Alpha, 12.5, 14.0, 11.25 and 13.75 km/h,
// doubled by the transform mul.

import (
	"bufio"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestLivePropertyQueryHTTPCycleAndEvents(t *testing.T) {
	mux, done := atsLiveMux(t)
	defer done()
	srv := httptest.NewServer(mux)
	defer srv.Close()
	const queries = "/collections/conformance/items/1/tproperties/speed/queries"

	// Registered: 201 and its cquery link, running
	rec := atsDo(t, mux, "POST", queries, `{"operation":"mul","arg":2,"intervalMs":20}`)
	if rec.Code != 201 {
		t.Fatalf("POST %s = %d (%s)", queries, rec.Code, rec.Body.String())
	}
	var link struct {
		QueryID string `json:"queryId"`
		Status  string `json:"status"`
		Href    string `json:"href"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &link); err != nil || link.QueryID == "" ||
		link.Status != "running" || !strings.HasSuffix(link.Href, "/"+link.QueryID+"/stream") {
		t.Fatalf("the cquery link is %s", rec.Body.String())
	}
	one := queries + "/" + link.QueryID

	// Listed and read
	rec = atsDo(t, mux, "GET", queries, "")
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"queryId":"`+link.QueryID+`"`) {
		t.Errorf("GET %s = %d, without the query (%s)", queries, rec.Code, rec.Body.String())
	}
	if rec = atsDo(t, mux, "GET", one, ""); rec.Code != 200 || !strings.Contains(rec.Body.String(), `"running"`) {
		t.Errorf("GET %s = %d (%s), want the running query", one, rec.Code, rec.Body.String())
	}

	// Its results as Server-Sent Events: each one event, its data the result
	resp, err := http.Get(srv.URL + one + "/stream")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); resp.StatusCode != 200 || ct != "text/event-stream" {
		t.Fatalf("the stream answered %d %s", resp.StatusCode, ct)
	}
	values := map[float64]bool{}
	lines := make(chan string)
	go func() {
		defer close(lines)
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			lines <- sc.Text()
		}
	}()
	deadline := time.After(10 * time.Second)
	for len(values) < 4 {
		select {
		case l, ok := <-lines:
			if !ok {
				t.Fatalf("the stream ended after %d of 4 values", len(values))
			}
			data, isData := strings.CutPrefix(l, "data: ")
			if !isData {
				continue
			}
			var ev struct {
				Value float64 `json:"value"`
			}
			if err := json.Unmarshal([]byte(data), &ev); err != nil {
				t.Fatalf("the event data %q is no JSON result", data)
			}
			values[ev.Value] = true
		case <-deadline:
			t.Fatalf("%d of 4 values read in 10 s", len(values))
		}
	}
	var got []float64
	for v := range values {
		got = append(got, v)
	}
	sort.Float64s(got)
	if len(got) != 4 || got[0] != 22.5 || got[1] != 25 || got[2] != 27.5 || got[3] != 28 {
		t.Errorf("the stream carries %v, want the doubled speeds 22.5, 25, 27.5, 28", got)
	}

	// Stopped: 204, then the query is gone and its stream ends
	if rec = atsDo(t, mux, "DELETE", one, ""); rec.Code != 204 {
		t.Errorf("DELETE %s = %d, want 204", one, rec.Code)
	}
	if rec = atsDo(t, mux, "GET", one, ""); rec.Code != 404 {
		t.Errorf("GET %s after DELETE = %d, want 404", one, rec.Code)
	}
	ended := time.After(5 * time.Second)
	for {
		select {
		case _, ok := <-lines:
			if !ok {
				return
			}
		case <-ended:
			t.Fatal("the stream did not end after the query was stopped")
		}
	}
}
