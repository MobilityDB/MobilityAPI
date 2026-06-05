// Live ingestion: a continuous query can run over a live-pushed stream instead
// of a replay of stored values. A producer POSTs records to an ingest endpoint
// and every live query on that property processes them in real time — the
// broker-agnostic form of the EDR Pub/Sub source (a Kafka/MQTT binding feeds the
// same hub).
package main

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// liveHub fans pushed records out to the live queries subscribed on a key
// (collection, feature, property). A subscriber is removed when its query's
// context is cancelled.
type liveHub struct {
	mu   sync.Mutex
	subs map[string]map[chan Instant]struct{}
}

var live = &liveHub{subs: map[string]map[chan Instant]struct{}{}}

func liveKey(cid string, fid int, pname string) string {
	return cid + "\x00" + strconv.Itoa(fid) + "\x00" + pname
}

// subscribe returns a source channel for a live query; it is closed and removed
// when ctx is cancelled (the query stops).
func (h *liveHub) subscribe(ctx context.Context, key string) <-chan Instant {
	ch := make(chan Instant, 64)
	h.mu.Lock()
	if h.subs[key] == nil {
		h.subs[key] = map[chan Instant]struct{}{}
	}
	h.subs[key][ch] = struct{}{}
	h.mu.Unlock()
	go func() {
		<-ctx.Done()
		h.mu.Lock()
		delete(h.subs[key], ch)
		close(ch)
		h.mu.Unlock()
	}()
	return ch
}

// publish delivers a record to every live query on the key (non-blocking: a slow
// query drops the record rather than stalling the producer).
func (h *liveHub) publish(key string, in Instant) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for ch := range h.subs[key] {
		select {
		case ch <- in:
			n++
		default:
		}
	}
	return n
}

// ingestProperty accepts a pushed record for a property and delivers it to the
// live queries on it.
func ingestProperty(w http.ResponseWriter, r *http.Request) {
	cid := r.PathValue("cid")
	name := r.PathValue("pname")
	if _, _, ok := collectionMeta(r.Context(), cid); !ok {
		httpErr(w, 404, "collection not found")
		return
	}
	fid, err := strconv.Atoi(r.PathValue("fid"))
	if err != nil {
		httpErr(w, 400, "invalid feature id")
		return
	}
	var body struct {
		Datetime string   `json:"datetime"`
		Value    *float64 `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Value == nil {
		httpErr(w, 400, "ingest record requires a numeric \"value\"")
		return
	}
	ts := strings.TrimSpace(body.Datetime)
	if ts == "" {
		ts = time.Now().UTC().Format("2006-01-02 15:04:05+00")
	}
	delivered := live.publish(liveKey(cid, fid, name), Instant{T: ts, V: *body.Value})
	writeJSON(w, 202, map[string]any{"delivered": delivered})
}
