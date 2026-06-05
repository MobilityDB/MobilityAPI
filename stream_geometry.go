// Geometry continuous query: streams a moving feature's position (its temporal
// point) as Server-Sent Events — the feed an animated map consumes. It reuses
// the same registry, lifecycle and SSE delivery as the property streams, but its
// records carry coordinates instead of a scalar value, and it needs no engine
// transform: the positions come straight from the stored trajectory.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"
)

// parsePositionsMFJSON extracts the (timestamp, coordinates) positions from a
// MovingPoint MF-JSON document, handling both the flat form and a sequence set.
func parsePositionsMFJSON(b []byte) ([]Event, error) {
	var doc map[string]any
	if err := json.Unmarshal(b, &doc); err != nil {
		return nil, err
	}
	var out []Event
	collect := func(m map[string]any) {
		dts, _ := m["datetimes"].([]any)
		coords, _ := m["coordinates"].([]any)
		n := len(dts)
		if len(coords) < n {
			n = len(coords)
		}
		for i := 0; i < n; i++ {
			ts, _ := dts[i].(string)
			out = append(out, Event{"datetime": ts, "coordinates": coords[i]})
		}
	}
	if seqs, ok := doc["sequences"].([]any); ok {
		for _, s := range seqs {
			if m, ok := s.(map[string]any); ok {
				collect(m)
			}
		}
	} else {
		collect(doc)
	}
	if len(out) == 0 {
		return nil, errors.New("temporal geometry has no positions to stream")
	}
	return out, nil
}

// geometryEvents loads the moving feature's trajectory once and replays its
// positions as a paced, looping Event stream.
func geometryEvents(ctx context.Context, tbl string, fid int, interval time.Duration) (<-chan Event, error) {
	// transform to WGS84 so the positions are web-map coordinates regardless of
	// the collection's stored CRS (e.g. ships are in EPSG:25832).
	var mf *string
	err := db.QueryRow(ctx, "SELECT asMFJSON(transform(trip, 4326)) FROM "+ident(tbl)+" WHERE id=$1", fid).Scan(&mf)
	if err != nil {
		return nil, err
	}
	if mf == nil {
		return nil, errors.New("feature has no trajectory")
	}
	positions, err := parsePositionsMFJSON([]byte(*mf))
	if err != nil {
		return nil, err
	}
	out := make(chan Event)
	go func() {
		defer close(out)
		t := time.NewTicker(interval)
		defer t.Stop()
		i := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				select {
				case out <- positions[i]:
				case <-ctx.Done():
					return
				}
				i = (i + 1) % len(positions)
			}
		}
	}()
	return out, nil
}

// postGeometryQuery registers a continuous query that streams the moving
// feature's position.
func postGeometryQuery(w http.ResponseWriter, r *http.Request) {
	cid := r.PathValue("cid")
	tbl, _, ok := collectionMeta(r.Context(), cid)
	if !ok {
		httpErr(w, 404, "collection not found")
		return
	}
	fid, err := strconv.Atoi(r.PathValue("fid"))
	if err != nil {
		httpErr(w, 400, "invalid feature id")
		return
	}
	var one int
	if err := db.QueryRow(r.Context(), "SELECT 1 FROM "+ident(tbl)+" WHERE id=$1", fid).Scan(&one); errors.Is(err, ErrNoRows) {
		httpErr(w, 404, "feature not found")
		return
	} else if err != nil {
		httpErr(w, 500, err.Error())
		return
	}

	var body struct {
		IntervalMs int `json:"intervalMs"`
	}
	if r.Body != nil {
		json.NewDecoder(r.Body).Decode(&body)
	}
	interval := time.Duration(body.IntervalMs) * time.Millisecond
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}

	qctx, qcancel := context.WithCancel(context.Background())
	events, err := geometryEvents(qctx, tbl, fid, interval)
	if err != nil {
		qcancel()
		httpErr(w, 400, err.Error())
		return
	}
	spec := QuerySpec{Kind: "geometry", CID: cid, FID: fid, Interval: interval}
	cq := streamReg.register(spec, nil, events, qcancel)
	writeJSON(w, 201, cqueryLink(r, cq))
}

// listGeometryQueries lists the geometry continuous queries of a feature.
func listGeometryQueries(w http.ResponseWriter, r *http.Request) {
	cid := r.PathValue("cid")
	fid, err := strconv.Atoi(r.PathValue("fid"))
	if err != nil {
		httpErr(w, 400, "invalid feature id")
		return
	}
	list := streamReg.list(cid, fid, "")
	out := make([]any, 0, len(list))
	for _, cq := range list {
		out = append(out, cqueryLink(r, cq))
	}
	writeJSON(w, 200, map[string]any{"queries": out, "numberReturned": len(out)})
}
