// Bulk trajectory delivery for animated map rendering: every feature's
// trajectory, instant-sampled to a viz interval and transformed to WGS84,
// served as one compact JSON document. It feeds a DeckGL TripsLayer that
// animates the whole fleet on a single temporal clock — the production-grade
// counterpart of the per-feature geometry stream. Instant sampling is exact at
// the sampled instants (no geometric simplification).
package main

import (
	"compress/gzip"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// tripSeg is one gap-free run of a trajectory: WGS84 [lon,lat] points with their
// epoch-second timestamps.
type tripSeg struct {
	Path [][]float64
	Time []int64
}

// parseTripSegments turns a MovingPoint MF-JSON document into one segment per
// sequence, so temporal gaps stay separate trips rather than interpolating
// across them.
func parseTripSegments(b []byte) []tripSeg {
	var doc map[string]any
	if json.Unmarshal(b, &doc) != nil {
		return nil
	}
	var out []tripSeg
	collect := func(m map[string]any) {
		dts, _ := m["datetimes"].([]any)
		coords, _ := m["coordinates"].([]any)
		n := len(dts)
		if len(coords) < n {
			n = len(coords)
		}
		seg := tripSeg{}
		for i := 0; i < n; i++ {
			ts, _ := dts[i].(string)
			t, err := time.Parse(time.RFC3339, rfc3339Tz(ts))
			if err != nil {
				continue
			}
			c, _ := coords[i].([]any)
			if len(c) < 2 {
				continue
			}
			lon, _ := c[0].(float64)
			lat, _ := c[1].(float64)
			seg.Path = append(seg.Path, []float64{math.Round(lon*1e5) / 1e5, math.Round(lat*1e5) / 1e5})
			seg.Time = append(seg.Time, t.Unix())
		}
		if len(seg.Path) > 0 {
			out = append(out, seg)
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
	return out
}

// trajectories serves every feature's trajectory for animated rendering: sampled
// to ~sample seconds (default 180), in WGS84, as {trips, tmin, tmax}. Each trip
// is one gap-free segment with parallel path/timestamps arrays, ready for a
// DeckGL TripsLayer driven by a currentTime clock.
func trajectories(w http.ResponseWriter, r *http.Request) {
	cid := r.PathValue("cid")
	tbl, _, ok := collectionMeta(r.Context(), cid)
	if !ok {
		httpErr(w, 404, "collection not found")
		return
	}
	sample := 180
	if s := r.URL.Query().Get("sample"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n >= 10 {
			sample = n
		}
	}

	// Pin UTC/ISO for this query so asMFJSON emits RFC 3339 datetimes regardless
	// of the connection's locale default.
	tx, err := db.Begin(r.Context())
	if err != nil {
		httpErr(w, 500, err.Error())
		return
	}
	defer tx.Rollback(r.Context())
	tx.Exec(r.Context(), "SET LOCAL timezone = 'UTC'")
	tx.Exec(r.Context(), "SET LOCAL datestyle = 'ISO, YMD'")

	rows, err := tx.Query(r.Context(),
		"SELECT id, asMFJSON(transform(tsample(trip, make_interval(secs => $1)), 4326)) "+
			"FROM "+ident(tbl)+" WHERE numInstants(tsample(trip, make_interval(secs => $1))) > 1 ORDER BY id",
		sample)
	if err != nil {
		httpErr(w, 500, err.Error())
		return
	}
	defer rows.Close()

	type trip struct {
		ID   int         `json:"id"`
		Path [][]float64 `json:"path"`
		Time []int64     `json:"timestamps"`
	}
	trips := []trip{}
	var tmin int64 = math.MaxInt64
	var tmax int64 = math.MinInt64
	for rows.Next() {
		var id int
		var mf *string
		if rows.Scan(&id, &mf) != nil || mf == nil {
			continue
		}
		for _, s := range parseTripSegments([]byte(*mf)) {
			if len(s.Path) < 2 {
				continue
			}
			trips = append(trips, trip{ID: id, Path: s.Path, Time: s.Time})
			if s.Time[0] < tmin {
				tmin = s.Time[0]
			}
			if s.Time[len(s.Time)-1] > tmax {
				tmax = s.Time[len(s.Time)-1]
			}
		}
	}
	if err := rows.Err(); err != nil {
		httpErr(w, 500, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	var out io.Writer = w
	if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
		w.Header().Set("Content-Encoding", "gzip")
		gz := gzip.NewWriter(w)
		defer gz.Close()
		out = gz
	}
	json.NewEncoder(out).Encode(map[string]any{
		"trips": trips, "tmin": tmin, "tmax": tmax, "sampleSeconds": sample,
	})
}
