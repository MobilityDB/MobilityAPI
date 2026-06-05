// Aggregate time series for the animated tutorial's evolving chart: the average
// of a temporal property across the fleet, grouped by ship type, over uniform
// time buckets. It is the animated sibling of the static per-category charts —
// the map clock sweeps it in lockstep. MEOS does the work: each vessel's stored
// property is instant-sampled (tsample) and the samples are averaged per
// (ship type, bucket). Instant sampling is exact at the sampled instants.
package main

import (
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// parseFloatSeries reads (timestamp, value) pairs from a MovingFloat MF-JSON
// document, handling both the flat form and a sequence set.
func parseFloatSeries(b []byte) []struct {
	t int64
	v float64
} {
	var doc map[string]any
	if json.Unmarshal(b, &doc) != nil {
		return nil
	}
	var out []struct {
		t int64
		v float64
	}
	collect := func(m map[string]any) {
		dts, _ := m["datetimes"].([]any)
		vals, _ := m["values"].([]any)
		n := len(dts)
		if len(vals) < n {
			n = len(vals)
		}
		for i := 0; i < n; i++ {
			ts, _ := dts[i].(string)
			tm, err := time.Parse(time.RFC3339, rfc3339Tz(ts))
			if err != nil {
				continue
			}
			v, ok := vals[i].(float64)
			if !ok {
				continue
			}
			out = append(out, struct {
				t int64
				v float64
			}{tm.Unix(), v})
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

// timeseries serves the fleet-wide average of the `speed` property, grouped by
// ship type, over `step`-second buckets: {buckets:[epoch…], uom, series:{type:[avg|null…]}}.
func timeseries(w http.ResponseWriter, r *http.Request) {
	cid := r.PathValue("cid")
	tbl, _, ok := collectionMeta(r.Context(), cid)
	if !ok {
		httpErr(w, 404, "collection not found")
		return
	}
	step := 600
	if s := r.URL.Query().Get("step"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n >= 60 {
			step = n
		}
	}

	rows, err := db.Query(r.Context(),
		"SELECT COALESCE(s.ship_type, 'Other'), "+
			"asMFJSON(tsample(p.vfloat, make_interval(secs => $2))) "+
			"FROM "+ident(tbl)+" s JOIN mf_tproperty p ON p.cid = $1 AND p.fid = s.id AND p.name = 'speed'",
		cid, step)
	if err != nil {
		httpErr(w, 500, err.Error())
		return
	}
	defer rows.Close()

	type acc struct {
		sum float64
		n   int
	}
	data := map[string]map[int64]*acc{} // ship type -> bucket start (epoch) -> accumulator
	var tmin int64 = 1<<62 - 1
	var tmax int64 = -(1 << 62)
	for rows.Next() {
		var typ, mf *string
		if rows.Scan(&typ, &mf) != nil || mf == nil {
			continue
		}
		t := "Other"
		if typ != nil && *typ != "" {
			t = *typ
		}
		bk := data[t]
		if bk == nil {
			bk = map[int64]*acc{}
			data[t] = bk
		}
		for _, p := range parseFloatSeries([]byte(*mf)) {
			b := p.t - (p.t % int64(step))
			a := bk[b]
			if a == nil {
				a = &acc{}
				bk[b] = a
			}
			a.sum += p.v
			a.n++
			if b < tmin {
				tmin = b
			}
			if b > tmax {
				tmax = b
			}
		}
	}
	if err := rows.Err(); err != nil {
		httpErr(w, 500, err.Error())
		return
	}

	out := map[string]any{"uom": "kn", "stepSeconds": step}
	if tmax < tmin {
		out["buckets"] = []int64{}
		out["series"] = map[string][]any{}
	} else {
		var buckets []int64
		for b := tmin; b <= tmax; b += int64(step) {
			buckets = append(buckets, b)
		}
		idx := map[int64]int{}
		for i, b := range buckets {
			idx[b] = i
		}
		series := map[string][]*float64{}
		types := make([]string, 0, len(data))
		for t := range data {
			types = append(types, t)
		}
		sort.Strings(types)
		for _, t := range types {
			row := make([]*float64, len(buckets))
			for b, a := range data[t] {
				if a.n > 0 {
					avg := a.sum / float64(a.n)
					row[idx[b]] = &avg
				}
			}
			series[t] = row
		}
		out["buckets"] = buckets
		out["series"] = series
	}

	w.Header().Set("Content-Type", "application/json")
	var wr io.Writer = w
	if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
		w.Header().Set("Content-Encoding", "gzip")
		gz := gzip.NewWriter(w)
		defer gz.Close()
		wr = gz
	}
	json.NewEncoder(wr).Encode(out)
}
