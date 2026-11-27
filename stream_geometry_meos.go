//go:build meos

// The window aggregations of a geometry continuous query
// (#geometryAggregations), computed by MEOS in process on the query's own
// thread, as #runAggregate computes those of a temporal property: the
// feature's temporal geometry is read once from its WKB, each window is its
// restriction to the window's span, and each aggregation is one MEOS function
// of it.
package main

/*
#cgo pkg-config: meos
#include <stdlib.h>
#include <meos.h>
#include <meos_geo.h>
#include <meos_cbuffer.h>
#include <meos_rgeo.h>
*/
import "C"

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
	"unsafe"
)

// pgEpochUnix is the Unix second of 2000-01-01, the origin of a TimestampTz.
const pgEpochUnix = 946684800

// geoWindow is one window of a geometry query: its bounds as TimestampTz, the
// upper one included for a COUNT window, whose bounds are its first and last
// instants, and excluded for a time window.
type geoWindow struct {
	lo, hi   C.TimestampTz
	upperInc bool
}

// geometryWindows answers the windows of the feature's instants times: COUNT
// windows of Size consecutive instants, TUMBLING windows of a fixed span
// aligned on its multiples, and HOPPING windows of a fixed span ending on each
// multiple of the hop, as #runAggregate and #runHopping close them over a
// property's records.
func geometryWindows(times []C.TimestampTz, w Window) []geoWindow {
	var out []geoWindow
	if len(times) == 0 {
		return out
	}
	if w.Type == "COUNT" {
		for i := 0; i+w.Size <= len(times); i += w.Size {
			out = append(out, geoWindow{times[i], times[i+w.Size-1], true})
		}
		return out
	}
	usec := func(sec int64) C.TimestampTz { return C.TimestampTz((sec - pgEpochUnix) * 1000000) }
	unix := func(t C.TimestampTz) int64 { return int64(t)/1000000 + pgEpochUnix }
	first, last := unix(times[0]), unix(times[len(times)-1])
	span := tumblingSpanSeconds(w)
	if w.Type == "TUMBLING" {
		for lo := first - (first % span); lo <= last; lo += span {
			out = append(out, geoWindow{usec(lo), usec(lo + span), false})
		}
		return out
	}
	hop := hopSeconds(w)
	for end := first - (first % hop) + hop; end-span <= last; end += hop {
		out = append(out, geoWindow{usec(end - span), usec(end), false})
	}
	return out
}

// geometryQuery is what a geometry aggregation reads: the feature's temporal
// value, the times of its instants, and the region of an INTERSECT in the
// value's system.
type geometryQuery struct {
	temp   *C.Temporal
	times  []C.TimestampTz
	region *C.GSERIALIZED
}

// openGeometryQuery reads the feature's value from its hex WKB and the region
// of an INTERSECT from WKT in CRS84, carried into the value's system or read as
// a geography for a value of geographies.
func openGeometryQuery(spec QuerySpec) (*geometryQuery, error) {
	hex := C.CString(spec.Value)
	defer C.free(unsafe.Pointer(hex))
	q := &geometryQuery{temp: C.temporal_from_hexwkb(hex)}
	if q.temp == nil {
		return nil, fmt.Errorf("the feature's temporal geometry cannot be read")
	}
	// WKB states no SRID, so the value takes the collection's, which its
	// region and its path are read against
	if spec.SRID > 0 && (shapeTypes[spec.GeoType] || temporalTypeByName[spec.GeoType] != nil &&
		temporalTypeByName[spec.GeoType].MFJSON == "MovingPoint") {
		withSRID := C.tspatial_set_srid(q.temp, C.int32_t(spec.SRID))
		C.free(unsafe.Pointer(q.temp))
		q.temp = withSRID
		if q.temp == nil {
			return nil, fmt.Errorf("the feature's temporal geometry cannot take SRID %d", spec.SRID)
		}
	}
	var n C.int
	ts := C.temporal_timestamps(q.temp, &n)
	if ts != nil {
		q.times = append(q.times, unsafe.Slice(ts, int(n))...)
		C.free(unsafe.Pointer(ts))
	}
	if spec.Agg == "INTERSECT" {
		wkt := C.CString(spec.Region)
		defer C.free(unsafe.Pointer(wkt))
		g := C.geom_in(wkt, -1)
		if g == nil {
			q.close()
			return nil, fmt.Errorf("region %s is no WKT geometry", spec.Region)
		}
		g4326 := C.geo_set_srid(g, 4326)
		C.free(unsafe.Pointer(g))
		switch {
		case geographic(spec.GeoType):
			q.region = C.geom_to_geog(g4326)
			C.free(unsafe.Pointer(g4326))
		case spec.SRID != 4326:
			q.region = C.geo_transform(g4326, C.int32_t(spec.SRID))
			C.free(unsafe.Pointer(g4326))
		default:
			q.region = g4326
		}
		if q.region == nil {
			q.close()
			return nil, fmt.Errorf("region %s cannot be read in the collection's system", spec.Region)
		}
	}
	return q, nil
}

func (q *geometryQuery) close() {
	if q.temp != nil {
		C.free(unsafe.Pointer(q.temp))
	}
	if q.region != nil {
		C.free(unsafe.Pointer(q.region))
	}
}

// traversed answers what a value of lines or polygons covers, the union of its
// values or the region its motion sweeps, each overlap counted once.
func traversed(typ string, t *C.Temporal) *C.GSERIALIZED {
	switch typ {
	case "trgeometry":
		return C.trgeometry_traversed_area(t, true)
	case "tcbuffer":
		return C.tcbuffer_traversed_area(t, true)
	}
	return C.tgeo_traversed_area(t, true)
}

// geometryValue computes the aggregation of one window, sub being the value
// restricted to it.
func geometryValue(spec QuerySpec, q *geometryQuery, sub *C.Temporal) (any, error) {
	switch spec.Agg {
	case "LENGTH":
		if shapeTypes[spec.GeoType] {
			g := traversed(spec.GeoType, sub)
			if g == nil {
				return nil, fmt.Errorf("the window's covered geometry cannot be computed")
			}
			defer C.free(unsafe.Pointer(g))
			return float64(C.geo_length(g, false)), nil
		}
		// A point in longitude and latitude travels its distance on the spheroid
		if spec.GeoType == "tgeompoint" && spec.SRID == 4326 {
			geog := C.tgeompoint_to_tgeogpoint(sub)
			if geog == nil {
				return nil, fmt.Errorf("the window cannot be read on the spheroid")
			}
			defer C.free(unsafe.Pointer(geog))
			return float64(C.tpoint_length(geog, true)), nil
		}
		return float64(C.tpoint_length(sub, true)), nil
	case "AREA":
		g := traversed(spec.GeoType, sub)
		if g == nil {
			return nil, fmt.Errorf("the window's covered geometry cannot be computed")
		}
		defer C.free(unsafe.Pointer(g))
		return float64(C.geo_area(g, false)), nil
	case "TO_TRAJ":
		path := C.tpoint_trajectory(sub, false)
		if path == nil {
			return nil, fmt.Errorf("the window's path cannot be computed")
		}
		defer C.free(unsafe.Pointer(path))
		// GeoJSON is written in WGS 84 (RFC 7946)
		if spec.SRID != 4326 && !geographic(spec.GeoType) {
			wgs := C.geo_transform(path, 4326)
			if wgs == nil {
				return nil, fmt.Errorf("the window's path cannot be written in WGS 84")
			}
			defer C.free(unsafe.Pointer(wgs))
			path = wgs
		}
		var size C.size_t
		js := C.geo_as_geojson(path, 0, 15, nil, &size)
		if js == nil {
			return nil, fmt.Errorf("the window's path cannot be written in GeoJSON")
		}
		defer C.free(unsafe.Pointer(js))
		return json.RawMessage(C.GoString(js)), nil
	case "INTERSECT":
		var res C.int
		switch spec.GeoType {
		case "trgeometry":
			res = C.eintersects_trgeometry_geo(sub, q.region)
		case "tcbuffer":
			res = C.eintersects_tcbuffer_geo(sub, q.region)
		default:
			res = C.eintersects_tgeo_geo(sub, q.region)
		}
		if res < 0 {
			return nil, fmt.Errorf("the window cannot be related to the region")
		}
		return res == 1, nil
	}
	return nil, fmt.Errorf("unknown aggregation %q", spec.Agg)
}

// runGeometry reads the query's value, reports on ready whether it could, and
// emits one aggregate per window, paced and looping as the position stream
// loops over the stored trajectory.
func (h *meosHandle) runGeometry(ctx context.Context, spec QuerySpec, ready chan<- error) {
	q, err := openGeometryQuery(spec)
	ready <- err
	if err != nil {
		h.setStatus("failed")
		return
	}
	defer q.close()
	windows := geometryWindows(q.times, spec.Window)
	if len(windows) == 0 {
		h.setStatus("stopped")
		return
	}
	tick := time.NewTicker(spec.Interval)
	defer tick.Stop()
	for {
		for _, w := range windows {
			select {
			case <-ctx.Done():
				h.setStatus("stopped")
				return
			case <-tick.C:
			}
			count := 0
			for _, t := range q.times {
				if t >= w.lo && (t < w.hi || (w.upperInc && t == w.hi)) {
					count++
				}
			}
			span := C.tstzspan_make(w.lo, w.hi, true, C.bool(w.upperInc))
			sub := C.temporal_at_tstzspan(q.temp, span)
			C.free(unsafe.Pointer(span))
			if sub == nil {
				continue
			}
			var val any
			if spec.Agg == "COUNT" {
				val = count
			} else {
				val, err = geometryValue(spec, q, sub)
			}
			C.free(unsafe.Pointer(sub))
			if err != nil {
				h.setStatus("failed")
				return
			}
			ev := Event{
				"windowStart": pgTimeRFC3339(w.lo),
				"windowEnd":   pgTimeRFC3339(w.hi),
				"aggregation": spec.Agg,
				"value":       val,
				"count":       count,
			}
			if !h.emit(ctx, ev) {
				return
			}
		}
	}
}

// pgTimeRFC3339 writes a TimestampTz as an RFC 3339 UTC time.
func pgTimeRFC3339(t C.TimestampTz) string {
	return time.UnixMicro(int64(t) + pgEpochUnix*1000000).UTC().Format(time.RFC3339Nano)
}
