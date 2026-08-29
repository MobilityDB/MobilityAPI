//go:build meos

// meosEngine is the default streaming engine: it runs MEOS in process through
// cgo, applying each lifted operation to a stream record the way a Flink/Spark
// operator would call a JMEOS UDF — no SQL, no database in the loop. It is
// built only under `-tags meos` (it links libmeos via cgo), so the default
// build stays cgo-free; the cluster engines (Flink/Kafka/Spark) plug into the
// same StreamEngine seam.
//
// The MEOS lifecycle has two levels and this engine drives both. meos_initialize
// installs the process-global allocator and error handler, and meos_finalize
// tears down process-global caches (PROJ and its SRS cache, the PRNGs), so the
// pair runs once for the process. The caches a query's own calls read -- the
// session timezone and the collation -- are thread-local, so each query
// initialises those on its own OS-locked thread; PROJ, GEOS and the RNGs are
// created lazily per thread inside MEOS. A stream record is an instant, on
// which every lifted function is exact (discrete interpolation applies it
// pointwise), so a continuous transform introduces no approximation.
package main

/*
#cgo CFLAGS: -I/usr/local/include
#cgo LDFLAGS: -lmeos
#include <stdlib.h>
#include <meos.h>
*/
import "C"

import (
	"context"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"
)

// defaultStreamEngine returns the in-process engine. MEOS is initialised on the
// query's own worker thread, not here, per the two-level lifecycle above.
func defaultStreamEngine() (StreamEngine, error) {
	return &meosEngine{}, nil
}

// meosProcess guards the process-global half of the lifecycle.
var meosProcess sync.Once

// initMEOSProcess installs the allocator and the process-global error handler
// once for the tier. meos_initialize installs the default handler, which ends
// the process on a MEOS error, so the no-exit handler is installed after it: a
// malformed record must fail its query, not the tier.
func initMEOSProcess() {
	meosProcess.Do(func() {
		C.meos_initialize()
		C.meos_initialize_noexit_error_handler()
	})
}

// initMEOSThread initialises the thread-local caches of the calling locked OS
// thread, and finalizeMEOSThread releases them. Each runs exactly once per
// thread: the query's goroutine never unlocks its thread, so the thread ends
// with the query and serves no second one.
func initMEOSThread() {
	utc := C.CString("UTC")
	defer C.free(unsafe.Pointer(utc))
	C.meos_initialize_timezone(utc)
	C.meos_initialize_collation()
}

func finalizeMEOSThread() {
	C.meos_finalize_collation()
	C.meos_finalize_timezone()
}

type meosEngine struct{}

func (e *meosEngine) Name() string { return "meos-local" }

func (e *meosEngine) Submit(ctx context.Context, spec QuerySpec, source <-chan Instant) (QueryHandle, error) {
	if spec.Agg != "" {
		if !aggregations[spec.Agg] {
			return nil, fmt.Errorf("unknown aggregation %q", spec.Agg)
		}
	} else if _, ok := liftedOps[spec.Op]; !ok {
		return nil, fmt.Errorf("unknown operation %q", spec.Op)
	}
	h := &meosHandle{results: make(chan Event, 64), status: "running"}
	go h.run(ctx, spec, source)
	return h, nil
}

// run pins the query to one OS thread, initialises that thread's MEOS caches,
// processes each record, and releases the caches when the query ends. The
// thread is deliberately never unlocked: the goroutine's exit ends the OS
// thread, so the caches are initialised and released exactly once on it.
func (h *meosHandle) run(ctx context.Context, spec QuerySpec, source <-chan Instant) {
	runtime.LockOSThread()
	initMEOSProcess()
	initMEOSThread()
	defer finalizeMEOSThread()
	defer close(h.results)
	if spec.Agg != "" {
		h.runAggregate(ctx, spec, source)
		return
	}
	h.runTransform(ctx, spec, source)
}

// runTransform applies a lifted operation to each record and emits the result.
func (h *meosHandle) runTransform(ctx context.Context, spec QuerySpec, source <-chan Instant) {
	for {
		select {
		case <-ctx.Done():
			h.setStatus("stopped")
			return
		case in, ok := <-source:
			if !ok {
				h.setStatus("stopped")
				return
			}
			out, err := liftInstant(spec.Op, spec.Arg, in)
			if err != nil {
				h.setStatus("failed")
				return
			}
			ev := Event{"datetime": out.T, "value": out.V, "property": spec.Pname, "operation": spec.Op}
			if !h.emit(ctx, ev) {
				return
			}
		}
	}
}

// runAggregate groups records into windows and emits one aggregate per window.
// COUNT windows close every spec.Window.Size records; TUMBLING windows close
// when a record's timestamp crosses the next span boundary.
func (h *meosHandle) runAggregate(ctx context.Context, spec QuerySpec, source <-chan Instant) {
	if spec.Window.Type == "HOPPING" {
		h.runHopping(ctx, spec, source)
		return
	}
	var win []Instant
	tumblingSpan := tumblingSpanSeconds(spec.Window)
	var winEndUnix int64 // exclusive upper bound for the open TUMBLING window
	for {
		select {
		case <-ctx.Done():
			h.setStatus("stopped")
			return
		case in, ok := <-source:
			if !ok {
				h.setStatus("stopped")
				return
			}
			if spec.Window.Type == "TUMBLING" {
				ts := instantUnix(in, len(win)) // best-effort event time
				if len(win) > 0 && ts >= winEndUnix {
					if !h.emitWindow(ctx, spec, win) {
						return
					}
					win = win[:0]
				}
				if len(win) == 0 {
					winEndUnix = ts - (ts % tumblingSpan) + tumblingSpan
				}
				win = append(win, in)
			} else { // COUNT
				win = append(win, in)
				if len(win) >= spec.Window.Size {
					if !h.emitWindow(ctx, spec, win) {
						return
					}
					win = win[:0]
				}
			}
		}
	}
}

// runHopping emits one aggregate every hop over the records of the last span —
// overlapping windows. It keeps a buffer of recent records, advances the emit
// boundary by the hop, and evicts records that can no longer fall in any window.
func (h *meosHandle) runHopping(ctx context.Context, spec QuerySpec, source <-chan Instant) {
	span := tumblingSpanSeconds(spec.Window)
	hop := hopSeconds(spec.Window)
	var buf []Instant
	var bufT []int64
	idx := 0
	var nextEmit int64
	started := false
	for {
		select {
		case <-ctx.Done():
			h.setStatus("stopped")
			return
		case in, ok := <-source:
			if !ok {
				h.setStatus("stopped")
				return
			}
			t := instantUnix(in, idx)
			idx++
			if !started {
				nextEmit = t - (t % hop) + hop
				started = true
			}
			buf = append(buf, in)
			bufT = append(bufT, t)
			for t >= nextEmit {
				lo := nextEmit - span
				var win []Instant
				for i, bt := range bufT {
					if bt >= lo && bt < nextEmit {
						win = append(win, buf[i])
					}
				}
				if len(win) > 0 {
					if !h.emitWindow(ctx, spec, win) {
						return
					}
				}
				nextEmit += hop
				keepFrom := nextEmit - span
				k := 0
				for k < len(bufT) && bufT[k] < keepFrom {
					k++
				}
				buf = buf[k:]
				bufT = bufT[k:]
			}
		}
	}
}

// hopSeconds converts a HOPPING window hop+unit into seconds.
func hopSeconds(w Window) int64 {
	hop := int64(w.Hop)
	switch w.HopUnit {
	case "MINUTES":
		hop *= 60
	case "HOURS":
		hop *= 3600
	}
	if hop <= 0 {
		hop = 1
	}
	return hop
}

// emitWindow computes the aggregate over a closed window and emits it.
func (h *meosHandle) emitWindow(ctx context.Context, spec QuerySpec, win []Instant) bool {
	val, count, err := windowAggregate(spec.Ptype, spec.Agg, win)
	if err != nil {
		h.setStatus("failed")
		return false
	}
	ev := Event{
		"windowStart": win[0].T,
		"windowEnd":   win[len(win)-1].T,
		"property":    spec.Pname,
		"aggregation": spec.Agg,
		"value":       val,
		"count":       count,
	}
	return h.emit(ctx, ev)
}

func (h *meosHandle) emit(ctx context.Context, ev Event) bool {
	select {
	case h.results <- ev:
		return true
	case <-ctx.Done():
		h.setStatus("stopped")
		return false
	}
}

// liftInstant applies one lifted scalar operation to a single tfloat instant,
// in process through MEOS. It must be called on a thread that has called
// meos_initialize(). The operation is pointwise and time-invariant, so the
// value is computed on a one-instant tfloat at a fixed canonical time and the
// record's own timestamp is carried through verbatim, independent of how the
// source serialises it.
func liftInstant(op string, arg float64, in Instant) (Instant, error) {
	txt := strconv.FormatFloat(in.V, 'g', -1, 64) + "@2000-01-01 00:00:00+00"
	cs := C.CString(txt)
	defer C.free(unsafe.Pointer(cs))
	temp := C.tfloat_in(cs)
	if temp == nil {
		return Instant{}, fmt.Errorf("tfloat_in failed for %q", txt)
	}
	defer C.free(unsafe.Pointer(temp))

	var res *C.Temporal
	switch op {
	case "ln":
		res = C.tfloat_ln(temp)
	case "exp":
		res = C.tfloat_exp(temp)
	case "log10":
		res = C.tfloat_log10(temp)
	case "ceil":
		res = C.tfloat_ceil(temp)
	case "floor":
		res = C.tfloat_floor(temp)
	case "abs":
		res = C.tnumber_abs(temp)
	case "degrees":
		res = C.tfloat_degrees(temp, false)
	case "radians":
		res = C.tfloat_radians(temp)
	case "sin":
		res = C.tfloat_sin(temp)
	case "cos":
		res = C.tfloat_cos(temp)
	case "tan":
		res = C.tfloat_tan(temp)
	case "add":
		res = C.add_tfloat_float(temp, C.double(arg))
	case "sub":
		res = C.sub_tfloat_float(temp, C.double(arg))
	case "mul":
		res = C.mul_tfloat_float(temp, C.double(arg))
	case "div":
		res = C.div_tfloat_float(temp, C.double(arg))
	default:
		return Instant{}, fmt.Errorf("unknown operation %q", op)
	}
	if res == nil {
		return Instant{}, fmt.Errorf("operation %q produced no result (domain error?)", op)
	}
	defer C.free(unsafe.Pointer(res))

	out := C.tfloat_out(res, C.int(15))
	defer C.free(unsafe.Pointer(out))
	return parseInstantText(C.GoString(out), in.T)
}

// parseInstantText reads MEOS's "value@timestamp" output back into an Instant,
// keeping the original timestamp (a pointwise lift preserves time).
func parseInstantText(s, t string) (Instant, error) {
	vs := s
	if at := strings.IndexByte(s, '@'); at >= 0 {
		vs = s[:at]
	}
	v, err := strconv.ParseFloat(strings.TrimSpace(vs), 64)
	if err != nil {
		return Instant{}, fmt.Errorf("cannot parse MEOS output %q: %w", s, err)
	}
	return Instant{T: t, V: v}, nil
}

// windowAggregate computes one aggregation over a window's values through MEOS.
// The window's values are assembled into a discrete tfloat at synthetic, ordered
// timestamps (the aggregate is value-based, so the times only order the records),
// and the MEOS accessor for the aggregation is applied.
// windowAggregate computes one aggregation over a window's values through MEOS,
// dispatching on the property type.
func windowAggregate(ptype, agg string, win []Instant) (any, int, error) {
	switch ptype {
	case "TText":
		return textAggregate(agg, win)
	case "TBoolean":
		return boolAggregate(agg, win)
	default:
		return numAggregate(agg, win)
	}
}

// synthWindowText assembles a window's values into a discrete temporal literal
// "{v0@t0, v1@t1, …}" at synthetic, ordered timestamps (the aggregate is
// value-based, so the times only order the records). value renders each record.
func synthWindowText(win []Instant, value func(Instant) string) string {
	base := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	var b strings.Builder
	b.WriteByte('{')
	for i, in := range win {
		if i > 0 {
			b.WriteString(", ")
		}
		ts := base.Add(time.Duration(i)*time.Second).Format("2006-01-02 15:04:05") + "+00"
		b.WriteString(value(in))
		b.WriteByte('@')
		b.WriteString(ts)
	}
	b.WriteByte('}')
	return b.String()
}

func numAggregate(agg string, win []Instant) (any, int, error) {
	base := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	var b strings.Builder
	b.WriteByte('{')
	for i, in := range win {
		if i > 0 {
			b.WriteString(", ")
		}
		ts := base.Add(time.Duration(i)*time.Second).Format("2006-01-02 15:04:05") + "+00"
		b.WriteString(strconv.FormatFloat(in.V, 'g', -1, 64))
		b.WriteByte('@')
		b.WriteString(ts)
	}
	b.WriteByte('}')

	cs := C.CString(b.String())
	defer C.free(unsafe.Pointer(cs))
	t := C.tfloat_in(cs)
	if t == nil {
		return 0, 0, fmt.Errorf("tfloat_in failed for the window")
	}
	defer C.free(unsafe.Pointer(t))
	n := int(C.temporal_num_instants(t))

	switch agg {
	case "COUNT":
		return float64(n), n, nil
	case "MIN":
		return float64(C.tfloat_min_value(t)), n, nil
	case "MAX":
		return float64(C.tfloat_max_value(t)), n, nil
	case "AVG":
		return float64(C.tnumber_avg_value(t)), n, nil
	case "SUM":
		var cnt C.int
		arr := C.tfloat_values(t, &cnt)
		defer C.free(unsafe.Pointer(arr))
		vals := unsafe.Slice((*C.double)(arr), int(cnt))
		sum := 0.0
		for _, v := range vals {
			sum += float64(v)
		}
		return sum, n, nil
	}
	return 0, n, fmt.Errorf("unknown aggregation %q", agg)
}

// escapeText renders a text value as a quoted MEOS text literal.
func escapeText(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "\"", "\\\"")
	return "\"" + s + "\""
}

// textAggregate computes COUNT / COUNT_DISTINCT over a TText window.
func textAggregate(agg string, win []Instant) (any, int, error) {
	cs := C.CString(synthWindowText(win, func(in Instant) string { return escapeText(in.S) }))
	defer C.free(unsafe.Pointer(cs))
	t := C.ttext_in(cs)
	if t == nil {
		return nil, 0, fmt.Errorf("ttext_in failed for the window")
	}
	defer C.free(unsafe.Pointer(t))
	n := int(C.temporal_num_instants(t))
	switch agg {
	case "COUNT":
		return float64(n), n, nil
	case "COUNT_DISTINCT":
		var cnt C.int
		arr := C.ttext_values(t, &cnt)
		defer C.free(unsafe.Pointer(arr))
		texts := unsafe.Slice(arr, int(cnt))
		set := map[string]struct{}{}
		for _, tx := range texts {
			cstr := C.text_out(tx)
			set[C.GoString(cstr)] = struct{}{}
			C.free(unsafe.Pointer(cstr))
			C.free(unsafe.Pointer(tx))
		}
		return float64(len(set)), n, nil
	}
	return nil, n, fmt.Errorf("unknown aggregation %q", agg)
}

// boolAggregate computes COUNT / ANY / ALL / COUNT_TRUE / COUNT_FALSE over a
// TBool window.
func boolAggregate(agg string, win []Instant) (any, int, error) {
	cs := C.CString(synthWindowText(win, func(in Instant) string {
		if in.S == "t" || in.S == "true" {
			return "t"
		}
		return "f"
	}))
	defer C.free(unsafe.Pointer(cs))
	t := C.tbool_in(cs)
	if t == nil {
		return nil, 0, fmt.Errorf("tbool_in failed for the window")
	}
	defer C.free(unsafe.Pointer(t))
	n := int(C.temporal_num_instants(t))
	var cnt C.int
	arr := C.tbool_values(t, &cnt)
	defer C.free(unsafe.Pointer(arr))
	vals := unsafe.Slice((*byte)(unsafe.Pointer(arr)), int(cnt))
	trueCount := 0
	for _, v := range vals {
		if v != 0 {
			trueCount++
		}
	}
	switch agg {
	case "COUNT":
		return float64(n), n, nil
	case "COUNT_TRUE":
		return float64(trueCount), n, nil
	case "COUNT_FALSE":
		return float64(n - trueCount), n, nil
	case "ANY":
		return trueCount > 0, n, nil
	case "ALL":
		return trueCount == n, n, nil
	}
	return nil, n, fmt.Errorf("unknown aggregation %q", agg)
}

// tumblingSpanSeconds converts a TUMBLING window size+unit into seconds.
func tumblingSpanSeconds(w Window) int64 {
	span := int64(w.Size)
	switch w.Unit {
	case "MINUTES":
		span *= 60
	case "HOURS":
		span *= 3600
	}
	if span <= 0 {
		span = 1
	}
	return span
}

// instantUnix parses a record's timestamp to Unix seconds for TUMBLING-window
// assignment, falling back to the record's index when the source serialises the
// timestamp in a non-standard form.
func instantUnix(in Instant, idx int) int64 {
	for _, layout := range []string{time.RFC3339, "2006-01-02T15:04:05Z07:00", "2006-01-02 15:04:05Z07:00", "2006-01-02 15:04:05-07"} {
		if ts, err := time.Parse(layout, in.T); err == nil {
			return ts.Unix()
		}
	}
	return int64(idx)
}

// meosHandle exposes the result channel and the live status of a running query.
type meosHandle struct {
	results chan Event
	mu      sync.Mutex
	status  string
}

func (h *meosHandle) Results() <-chan Event { return h.results }

func (h *meosHandle) Status() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.status
}

func (h *meosHandle) setStatus(s string) {
	h.mu.Lock()
	h.status = s
	h.mu.Unlock()
}

// Stop is a no-op: the control plane cancels the context, which ends the run
// goroutine (finalising MEOS on its thread) and closes the result channel.
func (h *meosHandle) Stop() error { return nil }
