//go:build meos

// meosEngine is the default streaming engine: it runs MEOS in process through
// cgo, applying each lifted operation to a stream record the way a Flink/Spark
// operator would call a JMEOS UDF — no SQL, no database in the loop. It is
// built only under `-tags meos` (it links libmeos via cgo), so the default
// build stays cgo-free; the cluster engines (Flink/Kafka/Spark) plug into the
// same StreamEngine seam.
//
// MEOS state is per-thread (PROJ context, SRS/ways caches, RNGs, errno, and the
// session timezone are thread-local; the error handler is process-global). Per
// that contract each query runs on its own OS-locked thread that calls
// meos_initialize() before its first MEOS call and meos_finalize() on exit, so
// queries run in parallel with no shared MEOS state. A stream record is an
// instant, on which every lifted function is exact (discrete interpolation
// applies it pointwise), so a continuous transform introduces no approximation.
package main

/*
#cgo CFLAGS: -I/usr/local/include
#cgo LDFLAGS: -lmeos
#include <stdlib.h>
#include <meos.h>

// degrees takes a normalize flag; wrap it so the Go side needs no C bool.
static Temporal *mfs_tfloat_degrees(const Temporal *t) { return tfloat_degrees(t, false); }
*/
import "C"

import (
	"context"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"unsafe"
)

// defaultStreamEngine returns the in-process engine. MEOS is initialised
// per-query on the query's own worker thread, not here, per the per-thread
// contract.
func defaultStreamEngine() (StreamEngine, error) {
	return &meosEngine{}, nil
}

type meosEngine struct{}

func (e *meosEngine) Name() string { return "meos-local" }

func (e *meosEngine) Submit(ctx context.Context, spec QuerySpec, source <-chan Instant) (QueryHandle, error) {
	if _, ok := liftedOps[spec.Op]; !ok {
		return nil, fmt.Errorf("unknown operation %q", spec.Op)
	}
	h := &meosHandle{results: make(chan Instant, 64), status: "running"}
	go h.run(ctx, spec, source)
	return h, nil
}

// run pins the query to one OS thread, initialises MEOS on it, transforms each
// record, and finalises MEOS before releasing the thread.
func (h *meosHandle) run(ctx context.Context, spec QuerySpec, source <-chan Instant) {
	runtime.LockOSThread()
	C.meos_initialize()
	C.meos_initialize_noexit_error_handler()
	defer runtime.UnlockOSThread() // runs last: keep the thread locked through meos_finalize
	defer C.meos_finalize()
	defer close(h.results)
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
			select {
			case h.results <- out:
			case <-ctx.Done():
				h.setStatus("stopped")
				return
			}
		}
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
		res = C.mfs_tfloat_degrees(temp)
	case "radians":
		res = C.tfloat_radians(temp)
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

// meosHandle exposes the result channel and the live status of a running query.
type meosHandle struct {
	results chan Instant
	mu      sync.Mutex
	status  string
}

func (h *meosHandle) Results() <-chan Instant { return h.results }

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
