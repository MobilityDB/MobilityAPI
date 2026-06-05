// flinkEngine is the cluster-engine realization of the StreamEngine seam: it
// runs each continuous query as a Flink DataStream job (MobilityFlink's
// MeosStatelessMap wiring over JMEOS) and bridges the job's output back into the
// control plane's SSE delivery. The control plane is unchanged — register,
// lifecycle and SSE are identical to the in-process meos-local engine; only the
// per-record execution moves to Flink, where MEOS runs as a JMEOS UDF (no SQL).
//
// The engine is pure Go (it spawns the bridge job and pipes through it), so it
// builds without cgo. The job command is configured by MFAPI_FLINK_CMD (the
// command prefix; the operation and its scalar argument are appended) and
// MFAPI_FLINK_LIBPATH (the libmeos path passed as LD_LIBRARY_PATH). Select it
// with MFAPI_STREAM_ENGINE=flink.
//
// One float per line goes to the job's stdin; one transformed float per line
// comes back on stdout, in order (the job runs at parallelism 1), so each output
// is paired with its source instant's timestamp through a FIFO.
package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
)

func newFlinkEngine() (StreamEngine, error) {
	cmd := os.Getenv("MFAPI_FLINK_CMD")
	if strings.TrimSpace(cmd) == "" {
		return nil, errors.New("flink engine requires MFAPI_FLINK_CMD (the bridge-job command prefix)")
	}
	return &flinkEngine{argv: strings.Fields(cmd), libPath: os.Getenv("MFAPI_FLINK_LIBPATH")}, nil
}

type flinkEngine struct {
	argv    []string
	libPath string
}

func (e *flinkEngine) Name() string { return "flink" }

func (e *flinkEngine) Submit(ctx context.Context, spec QuerySpec, source <-chan Instant) (QueryHandle, error) {
	info, ok := liftedOps[spec.Op]
	if !ok {
		return nil, fmt.Errorf("unknown operation %q", spec.Op)
	}
	args := append([]string{}, e.argv[1:]...)
	args = append(args, spec.Op)
	if info.needsArg {
		args = append(args, strconv.FormatFloat(spec.Arg, 'g', -1, 64))
	}
	cmd := exec.CommandContext(ctx, e.argv[0], args...)
	cmd.Env = os.Environ()
	if e.libPath != "" {
		cmd.Env = append(cmd.Env, "LD_LIBRARY_PATH="+e.libPath)
	}
	cmd.Stderr = io.Discard
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start flink bridge job: %w", err)
	}

	h := &flinkHandle{results: make(chan Instant, 64), status: "running"}
	ts := make(chan string, 1024) // source timestamps, paired with outputs by order

	// feeder: source instant values → the job's stdin (one float per line).
	go func() {
		defer stdin.Close()
		w := bufio.NewWriter(stdin)
		for {
			select {
			case <-ctx.Done():
				return
			case in, ok := <-source:
				if !ok {
					return
				}
				select {
				case ts <- in.T:
				case <-ctx.Done():
					return
				}
				if _, err := w.WriteString(strconv.FormatFloat(in.V, 'g', -1, 64) + "\n"); err != nil {
					return
				}
				if err := w.Flush(); err != nil {
					return
				}
			}
		}
	}()

	// reader: the job's stdout (transformed floats) → result instants, each
	// carrying its source instant's timestamp.
	go func() {
		defer close(h.results)
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			v, err := strconv.ParseFloat(strings.TrimSpace(sc.Text()), 64)
			if err != nil {
				continue
			}
			var t string
			select {
			case t = <-ts:
			case <-ctx.Done():
				h.setStatus("stopped")
				return
			}
			select {
			case h.results <- Instant{T: t, V: v}:
			case <-ctx.Done():
				h.setStatus("stopped")
				return
			}
		}
		h.setStatus("stopped")
	}()

	return h, nil
}

// flinkHandle exposes the result channel and the live status of a query running
// on the Flink bridge job.
type flinkHandle struct {
	results chan Instant
	mu      sync.Mutex
	status  string
}

func (h *flinkHandle) Results() <-chan Instant { return h.results }

func (h *flinkHandle) Status() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.status
}

func (h *flinkHandle) setStatus(s string) {
	h.mu.Lock()
	h.status = s
	h.mu.Unlock()
}

// Stop is a no-op: the control plane cancels the context, which kills the bridge
// job subprocess (exec.CommandContext) and ends the feeder/reader goroutines.
func (h *flinkHandle) Stop() error { return nil }
