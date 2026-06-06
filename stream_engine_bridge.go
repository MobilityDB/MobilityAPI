// bridgeEngine is the cluster-engine realization of the StreamEngine seam shared
// by every external streaming runtime (Flink, Kafka Streams, …): it runs each
// continuous query as a job in that runtime and bridges the job's output back
// into the control plane's SSE delivery. The control plane is unchanged —
// register, lifecycle and SSE are identical to the in-process meos-local engine;
// only the per-record execution moves to the runtime, where MEOS runs as a JMEOS
// UDF (no SQL).
//
// The engine is pure Go (it spawns the bridge job and pipes through it), so it
// builds without cgo. The job command is configured by <RUNTIME>_CMD (the command
// prefix; the operation and its scalar argument are appended) and <RUNTIME>_LIBPATH
// (the libmeos path passed as LD_LIBRARY_PATH): MFAPI_FLINK_CMD/MFAPI_FLINK_LIBPATH
// for Flink (MFAPI_STREAM_ENGINE=flink), MFAPI_KAFKA_CMD/MFAPI_KAFKA_LIBPATH for
// Kafka Streams (MFAPI_STREAM_ENGINE=kafka).
//
// One float per line goes to the job's stdin; one transformed float per line
// comes back on stdout, in order (the job runs at parallelism 1), so each output
// is paired with its source instant's timestamp through a FIFO.
package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
)

func newBridgeEngine(name, cmdEnv, libEnv string) (StreamEngine, error) {
	cmd := os.Getenv(cmdEnv)
	if strings.TrimSpace(cmd) == "" {
		return nil, fmt.Errorf("%s engine requires %s (the bridge-job command prefix)", name, cmdEnv)
	}
	return &bridgeEngine{name: name, argv: strings.Fields(cmd), libPath: os.Getenv(libEnv)}, nil
}

func newFlinkEngine() (StreamEngine, error) {
	return newBridgeEngine("flink", "MFAPI_FLINK_CMD", "MFAPI_FLINK_LIBPATH")
}

func newKafkaEngine() (StreamEngine, error) {
	return newBridgeEngine("kafka", "MFAPI_KAFKA_CMD", "MFAPI_KAFKA_LIBPATH")
}

type bridgeEngine struct {
	name    string
	argv    []string
	libPath string
}

func (e *bridgeEngine) Name() string { return e.name }

func (e *bridgeEngine) Submit(ctx context.Context, spec QuerySpec, source <-chan Instant) (QueryHandle, error) {
	if spec.Agg != "" {
		return nil, fmt.Errorf("the %s engine runs transforms; windowed aggregation is served by the in-process engine", e.name)
	}
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
		return nil, fmt.Errorf("start %s bridge job: %w", e.name, err)
	}

	h := &bridgeHandle{results: make(chan Event, 64), status: "running"}
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
			ev := Event{"datetime": t, "value": v, "property": spec.Pname, "operation": spec.Op}
			select {
			case h.results <- ev:
			case <-ctx.Done():
				h.setStatus("stopped")
				return
			}
		}
		h.setStatus("stopped")
	}()

	return h, nil
}

// bridgeHandle exposes the result channel and the live status of a query running
// on a bridge-job runtime (Flink, Kafka Streams, …).
type bridgeHandle struct {
	results chan Event
	mu      sync.Mutex
	status  string
}

func (h *bridgeHandle) Results() <-chan Event { return h.results }

func (h *bridgeHandle) Status() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.status
}

func (h *bridgeHandle) setStatus(s string) {
	h.mu.Lock()
	h.status = s
	h.mu.Unlock()
}

// Stop is a no-op: the control plane cancels the context, which kills the bridge
// job subprocess (exec.CommandContext) and ends the feeder/reader goroutines.
func (h *bridgeHandle) Stop() error { return nil }
