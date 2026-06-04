//go:build !meos

// The default build links no MEOS, so the streaming engine is unavailable: the
// control plane compiles and runs, and the continuous-query endpoints report
// that the engine must be built in. Rebuild with `-tags meos` (and libmeos on
// the link path) to enable the in-process MEOS engine; the Flink/Kafka/Spark
// engines plug into the same StreamEngine seam.
package main

import "errors"

func defaultStreamEngine() (StreamEngine, error) {
	return nil, errors.New("streaming engine not built: rebuild with -tags meos (libmeos required)")
}
