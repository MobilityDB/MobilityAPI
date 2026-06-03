//go:build !spark

package main

import "errors"

// openSpark is a stub in the default build; the MobilitySpark engine links the
// Spark Connect client and is compiled in only under -tags spark.
func openSpark(dsn string) (Backend, error) {
	return nil, errors.New("spark backend not built: rebuild with -tags spark")
}
