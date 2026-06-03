//go:build !duckdb

package main

import "errors"

// openDuck is a stub in the default build; the DuckDB/MobilityDuck engine links
// libduckdb via cgo and is compiled in only under `-tags duckdb`.
func openDuck(dsn string) (Backend, error) {
	return nil, errors.New("duckdb backend not built: rebuild with -tags duckdb")
}
