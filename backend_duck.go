//go:build duckdb

// duckBackend is the MobilityDuck (DuckDB) engine, selected by a duckdb:// DSN
// and built only under `-tags duckdb` (it links libduckdb via cgo, so the
// default PostgreSQL build stays a static, cgo-free binary). DuckDB is embedded
// and in-process — its idiom, not a wire-served database — so the MobilityDuck
// extension is LOADed on every pooled connection; the same canonical
// named-function SQL and the same Go response assembly serve it unchanged.
package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"os"
	"strings"

	"github.com/marcboeker/go-duckdb/v2"
)

func openDuck(dsn string) (Backend, error) {
	// duckdb:///path/to.duckdb | duckdb://:memory: | duckdb:// (in-memory)
	path := strings.TrimPrefix(dsn, "duckdb://")
	if path == ":memory:" {
		path = ""
	}
	cfg := "allow_unsigned_extensions=true"
	if path == "" {
		path = "?" + cfg
	} else {
		path += "?" + cfg
	}
	// Extensions to LOAD on each connection (comma-separated paths): the
	// MobilityDuck extension travels with the firm pin; spatial backs the
	// trajectory GeoJSON. DuckDB LOADs are per-connection.
	var exts []string
	for _, e := range strings.Split(os.Getenv("MFAPI_DUCKDB_EXTS"), ",") {
		if e = strings.TrimSpace(e); e != "" {
			exts = append(exts, e)
		}
	}
	connector, err := duckdb.NewConnector(path, func(execer driver.ExecerContext) error {
		for _, e := range exts {
			if _, err := execer.ExecContext(context.Background(), "LOAD '"+e+"'", nil); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &duckBackend{sql.OpenDB(connector)}, nil
}

type duckBackend struct{ db *sql.DB }

func (b *duckBackend) QueryRow(ctx context.Context, q string, a ...any) Row {
	return duckRow{b.db.QueryRowContext(ctx, q, a...)}
}
func (b *duckBackend) Query(ctx context.Context, q string, a ...any) (Rows, error) {
	rows, err := b.db.QueryContext(ctx, q, a...)
	if err != nil {
		return nil, err
	}
	return duckRows{rows}, nil
}
func (b *duckBackend) Exec(ctx context.Context, q string, a ...any) (int64, error) {
	res, err := b.db.ExecContext(ctx, q, a...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}
func (b *duckBackend) Begin(ctx context.Context) (Tx, error) {
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return duckTx{tx}, nil
}
func (b *duckBackend) Ping(ctx context.Context) error { return b.db.PingContext(ctx) }
func (b *duckBackend) Close()                         { b.db.Close() }

type duckRow struct{ r *sql.Row }

func (r duckRow) Scan(dest ...any) error {
	if err := r.r.Scan(dest...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNoRows
		}
		return err
	}
	return nil
}

type duckRows struct{ r *sql.Rows }

func (r duckRows) Next() bool          { return r.r.Next() }
func (r duckRows) Scan(d ...any) error { return r.r.Scan(d...) }
func (r duckRows) Close()              { r.r.Close() }
func (r duckRows) Err() error          { return r.r.Err() }

type duckTx struct{ tx *sql.Tx }

func (t duckTx) Exec(ctx context.Context, q string, a ...any) (int64, error) {
	res, err := t.tx.ExecContext(ctx, q, a...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}
func (t duckTx) Query(ctx context.Context, q string, a ...any) (Rows, error) {
	rows, err := t.tx.QueryContext(ctx, q, a...)
	if err != nil {
		return nil, err
	}
	return duckRows{rows}, nil
}
func (t duckTx) QueryRow(ctx context.Context, q string, a ...any) Row {
	return duckRow{t.tx.QueryRowContext(ctx, q, a...)}
}
func (t duckTx) Commit(ctx context.Context) error   { return t.tx.Commit() }
func (t duckTx) Rollback(ctx context.Context) error { return t.tx.Rollback() }
