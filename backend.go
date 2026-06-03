// Backend is the engine-neutral database seam the OGC tier runs over. The tier
// holds no engine-specific type: it calls named MEOS/MobilityDB functions
// through portable SQL and reads rows through this interface, so a second MEOS
// engine (e.g. MobilityDuck over DuckDB) is added by implementing Backend, not
// by touching the handlers.
package main

import (
	"context"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrNoRows is the engine-neutral "no rows" sentinel; each backend maps its
// driver's no-rows error onto it so handlers test one value.
var ErrNoRows = errors.New("no rows in result set")

// Row, Rows and Tx mirror the minimal shapes the handlers use.
type Row interface {
	Scan(dest ...any) error
}
type Rows interface {
	Next() bool
	Scan(dest ...any) error
	Close()
	Err() error
}
type Tx interface {
	Exec(ctx context.Context, sql string, args ...any) (int64, error)
	Query(ctx context.Context, sql string, args ...any) (Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) Row
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// Backend is one MEOS-backed engine. Exec returns the affected-row count.
type Backend interface {
	QueryRow(ctx context.Context, sql string, args ...any) Row
	Query(ctx context.Context, sql string, args ...any) (Rows, error)
	Exec(ctx context.Context, sql string, args ...any) (int64, error)
	Begin(ctx context.Context) (Tx, error)
	Ping(ctx context.Context) error
	Close()
}

// openBackend selects the engine from the DSN scheme. Today every DSN is
// PostgreSQL/MobilityDB; the scheme switch is where MobilityDuck plugs in.
func openBackend(dsn string) (Backend, error) {
	if strings.HasPrefix(dsn, "duckdb:") {
		return openDuck(dsn)
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = int32(envInt("MFAPI_MAXCONNS", 16))
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		return nil, err
	}
	return &pgBackend{pool}, nil
}

// pgBackend is the PostgreSQL/MobilityDB engine over a pgx pool.
type pgBackend struct{ pool *pgxpool.Pool }

func (b *pgBackend) QueryRow(ctx context.Context, sql string, a ...any) Row {
	return pgRow{b.pool.QueryRow(ctx, sql, a...)}
}
func (b *pgBackend) Query(ctx context.Context, sql string, a ...any) (Rows, error) {
	return b.pool.Query(ctx, sql, a...)
}
func (b *pgBackend) Exec(ctx context.Context, sql string, a ...any) (int64, error) {
	ct, err := b.pool.Exec(ctx, sql, a...)
	return ct.RowsAffected(), err
}
func (b *pgBackend) Begin(ctx context.Context) (Tx, error) {
	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return pgTx{tx}, nil
}
func (b *pgBackend) Ping(ctx context.Context) error { return b.pool.Ping(ctx) }
func (b *pgBackend) Close()                         { b.pool.Close() }

// pgRow maps pgx.ErrNoRows onto the neutral ErrNoRows.
type pgRow struct{ r pgx.Row }

func (r pgRow) Scan(dest ...any) error {
	if err := r.r.Scan(dest...); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNoRows
		}
		return err
	}
	return nil
}

type pgTx struct{ tx pgx.Tx }

func (t pgTx) Exec(ctx context.Context, sql string, a ...any) (int64, error) {
	ct, err := t.tx.Exec(ctx, sql, a...)
	return ct.RowsAffected(), err
}
func (t pgTx) Query(ctx context.Context, sql string, a ...any) (Rows, error) {
	return t.tx.Query(ctx, sql, a...)
}
func (t pgTx) QueryRow(ctx context.Context, sql string, a ...any) Row {
	return pgRow{t.tx.QueryRow(ctx, sql, a...)}
}
func (t pgTx) Commit(ctx context.Context) error   { return t.tx.Commit(ctx) }
func (t pgTx) Rollback(ctx context.Context) error { return t.tx.Rollback(ctx) }
