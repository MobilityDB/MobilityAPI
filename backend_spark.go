//go:build spark

// sparkBackend is the MobilitySpark (Apache Spark) engine over Spark Connect,
// selected by a spark:// DSN and built only under -tags spark. The same Go
// response assembly and the same canonical SQL serve it; per-engine the SQL is
// translated to the Spark idiom (dialect_spark.go): canonical function names
// remap to their MobilitySpark camelCase names and $N placeholders inline,
// since Spark Connect's Sql takes no bind parameters. Spark Connect has no
// transactions, so Begin returns a pass-through.
package main

import (
	"context"
	"reflect"
	"strings"

	"github.com/apache/spark-connect-go/v35/spark/sql"
	"github.com/apache/spark-connect-go/v35/spark/sql/types"
)

func openSpark(dsn string) (Backend, error) {
	remote := "sc://" + strings.TrimPrefix(dsn, "spark://")
	s, err := sql.NewSessionBuilder().Remote(remote).Build(context.Background())
	if err != nil {
		return nil, err
	}
	return &sparkBackend{s}, nil
}

type sparkBackend struct{ s sql.SparkSession }

func (b *sparkBackend) prep(q string, a []any) string { return inlineParams(rewriteSparkSQL(q), a) }

func (b *sparkBackend) Query(ctx context.Context, q string, a ...any) (Rows, error) {
	df, err := b.s.Sql(ctx, b.prep(q, a))
	if err != nil {
		return nil, err
	}
	rs, err := df.Collect(ctx)
	if err != nil {
		return nil, err
	}
	return &sparkRows{rows: rs, i: -1}, nil
}
func (b *sparkBackend) QueryRow(ctx context.Context, q string, a ...any) Row {
	rows, err := b.Query(ctx, q, a...)
	return sparkRow{rows: rows, err: err}
}
func (b *sparkBackend) Exec(ctx context.Context, q string, a ...any) (int64, error) {
	df, err := b.s.Sql(ctx, b.prep(q, a))
	if err != nil {
		return 0, err
	}
	_, err = df.Collect(ctx) // force execution
	return 0, err
}
func (b *sparkBackend) Begin(ctx context.Context) (Tx, error) { return sparkTx{b}, nil }
func (b *sparkBackend) Ping(ctx context.Context) error {
	_, err := b.s.Sql(ctx, "SELECT 1")
	return err
}
func (b *sparkBackend) Close() { b.s.Stop() }

type sparkRows struct {
	rows []types.Row
	i    int
}

func (r *sparkRows) Next() bool          { r.i++; return r.i < len(r.rows) }
func (r *sparkRows) Scan(d ...any) error { return scanSparkRow(r.rows[r.i], d) }
func (r *sparkRows) Close()              {}
func (r *sparkRows) Err() error          { return nil }

type sparkRow struct {
	rows Rows
	err  error
}

func (r sparkRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if !r.rows.Next() {
		return ErrNoRows
	}
	return r.rows.Scan(dest...)
}

// sparkTx is a pass-through: Spark Connect has no transactions.
type sparkTx struct{ b *sparkBackend }

func (t sparkTx) Exec(ctx context.Context, q string, a ...any) (int64, error) {
	return t.b.Exec(ctx, q, a...)
}
func (t sparkTx) Query(ctx context.Context, q string, a ...any) (Rows, error) {
	return t.b.Query(ctx, q, a...)
}
func (t sparkTx) QueryRow(ctx context.Context, q string, a ...any) Row {
	return t.b.QueryRow(ctx, q, a...)
}
func (t sparkTx) Commit(ctx context.Context) error   { return nil }
func (t sparkTx) Rollback(ctx context.Context) error { return nil }

func scanSparkRow(row types.Row, dest []any) error {
	vals := row.Values()
	for i, d := range dest {
		var v any
		if i < len(vals) {
			v = vals[i]
		}
		assignSpark(d, v)
	}
	return nil
}

// assignSpark sets the scan destination *T (or **T for nullable columns) from a
// Spark value, converting across the numeric/string widths Spark returns.
func assignSpark(dest, v any) {
	rv := reflect.ValueOf(dest)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return
	}
	elem := rv.Elem()
	if elem.Kind() == reflect.Ptr { // **T nullable
		if v == nil {
			elem.Set(reflect.Zero(elem.Type()))
			return
		}
		p := reflect.New(elem.Type().Elem())
		assignSpark(p.Interface(), v)
		elem.Set(p)
		return
	}
	if v == nil {
		return
	}
	src := reflect.ValueOf(v)
	switch elem.Kind() {
	case reflect.String:
		if src.Kind() == reflect.String {
			elem.SetString(src.String())
		}
	case reflect.Int, reflect.Int32, reflect.Int64:
		if src.CanInt() {
			elem.SetInt(src.Int())
		} else if src.CanFloat() {
			elem.SetInt(int64(src.Float()))
		}
	case reflect.Float32, reflect.Float64:
		if src.CanFloat() {
			elem.SetFloat(src.Float())
		} else if src.CanInt() {
			elem.SetFloat(float64(src.Int()))
		}
	case reflect.Slice: // []byte
		if b, ok := v.([]byte); ok {
			elem.SetBytes(b)
		} else if s, ok := v.(string); ok {
			elem.SetBytes([]byte(s))
		}
	}
}
