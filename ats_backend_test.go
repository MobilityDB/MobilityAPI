// A scripted Backend, so an abstract test that validates a RESPONSE STRUCTURE
// can run with no database.
//
// Annex A splits into two kinds of test, and only one of them needs data. An
// operation test issues a request and checks a status code; a `-success` test
// asks whether the returned DOCUMENT carries the required properties. The
// second kind needs a response, not a populated backend, and the tier already
// has the seam that supplies one: every handler reads through the Backend
// interface, so scripting that interface exercises the real handler, the real
// routing and the real serialisation, with the database replaced.
//
// Answers are matched on a SQL fragment rather than on the whole statement,
// because a handler composes its SQL from the collection's table name and the
// test is about the response the handler builds, never about the text it sends.
package main

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// fakeAnswer is the reply to any statement containing `match`.
type fakeAnswer struct {
	match string
	rows  [][]any
}

type fakeBackend struct{ answers []fakeAnswer }

func (f *fakeBackend) reply(sql string) [][]any {
	for _, a := range f.answers {
		if strings.Contains(sql, a.match) {
			return a.rows
		}
	}
	return nil
}

func (f *fakeBackend) QueryRow(_ context.Context, sql string, _ ...any) Row {
	if r := f.reply(sql); len(r) > 0 {
		return fakeRow{r[0]}
	}
	return fakeRow{nil}
}

func (f *fakeBackend) Query(_ context.Context, sql string, _ ...any) (Rows, error) {
	return &fakeRows{rows: f.reply(sql), i: -1}, nil
}

func (f *fakeBackend) Exec(context.Context, string, ...any) (int64, error) { return 1, nil }
func (f *fakeBackend) Begin(context.Context) (Tx, error)                   { return nil, errors.New("no tx") }
func (f *fakeBackend) Ping(context.Context) error                          { return nil }
func (f *fakeBackend) Close()                                              {}

type fakeRow struct{ vals []any }

func (r fakeRow) Scan(dest ...any) error {
	if r.vals == nil {
		return ErrNoRows
	}
	return fakeScan(r.vals, dest)
}

type fakeRows struct {
	rows [][]any
	i    int
}

func (r *fakeRows) Next() bool { r.i++; return r.i < len(r.rows) }
func (r *fakeRows) Scan(dest ...any) error {
	if r.i < 0 || r.i >= len(r.rows) {
		return ErrNoRows
	}
	return fakeScan(r.rows[r.i], dest)
}
func (r *fakeRows) Close()     {}
func (r *fakeRows) Err() error { return nil }

// fakeScan assigns a scripted value into whatever the handler passed. The
// handlers scan nullable columns into **T, so a nil value has to clear the
// pointer rather than fail: that is what the driver does and what the response
// shape depends on.
func fakeScan(vals []any, dest []any) error {
	if len(dest) > len(vals) {
		return fmt.Errorf("scan wants %d values, the answer holds %d", len(dest), len(vals))
	}
	for i, d := range dest {
		rv := reflect.ValueOf(d)
		if rv.Kind() != reflect.Ptr || rv.IsNil() {
			return fmt.Errorf("scan destination %d is not a pointer", i)
		}
		el := rv.Elem()
		v := vals[i]
		if v == nil {
			el.Set(reflect.Zero(el.Type()))
			continue
		}
		vv := reflect.ValueOf(v)
		switch {
		case vv.Type().AssignableTo(el.Type()):
			el.Set(vv)
		case vv.Type().ConvertibleTo(el.Type()):
			el.Set(vv.Convert(el.Type()))
		case el.Kind() == reflect.Ptr && vv.Type().AssignableTo(el.Type().Elem()):
			p := reflect.New(el.Type().Elem())
			p.Elem().Set(vv)
			el.Set(p)
		case el.Kind() == reflect.Ptr && vv.Type().ConvertibleTo(el.Type().Elem()):
			p := reflect.New(el.Type().Elem())
			p.Elem().Set(vv.Convert(el.Type().Elem()))
			el.Set(p)
		default:
			return fmt.Errorf("cannot assign %T to %s", v, el.Type())
		}
	}
	return nil
}

// withBackend installs a scripted backend for one test and restores the
// previous one, so a package-level handler dependency stays a test detail.
func withBackend(f *fakeBackend, body func()) {
	prev := db
	db = f
	defer func() { db = prev }()
	body()
}
