package main

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"testing"

	"github.com/andybalholm/brotli"
	"github.com/klauspost/compress/zstd"
	"github.com/parquet-go/parquet-go"
)

// decompress accepts every Content-Encoding the bulk endpoint advertises.
func TestDecompressRoundTrip(t *testing.T) {
	payload := []byte(`{"type":"FeatureCollection","features":[]}`)
	gzbuf := &bytes.Buffer{}
	gw := gzip.NewWriter(gzbuf)
	gw.Write(payload)
	gw.Close()
	zlbuf := &bytes.Buffer{}
	zw := zlib.NewWriter(zlbuf)
	zw.Write(payload)
	zw.Close()
	rawbuf := &bytes.Buffer{}
	fw, _ := flate.NewWriter(rawbuf, flate.DefaultCompression)
	fw.Write(payload)
	fw.Close()
	brbuf := &bytes.Buffer{}
	bw := brotli.NewWriter(brbuf)
	bw.Write(payload)
	bw.Close()
	zsbuf := &bytes.Buffer{}
	sw, _ := zstd.NewWriter(zsbuf)
	sw.Write(payload)
	sw.Close()

	cases := map[string][]byte{
		"":         payload,
		"identity": payload,
		"gzip":     gzbuf.Bytes(),
		"deflate":  zlbuf.Bytes(), // zlib-wrapped deflate
		"br":       brbuf.Bytes(),
		"zstd":     zsbuf.Bytes(),
	}
	for enc, body := range cases {
		got, err := decompress(body, enc)
		if err != nil {
			t.Fatalf("decompress(%q) error: %v", enc, err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("decompress(%q) = %q, want %q", enc, got, payload)
		}
	}
	// raw (headerless) deflate is accepted via the zlib->flate fallback
	if got, err := decompress(rawbuf.Bytes(), "deflate"); err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("raw deflate fallback failed: %v / %q", err, got)
	}
	if _, err := decompress(payload, "lzma"); err == nil {
		t.Fatal("unsupported Content-Encoding should error")
	}
}

func TestParseGeoJSONPoints(t *testing.T) {
	body := []byte(`{"type":"FeatureCollection","features":[
	  {"type":"Feature","id":"bus_42","geometry":{"type":"Point","coordinates":[4.3517,50.8466]},"properties":{"datetime":"2026-02-26T10:00:00Z"}},
	  {"type":"Feature","geometry":{"type":"Point","coordinates":[4.349,50.8501]},"properties":{"id":"bus_57","time":"2026-02-26T10:00:00Z"}}
	]}`)
	obs, err := parseGeoJSONPoints(body)
	if err != nil {
		t.Fatal(err)
	}
	if len(obs) != 2 {
		t.Fatalf("got %d observations, want 2", len(obs))
	}
	if obs[0].id != "bus_42" || obs[0].x != 4.3517 || obs[0].t != "2026-02-26T10:00:00Z" {
		t.Fatalf("first observation parsed wrong: %+v", obs[0])
	}
	if obs[1].id != "bus_57" { // id and time pulled from properties
		t.Fatalf("second observation id from properties failed: %+v", obs[1])
	}
	if _, err := parseGeoJSONPoints([]byte(`{"type":"Feature"}`)); err == nil {
		t.Fatal("non-FeatureCollection should error")
	}
}

func TestParseGeoParquet(t *testing.T) {
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[bulkPqRow](&buf)
	w.Write([]bulkPqRow{
		{Geometry: []byte{1, 2, 3}, ID: "bus_42", TS: "2026-02-26T10:00:00Z"},
		{Geometry: []byte{4, 5, 6}, ID: "bus_57", TS: "2026-02-26T10:01:00Z"},
	})
	w.Close()
	obs, err := parseGeoParquet(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if len(obs) != 2 || obs[0].id != "bus_42" || obs[1].t != "2026-02-26T10:01:00Z" {
		t.Fatalf("parsed parquet wrong: %+v", obs)
	}
	if obs[0].wkb == nil {
		t.Fatal("parquet observation should carry WKB")
	}
}
