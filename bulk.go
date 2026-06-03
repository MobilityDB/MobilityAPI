// Bulk ingestion of a real-time fleet feed for the OGC API – Moving Features
// tier (extension, not in conformsTo):
//
//	POST /collections/{cid}/bulk
//
// The body is a batch of (vehicleId, position, time) observations — every minute
// a city posts one point per vehicle — and each observation is appended as one
// instant to the matching moving feature's tgeompoint trajectory, creating the
// feature on first sight. The batch may be encoded as GeoJSON (a FeatureCollection
// of Point features) or GeoParquet (one row per observation), and may be
// compressed via Content-Encoding (gzip, deflate, br, zstd). The whole batch
// commits atomically. As everywhere in this tier, the geometry and temporal work
// run inside MobilityDB (ST_MakePoint / ST_GeomFromWKB, tgeompoint, appendInstant);
// the tier only decodes the wire format.
package main

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/andybalholm/brotli"
	"github.com/klauspost/compress/zstd"
	"github.com/parquet-go/parquet-go"
)

// bulkMaxBytes bounds the (decompressed) request body the tier will buffer.
var bulkMaxBytes = int64(envInt("MFAPI_BULK_MAXBYTES", 64<<20))

// observation is one (id, position, time) sample. GeoJSON sets x/y; GeoParquet
// carries the point as WKB, decoded by PostGIS rather than the tier.
type observation struct {
	id  string
	x   float64
	y   float64
	wkb []byte
	t   string
}

// bulkPqRow is the GeoParquet ingest shape: one row per observation, symmetric to
// the WKB-plus-sidecar export. ts is an ISO-8601 string so any producer's clock
// representation survives the round trip without a logical-type negotiation.
type bulkPqRow struct {
	Geometry []byte `parquet:"geometry"`
	ID       string `parquet:"id"`
	TS       string `parquet:"ts"`
}

func bulkIngest(w http.ResponseWriter, r *http.Request) {
	tbl, srid, ok := collectionMeta(r.Context(), r.PathValue("cid"))
	if !ok {
		httpErr(w, 404, "collection not found")
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, bulkMaxBytes))
	if err != nil {
		httpErr(w, 413, "request body too large or unreadable: "+err.Error())
		return
	}
	body, err = decompress(body, r.Header.Get("Content-Encoding"))
	if err != nil {
		httpErr(w, 415, err.Error())
		return
	}

	ct := strings.ToLower(r.Header.Get("Content-Type"))
	var obs []observation
	switch {
	case strings.Contains(ct, "parquet"):
		obs, err = parseGeoParquet(body)
	default:
		obs, err = parseGeoJSONPoints(body)
	}
	if err != nil {
		httpErr(w, 400, err.Error())
		return
	}

	created, extended, err := appendBatch(r.Context(), tbl, srid, obs)
	if err != nil {
		if isOrderingError(err) {
			httpErr(w, 409, "an observation is not strictly after the feature's last instant: "+err.Error())
			return
		}
		httpErr(w, 400, "ingest failed: "+err.Error())
		return
	}
	writeJSON(w, 201, map[string]any{
		"observations":     len(obs),
		"featuresCreated":  created,
		"featuresExtended": extended,
	})
}

// appendBatch appends every observation as one instant, in one transaction so the
// whole batch is atomic. A feature is created on first sight and extended with
// appendInstant afterwards; only the id and trip columns are touched, so the
// collection's feature table may carry any other columns.
func appendBatch(ctx context.Context, tbl string, srid int, obs []observation) (created, extended int, err error) {
	tx, err := db.Begin(ctx)
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback(ctx)

	for _, o := range obs {
		var inst string
		var args []any
		if o.wkb != nil {
			inst = "tgeompoint(ST_SetSRID(ST_GeomFromWKB($1),$2),CAST($3 AS timestamptz))"
			args = []any{o.wkb, srid, o.t}
		} else {
			inst = "tgeompoint(ST_SetSRID(ST_MakePoint($1,$2),$3),CAST($4 AS timestamptz))"
			args = []any{o.x, o.y, srid, o.t}
		}
		idP := "$" + strconv.Itoa(len(args)+1)
		tag, e := tx.Exec(ctx,
			"UPDATE "+ident(tbl)+" SET trip = appendInstant(trip, "+inst+") WHERE id="+idP,
			append(args, o.id)...)
		if e != nil {
			return 0, 0, e
		}
		if tag > 0 {
			extended++
			continue
		}
		if _, e := tx.Exec(ctx,
			"INSERT INTO "+ident(tbl)+"(id, trip) VALUES ("+idP+", "+inst+")",
			append(args, o.id)...); e != nil {
			return 0, 0, e
		}
		created++
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, 0, err
	}
	return created, extended, nil
}

// parseGeoJSONPoints reads a FeatureCollection of Point features, each carrying a
// vehicle id and a timestamp, into observations.
func parseGeoJSONPoints(body []byte) ([]observation, error) {
	var fc struct {
		Type     string `json:"type"`
		Features []struct {
			ID       json.RawMessage `json:"id"`
			Geometry struct {
				Type        string    `json:"type"`
				Coordinates []float64 `json:"coordinates"`
			} `json:"geometry"`
			Properties map[string]any  `json:"properties"`
			When       json.RawMessage `json:"when"`
		} `json:"features"`
	}
	if err := json.Unmarshal(body, &fc); err != nil {
		return nil, errors.New("invalid GeoJSON: " + err.Error())
	}
	if fc.Type != "FeatureCollection" {
		return nil, errors.New("bulk GeoJSON ingest expects a FeatureCollection")
	}
	obs := make([]observation, 0, len(fc.Features))
	for _, f := range fc.Features {
		if f.Geometry.Type != "Point" {
			return nil, errors.New("bulk ingest expects Point geometries")
		}
		if len(f.Geometry.Coordinates) < 2 {
			return nil, errors.New("a Point needs [x, y] coordinates")
		}
		id := jsonScalar(f.ID)
		if id == "" {
			id = scalar(f.Properties["id"])
		}
		if id == "" {
			return nil, errors.New("each feature needs an id (the vehicle identifier)")
		}
		t := scalar(f.Properties["datetime"])
		for _, k := range []string{"time", "timestamp", "t"} {
			if t == "" {
				t = scalar(f.Properties[k])
			}
		}
		if t == "" {
			t = jsonScalar(f.When)
		}
		if t == "" {
			return nil, errors.New("each feature needs a timestamp (properties.datetime)")
		}
		obs = append(obs, observation{id: id, x: f.Geometry.Coordinates[0], y: f.Geometry.Coordinates[1], t: t})
	}
	return obs, nil
}

// parseGeoParquet reads one observation per row (geometry WKB point, id, ts).
func parseGeoParquet(body []byte) ([]observation, error) {
	f, err := parquet.OpenFile(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		return nil, errors.New("invalid GeoParquet: " + err.Error())
	}
	pr := parquet.NewGenericReader[bulkPqRow](f)
	defer pr.Close()
	obs := make([]observation, 0, pr.NumRows())
	buf := make([]bulkPqRow, 512)
	for {
		n, e := pr.Read(buf)
		for _, row := range buf[:n] {
			if len(row.Geometry) == 0 {
				return nil, errors.New("GeoParquet row is missing the geometry column")
			}
			if row.ID == "" || row.TS == "" {
				return nil, errors.New("GeoParquet row is missing the id or ts column")
			}
			obs = append(obs, observation{id: row.ID, wkb: row.Geometry, t: row.TS})
		}
		if e == io.EOF {
			break
		}
		if e != nil {
			return nil, errors.New("GeoParquet read failed: " + e.Error())
		}
	}
	return obs, nil
}

// decompress transparently decodes a request body by its Content-Encoding. gzip,
// deflate, br and zstd are all supported; deflate accepts both the zlib-wrapped
// and raw stream forms seen in the wild.
func decompress(body []byte, encoding string) ([]byte, error) {
	switch strings.ToLower(strings.TrimSpace(encoding)) {
	case "", "identity":
		return body, nil
	case "gzip", "x-gzip":
		zr, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, errors.New("invalid gzip body: " + err.Error())
		}
		return io.ReadAll(zr)
	case "deflate":
		if zr, err := zlib.NewReader(bytes.NewReader(body)); err == nil {
			return io.ReadAll(zr)
		}
		return io.ReadAll(flate.NewReader(bytes.NewReader(body)))
	case "br":
		return io.ReadAll(brotli.NewReader(bytes.NewReader(body)))
	case "zstd":
		zr, err := zstd.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, errors.New("invalid zstd body: " + err.Error())
		}
		defer zr.Close()
		return io.ReadAll(zr)
	default:
		return nil, errors.New("unsupported Content-Encoding: " + encoding)
	}
}

// isOrderingError recognizes MobilityDB's rejection of an instant that is not
// strictly after the trajectory's last instant.
func isOrderingError(err error) bool {
	m := strings.ToLower(err.Error())
	return strings.Contains(m, "increasing") || strings.Contains(m, "overlap") ||
		strings.Contains(m, "ordered") || (strings.Contains(m, "must be") && strings.Contains(m, "after"))
}

// jsonScalar renders a JSON id/timestamp token (string or number) as plain text.
func jsonScalar(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	return strings.Trim(string(raw), `"`)
}

// scalar renders a decoded JSON value (string or number) as plain text.
func scalar(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case json.Number:
		return x.String()
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case nil:
		return ""
	default:
		b, _ := json.Marshal(x)
		return strings.Trim(string(b), `"`)
	}
}
