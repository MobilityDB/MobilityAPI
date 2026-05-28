// MobilityAPI-go — a thin, compiled OGC API – Moving Features tier over
// MobilityDB. Every temporal computation and (de)serialization runs in the
// database (asMFJSON / tgeompointFromMFJSON); this process only routes,
// pools connections, and applies the small OGC<->MobilityDB convention
// mapping (crs name + interpolation enum). No MEOS in the application tier
// (no cgo, no PyMEOS) — the parallel-path counterpart to the PyMEOS server.
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

var pool *pgxpool.Pool

// Assessed against live MobilityDB: it rejects OGC "Stepwise" and uses "Step";
// "Linear"/"Discrete" pass through. crs in MobilityDB MF-JSON is "EPSG:<n>",
// OGC uses the URN form. These two field mappings are the entire adapter.
var ogc2mdbInterp = map[string]string{"Linear": "Linear", "Stepwise": "Step", "Discrete": "Discrete"}

func ogcifyResponse(s string) string {
	// MobilityDB MF-JSON -> OGC MovingFeaturesJSON (compact jsonb output)
	s = strings.ReplaceAll(s, `"interpolation": "Step"`, `"interpolation": "Stepwise"`)
	s = strings.ReplaceAll(s, `"interpolation":"Step"`, `"interpolation":"Stepwise"`)
	// crs name "EPSG:25832" -> "urn:ogc:def:crs:EPSG::25832"
	s = epsgName.ReplaceAllString(s, `"name":"urn:ogc:def:crs:EPSG::$1"`)
	return s
}

var epsgName = regexp.MustCompile(`"name":\s*"EPSG:(\d+)"`)
var epsgURN = regexp.MustCompile(`EPSG:+(\d+)`)

func epsgFromCRS(raw json.RawMessage, def int) int {
	if len(raw) == 0 {
		return def
	}
	if m := epsgURN.FindStringSubmatch(string(raw)); m != nil {
		if n, err := strconv.Atoi(m[1]); err == nil {
			return n
		}
	}
	return def
}

func main() {
	dsn := os.Getenv("MFAPI_DSN")
	if dsn == "" {
		dsn = "postgres:///mfapi_demo?host=/tmp&port=5432&user=esteban"
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		log.Fatal(err)
	}
	cfg.MaxConns = 16 // the connection-pool knob — the concurrency story vs a single-threaded Python server
	pool, err = pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()
	if err := pool.Ping(context.Background()); err != nil {
		log.Fatal("db ping: ", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /", landing)
	mux.HandleFunc("GET /conformance", conformance)
	mux.HandleFunc("GET /collections", listCollections)
	mux.HandleFunc("GET /collections/{cid}", getCollection)
	mux.HandleFunc("GET /collections/{cid}/items", getItems)
	mux.HandleFunc("GET /collections/{cid}/items/{fid}", getItem)
	mux.HandleFunc("POST /collections/{cid}/items", postItem)

	addr := ":8088"
	log.Printf("MobilityAPI-go listening on %s (pool max=%d) — thin Go over MobilityDB SQL", addr, cfg.MaxConns)
	srv := &http.Server{Addr: addr, Handler: mux, ReadTimeout: 30 * time.Second, WriteTimeout: 120 * time.Second}
	log.Fatal(srv.ListenAndServe())
}

func writeJSONString(w http.ResponseWriter, code int, body string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write([]byte(body))
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func httpErr(w http.ResponseWriter, code int, desc string) {
	writeJSON(w, code, map[string]any{"code": strconv.Itoa(code), "description": desc})
}

// collectionTable validates the collection id against the registry and returns
// its feature table name (whitelist — no string interpolation of user input).
func collectionTable(ctx context.Context, cid string) (string, bool) {
	var ok bool
	if err := pool.QueryRow(ctx, `SELECT true FROM collections WHERE id=$1`, cid).Scan(&ok); err != nil {
		return "", false
	}
	return cid, ok // table name == collection id in this schema (registry-validated)
}

func landing(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]any{
		"title":       "MobilityAPI-go",
		"description": "OGC API – Moving Features over MobilityDB (thin Go tier)",
		"links": []map[string]string{
			{"rel": "self", "href": "/", "type": "application/json"},
			{"rel": "conformance", "href": "/conformance"},
			{"rel": "data", "href": "/collections"},
		},
	})
}

func conformance(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]any{"conformsTo": []string{
		"http://www.opengis.net/spec/ogcapi-movingfeatures-1/1.0/conf/movingfeatures",
		"http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/core",
	}})
}

func listCollections(w http.ResponseWriter, r *http.Request) {
	var body string
	err := pool.QueryRow(r.Context(), `
		SELECT jsonb_build_object('collections',
		  coalesce(jsonb_agg(jsonb_build_object(
		    'id', id, 'title', title, 'description', description,
		    'itemType', item_type,
		    'links', jsonb_build_array(jsonb_build_object('rel','items','href','/collections/'||id||'/items'))
		  )), '[]'::jsonb))::text
		FROM collections`).Scan(&body)
	if err != nil {
		httpErr(w, 500, err.Error())
		return
	}
	writeJSONString(w, 200, body)
}

func getCollection(w http.ResponseWriter, r *http.Request) {
	cid := r.PathValue("cid")
	var body string
	err := pool.QueryRow(r.Context(), `
		SELECT jsonb_build_object('id',id,'title',title,'description',description,'itemType',item_type)::text
		FROM collections WHERE id=$1`, cid).Scan(&body)
	if err != nil {
		httpErr(w, 404, "collection not found")
		return
	}
	writeJSONString(w, 200, body)
}

// getItems — the hot read path. The entire OGC FeatureCollection is assembled
// in SQL (jsonb + asMFJSON) and streamed; Go only applies the convention map.
func getItems(w http.ResponseWriter, r *http.Request) {
	tbl, ok := collectionTable(r.Context(), r.PathValue("cid"))
	if !ok {
		httpErr(w, 404, "collection not found")
		return
	}
	limit := 100
	if l := r.URL.Query().Get("limit"); l != "" {
		if n, e := strconv.Atoi(l); e == nil && n > 0 && n <= 10000 {
			limit = n
		}
	}
	q := `SELECT jsonb_build_object(
	    'type','FeatureCollection',
	    'numberReturned', count(*),
	    'features', coalesce(jsonb_agg(jsonb_build_object(
	        'type','Feature',
	        'id', id::text,
	        'properties', jsonb_build_object('mmsi',mmsi,'name',name),
	        'temporalGeometry', asMFJSON(trip)::jsonb
	    )), '[]'::jsonb)
	  )::text
	  FROM (SELECT id,mmsi,name,trip FROM ` + pgxIdent(tbl) + ` ORDER BY id LIMIT $1) s`
	var body string
	if err := pool.QueryRow(r.Context(), q, limit).Scan(&body); err != nil {
		httpErr(w, 500, err.Error())
		return
	}
	writeJSONString(w, 200, ogcifyResponse(body))
}

func getItem(w http.ResponseWriter, r *http.Request) {
	tbl, ok := collectionTable(r.Context(), r.PathValue("cid"))
	if !ok {
		httpErr(w, 404, "collection not found")
		return
	}
	fid, err := strconv.Atoi(r.PathValue("fid"))
	if err != nil {
		httpErr(w, 400, "invalid feature id")
		return
	}
	q := `SELECT jsonb_build_object(
	    'type','Feature','id',id::text,
	    'properties', jsonb_build_object('mmsi',mmsi,'name',name),
	    'temporalGeometry', asMFJSON(trip)::jsonb
	  )::text FROM ` + pgxIdent(tbl) + ` WHERE id=$1`
	var body string
	if err := pool.QueryRow(r.Context(), q, fid).Scan(&body); err != nil {
		httpErr(w, 404, "feature not found")
		return
	}
	writeJSONString(w, 200, ogcifyResponse(body))
}

// postItem — the write path. The OGC temporalGeometry is parsed entirely by
// MobilityDB (tgeompointFromMFJSON); Go only maps the interpolation enum and
// sets the SRID from the Feature crs. No MEOS in the tier.
func postItem(w http.ResponseWriter, r *http.Request) {
	tbl, ok := collectionTable(r.Context(), r.PathValue("cid"))
	if !ok {
		httpErr(w, 404, "collection not found")
		return
	}
	var feat struct {
		ID         json.Number     `json:"id"`
		Properties map[string]any  `json:"properties"`
		CRS        json.RawMessage `json:"crs"`
		TG         map[string]any  `json:"temporalGeometry"`
	}
	if err := json.NewDecoder(r.Body).Decode(&feat); err != nil || feat.TG == nil {
		httpErr(w, 400, "invalid Feature / missing temporalGeometry")
		return
	}
	if interp, ok := feat.TG["interpolation"].(string); ok {
		if m, ok := ogc2mdbInterp[interp]; ok {
			feat.TG["interpolation"] = m
		}
	}
	tgBytes, _ := json.Marshal(feat.TG)
	srid := epsgFromCRS(feat.CRS, 25832)
	id, _ := feat.ID.Int64()
	var name string
	if feat.Properties != nil {
		if n, ok := feat.Properties["name"].(string); ok {
			name = n
		}
	}
	_, err := pool.Exec(r.Context(),
		`INSERT INTO `+pgxIdent(tbl)+`(id,mmsi,name,trip)
		 VALUES ($1, $2, $3, setSRID(tgeompointFromMFJSON($4), $5))`,
		id, nil, name, string(tgBytes), srid)
	if err != nil {
		httpErr(w, 400, "ingest failed: "+err.Error())
		return
	}
	writeJSON(w, 201, map[string]any{"message": "created", "id": strconv.FormatInt(id, 10)})
}

// pgxIdent quotes a validated identifier (the collection id is registry-checked
// before reaching here, so this is defense-in-depth, not the primary guard).
func pgxIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
