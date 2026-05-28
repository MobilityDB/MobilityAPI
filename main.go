// MobilityAPI-go — a thin, compiled OGC API – Moving Features tier over
// MobilityDB, built for very large databases and the lakehouse direction:
//   * streaming responses (the FeatureCollection is written row-by-row from a
//     server-side cursor, so memory is bounded regardless of result size);
//   * keyset pagination (WHERE id > :after) with OGC next links — no OFFSET;
//   * index-using spatial/temporal filters (bbox, datetime) pushed to the
//     MobilityDB GiST index via the && operator;
//   * a streaming NDJSON bulk-export endpoint the lake (DuckDB / MobilityDuck /
//     Spark) can ingest directly.
// All temporal work and (de)serialization run in MobilityDB (asMFJSON,
// atTime, tgeompointFromMFJSON); the tier holds no MEOS (no cgo, no PyMEOS).
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/parquet-go/parquet-go"
)

var (
	pool         *pgxpool.Pool
	defaultLimit = envInt("MFAPI_DEFAULT_LIMIT", 100)
	maxLimit     = envInt("MFAPI_MAX_LIMIT", 10000)
	exportBatch  = envInt("MFAPI_EXPORT_LIMIT", 0)          // 0 = unbounded stream
	parquetRG    = envInt("MFAPI_PARQUET_ROWGROUP", 1024)   // rows per Parquet row group (bounds export memory)
)

// OGC <-> MobilityDB conventions (assessed against live MobilityDB):
// MobilityDB rejects "Stepwise" and uses "Step"; crs is "EPSG:<n>" vs the URN.
var ogc2mdbInterp = map[string]string{"Linear": "Linear", "Stepwise": "Step", "Discrete": "Discrete"}
var epsgName = regexp.MustCompile(`"name":\s*"EPSG:(\d+)"`)
var epsgURN = regexp.MustCompile(`EPSG:+(\d+)`)

func ogcify(s string) string {
	s = strings.ReplaceAll(s, `"interpolation": "Step"`, `"interpolation": "Stepwise"`)
	s = strings.ReplaceAll(s, `"interpolation":"Step"`, `"interpolation":"Stepwise"`)
	return epsgName.ReplaceAllString(s, `"name":"urn:ogc:def:crs:EPSG::$1"`)
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, e := strconv.Atoi(v); e == nil {
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
	cfg.MaxConns = int32(envInt("MFAPI_MAXCONNS", 16))
	pool, err = pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()
	if err := pool.Ping(context.Background()); err != nil {
		log.Fatal("db ping: ", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) { writeRaw(w, 200, `{"status":"ok"}`) })
	mux.HandleFunc("GET /", landing)
	mux.HandleFunc("GET /conformance", conformance)
	mux.HandleFunc("GET /collections", listCollections)
	mux.HandleFunc("GET /collections/{cid}", getCollection)
	mux.HandleFunc("GET /collections/{cid}/items", streamItems)
	mux.HandleFunc("GET /collections/{cid}/items/{fid}", getItem)
	mux.HandleFunc("POST /collections/{cid}/items", postItem)
	mux.HandleFunc("GET /collections/{cid}/export", export) // lakehouse bulk feed (NDJSON | Parquet)

	addr := ":" + strconv.Itoa(envInt("MFAPI_PORT", 8088))
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second}
	// streaming/export responses can be long-lived: no WriteTimeout (use ctx).
	go func() {
		log.Printf("MobilityAPI-go on %s (pool max=%d, default/max limit=%d/%d) — streaming, keyset-paged, lakehouse-ready", addr, cfg.MaxConns, defaultLimit, maxLimit)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	srv.Shutdown(ctx)
	log.Println("shut down")
}

func writeRaw(w http.ResponseWriter, code int, body string) {
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

// collectionMeta validates the collection id against the registry and returns
// the feature table name and crs (whitelist — no interpolation of user input).
func collectionMeta(ctx context.Context, cid string) (table string, srid int, ok bool) {
	err := pool.QueryRow(ctx, `SELECT id, crs FROM collections WHERE id=$1`, cid).Scan(&table, &srid)
	return table, srid, err == nil
}
func ident(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

func landing(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]any{
		"title": "MobilityAPI-go", "description": "OGC API – Moving Features over MobilityDB",
		"links": []map[string]string{
			{"rel": "self", "href": "/"}, {"rel": "conformance", "href": "/conformance"}, {"rel": "data", "href": "/collections"},
		}})
}
func conformance(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]any{"conformsTo": []string{
		"http://www.opengis.net/spec/ogcapi-movingfeatures-1/1.0/conf/movingfeatures",
		"http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/core",
	}})
}
func listCollections(w http.ResponseWriter, r *http.Request) {
	var body string
	if err := pool.QueryRow(r.Context(), `SELECT jsonb_build_object('collections',
	  coalesce(jsonb_agg(jsonb_build_object('id',id,'title',title,'description',description,'itemType',item_type,
	    'links', jsonb_build_array(
	      jsonb_build_object('rel','items','href','/collections/'||id||'/items'),
	      jsonb_build_object('rel','enclosure','href','/collections/'||id||'/export','type','application/x-ndjson')))),'[]'::jsonb))::text
	  FROM collections`).Scan(&body); err != nil {
		httpErr(w, 500, err.Error())
		return
	}
	writeRaw(w, 200, body)
}
func getCollection(w http.ResponseWriter, r *http.Request) {
	var body string
	if err := pool.QueryRow(r.Context(), `SELECT jsonb_build_object('id',id,'title',title,'description',description,'itemType',item_type)::text
	  FROM collections WHERE id=$1`, r.PathValue("cid")).Scan(&body); err != nil {
		httpErr(w, 404, "collection not found")
		return
	}
	writeRaw(w, 200, body)
}

// itemFilters builds the index-using WHERE clause and the temporalGeometry
// expression (clipped when subTrajectory=true) from the OGC query parameters.
func itemFilters(tbl string, srid int, q map[string][]string) (where string, tgExpr string, args []any, err error) {
	conds := []string{}
	add := func(c string, a ...any) { conds = append(conds, c); args = append(args, a...) }
	// keyset cursor: ?after=<id>
	if v := first(q, "after"); v != "" {
		id, e := strconv.ParseInt(v, 10, 64)
		if e != nil {
			return "", "", nil, errors.New("invalid 'after'")
		}
		add("id > $"+strconv.Itoa(len(args)+1), id)
	}
	// bbox: minx,miny,maxx,maxy in the collection CRS -> GiST && via stbox
	if v := first(q, "bbox"); v != "" {
		p := strings.Split(v, ",")
		if len(p) != 4 {
			return "", "", nil, errors.New("bbox must be minx,miny,maxx,maxy")
		}
		f := make([]any, 4)
		for i := range p {
			if f[i], err = strconv.ParseFloat(strings.TrimSpace(p[i]), 64); err != nil {
				return "", "", nil, errors.New("invalid bbox number")
			}
		}
		n := len(args)
		add("trip && stbox(ST_MakeEnvelope($"+itoa(n+1)+",$"+itoa(n+2)+",$"+itoa(n+3)+",$"+itoa(n+4)+",$"+itoa(n+5)+"))",
			f[0], f[1], f[2], f[3], srid)
	}
	if first(q, "subTrajectory") == "true" && first(q, "datetime") == "" {
		return "", "", nil, errors.New("subTrajectory requires a bounded datetime interval")
	}
	// datetime: start/end (RFC3339 interval) -> && on the time dimension
	dt := first(q, "datetime")
	if dt != "" {
		s, e, ok := splitInterval(dt)
		if !ok {
			return "", "", nil, errors.New("datetime must be start/end (RFC3339)")
		}
		n := len(args)
		span := "$" + itoa(n+1) + "::tstzspan"
		add("trip && "+span, "["+s+", "+e+"]")
		if first(q, "subTrajectory") == "true" {
			// clip each trajectory to the window; drop rows whose values fall in
			// a temporal gap (the time box overlaps but atTime is empty)
			tgExpr = "atTime(trip, " + span + ")"
			conds = append(conds, "atTime(trip, "+span+") IS NOT NULL")
		}
	}
	if tgExpr == "" {
		tgExpr = "trip"
	}
	if len(conds) > 0 {
		where = "WHERE " + strings.Join(conds, " AND ")
	}
	return where, tgExpr, args, nil
}

// streamItems writes the FeatureCollection incrementally from a server-side
// cursor: bounded memory for any result size; keyset next link for paging.
func streamItems(w http.ResponseWriter, r *http.Request) {
	tbl, srid, ok := collectionMeta(r.Context(), r.PathValue("cid"))
	if !ok {
		httpErr(w, 404, "collection not found")
		return
	}
	q := r.URL.Query()
	limit := defaultLimit
	if l := q.Get("limit"); l != "" {
		if n, e := strconv.Atoi(l); e == nil && n > 0 {
			limit = n
		}
	}
	if limit > maxLimit {
		limit = maxLimit
	}
	where, tgExpr, args, ferr := itemFilters(tbl, srid, q)
	if ferr != nil {
		httpErr(w, 400, ferr.Error())
		return
	}
	args = append(args, limit)
	sql := "SELECT id, jsonb_build_object('type','Feature','id',id::text," +
		"'properties',jsonb_build_object('mmsi',mmsi,'name',name)," +
		"'temporalGeometry', asMFJSON(" + tgExpr + ")::jsonb)::text " +
		"FROM " + ident(tbl) + " " + where +
		" ORDER BY id LIMIT $" + itoa(len(args))
	rows, err := pool.Query(r.Context(), sql, args...)
	if err != nil {
		httpErr(w, 500, err.Error())
		return
	}
	defer rows.Close()

	w.Header().Set("Content-Type", "application/json")
	bw := bufio.NewWriterSize(w, 64*1024)
	defer bw.Flush()
	bw.WriteString(`{"type":"FeatureCollection","features":[`)
	var lastID int64
	n := 0
	for rows.Next() {
		var id int64
		var feat string
		if err := rows.Scan(&id, &feat); err != nil {
			break
		}
		if n > 0 {
			bw.WriteByte(',')
		}
		bw.WriteString(ogcify(feat))
		lastID = id
		n++
		if n%256 == 0 {
			bw.Flush()
		}
	}
	bw.WriteString(`],"numberReturned":` + itoa(n))
	if n == limit { // a full page -> there may be more; emit a keyset next link
		nq := cloneQuery(q)
		nq.Set("after", strconv.FormatInt(lastID, 10))
		nq.Set("limit", itoa(limit))
		bw.WriteString(`,"links":[{"rel":"next","href":"/collections/` + r.PathValue("cid") + `/items?` + nq.Encode() + `"}]`)
	}
	bw.WriteString(`}`)
}

func getItem(w http.ResponseWriter, r *http.Request) {
	tbl, _, ok := collectionMeta(r.Context(), r.PathValue("cid"))
	if !ok {
		httpErr(w, 404, "collection not found")
		return
	}
	fid, err := strconv.Atoi(r.PathValue("fid"))
	if err != nil {
		httpErr(w, 400, "invalid feature id")
		return
	}
	var body string
	if err := pool.QueryRow(r.Context(), "SELECT jsonb_build_object('type','Feature','id',id::text,"+
		"'properties',jsonb_build_object('mmsi',mmsi,'name',name),'temporalGeometry',asMFJSON(trip)::jsonb)::text "+
		"FROM "+ident(tbl)+" WHERE id=$1", fid).Scan(&body); err != nil {
		httpErr(w, 404, "feature not found")
		return
	}
	writeRaw(w, 200, ogcify(body))
}

// export is the bulk lakehouse feed, streamed from a server-side cursor so
// memory is bounded for any size. Default is NDJSON (one Feature per line —
// DuckDB read_json_auto / Spark / pandas ingest it directly); ?format=parquet
// emits the columnar WKB + bbox/time sidecar form the lake can prune by space
// and time, and convert into the columnar temporal layout (MEOS-ARROW).
func export(w http.ResponseWriter, r *http.Request) {
	tbl, srid, ok := collectionMeta(r.Context(), r.PathValue("cid"))
	if !ok {
		httpErr(w, 404, "collection not found")
		return
	}
	where, tgExpr, args, ferr := itemFilters(tbl, srid, r.URL.Query())
	if ferr != nil {
		httpErr(w, 400, ferr.Error())
		return
	}
	tail := ""
	if exportBatch > 0 {
		args = append(args, exportBatch)
		tail = " LIMIT $" + itoa(len(args))
	}
	if r.URL.Query().Get("format") == "parquet" {
		streamParquet(w, r, tbl, tgExpr, where, tail, args)
		return
	}
	sql := "SELECT jsonb_build_object('type','Feature','id',id::text," +
		"'properties',jsonb_build_object('mmsi',mmsi,'name',name)," +
		"'temporalGeometry', asMFJSON(" + tgExpr + ")::jsonb)::text " +
		"FROM " + ident(tbl) + " " + where + " ORDER BY id" + tail
	rows, err := pool.Query(r.Context(), sql, args...)
	if err != nil {
		httpErr(w, 500, err.Error())
		return
	}
	defer rows.Close()
	w.Header().Set("Content-Type", "application/x-ndjson")
	bw := bufio.NewWriterSize(w, 64*1024)
	defer bw.Flush()
	n := 0
	for rows.Next() {
		var feat string
		if err := rows.Scan(&feat); err != nil {
			break
		}
		bw.WriteString(ogcify(feat))
		bw.WriteByte('\n')
		if n++; n%256 == 0 {
			bw.Flush()
		}
	}
}

// pqRow is the lakehouse Parquet schema: the trajectory WKB plus a bbox/time
// sidecar (xmin..tmax) so the lake prunes row groups by space and time.
type pqRow struct {
	ID   int64   `parquet:"id"`
	MMSI int64   `parquet:"mmsi"`
	Name string  `parquet:"name"`
	WKB  []byte  `parquet:"trajectory_wkb"`
	Xmin float64 `parquet:"xmin"`
	Ymin float64 `parquet:"ymin"`
	Xmax float64 `parquet:"xmax"`
	Ymax float64 `parquet:"ymax"`
	Tmin string  `parquet:"tmin"`
	Tmax string  `parquet:"tmax"`
}

// streamParquet writes Parquet from a server-side cursor, flushing a row group
// every few thousand rows so memory stays bounded and each row group carries
// its own min/max statistics for predicate pushdown. The trajectory geometry
// materialises once per row (g) so the sidecar accessors do not re-clip.
func streamParquet(w http.ResponseWriter, r *http.Request, tbl, tgExpr, where, tail string, args []any) {
	sql := "SELECT id, mmsi, name, asBinary(g)," +
		" Xmin(stbox(g)), Ymin(stbox(g)), Xmax(stbox(g)), Ymax(stbox(g))," +
		" Tmin(stbox(g))::text, Tmax(stbox(g))::text FROM (" +
		"SELECT id, coalesce(mmsi,0) AS mmsi, coalesce(name,'') AS name, " + tgExpr + " AS g" +
		" FROM " + ident(tbl) + " " + where + " ORDER BY id" + tail + ") s"
	rows, err := pool.Query(r.Context(), sql, args...)
	if err != nil {
		httpErr(w, 500, err.Error())
		return
	}
	defer rows.Close()
	w.Header().Set("Content-Type", "application/vnd.apache.parquet")
	w.Header().Set("Content-Disposition", `attachment; filename="`+tbl+`.parquet"`)
	pw := parquet.NewGenericWriter[pqRow](w)
	defer pw.Close()
	batch := make([]pqRow, 0, parquetRG)
	emit := func() bool { // one row group per batch: memory bounded by parquetRG, each group carries its own stats
		if len(batch) == 0 {
			return true
		}
		if _, e := pw.Write(batch); e != nil {
			return false
		}
		pw.Flush()
		batch = batch[:0]
		return true
	}
	for rows.Next() {
		var x pqRow
		if err := rows.Scan(&x.ID, &x.MMSI, &x.Name, &x.WKB, &x.Xmin, &x.Ymin, &x.Xmax, &x.Ymax, &x.Tmin, &x.Tmax); err != nil {
			break
		}
		batch = append(batch, x)
		if len(batch) == parquetRG && !emit() {
			break
		}
	}
	emit()
}

func postItem(w http.ResponseWriter, r *http.Request) {
	tbl, _, ok := collectionMeta(r.Context(), r.PathValue("cid"))
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
	srid := 25832
	if m := epsgURN.FindStringSubmatch(string(feat.CRS)); m != nil {
		srid, _ = strconv.Atoi(m[1])
	}
	id, _ := feat.ID.Int64()
	name, _ := feat.Properties["name"].(string)
	if _, err := pool.Exec(r.Context(),
		"INSERT INTO "+ident(tbl)+"(id,mmsi,name,trip) VALUES ($1,$2,$3, setSRID(tgeompointFromMFJSON($4), $5))",
		id, nil, name, string(tgBytes), srid); err != nil {
		httpErr(w, 400, "ingest failed: "+err.Error())
		return
	}
	writeJSON(w, 201, map[string]any{"message": "created", "id": strconv.FormatInt(id, 10)})
}

// small helpers
func itoa(n int) string { return strconv.Itoa(n) }
func first(q map[string][]string, k string) string {
	if v := q[k]; len(v) > 0 {
		return v[0]
	}
	return ""
}
func splitInterval(s string) (string, string, bool) {
	if i := strings.IndexByte(s, '/'); i > 0 {
		return s[:i], s[i+1:], true
	}
	return "", "", false
}
func cloneQuery(q map[string][]string) url.Values {
	v := url.Values{}
	for k, vs := range q {
		v[k] = append([]string(nil), vs...)
	}
	return v
}
