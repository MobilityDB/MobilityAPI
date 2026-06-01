// MobilityAPI-go — a thin, compiled OGC API – Moving Features tier over
// MobilityDB, built for very large databases and the lakehouse direction:
//   - streaming responses (the FeatureCollection is written row-by-row from a
//     server-side cursor, so memory is bounded regardless of result size);
//   - keyset pagination (WHERE id > :after) with OGC next links — no OFFSET;
//   - index-using spatial/temporal filters (bbox, datetime) pushed to the
//     MobilityDB GiST index via the && operator;
//   - a streaming NDJSON bulk-export endpoint the lake (DuckDB / MobilityDuck /
//     Spark) can ingest directly.
//
// All temporal work and (de)serialization run in MobilityDB (asMFJSON,
// atTime, tgeompointFromMFJSON); the tier holds no MEOS (no cgo, no PyMEOS).
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/parquet-go/parquet-go"
)

var (
	pool         *pgxpool.Pool
	defaultLimit = envInt("MFAPI_DEFAULT_LIMIT", 100)
	maxLimit     = envInt("MFAPI_MAX_LIMIT", 10000)
	exportBatch  = envInt("MFAPI_EXPORT_LIMIT", 0)        // 0 = unbounded stream
	parquetRG    = envInt("MFAPI_PARQUET_ROWGROUP", 1024) // rows per Parquet row group (bounds export memory)
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
	mux.HandleFunc("GET /api", apiDoc)
	mux.HandleFunc("GET /conformance", conformance)
	mux.HandleFunc("GET /collections", listCollections)
	mux.HandleFunc("POST /collections", postCollection)
	mux.HandleFunc("GET /collections/{cid}", getCollection)
	mux.HandleFunc("PUT /collections/{cid}", putCollection)
	mux.HandleFunc("DELETE /collections/{cid}", deleteCollection)
	mux.HandleFunc("GET /collections/{cid}/items", streamItems)
	mux.HandleFunc("GET /collections/{cid}/items/{fid}", getItem)
	mux.HandleFunc("POST /collections/{cid}/items", postItem)
	mux.HandleFunc("GET /collections/{cid}/export", export) // lakehouse bulk feed (NDJSON | Parquet)
	// OGC API – Moving Features sub-resources of a moving feature:
	mux.HandleFunc("GET /collections/{cid}/items/{fid}/tgsequence", tgSequence)
	mux.HandleFunc("GET /collections/{cid}/items/{fid}/tgsequence/{tgid}/{qtype}", tgSequenceQuery)
	mux.HandleFunc("GET /collections/{cid}/items/{fid}/tproperties", listTProperties)
	mux.HandleFunc("GET /collections/{cid}/items/{fid}/tproperties/{pname}", getTProperty)
	// writes: replace / delete a feature, append a temporally-disjoint sub-trajectory
	mux.HandleFunc("PUT /collections/{cid}/items/{fid}", putItem)
	mux.HandleFunc("DELETE /collections/{cid}/items/{fid}", deleteItem)
	mux.HandleFunc("POST /collections/{cid}/items/{fid}/tgsequence", postTgSequence)
	// derived properties are computed from the geometry, not independently stored
	mux.HandleFunc("POST /collections/{cid}/items/{fid}/tproperties", derivedReadOnly)
	mux.HandleFunc("POST /collections/{cid}/items/{fid}/tproperties/{pname}", derivedReadOnly)
	mux.HandleFunc("DELETE /collections/{cid}/items/{fid}/tproperties/{pname}", derivedReadOnly)
	mux.HandleFunc("DELETE /collections/{cid}/items/{fid}/tgsequence/{tgid}", deleteTgSequence)

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

// collectionGeneric reports whether a collection's feature table carries a
// generic `properties jsonb` column (collections created through POST
// /collections) rather than the typed ships columns (mmsi, name).
func collectionGeneric(ctx context.Context, table string) bool {
	var g bool
	pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM information_schema.columns
	  WHERE table_schema='public' AND table_name=$1 AND column_name='properties')`, table).Scan(&g)
	return g
}

// propsExpr is the SQL producing a feature's 'properties' JSON: the stored
// JSONB for generic collections, the typed (mmsi, name) object for ships.
func propsExpr(generic bool) string {
	if generic {
		return "coalesce(properties,'{}'::jsonb)"
	}
	return "jsonb_build_object('mmsi',mmsi,'name',name)"
}

// featCols lists the non-geometry feature columns carried through the inner
// selects so propsExpr can read them.
func featCols(generic bool) string {
	if generic {
		return "id, properties"
	}
	return "id, mmsi, name"
}

func ident(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

// validID guards identifiers interpolated into CREATE/DROP TABLE (which cannot
// be parameterised): a collection id must be a plain SQL identifier.
var validID = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

// featureObj builds the OGC MovingFeature JSON for a row exposing columns id,
// mmsi, name plus a geometry g and its STBOX b. sp and cp are the SQL parameter
// placeholders for the collection SRID and id. crs is emitted in EPSG:n form and
// rewritten to the OGC URN by ogcify; bbox/time come from the inline STBOX.
func featureObj(g, b, sp, cp, pe string, withGeom bool) string {
	s := "jsonb_build_object('type','Feature','id',id::text," +
		"'properties'," + pe + "," +
		"'crs',jsonb_build_object('type','Name','properties',jsonb_build_object('name','EPSG:'||" + sp + "::text))," +
		"'trs',jsonb_build_object('type','Link','properties',jsonb_build_object('type','ogcdef','href','http://www.opengis.net/def/uom/ISO-8601/0/Gregorian'))," +
		"'bbox',jsonb_build_array(Xmin(" + b + "),Ymin(" + b + "),Xmax(" + b + "),Ymax(" + b + "))," +
		"'time',jsonb_build_array(Tmin(" + b + ")::text,Tmax(" + b + ")::text)," +
		"'temporalGeometry',asMFJSON(" + g + ")::jsonb,"
	if withGeom {
		s += "'geometry',ST_AsGeoJSON(trajectory(" + g + "))::jsonb,"
	}
	return s + "'links',jsonb_build_array(jsonb_build_object('rel','self','href','/collections/'||" + cp + "||'/items/'||id)))"
}

func landing(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]any{
		"title": "MobilityAPI-go", "description": "OGC API – Moving Features over MobilityDB",
		"links": []map[string]string{
			{"rel": "self", "href": "/", "type": "application/json"},
			{"rel": "service-desc", "href": "/api", "type": "application/vnd.oai.openapi+json;version=3.0"},
			{"rel": "conformance", "href": "/conformance", "type": "application/json"},
			{"rel": "data", "href": "/collections", "type": "application/json"},
		}})
}
func conformance(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]any{"conformsTo": []string{
		"http://www.opengis.net/spec/ogcapi-movingfeatures-1/1.0/conf/common",
		"http://www.opengis.net/spec/ogcapi-movingfeatures-1/1.0/conf/mf-collection",
		"http://www.opengis.net/spec/ogcapi-movingfeatures-1/1.0/conf/movingfeatures",
		"http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/core",
		"http://www.opengis.net/spec/ogcapi-common-2/1.0/conf/collections",
	}})
}
func listCollections(w http.ResponseWriter, r *http.Request) {
	var body string
	if err := pool.QueryRow(r.Context(), `SELECT jsonb_build_object('collections',
	  coalesce(jsonb_agg(jsonb_build_object('id',id,'title',title,'description',description,'itemType',item_type,
	    'crs', jsonb_build_array('http://www.opengis.net/def/crs/EPSG/0/'||crs),
	    'links', jsonb_build_array(
	      jsonb_build_object('rel','self','href','/collections/'||id),
	      jsonb_build_object('rel','items','href','/collections/'||id||'/items'),
	      jsonb_build_object('rel','enclosure','href','/collections/'||id||'/export','type','application/x-ndjson')))),'[]'::jsonb),
	  'links', jsonb_build_array(jsonb_build_object('rel','self','href','/collections')))::text
	  FROM collections`).Scan(&body); err != nil {
		httpErr(w, 500, err.Error())
		return
	}
	writeRaw(w, 200, body)
}
func getCollection(w http.ResponseWriter, r *http.Request) {
	cid := r.PathValue("cid")
	tbl, srid, ok := collectionMeta(r.Context(), cid)
	if !ok {
		httpErr(w, 404, "collection not found")
		return
	}
	var title, desc, itemType string
	if err := pool.QueryRow(r.Context(), `SELECT title,description,item_type FROM collections WHERE id=$1`, cid).
		Scan(&title, &desc, &itemType); err != nil {
		httpErr(w, 404, "collection not found")
		return
	}
	crsURI := "http://www.opengis.net/def/crs/EPSG/0/" + itoa(srid)
	col := map[string]any{
		"id": cid, "title": title, "description": desc, "itemType": itemType,
		"crs": []string{crsURI},
		"links": []map[string]string{
			{"rel": "self", "href": "/collections/" + cid},
			{"rel": "items", "href": "/collections/" + cid + "/items"},
			{"rel": "enclosure", "href": "/collections/" + cid + "/export", "type": "application/x-ndjson"},
		},
	}
	// extent: spatial bbox and temporal interval from the collection's STBOX
	var xmin, ymin, xmax, ymax *float64
	var tmin, tmax *string
	if err := pool.QueryRow(r.Context(), "SELECT Xmin(e),Ymin(e),Xmax(e),Ymax(e),Tmin(e)::text,Tmax(e)::text "+
		"FROM (SELECT extent(trip) e FROM "+ident(tbl)+") s").Scan(&xmin, &ymin, &xmax, &ymax, &tmin, &tmax); err == nil &&
		xmin != nil && tmin != nil {
		col["extent"] = map[string]any{
			"spatial":  map[string]any{"bbox": [][]float64{{*xmin, *ymin, *xmax, *ymax}}, "crs": crsURI},
			"temporal": map[string]any{"interval": [][]string{{*tmin, *tmax}}, "trs": "http://www.opengis.net/def/uom/ISO-8601/0/Gregorian"},
		}
	}
	writeJSON(w, 200, col)
}

// epsgURI matches the EPSG code in an OGC CRS URI (.../def/crs/EPSG/0/<code>).
var epsgURI = regexp.MustCompile(`EPSG/\d+/(\d+)`)

// crsCode extracts an EPSG code from a collection body's crs field (a URI, a
// URI array, or a bare integer); it defaults to 4326 (CRS84) when absent.
func crsCode(raw json.RawMessage) int {
	s := string(raw)
	if m := epsgURI.FindStringSubmatch(s); m != nil {
		n, _ := strconv.Atoi(m[1])
		return n
	}
	if m := epsgURN.FindStringSubmatch(s); m != nil {
		n, _ := strconv.Atoi(m[1])
		return n
	}
	if n, err := strconv.Atoi(strings.Trim(s, " \t\n\"")); err == nil {
		return n
	}
	return 4326
}

// postCollection registers a new collection: it creates the feature table with
// the generic (id, properties, trip) schema and a GiST index, then records the
// collection metadata in the registry the tier reads.
func postCollection(w http.ResponseWriter, r *http.Request) {
	var c struct {
		ID          string          `json:"id"`
		Title       string          `json:"title"`
		Description string          `json:"description"`
		ItemType    string          `json:"itemType"`
		CRS         json.RawMessage `json:"crs"`
	}
	if err := json.NewDecoder(r.Body).Decode(&c); err != nil || c.ID == "" {
		httpErr(w, 400, "invalid Collection / missing id")
		return
	}
	if !validID.MatchString(c.ID) {
		httpErr(w, 400, "collection id must match [a-z_][a-z0-9_]* (a plain SQL identifier)")
		return
	}
	if _, _, exists := collectionMeta(r.Context(), c.ID); exists {
		httpErr(w, 409, "collection already exists")
		return
	}
	if c.ItemType == "" {
		c.ItemType = "movingfeature"
	}
	srid := crsCode(c.CRS)
	tx, err := pool.Begin(r.Context())
	if err != nil {
		httpErr(w, 500, err.Error())
		return
	}
	defer tx.Rollback(r.Context())
	if _, err := tx.Exec(r.Context(),
		`CREATE TABLE IF NOT EXISTS collections (id text PRIMARY KEY, title text,
		   description text, item_type text, crs integer)`); err != nil {
		httpErr(w, 500, err.Error())
		return
	}
	if _, err := tx.Exec(r.Context(),
		"CREATE TABLE "+ident(c.ID)+" (id integer PRIMARY KEY, properties jsonb, trip tgeompoint)"); err != nil {
		httpErr(w, 400, "create collection failed: "+err.Error())
		return
	}
	if _, err := tx.Exec(r.Context(),
		"CREATE INDEX ON "+ident(c.ID)+" USING gist (trip)"); err != nil {
		httpErr(w, 500, err.Error())
		return
	}
	if _, err := tx.Exec(r.Context(),
		`INSERT INTO collections (id,title,description,item_type,crs) VALUES ($1,$2,$3,$4,$5)`,
		c.ID, c.Title, c.Description, c.ItemType, srid); err != nil {
		httpErr(w, 400, "register collection failed: "+err.Error())
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		httpErr(w, 500, err.Error())
		return
	}
	w.Header().Set("Location", "/collections/"+c.ID)
	writeJSON(w, 201, map[string]any{"message": "created", "id": c.ID})
}

// putCollection replaces a collection's metadata (title, description, crs).
func putCollection(w http.ResponseWriter, r *http.Request) {
	cid := r.PathValue("cid")
	var c struct {
		Title       string          `json:"title"`
		Description string          `json:"description"`
		CRS         json.RawMessage `json:"crs"`
	}
	if err := json.NewDecoder(r.Body).Decode(&c); err != nil {
		httpErr(w, 400, "invalid Collection body")
		return
	}
	ct, err := pool.Exec(r.Context(),
		`UPDATE collections SET title=$2, description=$3, crs=$4 WHERE id=$1`,
		cid, c.Title, c.Description, crsCode(c.CRS))
	if err != nil {
		httpErr(w, 400, "update failed: "+err.Error())
		return
	}
	if ct.RowsAffected() == 0 {
		httpErr(w, 404, "collection not found")
		return
	}
	writeJSON(w, 200, map[string]any{"message": "replaced", "id": cid})
}

// deleteCollection removes a registered collection: it drops the feature table
// and deletes the registry row in one transaction.
func deleteCollection(w http.ResponseWriter, r *http.Request) {
	cid := r.PathValue("cid")
	tbl, _, ok := collectionMeta(r.Context(), cid)
	if !ok {
		httpErr(w, 404, "collection not found")
		return
	}
	tx, err := pool.Begin(r.Context())
	if err != nil {
		httpErr(w, 500, err.Error())
		return
	}
	defer tx.Rollback(r.Context())
	if _, err := tx.Exec(r.Context(), "DROP TABLE IF EXISTS "+ident(tbl)); err != nil {
		httpErr(w, 500, err.Error())
		return
	}
	if _, err := tx.Exec(r.Context(), "DELETE FROM collections WHERE id=$1", cid); err != nil {
		httpErr(w, 500, err.Error())
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		httpErr(w, 500, err.Error())
		return
	}
	w.WriteHeader(204)
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
	// OGC uses lowercase "subtrajectory"; accept the camelCase form too.
	sub := first(q, "subtrajectory")
	if sub == "" {
		sub = first(q, "subTrajectory")
	}
	if sub == "true" && first(q, "datetime") == "" {
		return "", "", nil, errors.New("subtrajectory requires a bounded datetime interval")
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
		if sub == "true" {
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
	lp := "$" + itoa(len(args))
	args = append(args, itoa(srid))
	sp := "$" + itoa(len(args))
	args = append(args, r.PathValue("cid"))
	cp := "$" + itoa(len(args))
	generic := collectionGeneric(r.Context(), tbl)
	fc, pe := featCols(generic), propsExpr(generic)
	sql := "SELECT id, " + featureObj("g", "b", sp, cp, pe, false) + "::text FROM (" +
		"SELECT " + fc + ", g, stbox(g) AS b FROM (" +
		"SELECT " + fc + ", " + tgExpr + " AS g FROM " + ident(tbl) + " " + where +
		" ORDER BY id LIMIT " + lp + ") i) s ORDER BY id"
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
	tbl, srid, ok := collectionMeta(r.Context(), r.PathValue("cid"))
	if !ok {
		httpErr(w, 404, "collection not found")
		return
	}
	fid, err := strconv.Atoi(r.PathValue("fid"))
	if err != nil {
		httpErr(w, 400, "invalid feature id")
		return
	}
	generic := collectionGeneric(r.Context(), tbl)
	fc, pe := featCols(generic), propsExpr(generic)
	sql := "SELECT " + featureObj("g", "b", "$2", "$3", pe, true) + "::text FROM (" +
		"SELECT " + fc + ", g, stbox(g) AS b FROM (" +
		"SELECT " + fc + ", trip AS g FROM " + ident(tbl) + " WHERE id=$1) i) s"
	var body string
	if err := pool.QueryRow(r.Context(), sql, fid, itoa(srid), r.PathValue("cid")).Scan(&body); err != nil {
		httpErr(w, 404, "feature not found")
		return
	}
	writeRaw(w, 200, ogcify(body))
}

// propSpec is a temporal property derived from the trajectory by an exact
// MobilityDB function (no resampling). speed/azimuth are piecewise-constant
// (Step) because the position is linearly interpolated between observations;
// cumulativeLength accumulates per segment (Linear). The handlers report each
// function's true interpolation — they do not coerce it.
type propSpec struct{ expr, uom, desc string }

var tProps = map[string]propSpec{
	"velocity": {"speed(trip)", "m/s", "Speed over ground (velocity magnitude), a piecewise-constant function of the trajectory."},
	"speed":    {"speed(trip)", "m/s", "Speed over ground, a piecewise-constant function of the trajectory."},
	"distance": {"cumulativeLength(trip)", "m", "Cumulative distance travelled along the trajectory."},
	"heading":  {"azimuth(trip)", "rad", "Heading (azimuth) over ground in radians, a piecewise-constant function of the trajectory."},
}
var tPropList = []string{"velocity", "distance", "heading"}

// clip wraps a temporal expression with atTime for the OGC leaf (instant set)
// or datetime (interval) selector, binding the selector value as a parameter.
func clip(expr string, q url.Values, args []any) (string, []any, error) {
	if lf := q.Get("leaf"); lf != "" {
		set, err := tstzSet(lf)
		if err != nil {
			return "", nil, err
		}
		args = append(args, set)
		return "atTime(" + expr + ", $" + itoa(len(args)) + "::tstzset)", args, nil
	}
	if dt := q.Get("datetime"); dt != "" {
		if s, e, ok := splitInterval(dt); ok {
			args = append(args, "["+s+", "+e+"]")
			return "atTime(" + expr + ", $" + itoa(len(args)) + "::tstzspan)", args, nil
		}
		args = append(args, "{"+strings.TrimSpace(dt)+"}") // a single instant
		return "atTime(" + expr + ", $" + itoa(len(args)) + "::tstzset)", args, nil
	}
	return expr, args, nil
}

// tgSequence returns the moving feature's temporal geometry (MF-JSON), optionally
// clipped to a datetime interval or to leaf instants.
func tgSequence(w http.ResponseWriter, r *http.Request) {
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
	expr, args, cerr := clip("trip", r.URL.Query(), []any{fid})
	if cerr != nil {
		httpErr(w, 400, cerr.Error())
		return
	}
	var body *string
	err = pool.QueryRow(r.Context(), "SELECT asMFJSON("+expr+")::text FROM "+ident(tbl)+" WHERE id=$1", args...).Scan(&body)
	if errors.Is(err, pgx.ErrNoRows) {
		httpErr(w, 404, "feature not found")
		return
	}
	if err != nil {
		httpErr(w, 400, err.Error())
		return
	}
	if body == nil {
		httpErr(w, 404, "no temporal geometry for the requested selector")
		return
	}
	writeRaw(w, 200, ogcify(*body))
}

// tgSequenceQuery serves the OGC TemporalGeometry derived queries. distance and
// velocity are exact MobilityDB functions; acceleration is NOT_IMPLEMENTED
// because with linearly interpolated position the speed is piecewise-constant,
// so its derivative is zero within each segment and undefined at the vertices.
func tgSequenceQuery(w http.ResponseWriter, r *http.Request) {
	q := strings.ToLower(r.PathValue("qtype"))
	if q == "acceleration" {
		httpErr(w, 501, "acceleration is not derivable: linearly interpolated position gives a piecewise-constant (Step) speed, whose derivative is zero within each segment and undefined at the vertices")
		return
	}
	if q != "velocity" && q != "distance" {
		httpErr(w, 404, "unknown query type: "+q)
		return
	}
	tbl, _, ok := collectionMeta(r.Context(), r.PathValue("cid"))
	if !ok {
		httpErr(w, 404, "collection not found")
		return
	}
	writeTemporalProperty(w, r, tbl, q, tProps[q])
}

// getTProperty serves one derived temporal property as an OGC temporalProperty.
func getTProperty(w http.ResponseWriter, r *http.Request) {
	name := strings.ToLower(r.PathValue("pname"))
	spec, ok := tProps[name]
	if !ok {
		httpErr(w, 404, "unknown temporal property: "+name)
		return
	}
	tbl, _, ok := collectionMeta(r.Context(), r.PathValue("cid"))
	if !ok {
		httpErr(w, 404, "collection not found")
		return
	}
	writeTemporalProperty(w, r, tbl, name, spec)
}

// writeTemporalProperty builds the OGC temporalProperty object in SQL: the
// derived tfloat is serialised with asMFJSON and reshaped into a valueSequence,
// carrying each segment's own interpolation verbatim from MobilityDB.
func writeTemporalProperty(w http.ResponseWriter, r *http.Request, tbl, name string, spec propSpec) {
	fid, err := strconv.Atoi(r.PathValue("fid"))
	if err != nil {
		httpErr(w, 400, "invalid feature id")
		return
	}
	expr, args, cerr := clip(spec.expr, r.URL.Query(), []any{fid, name, spec.uom, spec.desc})
	if cerr != nil {
		httpErr(w, 400, cerr.Error())
		return
	}
	args = append(args, r.URL.Path)
	selfP := "$" + itoa(len(args))
	// asMFJSON emits continuous values under "sequences" and discrete values
	// (instants / leaf selection) as a single top-level values/datetimes object;
	// reshape both into the OGC valueSequence, keeping the true interpolation.
	sql := "WITH base AS (SELECT asMFJSON(" + expr + ")::jsonb AS j FROM " + ident(tbl) + " WHERE id=$1) " +
		"SELECT jsonb_build_object('name',$2::text,'type','TReal','form',$3::text,'description',$4::text," +
		"'valueSequence', CASE" +
		" WHEN j ? 'sequences' THEN coalesce((SELECT jsonb_agg(jsonb_build_object(" +
		"'datetimes',seq->'datetimes','values',seq->'values','interpolation',j->>'interpolation'," +
		"'lower_inc',seq->'lower_inc','upper_inc',seq->'upper_inc') ORDER BY ord) " +
		"FROM jsonb_array_elements(j->'sequences') WITH ORDINALITY AS t(seq,ord)),'[]'::jsonb)" +
		" WHEN j ? 'datetimes' THEN jsonb_build_array(jsonb_build_object(" +
		"'datetimes',j->'datetimes','values',j->'values','interpolation',j->>'interpolation'," +
		"'lower_inc',j->'lower_inc','upper_inc',j->'upper_inc'))" +
		" ELSE '[]'::jsonb END," +
		"'links',jsonb_build_array(jsonb_build_object('rel','self','href'," + selfP + "::text)))::text FROM base"
	var body string
	err = pool.QueryRow(r.Context(), sql, args...).Scan(&body)
	if errors.Is(err, pgx.ErrNoRows) {
		httpErr(w, 404, "feature not found")
		return
	}
	if err != nil {
		httpErr(w, 400, err.Error())
		return
	}
	writeRaw(w, 200, ogcify(body))
}

// listTProperties lists the derived temporal properties available for a feature.
func listTProperties(w http.ResponseWriter, r *http.Request) {
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
	var one int
	if err := pool.QueryRow(r.Context(), "SELECT 1 FROM "+ident(tbl)+" WHERE id=$1", fid).Scan(&one); err != nil {
		httpErr(w, 404, "feature not found")
		return
	}
	base := r.URL.Path
	list := make([]map[string]any, 0, len(tPropList))
	for _, n := range tPropList {
		s := tProps[n]
		list = append(list, map[string]any{"name": n, "type": "TReal", "form": s.uom, "description": s.desc,
			"links": []map[string]string{{"rel": "self", "href": base + "/" + n}}})
	}
	writeJSON(w, 200, map[string]any{
		"temporalProperties": list, "numberReturned": len(list), "numberMatched": len(list),
		"timeStamp": time.Now().UTC().Format(time.RFC3339),
		"links":     []map[string]string{{"rel": "self", "href": base}},
	})
}

func tstzSet(csv string) (string, error) {
	parts := strings.Split(csv, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return "", errors.New("empty leaf selector")
	}
	return "{" + strings.Join(out, ", ") + "}", nil
}

// apiDoc serves a minimal OpenAPI definition (the OGC service-desc resource).
func apiDoc(w http.ResponseWriter, r *http.Request) {
	op := func(summary string) map[string]any {
		return map[string]any{"summary": summary, "responses": map[string]any{"200": map[string]any{"description": "OK"}}}
	}
	get := func(summary string) map[string]any { return map[string]any{"get": op(summary)} }
	doc := map[string]any{
		"openapi": "3.0.3",
		"info":    map[string]any{"title": "MobilityAPI-go", "version": "1.0.0", "description": "OGC API – Moving Features over MobilityDB"},
		"paths": map[string]any{
			"/":            get("Landing page"),
			"/api":         get("API definition"),
			"/conformance": get("Conformance declaration"),
			"/collections": map[string]any{
				"get":  op("Moving feature collections"),
				"post": op("Register a new collection"),
			},
			"/collections/{cid}": map[string]any{
				"get":    op("Collection metadata"),
				"put":    op("Replace collection metadata"),
				"delete": op("Delete a collection"),
			},
			"/collections/{cid}/items": map[string]any{
				"get":  op("Moving features (streamed, keyset-paged; bbox/datetime/subtrajectory filters)"),
				"post": op("Insert a moving feature"),
			},
			"/collections/{cid}/items/{fid}": map[string]any{
				"get":    op("A moving feature as a Feature"),
				"put":    op("Replace a moving feature"),
				"delete": op("Delete a moving feature"),
			},
			"/collections/{cid}/items/{fid}/tgsequence": map[string]any{
				"get":    op("Temporal geometry sequence (MF-JSON)"),
				"post":   op("Append a temporally-disjoint sub-trajectory"),
				"delete": op("501 — the feature carries a single temporal geometry; delete the feature instead"),
			},
			"/collections/{cid}/items/{fid}/tgsequence/{tgid}/{qtype}": get("Temporal geometry derived query: distance | velocity (acceleration → 501, not derivable for this motion model)"),
			"/collections/{cid}/items/{fid}/tproperties": map[string]any{
				"get":  op("Derived temporal properties of a feature"),
				"post": op("501 — temporal properties are derived from the geometry, not independently writable"),
			},
			"/collections/{cid}/items/{fid}/tproperties/{pname}": map[string]any{
				"get":    op("A derived temporal property (velocity | distance | heading)"),
				"post":   op("501 — derived property, not independently writable"),
				"delete": op("501 — derived property, not independently writable"),
			},
			"/collections/{cid}/export": get("Bulk lakehouse export: NDJSON, or ?format=parquet (WKB + bbox/time sidecar)"),
		},
	}
	w.Header().Set("Content-Type", "application/vnd.oai.openapi+json;version=3.0")
	w.WriteHeader(200)
	json.NewEncoder(w).Encode(doc)
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
	generic := collectionGeneric(r.Context(), tbl)
	if r.URL.Query().Get("format") == "parquet" {
		streamParquet(w, r, tbl, tgExpr, where, tail, args, generic)
		return
	}
	sql := "SELECT jsonb_build_object('type','Feature','id',id::text," +
		"'properties'," + propsExpr(generic) + "," +
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

// pqRowGeneric is the Parquet schema for collections with generic properties:
// the feature properties travel as one JSON column beside the WKB + sidecar.
type pqRowGeneric struct {
	ID         int64   `parquet:"id"`
	Properties string  `parquet:"properties"`
	WKB        []byte  `parquet:"trajectory_wkb"`
	Xmin       float64 `parquet:"xmin"`
	Ymin       float64 `parquet:"ymin"`
	Xmax       float64 `parquet:"xmax"`
	Ymax       float64 `parquet:"ymax"`
	Tmin       string  `parquet:"tmin"`
	Tmax       string  `parquet:"tmax"`
}

// streamParquet writes Parquet from a server-side cursor, flushing a row group
// every few thousand rows so memory stays bounded and each row group carries
// its own min/max statistics for predicate pushdown. The trajectory geometry
// materialises once per row (g) so the sidecar accessors do not re-clip.
func streamParquet(w http.ResponseWriter, r *http.Request, tbl, tgExpr, where, tail string, args []any, generic bool) {
	sidecar := " asBinary(g), Xmin(stbox(g)), Ymin(stbox(g)), Xmax(stbox(g)), Ymax(stbox(g))," +
		" Tmin(stbox(g))::text, Tmax(stbox(g))::text FROM (" +
		"SELECT id, %s " + tgExpr + " AS g FROM " + ident(tbl) + " " + where + " ORDER BY id" + tail + ") s"
	w.Header().Set("Content-Type", "application/vnd.apache.parquet")
	w.Header().Set("Content-Disposition", `attachment; filename="`+tbl+`.parquet"`)
	if generic {
		sql := "SELECT id, props," + fmt.Sprintf(sidecar, "coalesce(properties,'{}'::jsonb)::text AS props,")
		rows, err := pool.Query(r.Context(), sql, args...)
		if err != nil {
			httpErr(w, 500, err.Error())
			return
		}
		defer rows.Close()
		pw := parquet.NewGenericWriter[pqRowGeneric](w)
		defer pw.Close()
		batch := make([]pqRowGeneric, 0, parquetRG)
		emit := func() bool {
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
			var x pqRowGeneric
			if err := rows.Scan(&x.ID, &x.Properties, &x.WKB, &x.Xmin, &x.Ymin, &x.Xmax, &x.Ymax, &x.Tmin, &x.Tmax); err != nil {
				break
			}
			batch = append(batch, x)
			if len(batch) == parquetRG && !emit() {
				break
			}
		}
		emit()
		return
	}
	sql := "SELECT id, mmsi, name," + fmt.Sprintf(sidecar, "coalesce(mmsi,0) AS mmsi, coalesce(name,'') AS name,")
	rows, err := pool.Query(r.Context(), sql, args...)
	if err != nil {
		httpErr(w, 500, err.Error())
		return
	}
	defer rows.Close()
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
	tbl, srid, ok := collectionMeta(r.Context(), r.PathValue("cid"))
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
	// default to the collection CRS; an explicit feature crs overrides it
	if m := epsgURN.FindStringSubmatch(string(feat.CRS)); m != nil {
		srid, _ = strconv.Atoi(m[1])
	}
	id, _ := feat.ID.Int64()
	var execErr error
	if collectionGeneric(r.Context(), tbl) {
		_, execErr = pool.Exec(r.Context(),
			"INSERT INTO "+ident(tbl)+"(id,properties,trip) VALUES ($1,$2::jsonb, setSRID(tgeompointFromMFJSON($3), $4))",
			id, propsJSON(feat.Properties), string(tgBytes), srid)
	} else {
		name, _ := feat.Properties["name"].(string)
		_, execErr = pool.Exec(r.Context(),
			"INSERT INTO "+ident(tbl)+"(id,mmsi,name,trip) VALUES ($1,$2,$3, setSRID(tgeompointFromMFJSON($4), $5))",
			id, nil, name, string(tgBytes), srid)
	}
	if execErr != nil {
		httpErr(w, 400, "ingest failed: "+execErr.Error())
		return
	}
	writeJSON(w, 201, map[string]any{"message": "created", "id": strconv.FormatInt(id, 10)})
}

// featureTG decodes the temporalGeometry and name from a posted Feature (or a
// bare temporalGeometry), mapping the OGC interpolation token to MobilityDB.
func featureTG(r *http.Request) (tgText string, props map[string]any, err error) {
	var raw map[string]any
	if e := json.NewDecoder(r.Body).Decode(&raw); e != nil {
		return "", nil, errors.New("invalid JSON body")
	}
	tg := raw
	if inner, ok := raw["temporalGeometry"].(map[string]any); ok {
		tg = inner
	}
	if tg["type"] == nil {
		return "", nil, errors.New("missing temporalGeometry")
	}
	if interp, ok := tg["interpolation"].(string); ok {
		if m, ok := ogc2mdbInterp[interp]; ok {
			tg["interpolation"] = m
		}
	}
	if p, ok := raw["properties"].(map[string]any); ok {
		props = p
	}
	b, _ := json.Marshal(tg)
	return string(b), props, nil
}

// propsJSON serialises a feature's properties object for a generic collection's
// jsonb column ('{}' when absent).
func propsJSON(props map[string]any) string {
	if props == nil {
		return "{}"
	}
	b, _ := json.Marshal(props)
	return string(b)
}

// putItem replaces a moving feature's temporal geometry (and name).
func putItem(w http.ResponseWriter, r *http.Request) {
	tbl, srid, ok := collectionMeta(r.Context(), r.PathValue("cid"))
	if !ok {
		httpErr(w, 404, "collection not found")
		return
	}
	fid, err := strconv.Atoi(r.PathValue("fid"))
	if err != nil {
		httpErr(w, 400, "invalid feature id")
		return
	}
	tgText, props, derr := featureTG(r)
	if derr != nil {
		httpErr(w, 400, derr.Error())
		return
	}
	var ct pgconn.CommandTag
	if collectionGeneric(r.Context(), tbl) {
		ct, err = pool.Exec(r.Context(),
			"UPDATE "+ident(tbl)+" SET properties=$2::jsonb, trip=setSRID(tgeompointFromMFJSON($3), $4) WHERE id=$1",
			fid, propsJSON(props), tgText, srid)
	} else {
		name, _ := props["name"].(string)
		ct, err = pool.Exec(r.Context(),
			"UPDATE "+ident(tbl)+" SET name=$2, trip=setSRID(tgeompointFromMFJSON($3), $4) WHERE id=$1",
			fid, name, tgText, srid)
	}
	if err != nil {
		httpErr(w, 400, "update failed: "+err.Error())
		return
	}
	if ct.RowsAffected() == 0 {
		httpErr(w, 404, "feature not found")
		return
	}
	writeJSON(w, 200, map[string]any{"message": "replaced", "id": strconv.Itoa(fid)})
}

// deleteItem removes a moving feature.
func deleteItem(w http.ResponseWriter, r *http.Request) {
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
	ct, err := pool.Exec(r.Context(), "DELETE FROM "+ident(tbl)+" WHERE id=$1", fid)
	if err != nil {
		httpErr(w, 500, err.Error())
		return
	}
	if ct.RowsAffected() == 0 {
		httpErr(w, 404, "feature not found")
		return
	}
	w.WriteHeader(204)
}

// postTgSequence appends a temporally-disjoint sub-trajectory to the feature's
// temporal geometry. MobilityDB's merge rejects time overlap (mapped to 409).
func postTgSequence(w http.ResponseWriter, r *http.Request) {
	tbl, srid, ok := collectionMeta(r.Context(), r.PathValue("cid"))
	if !ok {
		httpErr(w, 404, "collection not found")
		return
	}
	fid, err := strconv.Atoi(r.PathValue("fid"))
	if err != nil {
		httpErr(w, 400, "invalid feature id")
		return
	}
	tgText, _, derr := featureTG(r)
	if derr != nil {
		httpErr(w, 400, derr.Error())
		return
	}
	ct, err := pool.Exec(r.Context(),
		"UPDATE "+ident(tbl)+" SET trip=merge(trip, setSRID(tgeompointFromMFJSON($2), $3)) WHERE id=$1",
		fid, tgText, srid)
	if err != nil {
		if strings.Contains(err.Error(), "overlap") {
			httpErr(w, 409, "the sub-trajectory overlaps the existing temporal geometry in time: "+err.Error())
			return
		}
		httpErr(w, 400, "append failed: "+err.Error())
		return
	}
	if ct.RowsAffected() == 0 {
		httpErr(w, 404, "feature not found")
		return
	}
	writeJSON(w, 200, map[string]any{"message": "appended", "id": strconv.Itoa(fid)})
}

// derivedReadOnly answers writes to derived temporal properties: they are
// computed from the trajectory, so they are modified through the geometry.
func derivedReadOnly(w http.ResponseWriter, r *http.Request) {
	httpErr(w, 501, "temporal properties here (velocity, distance, heading) are derived from the trajectory and are not independently writable; modify the temporal geometry instead")
}

// deleteTgSequence: the feature carries a single inseparable temporal geometry.
func deleteTgSequence(w http.ResponseWriter, r *http.Request) {
	httpErr(w, 501, "the moving feature has a single temporal geometry; delete the feature (DELETE .../items/{fid}) rather than an individual temporal primitive")
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
