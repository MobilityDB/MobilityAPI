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

	"github.com/parquet-go/parquet-go"
)

var (
	db           Backend
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

// hourOnlyOffset matches PostgreSQL's hour-only timezone offset on an ISO
// timestamp ("…18:57:52+00"); RFC 3339 — and strict parsers such as JS Date —
// require the ":00" minutes.
var hourOnlyOffset = regexp.MustCompile(`(\d\d:\d\d:\d\d(?:\.\d+)?)([+-]\d\d)(["\s]|$)`)

// rfc3339Tz completes hour-only timezone offsets so datetimes are RFC 3339.
func rfc3339Tz(s string) string { return hourOnlyOffset.ReplaceAllString(s, `$1$2:00$3`) }

func ogcify(s string) string {
	s = strings.ReplaceAll(s, `"interpolation": "Step"`, `"interpolation": "Stepwise"`)
	s = strings.ReplaceAll(s, `"interpolation":"Step"`, `"interpolation":"Stepwise"`)
	s = rfc3339Tz(s)
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
	var err error
	db, err = openBackend(dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	if err := db.Ping(context.Background()); err != nil {
		log.Fatal("db ping: ", err)
	}

	if broker := os.Getenv("MFAPI_MQTT_BROKER"); broker != "" {
		if _, err := startMQTTIngest(broker, "mfapi-"+strconv.Itoa(os.Getpid())); err != nil {
			log.Printf("MQTT ingestion disabled: %v", err)
		} else {
			log.Printf("MQTT ingestion on %s (topic mfapi/<cid>/<fid>/<pname>)", broker)
		}
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
	mux.HandleFunc("GET /collections/{cid}/trajectories", trajectories)
	mux.HandleFunc("GET /collections/{cid}/timeseries", timeseries)
	mux.HandleFunc("GET /collections/{cid}/items", streamItems)
	mux.HandleFunc("GET /collections/{cid}/items/{fid}", getItem)
	mux.HandleFunc("POST /collections/{cid}/items", postItem)
	mux.HandleFunc("GET /collections/{cid}/export", export) // lakehouse bulk feed (NDJSON | Parquet)
	// extension (not in conformsTo): bulk ingest of a real-time fleet feed
	mux.HandleFunc("POST /collections/{cid}/bulk", bulkIngest)
	// OGC API – Moving Features sub-resources of a moving feature:
	mux.HandleFunc("GET /collections/{cid}/items/{fid}/tgsequence", tgSequence)
	mux.HandleFunc("GET /collections/{cid}/items/{fid}/tgsequence/{tgid}/{qtype}", tgSequenceQuery)
	mux.HandleFunc("GET /collections/{cid}/items/{fid}/tproperties", listTProperties)
	mux.HandleFunc("GET /collections/{cid}/items/{fid}/tproperties/{pname}", getTProperty)
	// writes: replace / delete a feature, append a temporally-disjoint sub-trajectory
	mux.HandleFunc("PUT /collections/{cid}/items/{fid}", putItem)
	mux.HandleFunc("DELETE /collections/{cid}/items/{fid}", deleteItem)
	mux.HandleFunc("POST /collections/{cid}/items/{fid}/tgsequence", postTgSequence)
	// temporal properties are user-supplied, stored, time-varying attributes
	mux.HandleFunc("POST /collections/{cid}/items/{fid}/tproperties", postTProperties)
	mux.HandleFunc("POST /collections/{cid}/items/{fid}/tproperties/{pname}", postTPropertyValues)
	mux.HandleFunc("DELETE /collections/{cid}/items/{fid}/tproperties/{pname}", deleteTProperty)
	mux.HandleFunc("DELETE /collections/{cid}/items/{fid}/tgsequence/{tgid}", deleteTgSequence)
	// MF Part 4 (Stream Extension): continuous queries on a temporal property,
	// delivered over Server-Sent Events.
	mux.HandleFunc("POST /collections/{cid}/items/{fid}/tproperties/{pname}/queries", postQuery)
	mux.HandleFunc("GET /collections/{cid}/items/{fid}/tproperties/{pname}/queries", listQueries)
	mux.HandleFunc("GET /collections/{cid}/items/{fid}/tproperties/{pname}/queries/{qid}", getQuery)
	mux.HandleFunc("GET /collections/{cid}/items/{fid}/tproperties/{pname}/queries/{qid}/stream", streamQuery)
	mux.HandleFunc("DELETE /collections/{cid}/items/{fid}/tproperties/{pname}/queries/{qid}", deleteQuery)
	// live ingestion: a producer pushes records that live queries process in real time
	mux.HandleFunc("POST /collections/{cid}/items/{fid}/tproperties/{pname}/ingest", ingestProperty)
	// MF Part 4: a continuous query that streams the moving feature's position
	// (the temporal geometry), the feed for an animated map.
	mux.HandleFunc("POST /collections/{cid}/items/{fid}/tgsequence/queries", postGeometryQuery)
	mux.HandleFunc("GET /collections/{cid}/items/{fid}/tgsequence/queries", listGeometryQueries)
	mux.HandleFunc("GET /collections/{cid}/items/{fid}/tgsequence/queries/{qid}", getQuery)
	mux.HandleFunc("GET /collections/{cid}/items/{fid}/tgsequence/queries/{qid}/stream", streamQuery)
	mux.HandleFunc("DELETE /collections/{cid}/items/{fid}/tgsequence/queries/{qid}", deleteQuery)

	addr := ":" + strconv.Itoa(envInt("MFAPI_PORT", 8088))
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second}
	// streaming/export responses can be long-lived: no WriteTimeout (use ctx).
	go func() {
		log.Printf("MobilityAPI-go on %s (pool max=%d, default/max limit=%d/%d) — streaming, keyset-paged, lakehouse-ready", addr, envInt("MFAPI_MAXCONNS", 16), defaultLimit, maxLimit)
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
	err := db.QueryRow(ctx, `SELECT id, crs FROM collections WHERE id=$1`, cid).Scan(&table, &srid)
	return table, srid, err == nil
}

// collectionGeneric reports whether a collection's feature table carries a
// generic `properties jsonb` column (collections created through POST
// /collections) rather than the typed ships columns (mmsi, name).
func collectionGeneric(ctx context.Context, table string) bool {
	// Portable column probe: selecting the generic `properties` column succeeds
	// only when it exists, on PostgreSQL, DuckDB and Spark alike (Spark Connect
	// has no information_schema).
	rows, err := db.Query(ctx, "SELECT properties FROM "+ident(table)+" LIMIT 0")
	if err != nil {
		return false
	}
	rows.Close()
	return true
}

// featCols lists the non-geometry feature columns carried through the inner
// selects so the projection can read them.
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
		"http://www.opengis.net/spec/ogcapi-movingfeatures-4/1.0/conf/cquery",
		"http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/core",
		"http://www.opengis.net/spec/ogcapi-common-2/1.0/conf/collections",
	}})
}
func listCollections(w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query(r.Context(), `SELECT id, title, description, item_type, crs FROM collections ORDER BY id`)
	if err != nil {
		httpErr(w, 500, err.Error())
		return
	}
	defer rows.Close()
	cols := []map[string]any{}
	for rows.Next() {
		var id string
		var title, desc, itemType *string
		var crs int
		if err := rows.Scan(&id, &title, &desc, &itemType, &crs); err != nil {
			httpErr(w, 500, err.Error())
			return
		}
		cols = append(cols, map[string]any{
			"id": id, "title": title, "description": desc, "itemType": itemType,
			"crs": []string{"http://www.opengis.net/def/crs/EPSG/0/" + itoa(crs)},
			"links": []map[string]string{
				{"rel": "self", "href": "/collections/" + id},
				{"rel": "items", "href": "/collections/" + id + "/items"},
				{"rel": "enclosure", "href": "/collections/" + id + "/export", "type": "application/x-ndjson"},
			},
		})
	}
	writeJSON(w, 200, map[string]any{
		"collections": cols,
		"links":       []map[string]string{{"rel": "self", "href": "/collections"}},
	})
}
func getCollection(w http.ResponseWriter, r *http.Request) {
	cid := r.PathValue("cid")
	tbl, srid, ok := collectionMeta(r.Context(), cid)
	if !ok {
		httpErr(w, 404, "collection not found")
		return
	}
	var title, desc, itemType string
	if err := db.QueryRow(r.Context(), `SELECT title,description,item_type FROM collections WHERE id=$1`, cid).
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
	if err := db.QueryRow(r.Context(), "SELECT Xmin(e),Ymin(e),Xmax(e),Ymax(e),CAST(Tmin(e) AS text),CAST(Tmax(e) AS text) "+
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
	tx, err := db.Begin(r.Context())
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
	ct, err := db.Exec(r.Context(),
		`UPDATE collections SET title=$2, description=$3, crs=$4 WHERE id=$1`,
		cid, c.Title, c.Description, crsCode(c.CRS))
	if err != nil {
		httpErr(w, 400, "update failed: "+err.Error())
		return
	}
	if ct == 0 {
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
	tx, err := db.Begin(r.Context())
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
	purgeTProps(r.Context(), "cid=$1", cid)
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
	// bbox: minx,miny,maxx,maxy in the collection CRS. The GiST && on the
	// inline STBOX prefilters on the index; eintersects then refines to the
	// trajectories whose path actually crosses the envelope (exact, not the
	// bounding-box superset), so the result is "vessels that cross the box".
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
		env := "ST_MakeEnvelope($" + itoa(n+1) + ",$" + itoa(n+2) + ",$" + itoa(n+3) + ",$" + itoa(n+4) + ",$" + itoa(n+5) + ")"
		add("trip && stbox("+env+") AND eintersects(trip, "+env+")",
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
		span := "CAST($" + itoa(n+1) + " AS tstzspan)"
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
	generic := collectionGeneric(r.Context(), tbl)
	fc := featCols(generic)
	sql := "SELECT id, " + propSel(generic) + ", Xmin(b),Ymin(b),Xmax(b),Ymax(b),CAST(Tmin(b) AS text),CAST(Tmax(b) AS text), asMFJSON(g) FROM (" +
		"SELECT " + fc + ", g, stbox(g) AS b FROM (" +
		"SELECT " + fc + ", " + tgExpr + " AS g FROM " + ident(tbl) + " " + where +
		" ORDER BY id LIMIT " + lp + ") i) s ORDER BY id"
	rows, err := db.Query(r.Context(), sql, args...)
	if err != nil {
		httpErr(w, 500, err.Error())
		return
	}
	defer rows.Close()

	cid := r.PathValue("cid")
	w.Header().Set("Content-Type", "application/json")
	bw := bufio.NewWriterSize(w, 64*1024)
	defer bw.Flush()
	bw.WriteString(`{"type":"FeatureCollection","features":[`)
	var lastID int64
	n := 0
	for rows.Next() {
		id, props, bx, tmin, tmax, tgeom, serr := scanFeatureRow(rows, generic)
		if serr != nil {
			break
		}
		feat, ferr := buildFeature(id, props, srid, bx, tmin, tmax, tgeom, nil, cid)
		if ferr != nil {
			break
		}
		if n > 0 {
			bw.WriteByte(',')
		}
		bw.Write(feat)
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
	fc := featCols(generic)
	sql := "SELECT id, " + propSel(generic) + ", Xmin(b),Ymin(b),Xmax(b),Ymax(b),CAST(Tmin(b) AS text),CAST(Tmax(b) AS text), asMFJSON(g), CAST(ST_AsGeoJSON(trajectory(g)) AS text) FROM (" +
		"SELECT " + fc + ", g, stbox(g) AS b FROM (" +
		"SELECT " + fc + ", trip AS g FROM " + ident(tbl) + " WHERE id=$1) i) s"
	var id int64
	bx := make([]float64, 4)
	var tmin, tmax, tgeom, geom string
	var props json.RawMessage
	var serr error
	if generic {
		var pt string
		serr = db.QueryRow(r.Context(), sql, fid).Scan(&id, &pt, &bx[0], &bx[1], &bx[2], &bx[3], &tmin, &tmax, &tgeom, &geom)
		props = json.RawMessage(pt)
	} else {
		var mmsi *int64
		var name *string
		serr = db.QueryRow(r.Context(), sql, fid).Scan(&id, &mmsi, &name, &bx[0], &bx[1], &bx[2], &bx[3], &tmin, &tmax, &tgeom, &geom)
		props = typedProps(mmsi, name)
	}
	if serr != nil {
		httpErr(w, 404, "feature not found")
		return
	}
	feat, ferr := buildFeature(id, props, srid, bx, tmin, tmax, []byte(tgeom), []byte(geom), r.PathValue("cid"))
	if ferr != nil {
		httpErr(w, 500, ferr.Error())
		return
	}
	writeRaw(w, 200, string(feat))
}

// propSpec is a temporal property derived from the trajectory by an exact
// MobilityDB function (no resampling). speed/azimuth are piecewise-constant
// (Step) because the position is linearly interpolated between observations;
// cumulativeLength accumulates per segment (Linear). The handlers report each
// function's true interpolation — they do not coerce it.
type propSpec struct{ expr, uom, desc string }

var tProps = map[string]propSpec{
	"velocity": {"speed(trip)", "m/s", "Speed over ground (velocity magnitude), a piecewise-constant function of the trajectory."},
	"distance": {"cumulativeLength(trip)", "m", "Cumulative distance travelled along the trajectory."},
}

// tType describes how a scalar temporal property is carried: mf is the
// MobilityDB MF-JSON moving-type token, col its storage column, cast its type,
// ogc the canonical OGC type token, and defInterp the interpolation MobilityDB
// assumes when the body omits one.
type tType struct{ mf, col, cast, ogc, defInterp string }

// tPropType resolves an OGC temporal property type token to the four scalar
// temporal types MobilityDB carries as time-varying attribute values.
func tPropType(t string) (tType, bool) {
	switch strings.ToLower(strings.TrimSpace(t)) {
	case "", "treal", "tfloat", "measure", "real", "float", "double", "number":
		return tType{"MovingFloat", "vfloat", "tfloat", "TReal", "Linear"}, true
	case "tint", "tinteger", "integer", "int":
		return tType{"MovingInteger", "vint", "tint", "TInt", "Step"}, true
	case "ttext", "tstring", "text", "string":
		return tType{"MovingText", "vtext", "ttext", "TText", "Discrete"}, true
	case "tbool", "tboolean", "boolean", "bool":
		return tType{"MovingBoolean", "vbool", "tbool", "TBool", "Step"}, true
	}
	return tType{}, false
}

// orTrue returns the JSON boolean v, or true when v is absent — MF-JSON sequence
// bounds default to inclusive.
func orTrue(v any) bool {
	if b, ok := v.(bool); ok {
		return b
	}
	return true
}

// tPropMFJSON renders a MobilityDB MF-JSON document for one temporal property
// from its OGC body: either a flat {datetimes, values, interpolation} object or
// a valueSequence array (a sequence set when it holds more than one segment).
// The interpolation token is mapped from OGC to MobilityDB and defaults per type.
func tPropMFJSON(mfType, defInterp string, body map[string]any) (string, error) {
	mapInterp := func(v any) string {
		s, _ := v.(string)
		if s == "" {
			return defInterp
		}
		if m, ok := ogc2mdbInterp[s]; ok {
			return m
		}
		return s
	}
	if vs, ok := body["valueSequence"].([]any); ok && len(vs) > 0 {
		if len(vs) == 1 {
			seq, _ := vs[0].(map[string]any)
			return tPropMFJSON(mfType, defInterp, seq)
		}
		seqs := make([]any, 0, len(vs))
		interp := ""
		for _, s := range vs {
			m, _ := s.(map[string]any)
			if interp == "" {
				interp = mapInterp(m["interpolation"])
			}
			seqs = append(seqs, map[string]any{
				"values": m["values"], "datetimes": m["datetimes"],
				"lower_inc": orTrue(m["lower_inc"]), "upper_inc": orTrue(m["upper_inc"]),
			})
		}
		b, _ := json.Marshal(map[string]any{"type": mfType, "sequences": seqs, "interpolation": interp})
		return string(b), nil
	}
	if body["datetimes"] == nil || body["values"] == nil {
		return "", errors.New("temporal property requires datetimes and values (or a valueSequence)")
	}
	b, _ := json.Marshal(map[string]any{
		"type": mfType, "datetimes": body["datetimes"], "values": body["values"],
		"interpolation": mapInterp(body["interpolation"]),
		"lower_inc":     orTrue(body["lower_inc"]), "upper_inc": orTrue(body["upper_inc"]),
	})
	return string(b), nil
}

// ensureTPropTable creates the shared temporal-property store on first write: a
// row per (collection, feature, property) holding the value as a native
// MobilityDB temporal value in the column matching its type.
func ensureTPropTable(ctx context.Context, q interface {
	Exec(context.Context, string, ...any) (int64, error)
}) error {
	_, err := q.Exec(ctx, `CREATE TABLE IF NOT EXISTS mf_tproperty (
	  cid text NOT NULL, fid bigint NOT NULL, name text NOT NULL,
	  ptype text NOT NULL, uom text, description text,
	  vfloat tfloat, vint tint, vtext ttext, vbool tbool,
	  PRIMARY KEY (cid, fid, name))`)
	return err
}

// clip wraps a temporal expression with atTime for the OGC leaf (instant set)
// or datetime (interval) selector, binding the selector value as a parameter.
func clip(expr string, q url.Values, args []any) (string, []any, error) {
	if lf := q.Get("leaf"); lf != "" {
		set, err := tstzSet(lf)
		if err != nil {
			return "", nil, err
		}
		args = append(args, set)
		return "atTime(" + expr + ", CAST($" + itoa(len(args)) + " AS tstzset))", args, nil
	}
	if dt := q.Get("datetime"); dt != "" {
		if s, e, ok := splitInterval(dt); ok {
			args = append(args, "["+s+", "+e+"]")
			return "atTime(" + expr + ", CAST($" + itoa(len(args)) + " AS tstzspan))", args, nil
		}
		args = append(args, "{"+strings.TrimSpace(dt)+"}") // a single instant
		return "atTime(" + expr + ", CAST($" + itoa(len(args)) + " AS tstzset))", args, nil
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
	var one int
	if err := db.QueryRow(r.Context(), "SELECT 1 FROM "+ident(tbl)+" WHERE id=$1", fid).Scan(&one); errors.Is(err, ErrNoRows) {
		httpErr(w, 404, "feature not found")
		return
	} else if err != nil {
		httpErr(w, 500, err.Error())
		return
	}
	// The tgeompoint is a sequence set; each member sequence is one OGC temporal
	// primitive geometry, addressed by its 1-based index (sequenceN). The count
	// is iterated in the tier so the SQL stays portable (no generate_series).
	var nseq *int
	if err := db.QueryRow(r.Context(), "SELECT numSequences("+expr+") FROM "+ident(tbl)+" WHERE id=$1", args...).Scan(&nseq); err != nil {
		httpErr(w, 400, err.Error())
		return
	}
	count := 0
	if nseq != nil {
		count = *nseq
	}
	var ns []int
	var mfjsons [][]byte
	for n := 1; n <= count; n++ {
		var mf string
		if err := db.QueryRow(r.Context(), "SELECT asMFJSON(sequenceN("+expr+", "+itoa(n)+")) FROM "+ident(tbl)+" WHERE id=$1", args...).Scan(&mf); err != nil {
			break
		}
		ns = append(ns, n)
		mfjsons = append(mfjsons, []byte(mf))
	}
	body, err := buildTGSequence(r.URL.Path, ns, mfjsons)
	if err != nil {
		httpErr(w, 500, err.Error())
		return
	}
	writeRaw(w, 200, ogcify(string(body)))
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
	fid, err := strconv.Atoi(r.PathValue("fid"))
	if err != nil {
		httpErr(w, 400, "invalid feature id")
		return
	}
	tg, err := strconv.Atoi(r.PathValue("tgid"))
	if err != nil || tg < 1 {
		httpErr(w, 400, "invalid temporal geometry id (1-based index into the sequence)")
		return
	}
	var nseq *int
	err = db.QueryRow(r.Context(), "SELECT numSequences(trip) FROM "+ident(tbl)+" WHERE id=$1", fid).Scan(&nseq)
	if errors.Is(err, ErrNoRows) {
		httpErr(w, 404, "feature not found")
		return
	}
	if err != nil {
		httpErr(w, 400, err.Error())
		return
	}
	if nseq == nil || tg > *nseq {
		httpErr(w, 404, "no temporal geometry #"+itoa(tg)+" for this feature")
		return
	}
	// Address the temporal primitive geometry by its sequence index.
	spec := tProps[q]
	spec.expr = strings.Replace(spec.expr, "trip", "sequenceN(trip, "+itoa(tg)+")", 1)
	writeTemporalProperty(w, r, tbl, q, spec)
}

// getTProperty serves one stored temporal property as an OGC temporalProperty,
// optionally clipped to a datetime interval or to leaf instants.
func getTProperty(w http.ResponseWriter, r *http.Request) {
	cid := r.PathValue("cid")
	name := r.PathValue("pname")
	if _, _, ok := collectionMeta(r.Context(), cid); !ok {
		httpErr(w, 404, "collection not found")
		return
	}
	fid, err := strconv.Atoi(r.PathValue("fid"))
	if err != nil {
		httpErr(w, 400, "invalid feature id")
		return
	}
	var ptype, uom, desc string
	err = db.QueryRow(r.Context(),
		"SELECT ptype, coalesce(uom,''), coalesce(description,'') FROM mf_tproperty WHERE cid=$1 AND fid=$2 AND name=$3",
		cid, fid, name).Scan(&ptype, &uom, &desc)
	if errors.Is(err, ErrNoRows) {
		httpErr(w, 404, "unknown temporal property: "+name)
		return
	}
	if err != nil {
		httpErr(w, 500, err.Error())
		return
	}
	tt, ok := tPropType(ptype)
	if !ok {
		httpErr(w, 500, "stored property has an unknown type: "+ptype)
		return
	}
	expr, args, cerr := clip(tt.col, r.URL.Query(), []any{cid, fid, name})
	if cerr != nil {
		httpErr(w, 400, cerr.Error())
		return
	}
	sql := "SELECT asMFJSON(" + expr + ") FROM mf_tproperty WHERE cid=$1 AND fid=$2 AND name=$3"
	var mfjson *string
	if err := db.QueryRow(r.Context(), sql, args...).Scan(&mfjson); err != nil {
		httpErr(w, 400, err.Error())
		return
	}
	mf := []byte("null")
	if mfjson != nil {
		mf = []byte(*mfjson)
	}
	body, berr := reshapeTemporalProperty(name, tt.ogc, uom, desc, r.URL.Path, mf)
	if berr != nil {
		httpErr(w, 500, berr.Error())
		return
	}
	writeRaw(w, 200, ogcify(string(body)))
}

// writeTemporalProperty serves a derived measure as an OGC temporalProperty:
// the tfloat is serialised with asMFJSON and reshaped in the tier, carrying each
// segment's own interpolation verbatim from MobilityDB.
func writeTemporalProperty(w http.ResponseWriter, r *http.Request, tbl, name string, spec propSpec) {
	fid, err := strconv.Atoi(r.PathValue("fid"))
	if err != nil {
		httpErr(w, 400, "invalid feature id")
		return
	}
	expr, args, cerr := clip(spec.expr, r.URL.Query(), []any{fid})
	if cerr != nil {
		httpErr(w, 400, cerr.Error())
		return
	}
	sql := "SELECT asMFJSON(" + expr + ") FROM " + ident(tbl) + " WHERE id=$1"
	var mfjson *string
	err = db.QueryRow(r.Context(), sql, args...).Scan(&mfjson)
	if errors.Is(err, ErrNoRows) {
		httpErr(w, 404, "feature not found")
		return
	}
	if err != nil {
		httpErr(w, 400, err.Error())
		return
	}
	mf := []byte("null")
	if mfjson != nil {
		mf = []byte(*mfjson)
	}
	body, berr := reshapeTemporalProperty(name, "TReal", spec.uom, spec.desc, r.URL.Path, mf)
	if berr != nil {
		httpErr(w, 500, berr.Error())
		return
	}
	writeRaw(w, 200, ogcify(string(body)))
}

// listTProperties lists the stored temporal properties of a feature.
func listTProperties(w http.ResponseWriter, r *http.Request) {
	cid := r.PathValue("cid")
	tbl, _, ok := collectionMeta(r.Context(), cid)
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
	if err := db.QueryRow(r.Context(), "SELECT 1 FROM "+ident(tbl)+" WHERE id=$1", fid).Scan(&one); err != nil {
		httpErr(w, 404, "feature not found")
		return
	}
	base := r.URL.Path
	list := make([]map[string]any, 0)
	var reg *string
	db.QueryRow(r.Context(), "SELECT to_regclass('mf_tproperty')").Scan(&reg)
	if reg != nil {
		rows, err := db.Query(r.Context(),
			"SELECT name, ptype, coalesce(uom,''), coalesce(description,'') FROM mf_tproperty WHERE cid=$1 AND fid=$2 ORDER BY name",
			cid, fid)
		if err != nil {
			httpErr(w, 500, err.Error())
			return
		}
		defer rows.Close()
		for rows.Next() {
			var name, ptype, uom, desc string
			if err := rows.Scan(&name, &ptype, &uom, &desc); err != nil {
				break
			}
			list = append(list, map[string]any{"name": name, "type": ptype, "form": uom, "description": desc,
				"links": []map[string]string{{"rel": "self", "href": base + "/" + name}}})
		}
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
	// The query parameters OGC API - Moving Features - Part 1 specifies, in the
	// form its parameter requirements give them. getWith attaches them to the
	// operation that accepts them.
	param := func(name, in, desc string, schema map[string]any) map[string]any {
		return map[string]any{"name": name, "in": in, "description": desc, "required": false,
			"style": "form", "explode": false, "schema": schema}
	}
	limitParam := param("limit", "query",
		"The optional limit parameter limits the number of items presented in the response document.",
		map[string]any{"type": "integer", "minimum": 1, "maximum": maxLimit, "default": defaultLimit})
	bboxParam := param("bbox", "query",
		"Only features whose geometry intersects the bounding box are selected.",
		map[string]any{"type": "array", "minItems": 4, "maxItems": 6, "items": map[string]any{"type": "number"}})
	datetimeParam := param("datetime", "query",
		"Either a date-time or an interval. Only features that have a temporal geometry or temporal property that intersects the value are selected.",
		map[string]any{"type": "string"})
	leafParam := param("leaf", "query",
		"Only features with a temporal geometry or temporal property that intersect the given date-times are selected. The date-times are given as a comma-separated list.",
		map[string]any{"type": "array", "items": map[string]any{"type": "string", "format": "date-time"}})
	subTrajectoryParam := param("subTrajectory", "query",
		"Only a subsequence of the temporal geometry clipped to the datetime interval is returned. The datetime parameter is then a bounded interval and the leaf parameter is not used.",
		map[string]any{"type": "boolean", "default": false})
	subTemporalValueParam := param("subTemporalValue", "query",
		"Only a subsequence of the temporal property clipped to the datetime interval is returned. The datetime parameter is then a bounded interval and the leaf parameter is not used.",
		map[string]any{"type": "boolean", "default": false})
	withParams := func(o map[string]any, params ...map[string]any) map[string]any {
		o["parameters"] = params
		return o
	}
	doc := map[string]any{
		"openapi": "3.0.3",
		"info":    map[string]any{"title": "MobilityAPI-go", "version": "1.0.0", "description": "OGC API – Moving Features over MobilityDB"},
		"paths": map[string]any{
			"/":            get("Landing page"),
			"/api":         get("API definition"),
			"/conformance": get("Conformance declaration"),
			"/collections": map[string]any{
				"get":  withParams(op("Moving feature collections"), limitParam),
				"post": op("Register a new collection"),
			},
			"/collections/{cid}": map[string]any{
				"get":    op("Collection metadata"),
				"put":    op("Replace collection metadata"),
				"delete": op("Delete a collection"),
			},
			"/collections/{cid}/items": map[string]any{
				"get": withParams(op("Moving features (streamed, keyset-paged; bbox/datetime/subtrajectory filters)"),
					limitParam, bboxParam, datetimeParam, subTrajectoryParam),
				"post": op("Insert a moving feature"),
			},
			"/collections/{cid}/items/{fid}": map[string]any{
				"get":    op("A moving feature as a Feature"),
				"put":    op("Replace a moving feature"),
				"delete": op("Delete a moving feature"),
			},
			"/collections/{cid}/items/{fid}/tgsequence": map[string]any{
				"get": withParams(op("Temporal geometry sequence (TemporalGeometrySequence; members addressable by their 1-based id)"),
					limitParam, bboxParam, datetimeParam, leafParam, subTrajectoryParam),
				"post": op("Append a temporally-disjoint member sequence"),
			},
			"/collections/{cid}/items/{fid}/tgsequence/{tgid}": map[string]any{
				"delete": op("Delete a temporal primitive geometry (member sequence) by id"),
			},
			"/collections/{cid}/items/{fid}/tgsequence/{tgid}/{qtype}": get("Derived query on a member geometry: distance | velocity (acceleration → 501, not derivable for this motion model)"),
			"/collections/{cid}/items/{fid}/tproperties": map[string]any{
				"get": withParams(op("Stored temporal properties of a feature"),
					limitParam, datetimeParam, subTemporalValueParam),
				"post": op("Add one or more temporal properties (TReal | TInt | TText | TBool) to a feature"),
			},
			"/collections/{cid}/items/{fid}/tproperties/{pname}": map[string]any{
				"get": withParams(op("A stored temporal property as an OGC temporalProperty"),
					datetimeParam, leafParam, subTemporalValueParam),
				"post":   op("Append values to a temporal property (temporally disjoint; overlap → 409)"),
				"delete": op("Delete a temporal property"),
			},
			"/collections/{cid}/items/{fid}/tproperties/{pname}/queries": map[string]any{
				"get":  op("List the continuous queries on a temporal property"),
				"post": op("Register a continuous query (MF Part 4): a lifted transform (operation), or a windowed aggregation (aggregation + window: COUNT | TUMBLING | HOPPING). Set live:true to source from pushed records"),
			},
			"/collections/{cid}/items/{fid}/tproperties/{pname}/queries/{qid}": map[string]any{
				"get":    op("Continuous-query status (the cquery link object)"),
				"delete": op("Stop a continuous query"),
			},
			"/collections/{cid}/items/{fid}/tproperties/{pname}/queries/{qid}/stream": get("Continuous-query results as Server-Sent Events (see api/streaming-asyncapi.yaml)"),
			"/collections/{cid}/items/{fid}/tproperties/{pname}/ingest":               map[string]any{"post": op("Push a live record ({datetime, value}) to the live queries on the property")},
			"/collections/{cid}/items/{fid}/tgsequence/queries": map[string]any{
				"get":  op("List the geometry (position) continuous queries of a feature"),
				"post": op("Register a geometry continuous query streaming the moving feature's position"),
			},
			"/collections/{cid}/items/{fid}/tgsequence/queries/{qid}": map[string]any{
				"get":    op("Geometry continuous-query status (the cquery link object)"),
				"delete": op("Stop a geometry continuous query"),
			},
			"/collections/{cid}/items/{fid}/tgsequence/queries/{qid}/stream": get("Moving-feature positions as Server-Sent Events (see api/streaming-asyncapi.yaml)"),
			"/collections/{cid}/export":                                      get("Bulk lakehouse export: NDJSON, or ?format=parquet (WKB + bbox/time sidecar)"),
			"/collections/{cid}/bulk":                                        map[string]any{"post": op("Bulk ingest (extension): a batch of (vehicleId, position, time) observations as GeoJSON Points or GeoParquet, optionally gzip/deflate/br/zstd-compressed; each is appended as one instant")},
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
	sql := "SELECT id, " + propSel(generic) + ", asMFJSON(" + tgExpr + ") " +
		"FROM " + ident(tbl) + " " + where + " ORDER BY id" + tail
	rows, err := db.Query(r.Context(), sql, args...)
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
		id, props, tgeom, serr := scanExportRow(rows, generic)
		if serr != nil {
			break
		}
		feat, ferr := buildExportFeature(id, props, tgeom)
		if ferr != nil {
			break
		}
		bw.Write(feat)
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
		" CAST(Tmin(stbox(g)) AS text), CAST(Tmax(stbox(g)) AS text) FROM (" +
		"SELECT id, %s " + tgExpr + " AS g FROM " + ident(tbl) + " " + where + " ORDER BY id" + tail + ") s"
	w.Header().Set("Content-Type", "application/vnd.apache.parquet")
	w.Header().Set("Content-Disposition", `attachment; filename="`+tbl+`.parquet"`)
	if generic {
		sql := "SELECT id, props," + fmt.Sprintf(sidecar, "CAST(coalesce(properties,CAST('{}' AS jsonb)) AS text) AS props,")
		rows, err := db.Query(r.Context(), sql, args...)
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
	rows, err := db.Query(r.Context(), sql, args...)
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
		_, execErr = db.Exec(r.Context(),
			"INSERT INTO "+ident(tbl)+"(id,properties,trip) VALUES ($1,CAST($2 AS jsonb), setSRID(tgeompointFromMFJSON($3), $4))",
			id, propsJSON(feat.Properties), string(tgBytes), srid)
	} else {
		name, _ := feat.Properties["name"].(string)
		_, execErr = db.Exec(r.Context(),
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
	var ct int64
	if collectionGeneric(r.Context(), tbl) {
		ct, err = db.Exec(r.Context(),
			"UPDATE "+ident(tbl)+" SET properties=CAST($2 AS jsonb), trip=setSRID(tgeompointFromMFJSON($3), $4) WHERE id=$1",
			fid, propsJSON(props), tgText, srid)
	} else {
		name, _ := props["name"].(string)
		ct, err = db.Exec(r.Context(),
			"UPDATE "+ident(tbl)+" SET name=$2, trip=setSRID(tgeompointFromMFJSON($3), $4) WHERE id=$1",
			fid, name, tgText, srid)
	}
	if err != nil {
		httpErr(w, 400, "update failed: "+err.Error())
		return
	}
	if ct == 0 {
		httpErr(w, 404, "feature not found")
		return
	}
	writeJSON(w, 200, map[string]any{"message": "replaced", "id": strconv.Itoa(fid)})
}

// deleteItem removes a moving feature and any temporal properties stored on it.
func deleteItem(w http.ResponseWriter, r *http.Request) {
	cid := r.PathValue("cid")
	tbl, _, ok := collectionMeta(r.Context(), cid)
	if !ok {
		httpErr(w, 404, "collection not found")
		return
	}
	fid, err := strconv.Atoi(r.PathValue("fid"))
	if err != nil {
		httpErr(w, 400, "invalid feature id")
		return
	}
	ct, err := db.Exec(r.Context(), "DELETE FROM "+ident(tbl)+" WHERE id=$1", fid)
	if err != nil {
		httpErr(w, 500, err.Error())
		return
	}
	if ct == 0 {
		httpErr(w, 404, "feature not found")
		return
	}
	purgeTProps(r.Context(), "cid=$1 AND fid=$2", cid, fid)
	w.WriteHeader(204)
}

// purgeTProps deletes stored temporal properties matching the WHERE clause; it
// is a no-op when no property has ever been stored (the table is absent).
func purgeTProps(ctx context.Context, where string, args ...any) {
	var reg *string
	db.QueryRow(ctx, "SELECT to_regclass('mf_tproperty')").Scan(&reg)
	if reg != nil {
		db.Exec(ctx, "DELETE FROM mf_tproperty WHERE "+where, args...)
	}
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
	ct, err := db.Exec(r.Context(),
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
	if ct == 0 {
		httpErr(w, 404, "feature not found")
		return
	}
	writeJSON(w, 200, map[string]any{"message": "appended", "id": strconv.Itoa(fid)})
}

// postTProperties registers one or more stored temporal properties on a feature
// (the body is a single temporalProperty object or an array of them). Each value
// is parsed by MobilityDB's type-specific *FromMFJSON and stored as a native
// temporal value, so it is queryable with the same operators as the trajectory.
func postTProperties(w http.ResponseWriter, r *http.Request) {
	cid := r.PathValue("cid")
	tbl, _, ok := collectionMeta(r.Context(), cid)
	if !ok {
		httpErr(w, 404, "collection not found")
		return
	}
	fid, err := strconv.Atoi(r.PathValue("fid"))
	if err != nil {
		httpErr(w, 400, "invalid feature id")
		return
	}
	var raw json.RawMessage
	if e := json.NewDecoder(r.Body).Decode(&raw); e != nil {
		httpErr(w, 400, "invalid JSON body")
		return
	}
	var list []map[string]any
	if e := json.Unmarshal(raw, &list); e != nil {
		var one map[string]any
		if e2 := json.Unmarshal(raw, &one); e2 != nil {
			httpErr(w, 400, "invalid temporal property body")
			return
		}
		list = []map[string]any{one}
	}
	if len(list) == 0 {
		httpErr(w, 400, "no temporal property supplied")
		return
	}
	tx, err := db.Begin(r.Context())
	if err != nil {
		httpErr(w, 500, err.Error())
		return
	}
	defer tx.Rollback(r.Context())
	var one int
	if err := tx.QueryRow(r.Context(), "SELECT 1 FROM "+ident(tbl)+" WHERE id=$1", fid).Scan(&one); err != nil {
		httpErr(w, 404, "feature not found")
		return
	}
	if err := ensureTPropTable(r.Context(), tx); err != nil {
		httpErr(w, 500, err.Error())
		return
	}
	names := make([]string, 0, len(list))
	for _, p := range list {
		name, _ := p["name"].(string)
		if name == "" {
			httpErr(w, 400, "temporal property requires a name")
			return
		}
		typeTok, _ := p["type"].(string)
		tt, ok := tPropType(typeTok)
		if !ok {
			httpErr(w, 400, "unsupported temporal property type: "+typeTok)
			return
		}
		uom := strOf(p["form"])
		if uom == "" {
			uom = strOf(p["unitOfMeasure"])
		}
		mfjson, perr := tPropMFJSON(tt.mf, tt.defInterp, p)
		if perr != nil {
			httpErr(w, 400, perr.Error())
			return
		}
		if _, e := tx.Exec(r.Context(),
			"INSERT INTO mf_tproperty (cid,fid,name,ptype,uom,description,"+tt.col+") VALUES ($1,$2,$3,$4,$5,$6,"+tt.cast+"FromMFJSON($7))",
			cid, fid, name, tt.ogc, uom, strOf(p["description"]), mfjson); e != nil {
			if strings.Contains(e.Error(), "duplicate key") {
				httpErr(w, 409, "temporal property already exists: "+name)
				return
			}
			httpErr(w, 400, "store temporal property failed: "+e.Error())
			return
		}
		names = append(names, name)
	}
	if err := tx.Commit(r.Context()); err != nil {
		httpErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 201, map[string]any{"message": "created", "temporalProperties": names})
}

// postTPropertyValues appends more values to a stored temporal property; the new
// values must be temporally disjoint from the existing ones (overlap → 409).
func postTPropertyValues(w http.ResponseWriter, r *http.Request) {
	cid, name := r.PathValue("cid"), r.PathValue("pname")
	if _, _, ok := collectionMeta(r.Context(), cid); !ok {
		httpErr(w, 404, "collection not found")
		return
	}
	fid, err := strconv.Atoi(r.PathValue("fid"))
	if err != nil {
		httpErr(w, 400, "invalid feature id")
		return
	}
	var ptype string
	err = db.QueryRow(r.Context(), "SELECT ptype FROM mf_tproperty WHERE cid=$1 AND fid=$2 AND name=$3", cid, fid, name).Scan(&ptype)
	if errors.Is(err, ErrNoRows) {
		httpErr(w, 404, "unknown temporal property: "+name)
		return
	}
	if err != nil {
		httpErr(w, 500, err.Error())
		return
	}
	tt, ok := tPropType(ptype)
	if !ok {
		httpErr(w, 500, "stored property has an unknown type: "+ptype)
		return
	}
	var body map[string]any
	if e := json.NewDecoder(r.Body).Decode(&body); e != nil {
		httpErr(w, 400, "invalid JSON body")
		return
	}
	mfjson, perr := tPropMFJSON(tt.mf, tt.defInterp, body)
	if perr != nil {
		httpErr(w, 400, perr.Error())
		return
	}
	ct, e := db.Exec(r.Context(),
		"UPDATE mf_tproperty SET "+tt.col+"=merge("+tt.col+", "+tt.cast+"FromMFJSON($4)) WHERE cid=$1 AND fid=$2 AND name=$3",
		cid, fid, name, mfjson)
	if e != nil {
		if strings.Contains(e.Error(), "overlap") || strings.Contains(e.Error(), "common timestamp") {
			httpErr(w, 409, "the new values overlap the existing ones in time: "+e.Error())
			return
		}
		httpErr(w, 400, "append failed: "+e.Error())
		return
	}
	if ct == 0 {
		httpErr(w, 404, "unknown temporal property: "+name)
		return
	}
	writeJSON(w, 200, map[string]any{"message": "appended", "name": name})
}

// deleteTProperty removes a stored temporal property from a feature.
func deleteTProperty(w http.ResponseWriter, r *http.Request) {
	cid, name := r.PathValue("cid"), r.PathValue("pname")
	if _, _, ok := collectionMeta(r.Context(), cid); !ok {
		httpErr(w, 404, "collection not found")
		return
	}
	fid, err := strconv.Atoi(r.PathValue("fid"))
	if err != nil {
		httpErr(w, 400, "invalid feature id")
		return
	}
	var reg *string
	db.QueryRow(r.Context(), "SELECT to_regclass('mf_tproperty')").Scan(&reg)
	if reg == nil {
		httpErr(w, 404, "unknown temporal property: "+name)
		return
	}
	ct, e := db.Exec(r.Context(), "DELETE FROM mf_tproperty WHERE cid=$1 AND fid=$2 AND name=$3", cid, fid, name)
	if e != nil {
		httpErr(w, 500, e.Error())
		return
	}
	if ct == 0 {
		httpErr(w, 404, "unknown temporal property: "+name)
		return
	}
	w.WriteHeader(204)
}

// deleteTgSequence: the feature carries a single inseparable temporal geometry.
// deleteTgSequence removes one temporal primitive geometry (a member sequence)
// from the feature's tgeompoint sequence set, addressed by its 1-based index.
func deleteTgSequence(w http.ResponseWriter, r *http.Request) {
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
	tg, err := strconv.Atoi(r.PathValue("tgid"))
	if err != nil || tg < 1 {
		httpErr(w, 400, "invalid temporal geometry id (1-based index into the sequence)")
		return
	}
	var nseq *int
	err = db.QueryRow(r.Context(), "SELECT numSequences(trip) FROM "+ident(tbl)+" WHERE id=$1", fid).Scan(&nseq)
	if errors.Is(err, ErrNoRows) {
		httpErr(w, 404, "feature not found")
		return
	}
	if err != nil {
		httpErr(w, 400, err.Error())
		return
	}
	if nseq == nil || tg > *nseq {
		httpErr(w, 404, "no temporal geometry #"+itoa(tg)+" for this feature")
		return
	}
	if *nseq == 1 {
		httpErr(w, 409, "the feature has a single temporal geometry; delete the feature (DELETE .../items/{fid}) to remove it")
		return
	}
	// Remove the member by deleting its time span; deleteTime drops a whole
	// composing sequence and keeps the other members distinct.
	_, err = db.Exec(r.Context(),
		"UPDATE "+ident(tbl)+" SET trip = deleteTime(trip, getTime(sequenceN(trip, $2))) WHERE id = $1", fid, tg)
	if err != nil {
		httpErr(w, 400, "delete failed: "+err.Error())
		return
	}
	w.WriteHeader(204)
}

// small helpers
func itoa(n int) string  { return strconv.Itoa(n) }
func strOf(v any) string { s, _ := v.(string); return s }
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
