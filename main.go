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
	mux.HandleFunc("GET /collections/{cid}", getCollection)
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
func ident(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

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
	get := func(summary string) map[string]any {
		return map[string]any{"get": map[string]any{"summary": summary,
			"responses": map[string]any{"200": map[string]any{"description": "OK"}}}}
	}
	doc := map[string]any{
		"openapi": "3.0.3",
		"info":    map[string]any{"title": "MobilityAPI-go", "version": "1.0.0", "description": "OGC API – Moving Features over MobilityDB"},
		"paths": map[string]any{
			"/":                              get("Landing page"),
			"/conformance":                   get("Conformance declaration"),
			"/collections":                   get("Moving feature collections"),
			"/collections/{cid}":             get("Collection metadata"),
			"/collections/{cid}/items":       get("Moving features (streamed, keyset-paged; bbox/datetime/subtrajectory filters)"),
			"/collections/{cid}/items/{fid}": get("A moving feature as a Feature"),
			"/collections/{cid}/items/{fid}/tgsequence":                get("Temporal geometry sequence (MF-JSON)"),
			"/collections/{cid}/items/{fid}/tgsequence/{tgid}/{qtype}": get("Temporal geometry derived query: distance | velocity (acceleration is not derivable for this motion model)"),
			"/collections/{cid}/items/{fid}/tproperties":               get("Derived temporal properties of a feature"),
			"/collections/{cid}/items/{fid}/tproperties/{pname}":       get("A derived temporal property (velocity | distance | heading)"),
			"/collections/{cid}/export":                                get("Bulk lakehouse export: NDJSON, or ?format=parquet (WKB + bbox/time sidecar)"),
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

// featureTG decodes the temporalGeometry and name from a posted Feature (or a
// bare temporalGeometry), mapping the OGC interpolation token to MobilityDB.
func featureTG(r *http.Request) (tgText string, name string, err error) {
	var raw map[string]any
	if e := json.NewDecoder(r.Body).Decode(&raw); e != nil {
		return "", "", errors.New("invalid JSON body")
	}
	tg := raw
	if inner, ok := raw["temporalGeometry"].(map[string]any); ok {
		tg = inner
	}
	if tg["type"] == nil {
		return "", "", errors.New("missing temporalGeometry")
	}
	if interp, ok := tg["interpolation"].(string); ok {
		if m, ok := ogc2mdbInterp[interp]; ok {
			tg["interpolation"] = m
		}
	}
	if props, ok := raw["properties"].(map[string]any); ok {
		name, _ = props["name"].(string)
	}
	b, _ := json.Marshal(tg)
	return string(b), name, nil
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
	tgText, name, derr := featureTG(r)
	if derr != nil {
		httpErr(w, 400, derr.Error())
		return
	}
	ct, err := pool.Exec(r.Context(),
		"UPDATE "+ident(tbl)+" SET name=$2, trip=setSRID(tgeompointFromMFJSON($3), $4) WHERE id=$1",
		fid, name, tgText, srid)
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
