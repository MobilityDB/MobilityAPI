package main

// The queries of OGC API - Environmental Data Retrieval - Part 1: Core
// (OGC 19-086r6, version 1.1) over a collection of moving features, each
// answered by the moving features the query's geometry meets, as MobilityDB
// computes it, written as /items writes them:
//
// - /area answers the features whose temporal geometry ever intersects a
//   POLYGON, /cube those that ever intersect the rectangle of a bbox, and
//   /trajectory those that ever intersect a LINESTRING, each by eIntersects;
//   over a collection of geographies the bbox of /cube is the longitude and
//   latitude box between two meridians and two parallels, decided by
//   eOverlaps (#lonLatBoxTest);
// - /radius answers the features that ever come within a distance of a POINT,
//   on the spheroid, by eDwithin over the moving point read as a tgeogpoint.
//
// The geometry is read in CRS84 and carried into the collection's system. A
// datetime restricts each feature to that instant or interval before the test,
// so a feature is answered when it meets the geometry then. The answer is a
// FeatureCollection of whole features, in MF-JSON as /items writes it, or, with
// f=GeoJSON, in GeoJSON (RFC 7946), each feature the path it traverses in WGS 84
// and no temporal geometry. A feature's temporal properties are resources of
// their own (/tproperties), so the collection states no parameter.

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
)

// The output formats of a collection of moving features, as f names them:
// MF-JSON, the default, and GeoJSON.
const (
	edrMFJSON  = "MF-JSON"
	edrGeoJSON = "GeoJSON"
)

// edrFeatureFormats are the output formats of a collection of moving features.
var edrFeatureFormats = []string{edrMFJSON, edrGeoJSON}

// edrFeatureQueries are the queries a collection of moving features answers,
// with the WKT geometry each reads from coords; cube reads a bbox and no coords.
var edrFeatureQueries = []struct{ name, geometry string }{
	{"area", "POLYGON"},
	{"radius", "POINT"},
	{"cube", ""},
	{"trajectory", "LINESTRING"},
}

// edrFeatureQueryAnswered reports whether a collection of the temporal type typ
// answers a query: radius where its features can be read on the spheroid
// (#edrGeodetic), the others where MobilityDB states eIntersects over the type.
func edrFeatureQueryAnswered(typ, query string) bool {
	if query == "radius" {
		_, ok := edrGeodetic(typ, "trip")
		return ok
	}
	if query == "cube" && geographic(typ) {
		return typeHas(typ, "eOverlaps")
	}
	return typeHas(typ, "eIntersects")
}

// edrGeodetic is the SQL that reads the temporal geometry expr of type typ on
// the spheroid, as a temporal point over geography, over which MobilityDB
// computes eDwithin in metres: a moving point (MF-JSON's MovingPoint) over
// geography as it is, one over geometry in WGS 84 cast to tgeogpoint. Any
// other type has no such reading, a cast to tgeogpoint keeping a point of its
// values and not their extent, and the second answer says so. It decides by
// what the type has, as #inWGS84 and #staticGeometry do.
func edrGeodetic(typ, expr string) (string, bool) {
	t := temporalTypeByName[typ]
	if t == nil || t.MFJSON != "MovingPoint" {
		return "", false
	}
	if t.Base == "geography" && typeHas(typ, "eDwithin") {
		return expr, true
	}
	if t.Base == "geometry" && typeHas(typ, "tgeogpoint") && typeHas(typ, "transform") &&
		typeHas("tgeogpoint", "eDwithin") {
		return "tgeogpoint(transform(" + expr + ", 4326))", true
	}
	return "", false
}

// edrFeatureDataQueries is the data_queries member of the description of a
// collection of moving features of type typ: one link per query it answers,
// its variables as /req/edr/rc-common-variables states them.
func edrFeatureDataQueries(cid, typ string) map[string]any {
	out := map[string]any{}
	for _, q := range edrFeatureQueries {
		if !edrFeatureQueryAnswered(typ, q.name) {
			continue
		}
		vars := map[string]any{
			"query_type": q.name, "output_formats": edrFeatureFormats,
			"default_output_format": edrMFJSON,
		}
		switch q.name {
		case "radius":
			vars["within_units"] = edrDistanceUnits
		case "cube":
			vars["height_units"] = []string{"m"}
			vars["description"] = "The features are tested in two dimensions: z is read and not applied."
		}
		out[q.name] = map[string]any{"link": map[string]any{
			"href": "/collections/" + cid + "/" + q.name, "rel": "data", "type": "application/json",
			"variables": vars,
		}}
	}
	return out
}

// edrCollectionQuery answers GET /collections/{cid}/{query}: over a raster
// collection by #edrQuery, and, for a query #edrFeatureQueries holds, over a
// collection of moving features by #edrFeatureQuery.
func edrCollectionQuery(query string) http.HandlerFunc {
	raster := edrQuery(query)
	features := false
	for _, q := range edrFeatureQueries {
		features = features || q.name == query
	}
	if !features {
		return raster
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if _, ok := coverageMeta(r.Context(), r.PathValue("cid")); ok {
			raster(w, r)
			return
		}
		edrFeatureQuery(w, r, query)
	}
}

// edrFeatureQuery answers an EDR query over a collection of moving features,
// as #edrQuery answers one over a raster collection.
func edrFeatureQuery(w http.ResponseWriter, r *http.Request, query string) {
	cid := r.PathValue("cid")
	tbl, srid, ok := collectionMeta(r.Context(), cid)
	if !ok {
		httpErr(w, 404, "collection not found")
		return
	}
	typ := collectionType(r.Context(), cid)
	if !edrFeatureQueryAnswered(typ, query) {
		reason := "MobilityDB states no eIntersects over it"
		switch {
		case query == "radius":
			reason = "MobilityDB states no distance in metres from it to a point"
		case query == "cube" && geographic(typ):
			// As /items refuses a bbox over the same collection
			reason = "MobilityDB states no eOverlaps over it"
		}
		httpErr(w, 400, "the "+query+" query is not defined for a collection of "+typ+": "+reason)
		return
	}
	q := r.URL.Query()
	format := edrMFJSON
	if f := first(q, "f"); f != "" {
		switch {
		case strings.EqualFold(f, edrMFJSON):
		case strings.EqualFold(f, edrGeoJSON):
			format = edrGeoJSON
		default:
			httpErr(w, 400, "f="+f+" is not an output format of the collection; it is one of "+
				strings.Join(edrFeatureFormats, ", "))
			return
		}
	}
	if first(q, "parameter-name") != "" {
		httpErr(w, 400, "the collection states no parameter: it answers moving features, whose "+
			"temporal properties are read at /items/{fid}/tproperties")
		return
	}
	cond, args, err := edrFeatureCondition(r.Context(), query, tbl, typ, srid, q)
	if err != nil {
		httpErr(w, errStatus(err, 400), err.Error())
		return
	}
	edrWriteFeatures(w, r, cid, tbl, typ, srid, query, format, cond, args)
}

// edrFeatureCondition answers the condition a feature's row meets when it is
// in the answer, and its arguments: a box test the index answers, then the
// exact test over the temporal geometry, restricted to the datetime given.
func edrFeatureCondition(ctx context.Context, query, tbl, typ string, srid int,
	q map[string][]string) (string, []any, error) {
	coords := first(q, "coords")
	var geom string // the query's geometry in CRS84, as SQL over $1
	var args []any
	var cube []float64 // the bbox of a cube over a collection of geographies
	switch query {
	case "cube":
		for _, p := range []string{"bbox", "z"} {
			if first(q, p) == "" {
				return "", nil, statusErr{400, "the cube query needs " + p}
			}
		}
		bbox, err := parseBbox(first(q, "bbox"))
		if err != nil {
			return "", nil, err
		}
		if geographic(typ) {
			cube = bbox
			break
		}
		geom = "ST_MakeEnvelope($1, $2, $3, $4, 4326)"
		args = []any{bbox[0], bbox[1], bbox[2], bbox[3]}
	default:
		if coords == "" {
			return "", nil, statusErr{400, "the " + query + " query needs coords, a WKT geometry"}
		}
		want := map[string]string{"area": "POLYGON", "radius": "POINT", "trajectory": "LINESTRING"}[query]
		var typName *string
		var hasM *bool
		if err := db.QueryRow(ctx, "SELECT GeometryType(g), ST_HasM(g) FROM (SELECT "+
			"ST_GeomFromText($1, 4326) AS g) s", coords).Scan(&typName, &hasM); err != nil || typName == nil {
			return "", nil, statusErr{400, "coords " + coords + " is no WKT geometry"}
		}
		if strings.TrimSuffix(strings.ToUpper(*typName), "M") != want {
			return "", nil, statusErr{400, "the " + query + " query reads coords as a " + want +
				", not a " + *typName}
		}
		if hasM != nil && *hasM {
			return "", nil, statusErr{400, "a collection of moving features reads coords without " +
				"an M ordinate: the times of the query are given by datetime"}
		}
		geom = "ST_Force2D(ST_GeomFromText($1, 4326))"
		args = []any{coords}
	}
	// The temporal geometry the test reads: the feature's, or its restriction
	// to the datetime given, whose box restricts the index's test as well
	expr := "trip"
	timeBox := ""
	if dt := first(q, "datetime"); dt != "" {
		span, none, err := edrFeatureSpan(ctx, tbl, dt)
		if err != nil {
			return "", nil, err
		}
		if none {
			return "FALSE", nil, nil
		}
		args = append(args, span)
		p := "CAST($" + itoa(len(args)) + " AS tstzspan)"
		expr = "atTime(trip, " + p + ")"
		timeBox = " AND trip && " + p
	}
	// The bbox of a cube over a collection of geographies is a longitude and
	// latitude box on the spheroid (#lonLatBoxTest)
	if cube != nil {
		test, a, err := lonLatBoxTest(expr, cube, args)
		if err != nil {
			return "", nil, err
		}
		return test + timeBox, a, nil
	}
	// The geometry is read in the collection's system, or as a geography over
	// the spheroid for a type whose values are geographies, which MobilityDB
	// refuses to relate to a planar geometry
	t := temporalTypeByName[typ]
	geodetic := geographic(typ)
	inSystem := func(g string) string {
		if geodetic {
			return "CAST(" + g + " AS geography)"
		}
		args = append(args, srid)
		return "transform(" + g + ", $" + itoa(len(args)) + ")"
	}
	// The box test the index answers, where the type's box is an stbox; a type
	// boxed otherwise (a point cloud's tpcbox) is tested exactly alone
	boxTest := func(region string) string {
		if t != nil && t.Box != "stbox" {
			return "TRUE" + timeBox
		}
		return "trip && stbox(" + region + ")" + timeBox
	}
	if query != "radius" {
		region := inSystem(geom)
		return boxTest(region) + " AND eIntersects(" + expr + ", " + region + ")", args, nil
	}
	for _, p := range []string{"within", "within-units"} {
		if first(q, p) == "" {
			return "", nil, statusErr{400, "the radius query needs " + p}
		}
	}
	d, err := strconv.ParseFloat(first(q, "within"), 64)
	if err != nil || d <= 0 {
		return "", nil, statusErr{400, "within is a positive number"}
	}
	perUnit, ok := edrWidthUnits[strings.ToLower(first(q, "within-units"))]
	if !ok {
		return "", nil, statusErr{400, "within-units is one of " + strings.Join(edrDistanceUnits, ", ")}
	}
	metres := strconv.FormatFloat(d*perUnit, 'g', -1, 64)
	sphere, _ := edrGeodetic(typ, expr)
	// The index's box is that of a circle a tenth wider than the radius, taken
	// on the sphere and read in the collection's system: it holds every point
	// within the radius, the exact test deciding which features come within it
	wider := strconv.FormatFloat(d*perUnit*1.1, 'g', -1, 64)
	box := inSystem("CAST(ST_Buffer(CAST(" + geom + " AS geography), " + wider + ") AS geometry)")
	return boxTest(box) + " AND eDwithin(" + sphere + ", CAST(" + geom + " AS geography), " + metres + ")",
		args, nil
}

// edrFeatureSpan answers the tstzspan of a datetime over the collection whose
// features are in the table tbl, as #coverageSpan answers it over a raster
// collection: an instant, or an interval whose open end, "..", is the bound of
// the collection's own time extent on that side, since a tstzspan of MobilityDB
// is finite. The second answer is true where the interval holds no instant of
// the collection, so no feature is answered.
func edrFeatureSpan(ctx context.Context, tbl, dt string) (string, bool, error) {
	open := func(b string) bool { return b == "" || b == ".." }
	s, e, ok := splitInterval(dt)
	if !ok || (!open(s) && !open(e)) {
		span, err := coverageSpan(dt)
		return span, false, err
	}
	var lo, hi any
	if !open(s) {
		lo = s
	}
	if !open(e) {
		hi = e
	}
	var l, h *string
	var ordered *bool
	if err := db.QueryRow(ctx, "SELECT CAST(a AS text), CAST(b AS text), a <= b FROM (SELECT "+
		"coalesce(CAST($1 AS timestamptz), Tmin(x)) a, coalesce(CAST($2 AS timestamptz), Tmax(x)) b FROM ("+
		"SELECT extent(trip) x FROM "+ident(tbl)+") t) u", lo, hi).Scan(&l, &h, &ordered); err != nil {
		return "", false, statusErr{400, "datetime " + dt + " is neither an instant nor an interval"}
	}
	if l == nil || h == nil || ordered == nil || !*ordered {
		return "", true, nil
	}
	return "[" + *l + ", " + *h + "]", false, nil
}

// edrGeoFeature is a moving feature written in GeoJSON (RFC 7946): the path
// its temporal geometry traverses, in WGS 84, null for a type whose values
// have no single path.
type edrGeoFeature struct {
	Type       string          `json:"type"`
	ID         string          `json:"id"`
	Geometry   json.RawMessage `json:"geometry"`
	Properties json.RawMessage `json:"properties"`
	Links      []ogcLink       `json:"links"`
}

// edrWriteFeatures writes the features of the collection whose rows meet cond,
// as #streamItems writes a page of them: in id order, limit at a time, a next
// link carrying the keyset cursor after.
func edrWriteFeatures(w http.ResponseWriter, r *http.Request, cid, tbl, typ string, srid int,
	query, format, cond string, args []any) {
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
	if v := first(q, "after"); v != "" {
		id, e := strconv.ParseInt(v, 10, 64)
		if e != nil {
			httpErr(w, 400, "invalid 'after'")
			return
		}
		args = append(args, id)
		cond += " AND id > $" + itoa(len(args))
	}
	args = append(args, limit)
	lp := "$" + itoa(len(args))
	generic := collectionGeneric(r.Context(), tbl)
	fc := featCols(generic)
	// The last column is the feature's MF-JSON, or its path's GeoJSON in WGS 84
	last := "asMFJSON(g)"
	if format == edrGeoJSON {
		last = "coalesce(" + staticGeometry(typ, inWGS84(typ, "g")) + ", 'null')"
	}
	sql := "SELECT id, " + propSel(generic) + ", Xmin(b),Ymin(b),Xmax(b),Ymax(b),CAST(Tmin(b) AS text)," +
		"CAST(Tmax(b) AS text), " + last + " FROM (SELECT " + fc + ", g, " + boxOf(typ, "g") + " AS b FROM (" +
		"SELECT " + fc + ", trip AS g FROM " + ident(tbl) + " WHERE " + cond + " ORDER BY id LIMIT " + lp +
		") i) s ORDER BY id"
	rows, err := db.Query(r.Context(), sql, args...)
	if err != nil {
		httpErr(w, 400, "the "+query+" cannot be answered: "+err.Error())
		return
	}
	defer rows.Close()
	// The first row is read before the answer is begun, so a query MobilityDB
	// refuses is answered as refused and not as a collection of no feature
	more := rows.Next()
	if !more && rows.Err() != nil {
		httpErr(w, 400, "the "+query+" cannot be answered: "+rows.Err().Error())
		return
	}
	media := "application/json"
	if format == edrGeoJSON {
		media = "application/geo+json"
	}
	w.Header().Set("Content-Type", media)
	bw := bufio.NewWriterSize(w, 64*1024)
	defer bw.Flush()
	bw.WriteString(`{"type":"FeatureCollection","features":[`)
	var lastID int64
	n := 0
	for ; more; more = rows.Next() {
		id, props, bx, tmin, tmax, out, serr := scanFeatureRow(rows, generic)
		if serr != nil {
			break
		}
		var feat []byte
		if format == edrGeoJSON {
			feat, err = json.Marshal(edrGeoFeature{Type: "Feature", ID: strconv.FormatInt(id, 10),
				Geometry: json.RawMessage(out), Properties: props,
				Links: []ogcLink{{Rel: "self", Href: "/collections/" + cid + "/items/" + strconv.FormatInt(id, 10)}}})
		} else {
			feat, err = buildFeature(id, props, srid, bx, tmin, tmax, out, nil, cid)
		}
		if err != nil {
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
	if n == limit {
		nq := cloneQuery(q)
		nq.Set("after", strconv.FormatInt(lastID, 10))
		nq.Set("limit", itoa(limit))
		bw.WriteString(`,"links":[{"rel":"next","type":"` + media + `","href":"/collections/` + cid + "/" +
			query + "?" + nq.Encode() + `"}]`)
	}
	bw.WriteString(`}`)
}
