package main

// The queries of OGC API - Environmental Data Retrieval - Part 1: Core
// (OGC 19-086r6, version 1.1) over a raster collection, each answered in
// CoverageJSON by MobilityDB:
//
// - /position reads the rasters at a point, at one instant or over a span,
//   the values rasterValue reads along a point that does not move, which
//   asCoverageJSON writes as a Point or a PointSeries;
// - /trajectory reads them along a LINESTRINGM, whose M ordinate is the
//   number of seconds since the Unix epoch, as tgeompoint(geometry) reads it,
//   written as a Trajectory;
// - /area clips the raster of one period to a POLYGON and /corridor to the
//   buffer of a path by its width, each a Grid, as a coverage is one period.
//
// The values along a point or a path are read across the periods of the
// collection by the series reader over the rasters and their periods, so a
// path spanning two months reads each month's raster over its part of it.

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

// The conformance class of EDR the tier declares: the core, which
// /req/core/conformance names. The class queries of 1.1 holds eight queries, each
// one a server MAY answer, and the tier answers four; the class covjson of 1.1
// names CoverageJSON application/prs.coverage+json, where the tier writes
// application/vnd.cov+json, the type OGC CoverageJSON (OGC 21-069r2) defines.
const edrConformance = "http://www.opengis.net/spec/ogcapi-edr-1/1.1/conf/core"

// The queries a raster collection answers, with the WKT geometry each reads.
var edrQueries = []struct{ name, geometry string }{
	{"position", "POINT"},
	{"area", "POLYGON"},
	{"trajectory", "LINESTRING"},
	{"corridor", "LINESTRING"},
}

// edrFormat is the one output format, as f names it.
const edrFormat = "CoverageJSON"

// edrWidthUnits are the units corridor-width is given in, with the metres in
// one.
var edrWidthUnits = map[string]float64{"m": 1, "km": 1000}

// edrDataQueries is the data_queries member of a raster collection's
// description, beside the links #coverageLinks states: one link per query, its
// variables as /req/edr/rc-common-variables states them, the corridor's with
// its units. A raster has no vertical axis, so a corridor's height is read and
// not applied, which its variables state.
func edrDataQueries(cid string) map[string]any {
	out := map[string]any{}
	for _, q := range edrQueries {
		vars := map[string]any{
			"query_type": q.name, "output_formats": []string{edrFormat},
			"default_output_format": edrFormat,
		}
		if q.name == "corridor" {
			vars["width_units"] = []string{"km", "m"}
			vars["height_units"] = []string{"m"}
			vars["description"] = "The coverage has no vertical axis: corridor-height is read and not applied."
		}
		out[q.name] = map[string]any{"link": map[string]any{
			"href": "/collections/" + cid + "/" + q.name, "rel": "data", "variables": vars,
		}}
	}
	return out
}

// edrQuery answers GET /collections/{cid}/{query}, as #getCoverage answers
// /coverage: the raster collection read, f checked, and the query's answer
// written in CoverageJSON.
func edrQuery(query string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cid := r.PathValue("cid")
		src, ok := coverageMeta(r.Context(), cid)
		if !ok {
			httpErr(w, 404, "no raster collection "+cid)
			return
		}
		q := r.URL.Query()
		if f := first(q, "f"); f != "" && !strings.EqualFold(f, edrFormat) {
			httpErr(w, 400, "f="+f+" is not an output format of the collection; it is "+edrFormat)
			return
		}
		coords := first(q, "coords")
		if coords == "" {
			httpErr(w, 400, "the "+query+" query needs coords, a WKT geometry")
			return
		}
		var data []byte
		var err error
		if query == "position" || query == "trajectory" {
			data, err = edrAlong(r.Context(), cid, src, query, coords, q)
		} else {
			data, err = edrClip(r.Context(), src, query, coords, q)
		}
		if err != nil {
			httpErr(w, errStatus(err, 400), err.Error())
			return
		}
		w.Header().Set("Content-Type", mediaCovJSON)
		w.WriteHeader(200)
		w.Write(data)
	}
}

// edrCoords checks that coords is WKT of the geometry type a query reads, in
// CRS84, refusing it as #parseCoverageRequest refuses a bbox it cannot read;
// a trajectory reads a LINESTRINGM.
func edrCoords(ctx context.Context, query, want, coords string) error {
	var typ *string
	var hasM *bool
	if err := db.QueryRow(ctx, "SELECT GeometryType(g), ST_HasM(g) FROM (SELECT "+
		"ST_GeomFromText($1, 4326) AS g) s", coords).Scan(&typ, &hasM); err != nil || typ == nil {
		return statusErr{400, "coords " + coords + " is no WKT geometry"}
	}
	if strings.TrimSuffix(strings.ToUpper(*typ), "M") != want {
		return statusErr{400, "the " + query + " query reads coords as a " + want + ", not a " + *typ}
	}
	if query == "trajectory" && (hasM == nil || !*hasM) {
		return statusErr{400, "the trajectory query reads coords as a LINESTRINGM, its M " +
			"ordinate the seconds since the Unix epoch"}
	}
	return nil
}

// edrBands answers the bands parameter-name names, as #parseCoverageRequest
// reads properties, the fields keyed band1, band2, ...; nil for none.
func edrBands(q map[string][]string) ([]int, error) {
	p := first(q, "parameter-name")
	if p == "" {
		return nil, nil
	}
	var out []int
	for _, f := range strings.Split(p, ",") {
		f = strings.TrimSpace(f)
		b, err := strconv.Atoi(strings.TrimPrefix(f, "band"))
		if !strings.HasPrefix(f, "band") || err != nil || b < 1 {
			return nil, statusErr{400, "parameter-name " + f + " is not a field of the collection, band1 ..."}
		}
		out = append(out, b)
	}
	return out, nil
}

// edrAlong answers /position and /trajectory: the values the rasters of the
// collection give along a point or a path, read with the path in the rasters'
// system by the series reader, and written by asCoverageJSON over the path in
// CRS84, so the coverage's positions are those the client gave. The rows of
// the periods the path meets are passed in period order, as #coverageData
// passes the tiles of a period.
func edrAlong(ctx context.Context, cid string, src coverageSource, query, coords string,
	q map[string][]string) ([]byte, error) {
	reader := "rasterValue"
	if src.Type != "raster" {
		reader = "rasterTileValue"
	}
	for _, fn := range []string{reader, "asCoverageJSON"} {
		if err := needRaster(src.Type, fn); err != nil {
			return nil, err
		}
	}
	want := map[string]string{"position": "POINT", "trajectory": "LINESTRING"}[query]
	if err := edrCoords(ctx, query, want, coords); err != nil {
		return nil, err
	}
	bands, err := edrBands(q)
	if err != nil {
		return nil, err
	}
	if len(bands) > 1 {
		return nil, statusErr{400, "the values along a point or a path are read from one band: " +
			"parameter-name names one"}
	}
	band := 1
	if len(bands) == 1 {
		band = bands[0]
	}
	_, srid, _ := collectionMeta(ctx, cid)

	args := []any{coords}
	geom := "ST_GeomFromText($1, 4326)"
	var traj string
	if query == "trajectory" {
		if first(q, "datetime") != "" {
			return nil, statusErr{400, "a LINESTRINGM states its times, so datetime is not given with it"}
		}
		traj = "tgeompoint(" + geom + ")"
	} else {
		// A point at an instant, or held over a span: the span datetime names,
		// or that of the collection's periods
		dt := first(q, "datetime")
		if dt == "" {
			var lo, hi *string
			if err := db.QueryRow(ctx, "SELECT CAST(lower(p) AS text), CAST(upper(p) AS text) FROM ("+
				"SELECT extent("+ident(src.Time)+") p FROM "+ident(src.Table)+") s").Scan(&lo, &hi); err != nil ||
				lo == nil {
				return nil, statusErr{404, "the raster collection holds no raster"}
			}
			dt = *lo + "/" + *hi
		}
		if strings.Contains(dt, "/") {
			span, err := coverageSpan(dt)
			if err != nil {
				return nil, err
			}
			args = append(args, span)
			traj = "tgeompoint(" + geom + ", CAST($2 AS tstzspan), 'Step')"
		} else {
			args = append(args, dt)
			traj = "tgeompoint(" + geom + ", CAST($2 AS timestamptz))"
		}
	}
	rows := "array_agg(" + ident(src.Raster) + " ORDER BY " + ident(src.Time) + "), array_agg(" +
		ident(src.Time) + " ORDER BY " + ident(src.Time) + ")"
	read := reader + "(transform(t, " + itoa(srid) + "), " + rows
	if reader == "rasterValue" {
		args = append(args, band)
		read += ", $" + itoa(len(args))
	}
	read += ")"
	sql := "SELECT asCoverageJSON(" + read + ", t) FROM (SELECT " + traj + " AS t) p, " +
		ident(src.Table) + " WHERE " + ident(src.Time) + " && timeSpan(t) GROUP BY t"
	var text *string
	if err := db.QueryRow(ctx, sql, args...).Scan(&text); err != nil {
		if strings.Contains(err.Error(), "no rows") {
			return nil, statusErr{404, "no period of the raster collection meets the " + query}
		}
		return nil, statusErr{400, "the " + query + " cannot be read: " + err.Error()}
	}
	if text == nil {
		return nil, statusErr{404, "the " + query + " reads no value of the raster collection"}
	}
	return []byte(*text), nil
}

// edrClip answers /area and /corridor: the raster of one period clipped to a
// polygon, or to the buffer of a path by the corridor's width, a Grid written
// by #coverageData as /coverage writes a clipped raster.
func edrClip(ctx context.Context, src coverageSource, query, coords string,
	q map[string][]string) ([]byte, error) {
	want := map[string]string{"area": "POLYGON", "corridor": "LINESTRING"}[query]
	if err := edrCoords(ctx, query, want, coords); err != nil {
		return nil, err
	}
	region := "transform(ST_GeomFromText($2, 4326), SRID(r))"
	if query == "corridor" {
		for _, p := range []string{"corridor-width", "width-units", "corridor-height", "height-units"} {
			if first(q, p) == "" {
				return nil, statusErr{400, "the corridor query needs " + p}
			}
		}
		width, err := strconv.ParseFloat(first(q, "corridor-width"), 64)
		if err != nil || width <= 0 {
			return nil, statusErr{400, "corridor-width is a positive number"}
		}
		perUnit, ok := edrWidthUnits[strings.ToLower(first(q, "width-units"))]
		if !ok {
			return nil, statusErr{400, "width-units is one of km, m"}
		}
		// The buffer is taken on the sphere, in metres, then read in the
		// rasters' system
		region = "transform(CAST(ST_Buffer(CAST(ST_Force2D(ST_GeomFromText($2, 4326)) AS geography), " +
			strconv.FormatFloat(width*perUnit, 'g', -1, 64) + ") AS geometry), SRID(r))"
	}
	bands, err := edrBands(q)
	if err != nil {
		return nil, err
	}
	span := ""
	if dt := first(q, "datetime"); dt != "" {
		if span, err = coverageSpan(dt); err != nil {
			return nil, err
		}
	}
	period, err := coveragePeriod(ctx, src, span)
	if err != nil {
		return nil, err
	}
	data, _, _, err := coverageData(ctx, src, period, coverageRequest{
		region: region, regionArg: coords, bands: bands, writer: "asCoverageJSON"})
	return data, err
}

// edrPaths are the paths of the queries for the API definition, as #apiDoc
// states each path the tier serves.
func edrPaths() map[string]any {
	out := map[string]any{}
	for _, q := range edrQueries {
		out["/collections/{cid}/"+q.name] = map[string]any{"get": map[string]any{"summary": fmt.Sprintf(
			"OGC API - EDR: the %s query over a raster collection, coords a WKT %s, answered in CoverageJSON",
			q.name, q.geometry)}}
	}
	return out
}
