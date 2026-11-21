package main

// A raster collection, published as a coverage of OGC API - Coverages - Part 1
// (OGC 19-087, version 1.0.0rc4, a Candidate SWG Draft).
//
// A raster collection is registered, never created: it names an existing table,
// its period column (tstzspan) and its raster column, of a type the generated
// rasterTypes table names (raster or raquet), and the tier only reads it. The
// rows of a period are the tiles of one raster, and periods do not overlap, as
// MobilityDB reads a raster series (rasterValue over raster[] and tstzspan[]).
//
// A coverage answers one period. A datetime selecting exactly one period answers
// it; no datetime answers the latest period, which Content-Datetime names; a
// datetime meeting several periods is refused naming them, since GeoTIFF holds
// no time axis and a CoverageJSON coverage is written from one raster. The tiles
// of the period are joined by rasterUnion, clipped to bbox or subset and to the
// bands properties names by clip, and written by asTIFF or asCoverageJSON. Each
// of those is a function the generated table records over the collection's
// raster type, and a request needing one it does not record is refused naming
// it, so what a coverage answers is what MobilityDB declares.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// The conformance classes of OGC API - Coverages - Part 1 the tier implements.
const coverageConfBase = "https://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/"

var coverageConformance = []string{
	coverageConfBase + "core",
	coverageConfBase + "subsetting-spatial",
	coverageConfBase + "subsetting-temporal",
	coverageConfBase + "field-selection",
	coverageConfBase + "geotiff",
	coverageConfBase + "coveragejson",
}

// The link relations of a coverage collection and the media types of its
// encodings, as the draft names them.
const (
	relCoverage   = "http://www.opengis.net/def/rel/ogc/1.0/coverage"
	relSchema     = "http://www.opengis.net/def/rel/ogc/1.0/schema"
	mediaGeoTIFF  = "image/tiff; application=geotiff"
	mediaCovJSON  = "application/vnd.cov+json"
	mediaSchema   = "application/schema+json"
	coverageItems = "coverage"
)

// coverageEncodings are the encodings a coverage is written in, each with the
// SQL function writing it, in the order a client stating no preference gets
// them: GeoTIFF first, the type the collection's coverage link states.
var coverageEncodings = []struct{ media, fn string }{
	{mediaGeoTIFF, "asTIFF"},
	{mediaCovJSON, "asCoverageJSON"},
}

// coverageSource is what the registry records of a raster collection: its
// table, its period column, its raster column and the raster type of that
// column. Period, where set, is the one period of an instance
// (#coverageInstance) the source reads.
type coverageSource struct {
	Table, Time, Raster, Type string
	Period                    string
}

// from answers the SQL a query reads the rows of the source from: its table,
// or the rows of its period alone, under the table's own name, so the
// statements reading the table read an instance unchanged. The period is the
// text MobilityDB wrote of a period of the table, quoted as a literal, as
// #ident quotes a name.
func (src coverageSource) from() string {
	if src.Period == "" {
		return ident(src.Table)
	}
	return "(SELECT * FROM " + ident(src.Table) + " WHERE " + ident(src.Time) + " = CAST('" +
		strings.ReplaceAll(src.Period, "'", "''") + "' AS tstzspan)) AS " + ident(src.Table)
}

// coverageMeta answers the source of a raster collection, and false for a
// collection of moving features or a registry recording no raster collection,
// whose columns predate them.
func coverageMeta(ctx context.Context, cid string) (coverageSource, bool) {
	var tbl, tm, rs, typ *string
	err := db.QueryRow(ctx, `SELECT coverage_table, coverage_time, coverage_raster, raster_type
		FROM collections WHERE id=$1`, cid).Scan(&tbl, &tm, &rs, &typ)
	if err != nil || tbl == nil || *tbl == "" {
		return coverageSource{}, false
	}
	return coverageSource{Table: *tbl, Time: *tm, Raster: *rs, Type: *typ}, true
}

// rasterHas reports whether MobilityDB declares the SQL function fn over the
// raster type typ, reading the generated rasterTypes table as #typeHas reads
// temporalTypes.
func rasterHas(typ, fn string) bool {
	t := rasterTypeByName[typ]
	if t == nil {
		return false
	}
	for _, f := range t.Funcs {
		if strings.EqualFold(f, fn) {
			return true
		}
	}
	return false
}

// needRaster answers the refusal of a request needing fn over a raster type
// MobilityDB declares no fn over.
func needRaster(typ, fn string) error {
	if rasterHas(typ, fn) {
		return nil
	}
	return statusErr{501, fmt.Sprintf("a coverage of %s needs %s, which MobilityDB declares "+
		"over no %s", typ, fn, typ)}
}

// rasterTypeNames is the generated raster types, sorted, for an error a client
// reads.
func rasterTypeNames() string {
	names := make([]string, 0, len(rasterTypes))
	for _, t := range rasterTypes {
		names = append(names, t.Name)
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}

// coverageBody is the coverage member of a POST /collections body.
type coverageBody struct {
	Table      string `json:"table"`
	Time       string `json:"validtime"`
	Raster     string `json:"raster"`
	RasterType string `json:"rasterType"`
}

// registerCoverage records a raster collection: it checks that the table holds
// the period column as a tstzspan and the raster column as the stated raster
// type, reads the reference system of the rasters, and records the source. It
// creates nothing.
func registerCoverage(ctx context.Context, tx Tx, id, title, desc string, cb coverageBody) error {
	if cb.RasterType == "" {
		cb.RasterType = "raster"
	}
	if rasterTypeByName[cb.RasterType] == nil {
		return statusErr{400, "rasterType " + cb.RasterType + " is not a raster type; it is one of " +
			rasterTypeNames()}
	}
	for _, n := range []string{cb.Table, cb.Time, cb.Raster} {
		if !validID.MatchString(n) {
			return statusErr{400, "coverage table, validtime and raster must each match " +
				"[a-z_][a-z0-9_]* (a plain SQL identifier)"}
		}
	}
	probe := "SELECT CAST(" + ident(cb.Time) + " AS tstzspan), CAST(" + ident(cb.Raster) +
		" AS " + cb.RasterType + ") FROM " + ident(cb.Table) + " LIMIT 0"
	rows, err := tx.Query(ctx, probe)
	if err != nil {
		return statusErr{400, "the table " + cb.Table + " holds no tstzspan column " + cb.Time +
			" and " + cb.RasterType + " column " + cb.Raster + ": " + err.Error()}
	}
	rows.Close()
	srid := 3857
	if rasterHas(cb.RasterType, "SRID") {
		var s *int
		if err := tx.QueryRow(ctx, "SELECT SRID("+ident(cb.Raster)+") FROM "+ident(cb.Table)+
			" LIMIT 1").Scan(&s); err == nil && s != nil {
			srid = *s
		}
	}
	_, err = tx.Exec(ctx, `INSERT INTO collections (id,title,description,item_type,crs,
		coverage_table,coverage_time,coverage_raster,raster_type) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
		id, title, desc, coverageItems, srid, cb.Table, cb.Time, cb.Raster, cb.RasterType)
	return err
}

// registryColumns are the columns of the collections registry beyond those
// it was created with: the type a collection holds its moving features in, and
// the source of a raster collection.
var registryColumns = []string{"geometry_type", "coverage_table", "coverage_time",
	"coverage_raster", "raster_type"}

// ensureRegistry creates the collections registry where there is none and adds
// the registryColumns a registry created before them lacks, since CREATE TABLE
// IF NOT EXISTS leaves such a table as it stands, as #ensureTPropTable finds for
// its value columns.
func ensureRegistry(ctx context.Context, tx Tx) error {
	if _, err := tx.Exec(ctx,
		`CREATE TABLE IF NOT EXISTS collections (id text PRIMARY KEY, title text,
		   description text, item_type text, crs integer)`); err != nil {
		return err
	}
	for _, col := range registryColumns {
		if _, err := tx.Exec(ctx, "ALTER TABLE collections ADD COLUMN IF NOT EXISTS "+col+" text"); err != nil {
			return err
		}
	}
	return nil
}

// coverageLinks are the links of a raster collection in /collections and in
// its own description, each with the rel and type
// /req/core/rc-collection-info-links of EDR requires: the description, the
// coverage in each encoding, the schema, and one data link per query of
// #edrQueries, written as #edrDataQueries writes its links.
//
// The description of an instance, one period of the collection, links itself,
// the collection it belongs to and its own data links, the coverage and the
// schema being the collection's.
func coverageLinks(cid string, inst *coverageInstance) []map[string]string {
	base := "/collections/" + cid
	links := []map[string]string{
		{"rel": "self", "href": base, "type": "application/json"},
		{"rel": relCoverage, "href": base + "/coverage", "type": mediaGeoTIFF},
		{"rel": relCoverage, "href": base + "/coverage", "type": mediaCovJSON},
		{"rel": relSchema, "href": base + "/schema", "type": mediaSchema},
	}
	if inst != nil {
		links = []map[string]string{
			{"rel": "self", "href": inst.path(cid), "type": "application/json"},
			{"rel": "collection", "href": base, "type": "application/json"},
		}
		base = inst.path(cid)
	}
	for _, q := range edrQueries {
		links = append(links, map[string]string{"rel": "data", "href": base + "/" + q.name,
			"type": mediaCovJSON, "title": q.name})
	}
	return links
}

// coverageInstance is an instance of a raster collection, as
// /req/instances/src-md-op of EDR names one: a period of the collection, the
// rows of which are the tiles of one raster, identified by its start in RFC
// 3339, which no other period shares since the periods of a collection do not
// overlap.
type coverageInstance struct {
	ID, Period string
}

// path answers the path of the instance's description.
func (inst *coverageInstance) path(cid string) string {
	return "/collections/" + cid + "/instances/" + inst.ID
}

// coverageInstances answers the instances of a raster collection, one per
// period in time order, as #coveragePeriod reads the periods.
func coverageInstances(ctx context.Context, src coverageSource) ([]coverageInstance, error) {
	rows, err := db.Query(ctx, "SELECT DISTINCT CAST("+ident(src.Time)+" AS text), CAST(lower("+
		ident(src.Time)+") AS text), lower("+ident(src.Time)+") FROM "+ident(src.Table)+" ORDER BY 3")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []coverageInstance
	for rows.Next() {
		var period, start string
		var skip any
		if err := rows.Scan(&period, &start, &skip); err != nil {
			return nil, err
		}
		out = append(out, coverageInstance{ID: rfc3339(start), Period: period})
	}
	return out, rows.Err()
}

// coverageInstanceByID answers the instance of a raster collection an id
// names, and 404 for an id naming none.
func coverageInstanceByID(ctx context.Context, cid string, src coverageSource, id string) (*coverageInstance, error) {
	insts, err := coverageInstances(ctx, src)
	if err != nil {
		return nil, err
	}
	for i := range insts {
		if insts[i].ID == id {
			return &insts[i], nil
		}
	}
	return nil, statusErr{404, "the raster collection " + cid + " has no instance " + id}
}

// coverageCollection writes the description of a raster collection, the one
// #coverageDescription answers.
func coverageCollection(w http.ResponseWriter, r *http.Request, cid string, src coverageSource) {
	col, err := coverageDescription(r.Context(), cid, src, nil)
	if err != nil {
		httpErr(w, errStatus(err, 500), err.Error())
		return
	}
	writeJSON(w, 200, col)
}

// listInstances answers GET /collections/{cid}/instances, the instances of a
// raster collection, each described as #coverageDescription describes it, in
// the document instances.yaml of EDR 1.1 states.
func listInstances(w http.ResponseWriter, r *http.Request) {
	cid := r.PathValue("cid")
	src, ok := coverageMeta(r.Context(), cid)
	if !ok {
		httpErr(w, 404, "no raster collection "+cid)
		return
	}
	insts, err := coverageInstances(r.Context(), src)
	if err != nil {
		httpErr(w, 500, err.Error())
		return
	}
	out := []map[string]any{}
	for i := range insts {
		d, err := coverageDescription(r.Context(), cid, src, &insts[i])
		if err != nil {
			httpErr(w, errStatus(err, 500), err.Error())
			return
		}
		out = append(out, d)
	}
	writeJSON(w, 200, map[string]any{
		"links": []map[string]string{
			{"rel": "self", "href": "/collections/" + cid + "/instances", "type": "application/json"},
			{"rel": "collection", "href": "/collections/" + cid, "type": "application/json"},
		},
		"instances": out,
	})
}

// getInstance answers GET /collections/{cid}/instances/{iid}, the description
// of one instance as collection.yaml of EDR 1.1 states a description, the one
// #listInstances lists for it.
func getInstance(w http.ResponseWriter, r *http.Request) {
	cid := r.PathValue("cid")
	src, ok := coverageMeta(r.Context(), cid)
	if !ok {
		httpErr(w, 404, "no raster collection "+cid)
		return
	}
	inst, err := coverageInstanceByID(r.Context(), cid, src, r.PathValue("iid"))
	if err != nil {
		httpErr(w, errStatus(err, 500), err.Error())
		return
	}
	d, err := coverageDescription(r.Context(), cid, src, inst)
	if err != nil {
		httpErr(w, errStatus(err, 500), err.Error())
		return
	}
	writeJSON(w, 200, d)
}

// coverageDescription answers the description of a raster collection, which
// #listCollections lists and #coverageCollection writes, so that the two agree
// as /req/collections/src-md-success C of EDR requires: its links, its extent
// (the bounds of its rasters in CRS84 and the span of its periods), the EDR
// queries it answers and their output formats, and its bands as
// /req/edr/rc-parameters states parameters, written as asCoverageJSON writes
// its parameters and keyed by the names #edrBands reads from parameter-name.
//
// The description of an instance is that of its period alone: its id, its
// links (#coverageLinks), its queries and its extent, the bounds of the tiles
// of the period and the period itself.
func coverageDescription(ctx context.Context, cid string, src coverageSource,
	inst *coverageInstance) (map[string]any, error) {
	var title, desc string
	var srid int
	if err := db.QueryRow(ctx, `SELECT title,description,crs FROM collections WHERE id=$1`, cid).
		Scan(&title, &desc, &srid); err != nil {
		return nil, statusErr{404, "collection not found"}
	}
	bands, err := coverageBands(ctx, cid, src)
	if err != nil {
		return nil, err
	}
	params := map[string]any{}
	for i, ptype := range bands {
		key, label := "band"+itoa(i+1), "Band "+itoa(i+1)
		dataType := "integer"
		if jsonPixelType(ptype) == "number" {
			dataType = "float"
		}
		params[key] = map[string]any{
			"type": "Parameter", "id": key, "label": label, "data-type": dataType,
			"observedProperty": map[string]any{"label": map[string]string{"en": label}},
		}
	}
	id, base := cid, "/collections/"+cid
	if inst != nil {
		id, base = inst.ID, inst.path(cid)
		title = title + ", " + inst.Period
	}
	col := map[string]any{
		"id": id, "title": title, "description": desc, "itemType": coverageItems,
		"keywords":   []string{coverageItems, src.Type},
		"crs":        []string{"http://www.opengis.net/def/crs/EPSG/0/" + itoa(srid)},
		"rasterType": src.Type,
		"links":      coverageLinks(cid, inst),
		// The EDR queries the collection answers
		"data_queries":    edrDataQueries(base),
		"output_formats":  []string{edrFormat},
		"parameter_names": params,
		// A description linking a radius query states its distance units, as
		// /req/edr/rc-collection-info I requires
		"distanceunits": edrDistanceUnits,
	}
	if srid != 4326 {
		col["storageCrs"] = "http://www.opengis.net/def/crs/EPSG/0/" + itoa(srid)
	}
	if rasterHas(src.Type, "stbox") {
		var xmin, ymin, xmax, ymax *float64
		var tmin, tmax *string
		where, args := "", []any{}
		if inst != nil {
			where, args = " WHERE "+ident(src.Time)+" = CAST($1 AS tstzspan)", []any{inst.Period}
		}
		sql := "SELECT Xmin(b),Ymin(b),Xmax(b),Ymax(b),CAST(lower(p) AS text),CAST(upper(p) AS text) FROM (" +
			"SELECT transform(extent(stbox(" + ident(src.Raster) + ")), 4326) b, extent(" +
			ident(src.Time) + ") p FROM " + ident(src.Table) + where + ") s"
		if err := db.QueryRow(ctx, sql, args...).Scan(&xmin, &ymin, &xmax, &ymax, &tmin, &tmax); err == nil &&
			xmin != nil && tmin != nil {
			col["extent"] = map[string]any{
				"spatial": map[string]any{"bbox": [][]float64{{*xmin, *ymin, *xmax, *ymax}},
					"crs": "http://www.opengis.net/def/crs/OGC/1.3/CRS84"},
				"temporal": map[string]any{"interval": [][]string{{rfc3339(*tmin), rfc3339(*tmax)}},
					"trs": "http://www.opengis.net/def/uom/ISO-8601/0/Gregorian"},
			}
		}
	}
	return col, nil
}

// coverageSchema writes the fields of a raster collection, its bands, as
// /req/core/coverage-fields-schema states them: one per band, keyed as
// asCoverageJSON names its parameters (band1, band2, ...), in band order. The
// bands are read from a raster of the latest period.
func coverageSchema(w http.ResponseWriter, r *http.Request) {
	cid := r.PathValue("cid")
	src, ok := coverageMeta(r.Context(), cid)
	if !ok {
		httpErr(w, 404, "no raster collection "+cid)
		return
	}
	bands, err := coverageBands(r.Context(), cid, src)
	if err != nil {
		httpErr(w, errStatus(err, 500), err.Error())
		return
	}
	props := map[string]any{}
	seq := make([]string, 0, len(bands))
	for i, ptype := range bands {
		b := i + 1
		key := "band" + itoa(b)
		props[key] = map[string]any{"title": "Band " + itoa(b), "type": jsonPixelType(ptype),
			"x-ogc-pixelType": ptype}
		seq = append(seq, key)
	}
	w.Header().Set("Content-Type", mediaSchema)
	w.WriteHeader(200)
	json.NewEncoder(w).Encode(map[string]any{
		"$schema": "https://json-schema.org/draft/2020-12/schema", "type": "object",
		"title": cid, "properties": props, "x-ogc-propertySeq": seq,
	})
}

// coverageBands answers the pixel type of each band of a raster collection, in
// band order, read by numBands and bandPixelType from a raster of the period
// that ends last, the raster #coverageSchema and #coverageDescription both
// describe.
func coverageBands(ctx context.Context, cid string, src coverageSource) ([]string, error) {
	nbands := 1
	if rasterHas(src.Type, "numBands") {
		if err := db.QueryRow(ctx, "SELECT numBands("+ident(src.Raster)+") FROM "+
			ident(src.Table)+" ORDER BY "+ident(src.Time)+" DESC LIMIT 1").Scan(&nbands); err != nil {
			return nil, statusErr{404, "the raster collection " + cid + " holds no raster"}
		}
	}
	out := make([]string, 0, nbands)
	for b := 1; b <= nbands; b++ {
		var ptype string
		sql := "SELECT bandPixelType(" + ident(src.Raster) + ", " + itoa(b) + ") FROM " +
			ident(src.Table) + " ORDER BY " + ident(src.Time) + " DESC LIMIT 1"
		if src.Type != "raster" {
			sql = "SELECT bandPixelType(" + ident(src.Raster) + ") FROM " + ident(src.Table) +
				" ORDER BY " + ident(src.Time) + " DESC LIMIT 1"
		}
		if err := db.QueryRow(ctx, sql).Scan(&ptype); err != nil {
			return nil, err
		}
		out = append(out, ptype)
	}
	return out, nil
}

// jsonPixelType answers the JSON Schema type of a pixel type, an integer for
// the integer types and a number for the floating ones, in either the RaQuet
// spelling (uint8, float32) or the PostGIS one (8BUI, 32BF).
func jsonPixelType(p string) string {
	l := strings.ToLower(p)
	if strings.HasPrefix(l, "float") || strings.HasSuffix(l, "bf") {
		return "number"
	}
	return "integer"
}

// coverageRequest is a GET /coverage request, its parameters read.
type coverageRequest struct {
	span string    // the tstzspan datetime or a time subset selects, "" for none
	bbox []float64 // minLon, minLat, maxLon, maxLat in CRS84, nil for none
	// region is the SQL of a clip region in the rasters' system, reading
	// regionArg as $2 and the raster as r, "" for none; an EDR area or
	// corridor gives one in place of bbox
	region    string
	regionArg any
	bands     []int  // the bands properties selects, nil for all
	media     string // the media type negotiated
	writer    string // the function writing it
}

// parseBbox reads a bbox of four numbers, minLon,minLat,maxLon,maxLat in
// CRS84, the bbox #parseCoverageRequest reads for /coverage and #edrClip for
// an EDR cube.
func parseBbox(b string) ([]float64, error) {
	parts := strings.Split(b, ",")
	if len(parts) != 4 {
		return nil, statusErr{400, "bbox is minLon,minLat,maxLon,maxLat in CRS84"}
	}
	out := make([]float64, 0, 4)
	for _, p := range parts {
		f, err := strconv.ParseFloat(strings.TrimSpace(p), 64)
		if err != nil {
			return nil, statusErr{400, "bbox holds " + p + ", which is no number"}
		}
		out = append(out, f)
	}
	return out, nil
}

// subsetAxis matches one axis of a subset: its name and its interval or value.
var subsetAxis = regexp.MustCompile(`([A-Za-z]+)\(([^)]*)\)`)

// parseCoverageRequest reads the parameters of a coverage request against the
// bands the collection holds. The errors name the requirement they break.
func parseCoverageRequest(r *http.Request, nbands int) (coverageRequest, error) {
	q := r.URL.Query()
	var cr coverageRequest
	var lat, lon []string
	var timeSubset string
	for _, s := range q["subset"] {
		for _, m := range subsetAxis.FindAllStringSubmatch(s, -1) {
			switch strings.ToLower(m[1]) {
			case "lat":
				lat = strings.SplitN(m[2], ":", 2)
			case "lon":
				lon = strings.SplitN(m[2], ":", 2)
			case "time":
				timeSubset = m[2]
			default:
				return cr, statusErr{400, "subset names the axis " + m[1] +
					", which a coverage of this collection has not: it has Lat, Lon and time"}
			}
		}
	}
	if b := first(q, "bbox"); b != "" {
		if lat != nil || lon != nil {
			return cr, statusErr{400, "bbox and a spatial subset are given together"}
		}
		bbox, err := parseBbox(b)
		if err != nil {
			return cr, err
		}
		cr.bbox = bbox
	} else if lat != nil || lon != nil {
		cr.bbox = []float64{-180, -90, 180, 90}
		set := func(iv []string, lo, hi int) error {
			for i, v := range iv {
				if v = strings.TrimSpace(v); v == "*" || v == "" {
					continue
				}
				f, err := strconv.ParseFloat(v, 64)
				if err != nil {
					return statusErr{400, "subset holds " + v + ", which is no number"}
				}
				if len(iv) == 1 {
					cr.bbox[lo], cr.bbox[hi] = f, f
				} else if i == 0 {
					cr.bbox[lo] = f
				} else {
					cr.bbox[hi] = f
				}
			}
			return nil
		}
		if err := set(lon, 0, 2); err != nil {
			return cr, err
		}
		if err := set(lat, 1, 3); err != nil {
			return cr, err
		}
	}
	dt := first(q, "datetime")
	if dt != "" && timeSubset != "" {
		return cr, statusErr{400, "datetime and a time subset are given together"}
	}
	if timeSubset != "" {
		iv := strings.SplitN(timeSubset, ":", 2)
		for i := range iv {
			iv[i] = strings.Trim(strings.TrimSpace(iv[i]), `"`)
			if iv[i] == "*" {
				iv[i] = ".."
			}
		}
		dt = strings.Join(iv, "/")
	}
	if dt != "" {
		span, err := coverageSpan(dt)
		if err != nil {
			return cr, err
		}
		cr.span = span
	}
	props, excl := first(q, "properties"), first(q, "exclude-properties")
	if props != "" && excl != "" {
		return cr, statusErr{400, "properties and exclude-properties are given together"}
	}
	if props != "" || excl != "" {
		named := map[int]bool{}
		list := props
		if list == "" {
			list = excl
		}
		var order []int
		for _, p := range strings.Split(list, ",") {
			p = strings.TrimSpace(p)
			b, err := strconv.Atoi(strings.TrimPrefix(p, "band"))
			if !strings.HasPrefix(p, "band") || err != nil || b < 1 || b > nbands {
				return cr, statusErr{400, "the field " + p + " is not one of the collection's fields, " +
					"band1 to band" + itoa(nbands)}
			}
			if !named[b] {
				order = append(order, b)
			}
			named[b] = true
		}
		if props != "" {
			cr.bands = order
		} else {
			for b := 1; b <= nbands; b++ {
				if !named[b] {
					cr.bands = append(cr.bands, b)
				}
			}
			if len(cr.bands) == 0 {
				return cr, statusErr{400, "exclude-properties leaves no field"}
			}
		}
	}
	cr.media, cr.writer = negotiateCoverage(r.Header.Get("Accept"))
	if cr.media == "" {
		return cr, statusErr{406, "a coverage is written as " + mediaGeoTIFF + " or " + mediaCovJSON}
	}
	return cr, nil
}

// negotiateCoverage answers the encoding an Accept header selects: the first
// encoding it names, GeoTIFF when it names none of them through a wildcard or
// states no preference, and none when it admits neither.
func negotiateCoverage(accept string) (string, string) {
	if strings.TrimSpace(accept) == "" {
		return coverageEncodings[0].media, coverageEncodings[0].fn
	}
	best, bestAt := -1, len(accept)+1
	for i, e := range coverageEncodings {
		base := strings.SplitN(e.media, ";", 2)[0]
		if at := strings.Index(accept, base); at >= 0 && at < bestAt {
			best, bestAt = i, at
		}
	}
	if best >= 0 {
		return coverageEncodings[best].media, coverageEncodings[best].fn
	}
	if strings.Contains(accept, "*/*") || strings.Contains(accept, "image/*") {
		return coverageEncodings[0].media, coverageEncodings[0].fn
	}
	return "", ""
}

// coverageSpan answers the tstzspan of a datetime: an instant, or an interval
// whose open end is "..", as /req/subsetting-temporal/datetime writes them.
func coverageSpan(dt string) (string, error) {
	s, e, ok := splitInterval(dt)
	if !ok {
		if strings.Contains(dt, "/") || dt == ".." {
			return "", statusErr{400, "datetime " + dt + " is neither an instant nor an interval"}
		}
		return "[" + dt + ", " + dt + "]", nil
	}
	if s == "" || s == ".." {
		s = "-infinity"
	}
	if e == "" || e == ".." {
		e = "infinity"
	}
	return "[" + s + ", " + e + "]", nil
}

// coveragePeriod answers the one period a request selects, as text, and the
// refusal of a request selecting none or several.
func coveragePeriod(ctx context.Context, src coverageSource, span string) (string, error) {
	sql := "SELECT DISTINCT CAST(" + ident(src.Time) + " AS text), " + ident(src.Time) + " FROM " +
		src.from()
	var args []any
	if span != "" {
		sql += " WHERE " + ident(src.Time) + " && CAST($1 AS tstzspan)"
		args = append(args, span)
	}
	sql += " ORDER BY 2 DESC"
	if span == "" {
		sql += " LIMIT 1"
	}
	rows, err := db.Query(ctx, sql, args...)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var periods []string
	for rows.Next() {
		var p string
		var skip any
		if err := rows.Scan(&p, &skip); err != nil {
			return "", err
		}
		periods = append(periods, p)
	}
	switch {
	case len(periods) == 0 && span == "":
		return "", statusErr{404, "the raster collection holds no raster"}
	case len(periods) == 0:
		return "", statusErr{404, "no period of the raster collection meets " + span}
	case len(periods) > 1:
		sort.Strings(periods)
		return "", statusErr{400, "datetime meets " + itoa(len(periods)) + " periods of the raster " +
			"collection, " + strings.Join(periods, ", ") + "; a coverage answers one period"}
	}
	return periods[0], nil
}

// getCoverage answers GET /collections/{cid}/coverage: the raster of one
// period, clipped to the requested bounds and bands, in the negotiated
// encoding, with the Content-Datetime, Content-Bbox and Content-Crs headers
// /req/core/response-headers states.
func getCoverage(w http.ResponseWriter, r *http.Request) {
	cid := r.PathValue("cid")
	src, ok := coverageMeta(r.Context(), cid)
	if !ok {
		httpErr(w, 404, "no raster collection "+cid)
		return
	}
	nbands := 1
	if rasterHas(src.Type, "numBands") {
		if err := db.QueryRow(r.Context(), "SELECT numBands("+ident(src.Raster)+") FROM "+
			ident(src.Table)+" LIMIT 1").Scan(&nbands); err != nil {
			httpErr(w, 404, "the raster collection "+cid+" holds no raster")
			return
		}
	}
	cr, err := parseCoverageRequest(r, nbands)
	if err != nil {
		httpErr(w, errStatus(err, 400), err.Error())
		return
	}
	period, err := coveragePeriod(r.Context(), src, cr.span)
	if err != nil {
		httpErr(w, errStatus(err, 500), err.Error())
		return
	}
	data, bbox, srid, err := coverageData(r.Context(), src, period, cr)
	if err != nil {
		httpErr(w, errStatus(err, 500), err.Error())
		return
	}
	lo, hi := spanBounds(period)
	w.Header().Set("Content-Type", cr.media)
	w.Header().Set("Content-Datetime", rfc3339(lo)+"/"+rfc3339(hi))
	if bbox != nil {
		w.Header().Set("Content-Bbox", strings.Trim(strings.Join(strings.Fields(fmt.Sprint(bbox)), ","), "[]"))
	}
	if srid != 4326 {
		w.Header().Set("Content-Crs", "<http://www.opengis.net/def/crs/EPSG/0/"+itoa(srid)+">")
	}
	w.WriteHeader(200)
	w.Write(data)
}

// spanBounds answers the bounds of a tstzspan as MobilityDB writes it,
// "[t1, t2)".
func spanBounds(span string) (string, string) {
	s := strings.Trim(span, "[]()")
	parts := strings.SplitN(s, ",", 2)
	if len(parts) != 2 {
		return s, s
	}
	return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
}

// coverageData answers the bytes of the coverage of one period, the CRS84
// bounds of what it holds and its reference system. The tiles of the period are
// joined by rasterUnion where there are several, clipped by clip where bounds or
// bands are requested, and written by the negotiated writer.
func coverageData(ctx context.Context, src coverageSource, period string, cr coverageRequest) ([]byte, []float64, int, error) {
	var ntiles int
	if err := db.QueryRow(ctx, "SELECT COUNT(*) FROM "+src.from()+" WHERE "+ident(src.Time)+
		" = CAST($1 AS tstzspan)", period).Scan(&ntiles); err != nil {
		return nil, nil, 0, err
	}
	rast := ident(src.Raster)
	from := "(SELECT " + rast + " AS r FROM " + src.from() + " WHERE " + ident(src.Time) +
		" = CAST($1 AS tstzspan)) t"
	if ntiles > 1 {
		if err := needRaster(src.Type, "rasterUnion"); err != nil {
			return nil, nil, 0, err
		}
		from = "(SELECT rasterUnion(" + rast + ") AS r FROM " + src.from() + " WHERE " +
			ident(src.Time) + " = CAST($1 AS tstzspan)) t"
	}
	args := []any{period}
	expr := "r"
	if cr.bbox != nil || cr.region != "" || cr.bands != nil {
		if err := needRaster(src.Type, "clip"); err != nil {
			return nil, nil, 0, err
		}
		geom := "CAST(stbox(r) AS geometry)"
		if cr.bbox != nil {
			args = append(args, cr.bbox[0], cr.bbox[1], cr.bbox[2], cr.bbox[3])
			geom = "CAST(transform(stbox(ST_MakeEnvelope($2, $3, $4, $5, 4326)), SRID(r)) AS geometry)"
		} else if cr.region != "" {
			args = append(args, cr.regionArg)
			geom = cr.region
		}
		if cr.bands != nil {
			ids := make([]string, len(cr.bands))
			for i, b := range cr.bands {
				ids[i] = itoa(b)
			}
			expr = "clip(r, ARRAY[" + strings.Join(ids, ",") + "], " + geom + ")"
		} else {
			expr = "clip(r, " + geom + ")"
		}
	}
	if err := needRaster(src.Type, cr.writer); err != nil {
		return nil, nil, 0, err
	}
	write := cr.writer + "(c)"
	if cr.writer == "asCoverageJSON" {
		write = "CAST(" + write + " AS bytea)"
	}
	srid := 3857
	sridExpr := "3857"
	if rasterHas(src.Type, "SRID") {
		sridExpr = "SRID(c)"
	}
	sql := "SELECT " + write + ", Xmin(b), Ymin(b), Xmax(b), Ymax(b), " + sridExpr + " FROM (" +
		"SELECT c, transform(stbox(c), 4326) b FROM (SELECT " + expr + " AS c FROM " + from + ") u) v"
	var data []byte
	var xmin, ymin, xmax, ymax *float64
	if err := db.QueryRow(ctx, sql, args...).Scan(&data, &xmin, &ymin, &xmax, &ymax, &srid); err != nil {
		if strings.Contains(err.Error(), "no rows") {
			return nil, nil, 0, errors.New("the coverage holds no raster")
		}
		return nil, nil, 0, statusErr{400, "the coverage cannot be written: " + err.Error()}
	}
	var bbox []float64
	if xmin != nil {
		bbox = []float64{*xmin, *ymin, *xmax, *ymax}
	}
	return data, bbox, srid, nil
}
