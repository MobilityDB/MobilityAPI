package main

import (
	"context"
	"net/http"
	"strconv"
	"strings"
)

// rasterQueryDesc describes the answer of the raster query of a temporal geometry.
const rasterQueryDesc = "Values the rasters of a raster collection give along the trajectory, " +
	"each raster read over the part of the trajectory within its validity period."

// rasterQuery answers the raster query of a temporal geometry, a MobilityAPI
// query type beside distance and velocity: the values the rasters of the raster
// collection named by `collection` give along the sequence tg of the feature,
// derived when asked and never stored. The rows of the periods the sequence meets
// are passed in period order to the series reader, as #edrAlong passes them, and
// `valueRange=lo,hi` keeps the times whose value lies in [lo, hi]. A feature of
// type typ is read only where MobilityDB declares the reader over typ.
func rasterQuery(ctx context.Context, q map[string][]string, typ string, tg int) (propSpec, error) {
	rcid := first(q, "collection")
	if rcid == "" {
		return propSpec{}, statusErr{400, "the raster query names its raster collection: collection"}
	}
	src, ok := coverageMeta(ctx, rcid)
	if !ok {
		if _, _, found := collectionMeta(ctx, rcid); found {
			return propSpec{}, statusErr{400, "collection " + rcid + " holds moving features, not rasters"}
		}
		return propSpec{}, statusErr{404, "raster collection not found: " + rcid}
	}
	reader := "rasterValue"
	if src.Type != "raster" {
		reader = "rasterTileValue"
	}
	if err := needRaster(src.Type, reader); err != nil {
		return propSpec{}, err
	}
	if !typeHas(typ, reader) {
		return propSpec{}, statusErr{400, "raster is not defined for a collection of " + typ +
			": MobilityDB states no " + reader + " over it"}
	}
	band := 1
	if b := first(q, "band"); b != "" {
		n, err := strconv.Atoi(b)
		if err != nil || n < 1 {
			return propSpec{}, statusErr{400, "band " + b + " is not a band number, 1 ..."}
		}
		if reader != "rasterValue" && n != 1 {
			return propSpec{}, statusErr{400, "a " + src.Type + " collection is read by " + reader +
				", which takes no band"}
		}
		band = n
	}
	_, srid, _ := collectionMeta(ctx, rcid)

	seq := "sequenceN(trip, " + itoa(tg) + ")"
	rows := func(col string) string {
		return "(SELECT array_agg(" + ident(col) + " ORDER BY " + ident(src.Time) + ") FROM " +
			ident(src.Table) + " WHERE " + ident(src.Time) + " && timeSpan(" + seq + "))"
	}
	values := reader + "(transform(" + seq + ", " + itoa(srid) + "), " + rows(src.Raster) + ", " +
		rows(src.Time)
	if reader == "rasterValue" {
		values += ", " + itoa(band)
	}
	values += ")"
	if vr := first(q, "valueRange"); vr != "" {
		lo, hi, err := valueRange(vr)
		if err != nil {
			return propSpec{}, err
		}
		values = "atSpan(" + values + ", floatspan '[" + lo + ", " + hi + "]')"
	}
	return propSpec{expr: values, desc: rasterQueryDesc}, nil
}

// valueRange reads `lo,hi`, two numbers with lo at most hi, and answers them
// written back as numbers, so they enter a span literal as nothing else.
func valueRange(s string) (string, string, error) {
	parts := strings.Split(s, ",")
	if len(parts) != 2 {
		return "", "", statusErr{400, "valueRange " + s + " is not lo,hi"}
	}
	var b [2]float64
	for i, p := range parts {
		v, err := strconv.ParseFloat(strings.TrimSpace(p), 64)
		if err != nil {
			return "", "", statusErr{400, "valueRange " + s + " is not lo,hi"}
		}
		b[i] = v
	}
	if b[0] > b[1] {
		return "", "", statusErr{400, "valueRange " + s + " has its lower bound above its upper bound"}
	}
	f := func(v float64) string { return strconv.FormatFloat(v, 'g', -1, 64) }
	return f(b[0]), f(b[1]), nil
}

// writeRasterQuery serves the raster query through #writeTemporalProperty, the
// writer of every derived query, with the status a refusal names.
func writeRasterQuery(w http.ResponseWriter, r *http.Request, tbl string, tg int) {
	spec, err := rasterQuery(r.Context(), r.URL.Query(), collectionType(r.Context(), r.PathValue("cid")), tg)
	if err != nil {
		httpErr(w, errStatus(err, 400), err.Error())
		return
	}
	writeTemporalProperty(w, r, tbl, "raster", spec)
}
