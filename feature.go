// OGC MovingFeature assembly in the tier. The engine returns the canonical
// MEOS-function outputs (asMFJSON text, the STBOX accessors, the trajectory
// GeoJSON) plus the plain feature columns; the OGC Feature / FeatureCollection
// envelope is assembled here, once, for every backend. No temporal work runs
// in the tier — only JSON framing, which is not a MEOS concern.
package main

import (
	"encoding/json"
	"strconv"
)

type crsName struct {
	Type       string            `json:"type"`
	Properties map[string]string `json:"properties"`
}
type trsLink struct {
	Type       string            `json:"type"`
	Properties map[string]string `json:"properties"`
}
type ogcLink struct {
	Rel  string `json:"rel"`
	Href string `json:"href"`
}

// mfFeature is the OGC API – Moving Features "MovingFeature" object. Field order
// follows the MF-JSON shape; the temporalGeometry / geometry / properties are
// spliced verbatim from the engine (already-serialized JSON).
type mfFeature struct {
	Type             string          `json:"type"`
	ID               string          `json:"id"`
	Properties       json.RawMessage `json:"properties"`
	CRS              crsName         `json:"crs"`
	TRS              trsLink         `json:"trs"`
	Bbox             []float64       `json:"bbox"`
	Time             []string        `json:"time"`
	TemporalGeometry json.RawMessage `json:"temporalGeometry"`
	Geometry         json.RawMessage `json:"geometry,omitempty"`
	Links            []ogcLink       `json:"links"`
}

// exportFeature is the lighter NDJSON lakehouse-feed Feature: identity,
// properties and the temporalGeometry, without the OGC hypermedia envelope.
type exportFeature struct {
	Type             string          `json:"type"`
	ID               string          `json:"id"`
	Properties       json.RawMessage `json:"properties"`
	TemporalGeometry json.RawMessage `json:"temporalGeometry"`
}

func buildExportFeature(id int64, props json.RawMessage, tgeom []byte) ([]byte, error) {
	return json.Marshal(exportFeature{
		Type: "Feature", ID: strconv.FormatInt(id, 10),
		Properties:       props,
		TemporalGeometry: json.RawMessage(ogcify(string(tgeom))),
	})
}

func scanExportRow(rows Rows, generic bool) (id int64, props json.RawMessage, tgeom []byte, err error) {
	var tg string
	if generic {
		var pt string
		err = rows.Scan(&id, &pt, &tg)
		props = json.RawMessage(pt)
	} else {
		var mmsi *int64
		var name *string
		err = rows.Scan(&id, &mmsi, &name, &tg)
		props = typedProps(mmsi, name)
	}
	tgeom = []byte(tg)
	return
}

func crsURN(srid int) crsName {
	return crsName{Type: "Name", Properties: map[string]string{
		"name": "urn:ogc:def:crs:EPSG::" + strconv.Itoa(srid)}}
}

var trsGregorian = trsLink{Type: "Link", Properties: map[string]string{
	"type": "ogcdef", "href": "http://www.opengis.net/def/uom/ISO-8601/0/Gregorian"}}

// buildFeature assembles one MovingFeature. tgeom is the asMFJSON text and geom
// the trajectory GeoJSON (nil to omit); both are rewritten by ogcify to the OGC
// interpolation/crs vocabulary. props is the already-serialized properties JSON.
func buildFeature(id int64, props json.RawMessage, srid int, bbox []float64, tmin, tmax string, tgeom, geom []byte, cid string) ([]byte, error) {
	f := mfFeature{
		Type: "Feature", ID: strconv.FormatInt(id, 10),
		Properties:       props,
		CRS:              crsURN(srid),
		TRS:              trsGregorian,
		Bbox:             bbox,
		Time:             []string{tmin, tmax},
		TemporalGeometry: json.RawMessage(ogcify(string(tgeom))),
		Links:            []ogcLink{{Rel: "self", Href: "/collections/" + cid + "/items/" + strconv.FormatInt(id, 10)}},
	}
	if geom != nil {
		f.Geometry = json.RawMessage(ogcify(string(geom)))
	}
	return json.Marshal(f)
}

// propSel is the feature row's property projection: the stored properties text
// for generic collections, the typed (mmsi, name) columns for ships (assembled
// in the tier by typedProps, not a per-engine json builder in SQL).
func propSel(generic bool) string {
	if generic {
		return "coalesce(properties,'{}'::jsonb)::text"
	}
	return "mmsi, name"
}

// scanFeatureRow reads one row of the feature projection (id, properties,
// bbox xmin..ymax, tmin, tmax, asMFJSON) for either schema mode.
func scanFeatureRow(rows Rows, generic bool) (id int64, props json.RawMessage, bbox []float64, tmin, tmax string, tgeom []byte, err error) {
	bbox = make([]float64, 4)
	var tg string
	if generic {
		var pt string
		err = rows.Scan(&id, &pt, &bbox[0], &bbox[1], &bbox[2], &bbox[3], &tmin, &tmax, &tg)
		props = json.RawMessage(pt)
	} else {
		var mmsi *int64
		var name *string
		err = rows.Scan(&id, &mmsi, &name, &bbox[0], &bbox[1], &bbox[2], &bbox[3], &tmin, &tmax, &tg)
		props = typedProps(mmsi, name)
	}
	tgeom = []byte(tg)
	return
}

// typedProps builds the properties object for the typed ships schema in the
// tier (engine-agnostic), instead of a per-engine json builder in SQL.
func typedProps(mmsi *int64, name *string) json.RawMessage {
	m := map[string]any{}
	if mmsi != nil {
		m["mmsi"] = *mmsi
	} else {
		m["mmsi"] = nil
	}
	if name != nil {
		m["name"] = *name
	} else {
		m["name"] = nil
	}
	b, _ := json.Marshal(m)
	return b
}
