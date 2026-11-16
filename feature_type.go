package main

// The temporal type of a collection's feature geometry. A collection holds its
// moving features' temporal geometry in the trip column of one spatial temporal
// type of MEOS, named as meos_catalog.c names the SQL type (MEOS_TYPE_NAMES):
// tgeompoint by default, or any other the client states in temporalGeometryType
// when it creates the collection, th3index, tcbuffer and trgeometry among them.
// What a collection of a type can answer is what MobilityDB declares over that
// type, which the generated table states per type (TemporalType.Funcs), as
// #scalarTemporalTypes projects the same table for temporal properties.

import (
	"context"
	"sort"
	"strings"
)

// defaultGeometryType is the type of a collection that states none, the one
// every collection held before the type was a choice.
const defaultGeometryType = "tgeompoint"

// geometryTypes are the types a collection can hold its features in: every
// spatial temporal type MobilityDB reads from MF-JSON and writes back, and
// whose values it can merge, which is what a feature's write and append need.
var geometryTypes = func() map[string]*TemporalType {
	m := map[string]*TemporalType{}
	for i := range temporalTypes {
		t := &temporalTypes[i]
		if !t.Spatial || t.MFJSON == "" {
			continue
		}
		if !typeHasIn(t, t.Name+"FromMFJSON") || !typeHasIn(t, "asMFJSON") || !typeHasIn(t, "merge") {
			continue
		}
		m[t.Name] = t
	}
	return m
}()

// geometryTypeNames is geometryTypes' keys, sorted, for an error a client reads.
func geometryTypeNames() string {
	names := make([]string, 0, len(geometryTypes))
	for n := range geometryTypes {
		names = append(names, n)
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}

func typeHasIn(t *TemporalType, fn string) bool {
	for _, f := range t.Funcs {
		if strings.EqualFold(f, fn) {
			return true
		}
	}
	return false
}

// typeHas reports whether MobilityDB declares the SQL function fn with the
// temporal type typ as its first argument.
func typeHas(typ, fn string) bool {
	t := temporalTypeByName[typ]
	return t != nil && typeHasIn(t, fn)
}

// boxHas reports whether MobilityDB declares the SQL function fn over the
// bounding box type of the temporal type typ (stbox, or tpcbox for a point
// cloud), reading the generated table as #scalarTemporalTypes does.
func boxHas(typ, fn string) bool {
	t := temporalTypeByName[typ]
	if t == nil {
		return false
	}
	for _, f := range boxFunctions[t.Box] {
		if strings.EqualFold(f, fn) {
			return true
		}
	}
	return false
}

// collectionType answers the temporal type of a collection's trip column: the
// one the registry records, or defaultGeometryType for a collection that
// records none, which every collection created before the choice is, and the
// registry of such a deployment has no column for it at all.
func collectionType(ctx context.Context, cid string) string {
	var typ *string
	if err := db.QueryRow(ctx, `SELECT geometry_type FROM collections WHERE id=$1`, cid).Scan(&typ); err != nil || typ == nil || *typ == "" {
		return defaultGeometryType
	}
	return *typ
}

// tripReader is the SQL that reads a feature's temporal geometry of type typ
// from the MF-JSON in parameter $mf, giving it the SRID in parameter $srid
// where the type has one to set. A type with none (a cell, whose grid fixes
// its reference system, or a network point, whose ways table does) is read
// as MF-JSON states it, and the caller drops the SRID, its last argument,
// as the second answer says.
func tripReader(typ string, mf, srid int) (string, bool) {
	read := typ + "FromMFJSON($" + itoa(mf) + ")"
	if typeHas(typ, "setSRID") {
		return "setSRID(" + read + ", $" + itoa(srid) + ")", true
	}
	return read, false
}

// withSRID returns args unchanged where the reader consumes the SRID, its last
// argument, and args without it otherwise.
func withSRID(args []any, usesSRID bool) []any {
	if usesSRID {
		return args
	}
	return args[:len(args)-1]
}

// inWGS84 is the SQL that writes a feature's temporal geometry expr in WGS 84
// for an output the standard fixes there, where the type can be transformed;
// a type that cannot (a cell, held in WGS 84 by its grid) is written as is.
func inWGS84(typ, expr string) string {
	if typeHas(typ, "transform") {
		return "transform(" + expr + ", 4326)"
	}
	return expr
}

// boxOf is the SQL that builds the bounding box of a feature's temporal
// geometry expr with the constructor its type's box names (stbox, or tpcbox
// for a point cloud), as #inWGS84 writes the geometry by what its type has.
func boxOf(typ, expr string) string {
	box := "stbox"
	if t := temporalTypeByName[typ]; t != nil && t.Box != "" {
		box = t.Box
	}
	return box + "(" + expr + ")"
}

// staticGeometry is the SQL that writes the GeoJSON of the path a feature's
// temporal geometry expr traverses, where MobilityDB states trajectory over
// its type, and NULL otherwise (a cell sequence or a rigid body has no single
// path geometry), which the Feature then leaves out, as #inWGS84 writes the
// geometry by what its type has.
func staticGeometry(typ, expr string) string {
	if typeHas(typ, "trajectory") {
		// trajectory is the call over the temporal value; what wraps it reads
		// the geometry it returns, so the two are written apart.
		path := "trajectory(" + expr + ")"
		return "CAST(ST_AsGeoJSON(" + path + ") AS text)"
	}
	return "CAST(NULL AS text)"
}
