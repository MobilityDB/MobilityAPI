package main

import "testing"

// crsCode extracts an EPSG code from the crs field of a Collection body in any
// of its accepted forms, defaulting to 4326.
func TestCrsCode(t *testing.T) {
	cases := map[string]int{
		`["http://www.opengis.net/def/crs/EPSG/0/25832"]`: 25832,
		`"urn:ogc:def:crs:EPSG::3812"`:                    3812,
		`4326`:                                            4326,
		`"4258"`:                                          4258,
		`null`:                                            4326, // absent -> default CRS84
		`"not a crs"`:                                     4326,
	}
	for in, want := range cases {
		if got := crsCode([]byte(in)); got != want {
			t.Errorf("crsCode(%s) = %d, want %d", in, got, want)
		}
	}
}

// validID guards the collection id interpolated into CREATE/DROP TABLE.
func TestValidID(t *testing.T) {
	good := []string{"ships", "drones", "fleet_2026", "_tmp", "a"}
	bad := []string{"", "Ships", "bad-name", "drop table", "f leet", "2026fleet", "x;y", `a"b`}
	for _, s := range good {
		if !validID.MatchString(s) {
			t.Errorf("validID rejected a valid id %q", s)
		}
	}
	for _, s := range bad {
		if validID.MatchString(s) {
			t.Errorf("validID accepted an unsafe id %q", s)
		}
	}
}

// propsExpr / featCols switch the SQL between the generic JSONB properties
// column and the typed ships columns.
func TestSchemaMode(t *testing.T) {
	if propsExpr(true) != "coalesce(properties,'{}'::jsonb)" {
		t.Errorf("generic propsExpr wrong: %s", propsExpr(true))
	}
	if propsExpr(false) != "jsonb_build_object('mmsi',mmsi,'name',name)" {
		t.Errorf("typed propsExpr wrong: %s", propsExpr(false))
	}
	if featCols(true) != "id, properties" || featCols(false) != "id, mmsi, name" {
		t.Errorf("featCols wrong: %q / %q", featCols(true), featCols(false))
	}
}

// propsJSON serialises a feature's properties for the generic jsonb column.
func TestPropsJSON(t *testing.T) {
	if got := propsJSON(nil); got != "{}" {
		t.Errorf("propsJSON(nil) = %q, want {}", got)
	}
	if got := propsJSON(map[string]any{"model": "X500"}); got != `{"model":"X500"}` {
		t.Errorf("propsJSON = %q", got)
	}
}
