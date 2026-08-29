// The Annex A `-success` tests: does the returned DOCUMENT carry what the
// standard requires? They run against a scripted backend (ats_backend_test.go),
// so they exercise the real handler and the real serialisation without a
// database, and they run in the same job as every other test.
//
// ⛔ WHERE ANNEX A AND THE NORMATIVE SCHEMA DISAGREE, THE SCHEMA DECIDES, and
// the disagreement is recorded rather than silently resolved. Annex A's
// tgsequence-get-success step 1 demands `type` = "MovingGeometryCollection"
// with a `prism` array, while openapi/schemas/temporalGeometrySequence.yaml --
// the schema that same abstract test's step 8 points at -- requires `type`
// enum "TemporalGeometrySequence" and `geometrySequence`. The prose carries the
// older MF-JSON Prism vocabulary; the schema is what a validator runs. Asserting
// the prose would fail a tier that is correct.
package main

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
)

// A collection row as the handlers read it: id, title, description, item_type, crs.
func atsCollectionsBackend() *fakeBackend {
	return &fakeBackend{answers: []fakeAnswer{
		{match: "FROM collections ORDER BY id",
			rows: [][]any{{"ships", "Ships", "AIS vessels", "movingfeature", 4326}}},
		{match: "SELECT id, crs FROM collections WHERE id=",
			rows: [][]any{{"ships", 4326}}},
		{match: "SELECT title,description,item_type FROM collections WHERE id=",
			rows: [][]any{{"Ships", "AIS vessels", "movingfeature"}}},
		{match: "SELECT Xmin(e)",
			rows: [][]any{{3.0, 50.0, 8.0, 56.0, "2026-01-01T00:00:00+00", "2026-01-02T00:00:00+00"}}},
	}}
}

// /conf/mf-collection/collections-get-success — the Collections document, whose
// step 3 is explicit: an itemType property whose value is 'movingfeature'.
func TestATSCollectionsGetSuccess(t *testing.T) {
	withBackend(atsCollectionsBackend(), func() {
		rec := httptest.NewRecorder()
		listCollections(rec, httptest.NewRequest("GET", "/collections", nil))
		if rec.Code != 200 {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		var doc struct {
			Collections []struct {
				ID       string   `json:"id"`
				ItemType *string  `json:"itemType"`
				CRS      []string `json:"crs"`
				Links    []struct {
					Rel  string `json:"rel"`
					Href string `json:"href"`
				} `json:"links"`
			} `json:"collections"`
			Links []struct {
				Rel string `json:"rel"`
			} `json:"links"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &doc); err != nil {
			t.Fatal(err)
		}
		if len(doc.Collections) == 0 {
			t.Fatal("the Collections document carries no collection")
		}
		for _, c := range doc.Collections {
			if c.ItemType == nil || *c.ItemType != "movingfeature" {
				got := "absent"
				if c.ItemType != nil {
					got = *c.ItemType
				}
				t.Errorf("collection %q itemType = %s, want movingfeature", c.ID, got)
			}
			if len(c.CRS) == 0 {
				t.Errorf("collection %q declares no crs", c.ID)
			}
			var self bool
			for _, l := range c.Links {
				if l.Rel == "self" && l.Href != "" {
					self = true
				}
			}
			if !self {
				t.Errorf("collection %q carries no self link", c.ID)
			}
		}
		if len(doc.Links) == 0 {
			t.Error("the Collections document carries no links")
		}
	})
}

// /conf/mf-collection/collection-get-success — one Collection, with the
// identity, crs and links the resource requires.
func TestATSCollectionGetSuccess(t *testing.T) {
	withBackend(atsCollectionsBackend(), func() {
		req := httptest.NewRequest("GET", "/collections/ships", nil)
		req.SetPathValue("cid", "ships")
		rec := httptest.NewRecorder()
		getCollection(rec, req)
		if rec.Code != 200 {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		var col struct {
			ID       string   `json:"id"`
			ItemType string   `json:"itemType"`
			CRS      []string `json:"crs"`
			Links    []struct {
				Rel string `json:"rel"`
			} `json:"links"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &col); err != nil {
			t.Fatal(err)
		}
		if col.ID != "ships" {
			t.Errorf("id = %q, want ships", col.ID)
		}
		if col.ItemType != "movingfeature" {
			t.Errorf("itemType = %q, want movingfeature", col.ItemType)
		}
		if len(col.CRS) == 0 || len(col.Links) == 0 {
			t.Errorf("crs=%v links=%d, both are required", col.CRS, len(col.Links))
		}
	})
}

// /conf/movingfeatures/tproperties-get-success — every TemporalProperty carries
// a name and a type, and the type is one of the values the standard predefines.
func TestATSTPropertiesGetSuccess(t *testing.T) {
	predefined := map[string]bool{
		"TBoolean": true, "TText": true, "TInteger": true, "TReal": true, "TImage": true,
	}
	f := atsCollectionsBackend()
	f.answers = append(f.answers,
		fakeAnswer{match: "SELECT 1 FROM", rows: [][]any{{1}}},
		fakeAnswer{match: "to_regclass('mf_tproperty')", rows: [][]any{{"mf_tproperty"}}},
		fakeAnswer{match: "FROM mf_tproperty WHERE", rows: [][]any{
			{"speed", "TReal", "km/h", "speed over ground"},
			{"label", "TText", "", "vessel label"},
		}},
	)
	withBackend(f, func() {
		req := httptest.NewRequest("GET", "/collections/ships/items/1/tproperties", nil)
		req.SetPathValue("cid", "ships")
		req.SetPathValue("fid", "1")
		rec := httptest.NewRecorder()
		listTProperties(rec, req)
		if rec.Code != 200 {
			t.Fatalf("status = %d, want 200 (body %s)", rec.Code, rec.Body.String())
		}
		var doc struct {
			TemporalProperties []struct {
				Name string `json:"name"`
				Type string `json:"type"`
			} `json:"temporalProperties"`
			NumberReturned *int `json:"numberReturned"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &doc); err != nil {
			t.Fatal(err)
		}
		if len(doc.TemporalProperties) == 0 {
			t.Fatal("the TemporalProperties document carries no property")
		}
		for _, p := range doc.TemporalProperties {
			if p.Name == "" {
				t.Error("a temporal property carries no name")
			}
			if !predefined[p.Type] {
				t.Errorf("property %q type = %q, which is not one of TBoolean, TText, TInteger, TReal, TImage",
					p.Name, p.Type)
			}
		}
		// numberReturned is optional; when present it counts what was returned.
		if doc.NumberReturned != nil && *doc.NumberReturned != len(doc.TemporalProperties) {
			t.Errorf("numberReturned = %d, but %d properties are present",
				*doc.NumberReturned, len(doc.TemporalProperties))
		}
	})
}

// /conf/movingfeatures/tgsequence-get-success — the TemporalGeometrySequence
// document, asserted against openapi/schemas/temporalGeometrySequence.yaml:
// `type` is the enum's single value and `geometrySequence` is an array. See the
// file header for why the prose's MovingGeometryCollection/prism is not used.
func TestATSTGSequenceGetSuccess(t *testing.T) {
	const mfjson = `{"type":"MovingPoint","crs":{"type":"Name","properties":{"name":"urn:ogc:def:crs:EPSG::4326"}},` +
		`"coordinates":[[4.35,50.85],[4.36,50.86]],"datetimes":["2026-01-01T00:00:00+00","2026-01-01T00:10:00+00"],` +
		`"interpolation":"Linear"}`
	f := atsCollectionsBackend()
	f.answers = append(f.answers,
		fakeAnswer{match: "SELECT 1 FROM", rows: [][]any{{1}}},
		fakeAnswer{match: "SELECT numSequences(", rows: [][]any{{1}}},
		fakeAnswer{match: "SELECT asMFJSON(", rows: [][]any{{mfjson}}},
	)
	withBackend(f, func() {
		req := httptest.NewRequest("GET", "/collections/ships/items/1/tgsequence", nil)
		req.SetPathValue("cid", "ships")
		req.SetPathValue("fid", "1")
		rec := httptest.NewRecorder()
		tgSequence(rec, req)
		if rec.Code != 200 {
			t.Fatalf("status = %d, want 200 (body %s)", rec.Code, rec.Body.String())
		}
		var doc struct {
			Type             string            `json:"type"`
			GeometrySequence []json.RawMessage `json:"geometrySequence"`
			Links            []struct {
				Rel string `json:"rel"`
			} `json:"links"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &doc); err != nil {
			t.Fatal(err)
		}
		if doc.Type != "TemporalGeometrySequence" {
			t.Errorf("type = %q, want TemporalGeometrySequence (the schema's enum)", doc.Type)
		}
		if len(doc.GeometrySequence) == 0 {
			t.Error("geometrySequence is required and carries the TemporalPrimitiveGeometry items")
		}
		for i, m := range doc.GeometrySequence {
			var g map[string]any
			if err := json.Unmarshal(m, &g); err != nil {
				t.Errorf("geometrySequence[%d] is not an object: %v", i, err)
				continue
			}
			if _, ok := g["type"]; !ok {
				t.Errorf("geometrySequence[%d] carries no type", i)
			}
		}
	})
}
