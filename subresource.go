// OGC temporal sub-resource assembly in the tier: the TemporalGeometrySequence
// envelope and the temporalProperty valueSequence reshaping. The engine returns
// the canonical asMFJSON outputs; their reshaping into the OGC objects is JSON
// framing, assembled here once for every backend.
package main

import (
	"encoding/json"
	"strconv"
)

type tPropDoc struct {
	Name          string                       `json:"name"`
	Type          string                       `json:"type"`
	Form          string                       `json:"form"`
	Description   string                       `json:"description"`
	ValueSequence []map[string]json.RawMessage `json:"valueSequence"`
	Links         []ogcLink                    `json:"links"`
}

// reshapeTemporalProperty turns a value's asMFJSON (continuous values under
// "sequences" or a discrete top-level datetimes/values object) into an OGC
// temporalProperty, keeping each segment's interpolation verbatim.
func reshapeTemporalProperty(name, typ, form, desc, self string, mfjson []byte) ([]byte, error) {
	var j map[string]json.RawMessage
	if err := json.Unmarshal(mfjson, &j); err != nil {
		return nil, err
	}
	interp := j["interpolation"]
	seg := func(m map[string]json.RawMessage) map[string]json.RawMessage {
		return map[string]json.RawMessage{
			"datetimes": m["datetimes"], "values": m["values"], "interpolation": interp,
			"lower_inc": m["lower_inc"], "upper_inc": m["upper_inc"]}
	}
	vs := []map[string]json.RawMessage{}
	if raw, ok := j["sequences"]; ok {
		var arr []map[string]json.RawMessage
		if err := json.Unmarshal(raw, &arr); err != nil {
			return nil, err
		}
		for _, s := range arr {
			vs = append(vs, seg(s))
		}
	} else if _, ok := j["datetimes"]; ok {
		vs = append(vs, seg(j))
	}
	return json.Marshal(tPropDoc{Name: name, Type: typ, Form: form, Description: desc,
		ValueSequence: vs, Links: []ogcLink{{Rel: "self", Href: self}}})
}

type tgSeqDoc struct {
	Type             string            `json:"type"`
	GeometrySequence []json.RawMessage `json:"geometrySequence"`
	Links            []ogcLink         `json:"links"`
}

// buildTGSequence assembles the TemporalGeometrySequence: each member is the
// asMFJSON of one sequence with its 1-based id, addressable as {tGeometryId}.
func buildTGSequence(self string, ns []int, mfjsons [][]byte) ([]byte, error) {
	elems := []json.RawMessage{}
	for i, n := range ns {
		var m map[string]json.RawMessage
		if err := json.Unmarshal(mfjsons[i], &m); err != nil {
			return nil, err
		}
		m["id"] = json.RawMessage(strconv.Itoa(n))
		b, err := json.Marshal(m)
		if err != nil {
			return nil, err
		}
		elems = append(elems, b)
	}
	return json.Marshal(tgSeqDoc{Type: "TemporalGeometrySequence",
		GeometrySequence: elems, Links: []ogcLink{{Rel: "self", Href: self}}})
}
