package main

import (
	"encoding/json"
	"net/http/httptest"
	"regexp"
	"testing"
)

// rfc3339Shape is what a JSON Schema `format: date-time` admits: 'T' between the date
// and the time, and an offset of hours AND minutes, or Z.
var rfc3339Shape = regexp.MustCompile(
	`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?([+-]\d{2}:\d{2}|Z)$`)

func TestATSTimestampIsRFC3339(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		// What PostgreSQL returns: a space, and an offset abbreviated to the hour.
		{"2026-01-01 09:00:00+01", "2026-01-01T09:00:00+01:00"},
		{"2026-01-01 08:00:00+00", "2026-01-01T08:00:00Z"},
		{"2000-01-01 00:00:00-08", "2000-01-01T00:00:00-08:00"},
		// Sub-second precision survives; a whole second carries no fraction.
		{"2026-01-01 09:00:00.123456+01", "2026-01-01T09:00:00.123456+01:00"},
		// Already conformant, and left alone.
		{"2026-01-01T09:00:00+01:00", "2026-01-01T09:00:00+01:00"},
		// A half-hour offset, where the abbreviation never applied.
		{"2026-01-01 09:00:00+05:30", "2026-01-01T09:00:00+05:30"},
		// Nothing to render.
		{"", ""},
	} {
		if got := rfc3339(c.in); got != c.want {
			t.Errorf("rfc3339(%q) = %q, want %q", c.in, got, c.want)
		}
		if c.want != "" && !rfc3339Shape.MatchString(c.want) {
			t.Errorf("the expectation %q is not itself RFC 3339", c.want)
		}
	}
}

// An unparsable value is published as it stands, so a document carrying it fails
// validation loudly instead of carrying a substituted instant.
func TestATSTimestampLeavesTheUnrecognisedAlone(t *testing.T) {
	for _, s := range []string{"not a timestamp", "infinity", "-infinity"} {
		if got := rfc3339(s); got != s {
			t.Errorf("rfc3339(%q) = %q, want it unchanged", s, got)
		}
	}
}

// The Collection document's temporal extent carries the shape, which is where the
// schema reads it.
func TestATSCollectionExtentIntervalIsRFC3339(t *testing.T) {
	withBackend(atsCollectionsBackend(), func() {
		rec := httptest.NewRecorder()
		r := httptest.NewRequest("GET", "/collections/ships", nil)
		r.SetPathValue("cid", "ships")
		getCollection(rec, r)
		var col struct {
			Extent struct {
				Temporal struct {
					Interval [][]string `json:"interval"`
				} `json:"temporal"`
			} `json:"extent"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &col); err != nil {
			t.Fatal(err)
		}
		if len(col.Extent.Temporal.Interval) == 0 {
			t.Fatal("the collection states no temporal extent")
		}
		for _, iv := range col.Extent.Temporal.Interval {
			for _, ts := range iv {
				if !rfc3339Shape.MatchString(ts) {
					t.Errorf("extent interval carries %q, which `format: date-time` does not admit", ts)
				}
			}
		}
	})
}
