// Rendering a timestamp into a document.
//
// Every `format: date-time` in the OGC schemas is defined against RFC 3339, whose
// section 5.6 writes the date and the time separated by 'T' and the UTC offset with
// hours AND minutes. PostgreSQL's text rendering of a timestamptz satisfies neither
// half: it separates with a space and abbreviates a whole-hour offset to the hour.
//
// ⛔ THE CONVERSION LIVES HERE, NOT IN THE SQL. The tier speaks one canonical SQL to
// three backends, and the expression that would fix this in PostgreSQL — `to_jsonb`
// with a path extraction, or a `to_char` format string — exists in neither of the
// other two. A timestamp arrives as text from all three and leaves as RFC 3339 from
// one place.
package main

import (
	"regexp"
	"time"
)

// pgTimestampLayouts are the shapes a backend returns a timestamptz in: a space or a
// 'T' separator, an offset of hours or of hours and minutes, and optional fractional
// seconds. Go matches a two-digit offset with `-07` and a full one with `-07:00`, so
// the two are separate layouts.
var pgTimestampLayouts = []string{
	"2006-01-02 15:04:05.999999-07",
	"2006-01-02 15:04:05.999999-07:00",
	"2006-01-02T15:04:05.999999-07",
	"2006-01-02T15:04:05.999999-07:00",
	"2006-01-02 15:04:05.999999Z07:00",
	"2006-01-02T15:04:05.999999Z07:00",
}

// rfc3339Layout writes an instant with the 'T' separator, its fractional seconds
// when it has any, and its offset in hours and minutes as the backend stated it:
// a UTC instant reads +00:00, as MobilityDB's MF-JSON writes it.
const rfc3339Layout = "2006-01-02T15:04:05.999999999-07:00"

// rfc3339 renders a timestamp a backend returned as text in the form the schemas
// require. A value it cannot parse is returned unchanged: the tier publishes what the
// database holds rather than substituting a guess, and a document carrying an
// unrecognised timestamp fails validation loudly instead of silently carrying a
// different instant.
func rfc3339(s string) string {
	if s == "" {
		return s
	}
	for _, layout := range pgTimestampLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t.Format(rfc3339Layout)
		}
	}
	return s
}

// timestampInJSON matches a JSON string holding a timestamp in any shape
// #pgTimestampLayouts reads.
var timestampInJSON = regexp.MustCompile(
	`"(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}(?::\d{2})?))"`)

// rfc3339In renders every timestamp a JSON document holds through #rfc3339, so a
// document MobilityDB writes (MF-JSON) and a value the tier reads from a column
// leave in one form.
func rfc3339In(doc string) string {
	return timestampInJSON.ReplaceAllStringFunc(doc, func(q string) string {
		return `"` + rfc3339(q[1:len(q)-1]) + `"`
	})
}
