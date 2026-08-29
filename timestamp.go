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

import "time"

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
			return t.Format(time.RFC3339Nano)
		}
	}
	return s
}
