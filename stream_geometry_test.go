package main

import "testing"

// parsePositionsMFJSON reads (timestamp, coordinates) positions from both the
// flat and the sequence-set MovingPoint MF-JSON forms.
func TestParsePositionsMFJSON(t *testing.T) {
	flat := `{"type":"MovingPoint","datetimes":["2026-02-26T08:00:00Z","2026-02-26T08:00:20Z"],
	          "coordinates":[[12.50,55.70],[12.52,55.71]]}`
	got, err := parsePositionsMFJSON([]byte(flat))
	if err != nil || len(got) != 2 {
		t.Fatalf("flat parse = %v, %v", got, err)
	}
	c0, _ := got[0]["coordinates"].([]any)
	if len(c0) != 2 || c0[0] != 12.50 || c0[1] != 55.70 {
		t.Errorf("first position coordinates wrong: %v", got[0]["coordinates"])
	}

	seqset := `{"type":"MovingPoint","sequences":[
	  {"datetimes":["2026-02-26T08:00:00Z"],"coordinates":[[12.50,55.70]]},
	  {"datetimes":["2026-02-26T08:00:20Z"],"coordinates":[[12.52,55.71]]}]}`
	got, err = parsePositionsMFJSON([]byte(seqset))
	if err != nil || len(got) != 2 {
		t.Fatalf("sequence-set parse = %v, %v", got, err)
	}

	if _, err := parsePositionsMFJSON([]byte(`{"type":"MovingPoint"}`)); err == nil {
		t.Error("expected an error for a document with no positions")
	}
}

// rfc3339Tz completes PostgreSQL's hour-only timezone offset so datetimes are
// RFC 3339 (and parse in strict clients such as JS Date), without disturbing
// already-complete offsets or non-timestamp digits.
func TestRfc3339Tz(t *testing.T) {
	cases := map[string]string{
		`"2026-02-26T18:57:52+00"`:              `"2026-02-26T18:57:52+00:00"`,
		`"2026-02-26T18:57:52-05"`:              `"2026-02-26T18:57:52-05:00"`,
		`"2026-02-26T18:57:52.5+00"`:            `"2026-02-26T18:57:52.5+00:00"`,
		`"a":"2026-02-26T18:57:52+00","b":"x"`:  `"a":"2026-02-26T18:57:52+00:00","b":"x"`,
		`"2026-02-26T18:57:52+00:00"`:           `"2026-02-26T18:57:52+00:00"`,
		`"2026-02-26T18:57:52Z"`:                `"2026-02-26T18:57:52Z"`,
		`{"bbox":[152625.05,5883457.97,12.50]}`: `{"bbox":[152625.05,5883457.97,12.50]}`,
	}
	for in, want := range cases {
		if got := rfc3339Tz(in); got != want {
			t.Errorf("rfc3339Tz(%q) = %q, want %q", in, got, want)
		}
	}
}
