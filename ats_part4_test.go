// The requirements of the class cquery of OGC API – Moving Features – Part 4
// (Stream Extension), as executable rows beside the abstract tests of Part 1.
//
// Part 4 is an unpublished OGC draft that no public repository of OGC holds, and
// what it states no abstract test of; its requirements are the rows, each keyed
// by the identifier the draft gives it and read from CONFORMANCE.md, which
// records them from the draft MobilityAPI implements. A row names the test that
// reads the answer it states, as #atsDischargedBy does for Part 1.
//
// ⛔ THE ROWS FOLLOW THE DRAFT MOBILITYAPI READ. The public summary of a newer
// draft, the FOSS4G workshop of the Moving Features SWG in opengeospatial/mf-swg,
// describes a Continuous Query resource with id, pubtime, target, filter and
// queryResult; once that draft is at hand, these rows and CONFORMANCE.md follow it.
package main

import (
	"bytes"
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

// atsPart4Req is one requirement of the class cquery.
type atsPart4Req struct {
	id           string // the draft's identifier, as CONFORMANCE.md writes it
	number       int    // the draft's requirement number
	dischargedBy string // the test that reads the answer it states
}

// atsPart4 is the class cquery, its requirements in the draft's order.
var atsPart4 = []atsPart4Req{
	{"cquery_link", 1, "TestCqueryLinkShape"},
	{"window", 2, "TestMeosWindowBounds"},
	{"window_tumbling", 3, "TestMeosTumblingWindow"},
	{"window_hopping", 4, "TestMeosHoppingWindow"},
	{"window_count", 5, "TestMeosWindowAggregate"},
}

// conformanceReq matches a requirement row of CONFORMANCE.md: its number, its
// identifier and the cell naming where it is met.
var conformanceReq = regexp.MustCompile("(?m)^\\| \\*\\*Req (\\d+)\\*\\* `([a-z_]+)`[^|]*\\|[^|]*\\|([^|]*)\\|$")

// The rows are the requirements CONFORMANCE.md records, each with its number and
// identifier, and the test a row names is the one CONFORMANCE.md names for it.
func TestATSPart4RowsAreTheRecordedRequirements(t *testing.T) {
	doc, err := os.ReadFile("CONFORMANCE.md")
	if err != nil {
		t.Fatal(err)
	}
	recorded := conformanceReq.FindAllSubmatch(doc, -1)
	if len(recorded) == 0 {
		t.Fatal("CONFORMANCE.md records no requirement row the pattern reads, so this test would assert nothing")
	}
	if len(recorded) != len(atsPart4) {
		t.Errorf("CONFORMANCE.md records %d requirements of cquery, the rows hold %d", len(recorded), len(atsPart4))
	}
	for i, m := range recorded {
		if i >= len(atsPart4) {
			break
		}
		row := atsPart4[i]
		if string(m[1]) != itoa(row.number) || string(m[2]) != row.id {
			t.Errorf("row %d is Req %d %s, CONFORMANCE.md records Req %s %s", i, row.number, row.id, m[1], m[2])
		}
		if !bytes.Contains(m[3], []byte("`"+row.dischargedBy+"`")) {
			t.Errorf("Req %d %s names %s, which CONFORMANCE.md does not name where it is met: %s",
				row.number, row.id, row.dischargedBy, bytes.TrimSpace(m[3]))
		}
	}
}

// Every row names a test a test file declares, whatever build tag it runs under:
// the window requirements are read through the MEOS engine, which the conformance
// job builds with the meos tag.
func TestATSPart4RowsAreDischarged(t *testing.T) {
	files, err := filepath.Glob("*_test.go")
	if err != nil {
		t.Fatal(err)
	}
	var src []byte
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		src = append(src, b...)
	}
	for _, r := range atsPart4 {
		if !bytes.Contains(src, []byte("func "+r.dischargedBy+"(")) {
			t.Errorf("Req %d %s names %s, which no test file declares", r.number, r.id, r.dischargedBy)
		}
	}
}

// The class the rows belong to is one the service declares.
func TestATSPart4ClassIsDeclared(t *testing.T) {
	if !declaredClasses(t)[confCQuery] {
		t.Errorf("the conformance declaration does not list %s, whose requirements these rows are", confCQuery)
	}
}
