// Command codegen writes catalog_gen.go, the tier's table of MEOS temporal
// types, by reading the MEOS sources that define them.
//
// The tier needs five facts about each temporal type: its name, its base type,
// the token asMFJSON writes for it, whether it is spatial, and whether MEOS
// interpolates it linearly. MEOS states all five in C, and every one of them
// moves when a family is added, so a copy of them here is a copy that goes
// stale in silence. The generator reads the definitions themselves:
//
//	meos/src/temporal/meos_catalog.c  the type names, the base type and bounding
//	                                  box of each temporal type, and the
//	                                  predicates temporal_type, tnumber_type,
//	                                  tspatial_type and temptype_supports_linear
//	meos/src/temporal/type_out.c      temptype_as_mfjson_sb, the switch that
//	                                  writes each type's MF-JSON type token
//
// A type MEOS carries that this switch does not name has no MF-JSON form, and
// the empty token says so rather than a list kept here.
//
// Usage:
//
//	go run ./tools/codegen -mobilitydb <checkout> -o catalog_gen.go
//
// The output is committed, and CI regenerates it against the MobilityDB
// checkout it already has and refuses a difference, so a type MEOS gains
// reaches this tier as a failing check rather than as silence.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/format"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

func main() {
	mdb := flag.String("mobilitydb", "mobilitydb", "a MobilityDB checkout")
	out := flag.String("o", "catalog_gen.go", "file to write")
	flag.Parse()

	catalog, err := readSource(filepath.Join(*mdb, "meos", "src", "temporal", "meos_catalog.c"))
	if err != nil {
		fail(err)
	}
	typeOut, err := readSource(filepath.Join(*mdb, "meos", "src", "temporal", "type_out.c"))
	if err != nil {
		fail(err)
	}

	types, err := build(catalog, typeOut)
	if err != nil {
		fail(err)
	}

	src, err := render(types)
	if err != nil {
		fail(err)
	}
	if err := os.WriteFile(*out, src, 0o644); err != nil {
		fail(err)
	}
	fmt.Fprintf(os.Stderr, "codegen: %d temporal types written to %s\n", len(types), *out)
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "codegen:", err)
	os.Exit(1)
}

// temporalType is one row of the table.
type temporalType struct {
	Enum    string // the MeosType enumerator, e.g. T_TGEOMPOINT
	Name    string // the MEOS type name, e.g. tgeompoint
	Base    string // its base type's name, e.g. geometry
	Box     string // its bounding box type's name, e.g. stbox
	MFJSON  string // the token asMFJSON writes, e.g. MovingPoint
	Spatial bool
	Number  bool
	Linear  bool // MEOS interpolates it linearly between samples
}

// readSource returns the file with its C comments blanked, so a type
// enumerator named in prose is never read as a member of a list.
func readSource(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return stripComments(string(b)), nil
}

// stripComments replaces comment bodies with spaces, keeping every newline so
// the line structure survives. A comment opener inside a string literal is
// part of the string, so literals are stepped over rather than scanned.
func stripComments(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); {
		switch {
		case strings.HasPrefix(s[i:], "/*"):
			end := strings.Index(s[i+2:], "*/")
			if end < 0 {
				b.WriteString(blank(s[i:]))
				i = len(s)
				continue
			}
			seg := s[i : i+2+end+2]
			b.WriteString(blank(seg))
			i += len(seg)
		case strings.HasPrefix(s[i:], "//"):
			end := strings.IndexByte(s[i:], '\n')
			if end < 0 {
				end = len(s) - i
			}
			b.WriteString(blank(s[i : i+end]))
			i += end
		case s[i] == '"' || s[i] == '\'':
			n := literal(s, i)
			b.WriteString(s[i:n])
			i = n
		default:
			b.WriteByte(s[i])
			i++
		}
	}
	return b.String()
}

// literal returns the index just past the string or character literal starting
// at i, honouring backslash escapes.
func literal(s string, i int) int {
	quote := s[i]
	for j := i + 1; j < len(s); j++ {
		switch s[j] {
		case '\\':
			j++
		case quote:
			return j + 1
		}
	}
	return len(s)
}

// blank returns s with every byte other than a newline as a space.
func blank(s string) string {
	out := []byte(s)
	for i, c := range out {
		if c != '\n' {
			out[i] = ' '
		}
	}
	return string(out)
}

var (
	reName    = regexp.MustCompile(`\[(T_\w+)\]\s*=\s*"([^"]+)"`)
	reRelRow  = regexp.MustCompile(`\[(T_\w+)\]\s*=\s*\{([^}]*)\}`)
	reBase    = regexp.MustCompile(`\.temptype_basetype\s*=\s*(T_\w+)`)
	reBox     = regexp.MustCompile(`\.type_bboxtype\s*=\s*(T_\w+)`)
	reEnum    = regexp.MustCompile(`\bT_[A-Z0-9_]+\b`)
	reCase    = regexp.MustCompile(`\bcase\s+(T_\w+)\s*:`)
	reMFToken = regexp.MustCompile(`\{\\"type\\":\\"(\w+)\\"`)
)

func build(catalog, typeOut string) ([]temporalType, error) {
	names := map[string]string{}
	for _, m := range reName.FindAllStringSubmatch(catalog, -1) {
		names[m[1]] = m[2]
	}
	if len(names) == 0 {
		return nil, fmt.Errorf(`meos_catalog.c names no type: the [T_X] = "x" table is not where it is read from`)
	}

	base, box := map[string]string{}, map[string]string{}
	for _, m := range reRelRow.FindAllStringSubmatch(catalog, -1) {
		if b := reBase.FindStringSubmatch(m[2]); b != nil {
			base[m[1]] = b[1]
		}
		if b := reBox.FindStringSubmatch(m[2]); b != nil {
			box[m[1]] = b[1]
		}
	}

	temporal, err := predicate(catalog, "temporal_type")
	if err != nil {
		return nil, err
	}
	number, err := predicate(catalog, "tnumber_type")
	if err != nil {
		return nil, err
	}
	spatial, err := predicate(catalog, "tspatial_type")
	if err != nil {
		return nil, err
	}
	linear, err := predicate(catalog, "temptype_supports_linear")
	if err != nil {
		return nil, err
	}

	mfjson, err := mfjsonTokens(typeOut)
	if err != nil {
		return nil, err
	}

	var out []temporalType
	for enum := range temporal {
		name, ok := names[enum]
		if !ok {
			return nil, fmt.Errorf("%s is a temporal type the name table does not name", enum)
		}
		b, ok := base[enum]
		if !ok {
			return nil, fmt.Errorf("%s is a temporal type with no temptype_basetype", enum)
		}
		baseName, ok := names[b]
		if !ok {
			return nil, fmt.Errorf("%s has base type %s, which the name table does not name", enum, b)
		}
		boxName := ""
		if x, ok := box[enum]; ok {
			boxName = names[x]
		}
		out = append(out, temporalType{
			Enum:    enum,
			Name:    name,
			Base:    baseName,
			Box:     boxName,
			MFJSON:  mfjson[enum],
			Spatial: spatial[enum],
			Number:  number[enum],
			Linear:  linear[enum],
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// predicate returns the type enumerators named in the body of a boolean
// predicate such as tspatial_type. The body is taken by counting braces from
// the function's own opening brace, never by matching a pattern across it: a
// nested block ends a pattern early and silently shortens the list.
func predicate(src, fn string) (map[string]bool, error) {
	body, err := functionBody(src, fn)
	if err != nil {
		return nil, err
	}
	set := map[string]bool{}
	for _, m := range reEnum.FindAllString(body, -1) {
		set[m] = true
	}
	if len(set) == 0 {
		return nil, fmt.Errorf("%s names no type enumerator", fn)
	}
	return set, nil
}

// functionBody returns the braced body of the named function definition, which
// is the occurrence of the name at the start of a line followed by its
// parameter list: MEOS writes a definition's return type on the line above, so
// a definition is the only occurrence in that column.
func functionBody(src, fn string) (string, error) {
	re := regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(fn) + `\s*\(`)
	loc := re.FindStringIndex(src)
	if loc == nil {
		return "", fmt.Errorf("no definition of %s", fn)
	}
	open := strings.IndexByte(src[loc[1]:], '{')
	if open < 0 {
		return "", fmt.Errorf("%s has no body", fn)
	}
	start := loc[1] + open
	depth := 0
	for i := start; i < len(src); {
		switch src[i] {
		case '"', '\'':
			// A brace inside a string literal is text, not structure: the
			// MF-JSON switch writes `{\"type\":\"MovingFloat\",` for every
			// type, and counting those opens a depth that never closes.
			i = literal(src, i)
			continue
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return src[start+1 : i], nil
			}
		}
		i++
	}
	return "", fmt.Errorf("%s has an unterminated body", fn)
}

// mfjsonTokens reads temptype_as_mfjson_sb, whose case labels come in groups:
// several types share one token, since a geometry point and a geography point
// are both a MovingPoint. Labels accumulate until a token is written and are
// assigned together.
func mfjsonTokens(typeOut string) (map[string]string, error) {
	body, err := functionBody(typeOut, "temptype_as_mfjson_sb")
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	var pending []string
	for _, line := range strings.Split(body, "\n") {
		if m := reCase.FindStringSubmatch(line); m != nil {
			pending = append(pending, m[1])
			continue
		}
		if m := reMFToken.FindStringSubmatch(line); m != nil {
			for _, enum := range pending {
				out[enum] = m[1]
			}
			pending = nil
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("temptype_as_mfjson_sb names no type token")
	}
	return out, nil
}

func render(types []temporalType) ([]byte, error) {
	var b bytes.Buffer
	b.WriteString(`// Code generated by tools/codegen from the MEOS sources. DO NOT EDIT.
//
// Regenerate with:
//
//	go run ./tools/codegen -mobilitydb <checkout> -o catalog_gen.go
//
// The sources read are meos/src/temporal/meos_catalog.c (the type names, each
// temporal type's base and bounding box, and the predicates temporal_type,
// tnumber_type, tspatial_type and temptype_supports_linear) and
// meos/src/temporal/type_out.c (temptype_as_mfjson_sb).

package main

// TemporalType is one MEOS temporal type, as MEOS's own catalog describes it.
// MFJSON is empty for a type asMFJSON does not write, so an empty token is what
// says a type has no MF-JSON form.
type TemporalType struct {
	Name    string
	Base    string
	Box     string
	MFJSON  string
	Spatial bool
	Number  bool
	Linear  bool
}

// temporalTypes is every temporal type MEOS declares, ordered by name.
var temporalTypes = []TemporalType{
`)
	for _, t := range types {
		fmt.Fprintf(&b, "\t{Name: %q, Base: %q, Box: %q, MFJSON: %q, Spatial: %t, Number: %t, Linear: %t},\n",
			t.Name, t.Base, t.Box, t.MFJSON, t.Spatial, t.Number, t.Linear)
	}
	b.WriteString(`}

// temporalTypeByName indexes temporalTypes by the MEOS type name.
var temporalTypeByName = func() map[string]*TemporalType {
	m := make(map[string]*TemporalType, len(temporalTypes))
	for i := range temporalTypes {
		m[temporalTypes[i].Name] = &temporalTypes[i]
	}
	return m
}()
`)
	return format.Source(b.Bytes())
}
