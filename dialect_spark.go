// Canonical-to-Spark SQL translation. The tier emits canonical MEOS-API
// function names (identity on PostgreSQL/MobilityDB and MobilityDuck); for
// MobilitySpark they are the deterministic per-binding remap of those same
// canonical names (camelCase Spark idiom). The map is seeded from the
// registered MobilitySpark surface and is regenerated from the MEOS-API
// registry at the firm pin. Spark Connect's Sql takes no bind parameters, so
// $N placeholders are inlined here.
package main

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// sparkNameMap holds only the canonical names whose Spark idiom differs; every
// other function (sequenceN, numSequences, atTime, speed, cumulativeLength,
// eIntersects, trajectory, setSRID, tgeompoint, …) is identity.
var sparkNameMap = map[string]string{
	"asMFJSON":             "temporalAsMfjson",
	"tgeompointFromMFJSON": "tgeompointFromMfjson",
	"Xmin":                 "stboxXmin",
	"Ymin":                 "stboxYmin",
	"Xmax":                 "stboxXmax",
	"Ymax":                 "stboxYmax",
	"Tmin":                 "stboxTmin",
	"Tmax":                 "stboxTmax",
	"azimuth":              "tpointAzimuth",
}

var sparkFnCall = func() *regexp.Regexp {
	keys := make([]string, 0, len(sparkNameMap))
	for k := range sparkNameMap {
		keys = append(keys, regexp.QuoteMeta(k))
	}
	return regexp.MustCompile(`\b(` + strings.Join(keys, "|") + `)\s*\(`)
}()

// sparkIdent matches a double-quoted SQL identifier. The tier quotes identifiers
// Postgres-style ("name"); Spark SQL reads "..." as a string literal and quotes
// identifiers with backticks. All string literals reach Spark single-quoted (see
// sparkLiteral), so every double-quoted token is an identifier.
var sparkIdent = regexp.MustCompile(`"([^"]+)"`)

// sparkTextCast maps the SQL `text`/`jsonb` cast targets to Spark's `string`
// (Spark has neither type); the tier casts box accessors with `CAST(... AS text)`
// and the generic properties with `CAST(... AS jsonb)`.
var sparkTextCast = regexp.MustCompile(`(?i)\bAS\s+(text|jsonb)\b`)

// rewriteSparkSQL remaps the canonical function names that differ in the Spark
// idiom (function-call positions only, name followed by "(") and rewrites
// double-quoted identifiers to Spark's backtick quoting.
func rewriteSparkSQL(sql string) string {
	sql = sparkFnCall.ReplaceAllStringFunc(sql, func(m string) string {
		name := strings.TrimRight(m[:len(m)-1], " \t")
		return sparkNameMap[name] + "("
	})
	sql = sparkIdent.ReplaceAllString(sql, "`$1`")
	return sparkTextCast.ReplaceAllString(sql, "AS string")
}

// inlineParams substitutes $N placeholders with literals (Spark Connect Sql has
// no bind parameters). Numbers inline directly; everything else is a quoted,
// escaped string literal.
func inlineParams(sql string, args []any) string {
	for i := len(args); i >= 1; i-- { // high-to-low so $1 does not match $10
		sql = strings.ReplaceAll(sql, "$"+strconv.Itoa(i), sparkLiteral(args[i-1]))
	}
	return sql
}

func sparkLiteral(v any) string {
	switch x := v.(type) {
	case nil:
		return "NULL"
	case int:
		return strconv.Itoa(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64)
	case string:
		return "'" + strings.ReplaceAll(x, "'", "''") + "'"
	default:
		return "'" + strings.ReplaceAll(fmt.Sprintf("%v", v), "'", "''") + "'"
	}
}
