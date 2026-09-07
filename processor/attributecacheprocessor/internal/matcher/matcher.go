// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package matcher defines the column-level matcher and the match-engine
// abstractions used to look up rows from the cache. Concrete implementations
// (linear engine, optimized trie engine, per-match-type column matchers) land
// in follow-up tasks.
package matcher // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributecacheprocessor/internal/matcher"

// MatchType identifies the matching algorithm for a single match column.
type MatchType string

// Supported MatchType values.
const (
	MatchTypeString     MatchType = "string"
	MatchTypeRegex      MatchType = "regex"
	MatchTypeSQLPattern MatchType = "sqlpattern"
	MatchTypeRange      MatchType = "range"
)

// ColumnConfig is the per-column schema entry consumed by MatchEngine
// constructors. It is a lightweight mirror of the user-facing column
// configuration; the processor copies match-relevant fields here when it
// builds an engine.
type ColumnConfig struct {
	// Name is the column name as declared in the source data.
	Name string
	// MatchType selects the matching algorithm for this column. Ignored for
	// columns that are not used as match columns.
	MatchType MatchType
}

// MatchConfig carries the full column schema and symbol configuration for a
// single MatchEngine instance. Symbol values are captured at engine
// construction time so that Match calls do not need to read shared state.
type MatchConfig struct {
	// Columns lists every match column in evaluation order.
	Columns []ColumnConfig
	// DefaultSymbol is the cell value that signals "fallback when no more
	// specific row matches" (e.g. "**").
	DefaultSymbol string
	// MatchAllSymbol is the cell value that always matches, even when the
	// incoming attribute is absent (e.g. "%%").
	MatchAllSymbol string
	// NullSymbol is the cell value that matches only when the incoming
	// attribute is absent (e.g. "@@").
	NullSymbol string
}

// ColumnMatcher evaluates whether a single incoming attribute value matches a
// single cell value from the lookup table.
type ColumnMatcher interface {
	// Matches reports whether attributeValue (with present=false meaning the
	// attribute was absent on the item) satisfies the predicate represented
	// by cellValue.
	Matches(cellValue, attributeValue string, present bool) bool
}

// Row is the result type returned by MatchEngine.Match. The concrete type
// (alias of cache.Row) is wired in a follow-up task; declaring it as
// map[string]string here lets the rest of the package compile without a
// circular dependency on internal/cache during the skeleton phase.
type Row = map[string]string

// LookupTable is the read-only snapshot consumed by MatchEngine
// implementations. The full type lives in internal/cache; the matcher package
// only needs an opaque pointer for its interface signature.
type LookupTable struct {
	Columns []string
	Rows    []Row
}

// MatchEngine evaluates an incoming attribute set against a LookupTable and
// returns the best-matching Row, or nil when no row matches.
type MatchEngine interface {
	// Match finds the best row for the given attribute map. Returns nil when
	// no row matches. Implementations must be safe for concurrent reads.
	Match(table *LookupTable, attrs map[string]string) *Row
}
