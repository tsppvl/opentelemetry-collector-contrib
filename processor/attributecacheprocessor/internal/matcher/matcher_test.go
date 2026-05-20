// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package matcher

import (
	"testing"
)

const (
	symMatchAll = "%%"
	symNull     = "@@"
	symDefault  = "**"
)

func baseCfg(cols ...ColumnConfig) MatchConfig {
	return MatchConfig{
		Columns:        cols,
		DefaultSymbol:  symDefault,
		MatchAllSymbol: symMatchAll,
		NullSymbol:     symNull,
	}
}

// --- StringMatcher ----------------------------------------------------------

func TestStringMatcher_exact(t *testing.T) {
	m := NewStringMatcher(baseCfg())
	if !m.Matches("foo", "foo", true) {
		t.Fatal("expected match for equal values")
	}
	if m.Matches("foo", "bar", true) {
		t.Fatal("expected no-match for unequal values")
	}
	if m.Matches("foo", "", false) {
		t.Fatal("expected no-match for absent attr")
	}
}

func TestStringMatcher_matchAll(t *testing.T) {
	m := NewStringMatcher(baseCfg())
	if !m.Matches(symMatchAll, "anything", true) {
		t.Fatal("matchAll should match present")
	}
	if !m.Matches(symMatchAll, "", false) {
		t.Fatal("matchAll should match absent")
	}
}

func TestStringMatcher_null(t *testing.T) {
	m := NewStringMatcher(baseCfg())
	if !m.Matches(symNull, "", false) {
		t.Fatal("null should match absent")
	}
	if m.Matches(symNull, "x", true) {
		t.Fatal("null should not match present")
	}
}

// --- RegexMatcher -----------------------------------------------------------

func TestRegexMatcher_match(t *testing.T) {
	m := NewRegexMatcher(baseCfg())
	if !m.Matches("^foo[0-9]+$", "foo123", true) {
		t.Fatal("expected regex match")
	}
	if m.Matches("^foo[0-9]+$", "bar", true) {
		t.Fatal("expected regex no-match")
	}
}

func TestRegexMatcher_invalidRegex(t *testing.T) {
	m := NewRegexMatcher(baseCfg())
	// Unclosed group → invalid
	if m.Matches("(unclosed", "anything", true) {
		t.Fatal("invalid regex must not match")
	}
	// Same invalid pattern used a second time — exercises cached sentinel.
	if m.Matches("(unclosed", "anything", true) {
		t.Fatal("invalid regex must not match on cache hit")
	}
}

// --- SQLPatternMatcher ------------------------------------------------------

func TestSQLPatternMatcher_percent(t *testing.T) {
	m := NewSQLPatternMatcher(baseCfg())
	if !m.Matches("foo%", "foobar", true) {
		t.Fatal("percent should match suffix")
	}
	if !m.Matches("%bar", "foobar", true) {
		t.Fatal("percent should match prefix")
	}
	if !m.Matches("%", "anything", true) {
		t.Fatal("lone percent should match anything")
	}
	if m.Matches("foo%", "barbar", true) {
		t.Fatal("percent should anchor at start")
	}
}

func TestSQLPatternMatcher_underscore(t *testing.T) {
	m := NewSQLPatternMatcher(baseCfg())
	if !m.Matches("f_o", "foo", true) {
		t.Fatal("underscore should match single char")
	}
	if m.Matches("f_o", "fooo", true) {
		t.Fatal("underscore must not match two chars")
	}
	// Regexp metachars in the pattern must be escaped, not interpreted.
	if !m.Matches("a.b", "a.b", true) {
		t.Fatal("dot should be literal in SQL pattern")
	}
	if m.Matches("a.b", "axb", true) {
		t.Fatal("dot must not behave as regex wildcard")
	}
}

// --- RangeMatcher -----------------------------------------------------------

func TestRangeMatcher_inclusive(t *testing.T) {
	m := NewRangeMatcher(baseCfg())
	if !m.Matches("[1;10]", "1", true) {
		t.Fatal("low bound inclusive")
	}
	if !m.Matches("[1;10]", "10", true) {
		t.Fatal("high bound inclusive")
	}
	if !m.Matches("[1;10]", "5", true) {
		t.Fatal("interior must match")
	}
	if m.Matches("[1;10]", "0.999", true) {
		t.Fatal("below low must not match")
	}
}

func TestRangeMatcher_exclusive(t *testing.T) {
	m := NewRangeMatcher(baseCfg())
	if m.Matches("]1;10[", "1", true) {
		t.Fatal("low bound exclusive")
	}
	if m.Matches("]1;10[", "10", true) {
		t.Fatal("high bound exclusive")
	}
	if !m.Matches("]1;10[", "5", true) {
		t.Fatal("interior must match")
	}
}

func TestRangeMatcher_mixed(t *testing.T) {
	m := NewRangeMatcher(baseCfg())
	if !m.Matches("[1;10[", "1", true) {
		t.Fatal("low inclusive must match")
	}
	if m.Matches("[1;10[", "10", true) {
		t.Fatal("high exclusive must not match")
	}
	if !m.Matches("]1;10]", "10", true) {
		t.Fatal("high inclusive must match")
	}
	if m.Matches("]1;10]", "1", true) {
		t.Fatal("low exclusive must not match")
	}
}

func TestRangeMatcher_nonNumericAttr(t *testing.T) {
	m := NewRangeMatcher(baseCfg())
	if m.Matches("[1;10]", "abc", true) {
		t.Fatal("non-numeric attr must not match")
	}
	// Invalid range cell falls through to false.
	if m.Matches("not-a-range", "5", true) {
		t.Fatal("invalid range cell must not match")
	}
}

// --- LinearEngine -----------------------------------------------------------

func linearCfg() MatchConfig {
	return baseCfg(
		ColumnConfig{Name: "env", MatchType: MatchTypeString},
		ColumnConfig{Name: "service", MatchType: MatchTypeString},
	)
}

func TestLinearEngine_lastMatchWins(t *testing.T) {
	cfg := linearCfg()
	engine := NewLinearEngine(cfg)
	table := &LookupTable{
		Columns: []string{"env", "service", "owner"},
		Rows: []Row{
			{"env": "prod", "service": symMatchAll, "owner": "team-a"},
			{"env": "prod", "service": "checkout", "owner": "team-b"},
			{"env": "prod", "service": symMatchAll, "owner": "team-c"},
		},
	}
	got := engine.Match(table, map[string]string{"env": "prod", "service": "checkout"})
	if got == nil {
		t.Fatal("expected a match")
	}
	if (*got)["owner"] != "team-c" {
		t.Fatalf("expected last-match-wins owner=team-c, got %q", (*got)["owner"])
	}
}

func TestLinearEngine_noMatch(t *testing.T) {
	engine := NewLinearEngine(linearCfg())
	table := &LookupTable{
		Rows: []Row{
			{"env": "dev", "service": "checkout"},
		},
	}
	got := engine.Match(table, map[string]string{"env": "prod", "service": "checkout"})
	if got != nil {
		t.Fatalf("expected no match, got %+v", got)
	}
}

func TestLinearEngine_defaultSymbol(t *testing.T) {
	// Default symbol participates only in OptimizedEngine specificity logic;
	// for LinearEngine it is just an opaque cell value. With StringMatcher
	// it matches only when attribute equals it literally — i.e. it falls
	// back to no-match when attr differs. Validate the explicit fallback row
	// (matchAll) wins last.
	engine := NewLinearEngine(linearCfg())
	table := &LookupTable{
		Rows: []Row{
			{"env": "prod", "service": symDefault, "owner": "fallback"},
			{"env": "prod", "service": symMatchAll, "owner": "catchall"},
		},
	}
	got := engine.Match(table, map[string]string{"env": "prod", "service": "checkout"})
	if got == nil {
		t.Fatal("expected catchall to match via matchAll")
	}
	if (*got)["owner"] != "catchall" {
		t.Fatalf("expected owner=catchall, got %q", (*got)["owner"])
	}
}

// --- OptimizedEngine --------------------------------------------------------

func TestOptimizedEngine_basic(t *testing.T) {
	cfg := linearCfg()
	table := &LookupTable{
		Rows: []Row{
			{"env": "prod", "service": "checkout", "owner": "team-b"},
		},
	}
	engine := NewOptimizedEngine(table, cfg)
	got := engine.Match(table, map[string]string{"env": "prod", "service": "checkout"})
	if got == nil || (*got)["owner"] != "team-b" {
		t.Fatalf("expected owner=team-b, got %+v", got)
	}
	miss := engine.Match(table, map[string]string{"env": "prod", "service": "search"})
	if miss != nil {
		t.Fatalf("expected nil, got %+v", miss)
	}
}

func TestOptimizedEngine_specificity(t *testing.T) {
	cfg := linearCfg()
	table := &LookupTable{
		Rows: []Row{
			{"env": "prod", "service": symDefault, "owner": "fallback"},
			{"env": "prod", "service": "checkout", "owner": "specific"},
			{"env": "prod", "service": symMatchAll, "owner": "catchall"},
		},
	}
	engine := NewOptimizedEngine(table, cfg)

	// Specific literal wins over default and matchAll.
	got := engine.Match(table, map[string]string{"env": "prod", "service": "checkout"})
	if got == nil || (*got)["owner"] != "specific" {
		t.Fatalf("expected owner=specific, got %+v", got)
	}
	// No literal match → default wins over matchAll.
	got = engine.Match(table, map[string]string{"env": "prod", "service": "search"})
	if got == nil || (*got)["owner"] != "fallback" {
		t.Fatalf("expected owner=fallback (default), got %+v", got)
	}
}

func TestOptimizedEngine_matchAll(t *testing.T) {
	cfg := linearCfg()
	table := &LookupTable{
		Rows: []Row{
			{"env": "prod", "service": symMatchAll, "owner": "catchall"},
		},
	}
	engine := NewOptimizedEngine(table, cfg)
	// Missing attribute must still be picked up by matchAll.
	got := engine.Match(table, map[string]string{"env": "prod"})
	if got == nil || (*got)["owner"] != "catchall" {
		t.Fatalf("expected owner=catchall, got %+v", got)
	}
}

// ---------------------------------------------------------------------------
// Additional StringMatcher tests
// ---------------------------------------------------------------------------

func TestStringMatcher_emptyString(t *testing.T) {
	m := NewStringMatcher(baseCfg())
	// Empty string is a valid present attribute value that matches an empty cell.
	if !m.Matches("", "", true) {
		t.Fatal("empty cell should match empty attr when present")
	}
	// Empty string cell does not match a non-empty attr.
	if m.Matches("", "x", true) {
		t.Fatal("empty cell must not match non-empty attr")
	}
}

func TestStringMatcher_caseSensitive(t *testing.T) {
	m := NewStringMatcher(baseCfg())
	if m.Matches("Foo", "foo", true) {
		t.Fatal("StringMatcher must be case-sensitive")
	}
	if !m.Matches("FOO", "FOO", true) {
		t.Fatal("uppercase exact match must work")
	}
}

// ---------------------------------------------------------------------------
// Additional RegexMatcher tests — special symbols
// ---------------------------------------------------------------------------

func TestRegexMatcher_matchAllSymbol(t *testing.T) {
	m := NewRegexMatcher(baseCfg())
	if !m.Matches(symMatchAll, "anything", true) {
		t.Fatal("matchAll should match present attr")
	}
	if !m.Matches(symMatchAll, "", false) {
		t.Fatal("matchAll should match absent attr")
	}
}

func TestRegexMatcher_nullSymbol(t *testing.T) {
	m := NewRegexMatcher(baseCfg())
	if !m.Matches(symNull, "", false) {
		t.Fatal("null symbol should match absent attr")
	}
	if m.Matches(symNull, "x", true) {
		t.Fatal("null symbol must not match present attr")
	}
}

func TestRegexMatcher_absentAttrNoMatch(t *testing.T) {
	m := NewRegexMatcher(baseCfg())
	// A valid regex against an absent attr must return false (not panic).
	if m.Matches(".*", "", false) {
		t.Fatal("absent attr must not match even with .* pattern")
	}
}

// ---------------------------------------------------------------------------
// Additional SQLPatternMatcher tests — symbols + edge cases
// ---------------------------------------------------------------------------

func TestSQLPatternMatcher_matchAllSymbol(t *testing.T) {
	m := NewSQLPatternMatcher(baseCfg())
	if !m.Matches(symMatchAll, "anything", true) {
		t.Fatal("matchAll should match present attr")
	}
	if !m.Matches(symMatchAll, "", false) {
		t.Fatal("matchAll should match absent attr")
	}
}

func TestSQLPatternMatcher_nullSymbol(t *testing.T) {
	m := NewSQLPatternMatcher(baseCfg())
	if !m.Matches(symNull, "", false) {
		t.Fatal("null symbol should match absent attr")
	}
	if m.Matches(symNull, "x", true) {
		t.Fatal("null symbol must not match present attr")
	}
}

func TestSQLPatternMatcher_absentAttrNoMatch(t *testing.T) {
	m := NewSQLPatternMatcher(baseCfg())
	if m.Matches("%", "", false) {
		t.Fatal("absent attr must not match even with lone % pattern")
	}
}

func TestSQLPatternMatcher_contains(t *testing.T) {
	m := NewSQLPatternMatcher(baseCfg())
	if !m.Matches("%bar%", "foobarqux", true) {
		t.Fatal("contains pattern should match")
	}
	if m.Matches("%bar%", "foobaz", true) {
		t.Fatal("contains pattern must not match when substring absent")
	}
}

func TestSQLPatternMatcher_literalNoWildcard(t *testing.T) {
	m := NewSQLPatternMatcher(baseCfg())
	if !m.Matches("exact", "exact", true) {
		t.Fatal("literal pattern without wildcards must match equal string")
	}
	if m.Matches("exact", "exact2", true) {
		t.Fatal("literal pattern must not match longer string")
	}
}

func TestSQLPatternMatcher_underscoreCombination(t *testing.T) {
	m := NewSQLPatternMatcher(baseCfg())
	// Pattern "f_o_" matches exactly 4-char strings starting with f, 3rd char o.
	if !m.Matches("f_o_", "fxox", true) {
		t.Fatal("combined underscore pattern should match")
	}
	if m.Matches("f_o_", "fxoxx", true) {
		t.Fatal("underscore must match exactly one char")
	}
}

// ---------------------------------------------------------------------------
// Additional RangeMatcher tests — symbols + edge cases
// ---------------------------------------------------------------------------

func TestRangeMatcher_matchAllSymbol(t *testing.T) {
	m := NewRangeMatcher(baseCfg())
	if !m.Matches(symMatchAll, "anything", true) {
		t.Fatal("matchAll should match in RangeMatcher")
	}
	if !m.Matches(symMatchAll, "", false) {
		t.Fatal("matchAll should match absent attr")
	}
}

func TestRangeMatcher_nullSymbol(t *testing.T) {
	m := NewRangeMatcher(baseCfg())
	if !m.Matches(symNull, "", false) {
		t.Fatal("null symbol should match absent attr")
	}
	if m.Matches(symNull, "5", true) {
		t.Fatal("null symbol must not match present attr")
	}
}

func TestRangeMatcher_exactBound(t *testing.T) {
	m := NewRangeMatcher(baseCfg())
	// [5;5] is a degenerate range that matches only 5.
	if !m.Matches("[5;5]", "5", true) {
		t.Fatal("exact range [5;5] must match 5")
	}
	if m.Matches("[5;5]", "5.1", true) {
		t.Fatal("exact range [5;5] must not match 5.1")
	}
}

func TestRangeMatcher_absentAttrNoMatch(t *testing.T) {
	m := NewRangeMatcher(baseCfg())
	if m.Matches("[1;10]", "", false) {
		t.Fatal("absent attr must not match range")
	}
}

func TestRangeMatcher_negativeValues(t *testing.T) {
	m := NewRangeMatcher(baseCfg())
	if !m.Matches("[-10;-1]", "-5", true) {
		t.Fatal("negative range should match negative attr")
	}
	if m.Matches("[-10;-1]", "0", true) {
		t.Fatal("zero must not match negative range")
	}
}

func TestRangeMatcher_floatValues(t *testing.T) {
	m := NewRangeMatcher(baseCfg())
	if !m.Matches("[1.5;2.5]", "2.0", true) {
		t.Fatal("float range must match float attr")
	}
	if m.Matches("[1.5;2.5]", "3.0", true) {
		t.Fatal("float range must not match out-of-range float")
	}
}

// ---------------------------------------------------------------------------
// Additional LinearEngine tests — multi-column AND, null row, matchAll row
// ---------------------------------------------------------------------------

func TestLinearEngine_multiColumnAND(t *testing.T) {
	// Both columns must match; if only one matches the row must be skipped.
	cfg := baseCfg(
		ColumnConfig{Name: "env", MatchType: MatchTypeString},
		ColumnConfig{Name: "region", MatchType: MatchTypeString},
	)
	engine := NewLinearEngine(cfg)
	table := &LookupTable{
		Rows: []Row{
			{"env": "prod", "region": "eu", "owner": "eu-team"},
			{"env": "prod", "region": "us", "owner": "us-team"},
		},
	}
	got := engine.Match(table, map[string]string{"env": "prod", "region": "us"})
	if got == nil || (*got)["owner"] != "us-team" {
		t.Fatalf("expected us-team, got %+v", got)
	}
	// Only env matches, region doesn't → no match.
	miss := engine.Match(table, map[string]string{"env": "prod", "region": "apac"})
	if miss != nil {
		t.Fatalf("expected nil for unmatched region, got %+v", miss)
	}
}

func TestLinearEngine_nullRowMatchesAbsentAttr(t *testing.T) {
	// A row with nullSymbol in a column should match when that attr is absent.
	cfg := baseCfg(ColumnConfig{Name: "env", MatchType: MatchTypeString})
	engine := NewLinearEngine(cfg)
	table := &LookupTable{
		Rows: []Row{
			{"env": symNull, "owner": "null-team"},
		},
	}
	// Attr present → no match.
	miss := engine.Match(table, map[string]string{"env": "prod"})
	if miss != nil {
		t.Fatalf("null row must not match present attr, got %+v", miss)
	}
	// Attr absent → match.
	got := engine.Match(table, map[string]string{})
	if got == nil || (*got)["owner"] != "null-team" {
		t.Fatalf("expected null-team for absent attr, got %+v", got)
	}
}

func TestLinearEngine_matchAllRowAlwaysMatches(t *testing.T) {
	cfg := baseCfg(ColumnConfig{Name: "env", MatchType: MatchTypeString})
	engine := NewLinearEngine(cfg)
	table := &LookupTable{
		Rows: []Row{
			{"env": symMatchAll, "owner": "any-team"},
		},
	}
	// Present attr.
	if got := engine.Match(table, map[string]string{"env": "prod"}); got == nil || (*got)["owner"] != "any-team" {
		t.Fatalf("matchAll row must match present attr, got %+v", got)
	}
	// Absent attr.
	if got := engine.Match(table, map[string]string{}); got == nil || (*got)["owner"] != "any-team" {
		t.Fatalf("matchAll row must match absent attr, got %+v", got)
	}
}

func TestLinearEngine_nilTable(t *testing.T) {
	engine := NewLinearEngine(linearCfg())
	if got := engine.Match(nil, map[string]string{"env": "prod"}); got != nil {
		t.Fatalf("nil table must return nil, got %+v", got)
	}
}

func TestLinearEngine_singleColumnRegex(t *testing.T) {
	cfg := baseCfg(ColumnConfig{Name: "service", MatchType: MatchTypeRegex})
	engine := NewLinearEngine(cfg)
	table := &LookupTable{
		Rows: []Row{
			{"service": "^pay.*", "owner": "payments-team"},
			{"service": "^check.*", "owner": "checkout-team"},
		},
	}
	got := engine.Match(table, map[string]string{"service": "payment-v2"})
	if got == nil || (*got)["owner"] != "payments-team" {
		t.Fatalf("expected payments-team via regex, got %+v", got)
	}
}

func TestLinearEngine_singleColumnRange(t *testing.T) {
	cfg := baseCfg(ColumnConfig{Name: "latency_ms", MatchType: MatchTypeRange})
	engine := NewLinearEngine(cfg)
	table := &LookupTable{
		Rows: []Row{
			{"latency_ms": "[0;100]", "tier": "fast"},
			{"latency_ms": "]100;1000]", "tier": "slow"},
		},
	}
	got := engine.Match(table, map[string]string{"latency_ms": "250"})
	if got == nil || (*got)["tier"] != "slow" {
		t.Fatalf("expected slow tier for latency=250, got %+v", got)
	}
}

// ---------------------------------------------------------------------------
// NewColumnMatcher factory — unknown type falls back to StringMatcher
// ---------------------------------------------------------------------------

func TestNewColumnMatcher_unknownTypeFallsBackToString(t *testing.T) {
	cfg := baseCfg()
	m := NewColumnMatcher("totally-unknown-type", cfg)
	// StringMatcher semantics: exact equality.
	if !m.Matches("hello", "hello", true) {
		t.Fatal("unknown type must fall back to StringMatcher (exact match)")
	}
	if m.Matches("hello", "world", true) {
		t.Fatal("unknown type fallback must not match unequal values")
	}
}

func TestNewColumnMatcher_allTypes(t *testing.T) {
	cfg := baseCfg()
	types := []MatchType{MatchTypeString, MatchTypeRegex, MatchTypeSQLPattern, MatchTypeRange}
	for _, mt := range types {
		m := NewColumnMatcher(mt, cfg)
		if m == nil {
			t.Fatalf("NewColumnMatcher(%q) returned nil", mt)
		}
		// matchAll must work for every type.
		if !m.Matches(symMatchAll, "x", true) {
			t.Fatalf("matchAll must work for type %q", mt)
		}
	}
}
