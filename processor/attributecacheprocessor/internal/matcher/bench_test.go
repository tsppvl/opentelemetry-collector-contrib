// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package matcher

import (
	"fmt"
	"testing"
)

const benchRows = 100_000

// buildStringTable generates a LookupTable with n rows containing a single
// "service" column with values "service-0", "service-1", ... "service-n-1".
// The matching target is the last row (worst case for linear scan).
func buildStringTable(n int) (*LookupTable, map[string]string) {
	rows := make([]Row, n)
	for i := range rows {
		rows[i] = Row{"service": fmt.Sprintf("service-%d", i)}
	}
	table := &LookupTable{
		Columns: []string{"service"},
		Rows:    rows,
	}
	// Worst case: match the last row.
	target := map[string]string{"service": fmt.Sprintf("service-%d", n-1)}
	return table, target
}

// buildRegexTable generates a LookupTable with n rows where each row has a
// regex pattern in the "service" column. The last row matches the target.
func buildRegexTable(n int) (*LookupTable, map[string]string) {
	rows := make([]Row, n)
	for i := range rows {
		// Pattern that does NOT match except the last one.
		rows[i] = Row{"service": fmt.Sprintf("^svc-%05d$", i)}
	}
	// Last row: pattern that matches our target attribute value.
	rows[n-1] = Row{"service": "^target-service$"}
	table := &LookupTable{
		Columns: []string{"service"},
		Rows:    rows,
	}
	target := map[string]string{"service": "target-service"}
	return table, target
}

// buildMultiColTable generates a LookupTable with n rows and 5 columns. The
// last row is an exact match on all 5 columns.
func buildMultiColTable(n int) (*LookupTable, map[string]string) {
	cols := []string{"env", "region", "service", "tenant", "tier"}
	rows := make([]Row, n)
	for i := range rows {
		rows[i] = Row{
			"env":     fmt.Sprintf("env-%d", i),
			"region":  fmt.Sprintf("region-%d", i),
			"service": fmt.Sprintf("svc-%d", i),
			"tenant":  fmt.Sprintf("tenant-%d", i),
			"tier":    fmt.Sprintf("tier-%d", i),
		}
	}
	// Last row: will match target.
	last := n - 1
	rows[last] = Row{
		"env":     "prod",
		"region":  "eu-west-1",
		"service": "checkout",
		"tenant":  "acme",
		"tier":    "premium",
	}

	table := &LookupTable{Columns: cols, Rows: rows}
	target := map[string]string{
		"env":     "prod",
		"region":  "eu-west-1",
		"service": "checkout",
		"tenant":  "acme",
		"tier":    "premium",
	}
	return table, target
}

// BenchmarkLinearEngine100k measures LinearEngine performance with 100k rows,
// a single string-match column, matching the last row (worst case).
func BenchmarkLinearEngine100k(b *testing.B) {
	cfg := MatchConfig{
		Columns:        []ColumnConfig{{Name: "service", MatchType: MatchTypeString}},
		DefaultSymbol:  "**",
		MatchAllSymbol: "%%",
		NullSymbol:     "@@",
	}
	engine := NewLinearEngine(cfg)
	table, target := buildStringTable(benchRows)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		row := engine.Match(table, target)
		if row == nil {
			b.Fatal("expected a match")
		}
	}
}

// BenchmarkLinearEngineRegex100k measures LinearEngine performance with 100k rows,
// a single regex-match column, matching the last row (worst case).
func BenchmarkLinearEngineRegex100k(b *testing.B) {
	cfg := MatchConfig{
		Columns:        []ColumnConfig{{Name: "service", MatchType: MatchTypeRegex}},
		DefaultSymbol:  "**",
		MatchAllSymbol: "%%",
		NullSymbol:     "@@",
	}
	engine := NewLinearEngine(cfg)
	table, target := buildRegexTable(benchRows)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		row := engine.Match(table, target)
		if row == nil {
			b.Fatal("expected a match")
		}
	}
}

// BenchmarkLinearEngineMultiCol100k measures LinearEngine performance with 100k rows,
// five string-match columns, matching the last row (worst case).
func BenchmarkLinearEngineMultiCol100k(b *testing.B) {
	cfg := MatchConfig{
		Columns: []ColumnConfig{
			{Name: "env", MatchType: MatchTypeString},
			{Name: "region", MatchType: MatchTypeString},
			{Name: "service", MatchType: MatchTypeString},
			{Name: "tenant", MatchType: MatchTypeString},
			{Name: "tier", MatchType: MatchTypeString},
		},
		DefaultSymbol:  "**",
		MatchAllSymbol: "%%",
		NullSymbol:     "@@",
	}
	engine := NewLinearEngine(cfg)
	table, target := buildMultiColTable(benchRows)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		row := engine.Match(table, target)
		if row == nil {
			b.Fatal("expected a match")
		}
	}
}
