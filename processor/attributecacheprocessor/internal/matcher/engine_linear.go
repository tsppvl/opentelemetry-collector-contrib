// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package matcher // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributecacheprocessor/internal/matcher"

// LinearEngine evaluates every row in the table top-to-bottom and returns
// the last row that matches all match columns. Last-match-wins is the
// historical APG semantics for unindexed tables.
type LinearEngine struct {
	cfg      MatchConfig
	matchers []ColumnMatcher
}

// NewLinearEngine builds a LinearEngine, instantiating one ColumnMatcher per
// configured match column.
func NewLinearEngine(cfg MatchConfig) MatchEngine {
	matchers := make([]ColumnMatcher, len(cfg.Columns))
	for i, col := range cfg.Columns {
		matchers[i] = NewColumnMatcher(col.MatchType, cfg)
	}
	return &LinearEngine{cfg: cfg, matchers: matchers}
}

// Match scans table linearly. Returns nil if no row matches.
func (e *LinearEngine) Match(table *LookupTable, attrs map[string]string) *Row {
	if table == nil {
		return nil
	}
	var lastMatch *Row
	for i := range table.Rows {
		row := &table.Rows[i]
		if e.rowMatches(row, attrs) {
			lastMatch = row
		}
	}
	return lastMatch
}

func (e *LinearEngine) rowMatches(row *Row, attrs map[string]string) bool {
	for i, col := range e.cfg.Columns {
		cellVal := (*row)[col.Name]
		attrVal, present := attrs[col.Name]
		if !e.matchers[i].Matches(cellVal, attrVal, present) {
			return false
		}
	}
	return true
}
