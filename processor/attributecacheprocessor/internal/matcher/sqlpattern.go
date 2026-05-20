// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package matcher // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributecacheprocessor/internal/matcher"

import (
	"regexp"
	"strings"
	"sync"
)

// SQLPatternMatcher matches attrValue against cellValue using SQL LIKE
// semantics: '%' matches any (possibly empty) sequence and '_' matches any
// single character. All other regexp metacharacters are escaped before
// translation, and the resulting regexp is anchored and cached per cellValue.
type SQLPatternMatcher struct {
	matchAllSym string
	nullSym     string
	cache       sync.Map // map[string]*regexp.Regexp
}

// NewSQLPatternMatcher returns a SQLPatternMatcher configured with the
// symbols from cfg.
func NewSQLPatternMatcher(cfg MatchConfig) *SQLPatternMatcher {
	return &SQLPatternMatcher{
		matchAllSym: cfg.MatchAllSymbol,
		nullSym:     cfg.NullSymbol,
	}
}

// Matches reports whether attrValue matches the SQL LIKE pattern in
// cellValue.
func (m *SQLPatternMatcher) Matches(cellValue, attrValue string, present bool) bool {
	if m.matchAllSym != "" && cellValue == m.matchAllSym {
		return true
	}
	if m.nullSym != "" && cellValue == m.nullSym {
		return !present
	}
	if !present {
		return false
	}
	re := m.lookup(cellValue)
	if re == nil {
		return false
	}
	return re.MatchString(attrValue)
}

func (m *SQLPatternMatcher) lookup(cellValue string) *regexp.Regexp {
	if v, ok := m.cache.Load(cellValue); ok {
		if v == nil {
			return nil
		}
		return v.(*regexp.Regexp)
	}
	re := compileSQLPattern(cellValue)
	if re == nil {
		// Store a typed nil so repeated lookups skip the compile path.
		m.cache.Store(cellValue, (*regexp.Regexp)(nil))
		return nil
	}
	actual, _ := m.cache.LoadOrStore(cellValue, re)
	return actual.(*regexp.Regexp)
}

// compileSQLPattern translates a SQL LIKE pattern to an anchored Go regexp.
// Escape regexp metacharacters first, then convert '%' → '.*' and '_' → '.'.
func compileSQLPattern(pattern string) *regexp.Regexp {
	var b strings.Builder
	b.Grow(len(pattern) + 4)
	b.WriteByte('^')
	for _, r := range pattern {
		switch r {
		case '%':
			b.WriteString(".*")
		case '_':
			b.WriteByte('.')
		case '.', '*', '+', '?', '(', ')', '[', ']', '{', '}', '\\', '^', '$', '|':
			b.WriteByte('\\')
			b.WriteRune(r)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('$')
	re, err := regexp.Compile(b.String())
	if err != nil {
		return nil
	}
	return re
}
