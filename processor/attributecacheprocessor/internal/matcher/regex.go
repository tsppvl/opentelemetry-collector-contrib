// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package matcher // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributecacheprocessor/internal/matcher"

import (
	"regexp"
	"sync"
)

// RegexMatcher treats cellValue as a Go regular expression and tests it
// against attrValue. Compiled regexps are cached per cellValue so repeated
// matches against the same row pay the compile cost once.
type RegexMatcher struct {
	matchAllSym string
	nullSym     string
	cache       sync.Map // map[string]*regexEntry
}

// regexEntry holds a compiled regexp or a sentinel for invalid patterns so
// repeated lookups for an invalid cellValue do not recompile.
type regexEntry struct {
	re  *regexp.Regexp
	bad bool
}

// NewRegexMatcher returns a RegexMatcher configured with the symbols from
// cfg.
func NewRegexMatcher(cfg MatchConfig) *RegexMatcher {
	return &RegexMatcher{
		matchAllSym: cfg.MatchAllSymbol,
		nullSym:     cfg.NullSymbol,
	}
}

// Matches reports whether attrValue satisfies the regular expression in
// cellValue. Invalid patterns produce no match.
func (m *RegexMatcher) Matches(cellValue, attrValue string, present bool) bool {
	if m.matchAllSym != "" && cellValue == m.matchAllSym {
		return true
	}
	if m.nullSym != "" && cellValue == m.nullSym {
		return !present
	}
	if !present {
		return false
	}
	entry := m.lookup(cellValue)
	if entry.bad {
		return false
	}
	return entry.re.MatchString(attrValue)
}

func (m *RegexMatcher) lookup(cellValue string) *regexEntry {
	if v, ok := m.cache.Load(cellValue); ok {
		return v.(*regexEntry)
	}
	entry := &regexEntry{}
	re, err := regexp.Compile(cellValue)
	if err != nil {
		entry.bad = true
	} else {
		entry.re = re
	}
	actual, _ := m.cache.LoadOrStore(cellValue, entry)
	return actual.(*regexEntry)
}
