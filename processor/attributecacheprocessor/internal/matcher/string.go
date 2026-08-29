// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package matcher // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributecacheprocessor/internal/matcher"

// StringMatcher matches a cell value against an attribute value with exact,
// case-sensitive equality after handling the configured match-all and null
// symbols.
type StringMatcher struct {
	matchAllSym string
	nullSym     string
}

// NewStringMatcher returns a StringMatcher configured with the symbols from
// cfg.
func NewStringMatcher(cfg MatchConfig) *StringMatcher {
	return &StringMatcher{
		matchAllSym: cfg.MatchAllSymbol,
		nullSym:     cfg.NullSymbol,
	}
}

// Matches reports whether attrValue equals cellValue exactly. Symbol handling
// is applied first: matchAll always matches, null matches only when the
// attribute is absent, and any other cell value never matches an absent
// attribute.
func (m *StringMatcher) Matches(cellValue, attrValue string, present bool) bool {
	if m.matchAllSym != "" && cellValue == m.matchAllSym {
		return true
	}
	if m.nullSym != "" && cellValue == m.nullSym {
		return !present
	}
	if !present {
		return false
	}
	return cellValue == attrValue
}
