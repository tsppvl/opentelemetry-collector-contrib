// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package matcher // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributecacheprocessor/internal/matcher"

// NewColumnMatcher returns the ColumnMatcher implementation that corresponds
// to matchType. Symbol configuration is taken from cfg and captured in the
// returned matcher. Unknown match types fall back to StringMatcher (exact
// equality), which is the safest default.
func NewColumnMatcher(matchType MatchType, cfg MatchConfig) ColumnMatcher {
	switch matchType {
	case MatchTypeRegex:
		return NewRegexMatcher(cfg)
	case MatchTypeSQLPattern:
		return NewSQLPatternMatcher(cfg)
	case MatchTypeRange:
		return NewRangeMatcher(cfg)
	case MatchTypeString:
		return NewStringMatcher(cfg)
	default:
		return NewStringMatcher(cfg)
	}
}
