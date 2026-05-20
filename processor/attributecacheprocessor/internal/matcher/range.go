// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package matcher // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributecacheprocessor/internal/matcher"

import (
	"regexp"
	"strconv"
	"strings"
	"sync"
)

// RangeMatcher matches numeric attrValue against an interval encoded in
// cellValue. Supported syntaxes (lo and hi parsed as float64):
//
//	[lo;hi]   inclusive on both ends
//	[lo;hi[   inclusive low, exclusive high
//	]lo;hi]   exclusive low, inclusive high
//	]lo;hi[   exclusive on both ends
//
// Cells that are not parseable as an interval, or attrValues that are not
// parseable as a float64, never match.
type RangeMatcher struct {
	matchAllSym string
	nullSym     string
	cache       sync.Map // map[string]*rangeSpec
}

type rangeSpec struct {
	lo, hi   float64
	loIncl   bool
	hiIncl   bool
	parseErr bool
}

var rangePattern = regexp.MustCompile(`^([\[\]])(.*);(.*)([\[\]])$`)

// NewRangeMatcher returns a RangeMatcher configured with the symbols from
// cfg.
func NewRangeMatcher(cfg MatchConfig) *RangeMatcher {
	return &RangeMatcher{
		matchAllSym: cfg.MatchAllSymbol,
		nullSym:     cfg.NullSymbol,
	}
}

// Matches reports whether attrValue (parsed as float64) lies in the interval
// described by cellValue.
func (m *RangeMatcher) Matches(cellValue, attrValue string, present bool) bool {
	if m.matchAllSym != "" && cellValue == m.matchAllSym {
		return true
	}
	if m.nullSym != "" && cellValue == m.nullSym {
		return !present
	}
	if !present {
		return false
	}
	spec := m.lookup(cellValue)
	if spec.parseErr {
		return false
	}
	v, err := strconv.ParseFloat(strings.TrimSpace(attrValue), 64)
	if err != nil {
		return false
	}
	if spec.loIncl {
		if v < spec.lo {
			return false
		}
	} else if v <= spec.lo {
		return false
	}
	if spec.hiIncl {
		if v > spec.hi {
			return false
		}
	} else if v >= spec.hi {
		return false
	}
	return true
}

func (m *RangeMatcher) lookup(cellValue string) *rangeSpec {
	if v, ok := m.cache.Load(cellValue); ok {
		return v.(*rangeSpec)
	}
	spec := parseRange(cellValue)
	actual, _ := m.cache.LoadOrStore(cellValue, spec)
	return actual.(*rangeSpec)
}

func parseRange(s string) *rangeSpec {
	m := rangePattern.FindStringSubmatch(s)
	if m == nil {
		return &rangeSpec{parseErr: true}
	}
	loBracket, loStr, hiStr, hiBracket := m[1], m[2], m[3], m[4]
	lo, err := strconv.ParseFloat(strings.TrimSpace(loStr), 64)
	if err != nil {
		return &rangeSpec{parseErr: true}
	}
	hi, err := strconv.ParseFloat(strings.TrimSpace(hiStr), 64)
	if err != nil {
		return &rangeSpec{parseErr: true}
	}
	// '[' on the low side is inclusive; ']' on the low side is exclusive.
	// ']' on the high side is inclusive; '[' on the high side is exclusive.
	loIncl := loBracket == "["
	hiIncl := hiBracket == "]"
	return &rangeSpec{lo: lo, hi: hi, loIncl: loIncl, hiIncl: hiIncl}
}
