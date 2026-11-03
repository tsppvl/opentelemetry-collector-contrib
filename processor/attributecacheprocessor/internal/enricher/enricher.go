// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package enricher applies row values selected by the matcher to the
// attribute maps of incoming telemetry items.
package enricher // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributecacheprocessor/internal/enricher"

import "strings"

// AttributeLevel controls which attribute map receives the enriched value.
type AttributeLevel string

// Supported AttributeLevel values.
const (
	LevelResource  AttributeLevel = "resource"
	LevelScope     AttributeLevel = "scope"
	LevelAttribute AttributeLevel = "attribute"
)

// EnrichmentSpec describes a single enrichment column. It is built from the
// user-facing configuration and consumed by Enricher when an item matches a
// row.
type EnrichmentSpec struct {
	// SourceColumn is the column name in the lookup table.
	SourceColumn string
	// TargetName is the attribute name to write. Defaults to SourceColumn
	// when empty.
	TargetName string
	// Level selects the attribute map (resource / scope / item) that
	// receives the value.
	Level AttributeLevel
	// NullSymbol is the cell value that signals "do not write this
	// attribute" for the current row.
	NullSymbol string
}

// AttributeMap is the minimal write surface required by Enricher. The
// concrete pdata maps (pcommon.Map) satisfy this interface via thin wrappers
// in the processor package.
type AttributeMap interface {
	PutStr(key, value string)
	Remove(key string) bool
}

// Enricher writes selected row values onto a telemetry item's attribute
// maps. Instances are immutable after construction and safe for concurrent
// use.
type Enricher struct {
	// Columns lists the enrichment columns in evaluation order.
	Columns []EnrichmentSpec
	// InsertStart and InsertEnd delimit property-insertion templates inside
	// enrichment cell values (e.g. "[", "]").
	InsertStart string
	InsertEnd   string
	// NullSymbol is the global null marker; cells equal to NullSymbol are
	// skipped. May be overridden per-column via EnrichmentSpec.NullSymbol.
	NullSymbol string
	// DeleteAfterUse lists match-column attribute names to remove from the
	// item after enrichment has been written.
	DeleteAfterUse []string
}

// Enrich writes enrichment attributes from row to the supplied attribute
// maps. sourceAttrs supplies the read-only attribute view used for
// property-insertion template substitution.
//
// For each enrichment column, Enrich resolves the cell value, expands any
// property-insertion templates against sourceAttrs, and writes the result to
// the attribute map for the configured level. Cells equal to the (column or
// global) null symbol are skipped. Finally, attributes named in
// DeleteAfterUse are removed from all three maps.
func (e *Enricher) Enrich(row map[string]string, sourceAttrs map[string]string, resourceAttrs, scopeAttrs, itemAttrs AttributeMap) {
	if row == nil {
		return
	}
	for _, spec := range e.Columns {
		cellVal, ok := row[spec.SourceColumn]
		if !ok {
			continue
		}
		nullSym := spec.NullSymbol
		if nullSym == "" {
			nullSym = e.NullSymbol
		}
		if nullSym != "" && cellVal == nullSym {
			continue
		}
		cellVal = e.expandTemplate(cellVal, sourceAttrs)
		targetName := spec.TargetName
		if targetName == "" {
			targetName = spec.SourceColumn
		}
		switch spec.Level {
		case LevelResource:
			if resourceAttrs != nil {
				resourceAttrs.PutStr(targetName, cellVal)
			}
		case LevelScope:
			if scopeAttrs != nil {
				scopeAttrs.PutStr(targetName, cellVal)
			}
		default:
			if itemAttrs != nil {
				itemAttrs.PutStr(targetName, cellVal)
			}
		}
	}
	// delete_after_use removes match-column attributes only from item-level
	// (datapoint/log/span) maps. Resource and scope attribute maps are shared
	// across all items in a batch, so deleting from them would break
	// subsequent items that rely on those attributes for matching.
	for _, colName := range e.DeleteAfterUse {
		if itemAttrs != nil {
			itemAttrs.Remove(colName)
		}
	}
}

// expandTemplate replaces every occurrence of InsertStart+name+InsertEnd in
// val with the corresponding entry from sourceAttrs. Missing attributes
// expand to the empty string. The scan is single-pass over val: substituted
// text is not re-scanned, so templates cannot recurse.
func (e *Enricher) expandTemplate(val string, sourceAttrs map[string]string) string {
	if e.InsertStart == "" || e.InsertEnd == "" {
		return val
	}
	var b []byte
	i := 0
	for i < len(val) {
		startIdx := indexAt(val, e.InsertStart, i)
		if startIdx < 0 {
			if b == nil {
				return val
			}
			b = append(b, val[i:]...)
			break
		}
		endIdx := indexAt(val, e.InsertEnd, startIdx+len(e.InsertStart))
		if endIdx < 0 {
			if b == nil {
				return val
			}
			b = append(b, val[i:]...)
			break
		}
		name := val[startIdx+len(e.InsertStart) : endIdx]
		if b == nil {
			b = make([]byte, 0, len(val))
		}
		b = append(b, val[i:startIdx]...)
		if sourceAttrs != nil {
			b = append(b, sourceAttrs[name]...)
		}
		i = endIdx + len(e.InsertEnd)
	}
	if b == nil {
		return val
	}
	return string(b)
}

// indexAt returns the index of the first occurrence of sep in s starting at
// or after from, or -1 if sep is not present.
func indexAt(s, sep string, from int) int {
	if from > len(s) {
		return -1
	}
	if idx := strings.Index(s[from:], sep); idx >= 0 {
		return from + idx
	}
	return -1
}
