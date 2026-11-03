// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package datasource // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributecacheprocessor/internal/datasource"

import (
	"context"
	"sort"
)

// InlineConfig is the inline-source view used by the data source. The
// processor's user-facing config wraps the same Rows slice; this type avoids
// an import cycle between the datasource package and the processor package.
type InlineConfig struct {
	Rows []map[string]string
}

// InlineDataSource serves rows that are declared directly in YAML
// configuration. It is stateless: every Load call returns the same rows in
// the same order.
type InlineDataSource struct {
	rows []map[string]string
}

// NewInline creates an InlineDataSource from the given config. The config's
// Rows slice header is copied so that callers can safely mutate the original
// slice afterwards; the row maps themselves are shared and treated as
// read-only by the data source.
func NewInline(cfg *InlineConfig) *InlineDataSource {
	if cfg == nil || len(cfg.Rows) == 0 {
		return &InlineDataSource{}
	}
	rows := make([]map[string]string, len(cfg.Rows))
	copy(rows, cfg.Rows)
	return &InlineDataSource{rows: rows}
}

// Load returns the inline rows together with the column names derived from
// the first row's keys, sorted alphabetically for deterministic ordering.
// When no rows are configured, Load returns (nil, nil, nil) so callers can
// distinguish "no data" from a real error.
func (d *InlineDataSource) Load(_ context.Context) ([]string, []map[string]string, error) {
	if len(d.rows) == 0 {
		return nil, nil, nil
	}

	first := d.rows[0]
	cols := make([]string, 0, len(first))
	for k := range first {
		cols = append(cols, k)
	}
	sort.Strings(cols)

	return cols, d.rows, nil
}

// Close is a no-op: the inline source holds no external resources.
func (*InlineDataSource) Close(_ context.Context) error { return nil }

// Compile-time interface check.
var _ DataSource = (*InlineDataSource)(nil)
