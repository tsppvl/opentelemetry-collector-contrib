// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package attributecacheprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributecacheprocessor"

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/collector/processor"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributecacheprocessor/internal/metadata"
)

// telemetry wraps the generated TelemetryBuilder and exposes stable helper
// methods for emitting processor metrics. All helper methods are nil-safe:
// when t is nil (e.g. in tests that do not wire telemetry), the calls are
// silently no-ops.
//
// Every metric recording carries the processor component ID as the
// "processor" attribute so that multiple instances of the same processor
// type appear as separate time-series in Prometheus/Grafana.
type telemetry struct {
	builder *metadata.TelemetryBuilder
	attrs   metric.MeasurementOption // processor=<component-id> label
}

func newTelemetry(set processor.Settings) (*telemetry, error) {
	builder, err := metadata.NewTelemetryBuilder(set.TelemetrySettings)
	if err != nil {
		return nil, err
	}
	attrSet := attribute.NewSet(attribute.String("processor", set.ID.String()))
	return &telemetry{
		builder: builder,
		attrs:   metric.WithAttributeSet(attrSet),
	}, nil
}

// recordItemProcessed increments the items_processed counter by 1.
func (t *telemetry) recordItemProcessed(ctx context.Context) {
	if t == nil || t.builder == nil {
		return
	}
	t.builder.ProcessorAttributecacheItemsProcessed.Add(ctx, 1, t.attrs)
}

// recordItemEnriched increments the items_enriched counter by 1.
func (t *telemetry) recordItemEnriched(ctx context.Context) {
	if t == nil || t.builder == nil {
		return
	}
	t.builder.ProcessorAttributecacheItemsEnriched.Add(ctx, 1, t.attrs)
}

// recordItemPassthrough increments the items_passthrough counter by 1.
func (t *telemetry) recordItemPassthrough(ctx context.Context) {
	if t == nil || t.builder == nil {
		return
	}
	t.builder.ProcessorAttributecacheItemsPassthrough.Add(ctx, 1, t.attrs)
}

// recordLookupDuration records a lookup latency observation in seconds.
func (t *telemetry) recordLookupDuration(ctx context.Context, duration time.Duration) {
	if t == nil || t.builder == nil {
		return
	}
	t.builder.ProcessorAttributecacheLookupDuration.Record(ctx, duration.Seconds(), t.attrs)
}

// recordRefreshSuccess increments the refresh_total counter and records the
// current number of loaded rows in the table_rows gauge.
func (t *telemetry) recordRefreshSuccess(ctx context.Context, rowCount int64) {
	if t == nil || t.builder == nil {
		return
	}
	t.builder.ProcessorAttributecacheRefreshTotal.Add(ctx, 1, t.attrs)
	t.builder.ProcessorAttributecacheTableRows.Record(ctx, rowCount, t.attrs)
}

// recordRefreshError increments the refresh_errors_total counter.
func (t *telemetry) recordRefreshError(ctx context.Context) {
	if t == nil || t.builder == nil {
		return
	}
	t.builder.ProcessorAttributecacheRefreshErrorsTotal.Add(ctx, 1, t.attrs)
}
