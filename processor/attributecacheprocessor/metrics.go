// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package attributecacheprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributecacheprocessor"

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottldatapoint"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlmetric"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlresource"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributecacheprocessor/internal/matcher"
)

// processMetrics is the per-batch entry point for the metrics signal.
// It walks every datapoint, applies the configured OTTL input filters,
// looks the merged attribute set up against the cached table, and asks
// the enricher to write the matched row's enrichment columns.
func (p *attributeCacheProcessor) processMetrics(ctx context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	table := matcherTable(p.cache.Current())
	if table == nil || p.engine == nil || p.enricher == nil {
		// Cache not yet loaded or processor misconfigured; pass through.
		return md, nil
	}

	var errs error
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		rm := md.ResourceMetrics().At(i)
		resourceAttrs := rm.Resource().Attributes()

		if p.metricResourceFilter != nil {
			tCtx := ottlresource.NewTransformContextPtr(rm.Resource(), rm)
			matched, err := p.metricResourceFilter.Eval(ctx, tCtx)
			tCtx.Close()
			if err != nil {
				errs = multierr.Append(errs, err)
				if p.cfg.ErrorMode == ottl.PropagateError {
					continue
				}
			}
			if !matched {
				continue
			}
		}

		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			sm := rm.ScopeMetrics().At(j)
			scopeAttrs := sm.Scope().Attributes()

			for k := 0; k < sm.Metrics().Len(); k++ {
				metric := sm.Metrics().At(k)

				if p.metricFilter != nil {
					tCtx := ottlmetric.NewTransformContextPtr(rm, sm, metric)
					matched, err := p.metricFilter.Eval(ctx, tCtx)
					tCtx.Close()
					if err != nil {
						errs = multierr.Append(errs, err)
						if p.cfg.ErrorMode == ottl.PropagateError {
							continue
						}
					}
					if !matched {
						continue
					}
				}

				if err := p.applyToMetric(ctx, rm, sm, metric, table, resourceAttrs, scopeAttrs); err != nil {
					errs = multierr.Append(errs, err)
				}
			}
		}
	}

	if errs != nil {
		p.logger.Debug("attributecache: errors while processing metrics", zap.Error(errs))
	}
	return md, errs
}

// applyToMetric dispatches by datapoint type and calls enrichDataPoint for
// every datapoint of the metric.
func (p *attributeCacheProcessor) applyToMetric(
	ctx context.Context,
	rm pmetric.ResourceMetrics,
	sm pmetric.ScopeMetrics,
	metric pmetric.Metric,
	table *matcher.LookupTable,
	resourceAttrs, scopeAttrs pcommon.Map,
) error {
	var errs error
	//exhaustive:enforce
	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		dps := metric.Gauge().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			errs = multierr.Append(errs, p.enrichDataPoint(ctx, rm, sm, metric, dp, dp.Attributes(), table, resourceAttrs, scopeAttrs))
		}
	case pmetric.MetricTypeSum:
		dps := metric.Sum().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			errs = multierr.Append(errs, p.enrichDataPoint(ctx, rm, sm, metric, dp, dp.Attributes(), table, resourceAttrs, scopeAttrs))
		}
	case pmetric.MetricTypeHistogram:
		dps := metric.Histogram().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			errs = multierr.Append(errs, p.enrichDataPoint(ctx, rm, sm, metric, dp, dp.Attributes(), table, resourceAttrs, scopeAttrs))
		}
	case pmetric.MetricTypeExponentialHistogram:
		dps := metric.ExponentialHistogram().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			errs = multierr.Append(errs, p.enrichDataPoint(ctx, rm, sm, metric, dp, dp.Attributes(), table, resourceAttrs, scopeAttrs))
		}
	case pmetric.MetricTypeSummary:
		dps := metric.Summary().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			errs = multierr.Append(errs, p.enrichDataPoint(ctx, rm, sm, metric, dp, dp.Attributes(), table, resourceAttrs, scopeAttrs))
		}
	}
	return errs
}

// enrichDataPoint applies the per-datapoint filter, performs the table
// lookup, and writes the matched row's enrichment columns onto the
// resource/scope/datapoint attribute maps.
func (p *attributeCacheProcessor) enrichDataPoint(
	ctx context.Context,
	rm pmetric.ResourceMetrics,
	sm pmetric.ScopeMetrics,
	metric pmetric.Metric,
	dp any,
	dpAttrs pcommon.Map,
	table *matcher.LookupTable,
	resourceAttrs, scopeAttrs pcommon.Map,
) error {
	if p.datapointFilter != nil {
		tCtx := ottldatapoint.NewTransformContextPtr(rm, sm, metric, dp)
		matched, err := p.datapointFilter.Eval(ctx, tCtx)
		tCtx.Close()
		if err != nil {
			if p.cfg.ErrorMode == ottl.PropagateError {
				return err
			}
		}
		if !matched {
			return nil
		}
	}

	matchAttrs := p.extractMatchAttrs(resourceAttrs, scopeAttrs, dpAttrs)
	start := time.Now()
	row := p.engine.Match(table, matchAttrs)
	matched := row != nil
	p.recordLookup(ctx, start, matched)
	if !matched {
		if p.noMatchLogger != nil {
			p.noMatchLogger.debugNoMatch(matchAttrs)
		}
		return nil
	}
	src := sourceAttrsFor(resourceAttrs, scopeAttrs, dpAttrs)
	p.enricher.Enrich(*row, src,
		pcommonAttributeMap{m: resourceAttrs},
		pcommonAttributeMap{m: scopeAttrs},
		pcommonAttributeMap{m: dpAttrs},
	)
	return nil
}
