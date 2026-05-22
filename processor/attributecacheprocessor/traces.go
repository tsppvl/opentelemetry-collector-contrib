// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package attributecacheprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributecacheprocessor"

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlresource"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspan"
)

// processTraces is the per-batch entry point for the traces signal. It
// evaluates the OTTL input filters, looks up the merged attribute set,
// and writes enrichment columns onto each Span.
func (p *attributeCacheProcessor) processTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	table := p.currentTable.Load()
	if table == nil || p.engine == nil || p.enricher == nil {
		return td, nil
	}

	var errs error
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		resourceAttrs := rs.Resource().Attributes()

		if p.spanResourceFilter != nil {
			tCtx := ottlresource.NewTransformContextPtr(rs.Resource(), rs)
			matched, err := p.spanResourceFilter.Eval(ctx, tCtx)
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

		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			ss := rs.ScopeSpans().At(j)
			scopeAttrs := ss.Scope().Attributes()

			for k := 0; k < ss.Spans().Len(); k++ {
				span := ss.Spans().At(k)
				if p.spanFilter != nil {
					tCtx := ottlspan.NewTransformContextPtr(rs, ss, span)
					matched, err := p.spanFilter.Eval(ctx, tCtx)
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

				itemAttrs := span.Attributes()
				matchAttrs := p.extractMatchAttrs(resourceAttrs, scopeAttrs, itemAttrs)
				start := time.Now()
				row := p.engine.Match(table, matchAttrs)
				matched := row != nil
				p.recordLookup(ctx, start, matched)
				if !matched {
					if p.noMatchLogger != nil {
						p.noMatchLogger.debugNoMatch(matchAttrs)
					}
					continue
				}
				src := sourceAttrsFor(resourceAttrs, scopeAttrs, itemAttrs)
				p.enricher.Enrich(*row, src,
					pcommonAttributeMap{m: resourceAttrs},
					pcommonAttributeMap{m: scopeAttrs},
					pcommonAttributeMap{m: itemAttrs},
				)
			}
		}
	}

	if errs != nil {
		p.logger.Debug("attributecache: errors while processing traces", zap.Error(errs))
	}
	return td, errs
}
