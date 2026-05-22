// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package attributecacheprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributecacheprocessor"

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlresource"
)

// processLogs is the per-batch entry point for the logs signal. It
// evaluates the OTTL input filters, looks up the merged attribute set,
// and writes enrichment columns onto each LogRecord.
func (p *attributeCacheProcessor) processLogs(ctx context.Context, ld plog.Logs) (plog.Logs, error) {
	table := p.currentTable.Load()
	if table == nil || p.engine == nil || p.enricher == nil {
		return ld, nil
	}

	var errs error
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		resourceAttrs := rl.Resource().Attributes()

		if p.logResourceFilter != nil {
			tCtx := ottlresource.NewTransformContextPtr(rl.Resource(), rl)
			matched, err := p.logResourceFilter.Eval(ctx, tCtx)
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

		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			scopeAttrs := sl.Scope().Attributes()

			for k := 0; k < sl.LogRecords().Len(); k++ {
				lr := sl.LogRecords().At(k)
				if p.logFilter != nil {
					tCtx := ottllog.NewTransformContextPtr(rl, sl, lr)
					matched, err := p.logFilter.Eval(ctx, tCtx)
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

				itemAttrs := lr.Attributes()
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
		p.logger.Debug("attributecache: errors while processing logs", zap.Error(errs))
	}
	return ld, errs
}
