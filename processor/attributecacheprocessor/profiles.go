// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package attributecacheprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributecacheprocessor"

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlprofile"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlresource"
)

// processProfiles is the per-batch entry point for the profiles signal.
//
// Note: pprofile.Profile stores attributes via dictionary indices, not as a
// plain pcommon.Map. Iter-001 therefore matches and enriches profiles only at
// the resource and scope levels; item-level (per-Profile) enrichment requires
// attribute-table mutation and is intentionally deferred to a follow-up.
func (p *attributeCacheProcessor) processProfiles(ctx context.Context, pd pprofile.Profiles) (pprofile.Profiles, error) {
	table := matcherTable(p.cache.Current())
	if table == nil || p.engine == nil || p.enricher == nil {
		return pd, nil
	}

	dictionary := pd.Dictionary()

	var errs error
	for i := 0; i < pd.ResourceProfiles().Len(); i++ {
		rp := pd.ResourceProfiles().At(i)
		resourceAttrs := rp.Resource().Attributes()

		if p.profileResourceFilter != nil {
			tCtx := ottlresource.NewTransformContextPtr(rp.Resource(), rp)
			matched, err := p.profileResourceFilter.Eval(ctx, tCtx)
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

		for j := 0; j < rp.ScopeProfiles().Len(); j++ {
			sp := rp.ScopeProfiles().At(j)
			scopeAttrs := sp.Scope().Attributes()

			for k := 0; k < sp.Profiles().Len(); k++ {
				profile := sp.Profiles().At(k)
				if p.profileFilter != nil {
					tCtx := ottlprofile.NewTransformContextPtr(rp, sp, profile, dictionary)
					matched, err := p.profileFilter.Eval(ctx, tCtx)
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

				// Match at resource/scope level only — see package comment.
				emptyItem := pcommon.NewMap()
				matchAttrs := p.extractMatchAttrs(resourceAttrs, scopeAttrs, emptyItem)
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
				src := sourceAttrsFor(resourceAttrs, scopeAttrs, emptyItem)
				p.enricher.Enrich(*row, src,
					pcommonAttributeMap{m: resourceAttrs},
					pcommonAttributeMap{m: scopeAttrs},
					nil,
				)
			}
		}
	}

	if errs != nil {
		p.logger.Debug("attributecache: errors while processing profiles", zap.Error(errs))
	}
	return pd, errs
}
