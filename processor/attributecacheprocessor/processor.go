// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package attributecacheprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributecacheprocessor"

import (
	"context"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/filter/expr"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottldatapoint"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlmetric"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlprofile"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlresource"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspan"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributecacheprocessor/internal/cache"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributecacheprocessor/internal/enricher"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributecacheprocessor/internal/matcher"
)

// attributeCacheProcessor holds the shared state for every signal-specific
// processor instance: the lookup-table cache, match engine, enricher, and
// per-signal OTTL filter expressions.
type attributeCacheProcessor struct {
	cfg      *Config
	logger   *zap.Logger
	settings component.TelemetrySettings
	tel      *telemetry

	// noMatchLogger rate-limits debug log lines for items that did not match
	// any lookup-table row, suppressing log floods on large unmatched batches.
	noMatchLogger *rateLimitedLogger

	// cache holds the lookup table snapshot and runs the optional background
	// refresh ticker.
	cache *cache.Cache

	// currentTable holds the most-recently converted *matcher.LookupTable.
	// It is updated by the cache OnSuccess hook so that every signal
	// processor gets a stable pointer for each table snapshot. The
	// OptimizedEngine uses pointer equality to detect when the table has
	// changed and the trie must be rebuilt.
	currentTable atomic.Pointer[matcher.LookupTable]

	// engine evaluates incoming attributes against the lookup table and
	// returns the best-matching row.
	engine matcher.MatchEngine
	// enricher applies row values to the telemetry attribute maps.
	enricher *enricher.Enricher
	// matchColumnNames lists the match-column attribute keys; used to gather
	// match attributes from the various pdata levels.
	matchColumnNames []string

	// Per-signal OTTL input filters. nil filters mean "every item passes".
	metricResourceFilter expr.BoolExpr[*ottlresource.TransformContext]
	metricFilter         expr.BoolExpr[*ottlmetric.TransformContext]
	datapointFilter      expr.BoolExpr[*ottldatapoint.TransformContext]

	logResourceFilter expr.BoolExpr[*ottlresource.TransformContext]
	logFilter         expr.BoolExpr[*ottllog.TransformContext]

	spanResourceFilter expr.BoolExpr[*ottlresource.TransformContext]
	spanFilter         expr.BoolExpr[*ottlspan.TransformContext]

	profileResourceFilter expr.BoolExpr[*ottlresource.TransformContext]
	profileFilter         expr.BoolExpr[*ottlprofile.TransformContext]
}

func newAttributeCacheProcessor(set component.TelemetrySettings, cfg *Config, tel *telemetry, c *cache.Cache) *attributeCacheProcessor {
	return &attributeCacheProcessor{
		cfg:           cfg,
		logger:        set.Logger,
		settings:      set,
		tel:           tel,
		cache:         c,
		noMatchLogger: &rateLimitedLogger{logger: set.Logger},
	}
}

// Start performs the initial table load and launches the background refresh
// goroutine when a refresh interval is configured.
func (p *attributeCacheProcessor) Start(ctx context.Context, _ component.Host) error {
	if p.cache != nil {
		if err := p.cache.Start(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Shutdown stops the background refresh goroutine and closes the underlying
// data source.
func (p *attributeCacheProcessor) Shutdown(ctx context.Context) error {
	if p.cache != nil {
		return p.cache.Shutdown(ctx)
	}
	return nil
}

// pcommonAttributeMap adapts pcommon.Map to enricher.AttributeMap.
type pcommonAttributeMap struct{ m pcommon.Map }

func (w pcommonAttributeMap) PutStr(k, v string) { w.m.PutStr(k, v) }

func (w pcommonAttributeMap) Remove(k string) bool { return w.m.Remove(k) }

// matcherTable converts the cache snapshot into the matcher's view. Both
// types are structurally identical; the conversion is a thin wrapper that
// reuses the same underlying row maps without copying values.
func matcherTable(t *cache.LookupTable) *matcher.LookupTable {
	if t == nil {
		return nil
	}
	rows := make([]matcher.Row, len(t.Rows))
	for i := range t.Rows {
		rows[i] = matcher.Row(t.Rows[i])
	}
	return &matcher.LookupTable{Columns: t.Columns, Rows: rows}
}

// extractMatchAttrs collects values for the configured match columns from
// the supplied attribute levels. Item-level values override scope-level,
// which override resource-level. Only keys named in matchColumnNames are
// retained — this both bounds the map size and avoids paying for attribute
// values the matcher will never read.
func (p *attributeCacheProcessor) extractMatchAttrs(resourceAttrs, scopeAttrs, itemAttrs pcommon.Map) map[string]string {
	if len(p.matchColumnNames) == 0 {
		return nil
	}
	result := make(map[string]string, len(p.matchColumnNames))
	for _, name := range p.matchColumnNames {
		if v, ok := resourceAttrs.Get(name); ok {
			result[name] = v.AsString()
		}
		if v, ok := scopeAttrs.Get(name); ok {
			result[name] = v.AsString()
		}
		if v, ok := itemAttrs.Get(name); ok {
			result[name] = v.AsString()
		}
	}
	return result
}

// sourceAttrsFor returns the attribute map used as the source for
// property-insertion template expansion. Item-level values take precedence
// over scope and resource. The returned map contains the union of all keys.
func sourceAttrsFor(resourceAttrs, scopeAttrs, itemAttrs pcommon.Map) map[string]string {
	result := make(map[string]string, resourceAttrs.Len()+scopeAttrs.Len()+itemAttrs.Len())
	resourceAttrs.Range(func(k string, v pcommon.Value) bool {
		result[k] = v.AsString()
		return true
	})
	scopeAttrs.Range(func(k string, v pcommon.Value) bool {
		result[k] = v.AsString()
		return true
	})
	itemAttrs.Range(func(k string, v pcommon.Value) bool {
		result[k] = v.AsString()
		return true
	})
	return result
}

// recordLookup records per-item telemetry: lookup latency, items_processed,
// and either items_enriched or items_passthrough depending on whether the
// engine produced a match. start must be captured immediately before calling
// engine.Match().
func (p *attributeCacheProcessor) recordLookup(ctx context.Context, start time.Time, matched bool) {
	p.tel.recordItemProcessed(ctx)
	p.tel.recordLookupDuration(ctx, time.Since(start))
	if matched {
		p.tel.recordItemEnriched(ctx)
	} else {
		p.tel.recordItemPassthrough(ctx)
	}
}

// rateLimitedLogger suppresses repeated "no match" debug log lines to at
// most one per second per processor instance. This prevents flooding the log
// when large batches contain many unmatched items.
type rateLimitedLogger struct {
	logger  *zap.Logger
	lastLog atomic.Int64 // unix nanoseconds of the last emitted log
}

// debugNoMatch emits a debug-level "no match" log at most once per second.
func (r *rateLimitedLogger) debugNoMatch(attrs map[string]string) {
	now := time.Now().UnixNano()
	if now-r.lastLog.Load() < int64(time.Second) {
		return
	}
	r.lastLog.Store(now)
	r.logger.Debug("attributecache: no match", zap.Any("match_attrs", attrs))
}
