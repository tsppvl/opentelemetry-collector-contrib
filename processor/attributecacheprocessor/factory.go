// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package attributecacheprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributecacheprocessor"

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/xconsumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"
	"go.opentelemetry.io/collector/processor/processorhelper/xprocessorhelper"
	"go.opentelemetry.io/collector/processor/xprocessor"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/filter/filterottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributecacheprocessor/internal/cache"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributecacheprocessor/internal/datasource"
	// Blank import to register all six SQL drivers (clickhouse, mysql, pgx,
	// sqlserver, oracle, sqlite). The drivers package only contains blank
	// imports; importing it here makes them available to database/sql.
	_ "github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributecacheprocessor/internal/datasource/drivers"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributecacheprocessor/internal/enricher"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributecacheprocessor/internal/matcher"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributecacheprocessor/internal/metadata"
)

// processorCapabilities marks the processor as one that mutates pipeline
// data. Enrichment writes new attributes onto incoming items in place.
var processorCapabilities = consumer.Capabilities{MutatesData: true}

// NewFactory returns a new processor factory for the attributecache
// processor. xprocessor.NewFactory is used (rather than processor.NewFactory)
// because the processor supports the profiles signal in addition to traces,
// metrics, and logs.
func NewFactory() processor.Factory {
	return xprocessor.NewFactory(
		metadata.Type,
		createDefaultConfig,
		xprocessor.WithMetrics(createMetricsProcessor, metadata.MetricsStability),
		xprocessor.WithLogs(createLogsProcessor, metadata.LogsStability),
		xprocessor.WithTraces(createTracesProcessor, metadata.TracesStability),
		xprocessor.WithProfiles(createProfilesProcessor, metadata.ProfilesStability),
	)
}

// createDefaultConfig returns the configuration with safe, conservative
// defaults. Operators must always supply a source and column schema; the
// processor cannot do anything useful with the defaults alone, but the
// defaults still produce a valid Config that mdatagen lifecycle tests
// override at construction time.
func createDefaultConfig() component.Config {
	return &Config{
		RefreshInterval: 0,
		MatchMode:       MatchModeLinear,
		DefaultSymbol:   "**",
		MatchAllSymbol:  "%%",
		NullSymbol:      "@@",
		PropertyInsertion: PropertyInsertionConfig{
			Start: "[",
			End:   "]",
		},
		ErrorMode: ottl.PropagateError,
	}
}

func newProcessorFromConfig(set processor.Settings, cfg component.Config) (*attributeCacheProcessor, error) {
	oCfg, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("invalid config type: expected *Config, got %T", cfg)
	}
	tel, err := newTelemetry(set)
	if err != nil {
		return nil, fmt.Errorf("create telemetry: %w", err)
	}
	ds, err := createDataSource(oCfg, set)
	if err != nil {
		return nil, fmt.Errorf("create data source: %w", err)
	}

	// pPtr is set after the processor is constructed so that the OnSuccess
	// hook can reference it. The hook is only ever called from cache.Start()
	// or the refresh goroutine, both of which execute after this function
	// returns (i.e. after pPtr is assigned), so the dereference is safe.
	var pPtr *attributeCacheProcessor
	hooks := cache.RefreshHooks{
		OnSuccess: func(ctx context.Context, rowCount int64) {
			tel.recordRefreshSuccess(ctx, rowCount)
			// Publish a stable *matcher.LookupTable for the current snapshot
			// so the OptimizedEngine can detect table changes via pointer
			// equality without per-call allocation.
			pPtr.currentTable.Store(matcherTable(pPtr.cache.Current()))
		},
		OnError: func(ctx context.Context) {
			tel.recordRefreshError(ctx)
		},
	}
	c := cache.New(ds, oCfg.RefreshInterval, set.TelemetrySettings.Logger, hooks)
	p := newAttributeCacheProcessor(set.TelemetrySettings, oCfg, tel, c)
	pPtr = p

	if err := buildEngineAndEnricher(p, oCfg); err != nil {
		return nil, err
	}
	if err := compileInputFilters(p, oCfg, set); err != nil {
		return nil, err
	}
	return p, nil
}

// buildEngineAndEnricher constructs the match engine and enricher from the
// declarative column configuration.
func buildEngineAndEnricher(p *attributeCacheProcessor, cfg *Config) error {
	matchCols := make([]matcher.ColumnConfig, 0, len(cfg.Columns))
	matchNames := make([]string, 0, len(cfg.Columns))
	enrichSpecs := make([]enricher.EnrichmentSpec, 0, len(cfg.Columns))
	deleteAfterUse := make([]string, 0)

	for _, col := range cfg.Columns {
		switch col.Role {
		case ColumnRoleMatch:
			matchCols = append(matchCols, matcher.ColumnConfig{Name: col.Name, MatchType: col.MatchType})
			matchNames = append(matchNames, col.Name)
		case ColumnRoleEnrich:
			target := col.Target
			if target == "" {
				target = enricher.LevelAttribute
			}
			attrName := col.AttributeName
			if attrName == "" {
				attrName = col.Name
			}
			enrichSpecs = append(enrichSpecs, enricher.EnrichmentSpec{
				SourceColumn: col.Name,
				TargetName:   attrName,
				Level:        target,
			})
		}
		if col.DeleteAfterUse {
			deleteAfterUse = append(deleteAfterUse, col.Name)
		}
	}

	matchCfg := matcher.MatchConfig{
		Columns:        matchCols,
		DefaultSymbol:  cfg.DefaultSymbol,
		MatchAllSymbol: cfg.MatchAllSymbol,
		NullSymbol:     cfg.NullSymbol,
	}
	if cfg.MatchMode == MatchModeOptimized {
		// NewOptimizedEngine with nil table defers trie construction to the
		// first Match call after the cache loads (via currentTable).
		p.engine = matcher.NewOptimizedEngine(nil, matchCfg)
	} else {
		p.engine = matcher.NewLinearEngine(matchCfg)
	}
	p.matchColumnNames = matchNames

	p.enricher = &enricher.Enricher{
		Columns:        enrichSpecs,
		InsertStart:    cfg.PropertyInsertion.Start,
		InsertEnd:      cfg.PropertyInsertion.End,
		NullSymbol:     cfg.NullSymbol,
		DeleteAfterUse: deleteAfterUse,
	}
	return nil
}

// compileInputFilters parses the per-signal OTTL conditions once at startup.
// Empty/nil condition slices leave the corresponding filter as nil; the
// signal consumers treat a nil filter as "every item passes".
func compileInputFilters(p *attributeCacheProcessor, cfg *Config, set processor.Settings) error {
	var err error
	telSet := set.TelemetrySettings

	if cfg.InputFilter.Metrics != nil {
		mf := cfg.InputFilter.Metrics
		if len(mf.Resource) > 0 {
			if p.metricResourceFilter, err = filterottl.NewBoolExprForResource(mf.Resource, nil, cfg.ErrorMode, telSet); err != nil {
				return fmt.Errorf("input_filter.metrics.resource: %w", err)
			}
		}
		if len(mf.Metric) > 0 {
			if p.metricFilter, err = filterottl.NewBoolExprForMetric(mf.Metric, nil, cfg.ErrorMode, telSet); err != nil {
				return fmt.Errorf("input_filter.metrics.metric: %w", err)
			}
		}
		if len(mf.DataPoint) > 0 {
			if p.datapointFilter, err = filterottl.NewBoolExprForDataPoint(mf.DataPoint, nil, cfg.ErrorMode, telSet); err != nil {
				return fmt.Errorf("input_filter.metrics.datapoint: %w", err)
			}
		}
	}
	if cfg.InputFilter.Logs != nil {
		lf := cfg.InputFilter.Logs
		if len(lf.Resource) > 0 {
			if p.logResourceFilter, err = filterottl.NewBoolExprForResource(lf.Resource, nil, cfg.ErrorMode, telSet); err != nil {
				return fmt.Errorf("input_filter.logs.resource: %w", err)
			}
		}
		if len(lf.Log) > 0 {
			if p.logFilter, err = filterottl.NewBoolExprForLog(lf.Log, nil, cfg.ErrorMode, telSet); err != nil {
				return fmt.Errorf("input_filter.logs.log: %w", err)
			}
		}
	}
	if cfg.InputFilter.Traces != nil {
		tf := cfg.InputFilter.Traces
		if len(tf.Resource) > 0 {
			if p.spanResourceFilter, err = filterottl.NewBoolExprForResource(tf.Resource, nil, cfg.ErrorMode, telSet); err != nil {
				return fmt.Errorf("input_filter.traces.resource: %w", err)
			}
		}
		if len(tf.Span) > 0 {
			if p.spanFilter, err = filterottl.NewBoolExprForSpan(tf.Span, nil, cfg.ErrorMode, telSet); err != nil {
				return fmt.Errorf("input_filter.traces.span: %w", err)
			}
		}
	}
	if cfg.InputFilter.Profiles != nil {
		pf := cfg.InputFilter.Profiles
		if len(pf.Resource) > 0 {
			if p.profileResourceFilter, err = filterottl.NewBoolExprForResource(pf.Resource, nil, cfg.ErrorMode, telSet); err != nil {
				return fmt.Errorf("input_filter.profiles.resource: %w", err)
			}
		}
		if len(pf.Profile) > 0 {
			if p.profileFilter, err = filterottl.NewBoolExprForProfile(pf.Profile, nil, cfg.ErrorMode, telSet); err != nil {
				return fmt.Errorf("input_filter.profiles.profile: %w", err)
			}
		}
	}
	return nil
}

// createDataSource builds the concrete datasource.DataSource matching the
// configured Source.Type. All four source types (inline, csv, sql, neo4j)
// are implemented as of T-009.
func createDataSource(cfg *Config, set processor.Settings) (datasource.DataSource, error) {
	switch cfg.Source.Type {
	case SourceTypeInline:
		return datasource.NewInline(&datasource.InlineConfig{Rows: cfg.Source.Inline.Rows}), nil
	case SourceTypeCSV:
		return datasource.NewCSV(&datasource.CSVConfig{
			Path:           cfg.Source.CSV.Path,
			HasHeader:      cfg.Source.CSV.HasHeader,
			FieldSeparator: cfg.Source.CSV.FieldSeparator,
			FieldQuoting:   cfg.Source.CSV.FieldQuoting,
			Encoding:       cfg.Source.CSV.Encoding,
		}), nil
	case SourceTypeSQL:
		return datasource.NewSQL(&datasource.SQLConfig{
			Driver: cfg.Source.SQL.Driver,
			DSN:    string(cfg.Source.SQL.DSN),
			Query:  cfg.Source.SQL.Query,
		}, set.TelemetrySettings.Logger), nil
	case SourceTypeNeo4j:
		return datasource.NewNeo4j(&datasource.Neo4jConfig{
			URI:      cfg.Source.Neo4j.URI,
			Username: cfg.Source.Neo4j.Username,
			Password: string(cfg.Source.Neo4j.Password),
			Database: cfg.Source.Neo4j.Database,
			Query:    cfg.Source.Neo4j.Query,
		}, set.TelemetrySettings.Logger), nil
	default:
		return nil, fmt.Errorf("source type %q not recognized", cfg.Source.Type)
	}
}

func createMetricsProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Metrics,
) (processor.Metrics, error) {
	p, err := newProcessorFromConfig(set, cfg)
	if err != nil {
		return nil, err
	}
	return processorhelper.NewMetrics(
		ctx, set, cfg, nextConsumer,
		p.processMetrics,
		processorhelper.WithCapabilities(processorCapabilities),
		processorhelper.WithStart(p.Start),
		processorhelper.WithShutdown(p.Shutdown),
	)
}

func createLogsProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Logs,
) (processor.Logs, error) {
	p, err := newProcessorFromConfig(set, cfg)
	if err != nil {
		return nil, err
	}
	return processorhelper.NewLogs(
		ctx, set, cfg, nextConsumer,
		p.processLogs,
		processorhelper.WithCapabilities(processorCapabilities),
		processorhelper.WithStart(p.Start),
		processorhelper.WithShutdown(p.Shutdown),
	)
}

func createTracesProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Traces,
) (processor.Traces, error) {
	p, err := newProcessorFromConfig(set, cfg)
	if err != nil {
		return nil, err
	}
	return processorhelper.NewTraces(
		ctx, set, cfg, nextConsumer,
		p.processTraces,
		processorhelper.WithCapabilities(processorCapabilities),
		processorhelper.WithStart(p.Start),
		processorhelper.WithShutdown(p.Shutdown),
	)
}

func createProfilesProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer xconsumer.Profiles,
) (xprocessor.Profiles, error) {
	p, err := newProcessorFromConfig(set, cfg)
	if err != nil {
		return nil, err
	}
	return xprocessorhelper.NewProfiles(
		ctx, set, cfg, nextConsumer,
		p.processProfiles,
		xprocessorhelper.WithCapabilities(processorCapabilities),
		xprocessorhelper.WithStart(p.Start),
		xprocessorhelper.WithShutdown(p.Shutdown),
	)
}
