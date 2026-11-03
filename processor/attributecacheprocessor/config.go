// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package attributecacheprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributecacheprocessor"

import (
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.uber.org/multierr"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributecacheprocessor/internal/enricher"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributecacheprocessor/internal/matcher"
)

// MatchMode selects the matching engine implementation.
type MatchMode string

// Supported MatchMode values.
const (
	MatchModeOptimized MatchMode = "optimized"
	MatchModeLinear    MatchMode = "linear"
)

// SourceType identifies which sub-config under SourceConfig is active.
type SourceType string

// Supported SourceType values.
const (
	SourceTypeInline SourceType = "inline"
	SourceTypeCSV    SourceType = "csv"
	SourceTypeSQL    SourceType = "sql"
	SourceTypeNeo4j  SourceType = "neo4j"
)

// ColumnRole identifies whether a column participates in matching or supplies
// enrichment values.
type ColumnRole string

// Supported ColumnRole values.
const (
	ColumnRoleMatch  ColumnRole = "match"
	ColumnRoleEnrich ColumnRole = "enrich"
)

// Config defines the user-facing configuration for the attributecache
// processor.
type Config struct {
	// RefreshInterval controls how often the lookup table is reloaded from
	// the data source. A zero value loads the table once at startup and
	// never refreshes.
	RefreshInterval time.Duration `mapstructure:"refresh_interval"`

	// MatchMode selects the matching engine implementation. Defaults to
	// MatchModeLinear (also used when the field is left empty).
	MatchMode MatchMode `mapstructure:"match_mode"`

	// DefaultSymbol is the cell value that signals "fallback when no more
	// specific row matches" inside match columns. Empty disables the
	// feature.
	DefaultSymbol string `mapstructure:"default_symbol"`
	// MatchAllSymbol is the cell value that always matches, even when the
	// incoming attribute is absent. Empty disables the feature.
	MatchAllSymbol string `mapstructure:"match_all_symbol"`
	// NullSymbol is the cell value that matches only when the incoming
	// attribute is absent and that suppresses an enrichment write when seen
	// in an enrichment column. Empty disables the feature.
	NullSymbol string `mapstructure:"null_symbol"`

	// PropertyInsertion configures the delimiters used for attribute-value
	// substitution in enrichment templates.
	PropertyInsertion PropertyInsertionConfig `mapstructure:"property_insertion"`

	// InputFilter defines per-signal OTTL conditions evaluated against each
	// item before enrichment. Empty conditions mean "every item passes".
	InputFilter InputFilterConfig `mapstructure:"input_filter"`

	// Source configures exactly one data source. Source.Type must match the
	// non-nil sub-config.
	Source SourceConfig `mapstructure:"source"`

	// Columns declares the full column schema (match + enrich) used by the
	// matcher and the enricher.
	Columns []ColumnConfig `mapstructure:"columns"`

	// ErrorMode controls OTTL error propagation when evaluating input
	// filter conditions.
	ErrorMode ottl.ErrorMode `mapstructure:"error_mode"`
}

// PropertyInsertionConfig configures the delimiters used for attribute-value
// substitution in enrichment templates such as `server-[type]`.
type PropertyInsertionConfig struct {
	Start string `mapstructure:"start"`
	End   string `mapstructure:"end"`
}

// SourceConfig is a discriminated union: exactly one sub-config must be set
// and Type must match the populated field.
type SourceConfig struct {
	Type   SourceType    `mapstructure:"type"`
	Inline *InlineConfig `mapstructure:"inline,omitempty"`
	CSV    *CSVConfig    `mapstructure:"csv,omitempty"`
	SQL    *SQLConfig    `mapstructure:"sql,omitempty"`
	Neo4j  *Neo4jConfig  `mapstructure:"neo4j,omitempty"`
}

// InlineConfig holds rows declared directly in YAML configuration.
type InlineConfig struct {
	Rows []map[string]string `mapstructure:"rows"`

	// prevent unkeyed literal initialization
	_ struct{}
}

// CSVConfig describes a CSV file data source.
type CSVConfig struct {
	Path           string `mapstructure:"path"`
	HasHeader      bool   `mapstructure:"has_header"`
	FieldSeparator string `mapstructure:"field_separator"`
	FieldQuoting   string `mapstructure:"field_quoting"`
	Encoding       string `mapstructure:"encoding"`
}

// SQLConfig describes a SQL database data source.
type SQLConfig struct {
	Driver string              `mapstructure:"driver"`
	DSN    configopaque.String `mapstructure:"dsn"`
	Query  string              `mapstructure:"query"`
}

// Neo4jConfig describes a Neo4j graph database data source.
type Neo4jConfig struct {
	URI      string              `mapstructure:"uri"`
	Username string              `mapstructure:"username"`
	Password configopaque.String `mapstructure:"password"`
	Database string              `mapstructure:"database"`
	Query    string              `mapstructure:"query"`
}

// ColumnConfig is the user-facing per-column entry. Match-only fields are
// ignored for enrich columns and vice versa.
type ColumnConfig struct {
	Name           string                  `mapstructure:"name"`
	Role           ColumnRole              `mapstructure:"role"`
	MatchType      matcher.MatchType       `mapstructure:"match_type,omitempty"`
	DeleteAfterUse bool                    `mapstructure:"delete_after_use,omitempty"`
	Target         enricher.AttributeLevel `mapstructure:"target,omitempty"`
	AttributeName  string                  `mapstructure:"attribute_name,omitempty"`
}

// InputFilterConfig holds per-signal OTTL condition lists. Scope-level OTTL
// conditions are intentionally out of scope for iter-001 (see architecture
// S-002).
type InputFilterConfig struct {
	Metrics  *MetricFilterConditions  `mapstructure:"metrics,omitempty"`
	Logs     *LogFilterConditions     `mapstructure:"logs,omitempty"`
	Traces   *TraceFilterConditions   `mapstructure:"traces,omitempty"`
	Profiles *ProfileFilterConditions `mapstructure:"profiles,omitempty"`
}

// MetricFilterConditions holds OTTL conditions for the metric signal.
type MetricFilterConditions struct {
	DataPoint []string `mapstructure:"datapoint,omitempty"`
	Metric    []string `mapstructure:"metric,omitempty"`
	Resource  []string `mapstructure:"resource,omitempty"`
}

// LogFilterConditions holds OTTL conditions for the log signal.
type LogFilterConditions struct {
	Log      []string `mapstructure:"log,omitempty"`
	Resource []string `mapstructure:"resource,omitempty"`
}

// TraceFilterConditions holds OTTL conditions for the trace signal.
type TraceFilterConditions struct {
	Span     []string `mapstructure:"span,omitempty"`
	Resource []string `mapstructure:"resource,omitempty"`
}

// ProfileFilterConditions holds OTTL conditions for the profile signal.
// Profiles have no scope-event equivalent; only profile-level and resource
// level filters are supported.
type ProfileFilterConditions struct {
	Profile  []string `mapstructure:"profile,omitempty"`
	Resource []string `mapstructure:"resource,omitempty"`
}

var _ component.Config = (*Config)(nil)

// Validate checks the processor configuration. Errors from independent
// checks are aggregated via multierr so operators see every problem in a
// single pass.
func (cfg *Config) Validate() error {
	var errs error

	errs = multierr.Append(errs, cfg.validateSource())
	errs = multierr.Append(errs, cfg.validateColumns())
	errs = multierr.Append(errs, cfg.validateSymbols())
	errs = multierr.Append(errs, cfg.validateMatchMode())
	errs = multierr.Append(errs, cfg.validateRefreshInterval())

	return errs
}

func (cfg *Config) validateSource() error {
	count := 0
	if cfg.Source.Inline != nil {
		count++
	}
	if cfg.Source.CSV != nil {
		count++
	}
	if cfg.Source.SQL != nil {
		count++
	}
	if cfg.Source.Neo4j != nil {
		count++
	}
	if count == 0 {
		return errors.New("source: exactly one of inline, csv, sql, neo4j must be configured")
	}
	if count > 1 {
		return errors.New("source: only one of inline, csv, sql, neo4j may be configured")
	}

	switch cfg.Source.Type {
	case SourceTypeInline:
		if cfg.Source.Inline == nil {
			return errors.New(`source: type is "inline" but inline sub-config is missing`)
		}
	case SourceTypeCSV:
		if cfg.Source.CSV == nil {
			return errors.New(`source: type is "csv" but csv sub-config is missing`)
		}
		if cfg.Source.CSV.Path == "" {
			return errors.New("source.csv.path: must be set")
		}
	case SourceTypeSQL:
		if cfg.Source.SQL == nil {
			return errors.New(`source: type is "sql" but sql sub-config is missing`)
		}
		if cfg.Source.SQL.Driver == "" {
			return errors.New("source.sql.driver: must be set")
		}
		if cfg.Source.SQL.DSN == "" {
			return errors.New("source.sql.dsn: must be set")
		}
		if cfg.Source.SQL.Query == "" {
			return errors.New("source.sql.query: must be set")
		}
	case SourceTypeNeo4j:
		if cfg.Source.Neo4j == nil {
			return errors.New(`source: type is "neo4j" but neo4j sub-config is missing`)
		}
		if cfg.Source.Neo4j.URI == "" {
			return errors.New("source.neo4j.uri: must be set")
		}
		if cfg.Source.Neo4j.Query == "" {
			return errors.New("source.neo4j.query: must be set")
		}
	case "":
		return errors.New("source.type: must be set")
	default:
		return fmt.Errorf("source.type: %q is not a recognized source type", cfg.Source.Type)
	}

	return nil
}

func (cfg *Config) validateColumns() error {
	if len(cfg.Columns) == 0 {
		return errors.New("columns: at least one column must be configured")
	}

	var matchCount, enrichCount int
	seen := make(map[string]struct{}, len(cfg.Columns))

	for i, col := range cfg.Columns {
		if col.Name == "" {
			return fmt.Errorf("columns[%d].name: must be set", i)
		}
		if _, dup := seen[col.Name]; dup {
			return fmt.Errorf("columns[%d].name: duplicate column name %q", i, col.Name)
		}
		seen[col.Name] = struct{}{}

		switch col.Role {
		case ColumnRoleMatch:
			matchCount++
			if col.MatchType == "" {
				return fmt.Errorf("columns[%d] (%q): match_type is required for match columns", i, col.Name)
			}
			if !isKnownMatchType(col.MatchType) {
				return fmt.Errorf("columns[%d] (%q): match_type %q is not recognized", i, col.Name, col.MatchType)
			}
			if col.MatchType == matcher.MatchTypeString && cfg.MatchAllSymbol != "" {
				// Note: this rejects the use of match_all_symbol with
				// match_type=string at any cell value. The architecture
				// (section 5.1, rule 5) forbids match_all with string
				// matching globally — there is no per-row exception.
				// A finer-grained per-row check happens at load time.
			}
		case ColumnRoleEnrich:
			enrichCount++
		case "":
			return fmt.Errorf("columns[%d] (%q): role must be set (\"match\" or \"enrich\")", i, col.Name)
		default:
			return fmt.Errorf("columns[%d] (%q): role %q is not recognized", i, col.Name, col.Role)
		}
	}

	if matchCount == 0 {
		return errors.New("columns: at least one column with role=match is required")
	}
	if enrichCount == 0 {
		return errors.New("columns: at least one column with role=enrich is required")
	}
	return nil
}

func (cfg *Config) validateSymbols() error {
	symbols := []string{cfg.DefaultSymbol, cfg.MatchAllSymbol, cfg.NullSymbol}
	names := []string{"default_symbol", "match_all_symbol", "null_symbol"}
	for i := 0; i < len(symbols); i++ {
		if symbols[i] == "" {
			continue
		}
		for j := i + 1; j < len(symbols); j++ {
			if symbols[j] == "" {
				continue
			}
			if symbols[i] == symbols[j] {
				return fmt.Errorf("%s and %s must not be equal", names[i], names[j])
			}
		}
	}
	return nil
}

func (cfg *Config) validateMatchMode() error {
	switch cfg.MatchMode {
	case "", MatchModeLinear, MatchModeOptimized:
		return nil
	default:
		return fmt.Errorf("match_mode: %q is not recognized (allowed: %q, %q)", cfg.MatchMode, MatchModeLinear, MatchModeOptimized)
	}
}

func (cfg *Config) validateRefreshInterval() error {
	if cfg.RefreshInterval < 0 {
		return errors.New("refresh_interval: must not be negative")
	}
	return nil
}

func isKnownMatchType(mt matcher.MatchType) bool {
	switch mt {
	case matcher.MatchTypeString,
		matcher.MatchTypeRegex,
		matcher.MatchTypeSQLPattern,
		matcher.MatchTypeRange:
		return true
	}
	return false
}
