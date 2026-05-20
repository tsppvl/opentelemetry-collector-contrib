// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package attributecacheprocessor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributecacheprocessor/internal/enricher"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributecacheprocessor/internal/matcher"
)

func validBaseConfig() *Config {
	return &Config{
		MatchMode:      MatchModeLinear,
		DefaultSymbol:  "**",
		MatchAllSymbol: "%%",
		NullSymbol:     "@@",
		Source: SourceConfig{
			Type: SourceTypeInline,
			Inline: &InlineConfig{
				Rows: []map[string]string{{"host": "h1", "env": "prod"}},
			},
		},
		Columns: []ColumnConfig{
			{Name: "host", Role: ColumnRoleMatch, MatchType: matcher.MatchTypeString},
			{Name: "env", Role: ColumnRoleEnrich, Target: enricher.LevelResource},
		},
	}
}

func TestConfig_Validate_OK(t *testing.T) {
	cfg := validBaseConfig()
	require.NoError(t, cfg.Validate())
}

func TestConfig_Validate_Errors(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{
			name:    "no source configured",
			mutate:  func(c *Config) { c.Source = SourceConfig{Type: SourceTypeInline} },
			wantErr: "source: exactly one of inline, csv, sql, neo4j must be configured",
		},
		{
			name: "two sources configured",
			mutate: func(c *Config) {
				c.Source.CSV = &CSVConfig{Path: "/tmp/x.csv"}
			},
			wantErr: "source: only one of inline, csv, sql, neo4j may be configured",
		},
		{
			name:    "no match column",
			mutate:  func(c *Config) { c.Columns[0].Role = ColumnRoleEnrich },
			wantErr: "at least one column with role=match is required",
		},
		{
			name:    "no enrich column",
			mutate:  func(c *Config) { c.Columns[1].Role = ColumnRoleMatch; c.Columns[1].MatchType = matcher.MatchTypeString },
			wantErr: "at least one column with role=enrich is required",
		},
		{
			name:    "match column missing match_type",
			mutate:  func(c *Config) { c.Columns[0].MatchType = "" },
			wantErr: "match_type is required for match columns",
		},
		{
			name:    "duplicate column name",
			mutate:  func(c *Config) { c.Columns[1].Name = "host" },
			wantErr: "duplicate column name",
		},
		{
			name:    "equal default and match_all symbols",
			mutate:  func(c *Config) { c.MatchAllSymbol = "**" },
			wantErr: "default_symbol and match_all_symbol must not be equal",
		},
		{
			name:    "negative refresh interval",
			mutate:  func(c *Config) { c.RefreshInterval = -1 },
			wantErr: "refresh_interval: must not be negative",
		},
		{
			name:    "unknown match mode",
			mutate:  func(c *Config) { c.MatchMode = "weird" },
			wantErr: "match_mode: \"weird\" is not recognized",
		},
		{
			name:    "optimized match mode not yet supported",
			mutate:  func(c *Config) { c.MatchMode = MatchModeOptimized },
			wantErr: "match_mode 'optimized' is not yet supported; use 'linear'",
		},
		{
			name:    "unknown source type",
			mutate:  func(c *Config) { c.Source.Type = "kafka" },
			wantErr: "is not a recognized source type",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validBaseConfig()
			tc.mutate(cfg)
			err := cfg.Validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}
