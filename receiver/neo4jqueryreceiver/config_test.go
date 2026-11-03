// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package neo4jqueryreceiver

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"go.opentelemetry.io/collector/scraper/scraperhelper"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/neo4jqueryreceiver/internal/metadata"
)

var fileStorageID = component.MustNewID("file_storage")

func TestLoadConfig(t *testing.T) {
	t.Parallel()

	defaultController := scraperhelper.NewDefaultControllerConfig()
	defaultController.CollectionInterval = 10 * time.Second

	tests := []struct {
		fname         string
		expected      component.Config
		errorMessages []string
	}{
		{
			fname: "config.yaml",
			expected: &Config{
				ControllerConfig:      defaultController,
				URI:                   "bolt://localhost:7687",
				Username:              "neo4j",
				Password:              "s3cr3t",
				Database:              "movies",
				MaxConnectionPoolSize: 5,
				Telemetry:             TelemetryConfig{Logs: TelemetryLogsConfig{Query: true}},
				Queries: []Query{
					{
						Cypher:     "MATCH (m:Movie) WHERE m.released >= $since RETURN m.genre AS genre, count(*) AS count",
						Parameters: map[string]any{"since": 2000},
						Metrics: []MetricCfg{
							{
								MetricName:       "movie.count",
								ValueColumn:      "count",
								AttributeColumns: []string{"genre"},
								Monotonic:        false,
								ValueType:        MetricValueTypeInt,
								DataType:         MetricTypeSum,
								Aggregation:      MetricAggregationCumulative,
								StaticAttributes: map[string]string{"foo": "bar"},
							},
						},
					},
				},
			},
		},
		{
			fname: "config-minimal.yaml",
			expected: &Config{
				ControllerConfig: defaultController,
				URI:              "neo4j+s://graph.example.com",
				Queries: []Query{
					{
						Cypher: "MATCH (n) RETURN count(n) AS nodes",
						Metrics: []MetricCfg{
							{MetricName: "graph.nodes", ValueColumn: "nodes"},
						},
					},
				},
			},
		},
		{
			fname: "config-logs.yaml",
			expected: &Config{
				ControllerConfig: defaultController,
				URI:              "bolt://localhost:7687",
				Username:         "neo4j",
				Password:         "s3cr3t",
				StorageID:        &fileStorageID,
				Queries: []Query{
					{
						Cypher:             "MATCH (e:Event) WHERE e.id > $tracking_value RETURN e.id AS id, e.message AS message, e.level AS level, e.at AS at ORDER BY e.id",
						TrackingColumn:     "id",
						TrackingStartValue: 10000,
						Logs:               []LogsCfg{{BodyColumn: "message", AttributeColumns: []string{"level"}, TsColumn: "at"}},
					},
					{
						Cypher:             "MATCH (e:Event) WHERE e.at > datetime($since) RETURN e.message AS message, e.at AS at ORDER BY e.at",
						TrackingColumn:     "at",
						TrackingParameter:  "since",
						TrackingStartValue: "1970-01-01T00:00:00Z",
						Logs:               []LogsCfg{{BodyColumn: "message"}},
					},
				},
			},
		},
		{fname: "config-invalid-missing-uri.yaml", errorMessages: []string{"'uri' cannot be empty"}},
		{fname: "config-invalid-uri-scheme.yaml", errorMessages: []string{"unsupported scheme \"http\""}},
		{fname: "config-invalid-uri-no-host.yaml", errorMessages: []string{"'uri' must contain a host"}},
		{fname: "config-invalid-password-without-username.yaml", errorMessages: []string{"'username' must be set when 'password' is set"}},
		{fname: "config-invalid-pool-size.yaml", errorMessages: []string{"'max_connection_pool_size' cannot be negative"}},
		{fname: "config-invalid-missing-queries.yaml", errorMessages: []string{"'queries' cannot be empty"}},
		{fname: "config-invalid-missing-cypher.yaml", errorMessages: []string{"'cypher' cannot be empty"}},
		{fname: "config-invalid-missing-metrics.yaml", errorMessages: []string{"at least one of 'logs' and 'metrics' must not be empty"}},
		{fname: "config-invalid-missing-body-column.yaml", errorMessages: []string{"'body_column' must not be empty"}},
		{fname: "config-invalid-tracking-without-column.yaml", errorMessages: []string{"'tracking_start_value' requires 'tracking_column'", "'tracking_parameter' requires 'tracking_column'"}},
		{fname: "config-invalid-missing-metricname.yaml", errorMessages: []string{"'metric_name' cannot be empty"}},
		{fname: "config-invalid-missing-valuecolumn.yaml", errorMessages: []string{"'value_column' cannot be empty", "invalid metric config with metric_name 'one'"}},
		{fname: "config-invalid-valuetype.yaml", errorMessages: []string{"unsupported value_type: 'blob'"}},
		{fname: "config-invalid-datatype.yaml", errorMessages: []string{"unsupported data_type: 'histogram'"}},
		{fname: "config-invalid-aggregation.yaml", errorMessages: []string{"unsupported aggregation: 'sometimes'"}},
		{fname: "config-invalid-gauge-aggregation.yaml", errorMessages: []string{"aggregation=delta but data_type=gauge does not support aggregation"}},
		{fname: "config-invalid-row-condition.yaml", errorMessages: []string{"'row_condition.column' cannot be empty"}},
		{
			fname: "config-invalid-multierr.yaml",
			errorMessages: []string{
				"unsupported scheme \"ftp\"",
				"'cypher' cannot be empty",
				"'metric_name' cannot be empty",
				"'value_column' cannot be empty",
				"unsupported value_type: 'blob'",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.fname, func(t *testing.T) {
			cm, err := confmaptest.LoadConf(filepath.Join("testdata", tt.fname))
			require.NoError(t, err)

			factory := NewFactory()
			cfg := factory.CreateDefaultConfig()

			sub, err := cm.Sub(component.NewIDWithName(metadata.Type, "").String())
			require.NoError(t, err)
			require.NoError(t, sub.Unmarshal(cfg))

			err = confmap.Validate(cfg)
			if len(tt.errorMessages) > 0 {
				require.Error(t, err)
				for _, msg := range tt.errorMessages {
					assert.ErrorContains(t, err, msg)
				}
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expected, cfg)
		})
	}
}

func TestConfigValidate_Defaults(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	assert.Equal(t, 10*time.Second, cfg.CollectionInterval)
	assert.Equal(t, time.Second, cfg.InitialDelay)
	require.Error(t, cfg.Validate(), "an empty default config must not validate")
}

func TestQueryTrackingParameterDefault(t *testing.T) {
	q := Query{}
	assert.Equal(t, "tracking_value", q.trackingParameter())
	q.TrackingParameter = "since"
	assert.Equal(t, "since", q.trackingParameter())
}
