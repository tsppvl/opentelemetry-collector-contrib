// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package neo4jqueryreceiver

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/neo4jqueryreceiver/internal/metadata"
)

func TestNewFactory(t *testing.T) {
	factory := NewFactory()
	assert.Equal(t, metadata.Type, factory.Type())
	assert.Equal(t, component.StabilityLevelDevelopment, factory.MetricsStability())
	assert.Equal(t, component.StabilityLevelDevelopment, factory.LogsStability())
	assert.Equal(t, component.StabilityLevelUndefined, factory.TracesStability())
}

func TestCreateMetrics(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.URI = "bolt://localhost:7687"
	cfg.Queries = []Query{{
		Cypher:  "RETURN 1 AS one",
		Metrics: []MetricCfg{{MetricName: "one", ValueColumn: "one"}},
	}}

	rcvr, err := factory.CreateMetrics(t.Context(), receivertest.NewNopSettings(metadata.Type), cfg, consumertest.NewNop())
	require.NoError(t, err)
	require.NotNil(t, rcvr)
}

func TestCreateLogs(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.URI = "bolt://localhost:7687"
	cfg.Queries = []Query{{
		Cypher: "MATCH (l:Log) RETURN l.msg AS msg",
		Logs:   []LogsCfg{{BodyColumn: "msg"}},
	}}

	rcvr, err := factory.CreateLogs(t.Context(), receivertest.NewNopSettings(metadata.Type), cfg, consumertest.NewNop())
	require.NoError(t, err)
	require.NotNil(t, rcvr)
}
