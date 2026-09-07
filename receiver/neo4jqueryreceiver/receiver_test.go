// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package neo4jqueryreceiver

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componentstatus"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.opentelemetry.io/collector/scraper/scraperhelper"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/neo4jqueryreceiver/internal/metadata"
)

func testReceiverConfig(interval time.Duration) *Config {
	cc := scraperhelper.NewDefaultControllerConfig()
	cc.CollectionInterval = interval
	cc.InitialDelay = 0
	return &Config{
		ControllerConfig: cc,
		URI:              "bolt://localhost:7687",
		Queries: []Query{
			{
				Cypher:  "MATCH (m:Movie) RETURN m.genre AS genre, count(*) AS count",
				Metrics: []MetricCfg{{MetricName: "movie.genres", ValueColumn: "count", AttributeColumns: []string{"genre"}}},
			},
			{
				Cypher:  "MATCH (p:Person) RETURN count(p) AS people",
				Metrics: []MetricCfg{{MetricName: "people", ValueColumn: "people", ValueType: MetricValueTypeDouble}},
			},
		},
	}
}

func TestReceiver_EndToEndWithFakeClient(t *testing.T) {
	client := &fakeClient{rows: [][]row{{
		{"genre": "sci-fi", "count": int64(2), "people": 5.0},
	}}}
	sink := new(consumertest.MetricsSink)
	rcvr, err := createMetricsReceiverFunc(fakeFactory(client))(t.Context(), receivertest.NewNopSettings(metadata.Type), testReceiverConfig(10*time.Millisecond), sink)
	require.NoError(t, err)

	statusEvents := make(chan *componentstatus.Event, 100)
	host := &statusReporterHost{Host: componenttest.NewNopHost(), report: func(e *componentstatus.Event) {
		select {
		case statusEvents <- e:
		default:
		}
	}}
	require.NoError(t, rcvr.Start(t.Context(), host))

	require.Eventually(t, func() bool { return sink.DataPointCount() >= 2 }, 5*time.Second, 10*time.Millisecond)
	select {
	case e := <-statusEvents:
		assert.Equal(t, componentstatus.StatusOK, e.Status())
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for status event")
	}

	require.NoError(t, rcvr.Shutdown(t.Context()))
	assert.Equal(t, 1, client.closed, "the shared client is closed exactly once")

	names := map[string]bool{}
	for _, md := range sink.AllMetrics() {
		for i := 0; i < md.ResourceMetrics().Len(); i++ {
			sms := md.ResourceMetrics().At(i).ScopeMetrics()
			for j := 0; j < sms.Len(); j++ {
				for k := 0; k < sms.At(j).Metrics().Len(); k++ {
					names[sms.At(j).Metrics().At(k).Name()] = true
				}
			}
		}
	}
	assert.True(t, names["movie.genres"])
	assert.True(t, names["people"])
}

func TestReceiver_StartFailsWhenClientCannotBeCreated(t *testing.T) {
	factory := func(*Config, *zap.Logger) (dbClient, error) { return nil, errors.New("bad driver") }
	rcvr, err := createMetricsReceiverFunc(factory)(t.Context(), receivertest.NewNopSettings(metadata.Type), testReceiverConfig(time.Second), consumertest.NewNop())
	require.NoError(t, err)
	require.ErrorContains(t, rcvr.Start(t.Context(), componenttest.NewNopHost()), "bad driver")
}

func TestReceiver_RealDriverLifecycleWithoutServer(t *testing.T) {
	// The real driver connects lazily, so Start/Shutdown must work without a
	// reachable server and must not leak goroutines (checked by TestMain).
	cfg := testReceiverConfig(time.Hour)
	cfg.InitialDelay = time.Hour
	cfg.Username = "neo4j"
	cfg.Password = "secret"
	cfg.MaxConnectionPoolSize = 2
	rcvr, err := NewFactory().CreateMetrics(t.Context(), receivertest.NewNopSettings(metadata.Type), cfg, consumertest.NewNop())
	require.NoError(t, err)
	require.NoError(t, rcvr.Start(t.Context(), componenttest.NewNopHost()))
	require.NoError(t, rcvr.Shutdown(t.Context()))
}

func TestNewNeo4jClient_InvalidURI(t *testing.T) {
	_, err := newNeo4jClient(&Config{URI: "bolt://bad host name:7687"}, zap.NewNop())
	require.Error(t, err)
}
