// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package neo4jqueryreceiver

import (
	"context"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tcneo4j "github.com/testcontainers/testcontainers-go/modules/neo4j"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.opentelemetry.io/collector/scraper/scraperhelper"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/neo4jqueryreceiver/internal/metadata"
)

const integrationPassword = "integration-pass"

func TestIntegration(t *testing.T) {
	ctx := t.Context()

	ctr, err := tcneo4j.Run(ctx, "neo4j:5", tcneo4j.WithAdminPassword(integrationPassword))
	require.NoError(t, err, "start neo4j container")
	// Cleanup runs after t.Context() is cancelled, so use an uncancelled context there.
	cleanupCtx := context.WithoutCancel(ctx)
	t.Cleanup(func() {
		if terr := ctr.Terminate(cleanupCtx); terr != nil {
			t.Logf("neo4j container terminate: %v", terr)
		}
	})
	boltURL, err := ctr.BoltUrl(ctx)
	require.NoError(t, err)

	seedMovies(ctx, t, boltURL)

	cc := scraperhelper.NewDefaultControllerConfig()
	cc.CollectionInterval = 500 * time.Millisecond
	cc.InitialDelay = 0
	cc.Timeout = 10 * time.Second
	cfg := &Config{
		ControllerConfig: cc,
		URI:              boltURL,
		Username:         "neo4j",
		Password:         integrationPassword,
		Queries: []Query{
			{
				Cypher:     "MATCH (m:Movie) WHERE m.released >= $since RETURN m.genre AS genre, count(*) AS count ORDER BY genre",
				Parameters: map[string]any{"since": 1980},
				Metrics: []MetricCfg{{
					MetricName:       "movie.genres",
					ValueColumn:      "count",
					AttributeColumns: []string{"genre"},
					StaticAttributes: map[string]string{"source": "neo4j"},
				}},
			},
			{
				Cypher: "MATCH (m:Movie) RETURN avg(m.rating) AS avg_rating, max(m.released) AS newest, date('2026-01-01') AS d, datetime('2026-01-01T00:00:00Z') AS dt, null AS nothing",
				Metrics: []MetricCfg{
					{MetricName: "movie.rating.avg", ValueColumn: "avg_rating", ValueType: MetricValueTypeDouble, AttributeColumns: []string{"d", "dt"}},
					{MetricName: "movie.newest", ValueColumn: "newest", DataType: MetricTypeSum, Aggregation: MetricAggregationCumulative, TsColumn: "dt"},
				},
				IgnoreNullValues: true,
			},
		},
	}
	require.NoError(t, cfg.Validate())

	sink := new(consumertest.MetricsSink)
	rcvr, err := NewFactory().CreateMetrics(ctx, receivertest.NewNopSettings(metadata.Type), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, rcvr.Start(ctx, componenttest.NewNopHost()))
	t.Cleanup(func() { require.NoError(t, rcvr.Shutdown(cleanupCtx)) })

	require.Eventually(t, func() bool { return len(sink.AllMetrics()) >= 2 }, 30*time.Second, 100*time.Millisecond)

	// Logs with tracking: every movie must be delivered exactly once even
	// though the receiver keeps polling.
	logsCfg := &Config{
		ControllerConfig: cc,
		URI:              boltURL,
		Username:         "neo4j",
		Password:         integrationPassword,
		Queries: []Query{{
			Cypher:             "MATCH (m:Movie) WHERE m.released > $tracking_value RETURN m.released AS released, m.title AS title, m.genre AS genre ORDER BY m.released",
			TrackingColumn:     "released",
			TrackingStartValue: 1900,
			Logs:               []LogsCfg{{BodyColumn: "title", AttributeColumns: []string{"genre", "released"}}},
		}},
	}
	require.NoError(t, logsCfg.Validate())
	logsSink := new(consumertest.LogsSink)
	logsRcvr, err := NewFactory().CreateLogs(ctx, receivertest.NewNopSettings(metadata.Type), logsCfg, logsSink)
	require.NoError(t, err)
	require.NoError(t, logsRcvr.Start(ctx, componenttest.NewNopHost()))
	t.Cleanup(func() { require.NoError(t, logsRcvr.Shutdown(cleanupCtx)) })
	require.Eventually(t, func() bool { return logsSink.LogRecordCount() >= 4 }, 30*time.Second, 100*time.Millisecond)
	time.Sleep(3 * cc.CollectionInterval)
	assert.Equal(t, 4, logsSink.LogRecordCount(), "tracking prevents re-reading rows")
	var titles []string
	for _, ld := range logsSink.AllLogs() {
		recs := ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
		for i := 0; i < recs.Len(); i++ {
			titles = append(titles, recs.At(i).Body().Str())
			genre, ok := recs.At(i).Attributes().Get("genre")
			assert.True(t, ok)
			assert.NotEmpty(t, genre.Str())
		}
	}
	assert.Equal(t, []string{"Metropolis", "E.T.", "Die Hard", "The Matrix"}, titles)

	byName := map[string]pmetric.Metric{}
	for _, md := range sink.AllMetrics() {
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			sms := rms.At(i).ScopeMetrics()
			for j := 0; j < sms.Len(); j++ {
				assert.Equal(t, metadata.ScopeName, sms.At(j).Scope().Name())
				ms := sms.At(j).Metrics()
				for k := 0; k < ms.Len(); k++ {
					byName[ms.At(k).Name()] = ms.At(k)
				}
			}
		}
	}
	require.Contains(t, byName, "movie.genres")
	require.Contains(t, byName, "movie.rating.avg")
	require.Contains(t, byName, "movie.newest")

	genres := byName["movie.genres"].Gauge().DataPoints()
	require.Equal(t, 2, genres.Len())
	counts := map[string]int64{}
	for i := 0; i < genres.Len(); i++ {
		g, _ := genres.At(i).Attributes().Get("genre")
		src, _ := genres.At(i).Attributes().Get("source")
		assert.Equal(t, "neo4j", src.Str())
		counts[g.Str()] = genres.At(i).IntValue()
	}
	assert.Equal(t, map[string]int64{"action": 1, "sci-fi": 2}, counts)

	avg := byName["movie.rating.avg"].Gauge().DataPoints().At(0)
	assert.InDelta(t, 8.0, avg.DoubleValue(), 1e-9)
	d, _ := avg.Attributes().Get("d")
	assert.Equal(t, "2026-01-01", d.Str())
	dt, _ := avg.Attributes().Get("dt")
	assert.Equal(t, "2026-01-01T00:00:00Z", dt.Str())

	newest := byName["movie.newest"].Sum().DataPoints().At(0)
	assert.Equal(t, int64(1999), newest.IntValue())
	assert.Equal(t, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), newest.Timestamp().AsTime())
}

func seedMovies(ctx context.Context, t *testing.T, boltURL string) {
	t.Helper()
	drv, err := neo4j.NewDriverWithContext(boltURL, neo4j.BasicAuth("neo4j", integrationPassword, ""))
	require.NoError(t, err)
	defer func() { require.NoError(t, drv.Close(ctx)) }()

	_, err = neo4j.ExecuteQuery(ctx, drv, `
		CREATE (:Movie {title: 'E.T.', genre: 'sci-fi', released: 1982, rating: 7.9}),
		       (:Movie {title: 'The Matrix', genre: 'sci-fi', released: 1999, rating: 8.7}),
		       (:Movie {title: 'Die Hard', genre: 'action', released: 1988, rating: 8.2}),
		       (:Movie {title: 'Metropolis', genre: 'sci-fi', released: 1927, rating: 7.2})`,
		nil, neo4j.EagerResultTransformer)
	require.NoError(t, err)
}
