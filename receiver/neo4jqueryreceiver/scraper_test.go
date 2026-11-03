// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package neo4jqueryreceiver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componentstatus"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/scraper/scrapererror"
	"go.opentelemetry.io/collector/scraper/scraperhelper"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/neo4jqueryreceiver/internal/metadata"
)

// fakeClient is a dbClient that replays canned rows.
type fakeClient struct {
	rows       [][]row
	err        error
	calls      int
	lastCypher string
	lastParams map[string]any
	closed     int
}

func (f *fakeClient) queryRows(_ context.Context, cypher string, params map[string]any) ([]row, error) {
	f.calls++
	f.lastCypher = cypher
	f.lastParams = params
	if f.err != nil {
		return nil, f.err
	}
	if len(f.rows) == 0 {
		return nil, nil
	}
	idx := f.calls - 1
	if idx >= len(f.rows) {
		idx = len(f.rows) - 1
	}
	return f.rows[idx], nil
}

func (f *fakeClient) close(context.Context) error {
	f.closed++
	return nil
}

func fakeFactory(f *fakeClient) clientFactory {
	return func(*Config, *zap.Logger) (dbClient, error) { return f, nil }
}

func newTestScraper(t *testing.T, client *fakeClient, query Query, logger *zap.Logger) *queryScraper {
	t.Helper()
	cfg := &Config{URI: "bolt://localhost:7687", Queries: []Query{query}}
	conn := newConnection(cfg, fakeFactory(client), logger)
	require.NoError(t, conn.open())
	scrapeCfg := scraperhelper.NewDefaultControllerConfig()
	scrapeCfg.CollectionInterval = 30 * time.Second
	scope := pcommon.NewInstrumentationScope()
	scope.SetName(metadata.ScopeName)
	s := newQueryScraper(component.MustNewIDWithName("neo4j_query", "query-0"), query, scrapeCfg, conn, logger, scope)
	require.NoError(t, s.Start(t.Context(), componenttest.NewNopHost()))
	return s
}

func TestScraper_GaugeIntWithAttributes(t *testing.T) {
	client := &fakeClient{rows: [][]row{{
		{"genre": "sci-fi", "count": int64(2)},
		{"genre": "action", "count": int64(1)},
	}}}
	query := Query{
		Cypher:     "MATCH (m:Movie) RETURN m.genre AS genre, count(*) AS count",
		Parameters: map[string]any{"limit": 10},
		Metrics: []MetricCfg{{
			MetricName:       "movie.genres",
			ValueColumn:      "count",
			AttributeColumns: []string{"genre"},
			StaticAttributes: map[string]string{"db": "movies"},
			Unit:             "{movie}",
			Description:      "Movies per genre",
		}},
	}
	s := newTestScraper(t, client, query, zap.NewNop())

	md, err := s.ScrapeMetrics(t.Context())
	require.NoError(t, err)
	assert.Equal(t, query.Cypher, client.lastCypher)
	assert.Equal(t, query.Parameters, client.lastParams)

	require.Equal(t, 1, md.ResourceMetrics().Len())
	sm := md.ResourceMetrics().At(0).ScopeMetrics().At(0)
	assert.Equal(t, metadata.ScopeName, sm.Scope().Name())
	require.Equal(t, 1, sm.Metrics().Len())
	metric := sm.Metrics().At(0)
	assert.Equal(t, "movie.genres", metric.Name())
	assert.Equal(t, "{movie}", metric.Unit())
	assert.Equal(t, "Movies per genre", metric.Description())
	require.Equal(t, pmetric.MetricTypeGauge, metric.Type())
	dps := metric.Gauge().DataPoints()
	require.Equal(t, 2, dps.Len())

	assert.Equal(t, int64(2), dps.At(0).IntValue())
	genre, _ := dps.At(0).Attributes().Get("genre")
	assert.Equal(t, "sci-fi", genre.Str())
	db, _ := dps.At(0).Attributes().Get("db")
	assert.Equal(t, "movies", db.Str())
	assert.Equal(t, int64(1), dps.At(1).IntValue())
	assert.NotZero(t, dps.At(0).Timestamp())
	assert.Zero(t, dps.At(0).StartTimestamp(), "gauges carry no start timestamp")
}

func TestScraper_SumDoubleCumulativeAndDelta(t *testing.T) {
	client := &fakeClient{rows: [][]row{{{"avg": 2.5, "total": int64(9)}}}}
	query := Query{
		Cypher: "RETURN 2.5 AS avg, 9 AS total",
		Metrics: []MetricCfg{
			{MetricName: "avg", ValueColumn: "avg", ValueType: MetricValueTypeDouble, DataType: MetricTypeSum, Monotonic: true, Aggregation: MetricAggregationCumulative},
			{MetricName: "total", ValueColumn: "total", DataType: MetricTypeSum, Aggregation: MetricAggregationDelta},
		},
	}
	s := newTestScraper(t, client, query, zap.NewNop())

	md, err := s.ScrapeMetrics(t.Context())
	require.NoError(t, err)
	ms := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
	require.Equal(t, 2, ms.Len())

	avg := ms.At(0)
	require.Equal(t, pmetric.MetricTypeSum, avg.Type())
	assert.True(t, avg.Sum().IsMonotonic())
	assert.Equal(t, pmetric.AggregationTemporalityCumulative, avg.Sum().AggregationTemporality())
	avgDp := avg.Sum().DataPoints().At(0)
	assert.InDelta(t, 2.5, avgDp.DoubleValue(), 1e-9)
	assert.Equal(t, s.startTime, avgDp.StartTimestamp())

	total := ms.At(1)
	assert.False(t, total.Sum().IsMonotonic())
	assert.Equal(t, pmetric.AggregationTemporalityDelta, total.Sum().AggregationTemporality())
	totalDp := total.Sum().DataPoints().At(0)
	assert.Equal(t, int64(9), totalDp.IntValue())
	assert.Equal(t, totalDp.Timestamp().AsTime().Add(-30*time.Second), totalDp.StartTimestamp().AsTime())
}

func TestScraper_TimestampColumns(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC)
	client := &fakeClient{rows: [][]row{{{"v": int64(1), "start": start, "end": end.UnixNano()}}}}
	query := Query{
		Cypher: "RETURN 1",
		Metrics: []MetricCfg{{
			MetricName: "v", ValueColumn: "v", DataType: MetricTypeSum, Aggregation: MetricAggregationCumulative,
			StartTsColumn: "start", TsColumn: "end",
		}},
	}
	s := newTestScraper(t, client, query, zap.NewNop())

	md, err := s.ScrapeMetrics(t.Context())
	require.NoError(t, err)
	dp := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Sum().DataPoints().At(0)
	assert.Equal(t, pcommon.NewTimestampFromTime(start), dp.StartTimestamp())
	assert.Equal(t, pcommon.NewTimestampFromTime(end), dp.Timestamp())
}

func TestScraper_RowCondition(t *testing.T) {
	client := &fakeClient{rows: [][]row{{
		{"list": "databases", "items": int64(8)},
		{"list": "pools", "items": int64(4)},
		{"list": "users", "items": int64(2)},
	}}}
	query := Query{
		Cypher: "CALL something()",
		Metrics: []MetricCfg{
			{MetricName: "pools", ValueColumn: "items", RowCondition: &RowCondition{Column: "list", Value: "pools"}},
			{MetricName: "users", ValueColumn: "items", RowCondition: &RowCondition{Column: "list", Value: "users"}},
			{MetricName: "none", ValueColumn: "items", RowCondition: &RowCondition{Column: "list", Value: "missing"}},
			{MetricName: "nocol", ValueColumn: "items", RowCondition: &RowCondition{Column: "nope", Value: "x"}},
		},
	}
	s := newTestScraper(t, client, query, zap.NewNop())

	md, err := s.ScrapeMetrics(t.Context())
	require.NoError(t, err)
	ms := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
	require.Equal(t, 2, ms.Len(), "metrics without matching rows are not emitted")
	assert.Equal(t, "pools", ms.At(0).Name())
	assert.Equal(t, int64(4), ms.At(0).Gauge().DataPoints().At(0).IntValue())
	assert.Equal(t, "users", ms.At(1).Name())
	assert.Equal(t, int64(2), ms.At(1).Gauge().DataPoints().At(0).IntValue())
}

func TestScraper_PartialErrors(t *testing.T) {
	client := &fakeClient{rows: [][]row{{
		{"v": int64(1), "name": "ok"},
		{"v": "not-a-number", "name": "bad-value"},
		{"v": int64(3)},
		{"v": int64(4), "name": nil},
	}}}
	query := Query{
		Cypher:  "RETURN ...",
		Metrics: []MetricCfg{{MetricName: "v", ValueColumn: "v", AttributeColumns: []string{"name"}}},
	}
	s := newTestScraper(t, client, query, zap.NewNop())

	md, err := s.ScrapeMetrics(t.Context())
	require.Error(t, err)
	var partial scrapererror.PartialScrapeError
	require.ErrorAs(t, err, &partial)
	assert.Equal(t, 3, partial.Failed)
	assert.ErrorContains(t, err, "row 1")
	assert.ErrorContains(t, err, "row 2")
	assert.ErrorContains(t, err, "row 3")
	assert.ErrorContains(t, err, errNullValue.Error())

	dps := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints()
	require.Equal(t, 1, dps.Len(), "only the valid row produces a data point")
	assert.Equal(t, int64(1), dps.At(0).IntValue())
}

func TestScraper_MissingValueColumnAndEmptyResult(t *testing.T) {
	client := &fakeClient{rows: [][]row{{{"other": int64(1)}}, {}}}
	query := Query{Cypher: "RETURN 1", Metrics: []MetricCfg{{MetricName: "v", ValueColumn: "v"}}}
	s := newTestScraper(t, client, query, zap.NewNop())

	_, err := s.ScrapeMetrics(t.Context())
	require.ErrorContains(t, err, `value_column "v" not found in result set`)

	md, err := s.ScrapeMetrics(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 0, md.MetricCount(), "an empty result set yields no metrics")
}

func TestScraper_ClientError(t *testing.T) {
	client := &fakeClient{err: errors.New("connection refused")}
	query := Query{Cypher: "RETURN 1", Metrics: []MetricCfg{{MetricName: "v", ValueColumn: "v"}}}
	s := newTestScraper(t, client, query, zap.NewNop())

	_, err := s.ScrapeMetrics(t.Context())
	require.ErrorContains(t, err, "connection refused")
	var partial scrapererror.PartialScrapeError
	assert.NotErrorAs(t, err, &partial, "a failed query is a full scrape error")
}

func TestScraper_NullWarning(t *testing.T) {
	rows := [][]row{{{"v": int64(1), "unused": nil, "other": nil}}}
	query := Query{Cypher: "RETURN 1", Metrics: []MetricCfg{{MetricName: "v", ValueColumn: "v"}}}

	core, logs := observer.New(zap.WarnLevel)
	s := newTestScraper(t, &fakeClient{rows: rows}, query, zap.New(core))
	_, err := s.ScrapeMetrics(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, logs.Len())
	entry := logs.All()[0]
	assert.Contains(t, entry.Message, "null values")
	assert.Equal(t, []any{"other", "unused"}, entry.ContextMap()["columns"])

	query.IgnoreNullValues = true
	core, logs = observer.New(zap.WarnLevel)
	s = newTestScraper(t, &fakeClient{rows: rows}, query, zap.New(core))
	_, err = s.ScrapeMetrics(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 0, logs.Len(), "ignore_null_values suppresses the warning")
}

func TestScraper_Timeout(t *testing.T) {
	blocking := &blockingClient{}
	cfg := &Config{URI: "bolt://localhost:7687"}
	conn := newConnection(cfg, func(*Config, *zap.Logger) (dbClient, error) { return blocking, nil }, zap.NewNop())
	require.NoError(t, conn.open())
	scrapeCfg := scraperhelper.NewDefaultControllerConfig()
	scrapeCfg.Timeout = 20 * time.Millisecond
	query := Query{Cypher: "RETURN 1", Metrics: []MetricCfg{{MetricName: "v", ValueColumn: "v"}}}
	s := newQueryScraper(component.MustNewIDWithName("neo4j_query", "q"), query, scrapeCfg, conn, zap.NewNop(), pcommon.NewInstrumentationScope())
	require.NoError(t, s.Start(t.Context(), componenttest.NewNopHost()))

	_, err := s.ScrapeMetrics(t.Context())
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

// blockingClient waits until the context is cancelled.
type blockingClient struct{}

func (blockingClient) queryRows(ctx context.Context, _ string, _ map[string]any) ([]row, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (blockingClient) close(context.Context) error { return nil }

func TestScraper_StatusReporting(t *testing.T) {
	client := &fakeClient{rows: [][]row{{{"v": int64(1)}}}}
	query := Query{Cypher: "RETURN 1", Metrics: []MetricCfg{{MetricName: "v", ValueColumn: "v"}}}
	s := newTestScraper(t, client, query, zap.NewNop())

	var events []*componentstatus.Event
	host := &statusReporterHost{Host: componenttest.NewNopHost(), report: func(e *componentstatus.Event) { events = append(events, e) }}
	require.NoError(t, s.Start(t.Context(), host))

	_, err := s.ScrapeMetrics(t.Context())
	require.NoError(t, err)
	client.err = errors.New("boom")
	_, err = s.ScrapeMetrics(t.Context())
	require.Error(t, err)

	require.Len(t, events, 2)
	assert.Equal(t, componentstatus.StatusOK, events[0].Status())
	assert.Equal(t, componentstatus.StatusRecoverableError, events[1].Status())
	assert.ErrorContains(t, events[1].Err(), "boom")
}

func TestConnection_NotOpen(t *testing.T) {
	conn := newConnection(&Config{}, fakeFactory(&fakeClient{}), zap.NewNop())
	_, err := conn.queryRows(t.Context(), "RETURN 1", nil)
	require.ErrorContains(t, err, "not open")
	require.NoError(t, conn.close(t.Context()), "closing a never-opened connection is a no-op")
}

func TestConnection_OpenIsIdempotentAndCloseReleasesOnce(t *testing.T) {
	client := &fakeClient{}
	calls := 0
	conn := newConnection(&Config{}, func(*Config, *zap.Logger) (dbClient, error) { calls++; return client, nil }, zap.NewNop())
	require.NoError(t, conn.open())
	require.NoError(t, conn.open())
	assert.Equal(t, 1, calls)
	require.NoError(t, conn.close(t.Context()))
	require.NoError(t, conn.close(t.Context()))
	assert.Equal(t, 1, client.closed)
}

func TestConnection_FactoryError(t *testing.T) {
	conn := newConnection(&Config{}, func(*Config, *zap.Logger) (dbClient, error) { return nil, errors.New("bad uri") }, zap.NewNop())
	require.ErrorContains(t, conn.open(), "bad uri")
}

type statusReporterHost struct {
	component.Host
	report func(*componentstatus.Event)
}

func (h *statusReporterHost) Report(event *componentstatus.Event) {
	if h.report != nil {
		h.report(event)
	}
}
