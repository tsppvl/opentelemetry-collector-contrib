// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package neo4jqueryreceiver

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componentstatus"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/extension/xextension/storage"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.opentelemetry.io/collector/scraper/scraperhelper"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/neo4jqueryreceiver/internal/metadata"
)

// memStorage is an in-memory storage extension used to test tracking persistence.
type memStorage struct {
	mu   sync.Mutex
	data map[string][]byte
}

func newMemStorage() *memStorage { return &memStorage{data: map[string][]byte{}} }

func (*memStorage) Start(context.Context, component.Host) error { return nil }
func (*memStorage) Shutdown(context.Context) error              { return nil }
func (s *memStorage) GetClient(context.Context, component.Kind, component.ID, string) (storage.Client, error) {
	return s, nil
}

func (s *memStorage) Get(_ context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.data[key], nil
}

func (s *memStorage) Set(_ context.Context, key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
	return nil
}

func (s *memStorage) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
	return nil
}

func (*memStorage) Batch(context.Context, ...*storage.Operation) error { return nil }
func (*memStorage) Close(context.Context) error                        { return nil }

var storageID = component.MustNewID("file_storage")

// nopComponent is an extension that is not a storage extension.
type nopComponent struct{}

func (nopComponent) Start(context.Context, component.Host) error { return nil }
func (nopComponent) Shutdown(context.Context) error              { return nil }

type storageHost struct {
	component.Host
	ext component.Component
}

func (h *storageHost) GetExtensions() map[component.ID]component.Component {
	return map[component.ID]component.Component{storageID: h.ext}
}

func logsTestConfig(interval time.Duration, query Query) *Config {
	cc := scraperhelper.NewDefaultControllerConfig()
	cc.CollectionInterval = interval
	cc.InitialDelay = 0
	return &Config{ControllerConfig: cc, URI: "bolt://localhost:7687", Queries: []Query{query}}
}

func newTestLogsQueryReceiver(t *testing.T, client dbClient, query Query, st storage.Client) *logsQueryReceiver {
	t.Helper()
	conn := newConnection(&Config{}, func(*Config, *zap.Logger) (dbClient, error) { return client, nil }, zap.NewNop())
	require.NoError(t, conn.open())
	qr := newLogsQueryReceiver("query-0: "+query.Cypher, query, conn, zap.NewNop(), st)
	qr.start(t.Context())
	return qr
}

func TestLogsQuery_BodyAttributesAndTimestamp(t *testing.T) {
	when := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	client := &fakeClient{rows: [][]row{{
		{"msg": "hello", "level": "INFO", "n": int64(1), "at": when},
		{"msg": map[string]any{"k": "v"}, "level": "WARN", "n": int64(2), "at": when},
	}}}
	query := Query{
		Cypher: "MATCH (l:Log) RETURN l.msg AS msg, l.level AS level, l.n AS n, l.at AS at",
		Logs:   []LogsCfg{{BodyColumn: "msg", AttributeColumns: []string{"level", "n"}, TsColumn: "at"}},
	}
	qr := newTestLogsQueryReceiver(t, client, query, nil)

	logs, err := qr.collect(t.Context())
	require.NoError(t, err)
	assert.Nil(t, client.lastParams, "no tracking: parameters passed through unchanged")
	require.Equal(t, 2, logs.LogRecordCount())
	sl := logs.ResourceLogs().At(0).ScopeLogs().At(0)
	assert.Equal(t, metadata.ScopeName, sl.Scope().Name())

	first := sl.LogRecords().At(0)
	assert.Equal(t, "hello", first.Body().Str())
	level, _ := first.Attributes().Get("level")
	assert.Equal(t, "INFO", level.Str())
	n, _ := first.Attributes().Get("n")
	assert.Equal(t, "1", n.Str())
	assert.Equal(t, pcommon.NewTimestampFromTime(when), first.Timestamp())
	assert.NotZero(t, first.ObservedTimestamp())

	second := sl.LogRecords().At(1)
	assert.Equal(t, `{"k":"v"}`, second.Body().Str(), "complex bodies are rendered as JSON")
}

func TestLogsQuery_RowErrors(t *testing.T) {
	client := &fakeClient{rows: [][]row{{
		{"msg": "ok", "level": "INFO"},
		{"msg": nil, "level": "INFO"},
		{"level": "INFO"},
		{"msg": "no-level"},
	}}}
	query := Query{Cypher: "q", Logs: []LogsCfg{{BodyColumn: "msg", AttributeColumns: []string{"level"}}}}
	qr := newTestLogsQueryReceiver(t, client, query, nil)

	logs, err := qr.collect(t.Context())
	require.Error(t, err)
	assert.ErrorContains(t, err, "row 1")
	assert.ErrorContains(t, err, errNullValue.Error())
	assert.ErrorContains(t, err, `body_column "msg" not found`)
	assert.ErrorContains(t, err, `attribute_column "level" not found`)
	require.Equal(t, 1, logs.LogRecordCount(), "only the valid row produces a record")
	assert.Equal(t, "ok", logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().Str())
}

func TestLogsQuery_ClientError(t *testing.T) {
	qr := newTestLogsQueryReceiver(t, &fakeClient{err: errors.New("boom")}, Query{Cypher: "q", Logs: []LogsCfg{{BodyColumn: "msg"}}}, nil)
	_, err := qr.collect(t.Context())
	require.ErrorContains(t, err, "boom")
}

func TestLogsQuery_TrackingWithoutStorage(t *testing.T) {
	client := &fakeClient{rows: [][]row{
		{{"id": int64(11), "msg": "a"}, {"id": int64(12), "msg": "b"}},
		{},
		{{"id": int64(13), "msg": "c"}},
	}}
	query := Query{
		Cypher:             "MATCH (l:Log) WHERE l.id > $tracking_value RETURN l.id AS id, l.msg AS msg ORDER BY l.id",
		Parameters:         map[string]any{"limit": 100},
		Logs:               []LogsCfg{{BodyColumn: "msg"}},
		TrackingColumn:     "id",
		TrackingStartValue: 10,
	}
	qr := newTestLogsQueryReceiver(t, client, query, nil)

	logs, err := qr.collect(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 2, logs.LogRecordCount())
	assert.Equal(t, map[string]any{"limit": 100, "tracking_value": int64(10)}, client.lastParams, "start value is normalized to int64 and merged with parameters")
	assert.Equal(t, map[string]any{"limit": 100}, query.Parameters, "configured parameters are not mutated")

	logs, err = qr.collect(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 0, logs.LogRecordCount())
	assert.Equal(t, int64(12), client.lastParams["tracking_value"], "last row of the previous run is the new tracking value")

	_, err = qr.collect(t.Context())
	require.NoError(t, err)
	assert.Equal(t, int64(12), client.lastParams["tracking_value"], "an empty result keeps the tracking value")
	assert.Equal(t, int64(13), qr.trackingValue)
}

func TestLogsQuery_TrackingCustomParameterAndMissingColumn(t *testing.T) {
	client := &fakeClient{rows: [][]row{{{"msg": "a"}}}}
	query := Query{
		Cypher:             "q",
		Logs:               []LogsCfg{{BodyColumn: "msg"}},
		TrackingColumn:     "id",
		TrackingParameter:  "since",
		TrackingStartValue: "2026-01-01T00:00:00Z",
	}
	qr := newTestLogsQueryReceiver(t, client, query, nil)
	logs, err := qr.collect(t.Context())
	require.ErrorContains(t, err, `tracking_column "id" not found`)
	assert.Equal(t, 1, logs.LogRecordCount(), "records are still delivered")
	assert.Equal(t, "2026-01-01T00:00:00Z", client.lastParams["since"])
	assert.Equal(t, "2026-01-01T00:00:00Z", qr.trackingValue, "tracking value is unchanged when the column is missing")
}

func TestLogsQuery_TrackingPersistedAndRestored(t *testing.T) {
	st := newMemStorage()
	when := time.Date(2026, 5, 6, 7, 8, 9, 0, time.UTC)
	client := &fakeClient{rows: [][]row{{{"ts": when, "msg": "a"}}}}
	query := Query{Cypher: "q", Logs: []LogsCfg{{BodyColumn: "msg"}}, TrackingColumn: "ts", TrackingStartValue: "1970-01-01T00:00:00Z"}

	qr := newTestLogsQueryReceiver(t, client, query, st)
	_, err := qr.collect(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "2026-05-06T07:08:09Z", qr.trackingValue, "temporal values are normalized to their string form")
	assert.JSONEq(t, `"2026-05-06T07:08:09Z"`, string(st.data[qr.storageKey]))

	// A "restarted" query receiver picks the persisted value up instead of tracking_start_value.
	restarted := newTestLogsQueryReceiver(t, client, query, st)
	assert.Equal(t, "2026-05-06T07:08:09Z", restarted.trackingValue)
	_, err = restarted.collect(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "2026-05-06T07:08:09Z", client.lastParams["tracking_value"])

	// Numbers survive the round trip with their integer type.
	client = &fakeClient{rows: [][]row{{{"id": int64(1 << 60), "msg": "a"}}}}
	query = Query{Cypher: "q2", Logs: []LogsCfg{{BodyColumn: "msg"}}, TrackingColumn: "id"}
	qr = newTestLogsQueryReceiver(t, client, query, st)
	_, err = qr.collect(t.Context())
	require.NoError(t, err)
	restarted = newTestLogsQueryReceiver(t, client, query, st)
	assert.Equal(t, int64(1<<60), restarted.trackingValue)
}

func TestDecodeTrackingValue(t *testing.T) {
	assert.Equal(t, int64(42), decodeTrackingValue([]byte("42")))
	assert.InDelta(t, 4.5, decodeTrackingValue([]byte("4.5")), 1e-9)
	assert.Equal(t, "abc", decodeTrackingValue([]byte(`"abc"`)))
	assert.Equal(t, true, decodeTrackingValue([]byte("true")))
	assert.Equal(t, "raw-legacy", decodeTrackingValue([]byte("raw-legacy")), "non-JSON payloads are returned as strings")
	assert.Equal(t, "[1,2]", decodeTrackingValue([]byte("[1,2]")), "unsupported JSON shapes fall back to the raw string")
}

func TestNormalizeTrackingValue(t *testing.T) {
	assert.Nil(t, normalizeTrackingValue(nil))
	assert.Equal(t, int64(3), normalizeTrackingValue(3))
	assert.Equal(t, int64(3), normalizeTrackingValue(int32(3)))
	assert.Equal(t, float64(2.5), normalizeTrackingValue(float32(2.5)))
	assert.Equal(t, "x", normalizeTrackingValue("x"))
	assert.Equal(t, true, normalizeTrackingValue(true))
	assert.Equal(t, `["a"]`, normalizeTrackingValue([]any{"a"}))
}

func TestLogsReceiver_EndToEnd(t *testing.T) {
	client := &fakeClient{rows: [][]row{{{"id": int64(1), "msg": "first"}, {"id": int64(2), "msg": "second"}}, {}}}
	query := Query{
		Cypher:             "MATCH (l:Log) WHERE l.id > $tracking_value RETURN l.id AS id, l.msg AS msg",
		Logs:               []LogsCfg{{BodyColumn: "msg", AttributeColumns: []string{"id"}}},
		Metrics:            []MetricCfg{{MetricName: "ignored-by-logs", ValueColumn: "id"}},
		TrackingColumn:     "id",
		TrackingStartValue: 0,
	}
	cfg := logsTestConfig(10*time.Millisecond, query)
	cfg.StorageID = &storageID
	st := newMemStorage()

	sink := new(consumertest.LogsSink)
	rcvr, err := createLogsReceiverFunc(fakeFactory(client))(t.Context(), receivertest.NewNopSettings(metadata.Type), cfg, sink)
	require.NoError(t, err)

	statusEvents := make(chan *componentstatus.Event, 100)
	host := &statusReporterHost{Host: &storageHost{Host: componenttest.NewNopHost(), ext: st}, report: func(e *componentstatus.Event) {
		select {
		case statusEvents <- e:
		default:
		}
	}}
	require.NoError(t, rcvr.Start(t.Context(), host))
	require.NoError(t, rcvr.Start(t.Context(), host), "second start is ignored")

	require.Eventually(t, func() bool { return sink.LogRecordCount() >= 2 && client.calls >= 3 }, 5*time.Second, 5*time.Millisecond)
	select {
	case e := <-statusEvents:
		assert.Equal(t, componentstatus.StatusOK, e.Status())
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for status event")
	}

	require.NoError(t, rcvr.Shutdown(t.Context()))
	require.NoError(t, rcvr.Shutdown(t.Context()), "second shutdown is ignored")
	assert.Equal(t, 1, client.closed)

	assert.Equal(t, 2, sink.LogRecordCount(), "rows are delivered once thanks to tracking")
	assert.Equal(t, int64(2), client.lastParams["tracking_value"])
	assert.JSONEq(t, "2", string(st.data["query-0: "+query.Cypher+".trackingValue"]), "tracking value persisted through the storage extension")
	rec := sink.AllLogs()[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	assert.Equal(t, "first", rec.Body().Str())
}

func TestLogsReceiver_ErrorStatusAndNoRecordsOnFailure(t *testing.T) {
	client := &fakeClient{err: errors.New("connection refused")}
	cfg := logsTestConfig(10*time.Millisecond, Query{Cypher: "q", Logs: []LogsCfg{{BodyColumn: "msg"}}})
	sink := new(consumertest.LogsSink)
	rcvr, err := createLogsReceiverFunc(fakeFactory(client))(t.Context(), receivertest.NewNopSettings(metadata.Type), cfg, sink)
	require.NoError(t, err)

	statusEvents := make(chan *componentstatus.Event, 100)
	host := &statusReporterHost{Host: componenttest.NewNopHost(), report: func(e *componentstatus.Event) {
		select {
		case statusEvents <- e:
		default:
		}
	}}
	require.NoError(t, rcvr.Start(t.Context(), host))
	select {
	case e := <-statusEvents:
		assert.Equal(t, componentstatus.StatusRecoverableError, e.Status())
		assert.ErrorContains(t, e.Err(), "connection refused")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for status event")
	}
	require.NoError(t, rcvr.Shutdown(t.Context()))
	assert.Equal(t, 0, sink.LogRecordCount())
}

func TestLogsReceiver_StartErrors(t *testing.T) {
	cfg := logsTestConfig(time.Second, Query{Cypher: "q", Logs: []LogsCfg{{BodyColumn: "msg"}}})

	t.Run("client factory fails", func(t *testing.T) {
		factory := func(*Config, *zap.Logger) (dbClient, error) { return nil, errors.New("bad driver") }
		rcvr, err := createLogsReceiverFunc(factory)(t.Context(), receivertest.NewNopSettings(metadata.Type), cfg, consumertest.NewNop())
		require.NoError(t, err)
		require.ErrorContains(t, rcvr.Start(t.Context(), componenttest.NewNopHost()), "bad driver")
		require.NoError(t, rcvr.Shutdown(t.Context()))
	})

	t.Run("storage extension missing", func(t *testing.T) {
		withStorage := *cfg
		withStorage.StorageID = &storageID
		rcvr, err := createLogsReceiverFunc(fakeFactory(&fakeClient{}))(t.Context(), receivertest.NewNopSettings(metadata.Type), &withStorage, consumertest.NewNop())
		require.NoError(t, err)
		require.ErrorContains(t, rcvr.Start(t.Context(), componenttest.NewNopHost()), "storage extension 'file_storage' not found")
	})

	t.Run("extension is not a storage extension", func(t *testing.T) {
		withStorage := *cfg
		withStorage.StorageID = &storageID
		rcvr, err := createLogsReceiverFunc(fakeFactory(&fakeClient{}))(t.Context(), receivertest.NewNopSettings(metadata.Type), &withStorage, consumertest.NewNop())
		require.NoError(t, err)
		host := &storageHost{Host: componenttest.NewNopHost(), ext: nopComponent{}}
		err = rcvr.Start(t.Context(), host)
		require.ErrorContains(t, err, "non-storage extension")
	})
}

func TestLogsReceiver_Timeout(t *testing.T) {
	cfg := logsTestConfig(10*time.Millisecond, Query{Cypher: "q", Logs: []LogsCfg{{BodyColumn: "msg"}}})
	cfg.Timeout = 5 * time.Millisecond
	statusEvents := make(chan *componentstatus.Event, 100)
	host := &statusReporterHost{Host: componenttest.NewNopHost(), report: func(e *componentstatus.Event) {
		select {
		case statusEvents <- e:
		default:
		}
	}}
	rcvr, err := createLogsReceiverFunc(func(*Config, *zap.Logger) (dbClient, error) { return blockingClient{}, nil })(t.Context(), receivertest.NewNopSettings(metadata.Type), cfg, consumertest.NewNop())
	require.NoError(t, err)
	require.NoError(t, rcvr.Start(t.Context(), host))
	select {
	case e := <-statusEvents:
		assert.Equal(t, componentstatus.StatusRecoverableError, e.Status())
		assert.ErrorIs(t, e.Err(), context.DeadlineExceeded)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for status event")
	}
	require.NoError(t, rcvr.Shutdown(t.Context()))
}

func TestRowToLog_NoRecordOnError(t *testing.T) {
	record := plog.NewLogRecord()
	err := rowToLog(row{"msg": "x", "at": "not-a-timestamp"}, &LogsCfg{BodyColumn: "msg", TsColumn: "at"}, record)
	require.ErrorContains(t, err, `ts_column "at"`)
	assert.Equal(t, pcommon.ValueTypeEmpty, record.Body().Type(), "a failing row leaves the record untouched")
}
