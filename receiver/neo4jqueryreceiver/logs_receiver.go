// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package neo4jqueryreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/neo4jqueryreceiver"

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"sort"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componentstatus"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/extension/xextension/storage"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/neo4jqueryreceiver/internal/metadata"
)

// logsReceiver runs every query that has a logs section on the collection
// interval and forwards the resulting log records. It mirrors the logs part
// of the sqlqueryreceiver, including tracking of processed rows.
type logsReceiver struct {
	cfg          *Config
	settings     receiver.Settings
	conn         *connection
	nextConsumer consumer.Logs
	obsrecv      *receiverhelper.ObsReport

	queryReceivers []*logsQueryReceiver
	storageClient  storage.Client
	host           component.Host

	isStarted         bool
	shutdownRequested chan struct{}
	wg                sync.WaitGroup
}

func newLogsReceiver(cfg *Config, settings receiver.Settings, newClient clientFactory, nextConsumer consumer.Logs) (*logsReceiver, error) {
	obsrecv, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{
		ReceiverID:             settings.ID,
		ReceiverCreateSettings: settings,
	})
	if err != nil {
		return nil, err
	}
	return &logsReceiver{
		cfg:          cfg,
		settings:     settings,
		conn:         newConnection(cfg, newClient, settings.Logger),
		nextConsumer: nextConsumer,
		obsrecv:      obsrecv,
	}, nil
}

func (r *logsReceiver) Start(ctx context.Context, host component.Host) error {
	if r.isStarted {
		r.settings.Logger.Debug("requested start, but already started, ignoring.")
		return nil
	}
	r.settings.Logger.Debug("starting...")
	r.host = host
	r.shutdownRequested = make(chan struct{})

	var err error
	r.storageClient, err = getStorageClient(ctx, host, r.cfg.StorageID, r.settings.ID)
	if err != nil {
		return fmt.Errorf("error connecting to storage: %w", err)
	}
	if err = r.conn.open(); err != nil {
		return errors.Join(err, r.closeStorage(ctx))
	}

	r.queryReceivers = nil
	for i := range r.cfg.Queries {
		query := &r.cfg.Queries[i]
		if len(query.Logs) == 0 {
			continue
		}
		id := fmt.Sprintf("query-%d: %s", i, query.Cypher)
		qr := newLogsQueryReceiver(id, *query, r.conn, r.settings.Logger, r.storageClient)
		qr.start(ctx)
		r.queryReceivers = append(r.queryReceivers, qr)
	}

	r.isStarted = true
	r.startCollecting()
	r.settings.Logger.Debug("started.")
	return nil
}

func (r *logsReceiver) startCollecting() {
	r.wg.Go(func() {
		if r.cfg.InitialDelay > 0 {
			timer := time.NewTimer(r.cfg.InitialDelay)
			select {
			case <-timer.C:
				r.collect()
			case <-r.shutdownRequested:
				timer.Stop()
				return
			}
		}
		ticker := time.NewTicker(r.cfg.CollectionInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				r.collect()
			case <-r.shutdownRequested:
				return
			}
		}
	})
}

// collect runs all logs queries concurrently, merges their records, reports
// the component status and forwards the batch to the next consumer.
func (r *logsReceiver) collect() {
	type collectResult struct {
		logs plog.Logs
		err  error
	}
	results := make(chan collectResult, len(r.queryReceivers))
	for _, qr := range r.queryReceivers {
		go func(qr *logsQueryReceiver) {
			ctx := context.Background()
			if r.cfg.Timeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, r.cfg.Timeout)
				defer cancel()
			}
			logs, err := qr.collect(ctx)
			if err != nil {
				r.settings.Logger.Error("error collecting logs", zap.Error(err), zap.String("query", qr.id))
			}
			results <- collectResult{logs: logs, err: err}
		}(qr)
	}

	allLogs := plog.NewLogs()
	var collectErr error
	for range r.queryReceivers {
		select {
		case res := <-results:
			res.logs.ResourceLogs().MoveAndAppendTo(allLogs.ResourceLogs())
			collectErr = errors.Join(collectErr, res.err)
		case <-r.shutdownRequested:
			return
		}
	}

	if collectErr != nil {
		componentstatus.ReportStatus(r.host, componentstatus.NewRecoverableErrorEvent(collectErr))
	} else {
		componentstatus.ReportStatus(r.host, componentstatus.NewEvent(componentstatus.StatusOK))
	}

	count := allLogs.LogRecordCount()
	if count == 0 {
		return
	}
	ctx := r.obsrecv.StartLogsOp(context.Background())
	err := r.nextConsumer.ConsumeLogs(ctx, allLogs)
	r.obsrecv.EndLogsOp(ctx, metadata.Type.String(), count, err)
	if err != nil {
		r.settings.Logger.Error("failed to send logs", zap.Error(err))
	}
}

func (r *logsReceiver) Shutdown(ctx context.Context) error {
	if !r.isStarted {
		r.settings.Logger.Debug("requested shutdown, but not started, ignoring.")
		return nil
	}
	r.settings.Logger.Debug("stopping...")
	close(r.shutdownRequested)
	r.wg.Wait()
	err := errors.Join(r.closeStorage(ctx), r.conn.close(ctx))
	r.isStarted = false
	r.settings.Logger.Debug("stopped.")
	return err
}

func (r *logsReceiver) closeStorage(ctx context.Context) error {
	if r.storageClient == nil {
		return nil
	}
	err := r.storageClient.Close(ctx)
	r.storageClient = nil
	return err
}

// getStorageClient resolves the configured storage extension from the host.
// A nil storageID means persistence is disabled and a nil client is returned.
func getStorageClient(ctx context.Context, host component.Host, storageID *component.ID, receiverID component.ID) (storage.Client, error) {
	if storageID == nil {
		return nil, nil
	}
	ext, found := host.GetExtensions()[*storageID]
	if !found {
		return nil, fmt.Errorf("storage extension '%s' not found", storageID)
	}
	storageExt, ok := ext.(storage.Extension)
	if !ok {
		return nil, fmt.Errorf("non-storage extension '%s' specified as storage", storageID)
	}
	return storageExt.GetClient(ctx, component.KindReceiver, receiverID, "")
}

// logsQueryReceiver handles one query with a logs section.
type logsQueryReceiver struct {
	id     string
	query  Query
	conn   *connection
	logger *zap.Logger

	trackingValue any
	trackingParam string
	storageClient storage.Client
	storageKey    string
}

func newLogsQueryReceiver(id string, query Query, conn *connection, logger *zap.Logger, storageClient storage.Client) *logsQueryReceiver {
	return &logsQueryReceiver{
		id:            id,
		query:         query,
		conn:          conn,
		logger:        logger.With(zap.String("query_id", id)),
		trackingValue: normalizeTrackingValue(query.TrackingStartValue),
		trackingParam: query.trackingParameter(),
		storageClient: storageClient,
		storageKey:    id + ".trackingValue",
	}
}

// start restores the persisted tracking value, if any.
func (qr *logsQueryReceiver) start(ctx context.Context) {
	if qr.storageClient == nil || qr.query.TrackingColumn == "" {
		return
	}
	stored, err := qr.storageClient.Get(ctx, qr.storageKey)
	if err != nil {
		qr.logger.Warn("failed to read tracking value from storage, using tracking_start_value", zap.Error(err))
		return
	}
	if stored == nil {
		return
	}
	qr.trackingValue = decodeTrackingValue(stored)
}

func (qr *logsQueryReceiver) collect(ctx context.Context) (plog.Logs, error) {
	logs := plog.NewLogs()
	observedAt := pcommon.NewTimestampFromTime(time.Now())

	params := qr.query.Parameters
	if qr.query.TrackingColumn != "" {
		params = make(map[string]any, len(qr.query.Parameters)+1)
		maps.Copy(params, qr.query.Parameters)
		params[qr.trackingParam] = qr.trackingValue
	}

	rows, err := qr.conn.queryRows(ctx, qr.query.Cypher, params)
	if err != nil {
		return logs, fmt.Errorf("scraper: %w", err)
	}
	if !qr.query.IgnoreNullValues {
		warnNulls(qr.logger, rows)
	}

	var errs []error
	scope := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()
	scope.Scope().SetName(metadata.ScopeName)
	records := scope.LogRecords()
	for i := range qr.query.Logs {
		logsCfg := &qr.query.Logs[i]
		for j, r := range rows {
			record := plog.NewLogRecord()
			if err := rowToLog(r, logsCfg, record); err != nil {
				errs = append(errs, fmt.Errorf("row %d: %w", j, err))
				continue
			}
			record.SetObservedTimestamp(observedAt)
			record.MoveTo(records.AppendEmpty())
		}
	}
	if len(rows) > 0 {
		if err := qr.storeTrackingValue(ctx, rows[len(rows)-1]); err != nil {
			errs = append(errs, err)
		}
	}
	return logs, errors.Join(errs...)
}

// storeTrackingValue remembers the tracking column of the last row and
// persists it when storage is configured.
func (qr *logsQueryReceiver) storeTrackingValue(ctx context.Context, last row) error {
	if qr.query.TrackingColumn == "" {
		return nil
	}
	v, found := last[qr.query.TrackingColumn]
	if !found {
		return fmt.Errorf("tracking_column %q not found in result set", qr.query.TrackingColumn)
	}
	if v == nil {
		return fmt.Errorf("tracking_column %q: %w", qr.query.TrackingColumn, errNullValue)
	}
	qr.trackingValue = normalizeTrackingValue(v)
	if qr.storageClient == nil {
		return nil
	}
	encoded, err := json.Marshal(qr.trackingValue)
	if err != nil {
		return fmt.Errorf("encode tracking value: %w", err)
	}
	if err := qr.storageClient.Set(ctx, qr.storageKey, encoded); err != nil {
		return fmt.Errorf("persist tracking value: %w", err)
	}
	return nil
}

// normalizeTrackingValue reduces a tracking value to the types that survive a
// JSON round trip unchanged (int64, float64, bool, string), so the parameter
// passed to Cypher is identical before and after a collector restart.
// Temporal and complex values become their string form; use Cypher functions
// such as datetime($tracking_value) to convert them back.
func normalizeTrackingValue(v any) any {
	switch t := v.(type) {
	case nil:
		return nil
	case int64, float64, bool, string:
		return t
	case int:
		return int64(t)
	case int32:
		return int64(t)
	case uint:
		return int64(t)
	case uint32:
		return int64(t)
	case float32:
		return float64(t)
	}
	s, err := cellToString(v)
	if err != nil {
		return nil
	}
	return s
}

// decodeTrackingValue reverses the JSON encoding written by storeTrackingValue.
// Values written by older builds as raw strings are returned unchanged.
func decodeTrackingValue(b []byte) any {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return string(b)
	}
	switch t := v.(type) {
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return i
		}
		if f, err := strconv.ParseFloat(t.String(), 64); err == nil {
			return f
		}
		return t.String()
	case string, bool, float64, nil:
		return t
	}
	return string(b)
}

// rowToLog fills record from one row. Every referenced column is resolved
// before the record is touched so a failing row produces no record.
func rowToLog(r row, cfg *LogsCfg, record plog.LogRecord) error {
	var errs []error

	body, found := r[cfg.BodyColumn]
	var bodyStr string
	if !found {
		errs = append(errs, fmt.Errorf("body_column %q not found in result set", cfg.BodyColumn))
	} else if s, err := cellToString(body); err != nil {
		errs = append(errs, fmt.Errorf("body_column %q: %w", cfg.BodyColumn, err))
	} else {
		bodyStr = s
	}

	var ts pcommon.Timestamp
	if cfg.TsColumn != "" {
		v, found := r[cfg.TsColumn]
		if !found {
			errs = append(errs, fmt.Errorf("ts_column %q not found in result set", cfg.TsColumn))
		} else if parsed, err := cellToTimestamp(v); err != nil {
			errs = append(errs, fmt.Errorf("ts_column %q: %w", cfg.TsColumn, err))
		} else {
			ts = parsed
		}
	}

	attrs := pcommon.NewMap()
	for _, col := range cfg.AttributeColumns {
		v, found := r[col]
		if !found {
			errs = append(errs, fmt.Errorf("attribute_column %q not found in result set", col))
			continue
		}
		s, err := cellToString(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("attribute_column %q: %w", col, err))
			continue
		}
		attrs.PutStr(col, s)
	}

	if errs != nil {
		return errors.Join(errs...)
	}
	record.Body().SetStr(bodyStr)
	if cfg.TsColumn != "" {
		record.SetTimestamp(ts)
	}
	attrs.MoveTo(record.Attributes())
	return nil
}

// warnNulls logs one warning listing the columns that contained a Cypher
// null in at least one row.
func warnNulls(logger *zap.Logger, rows []row) {
	nullCols := map[string]struct{}{}
	for _, r := range rows {
		for k, v := range r {
			if v == nil {
				nullCols[k] = struct{}{}
			}
		}
	}
	if len(nullCols) == 0 {
		return
	}
	cols := make([]string, 0, len(nullCols))
	for k := range nullCols {
		cols = append(cols, k)
	}
	sort.Strings(cols)
	logger.Warn("query returned null values; set ignore_null_values to suppress this warning", zap.Strings("columns", cols))
}
