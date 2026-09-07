// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package neo4jqueryreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/neo4jqueryreceiver"

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componentstatus"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/scraper"
	"go.opentelemetry.io/collector/scraper/scrapererror"
	"go.opentelemetry.io/collector/scraper/scraperhelper"
	"go.uber.org/zap"
)

// queryScraper runs one configured Cypher query per collection interval and
// converts the result rows into metrics.
type queryScraper struct {
	id        component.ID
	query     Query
	scrapeCfg scraperhelper.ControllerConfig
	conn      *connection
	logger    *zap.Logger
	scope     pcommon.InstrumentationScope

	startTime pcommon.Timestamp
	host      component.Host
}

var _ scraper.Metrics = (*queryScraper)(nil)

func newQueryScraper(id component.ID, query Query, scrapeCfg scraperhelper.ControllerConfig, conn *connection, logger *zap.Logger, scope pcommon.InstrumentationScope) *queryScraper {
	return &queryScraper{
		id:        id,
		query:     query,
		scrapeCfg: scrapeCfg,
		conn:      conn,
		logger:    logger.With(zap.String("query_id", id.String())),
		scope:     scope,
	}
}

func (s *queryScraper) Start(_ context.Context, host component.Host) error {
	s.host = host
	s.startTime = pcommon.NewTimestampFromTime(time.Now())
	return nil
}

func (*queryScraper) Shutdown(context.Context) error {
	return nil
}

// ScrapeMetrics executes the query and builds metrics from its rows. Errors
// converting individual rows are collected into a partial scrape error so
// that the remaining rows are still delivered.
func (s *queryScraper) ScrapeMetrics(ctx context.Context) (pmetric.Metrics, error) {
	out, err := s.scrape(ctx)
	if err != nil {
		componentstatus.ReportStatus(s.host, componentstatus.NewRecoverableErrorEvent(err))
	} else {
		componentstatus.ReportStatus(s.host, componentstatus.NewEvent(componentstatus.StatusOK))
	}
	return out, err
}

func (s *queryScraper) scrape(ctx context.Context) (pmetric.Metrics, error) {
	out := pmetric.NewMetrics()

	if s.scrapeCfg.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.scrapeCfg.Timeout)
		defer cancel()
	}

	rows, err := s.conn.queryRows(ctx, s.query.Cypher, s.query.Parameters)
	if err != nil {
		return out, fmt.Errorf("scraper: %w", err)
	}
	if !s.query.IgnoreNullValues {
		warnNulls(s.logger, rows)
	}

	ts := pcommon.NewTimestampFromTime(time.Now())
	ms := out.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	s.scope.CopyTo(ms.Scope())

	var errs []error
	for i := range s.query.Metrics {
		metricCfg := &s.query.Metrics[i]
		metric := pmetric.NewMetric()
		dps := initMetric(metricCfg, metric)
		for j, r := range rows {
			if metricCfg.RowCondition != nil && !rowMatches(r, metricCfg.RowCondition) {
				continue
			}
			if err := rowToDataPoint(r, metricCfg, dps, s.startTime, ts, s.scrapeCfg); err != nil {
				errs = append(errs, fmt.Errorf("metric %q row %d: %w", metricCfg.MetricName, j, err))
			}
		}
		if dps.Len() > 0 {
			metric.MoveTo(ms.Metrics().AppendEmpty())
		}
	}
	if errs != nil {
		return out, scrapererror.NewPartialScrapeError(errors.Join(errs...), len(errs))
	}
	return out, nil
}

// rowMatches reports whether the row satisfies the row_condition.
func rowMatches(r row, cond *RowCondition) bool {
	v, found := r[cond.Column]
	if !found {
		return false
	}
	str, err := cellToString(v)
	if err != nil {
		return false
	}
	return str == cond.Value
}
