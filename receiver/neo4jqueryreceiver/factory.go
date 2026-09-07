// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package neo4jqueryreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/neo4jqueryreceiver"

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/scraper/scraperhelper"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/neo4jqueryreceiver/internal/metadata"
)

// NewFactory creates a factory for the Neo4j query receiver.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		metadata.Type,
		createDefaultConfig,
		receiver.WithMetrics(createMetricsReceiverFunc(newNeo4jClient), metadata.MetricsStability),
		receiver.WithLogs(createLogsReceiverFunc(newNeo4jClient), metadata.LogsStability),
	)
}

func createMetricsReceiverFunc(newClient clientFactory) receiver.CreateMetricsFunc {
	return func(
		_ context.Context,
		settings receiver.Settings,
		rCfg component.Config,
		nextConsumer consumer.Metrics,
	) (receiver.Metrics, error) {
		cfg := rCfg.(*Config)
		conn := newConnection(cfg, newClient, settings.Logger)

		scope := pcommon.NewInstrumentationScope()
		scope.SetName(metadata.ScopeName)

		var opts []scraperhelper.ControllerOption
		for i := range cfg.Queries {
			query := &cfg.Queries[i]
			if len(query.Metrics) == 0 {
				continue
			}
			id := component.MustNewIDWithName(metadata.Type.String(), fmt.Sprintf("query-%d", i))
			s := newQueryScraper(id, *query, cfg.ControllerConfig, conn, settings.Logger, scope)
			opts = append(opts, scraperhelper.AddMetricsScraper(metadata.Type, s))
		}

		controller, err := scraperhelper.NewMetricsController(&cfg.ControllerConfig, settings, nextConsumer, opts...)
		if err != nil {
			return nil, err
		}
		return &metricsReceiver{Metrics: controller, conn: conn}, nil
	}
}

func createLogsReceiverFunc(newClient clientFactory) receiver.CreateLogsFunc {
	return func(
		_ context.Context,
		settings receiver.Settings,
		rCfg component.Config,
		nextConsumer consumer.Logs,
	) (receiver.Logs, error) {
		return newLogsReceiver(rCfg.(*Config), settings, newClient, nextConsumer)
	}
}

// metricsReceiver wraps the scraper controller so the shared Neo4j
// connection is opened before the first scrape and closed after the last.
type metricsReceiver struct {
	receiver.Metrics
	conn *connection
}

func (r *metricsReceiver) Start(ctx context.Context, host component.Host) error {
	if err := r.conn.open(); err != nil {
		return err
	}
	if err := r.Metrics.Start(ctx, host); err != nil {
		return errors.Join(err, r.conn.close(ctx))
	}
	return nil
}

func (r *metricsReceiver) Shutdown(ctx context.Context) error {
	return errors.Join(r.Metrics.Shutdown(ctx), r.conn.close(ctx))
}
