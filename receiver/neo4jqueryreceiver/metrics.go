// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package neo4jqueryreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/neo4jqueryreceiver"

import (
	"errors"
	"fmt"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/scraper/scraperhelper"
)

// initMetric sets the descriptor fields of dest from cfg and returns the data
// point slice that rows are appended to.
func initMetric(cfg *MetricCfg, dest pmetric.Metric) pmetric.NumberDataPointSlice {
	dest.SetName(cfg.MetricName)
	dest.SetDescription(cfg.Description)
	dest.SetUnit(cfg.Unit)
	switch cfg.DataType {
	case MetricTypeSum:
		sum := dest.SetEmptySum()
		sum.SetIsMonotonic(cfg.Monotonic)
		sum.SetAggregationTemporality(cfgToAggregationTemporality(cfg.Aggregation))
		return sum.DataPoints()
	default: // MetricTypeUnspecified, MetricTypeGauge
		return dest.SetEmptyGauge().DataPoints()
	}
}

func cfgToAggregationTemporality(agg MetricAggregation) pmetric.AggregationTemporality {
	if agg == MetricAggregationDelta {
		return pmetric.AggregationTemporalityDelta
	}
	return pmetric.AggregationTemporalityCumulative
}

// rowToDataPoint converts one row into a data point appended to dps. Every
// referenced column is resolved before anything is appended, so a row that
// fails conversion produces an error and no data point.
func rowToDataPoint(r row, cfg *MetricCfg, dps pmetric.NumberDataPointSlice, startTime, ts pcommon.Timestamp, scrapeCfg scraperhelper.ControllerConfig) error {
	var errs []error

	if cfg.StartTsColumn != "" {
		v, found := r[cfg.StartTsColumn]
		if !found {
			errs = append(errs, fmt.Errorf("start_ts_column %q not found in result set", cfg.StartTsColumn))
		} else if parsed, err := cellToTimestamp(v); err != nil {
			errs = append(errs, fmt.Errorf("start_ts_column %q: %w", cfg.StartTsColumn, err))
		} else {
			startTime = parsed
		}
	}
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

	var intVal int64
	var doubleVal float64
	value, found := r[cfg.ValueColumn]
	if !found {
		errs = append(errs, fmt.Errorf("value_column %q not found in result set", cfg.ValueColumn))
	} else {
		var err error
		switch cfg.ValueType {
		case MetricValueTypeDouble:
			doubleVal, err = cellToFloat64(value)
		default: // MetricValueTypeUnspecified, MetricValueTypeInt
			intVal, err = cellToInt64(value)
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("value_column %q: %w", cfg.ValueColumn, err))
		}
	}

	attrs := pcommon.NewMap()
	for k, v := range cfg.StaticAttributes {
		attrs.PutStr(k, v)
	}
	for _, col := range cfg.AttributeColumns {
		v, found := r[col]
		if !found {
			errs = append(errs, fmt.Errorf("attribute_column %q not found in result set", col))
			continue
		}
		str, err := cellToString(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("attribute_column %q: %w", col, err))
			continue
		}
		attrs.PutStr(col, str)
	}

	if errs != nil {
		return errors.Join(errs...)
	}

	dp := dps.AppendEmpty()
	dp.SetTimestamp(ts)
	if cfg.DataType == MetricTypeSum {
		switch cfg.Aggregation {
		case MetricAggregationDelta:
			// A delta sum covers the previous collection interval.
			dp.SetStartTimestamp(pcommon.NewTimestampFromTime(ts.AsTime().Add(-scrapeCfg.CollectionInterval)))
		default: // cumulative
			dp.SetStartTimestamp(startTime)
		}
	}
	if cfg.ValueType == MetricValueTypeDouble {
		dp.SetDoubleValue(doubleVal)
	} else {
		dp.SetIntValue(intVal)
	}
	attrs.MoveTo(dp.Attributes())
	return nil
}
