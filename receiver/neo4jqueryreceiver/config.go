// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package neo4jqueryreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/neo4jqueryreceiver"

import (
	"errors"
	"fmt"
	"net/url"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/scraper/scraperhelper"
)

// supportedSchemes lists the URI schemes accepted by the Neo4j Bolt driver.
var supportedSchemes = map[string]bool{
	"neo4j":     true,
	"neo4j+s":   true,
	"neo4j+ssc": true,
	"bolt":      true,
	"bolt+s":    true,
	"bolt+ssc":  true,
}

// Config defines the configuration for the Neo4j query receiver.
type Config struct {
	scraperhelper.ControllerConfig `mapstructure:",squash"`

	// URI is the Bolt connection URI, e.g. "bolt://localhost:7687" or
	// "neo4j+s://my-cluster.example.com". The scheme selects routing and
	// TLS behavior exactly as documented by the Neo4j Go driver.
	URI string `mapstructure:"uri"`
	// Username for basic authentication. Leave empty for servers that allow
	// unauthenticated access.
	Username string `mapstructure:"username"`
	// Password for basic authentication.
	Password configopaque.String `mapstructure:"password"`
	// Database selects the target database. Empty uses the server default.
	Database string `mapstructure:"database"`
	// MaxConnectionPoolSize caps the number of pooled Bolt connections.
	// <= 0 keeps the driver default (100).
	MaxConnectionPoolSize int `mapstructure:"max_connection_pool_size"`
	// Queries is the list of Cypher statements to run on every collection.
	Queries []Query `mapstructure:"queries"`
	// StorageID names a storage extension used to persist the tracking value
	// of logs queries across collector restarts.
	StorageID *component.ID `mapstructure:"storage"`
	// Telemetry configures the receiver's own telemetry.
	Telemetry TelemetryConfig `mapstructure:"telemetry"`

	// prevent unkeyed literal initialization
	_ struct{}
}

func createDefaultConfig() component.Config {
	cc := scraperhelper.NewDefaultControllerConfig()
	cc.CollectionInterval = 10 * time.Second
	return &Config{
		ControllerConfig: cc,
	}
}

// Validate checks the receiver configuration.
func (c *Config) Validate() error {
	var errs []error
	if c.URI == "" {
		errs = append(errs, errors.New("'uri' cannot be empty"))
	} else {
		u, err := url.Parse(c.URI)
		switch {
		case err != nil:
			errs = append(errs, fmt.Errorf("'uri' is not a valid URI: %w", err))
		case !supportedSchemes[u.Scheme]:
			errs = append(errs, fmt.Errorf("'uri' has unsupported scheme %q: must be one of neo4j, neo4j+s, neo4j+ssc, bolt, bolt+s, bolt+ssc", u.Scheme))
		case u.Host == "":
			errs = append(errs, errors.New("'uri' must contain a host"))
		}
	}
	if c.Username == "" && c.Password != "" {
		errs = append(errs, errors.New("'username' must be set when 'password' is set"))
	}
	if c.MaxConnectionPoolSize < 0 {
		errs = append(errs, errors.New("'max_connection_pool_size' cannot be negative"))
	}
	if len(c.Queries) == 0 {
		errs = append(errs, errors.New("'queries' cannot be empty"))
	}
	for i := range c.Queries {
		if err := c.Queries[i].Validate(); err != nil {
			errs = append(errs, fmt.Errorf("query %d: %w", i, err))
		}
	}
	return errors.Join(errs...)
}

// Query is one Cypher statement plus the metrics derived from its rows.
type Query struct {
	// Cypher is the statement to execute. It should RETURN named columns;
	// column names are matched against value_column, attribute_columns, etc.
	Cypher string `mapstructure:"cypher"`
	// Parameters are passed to the statement as Cypher query parameters and
	// referenced in the statement as $name.
	Parameters map[string]any `mapstructure:"parameters"`
	// Metrics describes the metrics produced from every returned row.
	Metrics []MetricCfg `mapstructure:"metrics"`
	// Logs describes the log records produced from every returned row.
	Logs []LogsCfg `mapstructure:"logs"`
	// TrackingColumn applies only to logs. Names the column whose value from
	// the last returned row is passed as the tracking parameter on the next
	// run, so rows are not read twice.
	TrackingColumn string `mapstructure:"tracking_column"`
	// TrackingStartValue is the tracking parameter value used on the first
	// run. The YAML scalar type is preserved (quote it to force a string).
	TrackingStartValue any `mapstructure:"tracking_start_value"`
	// TrackingParameter is the Cypher parameter name that carries the
	// tracking value. Defaults to "tracking_value" (referenced as
	// $tracking_value in the statement).
	TrackingParameter string `mapstructure:"tracking_parameter"`
	// IgnoreNullValues suppresses the warning logged when a returned row
	// contains a null in a column that is not referenced by any metric.
	IgnoreNullValues bool `mapstructure:"ignore_null_values"`

	// prevent unkeyed literal initialization
	_ struct{}
}

// Validate checks a single query configuration.
func (q *Query) Validate() error {
	var errs []error
	if q.Cypher == "" {
		errs = append(errs, errors.New("'cypher' cannot be empty"))
	}
	if len(q.Metrics) == 0 && len(q.Logs) == 0 {
		errs = append(errs, errors.New("at least one of 'logs' and 'metrics' must not be empty"))
	}
	for i := range q.Metrics {
		if err := q.Metrics[i].Validate(); err != nil {
			errs = append(errs, err)
		}
	}
	for i := range q.Logs {
		if err := q.Logs[i].Validate(); err != nil {
			errs = append(errs, err)
		}
	}
	if q.TrackingColumn == "" && q.TrackingStartValue != nil {
		errs = append(errs, errors.New("'tracking_start_value' requires 'tracking_column'"))
	}
	if q.TrackingColumn == "" && q.TrackingParameter != "" {
		errs = append(errs, errors.New("'tracking_parameter' requires 'tracking_column'"))
	}
	return errors.Join(errs...)
}

// defaultTrackingParameter is the Cypher parameter name used for the
// tracking value when tracking_parameter is not set.
const defaultTrackingParameter = "tracking_value"

// trackingParameter returns the effective tracking parameter name.
func (q *Query) trackingParameter() string {
	if q.TrackingParameter != "" {
		return q.TrackingParameter
	}
	return defaultTrackingParameter
}

// LogsCfg describes the log records derived from the rows of a query.
type LogsCfg struct {
	// BodyColumn is the column whose value becomes the log record body.
	BodyColumn string `mapstructure:"body_column"`
	// AttributeColumns are set as log record attributes (as strings).
	AttributeColumns []string `mapstructure:"attribute_columns"`
	// TsColumn optionally provides the log record timestamp. When empty the
	// timestamp is left unset and only the observed timestamp is filled.
	TsColumn string `mapstructure:"ts_column"`

	// prevent unkeyed literal initialization
	_ struct{}
}

// Validate checks a single logs configuration.
func (c *LogsCfg) Validate() error {
	if c.BodyColumn == "" {
		return errors.New("'body_column' must not be empty")
	}
	return nil
}

// RowCondition filters query result rows for a metric. Only rows where the
// specified column equals the specified value are used to produce the metric.
type RowCondition struct {
	Column string `mapstructure:"column"`
	Value  string `mapstructure:"value"`

	// prevent unkeyed literal initialization
	_ struct{}
}

// MetricCfg describes one metric derived from the rows of a query. The field
// set intentionally mirrors the sqlqueryreceiver so configurations can be
// translated between the two receivers without changes.
type MetricCfg struct {
	MetricName       string            `mapstructure:"metric_name"`
	ValueColumn      string            `mapstructure:"value_column"`
	AttributeColumns []string          `mapstructure:"attribute_columns"`
	Monotonic        bool              `mapstructure:"monotonic"`
	ValueType        MetricValueType   `mapstructure:"value_type"`
	DataType         MetricType        `mapstructure:"data_type"`
	Aggregation      MetricAggregation `mapstructure:"aggregation"`
	Unit             string            `mapstructure:"unit"`
	Description      string            `mapstructure:"description"`
	StaticAttributes map[string]string `mapstructure:"static_attributes"`
	StartTsColumn    string            `mapstructure:"start_ts_column"`
	TsColumn         string            `mapstructure:"ts_column"`
	RowCondition     *RowCondition     `mapstructure:"row_condition"`

	// prevent unkeyed literal initialization
	_ struct{}
}

// Validate checks a single metric configuration.
func (c *MetricCfg) Validate() error {
	var errs []error
	if c.MetricName == "" {
		errs = append(errs, errors.New("'metric_name' cannot be empty"))
	}
	if c.ValueColumn == "" {
		errs = append(errs, errors.New("'value_column' cannot be empty"))
	}
	if err := c.ValueType.Validate(); err != nil {
		errs = append(errs, err)
	}
	if err := c.DataType.Validate(); err != nil {
		errs = append(errs, err)
	}
	if err := c.Aggregation.Validate(); err != nil {
		errs = append(errs, err)
	}
	if c.DataType == MetricTypeGauge && c.Aggregation != "" {
		errs = append(errs, fmt.Errorf("aggregation=%s but data_type=%s does not support aggregation", c.Aggregation, c.DataType))
	}
	if c.RowCondition != nil && c.RowCondition.Column == "" {
		errs = append(errs, errors.New("'row_condition.column' cannot be empty"))
	}
	if errs != nil && c.MetricName != "" {
		errs = append(errs, fmt.Errorf("invalid metric config with metric_name '%s'", c.MetricName))
	}
	return errors.Join(errs...)
}

// MetricType selects the OTLP metric data type.
type MetricType string

const (
	MetricTypeUnspecified MetricType = ""
	MetricTypeGauge       MetricType = "gauge"
	MetricTypeSum         MetricType = "sum"
)

// Validate checks the metric type.
func (t MetricType) Validate() error {
	switch t {
	case MetricTypeUnspecified, MetricTypeGauge, MetricTypeSum:
		return nil
	}
	return fmt.Errorf("metric config has unsupported data_type: '%s'", t)
}

// MetricValueType selects the data point number type.
type MetricValueType string

const (
	MetricValueTypeUnspecified MetricValueType = ""
	MetricValueTypeInt         MetricValueType = "int"
	MetricValueTypeDouble      MetricValueType = "double"
)

// Validate checks the value type.
func (t MetricValueType) Validate() error {
	switch t {
	case MetricValueTypeUnspecified, MetricValueTypeInt, MetricValueTypeDouble:
		return nil
	}
	return fmt.Errorf("metric config has unsupported value_type: '%s'", t)
}

// MetricAggregation selects the aggregation temporality of a sum.
type MetricAggregation string

const (
	MetricAggregationUnspecified MetricAggregation = ""
	MetricAggregationCumulative  MetricAggregation = "cumulative"
	MetricAggregationDelta       MetricAggregation = "delta"
)

// Validate checks the aggregation.
func (a MetricAggregation) Validate() error {
	switch a {
	case MetricAggregationUnspecified, MetricAggregationCumulative, MetricAggregationDelta:
		return nil
	}
	return fmt.Errorf("metric config has unsupported aggregation: '%s'", a)
}

// TelemetryConfig configures the receiver's own telemetry.
type TelemetryConfig struct {
	Logs TelemetryLogsConfig `mapstructure:"logs"`

	// prevent unkeyed literal initialization
	_ struct{}
}

// TelemetryLogsConfig configures the receiver's own logs.
type TelemetryLogsConfig struct {
	// Query logs the Cypher text and parameters at debug level on every run.
	Query bool `mapstructure:"query"`

	// prevent unkeyed literal initialization
	_ struct{}
}
