// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package datasource // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributecacheprocessor/internal/datasource"

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"go.uber.org/zap"
)

// SQLConfig is the SQL-source view used by the data source. It mirrors the
// processor's user-facing SQLConfig to avoid an import cycle between the
// datasource package and the processor package (same pattern as InlineConfig
// and CSVConfig).
type SQLConfig struct {
	// Driver is the database/sql driver name (e.g. "postgres", "mysql",
	// "sqlite", "clickhouse", "sqlserver", "oracle"). The driver must be
	// registered before Load is called; the drivers sub-package handles the
	// blank imports for all six supported drivers.
	Driver string
	// DSN is the data source name passed to sql.Open. Format is
	// driver-specific.
	DSN string
	// Query is the SELECT statement executed on every Load. It must return a
	// fixed set of columns whose names are usable as map keys.
	Query string
}

// validate checks the SQL configuration. Mirrors the user-facing
// Config.Validate behaviour so direct callers get the same guarantees.
func (c *SQLConfig) validate() error {
	if c == nil {
		return errors.New("sql config: must not be nil")
	}
	if c.Driver == "" {
		return errors.New("sql config: driver must be set")
	}
	if c.DSN == "" {
		return errors.New("sql config: dsn must be set")
	}
	if c.Query == "" {
		return errors.New("sql config: query must be set")
	}
	return nil
}

// SQLDataSource loads rows from a SQL database via database/sql. The
// connection pool is opened lazily on the first Load call and reused across
// subsequent refreshes; Close releases it during processor shutdown.
//
// All cells are scanned into *string so SQL NULL maps to the empty string
// rather than panicking. Any column-type mapping (numeric, time, etc.) that
// the driver supports is left to the driver's default string conversion.
type SQLDataSource struct {
	cfg    SQLConfig
	db     *sql.DB
	logger *zap.Logger
}

// NewSQL constructs a SQLDataSource. The config is copied so callers can
// safely mutate the original after construction. A nil logger is replaced
// with zap.NewNop so the source is safe to use without explicit wiring.
func NewSQL(cfg *SQLConfig, logger *zap.Logger) *SQLDataSource {
	d := &SQLDataSource{logger: logger}
	if d.logger == nil {
		d.logger = zap.NewNop()
	}
	if cfg != nil {
		d.cfg = *cfg
	}
	return d
}

// Load opens (lazily, on first call) the database, runs the configured
// query, and returns column names plus rows. NULL values map to the empty
// string. On any error Load returns (nil, nil, err); the cache layer keeps
// the previously loaded snapshot in that case.
func (d *SQLDataSource) Load(ctx context.Context) ([]string, []map[string]string, error) {
	if err := d.cfg.validate(); err != nil {
		return nil, nil, err
	}

	if d.db == nil {
		db, err := sql.Open(d.cfg.Driver, d.cfg.DSN)
		if err != nil {
			return nil, nil, fmt.Errorf("sql: open driver %q: %w", d.cfg.Driver, err)
		}
		if err := db.PingContext(ctx); err != nil {
			// Don't keep a half-broken handle around; force a fresh Open on
			// the next Load attempt.
			_ = db.Close()
			return nil, nil, fmt.Errorf("sql: ping driver %q: %w", d.cfg.Driver, err)
		}
		d.db = db
	}

	rows, err := d.db.QueryContext(ctx, d.cfg.Query)
	if err != nil {
		return nil, nil, fmt.Errorf("sql: query: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, nil, fmt.Errorf("sql: columns: %w", err)
	}
	if len(cols) == 0 {
		return nil, nil, errors.New("sql: query returned zero columns")
	}

	// Pre-allocate one set of *string scan targets and re-use it for every
	// row. We copy the dereferenced values into the per-row map so the
	// scratch buffer can be overwritten on the next Scan.
	scanBuf := make([]sql.NullString, len(cols))
	scanArgs := make([]any, len(cols))
	for i := range scanBuf {
		scanArgs[i] = &scanBuf[i]
	}

	var out []map[string]string
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return nil, nil, fmt.Errorf("sql: scan row: %w", err)
		}
		row := make(map[string]string, len(cols))
		for i, name := range cols {
			if scanBuf[i].Valid {
				row[name] = scanBuf[i].String
			} else {
				row[name] = ""
			}
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("sql: rows iteration: %w", err)
	}

	colsCopy := make([]string, len(cols))
	copy(colsCopy, cols)
	return colsCopy, out, nil
}

// Close releases the underlying *sql.DB if it has been opened. Calling
// Close on a never-loaded source is safe and returns nil.
func (d *SQLDataSource) Close(_ context.Context) error {
	if d == nil || d.db == nil {
		return nil
	}
	err := d.db.Close()
	d.db = nil
	return err
}

// Compile-time interface check.
var _ DataSource = (*SQLDataSource)(nil)
