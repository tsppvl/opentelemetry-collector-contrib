// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package datasource // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributecacheprocessor/internal/datasource"

import (
	"context"
	"errors"
	"fmt"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"go.uber.org/zap"
)

// Neo4jConfig is the Neo4j-source view used by the data source. It mirrors
// the processor's user-facing Neo4jConfig to avoid an import cycle between
// the datasource package and the processor package (same pattern as
// InlineConfig, CSVConfig, and SQLConfig).
type Neo4jConfig struct {
	// URI is the Bolt connection string (e.g. "bolt://localhost:7687",
	// "neo4j://host:7687"). Required.
	URI string
	// Username is the Neo4j account name. May be empty when the server
	// allows anonymous access.
	Username string
	// Password is the Neo4j account password. May be empty when the server
	// allows anonymous access.
	Password string
	// Database selects the target database (Neo4j 4.x+). Empty defaults to
	// the server-side default database.
	Database string
	// Query is the Cypher statement executed on every Load. It must return
	// a fixed set of columns whose names map to the configured columns.
	Query string
}

// validate checks the Neo4j configuration. Mirrors the user-facing
// Config.Validate behaviour so direct callers get the same guarantees.
func (c *Neo4jConfig) validate() error {
	if c == nil {
		return errors.New("neo4j config: must not be nil")
	}
	if c.URI == "" {
		return errors.New("neo4j config: uri must be set")
	}
	if c.Query == "" {
		return errors.New("neo4j config: query must be set")
	}
	return nil
}

// Neo4jDataSource loads rows from a Neo4j graph database via the official
// Bolt driver. The driver is opened lazily on the first Load call and reused
// across subsequent refreshes; Close releases it during processor shutdown.
//
// Cell values returned by Cypher (string, int, float, bool, nil, ...) are
// converted to string via fmt.Sprintf("%v", v); nil maps to the empty
// string.
type Neo4jDataSource struct {
	cfg    Neo4jConfig
	driver neo4j.DriverWithContext
	logger *zap.Logger
}

// NewNeo4j constructs a Neo4jDataSource. The config is copied so callers can
// safely mutate the original after construction. A nil logger is replaced
// with zap.NewNop so the source is safe to use without explicit wiring.
func NewNeo4j(cfg *Neo4jConfig, logger *zap.Logger) *Neo4jDataSource {
	d := &Neo4jDataSource{logger: logger}
	if d.logger == nil {
		d.logger = zap.NewNop()
	}
	if cfg != nil {
		d.cfg = *cfg
	}
	return d
}

// Load opens (lazily, on first call) the driver, runs the configured Cypher
// query inside a managed read transaction, and returns column names plus
// rows. On any error Load returns (nil, nil, err); the cache layer keeps the
// previously loaded snapshot in that case.
func (d *Neo4jDataSource) Load(ctx context.Context) ([]string, []map[string]string, error) {
	if err := d.cfg.validate(); err != nil {
		return nil, nil, err
	}

	if d.driver == nil {
		drv, err := neo4j.NewDriverWithContext(d.cfg.URI, neo4j.BasicAuth(d.cfg.Username, d.cfg.Password, ""))
		if err != nil {
			return nil, nil, fmt.Errorf("neo4j: open driver: %w", err)
		}
		// VerifyConnectivity actually establishes a connection so we fail
		// fast rather than waiting until the first session call. Failure
		// drops the half-open driver to force a fresh open on the next
		// Load attempt.
		if err := drv.VerifyConnectivity(ctx); err != nil {
			_ = drv.Close(ctx)
			return nil, nil, fmt.Errorf("neo4j: verify connectivity: %w", err)
		}
		d.driver = drv
	}

	session := d.driver.NewSession(ctx, neo4j.SessionConfig{
		DatabaseName: d.cfg.Database,
		AccessMode:   neo4j.AccessModeRead,
	})
	defer func() {
		if err := session.Close(ctx); err != nil {
			d.logger.Warn("neo4j: session close failed", zap.Error(err))
		}
	}()

	type loadResult struct {
		columns []string
		rows    []map[string]string
	}

	out, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		result, err := tx.Run(ctx, d.cfg.Query, nil)
		if err != nil {
			return nil, fmt.Errorf("run query: %w", err)
		}

		var columns []string
		var rows []map[string]string

		for result.Next(ctx) {
			rec := result.Record()
			if columns == nil {
				// Record.Keys is the same instance for every record in the
				// result; copying it here gives the data source a stable
				// independent view of column order.
				columns = make([]string, len(rec.Keys))
				copy(columns, rec.Keys)
			}
			row := make(map[string]string, len(rec.Keys))
			for i, key := range rec.Keys {
				if i >= len(rec.Values) {
					row[key] = ""
					continue
				}
				v := rec.Values[i]
				if v == nil {
					row[key] = ""
				} else {
					row[key] = fmt.Sprintf("%v", v)
				}
			}
			rows = append(rows, row)
		}
		if err := result.Err(); err != nil {
			return nil, fmt.Errorf("iterate records: %w", err)
		}

		// Empty result: still try to surface the Cypher RETURN keys so the
		// matcher sees the schema even when the query yields no rows.
		if columns == nil {
			keys, kerr := result.Keys()
			if kerr == nil {
				columns = make([]string, len(keys))
				copy(columns, keys)
			}
		}

		return loadResult{columns: columns, rows: rows}, nil
	})
	if err != nil {
		return nil, nil, fmt.Errorf("neo4j: %w", err)
	}

	res, ok := out.(loadResult)
	if !ok {
		return nil, nil, fmt.Errorf("neo4j: unexpected transaction result type %T", out)
	}
	return res.columns, res.rows, nil
}

// Close releases the underlying driver if it has been opened. Calling Close
// on a never-loaded source is safe and returns nil.
func (d *Neo4jDataSource) Close(ctx context.Context) error {
	if d == nil || d.driver == nil {
		return nil
	}
	err := d.driver.Close(ctx)
	d.driver = nil
	return err
}

// Compile-time interface check.
var _ DataSource = (*Neo4jDataSource)(nil)
