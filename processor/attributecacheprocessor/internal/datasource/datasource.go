// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package datasource defines the DataSource interface for loading lookup
// table rows from external stores (inline, CSV, SQL, Neo4j). Concrete
// implementations land in follow-up tasks.
package datasource // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributecacheprocessor/internal/datasource"

import "context"

// DataSource loads rows from an external data store.
//
// Each call to Load must return a complete, fresh snapshot of all rows. The
// processor calls Load on startup and on every refresh tick. Implementations
// must be safe to call sequentially from a single background goroutine.
type DataSource interface {
	// Load fetches all rows. It returns the column names in source order and
	// a slice of rows where each row maps column name to string value.
	Load(ctx context.Context) (columns []string, rows []map[string]string, err error)

	// Close releases any persistent resources held by the data source
	// (connections, file handles). It is called once during processor
	// shutdown.
	Close(ctx context.Context) error
}
