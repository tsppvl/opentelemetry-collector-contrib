// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package cache holds the in-memory lookup table and atomic-swap refresh
// machinery for the attributecache processor. The Cache loads a snapshot of
// rows from a datasource.DataSource at startup and, when configured, refreshes
// the snapshot on a background ticker. Reads are lock-free via
// atomic.Pointer[LookupTable].
package cache // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributecacheprocessor/internal/cache"

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributecacheprocessor/internal/datasource"
)

// Row holds all column values for a single table row.
type Row map[string]string

// LookupTable is an immutable, read-only snapshot of the loaded table.
//
// Once published via Cache.Current, the contents must not be mutated. The
// Cache replaces the snapshot atomically when the data source produces a new
// version.
type LookupTable struct {
	Columns []string
	Rows    []Row
}

// RefreshHooks allows the caller to observe cache refresh events without
// coupling the cache package to the telemetry builder directly. Both callbacks
// receive the context that was active at the time of the refresh. Either field
// may be nil.
type RefreshHooks struct {
	// OnSuccess is called after a successful table load. rowCount is the number
	// of rows in the newly loaded snapshot.
	OnSuccess func(ctx context.Context, rowCount int64)
	// OnError is called when a table refresh fails. The previous snapshot is
	// kept; the error has already been logged by the refresh loop.
	OnError func(ctx context.Context)
}

// Cache wraps an atomic.Pointer[LookupTable] and an optional background
// refresh goroutine that periodically reloads the snapshot from the data
// source.
type Cache struct {
	table    atomic.Pointer[LookupTable]
	source   datasource.DataSource
	interval time.Duration
	logger   *zap.Logger
	hooks    RefreshHooks
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// New creates a Cache backed by the given data source. New does NOT start the
// background refresh goroutine and does NOT load any rows; call Start to do
// that.
func New(source datasource.DataSource, interval time.Duration, logger *zap.Logger, hooks RefreshHooks) *Cache {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Cache{
		source:   source,
		interval: interval,
		logger:   logger,
		hooks:    hooks,
		stopCh:   make(chan struct{}),
	}
}

// Start performs a synchronous initial load of the table. If the load fails,
// Start returns the error and does not start the refresh goroutine. On
// success, Start launches the background refresh ticker when interval > 0.
func (c *Cache) Start(ctx context.Context) error {
	table, err := loadTable(ctx, c.source)
	if err != nil {
		return fmt.Errorf("initial table load: %w", err)
	}
	c.table.Store(table)
	if c.hooks.OnSuccess != nil {
		c.hooks.OnSuccess(ctx, int64(len(table.Rows)))
	}

	if c.interval > 0 {
		c.wg.Add(1)
		go c.refreshLoop()
	}
	return nil
}

// Shutdown stops the background refresh goroutine (waiting for it to exit)
// and closes the underlying data source. Shutdown is safe to call when Start
// was never called.
func (c *Cache) Shutdown(ctx context.Context) error {
	// Signal stop only once; sending on an already-closed channel panics.
	select {
	case <-c.stopCh:
		// already closed
	default:
		close(c.stopCh)
	}
	c.wg.Wait()

	if c.source != nil {
		if err := c.source.Close(ctx); err != nil {
			return fmt.Errorf("close data source: %w", err)
		}
	}
	return nil
}

// Current returns the most recent LookupTable snapshot. It never blocks. The
// returned pointer is safe to read concurrently but must not be mutated.
// Current returns nil if Start has not yet completed successfully.
func (c *Cache) Current() *LookupTable {
	return c.table.Load()
}

// refreshLoop runs in its own goroutine and reloads the table on every tick.
// On load error the previous snapshot stays in place and the error is
// logged.
func (c *Cache) refreshLoop() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			// Use a fresh background context for refresh; the original Start
			// context may already be done.
			ctx := context.Background()
			table, err := loadTable(ctx, c.source)
			if err != nil {
				c.logger.Error("attributecache: refresh failed; keeping previous snapshot", zap.Error(err))
				if c.hooks.OnError != nil {
					c.hooks.OnError(ctx)
				}
				continue
			}
			c.table.Store(table)
			if c.hooks.OnSuccess != nil {
				c.hooks.OnSuccess(ctx, int64(len(table.Rows)))
			}
		}
	}
}

// loadTable fetches a fresh snapshot from the data source and converts the
// raw row maps into Row values typed for the cache.
func loadTable(ctx context.Context, source datasource.DataSource) (*LookupTable, error) {
	if source == nil {
		return nil, fmt.Errorf("cache: data source is nil")
	}
	cols, rawRows, err := source.Load(ctx)
	if err != nil {
		return nil, err
	}
	rows := make([]Row, len(rawRows))
	for i, r := range rawRows {
		rows[i] = Row(r)
	}
	return &LookupTable{Columns: cols, Rows: rows}, nil
}
