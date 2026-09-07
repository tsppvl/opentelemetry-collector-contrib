// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package neo4jqueryreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/neo4jqueryreceiver"

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/config"
	"go.uber.org/zap"
)

// dbClient abstracts the Neo4j driver so scrapers can be tested without a
// database.
type dbClient interface {
	// queryRows executes the Cypher statement with the given parameters and
	// returns every record of the result.
	queryRows(ctx context.Context, cypher string, params map[string]any) ([]row, error)
	// close releases all resources held by the client.
	close(ctx context.Context) error
}

// clientFactory creates a dbClient for the receiver configuration. The
// production factory is newNeo4jClient; tests inject fakes.
type clientFactory func(cfg *Config, logger *zap.Logger) (dbClient, error)

// neo4jClient is the production dbClient backed by the official Bolt driver.
type neo4jClient struct {
	driver     neo4j.DriverWithContext
	database   string
	logger     *zap.Logger
	logQueries bool
}

// newNeo4jClient opens a Bolt driver. The driver connects lazily, so no
// network I/O happens here; connectivity problems surface on the first scrape.
func newNeo4jClient(cfg *Config, logger *zap.Logger) (dbClient, error) {
	auth := neo4j.NoAuth()
	if cfg.Username != "" {
		auth = neo4j.BasicAuth(cfg.Username, string(cfg.Password), "")
	}
	drv, err := neo4j.NewDriverWithContext(cfg.URI, auth, func(c *config.Config) {
		if cfg.MaxConnectionPoolSize > 0 {
			c.MaxConnectionPoolSize = cfg.MaxConnectionPoolSize
		}
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create neo4j driver: %w", err)
	}
	return &neo4jClient{
		driver:     drv,
		database:   cfg.Database,
		logger:     logger,
		logQueries: cfg.Telemetry.Logs.Query,
	}, nil
}

func (c *neo4jClient) queryRows(ctx context.Context, cypher string, params map[string]any) ([]row, error) {
	if c.logQueries {
		c.logger.Debug("Running query", zap.String("query", cypher), zap.Any("parameters", params))
	} else {
		c.logger.Debug("Running query")
	}
	opts := []neo4j.ExecuteQueryConfigurationOption{neo4j.ExecuteQueryWithReadersRouting()}
	if c.database != "" {
		opts = append(opts, neo4j.ExecuteQueryWithDatabase(c.database))
	}
	result, err := neo4j.ExecuteQuery(ctx, c.driver, cypher, params, neo4j.EagerResultTransformer, opts...)
	if err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(result.Records))
	for _, rec := range result.Records {
		r := make(row, len(rec.Keys))
		for i, key := range rec.Keys {
			if i < len(rec.Values) {
				r[key] = rec.Values[i]
			} else {
				r[key] = nil
			}
		}
		rows = append(rows, r)
	}
	return rows, nil
}

func (c *neo4jClient) close(ctx context.Context) error {
	return c.driver.Close(ctx)
}

// connection owns the single dbClient shared by all query scrapers of one
// receiver instance. It is opened when the receiver starts and closed when it
// shuts down, so every query reuses the same Bolt connection pool.
type connection struct {
	cfg       *Config
	newClient clientFactory
	logger    *zap.Logger
	mu        sync.Mutex
	client    dbClient
}

func newConnection(cfg *Config, newClient clientFactory, logger *zap.Logger) *connection {
	return &connection{cfg: cfg, newClient: newClient, logger: logger}
}

// open creates the client if it does not exist yet.
func (c *connection) open() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.client != nil {
		return nil
	}
	client, err := c.newClient(c.cfg, c.logger)
	if err != nil {
		return err
	}
	c.client = client
	return nil
}

// queryRows forwards to the open client.
func (c *connection) queryRows(ctx context.Context, cypher string, params map[string]any) ([]row, error) {
	c.mu.Lock()
	client := c.client
	c.mu.Unlock()
	if client == nil {
		return nil, errors.New("neo4j connection is not open")
	}
	return client.queryRows(ctx, cypher, params)
}

// close releases the client. It is safe to call more than once.
func (c *connection) close(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.client == nil {
		return nil
	}
	err := c.client.close(ctx)
	c.client = nil
	return err
}
