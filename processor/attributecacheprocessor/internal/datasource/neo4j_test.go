// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package datasource

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNeo4jDataSource_NewReturnsNonNil verifies the constructor never
// returns nil even when the caller passes a nil config or nil logger. A nil
// receiver would surface as a panic later in Load/Close which must be
// avoided.
func TestNeo4jDataSource_NewReturnsNonNil(t *testing.T) {
	ds := NewNeo4j(&Neo4jConfig{
		URI:      "bolt://localhost:7687",
		Username: "neo4j",
		Password: "password",
		Database: "neo4j",
		Query:    "MATCH (n) RETURN n.name AS name",
	}, nil)
	require.NotNil(t, ds)

	// Nil config must not panic the constructor.
	dsNil := NewNeo4j(nil, nil)
	require.NotNil(t, dsNil)
}

// TestNeo4jDataSource_CloseBeforeOpen ensures that closing a freshly
// constructed source (driver never opened) returns nil and never panics.
// The processor shutdown path always calls Close, including on sources
// whose first Load failed before the driver could be created.
func TestNeo4jDataSource_CloseBeforeOpen(t *testing.T) {
	ds := NewNeo4j(&Neo4jConfig{
		URI:   "bolt://localhost:7687",
		Query: "MATCH (n) RETURN n",
	}, nil)

	require.NoError(t, ds.Close(context.Background()))
	// A second Close is also safe.
	require.NoError(t, ds.Close(context.Background()))

	// Close on a nil receiver must also be a no-op (defensive).
	var nilDS *Neo4jDataSource
	require.NoError(t, nilDS.Close(context.Background()))
}

// TestNeo4jDataSource_ConfigDefaults verifies that the constructor copies
// the supplied config (so later mutations to the caller's struct don't
// affect the source) and validates required fields on Load. No real Neo4j
// server is contacted: the validation error is returned synchronously.
func TestNeo4jDataSource_ConfigDefaults(t *testing.T) {
	cfg := &Neo4jConfig{
		URI:      "bolt://example:7687",
		Username: "u",
		Password: "p",
		Database: "db",
		Query:    "RETURN 1",
	}
	ds := NewNeo4j(cfg, nil)
	require.NotNil(t, ds)

	// Mutating the caller's copy must not affect the source.
	cfg.URI = "bolt://other:7687"
	cfg.Query = ""
	assert.Equal(t, "bolt://example:7687", ds.cfg.URI)
	assert.Equal(t, "RETURN 1", ds.cfg.Query)
	assert.Equal(t, "u", ds.cfg.Username)
	assert.Equal(t, "p", ds.cfg.Password)
	assert.Equal(t, "db", ds.cfg.Database)

	// Missing required fields fail at validation, before any network call.
	bad := NewNeo4j(&Neo4jConfig{Query: "RETURN 1"}, nil)
	_, _, err := bad.Load(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "uri")

	bad2 := NewNeo4j(&Neo4jConfig{URI: "bolt://x"}, nil)
	_, _, err = bad2.Load(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "query")
}
