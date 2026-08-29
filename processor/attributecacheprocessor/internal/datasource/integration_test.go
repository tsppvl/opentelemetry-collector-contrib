// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package datasource

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	tcmysql "github.com/testcontainers/testcontainers-go/modules/mysql"
	tcneo4j "github.com/testcontainers/testcontainers-go/modules/neo4j"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributecacheprocessor/internal/datasource/drivers"
)

// ---------------------------------------------------------------------------
// Postgres integration test
// ---------------------------------------------------------------------------

func TestIntegration_Postgres(t *testing.T) {
	ctx := context.Background()

	ctr, err := tcpostgres.Run(ctx, "postgres:16",
		tcpostgres.WithUsername("testuser"),
		tcpostgres.WithPassword("testpass"),
		tcpostgres.WithDatabase("testdb"),
		tcpostgres.BasicWaitStrategies(),
	)
	require.NoError(t, err, "start postgres container")
	t.Cleanup(func() {
		if err := ctr.Terminate(ctx); err != nil {
			t.Logf("postgres container terminate: %v", err)
		}
	})

	connStr, err := ctr.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err, "get connection string")

	// Seed test data directly via database/sql.
	db, err := sql.Open("pgx", connStr)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.ExecContext(ctx, `CREATE TABLE employees (
		id   TEXT,
		name TEXT,
		dept TEXT
	)`)
	require.NoError(t, err)

	employees := []struct{ id, name, dept string }{
		{"1", "alice", "eng"},
		{"2", "bob", "ops"},
		{"3", "carol", "sre"},
		{"4", "dave", "eng"},
		{"5", "eve", "sec"},
	}
	for _, e := range employees {
		_, err = db.ExecContext(ctx,
			`INSERT INTO employees (id, name, dept) VALUES ($1, $2, $3)`,
			e.id, e.name, e.dept)
		require.NoError(t, err)
	}

	// Now use SQLDataSource (pgx driver is registered as "pgx" by drivers package).
	ds := NewSQL(&SQLConfig{
		Driver: "pgx",
		DSN:    connStr,
		Query:  "SELECT id, name, dept FROM employees ORDER BY id",
	}, nil)
	t.Cleanup(func() { _ = ds.Close(ctx) })

	cols, rows, err := ds.Load(ctx)
	require.NoError(t, err, "Load must succeed")

	assert.Equal(t, []string{"id", "name", "dept"}, cols)
	require.Len(t, rows, 5, "expected 5 rows")
	assert.Equal(t, "alice", rows[0]["name"])
	assert.Equal(t, "eng", rows[0]["dept"])
	assert.Equal(t, "bob", rows[1]["name"])
	assert.Equal(t, "eve", rows[4]["name"])
	assert.Equal(t, "sec", rows[4]["dept"])
}

// ---------------------------------------------------------------------------
// MySQL integration test
// ---------------------------------------------------------------------------

func TestIntegration_MySQL(t *testing.T) {
	ctx := context.Background()

	ctr, err := tcmysql.Run(ctx, "mysql:8",
		tcmysql.WithUsername("testuser"),
		tcmysql.WithPassword("testpass"),
		tcmysql.WithDatabase("testdb"),
	)
	require.NoError(t, err, "start mysql container")
	t.Cleanup(func() {
		if err := ctr.Terminate(ctx); err != nil {
			t.Logf("mysql container terminate: %v", err)
		}
	})

	connStr, err := ctr.ConnectionString(ctx)
	require.NoError(t, err, "get connection string")

	// Seed test data.
	db, err := sql.Open("mysql", connStr)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.ExecContext(ctx, `CREATE TABLE products (
		sku   VARCHAR(32),
		label VARCHAR(128),
		price VARCHAR(32)
	)`)
	require.NoError(t, err)

	products := []struct{ sku, label, price string }{
		{"P001", "Widget A", "9.99"},
		{"P002", "Widget B", "19.99"},
		{"P003", "Gadget X", "49.99"},
		{"P004", "Gadget Y", "99.99"},
		{"P005", "Thing Z", "4.99"},
	}
	for _, p := range products {
		_, err = db.ExecContext(ctx,
			`INSERT INTO products (sku, label, price) VALUES (?, ?, ?)`,
			p.sku, p.label, p.price)
		require.NoError(t, err)
	}

	ds := NewSQL(&SQLConfig{
		Driver: "mysql",
		DSN:    connStr,
		Query:  "SELECT sku, label, price FROM products ORDER BY sku",
	}, nil)
	t.Cleanup(func() { _ = ds.Close(ctx) })

	cols, rows, err := ds.Load(ctx)
	require.NoError(t, err, "Load must succeed")

	assert.Equal(t, []string{"sku", "label", "price"}, cols)
	require.Len(t, rows, 5, "expected 5 rows")
	assert.Equal(t, "P001", rows[0]["sku"])
	assert.Equal(t, "Widget A", rows[0]["label"])
	assert.Equal(t, "9.99", rows[0]["price"])
	assert.Equal(t, "P005", rows[4]["sku"])
	assert.Equal(t, "4.99", rows[4]["price"])
}

// ---------------------------------------------------------------------------
// Neo4j integration test
// ---------------------------------------------------------------------------

func TestIntegration_Neo4j(t *testing.T) {
	ctx := context.Background()

	ctr, err := tcneo4j.Run(ctx, "neo4j:5",
		tcneo4j.WithoutAuthentication(),
	)
	require.NoError(t, err, "start neo4j container")
	t.Cleanup(func() {
		if err := ctr.Terminate(ctx); err != nil {
			t.Logf("neo4j container terminate: %v", err)
		}
	})

	boltURL, err := ctr.BoltUrl(ctx)
	require.NoError(t, err, "get bolt URL")

	// Seed data using a write transaction directly via the neo4j driver.
	drv, err := neo4j.NewDriverWithContext(boltURL, neo4j.NoAuth())
	require.NoError(t, err, "open neo4j driver for seeding")
	defer drv.Close(ctx) //nolint:errcheck

	session := drv.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx) //nolint:errcheck

	cities := []struct{ id, city, country string }{
		{"1", "Berlin", "DE"},
		{"2", "Prague", "CZ"},
		{"3", "Tokyo", "JP"},
		{"4", "Paris", "FR"},
		{"5", "Madrid", "ES"},
	}
	for _, c := range cities {
		_, err = session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
			_, err := tx.Run(ctx,
				"CREATE (c:City {id: $id, city: $city, country: $country})",
				map[string]any{"id": c.id, "city": c.city, "country": c.country},
			)
			return nil, err
		})
		require.NoErrorf(t, err, "seed city %s", c.city)
	}
	require.NoError(t, session.Close(ctx))
	require.NoError(t, drv.Close(ctx))

	// Query via Neo4jDataSource (read path).
	ds := NewNeo4j(&Neo4jConfig{
		URI:      boltURL,
		Username: "",
		Password: "",
		Query:    "MATCH (c:City) RETURN c.id AS id, c.city AS city, c.country AS country ORDER BY c.id",
	}, nil)
	t.Cleanup(func() { _ = ds.Close(ctx) })

	cols, rows, err := ds.Load(ctx)
	require.NoError(t, err, "Load must succeed")

	assert.Len(t, cols, 3, "expected 3 columns")
	require.Len(t, rows, 5, "expected 5 rows")

	// Spot-check a couple of rows.
	var berlinFound, tokyoFound bool
	for _, row := range rows {
		if row["city"] == "Berlin" && row["country"] == "DE" {
			berlinFound = true
		}
		if row["city"] == "Tokyo" && row["country"] == "JP" {
			tokyoFound = true
		}
	}
	assert.True(t, berlinFound, "Berlin row must be present")
	assert.True(t, tokyoFound, "Tokyo row must be present")
}

// ---------------------------------------------------------------------------
// Inline integration test (no container)
// ---------------------------------------------------------------------------

func TestIntegration_Inline(t *testing.T) {
	rows := []map[string]string{
		{"env": "prod", "service": "checkout", "owner": "team-a"},
		{"env": "prod", "service": "payments", "owner": "team-b"},
		{"env": "staging", "service": "checkout", "owner": "team-c"},
		{"env": "dev", "service": "search", "owner": "team-d"},
		{"env": "dev", "service": "api", "owner": "team-e"},
	}

	ds := NewInline(&InlineConfig{Rows: rows})

	cols, got, err := ds.Load(context.Background())
	require.NoError(t, err)
	// Columns are sorted alphabetically by InlineDataSource.
	assert.Equal(t, []string{"env", "owner", "service"}, cols)
	require.Len(t, got, 5, "expected 5 rows")

	// Verify all rows are present (InlineDataSource returns the same slice).
	assert.Equal(t, "prod", got[0]["env"])
	assert.Equal(t, "checkout", got[0]["service"])
	assert.Equal(t, "team-a", got[0]["owner"])

	assert.Equal(t, "dev", got[4]["env"])
	assert.Equal(t, "api", got[4]["service"])
	assert.Equal(t, "team-e", got[4]["owner"])

	// Close is a no-op but must not error.
	assert.NoError(t, ds.Close(context.Background()))
}

// ---------------------------------------------------------------------------
// Tempfile CSV integration test (no container)
// ---------------------------------------------------------------------------

func TestIntegration_CSV_HeaderMode(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "test-*.csv")
	require.NoError(t, err)

	content := "country,capital,population\n" +
		"Germany,Berlin,83000000\n" +
		"CzechRepublic,Prague,10900000\n" +
		"Japan,Tokyo,126000000\n" +
		"France,Paris,67000000\n" +
		"Spain,Madrid,47000000\n"

	_, err = fmt.Fprint(f, content)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	ds := NewCSV(&CSVConfig{
		Path:      f.Name(),
		HasHeader: true,
	})

	cols, rows, err := ds.Load(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"country", "capital", "population"}, cols)
	require.Len(t, rows, 5, "expected 5 data rows")

	assert.Equal(t, "Germany", rows[0]["country"])
	assert.Equal(t, "Berlin", rows[0]["capital"])
	assert.Equal(t, "83000000", rows[0]["population"])

	assert.Equal(t, "Spain", rows[4]["country"])
	assert.Equal(t, "Madrid", rows[4]["capital"])

	assert.NoError(t, ds.Close(context.Background()))
}

func TestIntegration_CSV_PositionalMode(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "test-positional-*.csv")
	require.NoError(t, err)

	// No header — positional mode, columns emitted as col0, col1, col2.
	content := "prod,checkout,team-a\n" +
		"prod,payments,team-b\n" +
		"staging,checkout,team-c\n" +
		"dev,search,team-d\n" +
		"dev,api,team-e\n"

	_, err = fmt.Fprint(f, content)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	ds := NewCSV(&CSVConfig{
		Path:      f.Name(),
		HasHeader: false,
	})

	cols, rows, err := ds.Load(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"col0", "col1", "col2"}, cols)
	require.Len(t, rows, 5, "expected 5 rows")

	assert.Equal(t, "prod", rows[0]["col0"])
	assert.Equal(t, "checkout", rows[0]["col1"])
	assert.Equal(t, "team-a", rows[0]["col2"])

	assert.Equal(t, "dev", rows[4]["col0"])
	assert.Equal(t, "api", rows[4]["col1"])
	assert.Equal(t, "team-e", rows[4]["col2"])

	assert.NoError(t, ds.Close(context.Background()))
}
