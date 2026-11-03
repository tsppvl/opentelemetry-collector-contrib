// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package datasource

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	// Blank import to register all six SQL drivers — required so that
	// TestDriversRegistered and the SQLite-backed tests find their drivers
	// without each test having to import the underlying packages.
	_ "github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributecacheprocessor/internal/datasource/drivers"
)

// sqliteDSN returns a per-test SQLite DSN backed by an on-disk file in
// t.TempDir(). Using a real file (rather than ":memory:") avoids
// connection-pool-induced cache surprises across the multiple connections
// database/sql may open: every connection sees the same persisted state.
func sqliteDSN(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "test.db")
}

// seedSQLite opens the given DSN with the "sqlite" driver, applies the
// supplied DDL/DML statements, and returns the closed DB so the test code
// can re-open it through SQLDataSource.
func seedSQLite(t *testing.T, dsn string, stmts ...string) {
	t.Helper()
	db, err := sql.Open("sqlite", dsn)
	require.NoError(t, err)
	defer db.Close()
	for _, s := range stmts {
		_, err := db.Exec(s)
		require.NoErrorf(t, err, "seed stmt: %s", s)
	}
}

func TestSQLLoad_basic(t *testing.T) {
	dsn := sqliteDSN(t)
	seedSQLite(t, dsn,
		`CREATE TABLE people (name TEXT, city TEXT, role TEXT)`,
		`INSERT INTO people VALUES ('alice','berlin','eng'),('bob','prague','ops'),('carol','tokyo','sre')`,
	)

	ds := NewSQL(&SQLConfig{
		Driver: "sqlite",
		DSN:    dsn,
		Query:  "SELECT name, city, role FROM people ORDER BY name",
	}, nil)
	t.Cleanup(func() { _ = ds.Close(context.Background()) })

	cols, rows, err := ds.Load(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"name", "city", "role"}, cols)
	require.Len(t, rows, 3)
	assert.Equal(t, "alice", rows[0]["name"])
	assert.Equal(t, "berlin", rows[0]["city"])
	assert.Equal(t, "eng", rows[0]["role"])
	assert.Equal(t, "bob", rows[1]["name"])
	assert.Equal(t, "tokyo", rows[2]["city"])

	// A second Load reuses the open *sql.DB and must succeed too.
	_, rows2, err := ds.Load(context.Background())
	require.NoError(t, err)
	assert.Len(t, rows2, 3)
}

func TestSQLLoad_nullValues(t *testing.T) {
	dsn := sqliteDSN(t)
	seedSQLite(t, dsn,
		`CREATE TABLE t (a TEXT, b TEXT)`,
		`INSERT INTO t VALUES ('one', NULL), (NULL, 'two')`,
	)

	ds := NewSQL(&SQLConfig{
		Driver: "sqlite",
		DSN:    dsn,
		Query:  "SELECT a, b FROM t ORDER BY a IS NULL, a",
	}, nil)
	t.Cleanup(func() { _ = ds.Close(context.Background()) })

	cols, rows, err := ds.Load(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, cols)
	require.Len(t, rows, 2)

	assert.Equal(t, "one", rows[0]["a"])
	assert.Equal(t, "", rows[0]["b"], "NULL must scan to empty string, not panic")

	assert.Equal(t, "", rows[1]["a"], "NULL must scan to empty string, not panic")
	assert.Equal(t, "two", rows[1]["b"])
}

func TestSQLLoad_queryError(t *testing.T) {
	dsn := sqliteDSN(t)
	seedSQLite(t, dsn, `CREATE TABLE t (a TEXT)`)

	ds := NewSQL(&SQLConfig{
		Driver: "sqlite",
		DSN:    dsn,
		Query:  "SELECT * FROM table_that_does_not_exist",
	}, nil)
	t.Cleanup(func() { _ = ds.Close(context.Background()) })

	cols, rows, err := ds.Load(context.Background())
	require.Error(t, err)
	assert.Nil(t, cols)
	assert.Nil(t, rows)
}

func TestSQLLoad_closeIdle(t *testing.T) {
	// Source is constructed but Load is never called → db is nil → Close
	// must be a no-op and must never panic.
	ds := NewSQL(&SQLConfig{
		Driver: "sqlite",
		DSN:    sqliteDSN(t),
		Query:  "SELECT 1",
	}, nil)

	require.NoError(t, ds.Close(context.Background()))
	// Calling Close a second time is also safe.
	require.NoError(t, ds.Close(context.Background()))
}

func TestDriversRegistered(t *testing.T) {
	want := []string{
		"clickhouse",
		"mysql",
		"pgx",
		"sqlserver",
		"oracle",
		"sqlite",
	}

	got := make(map[string]struct{})
	for _, name := range sql.Drivers() {
		got[name] = struct{}{}
	}

	for _, name := range want {
		_, ok := got[name]
		assert.Truef(t, ok, "driver %q not registered (got: %v)", name, sql.Drivers())
	}
}
