// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package datasource

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeTempFile creates a file inside t.TempDir() and returns its absolute path.
func writeTempFile(t *testing.T, name, content string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(p, []byte(content), 0o600))
	return p
}

func TestCSVLoad_headerMode(t *testing.T) {
	const data = "name,city,role\nalice,berlin,eng\nbob,prague,ops\ncarol,tokyo,sre\n"
	path := writeTempFile(t, "header.csv", data)

	ds := NewCSV(&CSVConfig{Path: path, HasHeader: true})
	cols, rows, err := ds.Load(context.Background())
	require.NoError(t, err)

	assert.Equal(t, []string{"name", "city", "role"}, cols)
	require.Len(t, rows, 3)
	assert.Equal(t, "alice", rows[0]["name"])
	assert.Equal(t, "berlin", rows[0]["city"])
	assert.Equal(t, "eng", rows[0]["role"])
	assert.Equal(t, "bob", rows[1]["name"])
	assert.Equal(t, "tokyo", rows[2]["city"])

	require.NoError(t, ds.Close(context.Background()))
}

func TestCSVLoad_positionalMode(t *testing.T) {
	const data = "alice,berlin,eng\nbob,prague,ops\n"
	path := writeTempFile(t, "positional.csv", data)

	ds := NewCSV(&CSVConfig{Path: path, HasHeader: false})
	cols, rows, err := ds.Load(context.Background())
	require.NoError(t, err)

	assert.Equal(t, []string{"col0", "col1", "col2"}, cols)
	require.Len(t, rows, 2)
	assert.Equal(t, "alice", rows[0]["col0"])
	assert.Equal(t, "berlin", rows[0]["col1"])
	assert.Equal(t, "eng", rows[0]["col2"])
	assert.Equal(t, "bob", rows[1]["col0"])
	assert.Equal(t, "ops", rows[1]["col2"])
}

func TestCSVLoad_shortRow(t *testing.T) {
	// Second row has only 2 fields where the widest row has 3 → must be skipped.
	const data = "alice,berlin,eng\nbob,prague\ncarol,tokyo,sre\n"
	path := writeTempFile(t, "short.csv", data)

	ds := NewCSV(&CSVConfig{Path: path, HasHeader: false})
	cols, rows, err := ds.Load(context.Background())
	require.NoError(t, err)

	assert.Equal(t, []string{"col0", "col1", "col2"}, cols)
	require.Len(t, rows, 2, "row with too few fields must be skipped")
	assert.Equal(t, "alice", rows[0]["col0"])
	assert.Equal(t, "carol", rows[1]["col0"])
	assert.Equal(t, "sre", rows[1]["col2"])
}

func TestCSVLoad_customSeparator(t *testing.T) {
	const data = "name;city;role\nalice;berlin;eng\nbob;prague;ops\n"
	path := writeTempFile(t, "semicolon.csv", data)

	ds := NewCSV(&CSVConfig{Path: path, HasHeader: true, FieldSeparator: ";"})
	cols, rows, err := ds.Load(context.Background())
	require.NoError(t, err)

	assert.Equal(t, []string{"name", "city", "role"}, cols)
	require.Len(t, rows, 2)
	assert.Equal(t, "alice", rows[0]["name"])
	assert.Equal(t, "prague", rows[1]["city"])
}

func TestCSVLoad_emptyFile(t *testing.T) {
	t.Run("header mode", func(t *testing.T) {
		path := writeTempFile(t, "empty_header.csv", "")
		ds := NewCSV(&CSVConfig{Path: path, HasHeader: true})
		cols, rows, err := ds.Load(context.Background())
		require.NoError(t, err)
		assert.Nil(t, cols)
		assert.Nil(t, rows)
	})
	t.Run("positional mode", func(t *testing.T) {
		path := writeTempFile(t, "empty_positional.csv", "")
		ds := NewCSV(&CSVConfig{Path: path, HasHeader: false})
		cols, rows, err := ds.Load(context.Background())
		require.NoError(t, err)
		assert.Nil(t, cols)
		assert.Nil(t, rows)
	})
}

func TestCSVLoad_validation(t *testing.T) {
	t.Run("missing path", func(t *testing.T) {
		ds := NewCSV(&CSVConfig{})
		_, _, err := ds.Load(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "path must be set")
	})

	t.Run("multi-rune separator", func(t *testing.T) {
		path := writeTempFile(t, "any.csv", "a,b\n1,2\n")
		ds := NewCSV(&CSVConfig{Path: path, HasHeader: true, FieldSeparator: ";;"})
		_, _, err := ds.Load(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "field_separator")
	})

	t.Run("missing file", func(t *testing.T) {
		ds := NewCSV(&CSVConfig{Path: filepath.Join(t.TempDir(), "does-not-exist.csv"), HasHeader: true})
		_, _, err := ds.Load(context.Background())
		require.Error(t, err)
	})
}

func TestCSVLoad_nilConfig(t *testing.T) {
	ds := NewCSV(nil)
	_, _, err := ds.Load(context.Background())
	require.Error(t, err, "nil-derived config must produce an error (path must be set)")
}

func TestCSVLoad_headerModeSkipsUnequalRows(t *testing.T) {
	// Header has 3 cols; one data row has 4 (too many) and one has 2 (too few).
	// Both must be skipped; only the valid row survives.
	const data = "a,b,c\nv1,v2,v3\nonly-two,fields\ntoo,many,fields,x\n"
	path := writeTempFile(t, "unequal.csv", data)

	ds := NewCSV(&CSVConfig{Path: path, HasHeader: true})
	cols, rows, err := ds.Load(context.Background())
	require.NoError(t, err)

	assert.Equal(t, []string{"a", "b", "c"}, cols)
	require.Len(t, rows, 1, "only the valid 3-field row should survive")
	assert.Equal(t, "v1", rows[0]["a"])
}

func TestCSVLoad_headerModeOnlyHeader(t *testing.T) {
	// File contains a header but no data rows.
	const data = "name,city,role\n"
	path := writeTempFile(t, "header_only.csv", data)

	ds := NewCSV(&CSVConfig{Path: path, HasHeader: true})
	cols, rows, err := ds.Load(context.Background())
	require.NoError(t, err)

	assert.Equal(t, []string{"name", "city", "role"}, cols)
	assert.Empty(t, rows)
}

func TestCSVLoad_positionalModeAllEqual(t *testing.T) {
	// All rows have the same width — the happy path for positional mode.
	const data = "x,y\na,b\nc,d\n"
	path := writeTempFile(t, "positional_equal.csv", data)

	ds := NewCSV(&CSVConfig{Path: path, HasHeader: false})
	cols, rows, err := ds.Load(context.Background())
	require.NoError(t, err)

	assert.Equal(t, []string{"col0", "col1"}, cols)
	require.Len(t, rows, 3)
	assert.Equal(t, "x", rows[0]["col0"])
	assert.Equal(t, "d", rows[2]["col1"])
}

func TestCSVLoad_emptyFieldsPreserved(t *testing.T) {
	// Empty fields in data rows must be stored as empty string, not omitted.
	const data = "a,b,c\nv1,,v3\n"
	path := writeTempFile(t, "empty_field.csv", data)

	ds := NewCSV(&CSVConfig{Path: path, HasHeader: true})
	cols, rows, err := ds.Load(context.Background())
	require.NoError(t, err)

	assert.Equal(t, []string{"a", "b", "c"}, cols)
	require.Len(t, rows, 1)
	assert.Equal(t, "v1", rows[0]["a"])
	assert.Equal(t, "", rows[0]["b"], "empty field must be stored as empty string")
	assert.Equal(t, "v3", rows[0]["c"])
}

func TestCSVLoad_closeIsNoOp(t *testing.T) {
	path := writeTempFile(t, "noop.csv", "a,b\n1,2\n")
	ds := NewCSV(&CSVConfig{Path: path, HasHeader: true})
	_, _, err := ds.Load(context.Background())
	require.NoError(t, err)
	// Close must not return an error regardless of how many times it's called.
	require.NoError(t, ds.Close(context.Background()))
	require.NoError(t, ds.Close(context.Background()))
}

func TestCSVLoad_fieldQuotingValidation(t *testing.T) {
	// Multi-rune field_quoting must be rejected.
	path := writeTempFile(t, "any2.csv", "a,b\n1,2\n")
	ds := NewCSV(&CSVConfig{Path: path, HasHeader: true, FieldQuoting: "``"})
	_, _, err := ds.Load(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "field_quoting")
}

func TestCSVLoad_quotedFields(t *testing.T) {
	// Values may contain the separator if enclosed in quotes.
	const data = "name,desc\nalice,\"hello, world\"\nbob,plain\n"
	path := writeTempFile(t, "quoted.csv", data)

	ds := NewCSV(&CSVConfig{Path: path, HasHeader: true})
	cols, rows, err := ds.Load(context.Background())
	require.NoError(t, err)

	assert.Equal(t, []string{"name", "desc"}, cols)
	require.Len(t, rows, 2)
	assert.Equal(t, "hello, world", rows[0]["desc"])
	assert.Equal(t, "plain", rows[1]["desc"])
}
