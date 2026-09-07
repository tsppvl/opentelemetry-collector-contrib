// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package datasource

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInlineLoad_empty(t *testing.T) {
	t.Run("nil config", func(t *testing.T) {
		ds := NewInline(nil)
		cols, rows, err := ds.Load(context.Background())
		require.NoError(t, err)
		assert.Nil(t, cols)
		assert.Nil(t, rows)
	})
	t.Run("empty rows", func(t *testing.T) {
		ds := NewInline(&InlineConfig{})
		cols, rows, err := ds.Load(context.Background())
		require.NoError(t, err)
		assert.Nil(t, cols)
		assert.Nil(t, rows)
	})
}

func TestInlineLoad_columns(t *testing.T) {
	cfg := &InlineConfig{
		Rows: []map[string]string{
			{"zeta": "1", "alpha": "2", "mike": "3"},
		},
	}
	ds := NewInline(cfg)

	cols, _, err := ds.Load(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"alpha", "mike", "zeta"}, cols, "columns must be sorted alphabetically")
}

func TestInlineLoad_rows(t *testing.T) {
	cfg := &InlineConfig{
		Rows: []map[string]string{
			{"name": "alice", "city": "berlin"},
			{"name": "bob", "city": "prague"},
			{"name": "carol", "city": "tokyo"},
		},
	}
	ds := NewInline(cfg)

	cols, rows, err := ds.Load(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"city", "name"}, cols)
	require.Len(t, rows, 3)
	assert.Equal(t, "alice", rows[0]["name"])
	assert.Equal(t, "berlin", rows[0]["city"])
	assert.Equal(t, "bob", rows[1]["name"])
	assert.Equal(t, "carol", rows[2]["name"])

	// Close is a no-op.
	require.NoError(t, ds.Close(context.Background()))
}

func TestInlineLoad_doesNotMutateConfig(t *testing.T) {
	original := []map[string]string{
		{"k": "v1"},
		{"k": "v2"},
	}
	cfg := &InlineConfig{Rows: original}
	ds := NewInline(cfg)

	// Mutate the original slice header (e.g. caller appends after construction).
	cfg.Rows = append(cfg.Rows, map[string]string{"k": "v3"})

	_, rows, err := ds.Load(context.Background())
	require.NoError(t, err)
	assert.Len(t, rows, 2, "data source must snapshot the slice header at construction time")
}
