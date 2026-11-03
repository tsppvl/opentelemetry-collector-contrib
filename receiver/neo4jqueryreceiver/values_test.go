// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package neo4jqueryreceiver

import (
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func TestCellToString(t *testing.T) {
	ts := time.Date(2026, 9, 7, 12, 30, 0, 500, time.UTC)
	tests := []struct {
		name string
		in   any
		want string
	}{
		{"string", "sci-fi", "sci-fi"},
		{"bool", true, "true"},
		{"int64", int64(42), "42"},
		{"int", 7, "7"},
		{"float whole", 2.0, "2"},
		{"float fraction", 2.5, "2.5"},
		{"bytes", []byte("raw"), "raw"},
		{"time", ts, "2026-09-07T12:30:00.0000005Z"},
		{"local datetime", dbtype.LocalDateTime(time.Date(2026, 9, 7, 12, 30, 0, 0, time.Local)), "2026-09-07T12:30:00"},
		{"date", dbtype.Date(time.Date(2026, 9, 7, 0, 0, 0, 0, time.Local)), "2026-09-07"},
		{"duration", dbtype.Duration{Months: 1, Days: 2, Seconds: 3}, "P1M2DT3S"},
		{"list", []any{"a", int64(1)}, `["a",1]`},
		{"map", map[string]any{"k": "v"}, `{"k":"v"}`},
		{"node", dbtype.Node{ElementId: "4:abc:1", Labels: []string{"Movie"}, Props: map[string]any{"title": "E.T."}}, `{"Id":0,"ElementId":"4:abc:1","Labels":["Movie"],"Props":{"title":"E.T."}}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := cellToString(tt.in)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}

	_, err := cellToString(nil)
	require.ErrorIs(t, err, errNullValue)
}

func TestCellToInt64(t *testing.T) {
	tests := []struct {
		name    string
		in      any
		want    int64
		wantErr bool
	}{
		{"int64", int64(5), 5, false},
		{"int", 5, 5, false},
		{"whole float", 5.0, 5, false},
		{"fractional float", 5.5, 0, true},
		{"bool true", true, 1, false},
		{"bool false", false, 0, false},
		{"numeric string", "12", 12, false},
		{"bad string", "twelve", 0, true},
		{"null", nil, 0, true},
		{"unsupported", []any{1}, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := cellToInt64(tt.in)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestCellToFloat64(t *testing.T) {
	tests := []struct {
		name    string
		in      any
		want    float64
		wantErr bool
	}{
		{"float", 2.5, 2.5, false},
		{"int64", int64(3), 3, false},
		{"int", 3, 3, false},
		{"bool", true, 1, false},
		{"numeric string", "1.25", 1.25, false},
		{"bad string", "pi", 0, true},
		{"null", nil, 0, true},
		{"unsupported", map[string]any{}, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := cellToFloat64(tt.in)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.InDelta(t, tt.want, got, 1e-9)
		})
	}
}

func TestCellToTimestamp(t *testing.T) {
	when := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	want := pcommon.NewTimestampFromTime(when)
	nanos := when.UnixNano()

	tests := []struct {
		name    string
		in      any
		want    pcommon.Timestamp
		wantErr bool
	}{
		{"time", when, want, false},
		{"local datetime", dbtype.LocalDateTime(when), pcommon.NewTimestampFromTime(dbtype.LocalDateTime(when).Time()), false},
		{"date", dbtype.Date(when), pcommon.NewTimestampFromTime(dbtype.Date(when).Time()), false},
		{"int64 nanos", nanos, want, false},
		{"int nanos", int(nanos), want, false},
		{"float nanos", float64(nanos), want, false},
		{"string nanos", "1788782400000000000", want, false},
		{"negative", int64(-1), 0, true},
		{"fractional float", 1.5, 0, true},
		{"bad string", "yesterday", 0, true},
		{"null", nil, 0, true},
		{"unsupported", true, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := cellToTimestamp(tt.in)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
