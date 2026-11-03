// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package neo4jqueryreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/neo4jqueryreceiver"

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

// errNullValue is returned when a Cypher null is found where a value is required.
var errNullValue = errors.New("null value")

// row is a single Cypher result record keyed by RETURN column name. Values
// keep the Go type delivered by the Bolt driver (int64, float64, string,
// bool, time.Time, dbtype.* ...); a Cypher null is stored as nil.
type row map[string]any

// cellToString renders a Cypher value as the string used for data point
// attributes and row_condition matching.
func cellToString(v any) (string, error) {
	switch t := v.(type) {
	case nil:
		return "", errNullValue
	case string:
		return t, nil
	case bool:
		return strconv.FormatBool(t), nil
	case int64:
		return strconv.FormatInt(t, 10), nil
	case int:
		return strconv.Itoa(t), nil
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64), nil
	case []byte:
		return string(t), nil
	case time.Time:
		return t.Format(time.RFC3339Nano), nil
	case dbtype.LocalDateTime:
		return t.String(), nil
	case dbtype.Date:
		return t.String(), nil
	case fmt.Stringer:
		return t.String(), nil
	}
	// Lists, maps, nodes, relationships and paths: JSON is the most useful
	// textual form; fall back to %v for anything the encoder rejects.
	if b, err := json.Marshal(v); err == nil {
		return string(b), nil
	}
	return fmt.Sprintf("%v", v), nil
}

// cellToInt64 converts a Cypher value to an integer data point value.
// Floats are accepted only when they carry no fractional part so that a
// misconfigured value_type surfaces as an error instead of silent truncation.
func cellToInt64(v any) (int64, error) {
	switch t := v.(type) {
	case nil:
		return 0, errNullValue
	case int64:
		return t, nil
	case int:
		return int64(t), nil
	case float64:
		if math.IsNaN(t) || math.IsInf(t, 0) || t != math.Trunc(t) {
			return 0, fmt.Errorf("value %v is not an integer; use value_type: double", t)
		}
		return int64(t), nil
	case bool:
		if t {
			return 1, nil
		}
		return 0, nil
	case string:
		i, err := strconv.ParseInt(t, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("error converting %q to integer: %w", t, err)
		}
		return i, nil
	}
	return 0, fmt.Errorf("cannot convert %T to integer", v)
}

// cellToFloat64 converts a Cypher value to a double data point value.
func cellToFloat64(v any) (float64, error) {
	switch t := v.(type) {
	case nil:
		return 0, errNullValue
	case float64:
		return t, nil
	case int64:
		return float64(t), nil
	case int:
		return float64(t), nil
	case bool:
		if t {
			return 1, nil
		}
		return 0, nil
	case string:
		f, err := strconv.ParseFloat(t, 64)
		if err != nil {
			return 0, fmt.Errorf("error converting %q to double: %w", t, err)
		}
		return f, nil
	}
	return 0, fmt.Errorf("cannot convert %T to double", v)
}

// cellToTimestamp converts a Cypher value to a pcommon.Timestamp. Temporal
// driver types are used directly; numbers and numeric strings are read as
// nanoseconds since the Unix epoch, matching the sqlqueryreceiver.
func cellToTimestamp(v any) (pcommon.Timestamp, error) {
	switch t := v.(type) {
	case nil:
		return 0, errNullValue
	case time.Time:
		return pcommon.NewTimestampFromTime(t), nil
	case dbtype.LocalDateTime:
		return pcommon.NewTimestampFromTime(t.Time()), nil
	case dbtype.Date:
		return pcommon.NewTimestampFromTime(t.Time()), nil
	case int64:
		if t < 0 {
			return 0, fmt.Errorf("timestamp %d must not be negative", t)
		}
		return pcommon.Timestamp(t), nil
	case int:
		if t < 0 {
			return 0, fmt.Errorf("timestamp %d must not be negative", t)
		}
		return pcommon.Timestamp(t), nil
	case float64:
		if t < 0 || t != math.Trunc(t) {
			return 0, fmt.Errorf("timestamp %v must be a non-negative integer number of nanoseconds", t)
		}
		return pcommon.Timestamp(t), nil
	case string:
		i, err := strconv.ParseInt(t, 10, 64)
		if err != nil || i < 0 {
			return 0, fmt.Errorf("timestamp %q must be a non-negative integer number of nanoseconds", t)
		}
		return pcommon.Timestamp(i), nil
	}
	return 0, fmt.Errorf("cannot convert %T to timestamp", v)
}
